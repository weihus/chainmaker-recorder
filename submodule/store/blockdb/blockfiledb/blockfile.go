/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/

package blockfiledb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	lwsf "chainmaker.org/chainmaker/lws/file"
	storePb "chainmaker.org/chainmaker/pb-go/v2/store"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/store/v2/utils"
	"github.com/tidwall/tinylru"
)

var (
	// ErrCorrupt is returns when the log is corrupt.
	ErrCorrupt = errors.New("log corrupt")

	// ErrClosed is returned when an operation cannot be completed because
	// the log is closed.
	ErrClosed = errors.New("log closed")

	// ErrNotFound is returned when an entry is not found.
	ErrNotFound = errors.New("not found")

	// ErrOutOfOrder is returned from Write() when the index is not equal to
	// LastIndex()+1. It's required that log monotonically grows by one and has
	// no gaps. Thus, the series 10,11,12,13,14 is valid, but 10,11,13,14 is
	// not because there's a gap between 11 and 13. Also, 10,12,11,13 is not
	// valid because 12 and 11 are out of order.
	ErrOutOfOrder = errors.New("out of order")

	// ErrInvalidateIndex wrap "invalidate rfile index"
	ErrInvalidateIndex = errors.New("invalidate rfile index")

	// ErrBlockWrite wrap "write block wfile size invalidate"
	ErrBlockWrite = errors.New("write block wfile size invalidate")
)

const (
	dbFileSuffix          = ".fdb"
	lastFileSuffix        = ".END"
	dbFileNameLen         = 20
	blockFilenameTemplate = "%020d"
)

// Options for BlockFile
type Options struct {
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync bool
	// SegmentSize of each segment. This is just a target value, actual size
	// may differ. Default is 20 MB.
	SegmentSize int
	// SegmentCacheSize is the maximum number of segments that will be held in
	// memory for caching. Increasing this value may enhance performance for
	// concurrent read operations. Default is 1
	SegmentCacheSize int
	// NoCopy allows for the Read() operation to return the raw underlying data
	// slice. This is an optimization to help minimize allocations. When this
	// option is set, do not modify the returned data because it may affect
	// other Read calls. Default false
	NoCopy bool
	// UseMmap It is a method of memory-mapped rfile I/O. It implements demand
	// paging because rfile contents are not read from disk directly and initially
	// do not use physical RAM at all
	UseMmap bool

	// ReadTimeOut read block file time out conf (ms)
	ReadTimeOut int64
}

// DefaultOptions for Open().
var DefaultOptions = &Options{
	NoSync:           false,    // Fsync after every write
	SegmentSize:      67108864, // 64 MB log segment files.
	SegmentCacheSize: 25,       // Number of cached in-memory segments
	NoCopy:           false,    // Make a new copy of data for every Read call.
	UseMmap:          true,     // use mmap for faster write block to file.
}

// BlockFile represents a block to rfile
type BlockFile struct {
	mu              sync.RWMutex
	path            string        // absolute path to log directory
	tmpPath         string        // absolute tmpPath to log directory, we can use this tmpPath to accelerate IO
	opts            Options       // log options
	closed          bool          // log is closed
	corrupt         bool          // log may be corrupt
	lastSegment     *segment      // last log segment
	lastIndex       uint64        // index of the last entry in log
	sfile           *lockableFile // tail segment rfile handle
	wbatch          Batch         // reusable write batch
	logger          protocol.Logger
	bclock          time.Time
	cachedBuf       []byte
	openedFileCache tinylru.LRU   // openedFile entries cache
	readTimeOut     time.Duration // read block file default time out
}

type lockableFile struct {
	sync.RWMutex
	//blkWriter BlockWriter
	wfile lwsf.WalFile
	rfile *os.File
}

// segment represents a single segment rfile.
type segment struct {
	path  string // path of segment rfile
	name  string // name of segment rfile
	index uint64 // first index of segment
	ebuf  []byte // cached entries buffer, storage format of one log entry: checksum|data_size|data
	epos  []bpos // cached entries positions in buffer
}

type bpos struct {
	pos       int // byte position
	end       int // one byte past pos
	prefixLen int
}

// Open a new write ahead log
//  @Description:
//  @param path
//  @param opts
//  @param logger
//  @return *BlockFile
//  @return error
func Open(path, tmpPath string, opts *Options, logger protocol.Logger) (*BlockFile, error) {
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.SegmentCacheSize <= 0 {
		opts.SegmentCacheSize = DefaultOptions.SegmentCacheSize
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultOptions.SegmentSize
	}

	var err error
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}

	useTmpFile := false
	if len(tmpPath) > 0 {
		if tmpPath, err = filepath.Abs(tmpPath); err == nil {
			if err = os.MkdirAll(tmpPath, 0777); err != nil {
				return nil, err
			}
			if err = utils.CheckPathRWMod(tmpPath); err == nil {
				useTmpFile = true
			}
		}
	}

	if !useTmpFile {
		tmpPath = ""
		logger.Infof("tmpPath is invalidate, will write block to path directly, err: %v", err)
	}

	rTimeOut := 1000 * time.Millisecond
	if opts.ReadTimeOut > 0 {
		rTimeOut = time.Duration(opts.ReadTimeOut) * time.Millisecond
	}

	l := &BlockFile{
		path:        path,
		tmpPath:     tmpPath,
		opts:        *opts,
		logger:      logger,
		cachedBuf:   make([]byte, 0, int(float32(opts.SegmentSize)*float32(1.5))),
		readTimeOut: rTimeOut,
	}

	l.openedFileCache.Resize(l.opts.SegmentCacheSize)
	if err = os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	if err = utils.CheckPathRWMod(path); err != nil {
		return nil, err
	}
	if err = l.load(); err != nil {
		return nil, err
	}

	if useTmpFile && l.useTmpPath() {
		go l.moveFDBFile()
	}

	return l, nil
}

// pushCache  add segFile to openedFileCache and remove segFile more than cache capacity
//  @Description:
//  @receiver l
//  @param path
//  @param segFile
func (l *BlockFile) pushCache(path string, segFile *lockableFile) {
	if strings.HasSuffix(path, lastFileSuffix) {
		// has lastFileSuffix mean this is a writing rfile, only happened when add new block entry to rfile,
		// we do not use ofile in other place, since we read new added block entry from memory (l.lastSegment.ebuf),
		// so we should not cache this rfile object
		return
	}
	_, _, _, v, evicted :=
		l.openedFileCache.SetEvicted(path, segFile)
	if evicted {
		// nolint
		if v == nil {
			return
		}
		if lfile, ok := v.(*lockableFile); ok {
			lfile.Lock()
			if lfile.wfile != nil {
				_ = lfile.wfile.Close()
			}
			if lfile.rfile != nil {
				_ = lfile.rfile.Close()
			}
			lfile.Unlock()
		}
	}
}

//  load
//  @Description: load all the segments. This operation also cleans up any START/END segments.
//  @receiver l
//  @return error
func (l *BlockFile) load() error {
	var err error
	if err = l.loadFromPath(); err != nil {
		return err
	}

	// for the first time to start
	if l.lastSegment == nil {
		// Create a new log
		segName := l.segmentName(1)
		l.lastSegment = &segment{
			name:  segName,
			index: 1,
			path:  l.segmentPathWithENDSuffix(segName),
			ebuf:  l.cachedBuf[:0],
		}
		l.lastIndex = 0
		l.sfile, err = l.openWriteFile(l.lastSegment.path)
		return err
	}

	// Open the last segment for appending
	if l.sfile, err = l.openWriteFile(l.lastSegment.path); err != nil {
		return err
	}

	// Customize part start
	// Load the last segment, only load uncorrupted log entries
	if err = l.loadSegmentEntriesForRestarting(l.lastSegment); err != nil {
		return err
	}
	// Customize part end
	l.lastIndex = l.lastSegment.index + uint64(len(l.lastSegment.epos)) - 1
	return nil
}

//  loadFromPath
//  @Description: 从指定目录加载segment/文件
//  @receiver l
//  @param path
//  @return error
func (l *BlockFile) loadFromPath() error {
	path := l.path
	if l.useTmpPath() {
		path = l.tmpPath
	}
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	var index, endFile, maxIndex uint64
	// during the restart, wal files are loaded to log.segments
	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() {
			continue
		}

		// mv .fdb file from tmpPath to path
		if l.useTmpPath() && strings.HasSuffix(name, dbFileSuffix) {
			if err1 := l.copyThenRemoveFile(
				filepath.Join(l.tmpPath, name),
				filepath.Join(l.path, name)); err1 != nil {
				return err1
			}
			continue
		}

		index, err = strconv.ParseUint(name[1:dbFileNameLen], 10, 64) // index most time is the first height of bfdb rfile
		if err != nil || index == 0 {
			continue
		}

		if index > maxIndex {
			maxIndex = index
		}

		if strings.HasSuffix(name, lastFileSuffix) && endFile < index {
			endFile = index
		}
	}

	segName := ""
	// use the max endFile
	if endFile > 0 {
		segName = l.segmentName(endFile)
		index = endFile
	} else if endFile == 0 && maxIndex > 0 {
		// can only find .fdb files instead of .END file, we need fix this encounter by rename last .fdb to .fdb.END
		segName = l.segmentName(maxIndex)
		fdbPath := l.segmentPath(segName, path)
		endPath := l.segmentPathWithENDSuffix(segName)
		l.logger.Warnf("can not find .END file, will move %s to %s as END file", fdbPath, endPath)
		if err1 := os.Rename(fdbPath, endPath); err1 != nil {
			return err1
		}
		index = maxIndex
	}

	if len(segName) == dbFileNameLen {
		l.lastSegment = &segment{
			name:  segName,
			index: index,
			path:  l.segmentPathWithENDSuffix(segName),
			ebuf:  l.cachedBuf[:0],
		}
	}

	return nil
}

//  openReadFile open read file from path then add opened file to openedFileCache
//  @Description:
//  @receiver l
//  @param path
//  @return *lockableFile
//  @return error
func (l *BlockFile) openReadFile(path string) (*lockableFile, error) {
	// Open the appropriate rfile as read-only.
	var (
		err     error
		isExist bool
		rfile   *os.File
		ofile   *lockableFile
	)

	fileV, isOK := l.openedFileCache.Get(path)
	if isOK && fileV != nil {
		if isExist, _ = utils.PathExists(path); isExist {
			ofil, ok := fileV.(*lockableFile)
			if ok && ofil != nil && ofil.rfile != nil {
				l.pushCache(path, ofil)
				return ofil, nil
			}
		}
	}

	if isExist, err = utils.PathExists(path); err != nil {
		return nil, err
	} else if !isExist {
		return nil, fmt.Errorf("bfdb rfile:%s missed", path)
	}

	rfile, err = os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	if _, err = rfile.Seek(0, 2); err != nil {
		return nil, err
	}

	ofile = &lockableFile{rfile: rfile}
	l.pushCache(path, ofile)
	return ofile, nil
}

// openReadFileWithTimeOut openReadFile with timeout feature
// @Description:
// @receiver l
// @param path
// @param timeOut
// @return *lockableFile
// @return error
func (l *BlockFile) openReadFileWithTimeOut(path string, timeOut time.Duration) (*lockableFile, error) {
	var (
		err    error
		lbFile *lockableFile
	)

	ch1 := make(chan int, 1)
	go func(ch chan<- int) {
		lbFile, err = l.openReadFile(path)
		ch <- 1
	}(ch1)
	timer := time.NewTimer(timeOut)

	select {
	case <-ch1:
		return lbFile, err
	case <-timer.C:
		err = fmt.Errorf("time out")
	}
	return nil, err
}

//  openWriteFile
//  @Description:
//  @receiver l
//  @param path
//  @return *lockableFile
//  @return error
func (l *BlockFile) openWriteFile(path string) (*lockableFile, error) {
	var (
		err     error
		isExist bool
		wfile   lwsf.WalFile
	)

	if isExist, err = utils.PathExists(path); err != nil {
		return nil, err
	}

	if !isExist && !strings.HasSuffix(path, lastFileSuffix) {
		return nil, fmt.Errorf("bfdb wfile:%s missed", path)
	}

	if l.opts.UseMmap {
		if wfile, err = lwsf.NewMmapFile(path, l.opts.SegmentSize); err != nil {
			l.logger.Warnf("failed use mmap: %v", err)
		}
	}

	if wfile == nil || err != nil {
		if wfile, err = lwsf.NewFile(path); err != nil {
			return nil, err
		}
	}

	return &lockableFile{wfile: wfile}, nil
}

//  segmentName
//  @Description: segmentName returns a 20-byte textual representation of an index
// for lexical ordering. This is used for the rfile names of log segments.
//  @receiver l
//  @param index
//  @return string
func (l *BlockFile) segmentName(index uint64) string {
	return fmt.Sprintf(blockFilenameTemplate, index)
}

//  segmentPath
//  @Description:
//  @receiver l
//  @param name
//  @return string
func (l *BlockFile) segmentPath(name, path string) string {
	return fmt.Sprintf("%s%s", filepath.Join(path, name), dbFileSuffix)
}

//  segmentPathWithENDSuffix
//  @Description:
//  @receiver l
//  @param name
//  @return string
func (l *BlockFile) segmentPathWithENDSuffix(name string) string {
	if strings.HasSuffix(name, lastFileSuffix) {
		return name
	}
	path := l.path
	if l.useTmpPath() {
		path = l.tmpPath
	}

	return fmt.Sprintf("%s%s", l.segmentPath(name, path), lastFileSuffix)
}

// Close the log.
//  @Description:
//  @receiver l
//  @return error
func (l *BlockFile) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		if l.corrupt {
			return ErrCorrupt
		}
		return ErrClosed
	}
	if err := l.sfile.wfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.wfile.Close(); err != nil {
		return err
	}
	l.closed = true
	if l.corrupt {
		return ErrCorrupt
	}

	l.openedFileCache.Resize(l.opts.SegmentCacheSize)
	return nil
}

//  Write
//  @Description: Write an entry to the block rfile db.
//  @receiver l
//  @param index
//  @param data
//  @return fileName
//  @return offset
//  @return blkLen
//  @return err
func (l *BlockFile) Write(index uint64, data []byte) (fileName string, offset, blkLen uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.bclock = time.Now()
	if l.corrupt {
		return "", 0, 0, ErrCorrupt
	} else if l.closed {
		return "", 0, 0, ErrClosed
	}
	l.wbatch.Write(index, data)
	bwi, err := l.writeBatch(&l.wbatch)
	if err != nil {
		return "", 0, 0, err
	}
	return bwi.FileName, bwi.Offset, bwi.ByteLen, nil
}

//func (l *BlockFile) appendEntry(dst []byte, index uint64, data []byte) (out []byte,
//	epos bpos) {
//	return appendBinaryEntry(dst, data)
//}

//  cycle
//  @Description: Cycle the old segment for a new segment.
//  @receiver l
//  @return error
func (l *BlockFile) cycle() error {
	if l.opts.NoSync {
		if err := l.sfile.wfile.Sync(); err != nil {
			return err
		}
	}
	if err := l.sfile.wfile.Close(); err != nil {
		return err
	}

	// remove pre last segment name's end suffix
	orgPath := l.lastSegment.path
	if !strings.HasSuffix(orgPath, lastFileSuffix) {
		return fmt.Errorf("last segment rfile dot not end with %s", lastFileSuffix)
	}
	finalPath := orgPath[:len(orgPath)-len(lastFileSuffix)]
	if err := os.Rename(orgPath, finalPath); err != nil {
		return err
	}

	// cache the previous lockfile
	l.pushCache(finalPath, l.sfile)

	// TODO power down issue
	segName := l.segmentName(l.lastIndex + 1)
	s := &segment{
		name:  segName,
		index: l.lastIndex + 1,
		path:  l.segmentPathWithENDSuffix(segName),
		ebuf:  l.cachedBuf[:0],
	}
	var err error
	if l.sfile, err = l.openWriteFile(s.path); err != nil {
		return err
	}
	l.lastSegment = s
	return nil
}

//  appendBinaryEntry
//  @Description: 拼接一个entry
//  @receiver l
//  @param dst
//  @param data
//  @return out
//  @return epos
func (l *BlockFile) appendBinaryEntry(dst []byte, data []byte) (out []byte, epos bpos) {
	// checksum + data_size + data
	pos := len(dst)
	// Customize part start
	dst = appendChecksum(dst, NewCRC(data).Value())
	// Customize part end
	dst = appendUvarint(dst, uint64(len(data)))
	prefixLen := len(dst) - pos
	dst = append(dst, data...)
	return dst, bpos{pos, len(dst), prefixLen}
}

//  appendChecksum
//  @Description: Customize part start
//  @param dst
//  @param checksum
//  @return []byte
func appendChecksum(dst []byte, checksum uint32) []byte {
	dst = append(dst, []byte("0000")...)
	binary.LittleEndian.PutUint32(dst[len(dst)-4:], checksum)
	return dst
}

//
//  appendUvarint
//  @Description: Customize part end
//  @param dst
//  @param x 整型序列化
//  @return []byte
func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

// Batch of entries. Used to write multiple entries at once using WriteBatch().
//  @Description:
type Batch struct {
	entry batchEntry
	data  []byte
}

//  batchEntry
//  @Description:
type batchEntry struct {
	index uint64
	size  int
}

//  Write
//  @Description: Write an entry to the batch
//  @receiver b
//  @param index
//  @param data
func (b *Batch) Write(index uint64, data []byte) {
	b.entry = batchEntry{index, len(data)}
	b.data = data
}

// WriteBatch writes the entries in the batch to the log in the order that they
// were added to the batch. The batch is cleared upon a successful return.
//  @Description:
//  @receiver l
//  @param b
//  @return *storePb.StoreInfo
//  @return error
func (l *BlockFile) WriteBatch(b *Batch) (*storePb.StoreInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return nil, ErrCorrupt
	} else if l.closed {
		return nil, ErrClosed
	}
	if b.entry.size == 0 {
		return nil, nil
	}
	return l.writeBatch(b)
}

//  writeBatch 写一个batch 返回索引信息
//  @Description:
//  @receiver l
//  @param b
//  @return *storePb.StoreInfo
//  @return error
func (l *BlockFile) writeBatch(b *Batch) (*storePb.StoreInfo, error) {
	// check that indexes in batch are same
	if b.entry.index != l.lastIndex+uint64(1) {
		l.logger.Errorf(fmt.Sprintf("out of order, b.entry.index: %d and l.lastIndex+uint64(1): %d",
			b.entry.index, l.lastIndex+uint64(1)))
		if l.lastIndex == 0 {
			l.logger.Errorf("your block rfile db is damaged or not use this feature before, " +
				"please check your disable_block_file_db setting in chainmaker.yml")
		}
		return nil, ErrOutOfOrder
	}

	// load the tail segment
	s := l.lastSegment
	if len(s.ebuf) > l.opts.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return nil, err
		}
		s = l.lastSegment
	}

	var epos bpos
	s.ebuf, epos = l.appendBinaryEntry(s.ebuf, b.data)
	s.epos = append(s.epos, epos)

	startTime := time.Now()
	l.sfile.Lock()
	if _, err := l.sfile.wfile.WriteAt(s.ebuf[epos.pos:epos.end], int64(epos.pos)); err != nil {
		l.logger.Errorf("write rfile: %s in %d err: %v", s.path, s.index+uint64(len(s.epos)), err)
		return nil, err
	}
	l.lastIndex = b.entry.index
	l.sfile.Unlock()
	l.logger.Debugf("writeBatch block[%d] rfile.WriteAt time: %v", l.lastIndex, utils.ElapsedMillisSeconds(startTime))

	if !l.opts.NoSync {
		if err := l.sfile.wfile.Sync(); err != nil {
			return nil, err
		}
	}
	if epos.end-epos.pos != b.entry.size+epos.prefixLen {
		return nil, ErrBlockWrite
	}
	return &storePb.StoreInfo{
		FileName: l.lastSegment.name[:dbFileNameLen],
		Offset:   uint64(epos.pos + epos.prefixLen),
		ByteLen:  uint64(b.entry.size),
	}, nil
}

// LastIndex returns the index of the last entry in the log. Returns zero when
//  @Description:
// log has no entries.
//  @receiver l
//  @return index
//  @return err
func (l *BlockFile) LastIndex() (index uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return 0, ErrCorrupt
	} else if l.closed {
		return 0, ErrClosed
	}
	if l.lastIndex == 0 {
		return 0, nil
	}
	return l.lastIndex, nil
}

//  loadSegmentEntriesForRestarting loads ebuf and epos in the segment when restarting
//  @Description:
//  @receiver l
//  @param s
//  @return error
func (l *BlockFile) loadSegmentEntriesForRestarting(s *segment) error {
	data, err := ioutil.ReadFile(s.path)
	if err != nil {
		return err
	}

	var (
		epos []bpos
		pos  int
	)
	ebuf := data
	for exidx := s.index; len(data) > 0; exidx++ {
		var n, prefixLen int
		n, prefixLen, err = loadNextBinaryEntry(data)
		// if there are corrupted log entries, the corrupted and subsequent data are discarded
		if err != nil {
			break
		}
		data = data[n:]
		epos = append(epos, bpos{pos, pos + n, prefixLen})
		pos += n
		// load uncorrupted data
		s.ebuf = ebuf[0:pos]
		s.epos = epos
	}

	return nil
}

//  loadNextBinaryEntry 读取entry,返回
//  @Description:
//  @param data
//  @return n
//  @return prefixLen
//  @return err
func loadNextBinaryEntry(data []byte) (n, prefixLen int, err error) {
	// Customize part start
	// checksum + data_size + data
	// checksum read
	if len(data) < 4 {
		return 0, 0, ErrCorrupt
	}
	checksum := binary.LittleEndian.Uint32(data[:4])
	// binary read
	data = data[4:]
	// Customize part end
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, 0, ErrCorrupt
	}
	// Customize part start
	// verify checksum
	if checksum != NewCRC(data[n:uint64(n)+size]).Value() {
		return 0, 0, ErrCorrupt
	}
	prefixLen = 4 + n
	return prefixLen + int(size), prefixLen, nil
	// Customize part end
}

// ReadLastSegSection an entry from the log. Returns a byte slice containing the data entry.
//  @Description:
//  @receiver l
//  @param index
//  @return data
//  @return fileName
//  @return offset
//  @return byteLen
//  @return err
func (l *BlockFile) ReadLastSegSection(index uint64) (data []byte,
	fileName string, offset uint64, byteLen uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return nil, "", 0, 0, ErrCorrupt
	} else if l.closed {
		return nil, "", 0, 0, ErrClosed
	}

	s := l.lastSegment
	if index == 0 || index < s.index || index > l.lastIndex {
		return nil, "", 0, 0, ErrNotFound
	}
	epos := s.epos[index-s.index]
	edata := s.ebuf[epos.pos:epos.end]

	// Customize part start
	// checksum read
	checksum := binary.LittleEndian.Uint32(edata[:4])
	// binary read
	edata = edata[4:]
	// Customize part end
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, "", 0, 0, ErrCorrupt
	}
	if uint64(len(edata)-n) < size {
		return nil, "", 0, 0, ErrCorrupt
	}
	// Customize part start
	if checksum != NewCRC(edata[n:]).Value() {
		return nil, "", 0, 0, ErrCorrupt
	}
	// Customize part end
	if l.opts.NoCopy {
		data = edata[n : uint64(n)+size]
	} else {
		data = make([]byte, size)
		copy(data, edata[n:])
	}
	fileName = l.lastSegment.name[:dbFileNameLen]
	offset = uint64(epos.pos + epos.prefixLen)
	byteLen = uint64(len(data))
	return data, fileName, offset, byteLen, nil
}

// doReadFileSection an entry from the log. Returns a byte slice containing the data entry.
// @Description:
// @receiver l
// @param fiIndex
// @return []byte
// @return error
func (l *BlockFile) doReadFileSection(fiIndex *storePb.StoreInfo) ([]byte, error) {
	if fiIndex == nil || len(fiIndex.FileName) != dbFileNameLen || fiIndex.ByteLen == 0 {
		l.logger.Warnf("invalidate file index: %s", FileIndexToString(fiIndex))
		return nil, ErrInvalidateIndex
	}

	path := l.segmentPath(fiIndex.FileName, l.path)
	if fiIndex.FileName == l.lastSegment.name[:dbFileNameLen] {
		path = l.segmentPathWithENDSuffix(fiIndex.FileName)
	}

	l.logger.Debugf("read file section, path: %s, offset: %d, dataLen: %d", path, fiIndex.Offset, fiIndex.ByteLen)
	lfile, err := l.openReadFileWithTimeOut(path, time.Second)
	if err != nil {
		l.logger.Warnf("read file section, open path: %s failed: %d", path, err)
		return nil, err
	}

	data := make([]byte, fiIndex.ByteLen)
	lfile.RLock()
	n, err1 := lfile.rfile.ReadAt(data, int64(fiIndex.Offset))
	lfile.RUnlock()
	if err1 != nil {
		l.logger.Warnf("read file section, read path: %s  data failed: %v", path, err1)
		return nil, err1
	}
	if uint64(n) != fiIndex.ByteLen {
		return nil, fmt.Errorf("read block file size invalidate, wanted: %d, actual: %d", fiIndex.ByteLen, n)
	}
	return data, nil
}

// ReadFileSection add next time
// @Description:
// @receiver l
// @param fiIndex
// @param timeOut
// @return []byte
// @return error
func (l *BlockFile) ReadFileSection(fiIndex *storePb.StoreInfo, timeOut time.Duration) ([]byte, error) {
	var (
		err  error
		data []byte
	)

	ch1 := make(chan int, 1)
	go func(ch chan<- int) {
		data, err = l.doReadFileSection(fiIndex)
		ch <- 1
	}(ch1)

	if timeOut == 0 {
		timeOut = l.readTimeOut
	}
	timer := time.NewTimer(timeOut)

	select {
	case <-ch1:
		return data, err
	case <-timer.C:
		l.logger.Warnf("read file: %s, time out for %v", fiIndex.FileName, timeOut)
		err = fmt.Errorf("time out")
	}

	return nil, err
}

// ClearCache clears the segment cache
//  @Description:
//  @receiver l
//  @return error
func (l *BlockFile) ClearCache() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	l.clearCache()
	return nil
}

//  clearCache 清除cache
//  @Description:
//  @receiver l
func (l *BlockFile) clearCache() {
	l.openedFileCache.Range(func(_, v interface{}) bool {
		// nolint
		if v == nil {
			return true
		}
		if s, ok := v.(*lockableFile); ok {
			s.Lock()
			_ = s.rfile.Close()
			s.Unlock()
		}
		return true
	})
	l.openedFileCache = tinylru.LRU{}
	l.openedFileCache.Resize(l.opts.SegmentCacheSize)
}

// Sync performs a fsync on the log. This is not necessary when the
// NoSync option is set to false.
// @Description:
// @receiver l
// @return error
func (l *BlockFile) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	return l.sfile.wfile.Sync()
}

// TruncateFront 清除数据
//  @Description:
//  @receiver l
//  @param index
//  @return error
func (l *BlockFile) TruncateFront(index uint64) error {
	return nil
}

// useTmpPath tmpPath flag
// @Description:
// @receiver l
// @return bool
func (l *BlockFile) useTmpPath() bool {
	return len(l.tmpPath) > 0
}

// moveFDBFile move fdb file from tmpPath to origin bfdb path.
// this func will scan tmpPath directory then trigger move action and wait 10 second
// @Description:
// @receiver l
func (l *BlockFile) moveFDBFile() {
	var err error
	interval := time.Second * 10

	for {
		if err = l.scanTmpPath(); err != nil {
			l.logger.Warnf("move fdb file failed, err: %v", err)
		}
		time.Sleep(interval)
	}
}

// scanTmpPath scan tmpPath directory then move .fdb file
// @Description:
// @receiver l
// @return error
func (l *BlockFile) scanTmpPath() error {
	if !l.useTmpPath() {
		return nil
	}

	fis, err := ioutil.ReadDir(l.tmpPath)
	if err != nil {
		return err
	}
	// during the restart, wal files are loaded to log.segments
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		// mv .fdb file from tmpPath to path
		name := fi.Name()
		if l.useTmpPath() && strings.HasSuffix(name, dbFileSuffix) {
			if err1 := l.copyThenRemoveFile(
				filepath.Join(l.tmpPath, name),
				filepath.Join(l.path, name)); err1 != nil {
				return err1
			}
		}
	}

	return nil
}

// FileIndexToString convert
//  @Description:
//  @param fiIndex
//  @return string
func FileIndexToString(fiIndex *storePb.StoreInfo) string {
	if fiIndex == nil {
		return "rfile index is nil"
	}

	return fmt.Sprintf("fileIndex: fileName: %s, offset: %d, byteLen: %d",
		fiIndex.GetFileName(), fiIndex.GetOffset(), fiIndex.GetByteLen())
}

// copyThenRemoveFile  copy src file to dst file.
//	these two path may be located in different disk, so can not use 'mv' directly to prevent network or power issue
// @Description:
// @param src
// @param dst
// @return error
func (l *BlockFile) copyThenRemoveFile(src, dst string) error {
	var (
		err     error
		srcf    *os.File
		srcStat os.FileInfo
	)

	fileV, isOK := l.openedFileCache.Get(src)
	if isOK && fileV != nil {
		if isExist, _ := utils.PathExists(src); isExist {
			ofil, ok := fileV.(*lockableFile)
			if ok {
				if ofil.wfile != nil {
					_ = ofil.wfile.Close()
				}
				if ofil.rfile != nil {
					_ = ofil.rfile.Close()
				}
				ofil.rfile, err = os.Open(src)
				if err != nil {
					return err
				}
				srcf = ofil.rfile
			}
		}
	}

	if srcf == nil {
		srcStat, err = os.Stat(src)
		if err != nil {
			return err
		}
		_, err = os.Stat(filepath.Join(dst, "../"))
		if err != nil {
			return err
		}

		srcf, err = os.Open(src)
		if err != nil {
			return err
		}
	}
	defer srcf.Close()

	dstf, err2 := os.Create(dst)
	if err2 != nil {
		return err
	}
	defer dstf.Close()

	// copy file
	var written int64
	for count := 1; count <= 3; count++ {
		if written, err = io.Copy(dstf, srcf); err != nil {
			if count < 3 {
				err = nil
			}
			// we can add wait action if you need
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	if err != nil {
		return err
	}

	if srcStat != nil && written != srcStat.Size() {
		return fmt.Errorf("copy file %s[len: %d] to file: %s[len: %d] uncompleted",
			src, srcStat.Size(), dst, written)
	}

	l.openedFileCache.Delete(src)
	//l.pushCache(dst, &lockableFile{file: dstf})
	// remove src file
	return os.Remove(src)
}
