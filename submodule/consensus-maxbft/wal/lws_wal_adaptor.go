/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package wal

import (
	"fmt"
	"sync"

	"chainmaker.org/chainmaker/protocol/v2"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/pacemaker"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/vote"

	"chainmaker.org/chainmaker/lws"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"

	"github.com/gogo/protobuf/proto"
)

var _ WalAdaptor = (*LwsWalAdaptor)(nil)

// EntryTypeWalEntry is the entry type in lws
const EntryTypeWalEntry = int8(1)

// ErrWrongFormattingData is an error about wrong formatting data
var ErrWrongFormattingData = fmt.Errorf("wrong formatting data")

// LwsWalAdaptor is an implementation of interface WalAdaptor
type LwsWalAdaptor struct {
	// mutex to protect heightStatus, lastUnCommittedWalIndex, lastIndex
	mtx sync.Mutex

	// belong to witch epoch
	epochId uint64

	// map[block height][lws index]
	heightStatus map[uint64]uint64

	// first index of the first unCommitted block messages
	lastUnCommittedWalIndex uint64

	// current last lws index
	lastIndex uint64

	// log write system
	lws *lws.Lws

	voter         vote.Voter
	fork          forest.Forester
	pacemaker     pacemaker.PaceMaker
	voteCollector vote.VotesCollector
	verifier      protocol.BlockVerifier

	log protocol.Logger
}

// WalEntryCoder used to encode and decode data in lws
type WalEntryCoder struct {
}

// Type return entry type wal entry
func (c *WalEntryCoder) Type() int8 {
	return EntryTypeWalEntry
}

// Encode serialize wal entry to []byte
func (c *WalEntryCoder) Encode(s interface{}) ([]byte, error) {
	return proto.Marshal(s.(*maxbftpb.WalEntry))
}

// Decode deserialize []byte to wal entry
func (c *WalEntryCoder) Decode(data []byte) (interface{}, error) {
	entry := new(maxbftpb.WalEntry)
	err := proto.Unmarshal(data, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// NewLwsWalAdaptor initial a wal object
func NewLwsWalAdaptor(path string, epochId uint64, voter vote.Voter,
	forester forest.Forester, maker pacemaker.PaceMaker, collector vote.VotesCollector,
	verifier protocol.BlockVerifier, log protocol.Logger) (WalAdaptor, error) {
	var err error
	l := &LwsWalAdaptor{
		epochId:       epochId,
		voter:         voter,
		fork:          forester,
		pacemaker:     maker,
		voteCollector: collector,
		verifier:      verifier,
		heightStatus:  make(map[uint64]uint64),
		log:           log,
	}
	l.lws, err = lws.Open(path, lws.WithWriteFlag(lws.WF_SYNCFLUSH, 0))
	if err != nil {
		err = fmt.Errorf("open lws file failed. error:%+v", err)
		return nil, err
	}

	// register WalEntryCoder to the log write system
	if err = l.lws.RegisterCoder(&WalEntryCoder{}); err != nil {
		return nil, err
	}

	return l, nil
}

// GetHeightStatus return current height status of the wal
func (l *LwsWalAdaptor) GetHeightStatus() map[uint64]uint64 {
	return l.heightStatus
}

// SaveWalEntry save an entry to the wal
func (l *LwsWalAdaptor) SaveWalEntry(msgType maxbftpb.MessageType, msg []byte) (uint64, error) {
	var (
		err   error
		index uint64
	)

	l.mtx.Lock()
	defer l.mtx.Unlock()

	// construct wal entry
	entry := &maxbftpb.WalEntry{
		MsgType:           msgType,
		Msg:               msg,
		LastSnapshotIndex: l.lastUnCommittedWalIndex,
	}

	// write the wal entry to log write system
	if index, err = l.lws.WriteRetIndex(EntryTypeWalEntry, entry); err != nil {
		return 0, err
	}

	// update lastIndex
	l.lastIndex = index

	return index, nil
}

// ReplayWal replay wal file after the node was restarted
func (l *LwsWalAdaptor) ReplayWal() (bool, error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	// initial an iterator for wal file and skip to the end of the file
	reader := l.lws.NewLogIterator()
	defer reader.Release()

	reader.SkipToLast()

	// this is an empty wal file
	if !reader.HasPre() {
		l.log.Debugf("null wal content to replay")
		return false, nil
	}

	var (
		entry   *maxbftpb.WalEntry
		ok      bool
		updated bool
	)
	l.log.Infof("start replay wal")
	// get the last element in log write system
	element := reader.Previous()
	obj, err := element.GetObj()
	if err != nil {
		return false, err
	}

	if entry, ok = obj.(*maxbftpb.WalEntry); !ok {
		return false, ErrWrongFormattingData
	}

	// load the last index of log write system
	l.lastIndex = element.Index()

	// calculate the entry count to replay
	l.lastUnCommittedWalIndex = 1
	if entry.LastSnapshotIndex > 0 {
		l.lastUnCommittedWalIndex = entry.LastSnapshotIndex
	}
	entryCount := l.lastIndex - l.lastUnCommittedWalIndex + 1
	l.log.Infof(" replayWal, epochId: %d, start index: %d, lastWalIndex: %d,"+
		" entryCount: %d", l.epochId, entry.LastSnapshotIndex, l.lastIndex, entryCount)
	// wal file may have been tampered
	if entryCount <= 0 {
		return false, fmt.Errorf("invalid wal file")
	}

	// skip to the first element to replay
	reader.PreviousN(int(entryCount))

	// process objects base on message type of each of them
	for reader.HasNext() {
		if obj, err = reader.Next().GetObj(); err != nil {
			return false, err
		}
		objUpdated, err := l.processObj(obj)
		if err != nil {
			return updated, err
		}
		if objUpdated && !updated {
			updated = true
		}
	}
	return updated, nil
}

// UpdateWalIndexAndTrunc update wal index and truncate the wal file
func (l *LwsWalAdaptor) UpdateWalIndexAndTrunc(committedHeight uint64) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	for height := range l.heightStatus {
		if height <= committedHeight {
			delete(l.heightStatus, height)
		}
	}
	var ok bool
	if l.lastUnCommittedWalIndex, ok = l.heightStatus[committedHeight+1]; ok {
		// calculate the entry count to retain
		keepCount := l.lastIndex - l.lastUnCommittedWalIndex + 1

		// purge the file, keep only the last keepCount entries
		// note: due to the nature of the LWS system, the latest file will not be purge
		l.log.Debugf("updateWalIndexAndTrunc keepCount:%d,"+
			" lastUnCommittedWalIndex: %d", keepCount, l.lastUnCommittedWalIndex)
		if err := l.lws.Purge(lws.PurgeWithSoftEntries(int(keepCount)), lws.PurgeWithAsync()); err != nil {
			l.log.Warnf("updateWalIndexAndTrunc failed. error:%+v", err)
		}
	}
}

// AddProposalWalIndex add proposal wal index to wal
func (l *LwsWalAdaptor) AddProposalWalIndex(proposalHeight, index uint64) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	l.heightStatus[proposalHeight] = index
}

// AddNewView add new view information to wal
func (l *LwsWalAdaptor) AddNewView() error {
	view := l.pacemaker.CurView()
	newView := &maxbftpb.ViewData{
		View:    view,
		EpochId: l.epochId,
	}
	msg, err := proto.Marshal(newView)
	if err != nil {
		l.log.Warnf("handleTimeout failed. marshal newView data error:%+v", err)
		return err
	}
	if _, err := l.SaveWalEntry(maxbftpb.MessageType_NEW_VIEW_MESSAGE, msg); err != nil {
		l.log.Warnf("handleTimeout failed. save wal entry error:%+v", err)
		return err
	}
	l.log.Debugf("AddNewView save new view information to wal:[%d]", view)
	return nil
}

func (l *LwsWalAdaptor) processObj(obj interface{}) (bool, error) {
	var (
		entry *maxbftpb.WalEntry
		err   error
		ok    bool
	)
	if entry, ok = obj.(*maxbftpb.WalEntry); !ok {
		return false, ErrWrongFormattingData
	}

	switch entry.MsgType {

	// process proposal msg
	case maxbftpb.MessageType_PROPOSAL_MESSAGE:
		proposal := new(maxbftpb.ProposalData)
		err = proto.Unmarshal(entry.Msg, proposal)
		if err != nil {
			l.log.Errorf("replayWal unmarshal proposal failed. error:%+v", err)
			return false, err
		}

		if proposal == nil || proposal.Block == nil {
			l.log.Debugf("replayWal: got a nil proposal or the block is nil")
			return false, nil
		}

		if proposal.EpochId != l.epochId {
			l.log.Debugf("replayWal: got a proposal not in current epoch")
			return false, nil
		}

		header := proposal.Block.Header
		l.log.DebugDynamic(func() string {
			return fmt.Sprintf("replayWal proposal [%d:%d:%x]",
				header.BlockHeight, proposal.View, header.BlockHash)
		})
		if l.fork.GetFinalQC().Height() >= header.BlockHeight {
			l.log.DebugDynamic(func() string {
				return fmt.Sprintf("replayWal the %d block has been committed", header.BlockHeight)
			})
			return false, nil
		}

		// 1. update consensus state using qc in the proposal
		// 2. add the proposal to forest
		_, err = l.fork.UpdateStatesByProposal(proposal, true)
		if err != nil {
			l.log.Warnf("replayWal update states by qc in the proposal from wal failed, reason: %s", err)
			return false, nil
		}
		if err = l.fork.AddProposal(proposal, true); err != nil {
			l.log.Warnf("replayWal UpdateStatesByProposal[%d:%d:%x] failed: %+v",
				header.BlockHeight, proposal.View, header.BlockHash, err)
			return true, nil
		}
		if err = l.verifier.VerifyBlockWithRwSets(proposal.Block, proposal.TxRwSet,
			protocol.SYNC_VERIFY); err != nil {
			l.log.Warnf("replayWal VerifyBlock[%d:%d:%x]  failed:%+v",
				header.BlockHeight, proposal.View, header.BlockHash, err)
		}
		return true, nil
	// process vote msg
	case maxbftpb.MessageType_VOTE_MESSAGE:
		voteInfo := new(maxbftpb.VoteData)
		err = proto.Unmarshal(entry.Msg, voteInfo)
		if err != nil {
			l.log.Warnf("replayWal unmarshal vote failed. error:%+v", err)
			return false, err
		}
		l.log.DebugDynamic(func() string {
			return fmt.Sprintf("replayWal vote [%d:%d:%x]", voteInfo.Height, voteInfo.View, voteInfo.BlockId)
		})
		if l.epochId != voteInfo.EpochId {
			l.log.Debugf("replayWal: got a vote not in current epoch")
			return false, nil
		}
		if l.fork.GetFinalQC().Height() >= voteInfo.Height {
			l.log.DebugDynamic(func() string {
				return fmt.Sprintf("replayWal the %d vote was too old or  ", voteInfo.Height)
			})
			return false, nil
		}
		// put the vote into VoteCollector
		if err = l.voteCollector.AddVote(voteInfo); err != nil {
			l.log.Warnf("replayWal AddVote failed, reason: %s", err)
			return false, nil
		}
		return true, nil
	// process new view message
	case maxbftpb.MessageType_NEW_VIEW_MESSAGE:
		newView := new(maxbftpb.ViewData)
		err = proto.Unmarshal(entry.Msg, newView)
		if err != nil {
			l.log.Errorf("replayWal unmarshal newNewMessage failed. error:%+v", err)
			return false, err
		}

		l.log.DebugDynamic(func() string {
			return fmt.Sprintf("replayWal view message %d", newView.View)
		})
		if l.epochId != newView.EpochId {
			l.log.Debugf("replayWal: got a viewChange not in current epoch")
			return false, nil
		}
		if l.pacemaker.CurView() >= newView.View {
			l.log.DebugDynamic(func() string {
				return fmt.Sprintf("the view %d has been passed", newView.View)
			})
			return false, nil
		}
		// update pacemaker with the new view data
		return l.pacemaker.UpdateWithView(newView.View), nil

	// got an unknown type message
	default:
		return false, ErrWrongFormattingData
	}
}

// Clear clear the wal when switch epoch
func (l *LwsWalAdaptor) Clear() error {
	if l.lastIndex != 0 {
		l.lastUnCommittedWalIndex = l.lastIndex + 1
	} else {
		// initial an iterator for wal file and skip to the end of the file
		reader := l.lws.NewLogIterator()
		defer reader.Release()

		reader.SkipToLast()

		// this is an empty wal file
		if !reader.HasPre() {
			l.log.Debugf("null wal content to replay")
			return nil
		}

		// get the last element in log write system
		element := reader.Previous()

		// ensure that the wal starts with this new newView entry now
		l.lastUnCommittedWalIndex = element.Index() + 1
	}

	// add a new view to record the last index to the wal
	if err := l.AddNewView(); err != nil {
		return err
	}

	// clear heightStatus
	l.heightStatus = make(map[uint64]uint64)

	// purge wal file
	if err := l.lws.Purge(lws.PurgeWithSoftEntries(1), lws.PurgeWithAsync()); err != nil {
		l.log.Warnf("updateWalIndexAndTrunc failed. error:%+v", err)
	}
	return nil
}

// Close the wal
func (l *LwsWalAdaptor) Close() {
	l.lws.Close()
}
