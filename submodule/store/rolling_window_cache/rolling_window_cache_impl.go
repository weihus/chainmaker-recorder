/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package rolling_window_cache

import (
	"errors"
	"sync"
	"time"

	bytesconv "chainmaker.org/chainmaker/common/v2/bytehelper"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/store/v2/cache"
	"chainmaker.org/chainmaker/store/v2/serialization"
	"chainmaker.org/chainmaker/store/v2/types"
	"github.com/gogo/protobuf/proto"
)

const (
	blockNumIdxKeyPrefix = 'n'
)

var (
	errGetBatchPool   = errors.New("get updatebatch error")
	errBlockInfoNil   = errors.New("blockInfo is nil")
	errBlockNil       = errors.New("block is nil")
	errBlockHeaderNil = errors.New("block header is nil")
	//errBlockTxsNil    = errors.New("block txs is nil")
)

/*
| 1                         10|               | 0  1  2                      10|
 —— —— —— —— —— —— —— —— —— ——                 —— —— —— —— —— —— —— —— —— —— ——
 Transaction Pool (cap 10 )                    Rolling Window Cache (cap 11  ) 1.1*tx_pool_cap

      ---->                                   ^ ---> rolling window a step     ^ --->

| 2                         11|                  | 1  2  3                      11|
 —— —— —— —— —— —— —— —— —— ——                    —— —— —— —— —— —— —— —— —— —— ——
 Transaction Pool (cap 10 )                       Rolling Window Cache (cap 11  ) 1.1*tx_pool_cap

         RollingWindowCache always covers TransactionPool
*/

// RollingWindowCacher RollingWindowCacher 中 cache 1.1倍交易池大小 的txid
// @Description:
// 保证 当前窗口，可以覆盖 交易池大小的txid
// 保证 交易在做范围查重时的命中率达到100%
type RollingWindowCacher struct {
	Cache            *cache.StoreCacheMgr
	txIdCount        uint64
	currCount        uint64
	startBlockHeight uint64
	endBlockHeight   uint64
	lastBlockHeight  uint64
	sync.RWMutex
	batchPool    sync.Pool
	logger       protocol.Logger
	blockSerChan chan *serialization.BlockWithSerializedInfo

	goodQueryClockTurntable ClockTurntable
	badQueryClockTurntable  ClockTurntable

	CurrCache protocol.StoreBatcher
}

// NewRollingWindowCacher 创建一个 滑动窗口 cache
// @Description:
// @param txIdCount
// @param currCount
// @param startBlockHeight
// @param endBlockHeight
// @param lastBlockHeight
// @param logger
// @return RollingWindowCache
func NewRollingWindowCacher(txIdCount, currCount, startBlockHeight, endBlockHeight, lastBlockHeight uint64,
	logger protocol.Logger) RollingWindowCache {
	r := &RollingWindowCacher{
		Cache:                   cache.NewStoreCacheMgr("", 10, logger),
		txIdCount:               txIdCount,
		currCount:               currCount,
		startBlockHeight:        startBlockHeight,
		endBlockHeight:          endBlockHeight,
		lastBlockHeight:         lastBlockHeight,
		blockSerChan:            make(chan *serialization.BlockWithSerializedInfo, 10),
		logger:                  logger,
		goodQueryClockTurntable: NewClockTurntableInstrument(),
		badQueryClockTurntable:  NewClockTurntableInstrument(),
		CurrCache:               types.NewUpdateBatch(),
	}
	r.batchPool.New = func() interface{} {
		return types.NewUpdateBatch()
	}

	//异步消费，更新cache
	go r.Consumer()

	//异步动态自适应调整窗口
	go r.scaling()
	return r

}

// InitGenesis commit genesis block
// @Description:
// @receiver r
// @param genesisBlock
// @return error
func (r *RollingWindowCacher) InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error {
	return r.ResetRWCache(genesisBlock)
}

// CommitBlock  commits the txId to chan
// @Description:
// @receiver r
// @param blockInfo
// @param isCache
// @return error
func (r *RollingWindowCacher) CommitBlock(blockInfo *serialization.BlockWithSerializedInfo, isCache bool) error {
	if isCache {
		if blockInfo == nil {
			r.logger.Errorf("blockInfo is nil,when commitblock")
			return errBlockInfoNil
		}
		if blockInfo.Block == nil {
			r.logger.Errorf("block is nil,when commitblock")
			return errBlockNil
		}
		if blockInfo.Block.Header == nil {
			r.logger.Errorf("block header is nil,when commitblock")
			return errBlockHeaderNil
		}

		r.blockSerChan <- blockInfo
		return nil
	}
	return nil
}

// ResetRWCache  use the last  block to reset RWCache ,when blockstore is restarting
// @Description:
// @receiver r
// @param blockInfo
// @return error
func (r *RollingWindowCacher) ResetRWCache(blockInfo *serialization.BlockWithSerializedInfo) error {
	r.Lock()
	defer r.Unlock()

	batch, ok := r.batchPool.Get().(*types.UpdateBatch)
	if !ok {
		r.logger.Errorf("chain[%s]: blockInfo[%d] get updatebatch error in rollingWindowCache",
			blockInfo.Block.Header.ChainId, blockInfo.Block.Header.BlockHeight)
		return errGetBatchPool
	}
	batch.ReSet()

	//batch := types.NewUpdateBatch()
	block := blockInfo.Block

	// 1. Concurrency batch Put
	//并发更新cache,ConcurrencyMap,提升并发更新能力
	//groups := len(blockInfo.SerializedContractEvents)
	wg := &sync.WaitGroup{}
	wg.Add(len(blockInfo.SerializedTxs))
	heightKey := constructBlockNumKey(block.Header.BlockHeight)

	for index, txBytes := range blockInfo.SerializedTxs {
		go func(index int, txBytes []byte, batch protocol.StoreBatcher, wg *sync.WaitGroup) {
			defer wg.Done()
			tx := blockInfo.Block.Txs[index]
			//txIdKey := constructTxIDKey(tx.Payload.TxId)
			txIdKey := bytesconv.StringToBytes(tx.Payload.TxId)
			batch.Put(txIdKey, heightKey)

			r.logger.Debugf("chain[%s]: blockInfo[%d] batch transaction index[%d] txid[%s]",
				block.Header.ChainId, block.Header.BlockHeight, index, tx.Payload.TxId)
		}(index, txBytes, batch, wg)
	}

	wg.Wait()
	r.startBlockHeight = blockInfo.Block.Header.BlockHeight
	r.endBlockHeight = r.startBlockHeight
	r.currCount = uint64(len(blockInfo.Txs))
	r.Cache.AddBlock(block.Header.BlockHeight, batch)
	r.lastBlockHeight = blockInfo.Block.Header.BlockHeight

	// 2. 写 currCache
	for k, v := range batch.KVs() {
		r.CurrCache.Put(bytesconv.StringToBytes(k), v)
	}

	return nil
}

// Has 判断 key 在 start >r.startBlockHeight条件下，在 [start,r.endBlockHeight]区间内 是否存在
// @Description:
// @receiver r
// @param key
// @param start
// @return bool start是否在滑动窗口内
// @return bool key是否存在
// @return error
func (r *RollingWindowCacher) Has(key string, start uint64) (bool, bool, error) {
	r.logger.Debugf("start get Rlock in has")
	r.RLock()
	startBlockHeight := r.startBlockHeight
	endBlockHeight := r.endBlockHeight
	r.RUnlock()
	r.logger.Debugf("end get Rlock in has")

	//txKey := bytesconv.BytesToString(constructTxIDKey(key))
	txKey := key
	//b := r.CurrCache.Has(bytesconv.StringToBytes(txKey))
	//return true, b, nil

	//fmt.Println("start ,r.startBlockHeight=",start," ",r.startBlockHeight)
	//if start < r.startBlockHeight || start > r.endBlockHeight {
	if start < startBlockHeight {
		//r.logger.Debugf("start badQueryClockTurntable")
		// update badQueryClockTurntable for computing query status and analyzing performance
		r.badQueryClockTurntable.Add(1)
		//r.logger.Debugf("end badQueryClockTurntable")
		r.logger.Debugf("out of range,start:[%d],startBlockHeight:[%d],"+
			"endBlockHeight:[%d]", start, startBlockHeight, endBlockHeight)
		return false, false, nil
	}

	//_, b := r.Cache.Get(txKey)
	//r.logger.Debugf("start HasFromHeight")
	//_, b := r.Cache.HasFromHeight(txKey, start, r.endBlockHeight)
	b := r.CurrCache.Has(bytesconv.StringToBytes(txKey))
	//r.logger.Debugf("end HasFromHeight")

	//update goodQueryClockTurntable for computing query status and analyzing performance
	// |<-----1, 2, 3, 4, 5, 6, 7, 8, 9, 10------->|
	// if start >= 3, then we think the window between 1 and 3 ( [1-3]) is not needed
	// so we update status
	//if (start-r.startBlockHeight)*10 <= (r.endBlockHeight-r.startBlockHeight)*3 {
	if (start-startBlockHeight)*10 <= (endBlockHeight-startBlockHeight)*3 {
		//r.logger.Debugf("start goodQueryClockTurntable")
		r.goodQueryClockTurntable.Add(1)
		//r.logger.Debugf("end goodQueryClockTurntable")
	}
	return true, b, nil

}

// Consumer  异步消费 管道数据，完成对滑动窗口缓存的更新
// @Description:
// @receiver r
func (r *RollingWindowCacher) Consumer() {
	defer func() {
		if err := recover(); err != nil {
			r.logger.Errorf("consumer has a error in rollingWindowCache, errinfo:[%s]", err)
		}
	}()

	for {
		blockInfo, chanOk := <-r.blockSerChan
		if !chanOk {
			r.logger.Infof("blockSerChan close in rollingWindowCache")
			return
		}
		if blockInfo == nil {
			r.logger.Errorf("blockInfo is nil")
			panic(errBlockNil)
		}
		if blockInfo.Block == nil {
			r.logger.Errorf("Block is nil")
			panic(errBlockInfoNil)
		}
		if blockInfo.Block.Header == nil {
			r.logger.Errorf("Block header is nil")
			panic(errBlockHeaderNil)
		}

		//}
		//for blockInfo := range r.blockSerChan {

		batch, ok := r.batchPool.Get().(*types.UpdateBatch)
		if !ok {
			r.logger.Errorf("chain[%s]: blockInfo[%d] get updatebatch error in rollingWindowCache",
				blockInfo.Block.Header.ChainId, blockInfo.Block.Header.BlockHeight)
		}
		batch.ReSet()

		//batch := types.NewUpdateBatch()
		block := blockInfo.Block

		r.RLock()
		lastBlockHeight := r.lastBlockHeight
		r.RUnlock()
		if block.Header.BlockHeight <= lastBlockHeight {
			continue
		}
		// 1. Concurrency batch Put
		//并发更新cache,ConcurrencyMap,提升并发更新能力
		//groups := len(blockInfo.SerializedContractEvents)
		wg := &sync.WaitGroup{}
		wg.Add(len(blockInfo.SerializedTxs))
		heightKey := constructBlockNumKey(block.Header.BlockHeight)

		for index, txBytes := range blockInfo.SerializedTxs {
			go func(index int, txBytes []byte, batch protocol.StoreBatcher, wg *sync.WaitGroup) {
				defer wg.Done()
				defer func() {
					if err := recover(); err != nil {
						r.logger.Errorf("SerializedTxs error in rollingWindowCache, errinfo:[%s]", err)
					}
				}()
				tx := blockInfo.Block.Txs[index]
				//txIdKey := constructTxIDKey(tx.Payload.TxId)
				txIdKey := bytesconv.StringToBytes(tx.Payload.TxId)
				batch.Put(txIdKey, heightKey)

				r.logger.Debugf("chain[%s]: blockInfo[%d] batch transaction index[%d] txid[%s]",
					block.Header.ChainId, block.Header.BlockHeight, index, tx.Payload.TxId)
			}(index, txBytes, batch, wg)
		}

		wg.Wait()

		// 2. 写 交易id到 当前 currCache
		wgBatch := &sync.WaitGroup{}
		wgBatch.Add(len(blockInfo.SerializedTxs))
		for k, v := range batch.KVs() {
			go func(k string, v []byte) {
				defer wgBatch.Done()
				r.CurrCache.Put(bytesconv.StringToBytes(k), v)
			}(k, v)
		}
		wgBatch.Wait()

		r.logger.Debugf("rolling window left border,start get Lock ")
		//r.Lock()
		r.logger.Debugf("rolling window left border,end get Lock ")

		// 3. 滑动一个窗口 右滑动一个单位
		// 写 当前区块的交易 到 map cache中
		r.Lock()
		r.Cache.AddBlock(block.Header.BlockHeight, batch)
		r.lastBlockHeight = block.Header.BlockHeight
		r.endBlockHeight = block.Header.BlockHeight
		//如果当前窗口没有元素，更新窗口左边界
		if r.currCount == 0 {
			r.startBlockHeight = block.Header.BlockHeight
		}
		// cache中当前总交易量增加
		r.currCount = r.currCount + uint64(len(blockInfo.Txs))
		r.Unlock()

		//r.Unlock()
		// 4. 滑动一个窗口 左滑动一个单位
		//如果cache中数据大于等于 缓存配置大小，则，删除最旧的块
		//if r.currCount > r.txIdCount {
		r.rollingLeft(blockInfo)
		//r.Unlock()

	}

}

// rollingLeft delete some tx from rwc if these txs are expired, rolling left boundary of window
// 滑动一个窗口 左滑动若干单位
func (r *RollingWindowCacher) rollingLeft(blockInfo *serialization.BlockWithSerializedInfo) {
	//如果cache中数据大于等于 缓存配置大小，则，删除最旧的块
	//if r.currCount > r.txIdCount {
	r.logger.Debugf("rolling leftHeight:[%d], "+
		"currCount:[%d], txIdCount:[%d]", r.startBlockHeight, r.currCount, r.txIdCount)

	var startTxIdCount int
	for r.currCount > r.txIdCount {
		r.logger.Debugf("rolling window left border,leftHeight:[%d], "+
			"currCount:[%d], txIdCount:[%d]", r.startBlockHeight, r.currCount, r.txIdCount)
		startBatch, err := r.Cache.GetBatch(r.startBlockHeight)
		if err != nil {
			r.logger.Infof("chain[%s]: blockInfo[%d] can not get GetBatch  in rollingWindowCache, info:[%s]",
				blockInfo.Block.Header.ChainId, blockInfo.Block.Header.BlockHeight, err)
			break
		}
		startTxIdCount = startBatch.Len()

		//清理 r.CurrCache 中元素
		wgDelete := &sync.WaitGroup{}
		wgDelete.Add(startBatch.Len())
		for k := range startBatch.KVs() {
			go func(k string) {
				r.Lock()
				defer wgDelete.Done()
				defer r.Unlock()
				r.CurrCache.Remove(bytesconv.StringToBytes(k))
			}(k)
		}
		wgDelete.Wait()

		//清理 map cache中 的过期 height 对应交易集
		r.Cache.DelBlock(r.startBlockHeight)
		r.Lock()
		r.startBlockHeight++
		r.currCount = r.currCount - uint64(startTxIdCount)
		r.Unlock()
		//放回对象池
		r.batchPool.Put(startBatch)
	}
}

// scaling  check whether is need to expand the capacity or narrow
// @Description:
// @receiver r
func (r *RollingWindowCacher) scaling() {
	expandSleepDuration := uint64(3)
	narrowSleepDuration := uint64(10)
	monitorSleepDuration := uint64(10)

	// expand
	go func() {
		// check per expandSleepDuration second
		//expandSleepDuration := uint64(3)
		r.expandRWC(expandSleepDuration)
	}()

	// narrow
	go func() {
		//check per narrowSleepDuration second
		//narrowSleepDuration := uint64(10)
		r.narrowRWC(expandSleepDuration, narrowSleepDuration)
	}()

	// monitor
	go func() {
		for {
			r.logger.Debugf("cache block's sum:[%d]", r.Cache.GetLength())
			time.Sleep(time.Duration(monitorSleepDuration) * time.Second)
		}
	}()

}

// isNeedExpand return true if you need to expand the window size
// @Description:
// @receiver r
func (r *RollingWindowCacher) isNeedExpand(expandSleepDuration uint64) bool {
	// there are some queries that are out of range,so we need to expand the size of window
	return r.badQueryClockTurntable.GetLastSecond(expandSleepDuration) > 0

}

// isNeedNarrow return true if you need to reduce the window size
// @Description:
// @receiver r
func (r *RollingWindowCacher) isNeedNarrow(expandSleepDuration, narrowSleepDuration uint64) bool {
	// no queries are out of boundary ,and no queries fall in the older range,
	// so we need to reduce the size of window
	return r.badQueryClockTurntable.GetLastSecond(expandSleepDuration) == 0 &&
		r.goodQueryClockTurntable.GetLastSecond(narrowSleepDuration) == 0
}

// expandRWC to expand the window size
// @Description:
// @receiver r
func (r *RollingWindowCacher) expandRWC(expandSleepDuration uint64) {
	// check per expandSleepDuration second
	//expandSleepDuration := uint64(3)
	for {
		r.RLock()
		txIdCount := r.txIdCount
		currCount := r.currCount
		r.RUnlock()
		if r.isNeedExpand(expandSleepDuration) {
			//old := r.txIdCount
			old := txIdCount
			// 30% increase
			//r.txIdCount = uint64(float64(r.txIdCount) * 1.3)
			txIdCount = uint64(float64(txIdCount) * 1.3)

			// or double
			//if r.txIdCount <= old {
			if txIdCount <= old {
				txIdCount = 2 * old
			}
			// can be expanded up to 9 times the current actual element
			if txIdCount < 10*currCount {
				r.Lock()
				r.txIdCount = txIdCount
				r.Unlock()
				r.logger.Debugf("expand rwc capacity,original:[%d],current:[%d],"+
					"startHeight:[%d],endHeight:[%d]", old, r.txIdCount, r.startBlockHeight, r.endBlockHeight)
			}

		}
		time.Sleep(time.Duration(expandSleepDuration) * time.Second)
	}

}

// narrowRWC to reduce the window size
// @Description:
// @receiver r
func (r *RollingWindowCacher) narrowRWC(expandSleepDuration, narrowSleepDuration uint64) {
	//check per narrowSleepDuration second
	//narrowSleepDuration := uint64(10)
	for {
		r.RLock()
		txIdCount := r.txIdCount
		r.RUnlock()
		// none of the requests fall within the entire interval near the left boundary
		// for example,range is [1,10],
		// [1,10] = [[1,3],[4,10]].
		// the [1,3] is older, the [4,10] is newer
		// no queries fall in the [1,3] ,so the [1,3] is wasteful,
		// we need to reduce the range, resize [1,10] to [3,10] (or [2,10])
		if r.isNeedNarrow(expandSleepDuration, narrowSleepDuration) {
			// 20% decrease
			old := txIdCount
			txIdCount = uint64(float64(txIdCount) - float64(txIdCount)*0.2)

			if txIdCount > old {
				txIdCount = old
			}

			// mini size is 1000000
			if txIdCount <= 1000000 {
				txIdCount = 1000000
			}

			r.Lock()
			r.txIdCount = txIdCount
			r.Unlock()

			r.logger.Debugf("narrow rwc capacity,original:[%d],current:[%d],"+
				"startHeight:[%d],endHeight:[%d]", old, r.txIdCount, r.startBlockHeight, r.endBlockHeight)
		}
		time.Sleep(time.Duration(narrowSleepDuration) * time.Second)
	}
}

//// constructTxIDKey 给交易添加交易前缀
//// @Description:
//// @param txId
//// @return []byte
//func constructTxIDKey(txId string) []byte {
//	return append([]byte{txIDIdxKeyPrefix}, bytesconv.StringToBytes(txId)...)
//}

// constructBlockNumKey 给区块高度添加一个头
// @Description:
// @param blockNum
// @return []byte
func constructBlockNumKey(blockNum uint64) []byte {
	blkNumBytes := encodeBlockNum(blockNum)
	return append([]byte{blockNumIdxKeyPrefix}, blkNumBytes...)
}

// encodeBlockNum 序列化 整型数据
// @Description:
// @param blockNum
// @return []byte
func encodeBlockNum(blockNum uint64) []byte {
	return proto.EncodeVarint(blockNum)
}
