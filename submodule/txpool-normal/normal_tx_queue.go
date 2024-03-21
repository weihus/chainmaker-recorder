/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"fmt"
	"sync"
	"sync/atomic"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
)

// txQueue is actual tx pool contains config queue and common queues
type txQueue struct {
	configQueue         *txList // config tx queue, only one
	configTxCountAtomic *int32  // config tx num

	commonQueues        []*txList // common tx queues
	commonTxCountAtomic *int32    // common tx num

	chainConf protocol.ChainConf
	log       protocol.Logger
}

// newTxQueue create txQueue
func newTxQueue(queueNum int, chainConf protocol.ChainConf, blockStore protocol.BlockchainStore,
	log protocol.Logger) *txQueue {
	// init tx count
	configTxCountAtomic := int32(0)
	commonTxCountAtomic := int32(0)
	// create commonQueue
	commonQueue := make([]*txList, 0, queueNum)
	for i := 0; i < queueNum; i++ {
		commonQueue = append(commonQueue, newNormalTxList(&commonTxCountAtomic, chainConf, blockStore, log))
	}
	// create txQueue
	return &txQueue{
		configQueue:         newNormalTxList(&configTxCountAtomic, chainConf, blockStore, log),
		configTxCountAtomic: &configTxCountAtomic,
		commonQueues:        commonQueue,
		commonTxCountAtomic: &commonTxCountAtomic,
		chainConf:           chainConf,
		log:                 log,
	}
}

// addConfigTx add a config tx to config queue
func (queue *txQueue) addConfigTx(mtx *memTx) {
	defer func() {
		queue.log.Debugf("txPool status: %s", queue.status())
	}()
	// add config tx to confQueue
	confQueue := queue.configQueue
	confQueue.PutTx(mtx, confQueue.addTxValidate)
}

// addCommonTx add a common tx to common queue
func (queue *txQueue) addCommonTx(mtx *memTx, idx int) {
	// idx should be valid
	if idx < 0 || idx >= queue.commonQueuesNum() {
		return
	}
	defer func() {
		queue.log.Debugf("txPool status: %s", queue.status())
	}()
	// add common tx to a comQueue
	comQueue := queue.commonQueues[idx]
	comQueue.PutTx(mtx, comQueue.addTxValidate)
}

// addCommonTxs add common txs to common queue
func (queue *txQueue) addCommonTxs(mtxsTable [][]*memTx) {
	if len(mtxsTable) != queue.commonQueuesNum() {
		return
	}
	defer func() {
		queue.log.Debugf("txPool status: %s", queue.status())
	}()
	// add txs to different comQueue
	var wg sync.WaitGroup
	for i := 0; i < queue.commonQueuesNum(); i++ {
		if len(mtxsTable[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, txs []*memTx) {
			defer wg.Done()
			comQueue := queue.commonQueues[idx]
			comQueue.PutTxs(txs, comQueue.addTxValidate)
		}(i, mtxsTable[i])
	}
	wg.Wait()
}

// fetchTxBatch fetch a batch of config tx or common txs
func (queue *txQueue) fetchTxBatch() []*memTx {
	// 1.fetch config tx
	if configQueueSize := queue.configTxCount(); configQueueSize > 0 {
		confQueue := queue.configQueue
		mtxs, _ := confQueue.FetchTxs(1, confQueue.fetchTxValidate)
		queue.log.Debugw("txQueue fetch config tx", "txCount", 1, "configQueueSize",
			configQueueSize, "txs", len(mtxs))
		return mtxs
	}
	// 2.fetch common txs
	commonTxCount := queue.commonTxCount()
	if commonTxCount == 0 {
		return nil
	}
	var perQueueTxsNum int
	maxTxCount := MaxTxCount(queue.chainConf)
	if int(commonTxCount) <= maxTxCount {
		// may be the num of fetched txs is greater than MaxTxCount, but will retry
		perQueueTxsNum = int(commonTxCount)
	} else {
		// MaxTxCount is greater than CommonQueueNum, that means perQueueNum is always greater than 0
		perQueueTxsNum = maxTxCount / queue.commonQueuesNum()
	}
	// fetch txs from different common queues
	var wg sync.WaitGroup
	var idxAtomic int32 = -1
	mtxs := make([]*memTx, perQueueTxsNum*queue.commonQueuesNum())
	for i := 0; i < queue.commonQueuesNum(); i++ {
		wg.Add(1)
		go func(idx int, idxAtom *int32) {
			defer wg.Done()
			comQueue := queue.commonQueues[idx]
			if fetchedTxs, _ := comQueue.FetchTxs(perQueueTxsNum, comQueue.fetchTxValidate); len(fetchedTxs) != 0 {
				for _, mtx := range fetchedTxs {
					mtxs[atomic.AddInt32(idxAtom, 1)] = mtx
				}
			}
		}(i, &idxAtomic)
	}
	wg.Wait()
	// if there are enough txs, but get the txs less than block capacity
	// fetch from a queue again
	mtxs = mtxs[:idxAtomic+1]
	if int(commonTxCount) >= maxTxCount && len(mtxs) < maxTxCount {
		idx := queue.secondFetchedIdx()
		comQueue := queue.commonQueues[idx]
		wantNum := maxTxCount - len(mtxs)
		secondFetchedTxs, _ := comQueue.FetchTxs(wantNum, comQueue.fetchTxValidate)
		mtxs = append(mtxs, secondFetchedTxs...)
		queue.log.Debugw("second fetch", "queue index", idx,
			"want", wantNum, "txs", len(secondFetchedTxs))
	}
	queue.log.Debugw("txQueue fetch common txs", "commonQueueSize", commonTxCount, "txs", len(mtxs))
	return mtxs
}

// getTxByTxId get tx by txId
func (queue *txQueue) getTxByTxId(txId string) (mtx *memTx, err error) {
	// config tx
	if mtx, err = queue.configQueue.GetTxByTxId(txId); err == nil {
		return
	}
	// common tx
	for _, comQueue := range queue.commonQueues {
		if mtx, err = comQueue.GetTxByTxId(txId); err == nil {
			return
		}
	}
	return nil, err
}

// getCommonTxsByTxIds get common txs by txIds
func (queue *txQueue) getCommonTxsByTxIds(txIdsTable [][]string) (
	mtxsRet map[string]*memTx, txsMis map[string]struct{}) {
	if len(txIdsTable) != queue.commonQueuesNum() {
		return
	}
	// get common txs
	// result tale
	mtxsRetTable := make([]map[string]*memTx, queue.commonQueuesNum())
	txsMisTable := make([]map[string]struct{}, queue.commonQueuesNum())
	// get txs concurrently
	var wg sync.WaitGroup
	for i := 0; i < queue.commonQueuesNum(); i++ {
		if len(txIdsTable[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, txIds []string) {
			defer wg.Done()
			// get result to table
			comQueue := queue.commonQueues[idx]
			mtxsRetTable[idx], txsMisTable[idx] = comQueue.GetTxsByTxIds(txIds)
		}(i, txIdsTable[i])
	}
	wg.Wait()
	// merge result table to final result
	mtxsRet = make(map[string]*memTx, MaxTxCount(queue.chainConf))
	txsMis = make(map[string]struct{}, MaxTxCount(queue.chainConf))
	for i := 0; i < len(mtxsRetTable); i++ {
		for k, v := range mtxsRetTable[i] {
			mtxsRet[k] = v
		}
		for k := range txsMisTable[i] {
			txsMis[k] = struct{}{}
		}
	}
	return mtxsRet, txsMis
}

// addConfigTxToPendingCache add config tx to pendingCache
func (queue *txQueue) addConfigTxToPendingCache(mtx *memTx) {
	queue.configQueue.AddTxsToPending([]*memTx{mtx})
}

// addCommonTxsToPendingCache add common tx to pendingCache
func (queue *txQueue) addCommonTxsToPendingCache(mtxsTable [][]*memTx) {
	if len(mtxsTable) != queue.commonQueuesNum() {
		return
	}
	// add common txs to different queues concurrently
	var wg sync.WaitGroup
	for i := 0; i < queue.commonQueuesNum(); i++ {
		if len(mtxsTable[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, txs []*memTx) {
			defer wg.Done()
			comQueue := queue.commonQueues[idx]
			comQueue.AddTxsToPending(txs)
		}(i, mtxsTable[i])
	}
	wg.Wait()
}

// retryConfigTxs re-add config tx to config queue
func (queue *txQueue) retryConfigTxs(mtxs []*memTx) {
	// re-add config txs
	confQueue := queue.configQueue
	confQueue.PutTxs(mtxs, confQueue.retryTxValidate)
}

// retryCommonTxs re-add common tx to common queue
func (queue *txQueue) retryCommonTxs(mtxsTable [][]*memTx) {
	if len(mtxsTable) != queue.commonQueuesNum() {
		return
	}
	// re-add common txs to different queues concurrently
	// will verify whether tx exist in queue
	var wg sync.WaitGroup
	for i := 0; i < queue.commonQueuesNum(); i++ {
		if len(mtxsTable[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, txs []*memTx) {
			defer wg.Done()
			comQueue := queue.commonQueues[idx]
			comQueue.PutTxs(txs, comQueue.retryTxValidate)
		}(i, mtxsTable[i])
	}
	wg.Wait()
}

// removeConfigTxs remove config txs from config queue or pendingCache
func (queue *txQueue) removeConfigTxs(txIds []string, onlyInPending bool) {
	// only remove config txs from pendingCache
	if onlyInPending {
		queue.configQueue.RemoveTxsInPending(txIds)
		// remove config txs from pendingCache and queue
	} else {
		queue.configQueue.RemoveTxs(txIds)
	}
}

// removeCommonTxs remove common txs from common queue or pendingCache
func (queue *txQueue) removeCommonTxs(txIdsTable [][]string, onlyInPending bool) {
	if len(txIdsTable) != queue.commonQueuesNum() {
		return
	}
	// remove common txs from different queues concurrently
	var wg sync.WaitGroup
	for i := 0; i < queue.commonQueuesNum(); i++ {
		if len(txIdsTable) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, txIds []string) {
			defer wg.Done()
			// only remove txs from pendingCache
			if onlyInPending {
				queue.commonQueues[idx].RemoveTxsInPending(txIds)
				// remove txs from pendingCache and queue
			} else {
				queue.commonQueues[idx].RemoveTxs(txIds)
			}
		}(i, txIdsTable[i])
	}
	wg.Wait()
}

// configTxExists verify whether config tx exist in config queue
func (queue *txQueue) configTxExists(txId string) bool {
	return queue.configQueue.HasTx(txId)
}

// commonTxExists verify whether common tx exist in common queue
func (queue *txQueue) commonTxExists(txId string, idx int) bool {
	if idx < 0 || idx >= queue.commonQueuesNum() {
		return false
	}
	return queue.commonQueues[idx].HasTx(txId)
}

// getPoolStatus get tx pool status
func (queue *txQueue) getPoolStatus() (txPoolStatus *txpoolPb.TxPoolStatus) {
	// new value
	var configTxInQueue, configTxInPending, commonTxInQueue, commonTxInPending int
	// get config tx count
	configTxInQueue, configTxInPending = queue.configQueue.Size()
	// get common tx count
	for _, comQueue := range queue.commonQueues {
		qn, pn := comQueue.Size()
		commonTxInQueue += qn
		commonTxInPending += pn
	}
	// return result
	return &txpoolPb.TxPoolStatus{
		ConfigTxPoolSize:     int32(MaxConfigTxPoolSize()),
		CommonTxPoolSize:     int32(MaxCommonTxPoolSize()),
		ConfigTxNumInQueue:   int32(configTxInQueue),
		ConfigTxNumInPending: int32(configTxInPending),
		CommonTxNumInQueue:   int32(commonTxInQueue),
		CommonTxNumInPending: int32(commonTxInPending),
	}
}

// getPoolStatus get config or common txs in queue or pending
func (queue *txQueue) getTxsByTxTypeAndStage(txType, txStage int32) (txs []*commonPb.Transaction, txIds []string) {
	// get config txs
	if txType == int32(txpoolPb.TxType_CONFIG_TX) {
		txs, txIds = queue.getConfigTxsByTxStage(txStage)
		return
		// get common txs
	} else if txType == int32(txpoolPb.TxType_COMMON_TX) {
		txs, txIds = queue.getCommonTxsByTxStage(txStage)
		return
		// get config and common txs
	} else if txType == int32(txpoolPb.TxType_ALL_TYPE) {
		confTxsQue, confTxIdsQue := queue.getConfigTxsByTxStage(txStage)
		txs = append(txs, confTxsQue...)
		txIds = append(txIds, confTxIdsQue...)
		commTxsPen, commTxIds := queue.getCommonTxsByTxStage(txStage)
		txs = append(txs, commTxsPen...)
		txIds = append(txIds, commTxIds...)
		return
	}
	return
}

// getConfigTxsByTxStage get config txs in queue or pending
func (queue *txQueue) getConfigTxsByTxStage(txStage int32) (txs []*commonPb.Transaction, txIds []string) {
	// get config txs in queue
	if txStage == int32(txpoolPb.TxStage_IN_QUEUE) {
		txs, txIds = queue.configQueue.GetTxsByStage(true, false)
		return
		// get config txs in pendingCache
	} else if txStage == int32(txpoolPb.TxStage_IN_PENDING) {
		txs, txIds = queue.configQueue.GetTxsByStage(false, true)
		return
		// get config txs in queue and pendingCache
	} else if txStage == int32(txpoolPb.TxStage_ALL_STAGE) {
		txs, txIds = queue.configQueue.GetTxsByStage(true, true)
		return
	}
	return
}

// getCommonTxsByTxStage get common txs in queue or pending
func (queue *txQueue) getCommonTxsByTxStage(txStage int32) (txs []*commonPb.Transaction, txIds []string) {
	// get common txs in queue
	if txStage == int32(txpoolPb.TxStage_IN_QUEUE) {
		for _, comQueue := range queue.commonQueues {
			ts, ids := comQueue.GetTxsByStage(true, false)
			txs = append(txs, ts...)
			txIds = append(txIds, ids...)
		}
		return
		// get common txs in pendingCache
	} else if txStage == int32(txpoolPb.TxStage_IN_PENDING) {
		for _, comQueue := range queue.commonQueues {
			ts, ids := comQueue.GetTxsByStage(false, true)
			txs = append(txs, ts...)
			txIds = append(txIds, ids...)
		}
		return
		// get common txs in queue and pendingCache
	} else if txStage == int32(txpoolPb.TxStage_ALL_STAGE) {
		for _, comQueue := range queue.commonQueues {
			ts, ids := comQueue.GetTxsByStage(true, true)
			txs = append(txs, ts...)
			txIds = append(txIds, ids...)
		}
		return
	}
	return
}

// commonQueuesNum return common queue num
func (queue *txQueue) commonQueuesNum() int {
	return len(queue.commonQueues)
}

// configTxCount return config tx queue size
func (queue *txQueue) configTxCount() int32 {
	return atomic.LoadInt32(queue.configTxCountAtomic)
}

// commonTxCount return common tx queue size
func (queue *txQueue) commonTxCount() int32 {
	return atomic.LoadInt32(queue.commonTxCountAtomic)
}

// isFull verify whether config or common queue is full
func (queue *txQueue) isFull(tx *commonPb.Transaction) bool {
	// config tx
	if isConfigTx(tx, queue.chainConf) {
		configTxSize := atomic.LoadInt32(queue.configTxCountAtomic)
		if configTxSize >= int32(MaxConfigTxPoolSize()) {
			queue.log.Warnf("AddTx configTxPool is full, txId:%s, configQueueSize:%d", tx.Payload.GetTxId(),
				configTxSize)
			return true
		}
		return false
	}
	// common tx
	commonTxSize := atomic.LoadInt32(queue.commonTxCountAtomic)
	if commonTxSize >= int32(MaxCommonTxPoolSize()) {
		queue.log.Warnf("AddTx commonTxPool is full, txId:%s, commonQueueSize:%d", tx.Payload.GetTxId(),
			commonTxSize)
		return true
	}
	return false
}

// secondFetchedIdx return the second fetched index of common queue than the num of txs is max
func (queue *txQueue) secondFetchedIdx() int {
	var (
		maxSize = 0
		maxIdx  = 0
	)
	for idx, comQue := range queue.commonQueues {
		if txSize := comQue.QueueSize(); txSize > maxSize {
			maxIdx = idx
		}
	}
	return maxIdx
}

// status print tx pool status containing the number of config txs and common txs in queue
func (queue *txQueue) status() string {
	return fmt.Sprintf("common txs queue size:%d, config txs queue size:%d",
		queue.commonTxCount(), queue.configTxCount())
}
