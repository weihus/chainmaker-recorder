/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"sync"
	"sync/atomic"

	"chainmaker.org/chainmaker/common/v2/linkedhashmap"
	"chainmaker.org/chainmaker/common/v2/monitor"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// StageMod identify the location of the transaction
type StageMod int

const (
	// StageUnknown is unknown
	StageUnknown StageMod = iota
	// StageInQueue identify that the transaction is in a queue, is unpacked
	StageInQueue
	// StageInPending identify that the transaction is in a pendingCache, has been packed
	StageInPending
	// StageInQueueAndPending identify that the transaction is in a queue or pendingCache
	StageInQueueAndPending
)

// batchList structure of store batch in memory
type batchList struct {
	sync.RWMutex
	queue              *linkedhashmap.LinkedHashMap // key:batchId val:*memTxBatch
	txCountInQueAtomic *int32                       // the num of tx in the queue of this batchList
	txTotalInQueAtomic *int32                       // the num of tx in the queue of some batchLists

	pendingCache sync.Map // key:batchId val:*memTxBatch

	chainConf        protocol.ChainConf
	metricTxPoolSize *prometheus.GaugeVec
	log              protocol.Logger
}

// newBatchList create batchList
func newBatchList(txTotalAtomic *int32, chainConf protocol.ChainConf, log protocol.Logger) *batchList {
	// create batchList
	txCountInQue := int32(0)
	list := &batchList{
		queue:              linkedhashmap.NewLinkedHashMap(),
		txCountInQueAtomic: &txCountInQue,
		txTotalInQueAtomic: txTotalAtomic,
		pendingCache:       sync.Map{},
		chainConf:          chainConf,
		log:                log,
	}
	// if open monitor
	// then new GaugeVec Metric
	if MonitorEnabled {
		list.metricTxPoolSize = monitor.NewGaugeVec(
			monitor.SUBSYSTEM_TXPOOL,
			monitor.MetricTxPoolSize,
			monitor.HelpTxPoolSizeMetric,
			monitor.ChainId, monitor.PoolType)
	}
	return list
}

// PutTxBatches put batches to batchList
func (l *batchList) PutTxBatches(mBatches []*memTxBatch, validate batchValidateFunc) {
	// should not be nil
	if len(mBatches) == 0 {
		return
	}
	// wait lock
	l.Lock()
	defer l.Unlock()
	// put batches to batchList
	for _, mBatch := range mBatches {
		l.addTxBatch(mBatch, validate)
	}
	// update metric monitor
	tx := mBatches[0].batch.Txs[0]
	l.monitor(tx, atomic.LoadInt32(l.txTotalInQueAtomic))
}

// PutTxBatch put batch to batchList
func (l *batchList) PutTxBatch(mBatch *memTxBatch, validate batchValidateFunc) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	// put batch to batchList
	l.addTxBatch(mBatch, validate)
	// update metric monitor
	tx := mBatch.batch.Txs[0]
	l.monitor(tx, atomic.LoadInt32(l.txTotalInQueAtomic))
}

// PutTxBatchToPendingCache add batch to the pendingCache of batchList
func (l *batchList) PutTxBatchToPendingCache(mBatch *memTxBatch, validate batchValidateFunc) {
	// verify whether batch is valid if validate is not nil
	if validate == nil || validate(mBatch) == nil {
		// add batch to pendingCache
		l.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
}

// FetchTxBatches fetch some batches from queue by txCount and move these to pendingCache
func (l *batchList) FetchTxBatches(txCount int, validate batchValidateFunc) []*memTxBatch {
	// update count
	txCountQue := l.TxCountInQue()
	if txCountQue < txCount {
		txCount = txCountQue
	}
	var (
		mBatches   []*memTxBatch
		errBatches []*memTxBatch
	)
	// wait lock
	l.Lock()
	// process fetched batches
	defer func() {
		// delete err batches
		if len(errBatches) > 0 {
			for _, mBatch := range errBatches {
				l.queue.Remove(mBatch.getBatchId())
			}
			l.addTxCountAndTxTotal(-1 * calcTxNumInMemBatches(errBatches))
			tx := errBatches[0].batch.Txs[0]
			l.monitor(tx, atomic.LoadInt32(l.txTotalInQueAtomic))
		}
		// remove valid batches from queue to pendingCache
		if len(mBatches) > 0 {
			for _, mBatch := range mBatches {
				batchId := mBatch.getBatchId()
				l.queue.Remove(batchId)
				l.pendingCache.Store(batchId, mBatch)
			}
			l.addTxCountAndTxTotal(-1 * calcTxNumInMemBatches(mBatches))
			tx := mBatches[0].batch.Txs[0]
			l.monitor(tx, atomic.LoadInt32(l.txTotalInQueAtomic))
		}
		l.Unlock()
	}()
	// if there is batch in batchList
	// then get some batches from batchList by txCount to generate block
	if txCountQue > 0 {
		mBatches, errBatches = l.popTxBatchesFromQueue(txCount, validate)
	}
	l.log.Debugw("batchList fetch batches", "count", txCount, "txCountInQue", txCountQue,
		"batches", len(mBatches), "txs", calcTxNumInMemBatches(mBatches),
		"errBatches", len(errBatches), "errTxs", calcTxNumInMemBatches(errBatches))
	return mBatches
}

// GetTxBatches get batches by batchIds and stage
// return batchesRet that in batchList and batchMis than not in batchList
func (l *batchList) GetTxBatches(batchIds []string, stage StageMod) (
	batchesRet map[string]*memTxBatch, batchMis map[string]struct{}) {
	if stage <= StageUnknown || stage > StageInQueueAndPending {
		return nil, createBatchIdsMap(batchIds)
	}
	batchesRet = make(map[string]*memTxBatch, len(batchIds))
	batchMis = make(map[string]struct{}, len(batchIds))
	var mBatch *memTxBatch
	// get batches from pendingCache
	if stage == StageInPending || stage == StageInQueueAndPending {
		for _, batchId := range batchIds {
			if val, ok := l.pendingCache.Load(batchId); ok && val != nil {
				if mBatch, ok = val.(*memTxBatch); ok {
					batchesRet[batchId] = mBatch
				}
			}
		}
	}
	// get batches from queue
	if stage == StageInQueue || stage == StageInQueueAndPending {
		// wait lock
		l.RLock()
		defer l.RUnlock()
		// get batches from queue
		var ok bool
		for _, batchId := range batchIds {
			if val := l.queue.Get(batchId); val != nil {
				if mBatch, ok = val.(*memTxBatch); ok {
					batchesRet[batchId] = mBatch
				}
			}
		}
	}
	// calc miss batches
	for _, batchId := range batchIds {
		if _, ok := batchesRet[batchId]; !ok {
			batchMis[batchId] = struct{}{}
		}
	}
	l.log.Debugw("batchList get batches", "batchIds", len(batchIds),
		"batchesRet", len(batchesRet), "txs", calcTxNumInMemBatchesMap(batchesRet), "batchMis", len(batchMis))
	return
}

// MoveTxBatchesToQueue delete batches in pendingCache and move batches to queue
func (l *batchList) MoveTxBatchesToQueue(batchIds []string, validate batchValidateFunc) bool {
	// wait lock
	l.Lock()
	defer l.Unlock()
	var (
		mBatch *memTxBatch
		txNum  int32
		added  int
	)
	// delete batches in pendingCache and move batches to queue
	for _, batchId := range batchIds {
		if val, ok := l.pendingCache.LoadAndDelete(batchId); ok && val != nil {
			if mBatch, ok = val.(*memTxBatch); ok {
				if validate == nil || validate(mBatch) == nil {
					if ok = l.queue.Add(batchId, mBatch); ok {
						txNum += mBatch.getTxNum()
					}
				}
				added++
			}
		}
	}
	// update tx num
	l.addTxCountAndTxTotal(+1 * txNum)
	ok := len(batchIds) == added
	l.log.Debugw("batchList move batches from pending to queue", "batchIds", len(batchIds), "result", ok)
	return ok
}

// MoveTxBatchesToPending delete batches in queue and move batches to pendingCache
func (l *batchList) MoveTxBatchesToPending(batchIds []string) bool {
	// wait lock
	l.Lock()
	defer l.Unlock()
	var (
		mBatch *memTxBatch
		txNum  int32
		added  int
	)
	// delete batches in queue and move batches to pendingCache
	for _, batchId := range batchIds {
		if ok, val := l.queue.Remove(batchId); ok && val != nil {
			if mBatch, ok = val.(*memTxBatch); ok {
				// note: for batch pool only can move batch that in queue to pendingCache
				l.pendingCache.Store(batchId, mBatch)
				txNum += mBatch.getTxNum()
				added++
			}
		}
	}
	// update tx num
	l.addTxCountAndTxTotal(-1 * txNum)
	ok := len(batchIds) == added
	l.log.Debugw("batchList move batches from queue to pending", "batchIds", len(batchIds), "result", ok)
	return ok
}

// RemoveTxBatches remove batches from queue or pending by batchIds and delMod
func (l *batchList) RemoveTxBatches(batchIds []string, stage StageMod) bool {
	// verify stage
	if stage <= StageUnknown || stage > StageInQueueAndPending {
		return false
	}
	var removed int
	// only delete batches in pendingCache
	if stage == StageInPending {
		for _, batchId := range batchIds {
			if val, ok := l.pendingCache.LoadAndDelete(batchId); val != nil && ok {
				removed++
			}
		}
		return len(batchIds) == removed
	}

	// wait lock
	l.Lock()
	defer l.Unlock()
	// delete batches from queue, may be from pendingCache
	var (
		mBatch *memTxBatch
		txNum  int32
	)
	for _, batchId := range batchIds {
		if ok, val := l.queue.Remove(batchId); ok && val != nil {
			if mBatch, ok = val.(*memTxBatch); ok {
				txNum += mBatch.getTxNum()
				removed++
			}
		}
		if stage == StageInQueueAndPending {
			if val, ok := l.pendingCache.LoadAndDelete(batchId); val != nil && ok {
				removed++
			}
		}
	}
	// update tx count and monitor
	if txNum > 0 {
		l.addTxCountAndTxTotal(-1 * txNum)
	}
	if mBatch != nil {
		tx := mBatch.batch.Txs[0]
		l.monitor(tx, atomic.LoadInt32(l.txTotalInQueAtomic))
	}
	return len(batchIds) == removed
}

// HasTxBatch verify whether txBatch exist in queue or pendingCache
func (l *batchList) HasTxBatch(batchId string) bool {
	// get txBatch from pendingCache
	if val, ok := l.pendingCache.Load(batchId); ok && val != nil {
		return true
	}
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// get tx in queue
	return l.queue.Get(batchId) != nil
}

// HasTxBatchWithoutTimestamp verify whether batch exist in queue or pendingCache
func (l *batchList) HasTxBatchWithoutTimestamp(batchIdWithoutTimestamp string) (exist bool) {
	// verify whether batch exist in pendingCache
	l.pendingCache.Range(func(k, val interface{}) bool {
		if mBatch, ok := val.(*memTxBatch); ok {
			if getNodeIdAndBatchHash(mBatch.getBatchId()) == batchIdWithoutTimestamp {
				exist = true
				return false
			}
		}
		return true
	})
	// if batch exist in pendingCache, return result
	if exist {
		return
	}
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// verify whether batch exist in queue
	node := l.queue.GetLinkList().Front()
	for node != nil {
		if batchId, ok := node.Value.(string); ok {
			if getNodeIdAndBatchHash(batchId) == batchIdWithoutTimestamp {
				exist = true
				return
			}
		}
		node = node.Next()
	}
	return
}

// GetTx get tx from queue and pendingCache
func (l *batchList) GetTx(txId string) (tx *commonPb.Transaction, exist bool) {
	// get tx from pendingCache
	var idx int32
	l.pendingCache.Range(func(k, val interface{}) bool {
		if mBatch, ok := val.(*memTxBatch); ok {
			if idx, exist = mBatch.batch.TxIdsMap[txId]; exist {
				tx = mBatch.batch.Txs[idx]
				return false
			}
		}
		return true
	})
	// if tx exist in pendingCache, return result
	if exist {
		return
	}
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// get tx in queue
	node := l.queue.GetLinkList().Front()
	for node != nil {
		if batchId, ok := node.Value.(string); ok {
			if mBatch, ok1 := l.queue.Get(batchId).(*memTxBatch); ok1 {
				if idx, exist = mBatch.batch.TxIdsMap[txId]; exist {
					tx = mBatch.batch.Txs[idx]
					return
				}
			}
		}
		node = node.Next()
	}
	return
}

// GetTxBatchesByStage get batches in queue or pendingCache
func (l *batchList) GetTxBatchesByStage(stage StageMod) (mBatches []*memTxBatch, batchIds []string) {
	// verify stage
	if stage <= StageUnknown || stage > StageInQueueAndPending {
		return
	}
	batchSize := l.BatchSize()
	mBatches = make([]*memTxBatch, 0, batchSize)
	batchIds = make([]string, 0, batchSize)
	// get batches in pendingCache
	if stage == StageInPending || stage == StageInQueueAndPending {
		l.pendingCache.Range(func(k, val interface{}) bool {
			if mBatch, ok := val.(*memTxBatch); ok {
				mBatches = append(mBatches, mBatch)
				batchIds = append(batchIds, mBatch.getBatchId())
			}
			return true
		})
	}
	// get batches in queue
	if stage == StageInQueue || stage == StageInQueueAndPending {
		// wait lock
		l.RLock()
		defer l.RUnlock()
		node := l.queue.GetLinkList().Front()
		for node != nil {
			if batchId, ok := node.Value.(string); ok {
				if mBatch, ok1 := l.queue.Get(batchId).(*memTxBatch); ok1 {
					mBatches = append(mBatches, mBatch)
					batchIds = append(batchIds, mBatch.getBatchId())
				}
			}
			node = node.Next()
		}
	}
	return
}

// BatchSize return the num of batch in queue
func (l *batchList) BatchSize() (queueSize int) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	// get queue size
	queueSize = l.queue.Size()
	return
}

// TxCountInQue return the num of tx in queue
func (l *batchList) TxCountInQue() (txCountQue int) {
	txCountQue = int(atomic.LoadInt32(l.txCountInQueAtomic))
	return
}

// TxCountInPending return the num of tx in pendingCache
func (l *batchList) TxCountInPending() (txCountPen int) {
	// get tx count in pendingCache
	l.pendingCache.Range(func(k, val interface{}) bool {
		if mBatch, ok := val.(*memTxBatch); ok {
			txCountPen += int(mBatch.getTxNum())
		}
		return true
	})
	return
}

// addTxBatch add batch to batchList
func (l *batchList) addTxBatch(mBatch *memTxBatch, validate batchValidateFunc) {
	// verify whether batch is valid if validate is not nil
	if validate == nil || validate(mBatch) == nil {
		// add batch to batchList
		if ok := l.queue.Add(mBatch.getBatchId(), mBatch); ok {
			// update queue size
			l.addTxCountAndTxTotal(+1 * mBatch.getTxNum())
		}
	}
}

// popTxBatchesFromQueue get batches from queue
func (l *batchList) popTxBatchesFromQueue(txCount int, validate batchValidateFunc) (
	mBatches []*memTxBatch, errBatches []*memTxBatch) {
	// new value
	batchSize := BatchMaxSize(l.chainConf)
	mBatches = make([]*memTxBatch, 0, txCount/batchSize)
	errBatches = make([]*memTxBatch, 0, txCount/batchSize)
	// get batch from the front of batchList
	node := l.queue.GetLinkList().Front()
	for node != nil && txCount > 0 {
		batchId, ok := node.Value.(string)
		if !ok {
			l.log.Errorf("interface value transfer into string failed")
		}
		mBatch, ok1 := l.queue.Get(batchId).(*memTxBatch)
		if !ok1 {
			l.log.Errorf("interface val transfer into *memTxBatch failed")
		}
		// verify whether txs in batch are expired
		if validate != nil && validate(mBatch) != nil {
			errBatches = append(errBatches, mBatch)
		} else {
			// need to get all valid txs
			txCount -= int(mBatch.getValidTxNum())
			// the number of tx retrieved cannot exceed txCount
			if txCount >= 0 {
				mBatches = append(mBatches, mBatch)
			}
		}
		node = node.Next()
	}
	return
}

// addTxCountAndTxTotal modify txCountInQueAtomic and txTotalInQueAtomic
func (l *batchList) addTxCountAndTxTotal(num int32) {
	atomic.AddInt32(l.txCountInQueAtomic, num)
	atomic.AddInt32(l.txTotalInQueAtomic, num)
}

// monitor metric tx num in queue
func (l *batchList) monitor(tx *commonPb.Transaction, len int32) {
	if MonitorEnabled {
		if isConfigTx(tx, l.chainConf) {
			// config tx
			go l.metricTxPoolSize.WithLabelValues(tx.Payload.ChainId, "config").Set(float64(len))
		} else {
			// common tx
			go l.metricTxPoolSize.WithLabelValues(tx.Payload.ChainId, "normal").Set(float64(len))
		}
	}
}
