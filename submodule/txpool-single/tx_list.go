/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"fmt"
	"sync"

	"chainmaker.org/chainmaker/common/v2/linkedhashmap"
	"chainmaker.org/chainmaker/common/v2/monitor"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// txList Structure of store transactions in memory
type txList struct {
	sync.RWMutex
	queue        *linkedhashmap.LinkedHashMap // orderly store *memTx. key: txId val:*memTx
	pendingCache sync.Map                     // a place where transactions are stored after Fetch. key: txId val:*memTx

	chainConf        protocol.ChainConf   // chain config
	metricTxPoolSize *prometheus.GaugeVec // metric
	log              protocol.Logger      // log
}

// newTxList create txList
func newTxList(chainConf protocol.ChainConf, log protocol.Logger) *txList {
	// create txList
	list := &txList{
		queue:        linkedhashmap.NewLinkedHashMap(),
		pendingCache: sync.Map{},
		chainConf:    chainConf,
		log:          log,
	}
	// if enable monitor
	// create protocol metric
	if MonitorEnabled {
		list.metricTxPoolSize = monitor.NewGaugeVec(
			monitor.SUBSYSTEM_TXPOOL,
			monitor.MetricTxPoolSize,
			monitor.HelpTxPoolSizeMetric,
			monitor.ChainId, monitor.PoolType)
	}
	return list
}

// Put Add transaction to the txList
func (l *txList) Put(mtxs []*memTx, source protocol.TxSource, validate txValidateFunc) {
	// should not be nil
	if len(mtxs) == 0 {
		return
	}
	// put txs to queue
	for _, mtx := range mtxs {
		l.addTxs(mtx, source, validate)
	}
	// update metric monitor
	l.monitor(mtxs[0], l.queue.Size())
}

// addTxs add txs to txList
func (l *txList) addTxs(mtx *memTx, source protocol.TxSource, validate txValidateFunc) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	// note: validate is nil now
	if validate == nil || validate(mtx) == nil {
		txId := mtx.getTxId()
		if source != protocol.INTERNAL {
			if val, ok := l.pendingCache.Load(txId); ok && val != nil {
				return
			}
		}
		// verify whether tx exist in queue
		if l.queue.Get(txId) != nil {
			return
		}
		// add tx to queue
		l.queue.Add(txId, mtx)
	}
}

// Fetch Gets a list of stored transactions
func (l *txList) Fetch(count int, validate txValidateFunc) (
	[]*memTx, []string) {
	// update count
	queueLen := l.queue.Size()
	if queueLen < count {
		count = queueLen
	}
	var (
		mtxs    []*memTx
		txIds   []string
		errKeys []string
	)
	l.Lock()
	defer func() {
		if len(errKeys) > 0 {
			for _, txId := range errKeys {
				l.queue.Remove(txId)
			}
		}
		if len(mtxs) > 0 {
			// move txs from queue to pendingCache
			for _, mtx := range mtxs {
				txId := mtx.getTxId()
				l.queue.Remove(txId)
				l.pendingCache.Store(txId, mtx)
			}
			// update metric monitor
			l.monitor(mtxs[0], l.queue.Size())
		}
		l.Unlock()
	}()
	// if there is txs in queue
	// then get some txs from queue by count to generate block
	if count > 0 {
		mtxs, txIds, errKeys = l.getTxsFromQueue(count, validate)
	}
	l.log.Debugw("txList fetch txs", "count", count, "queueSize", queueLen,
		"txs", len(mtxs), "errKeys", len(errKeys))
	return mtxs, txIds
}

// Get Retrieve the transaction from txList by the txId
// return error when the transaction does not exist
func (l *txList) Get(txId string) (tx *memTx, err error) {
	// first get txs from pendingCache
	if val, ok := l.pendingCache.Load(txId); ok && val != nil {
		if mtx, ok1 := val.(*memTx); ok1 {
			l.log.Debugw("txList get tx in pending", "txId", txId, "exist", true)
			return mtx, nil
		}
		l.log.Errorf("interface val transfer into *memTx failed")
		l.pendingCache.Delete(txId)
	}
	// second get txs from queue
	l.RLock()
	defer l.RUnlock()
	if val := l.queue.Get(txId); val != nil {
		if mtx, ok := val.(*memTx); ok {
			l.log.Debugw("txList get tx in queue", "txId", txId, "exist", true)
			return mtx, nil
		}
		l.log.Errorf("interface val transfer into *memTx failed")
		l.queue.Remove(txId)
	}
	l.log.Debugw("txList get tx failed", "txId", txId, "exist", false)
	return nil, fmt.Errorf("tx pool no this transaction")
}

// GetTxs Retrieve transactions from txList by txIds
// return transactions found in txList and missing transactions not found in txList
func (l *txList) GetTxs(txIds []string) (mtxsRet map[string]*memTx, txsMis map[string]struct{}) {
	mtxsRet = make(map[string]*memTx, len(txIds))
	txsMis = make(map[string]struct{}, len(txIds))
	txIdsNoPending := make([]string, 0, len(txIds))
	// first get txs from pendingCache
	for _, txId := range txIds {
		if val, ok := l.pendingCache.Load(txId); ok && val != nil {
			if mtx, ok1 := val.(*memTx); ok1 {
				mtxsRet[txId] = mtx
			} else {
				l.log.Errorf("interface val transfer into *memTx failed")
				l.pendingCache.Delete(txId)
				txIdsNoPending = append(txIdsNoPending, txId)
			}
		} else {
			txIdsNoPending = append(txIdsNoPending, txId)
		}
	}
	// second get txs from queue
	l.RLock()
	defer l.RUnlock()
	for _, txId := range txIdsNoPending {
		if val := l.queue.Get(txId); val != nil {
			if mtx, ok := val.(*memTx); ok {
				mtxsRet[txId] = mtx
			} else {
				l.log.Errorf("interface val transfer into *memTx failed")
				l.queue.Remove(txId)
				txsMis[txId] = struct{}{}
			}
		} else {
			txsMis[txId] = struct{}{}
		}
	}
	l.log.Debugw("txList get txs by txIds", "want", len(txIds), "get", len(mtxsRet))
	return mtxsRet, txsMis
}

// Delete Delete transactions from TXList by the txIds
func (l *txList) Delete(txIds []string) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	// delete txs in queue and pendingCache
	for _, txId := range txIds {
		l.queue.Remove(txId)
		l.pendingCache.Delete(txId)
	}
}

// DeleteTxsInPending Delete transactions in PendingCache by the txIds
func (l *txList) DeleteTxsInPending(txIds []string) {
	for _, txId := range txIds {
		l.pendingCache.Delete(txId)
	}
}

// getTxsFromQueue get txs from queue
func (l *txList) getTxsFromQueue(count int, validate txValidateFunc) (
	mtxs []*memTx, txIds []string, errKeys []string) {
	mtxs = make([]*memTx, 0, count)
	txIds = make([]string, 0, count)
	errKeys = make([]string, 0, count)
	node := l.queue.GetLinkList().Front()
	for node != nil && count > 0 {
		txId, ok := node.Value.(string)
		if !ok {
			l.log.Errorf("interface value transfer into string failed")
		}
		mtx, ok1 := l.queue.Get(txId).(*memTx)
		if !ok1 {
			l.log.Errorf("interface val transfer into *commonPb.Transaction failed")
		}
		if validate != nil && validate(mtx) != nil {
			errKeys = append(errKeys, txId)
		} else {
			// ensure tx no in pending
			if _, ok2 := l.pendingCache.Load(txId); ok2 {
				l.log.Warnf("tx:%s can not duplicate to package block", txId)
				errKeys = append(errKeys, txId)
			} else {
				mtxs = append(mtxs, mtx)
				txIds = append(txIds, txId)
				// need to get all valid txs
				count--
			}
		}
		node = node.Next()
	}
	return
}

// Has Determine if the transaction exists in the txList
func (l *txList) Has(txId string, checkPending bool) (exist bool) {
	// first get txs in pendingCache
	if checkPending {
		if val, ok := l.pendingCache.Load(txId); ok && val != nil {
			return true
		}
	}
	// second get txs in queue
	l.RLock()
	defer l.RUnlock()
	return l.queue.Get(txId) != nil
}

// GetTxsByStage Get transactions in queue or pending
func (l *txList) GetTxsByStage(inQueue, inPending bool) (txs []*commonPb.Transaction, txIds []string) {
	totalSize := l.queueSize() + l.pendingSize()
	txs = make([]*commonPb.Transaction, 0, totalSize)
	txIds = make([]string, 0, totalSize)
	// get txs in pending
	if inPending {
		l.pendingCache.Range(func(k, v interface{}) bool {
			txs = append(txs, v.(*memTx).getTx())
			txIds = append(txIds, k.(string))
			return true
		})
	}
	// get txs in queue
	if inQueue {
		l.RLock()
		defer l.RUnlock()
		node := l.queue.GetLinkList().Front()
		for node != nil {
			txId, ok := node.Value.(string)
			if !ok {
				l.log.Errorf("interface value transfer into string failed")
			}
			mtx, ok1 := l.queue.Get(txId).(*memTx)
			if !ok1 {
				l.log.Errorf("interface val transfer into *memTx failed")
			}
			txs = append(txs, mtx.getTx())
			txIds = append(txIds, txId)
			node = node.Next()
		}
	}
	return
}

// appendTxsToPendingCache Delete transaction in queue and move to pending
func (l *txList) appendTxsToPendingCache(txs []*commonPb.Transaction, blockHeight uint64) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	for _, tx := range txs {
		// delete txs in queue and add txs to pendingCache
		l.queue.Remove(tx.Payload.TxId)
		l.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: blockHeight})
	}
}

// queueSize Get the number of transactions stored in the queue
func (l *txList) queueSize() int {
	// wait lock
	l.RLock()
	defer l.RUnlock()
	return l.queue.Size()
}

// pendingSize Get the number of transactions stored in the pending
func (l *txList) pendingSize() int {
	var pendingSize int
	l.pendingCache.Range(func(k, v interface{}) bool {
		pendingSize++
		return true
	})
	return pendingSize
}

// monitor count tx pool size
func (l *txList) monitor(mtx *memTx, len int) {
	if MonitorEnabled {
		if isConfigTx(mtx.getTx(), l.chainConf) {
			go l.metricTxPoolSize.WithLabelValues(mtx.getChainId(), "config").Set(float64(len))
		} else {
			go l.metricTxPoolSize.WithLabelValues(mtx.getChainId(), "normal").Set(float64(len))
		}
	}
}
