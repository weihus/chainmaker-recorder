/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"fmt"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
)

type txQueue struct {
	commonTxQueue *txList // common transaction queue
	configTxQueue *txList // config transaction queue

	log protocol.Logger // log
}

// newQueue create txQueue
func newQueue(chainConf protocol.ChainConf, log protocol.Logger) *txQueue {
	// create txQueue
	return &txQueue{
		log:           log,
		commonTxQueue: newTxList(chainConf, log),
		configTxQueue: newTxList(chainConf, log),
	}
}

// addTxsToConfigQueue add config txs to queue
func (queue *txQueue) addTxsToConfigQueue(memTxs *mempoolTxs) {
	// note: validate is nil
	queue.configTxQueue.Put(memTxs.mtxs, memTxs.source, nil)
}

// addTxsToCommonQueue add common txs to queue
func (queue *txQueue) addTxsToCommonQueue(memTxs *mempoolTxs) {
	// note: validate is nil
	queue.commonTxQueue.Put(memTxs.mtxs, memTxs.source, nil)
}

// fetch get a batch of transactions from the tx pool to generate new block
// if there is config tx, get config tx first
func (queue *txQueue) fetch(expectedCount int) []*memTx {
	// first fetch the config transaction
	if configQueueSize := queue.configTxsCount(); configQueueSize > 0 {
		if mtxs, _ := queue.configTxQueue.Fetch(1, queue.configTxQueue.fetchTxValidate); len(mtxs) > 0 {
			queue.log.Debugw("txQueue fetch config tx", "txCount", 1, "configQueueSize",
				configQueueSize, "txs", len(mtxs))
			return mtxs
		}
	}
	// second fetch the common transaction
	if commonQueueSize := queue.commonTxsCount(); commonQueueSize > 0 {
		if mtxs, _ := queue.commonTxQueue.Fetch(expectedCount, queue.commonTxQueue.fetchTxValidate); len(mtxs) > 0 {
			queue.log.Debugw("txQueue fetch common txs", "txCount", expectedCount, "commonQueueSize",
				commonQueueSize, "txs", len(mtxs))
			return mtxs
		}
	}
	return nil
}

// get get a tx by txId
func (queue *txQueue) get(txId string) (mtx *memTx, err error) {
	// common tx
	if mtx, err = queue.commonTxQueue.Get(txId); err == nil {
		return mtx, nil
	}
	// config tx
	if mtx, err = queue.configTxQueue.Get(txId); err == nil {
		return mtx, nil
	}
	return nil, err
}

// getCommonTxs get common txs by txIds
func (queue *txQueue) getCommonTxs(txIds []string) (map[string]*memTx, map[string]struct{}) {
	return queue.commonTxQueue.GetTxs(txIds)
}

// getTxsInPending get config or common txs in pendingCache by txIds
func (queue *txQueue) getTxsInPending(txIds []string) (mtxsRet map[string]*memTx, txsMis map[string]struct{}) {
	// common tx
	mtxsRet, txsMis = queue.commonTxQueue.GetTxs(txIds)
	if len(mtxsRet) > 0 {
		return
	}
	// config tx
	mtxsRet, txsMis = queue.configTxQueue.GetTxs(txIds)
	return
}

// appendConfigTxsToPendingCache append config txs to pending cache
func (queue *txQueue) appendConfigTxsToPendingCache(txs []*commonPb.Transaction, blockHeight uint64) {
	queue.configTxQueue.appendTxsToPendingCache(txs, blockHeight)
}

// appendCommonTxsToPendingCache append common txs to pending cache
func (queue *txQueue) appendCommonTxsToPendingCache(txs []*commonPb.Transaction, blockHeight uint64) {
	queue.commonTxQueue.appendTxsToPendingCache(txs, blockHeight)
}

// deleteConfigTxs delete config txs in queue and pending cache
func (queue *txQueue) deleteConfigTxs(txIds []string) {
	queue.configTxQueue.Delete(txIds)
}

// deleteCommonTxs delete common txs in queue and pending cache
func (queue *txQueue) deleteCommonTxs(txIds []string) {
	queue.commonTxQueue.Delete(txIds)
}

// deleteConfigTxsInPending delete config txs in pending cache
func (queue *txQueue) deleteConfigTxsInPending(txs []*commonPb.Transaction) {
	for _, tx := range txs {
		queue.configTxQueue.DeleteTxsInPending([]string{tx.Payload.TxId})
	}
}

// deleteCommonTxsInPending delete common txs in pending cache
func (queue *txQueue) deleteCommonTxsInPending(txs []*commonPb.Transaction) {
	for _, tx := range txs {
		queue.commonTxQueue.DeleteTxsInPending([]string{tx.Payload.TxId})
	}
}

// getPoolStatus get pool status
func (queue *txQueue) getPoolStatus() *txpoolPb.TxPoolStatus {
	return &txpoolPb.TxPoolStatus{
		ConfigTxPoolSize:     int32(MaxConfigTxPoolSize()),
		CommonTxPoolSize:     int32(MaxCommonTxPoolSize()),
		ConfigTxNumInQueue:   int32(queue.configTxQueue.queueSize()),
		ConfigTxNumInPending: int32(queue.configTxQueue.pendingSize()),
		CommonTxNumInQueue:   int32(queue.commonTxQueue.queueSize()),
		CommonTxNumInPending: int32(queue.commonTxQueue.pendingSize()),
	}
}

// getTxsByTxTypeAndStage get txs and txIds by txType and txStage
func (queue *txQueue) getTxsByTxTypeAndStage(txType, txStage int32) (txs []*commonPb.Transaction, txIds []string) {
	if txType == int32(txpoolPb.TxType_CONFIG_TX) {
		txs, txIds = queue.getConfigTxsByTxStage(txStage)
		return
	} else if txType == int32(txpoolPb.TxType_COMMON_TX) {
		txs, txIds = queue.getCommonTxsByTxStage(txStage)
		return
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

// getConfigTxsByTxStage get config txs and txIds by txStage
func (queue *txQueue) getConfigTxsByTxStage(txStage int32) (txs []*commonPb.Transaction, txIds []string) {
	if txStage == int32(txpoolPb.TxStage_IN_QUEUE) {
		txs, txIds = queue.configTxQueue.GetTxsByStage(true, false)
		return
	} else if txStage == int32(txpoolPb.TxStage_IN_PENDING) {
		txs, txIds = queue.configTxQueue.GetTxsByStage(false, true)
		return
	} else if txStage == int32(txpoolPb.TxStage_ALL_STAGE) {
		txs, txIds = queue.configTxQueue.GetTxsByStage(true, true)
		return
	}
	return
}

// getCommonTxsByTxStage get common txs and txIds by txStage
func (queue *txQueue) getCommonTxsByTxStage(txStage int32) (txs []*commonPb.Transaction, txIds []string) {
	if txStage == int32(txpoolPb.TxStage_IN_QUEUE) {
		txs, txIds = queue.commonTxQueue.GetTxsByStage(true, false)
		return
	} else if txStage == int32(txpoolPb.TxStage_IN_PENDING) {
		txs, txIds = queue.commonTxQueue.GetTxsByStage(false, true)
		return
	} else if txStage == int32(txpoolPb.TxStage_ALL_STAGE) {
		txs, txIds = queue.commonTxQueue.GetTxsByStage(true, true)
		return
	}
	return
}

// has verify whether exist tx in queue ore pending
func (queue *txQueue) has(tx *commonPb.Transaction, checkPending bool) bool {
	// common tx
	if queue.commonTxQueue.Has(tx.Payload.TxId, checkPending) {
		return true
	}
	// config tx
	return queue.configTxQueue.Has(tx.Payload.TxId, checkPending)
}

// configTxsCount return the number of config txs in queue
func (queue *txQueue) configTxsCount() int {
	return queue.configTxQueue.queueSize()
}

// commonTxsCount return the number of common txs in queue
func (queue *txQueue) commonTxsCount() int {
	return queue.commonTxQueue.queueSize()
}

// status print tx pool status containing the number of config txs and common txs in queue
func (queue *txQueue) status() string {
	return fmt.Sprintf("common txs queue size:%d, config txs queue size:%d",
		queue.commonTxQueue.queueSize(), queue.configTxQueue.queueSize())
}
