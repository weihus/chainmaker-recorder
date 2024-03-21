/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"sync"
	"sync/atomic"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
)

// Dispatcher is used to distribute transaction to different common queues
type Dispatcher interface {
	// DistTx distribute a tx to a specific common queues
	DistTx(txId string, queueNum int) int
	// DistTxs distribute txs to different common queues
	DistTxs(txs []*commonPb.Transaction, queueNum int) (txsTable [][]*commonPb.Transaction)
	// DistTxsToMemTxs distribute txs to different common queues and generate mtxsTable
	DistTxsToMemTxs(txs []*commonPb.Transaction, queueNum int, height uint64) (mtxsTable [][]*memTx)
	// DistTxIds distribute txIds to different common queues
	DistTxIds(txIds []string, queueNum int) (txIdsTable [][]string)
}

// txDispatcher is a impl of Dispatcher
type txDispatcher struct {
	log protocol.Logger
}

// newTxDispatcher create txDispatcher
func newTxDispatcher(log protocol.Logger) *txDispatcher {
	// return txDispatcher
	return &txDispatcher{log: log}
}

// DistTx distribute a tx to a specific common queues
func (d *txDispatcher) DistTx(txId string, queueNum int) int {
	// select three bytes from the front, middle and back
	// add these three bytes, mod the sum
	// Note: the queueNum must be an exponent of 2 and less than 256
	length := len(txId)
	if length == 0 || queueNum <= 0 || queueNum > 256 {
		return 0
	}
	if length == 1 {
		return int(txId[0] & uint8(queueNum-1))
	}
	if length == 2 {
		return int((txId[0] + txId[1]) & uint8(queueNum-1))
	}
	return int((txId[0] + txId[(length-1)/2] + txId[length-1]) & uint8(queueNum-1))
}

// DistTxs distribute txs to different common queues
func (d *txDispatcher) DistTxs(txs []*commonPb.Transaction, queueNum int) [][]*commonPb.Transaction {
	if len(txs) == 0 {
		return nil
	}
	// distribute txs to different common queues
	// distribute result table
	disTable := make([][]*commonPb.Transaction, queueNum)
	for i := range disTable {
		disTable[i] = make([]*commonPb.Transaction, len(txs))
	}
	// the index of result table
	// it is an atomic
	indexTable := make([]int32, queueNum)
	for i := range indexTable {
		indexTable[i] = -1
	}
	// distribute concurrently
	var wg sync.WaitGroup
	for _, perTxs := range segmentTxs(txs) {
		wg.Add(1)
		go func(perTxs []*commonPb.Transaction) {
			defer wg.Done()
			for _, tx := range perTxs {
				idx := d.DistTx(tx.Payload.TxId, queueNum)
				// put distribute result to table
				queuedTxs := disTable[idx]
				queuedTxs[atomic.AddInt32(&indexTable[idx], 1)] = tx
			}
		}(perTxs)
	}
	wg.Wait()
	// prune
	for i := 0; i < len(disTable); i++ {
		disTable[i] = disTable[i][:atomic.LoadInt32(&indexTable[i])+1]
	}
	return disTable
}

// DistTxsToMemTxs distribute txs to different common queue and generate memTx
func (d *txDispatcher) DistTxsToMemTxs(txs []*commonPb.Transaction, queueNum int, height uint64) [][]*memTx {
	if len(txs) == 0 {
		return nil
	}
	// distribute txs to different common queues
	// distribute result table
	disTable := make([][]*memTx, queueNum)
	for i := range disTable {
		disTable[i] = make([]*memTx, len(txs))
	}
	// the index of result table
	// it is an atomic
	indexTable := make([]int32, queueNum)
	for i := range indexTable {
		indexTable[i] = -1
	}
	// distribute concurrently
	var wg sync.WaitGroup
	for _, perTxs := range segmentTxs(txs) {
		wg.Add(1)
		go func(perTxs []*commonPb.Transaction) {
			defer wg.Done()
			for _, tx := range perTxs {
				idx := d.DistTx(tx.Payload.TxId, queueNum)
				// put distribute result to table
				queuedMtxs := disTable[idx]
				queuedMtxs[atomic.AddInt32(&indexTable[idx], 1)] = &memTx{tx: tx, dbHeight: height}
			}
		}(perTxs)
	}
	wg.Wait()
	// prune
	for i := 0; i < len(disTable); i++ {
		disTable[i] = disTable[i][:atomic.LoadInt32(&indexTable[i])+1]
	}
	return disTable
}

// DistTxIds distribute txIds to different common queue
func (d *txDispatcher) DistTxIds(txIds []string, queueNum int) [][]string {
	if len(txIds) == 0 {
		return nil
	}
	// distribute txIds to different common queues
	// distribute result table
	disTable := make([][]string, queueNum)
	for i := range disTable {
		disTable[i] = make([]string, len(txIds))
	}
	// the index of result table
	// it is an atomic
	indexTable := make([]int32, queueNum)
	for i := range indexTable {
		indexTable[i] = -1
	}
	// distribute concurrently
	var wg sync.WaitGroup
	for _, perTxIds := range segmentTxIds(txIds) {
		wg.Add(1)
		go func(perTxIds []string) {
			defer wg.Done()
			for _, txId := range perTxIds {
				idx := d.DistTx(txId, queueNum)
				// put distribute result to table
				queuedTxIds := disTable[idx]
				queuedTxIds[atomic.AddInt32(&indexTable[idx], 1)] = txId
			}
		}(perTxIds)
	}
	wg.Wait()
	// prune
	for i := 0; i < len(disTable); i++ {
		disTable[i] = disTable[i][:atomic.LoadInt32(&indexTable[i])+1]
	}
	return disTable
}
