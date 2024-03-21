/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_PutTxBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches
	mBatches := generateMBatches(20, false, 2)
	list.PutTxBatches(mBatches[0:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)
	// add duplicated batch with queue
	list.PutTxBatches(mBatches[0:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)
	// add duplicated batch with pending
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)
	list.PutTxBatches(mBatches[10:20], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)
}

func Test_PutTxBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches
	mBatches := generateMBatches(20, false, 2)
	for _, mBatch := range mBatches[0:10] {
		list.PutTxBatch(mBatch, list.addTxBatchValidate)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)
	// add duplicated batch with queue
	mBatch1 := mBatches[0]
	list.PutTxBatch(mBatch1, list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)
	// add duplicated batch with pending
	mBatch11 := mBatches[10]
	list.pendingCache.Store(mBatch11.getBatchId(), mBatch11)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 2, txCountInPen)
	list.PutTxBatch(mBatch11, list.addTxBatchValidate)
	list.pendingCache.Store(mBatch11.getBatchId(), mBatch11)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 2, txCountInPen)
}

func Test_PutTxBatchToPendingCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to pendingCache
	mBatches := generateMBatches(20, false, 2)
	for _, mBatch := range mBatches[0:10] {
		list.PutTxBatchToPendingCache(mBatch, nil)
	}
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)
	// add duplicated batch with pendingCache
	mBatch1 := mBatches[0]
	list.PutTxBatchToPendingCache(mBatch1, nil)
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)
}

func Test_FetchTxBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches
	mBatches := generateMBatches(10, false, 2)
	list.PutTxBatches(mBatches, list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// fetch 4 txCount, return 2 batches
	batchesRet := list.FetchTxBatches(4, list.fetchTxBatchValidate)
	require.EqualValues(t, 2, len(batchesRet))
	require.EqualValues(t, 8, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 16, txCountInQue)
	require.EqualValues(t, 4, txCountInPen)

	// fetch 3 txCount, return 1 batch
	batchesRet = list.FetchTxBatches(3, list.fetchTxBatchValidate)
	require.EqualValues(t, 1, len(batchesRet))
	require.EqualValues(t, 7, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 14, txCountInQue)
	require.EqualValues(t, 6, txCountInPen)

	// fetch 15 txCount, return 7 batches
	batchesRet = list.FetchTxBatches(15, list.fetchTxBatchValidate)
	require.EqualValues(t, 7, len(batchesRet))
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)
}

func Test_GetTxBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// get 10 batches in queue
	batchIds := generateBatchIdsByBatches(mBatches)
	batchesRet, batchMis := list.GetTxBatches(batchIds[:10], StageInQueue)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchMis))
	batchesRet, batchMis = list.GetTxBatches(batchIds[:10], StageInPending)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchMis))
	batchesRet, batchMis = list.GetTxBatches(batchIds[:10], StageInQueueAndPending)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchMis))

	// get 10 batches in pendingCache
	batchesRet, batchMis = list.GetTxBatches(batchIds[10:20], StageInQueue)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchMis))
	batchesRet, batchMis = list.GetTxBatches(batchIds[10:20], StageInPending)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchMis))
	batchesRet, batchMis = list.GetTxBatches(batchIds[10:20], StageInQueueAndPending)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchMis))

	// get 10 batches no in queue or pendingCache
	batchesRet, batchMis = list.GetTxBatches(batchIds[20:30], StageInQueue)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchMis))
	batchesRet, batchMis = list.GetTxBatches(batchIds[20:30], StageInPending)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchMis))
	batchesRet, batchMis = list.GetTxBatches(batchIds[20:30], StageInQueueAndPending)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchMis))
}

func Test_MoveTxBatchesToQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// move 10 batches in queue to queue
	batchIds := generateBatchIdsByBatches(mBatches)
	ok := list.MoveTxBatchesToQueue(batchIds[:10], nil)
	require.EqualValues(t, ok, false)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// move 10 batches in pendingCache to queue
	ok = list.MoveTxBatchesToQueue(batchIds[10:20], nil)
	require.EqualValues(t, ok, true)
	require.EqualValues(t, 20, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 40, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// move 10 batches not in pool to pendingCache
	ok = list.MoveTxBatchesToPending(batchIds[20:30])
	require.EqualValues(t, ok, false)
	require.EqualValues(t, 20, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 40, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)
}

func Test_MoveTxBatchesToPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// move 10 batches in queue to pendingCache
	batchIds := generateBatchIdsByBatches(mBatches)
	ok := list.MoveTxBatchesToPending(batchIds[:10])
	require.EqualValues(t, ok, true)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 40, txCountInPen)

	// move 10 batches in pendingCache to pendingCache
	ok = list.MoveTxBatchesToPending(batchIds[10:20])
	require.EqualValues(t, ok, false)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 40, txCountInPen)

	// move 10 batches not in pool to pendingCache
	ok = list.MoveTxBatchesToPending(batchIds[20:30])
	require.EqualValues(t, ok, false)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 40, txCountInPen)
}

func Test_RemoveTxBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// delete 10 batches in queue
	batchIds := generateBatchIdsByBatches(mBatches)
	ok := list.RemoveTxBatches(batchIds[:10], StageInPending)
	require.EqualValues(t, ok, false)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	ok = list.RemoveTxBatches(batchIds[:10], StageInQueue)
	require.EqualValues(t, ok, true)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	ok = list.RemoveTxBatches(batchIds[:10], StageInQueueAndPending)
	require.EqualValues(t, ok, true)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// delete 10 batches in pendingCache
	ok = list.RemoveTxBatches(batchIds[10:20], StageInQueue)
	require.EqualValues(t, ok, false)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	ok = list.RemoveTxBatches(batchIds[10:20], StageInPending)
	require.EqualValues(t, ok, true)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	ok = list.RemoveTxBatches(batchIds[10:20], StageInQueueAndPending)
	require.EqualValues(t, ok, true)
	require.EqualValues(t, 0, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 0, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)
}

func Test_HasTxBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// tx exist in queue
	batchIds := generateBatchIdsByBatches(mBatches)
	require.EqualValues(t, true, list.HasTxBatch(batchIds[0]))
	// tx exist in pendingCache
	require.EqualValues(t, true, list.HasTxBatch(batchIds[10]))
	// tx no exist in pool
	require.EqualValues(t, false, list.HasTxBatch(batchIds[20]))
}

func Test_HasTxBatchWithoutTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// tx exist in queue
	batchIds := generateBatchIdsByBatches(mBatches)
	require.EqualValues(t, true, list.HasTxBatchWithoutTimestamp(getNodeIdAndBatchHash(batchIds[0])))
	// tx exist in pendingCache
	require.EqualValues(t, true, list.HasTxBatchWithoutTimestamp(getNodeIdAndBatchHash(batchIds[10])))
	// tx no exist in pool
	require.EqualValues(t, false, list.HasTxBatchWithoutTimestamp(getNodeIdAndBatchHash(batchIds[20])))
}

func Test_GetTx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// get tx in queue
	tx0 := mBatches[0].batch.Txs[0]
	tx, exist := list.GetTx(tx0.Payload.TxId)
	require.EqualValues(t, true, exist)
	require.EqualValues(t, tx0, tx)
	// get tx in pendingCache
	tx10 := mBatches[10].batch.Txs[0]
	tx, exist = list.GetTx(tx10.Payload.TxId)
	require.EqualValues(t, true, exist)
	require.EqualValues(t, tx10, tx)
	// get tx no in pool
	tx20 := mBatches[20].batch.Txs[0]
	tx, exist = list.GetTx(tx20.Payload.TxId)
	require.EqualValues(t, false, exist)
	require.Nil(t, tx)
}

func Test_GetTxBatchesByStage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(10, 10, 2, false, "")
	// create batchList
	txCount := int32(0)
	list := newBatchList(&txCount, newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 batches to queue
	mBatches := generateMBatches(30, false, 2)
	list.PutTxBatches(mBatches[:10], list.addTxBatchValidate)
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue := list.TxCountInQue()
	txCountInPen := list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 0, txCountInPen)

	// add 10 batches to pendingCache
	for _, mBatch := range mBatches[10:20] {
		list.pendingCache.Store(mBatch.getBatchId(), mBatch)
	}
	require.EqualValues(t, 10, list.BatchSize())
	txCountInQue = list.TxCountInQue()
	txCountInPen = list.TxCountInPending()
	require.EqualValues(t, 20, txCountInQue)
	require.EqualValues(t, 20, txCountInPen)

	// get batches in queue
	batchesRet, batchIds := list.GetTxBatchesByStage(StageInQueue)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 10, len(batchIds))
	require.EqualValues(t, batchesRet[0].getBatchId(), batchIds[0])

	// get batches in pendingCache
	batchesRet, batchIds = list.GetTxBatchesByStage(StageInPending)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 10, len(batchIds))
	require.EqualValues(t, batchesRet[0].getBatchId(), batchIds[0])

	// get batches in pool
	batchesRet, batchIds = list.GetTxBatchesByStage(StageInQueueAndPending)
	require.EqualValues(t, 20, len(batchesRet))
	require.EqualValues(t, 20, len(batchIds))
	require.EqualValues(t, batchesRet[0].getBatchId(), batchIds[0])
}
