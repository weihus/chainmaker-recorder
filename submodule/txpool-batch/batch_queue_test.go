/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"testing"

	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_addConfigTxBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config batches to queue
	mBatches := generateMBatches(30, true, 1)
	for _, mBatch := range mBatches[0:10] {
		queue.addConfigTxBatch(mBatch)
	}
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 0, queue.commonTxCount())

	// add duplicated batch with pool
	for _, mBatch := range mBatches[0:10] {
		queue.addConfigTxBatch(mBatch)
	}
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 0, queue.commonTxCount())
}

func Test_addConfigTxBatchToPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config batches to pendingCache
	mBatches := generateMBatches(30, true, 1)
	for _, mBatch := range mBatches[0:10] {
		queue.addConfigTxBatchToPending(mBatch)
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 10, queue.configBatchQueue.TxCountInPending())
	require.EqualValues(t, 0, queue.commonTxCount())

	// add duplicated batch with pool
	for _, mBatch := range mBatches[0:10] {
		queue.addConfigTxBatchToPending(mBatch)
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 10, queue.configBatchQueue.TxCountInPending())
	require.EqualValues(t, 0, queue.commonTxCount())
}

func Test_addCommonTxBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 common batches
	mBatches := generateMBatches(30, false, 2)
	for _, mBatch := range mBatches[0:10] {
		queue.addCommonTxBatch(mBatch)
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// add duplicated batch with pool
	for _, mBatch := range mBatches[0:10] {
		queue.addCommonTxBatch(mBatch)
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())
}

func Test_addCommonTxBatchToPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 common batches to pendingCache
	mBatches := generateMBatches(30, false, 2)
	for _, mBatch := range mBatches[0:10] {
		queue.addCommonTxBatchToPending(mBatch)
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 0, queue.commonTxCount())
	status := queue.getPoolStatus()
	require.EqualValues(t, 20, status.CommonTxNumInPending)

	// add duplicated batch with pool
	for _, mBatch := range mBatches[0:10] {
		queue.addCommonTxBatchToPending(mBatch)
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 0, queue.commonTxCount())
	status = queue.getPoolStatus()
	require.EqualValues(t, 20, status.CommonTxNumInPending)
}

func Test_addCommonTxBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 common batches
	mBatches := generateMBatches(30, false, 2)
	queue.addCommonTxBatches(mBatches[0:10])
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// add duplicated batch with pool
	queue.addCommonTxBatches(mBatches[0:10])
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())
}

func Test_fetchTxBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 2, false), newMockLogger())

	// add 10 config and 10 common batches
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 1)
	//commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[0:10] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.addCommonTxBatches(commMBatches[0:10])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 10, queue.commonTxCount())

	// fetch 10 config batches
	for i := 0; i < 10; i++ {
		batchesRet := queue.fetchTxBatches()
		require.EqualValues(t, 1, len(batchesRet))
		require.EqualValues(t, batchesRet[0].batch.BatchId, confBatchIds[i])
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 10, queue.commonTxCount())

	// fetch 5 common batches
	for i := 0; i < 5; i++ {
		batchesRet := queue.fetchTxBatches()
		require.EqualValues(t, 2, len(batchesRet))
	}
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 0, queue.commonTxCount())

	// fetch no batches
	for i := 0; i < 10; i++ {
		batchesRet := queue.fetchTxBatches()
		require.EqualValues(t, 0, len(batchesRet))
	}

}

func Test_getTxBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config and 10 common batches
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[:10] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.addCommonTxBatches(commMBatches[:10])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// get 10 config batches
	batchesRet, batchesMis := queue.getTxBatches(confBatchIds[:10], StageInQueueAndPending)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchesMis))

	batchesRet, batchesMis = queue.getTxBatches(confBatchIds[:10], StageInQueue)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchesMis))

	batchesRet, batchesMis = queue.getTxBatches(confBatchIds[:10], StageInPending)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchesMis))

	batchesRet, batchesMis = queue.getTxBatches(confBatchIds[10:20], StageInQueueAndPending)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchesMis))

	// get 10 common batches
	batchesRet, batchesMis = queue.getTxBatches(commBatchIds[:10], StageInQueueAndPending)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchesMis))

	batchesRet, batchesMis = queue.getTxBatches(commBatchIds[:10], StageInQueue)
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchesMis))

	batchesRet, batchesMis = queue.getTxBatches(commBatchIds[:10], StageInPending)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchesMis))

	batchesRet, batchesMis = queue.getTxBatches(commBatchIds[10:20], StageInQueueAndPending)
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchesMis))
}

func Test_moveBatchesToQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config and 10 common batches to queue
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[:10] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.addCommonTxBatches(commMBatches[:10])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// add 10 config and 10 common batches to pendingCache
	for _, mBatch := range confMBatches[10:20] {
		queue.addConfigTxBatchToPending(mBatch)
	}
	for _, mBatch := range commMBatches[10:20] {
		queue.addCommonTxBatchToPending(mBatch)
	}
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())
	status := queue.getPoolStatus()
	require.EqualValues(t, 10, status.ConfigTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInPending)

	// move 10 config batches in queue to queue, no ok
	queue.moveBatchesToQueue(confBatchIds[:10], nil)
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())
	status = queue.getPoolStatus()
	require.EqualValues(t, 10, status.ConfigTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInPending)

	// move 10 common batches in pendingCache to queue, ok
	queue.moveBatchesToQueue(commBatchIds[10:20], nil)
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 40, queue.commonTxCount())
	status = queue.getPoolStatus()
	require.EqualValues(t, 10, status.ConfigTxNumInQueue)
	require.EqualValues(t, 40, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 0, status.CommonTxNumInPending)

	// move 10 config batches in pendingCache to queue, ok
	queue.moveBatchesToQueue(confBatchIds[10:20], nil)
	require.EqualValues(t, 20, queue.configTxCount())
	require.EqualValues(t, 40, queue.commonTxCount())
	status = queue.getPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxNumInQueue)
	require.EqualValues(t, 40, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	require.EqualValues(t, 0, status.CommonTxNumInPending)

	// move 10 batches no in pool to queue, no ok
	queue.moveBatchesToQueue(confBatchIds[20:30], nil)
	require.EqualValues(t, 20, queue.configTxCount())
	require.EqualValues(t, 40, queue.commonTxCount())
	status = queue.getPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxNumInQueue)
	require.EqualValues(t, 40, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
}

func Test_moveBatchesToPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config and 10 common batches
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[:10] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.addCommonTxBatches(commMBatches[:10])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// add 10 config batches to pendingCache
	queue.moveBatchesToPending(confBatchIds[:10])
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	queue.moveBatchesToPending(confBatchIds[:10])
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// add 10 common batches to pendingCache
	queue.moveBatchesToPending(commBatchIds[:10])
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 0, queue.commonTxCount())

	queue.moveBatchesToPending(commBatchIds[:10])
	require.EqualValues(t, 0, queue.configTxCount())
	require.EqualValues(t, 0, queue.commonTxCount())
}

func Test_removeBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 40, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config and 10 common batches to queue and pendingCache
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[:20] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.moveBatchesToPending(confBatchIds[10:20])
	queue.addCommonTxBatches(commMBatches[:20])
	queue.moveBatchesToPending(commBatchIds[10:20])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// get pool status
	status := queue.getPoolStatus()
	require.EqualValues(t, 10, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInPending)

	// remove 20 config batches
	queue.removeBatches(confBatchIds[:5])
	status = queue.getPoolStatus()
	require.EqualValues(t, 5, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInPending)
	queue.removeBatches(confBatchIds[5:])
	status = queue.getPoolStatus()
	require.EqualValues(t, 0, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInPending)

	// remove 20 common batches
	queue.removeBatches(commBatchIds[:5])
	status = queue.getPoolStatus()
	require.EqualValues(t, 0, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInPending)
	queue.removeBatches(commBatchIds[5:])
	status = queue.getPoolStatus()
	require.EqualValues(t, 0, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	require.EqualValues(t, 0, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
}

func Test_txBatchExist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 40, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config and 10 common batches to queue and pendingCache
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[:20] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.moveBatchesToPending(confBatchIds[10:20])
	queue.addCommonTxBatches(commMBatches[:20])
	queue.moveBatchesToPending(commBatchIds[10:20])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// config batch exit in pool
	require.EqualValues(t, true, queue.txBatchExist(confMBatches[0].batch))
	require.EqualValues(t, true, queue.txBatchExist(confMBatches[10].batch))
	require.EqualValues(t, false, queue.txBatchExist(confMBatches[20].batch))

	// common batch exit in pool
	require.EqualValues(t, true, queue.txBatchExist(commMBatches[0].batch))
	require.EqualValues(t, true, queue.txBatchExist(commMBatches[10].batch))
	require.EqualValues(t, false, queue.txBatchExist(commMBatches[20].batch))
}

func Test_isFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 40, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 20 config and 20 common batches to queue
	confMBatches := generateMBatches(30, true, 1)
	commMBatches := generateMBatches(30, false, 2)
	for _, mBatch := range confMBatches[:20] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.addCommonTxBatches(commMBatches[:20])
	require.EqualValues(t, 20, queue.configTxCount())
	require.EqualValues(t, 40, queue.commonTxCount())

	// config batch pool
	require.EqualValues(t, true, queue.isFull(generateTxs(1, true)[0]))
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	queue.moveBatchesToPending(confBatchIds[10:20])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, false, queue.isFull(generateTxs(1, true)[0]))

	// common batch pool
	require.EqualValues(t, true, queue.isFull(generateTxs(1, false)[0]))
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	queue.moveBatchesToPending(commBatchIds[10:20])
	require.EqualValues(t, 20, queue.commonTxCount())
	require.EqualValues(t, false, queue.isFull(generateTxs(1, false)[0]))
}

func Test_getPoolStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 40, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config and 10 common batches to queue and pendingCache
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[:20] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.moveBatchesToPending(confBatchIds[10:20])
	queue.addCommonTxBatches(commMBatches[:20])
	queue.moveBatchesToPending(commBatchIds[10:20])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())

	// get pool status
	status := queue.getPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxPoolSize)
	require.EqualValues(t, 40, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInPending)
}

func Test_getTxsByTxTypeAndStage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 40, 2, false, "")
	// create batchQueue
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), newMockLogger())

	// add 10 config and 10 common batches to queue and pendingCache
	confMBatches := generateMBatches(30, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	commMBatches := generateMBatches(30, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	for _, mBatch := range confMBatches[:20] {
		queue.addConfigTxBatch(mBatch)
	}
	queue.moveBatchesToPending(confBatchIds[10:20])
	queue.addCommonTxBatches(commMBatches[:20])
	queue.moveBatchesToPending(commBatchIds[10:20])
	require.EqualValues(t, 10, queue.configTxCount())
	require.EqualValues(t, 20, queue.commonTxCount())
	status := queue.getPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxPoolSize)
	require.EqualValues(t, 40, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 20, status.CommonTxNumInPending)

	// get config txs
	txs, txIds := queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 10, len(txs))
	require.EqualValues(t, 10, len(txIds))
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 10, len(txs))
	require.EqualValues(t, 10, len(txIds))
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 20, len(txs))
	require.EqualValues(t, 20, len(txIds))

	// get common txs
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 20, len(txs))
	require.EqualValues(t, 20, len(txIds))
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 20, len(txs))
	require.EqualValues(t, 20, len(txIds))
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 40, len(txs))
	require.EqualValues(t, 40, len(txIds))

	// get all txs
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 30, len(txs))
	require.EqualValues(t, 30, len(txIds))
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 30, len(txs))
	require.EqualValues(t, 30, len(txIds))
	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 60, len(txs))
	require.EqualValues(t, 60, len(txIds))
}
