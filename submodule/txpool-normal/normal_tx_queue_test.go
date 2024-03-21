/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"sync/atomic"
	"testing"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// Test_addConfigTx test addConfigTx
func Test_addConfigTx(t *testing.T) {
	// 0.init source
	confTxs, _ := generateMemTxs(10, 0, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	queue := newTxQueue(10, chainConf, store.store, log)
	// 1. put config tx to configQueue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 2. put repeat config tx failed
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
}

// Test_addCommonTx test addCommonTx
func Test_addCommonTx(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	mtxs, _, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(mtxs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common tx to commonQueue
	queue.addCommonTx(mtxs[0], disp.DistTx(mtxs[0].getTxId(), queueNum))
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.commonTxCountAtomic))
	// 2. put repeat common tx failed
	queue.addCommonTx(mtxs[0], disp.DistTx(mtxs[0].getTxId(), queueNum))
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.commonTxCountAtomic))
}

// Test_addCommonTxs test addCommonTxs
func Test_addCommonTxs(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common txs to commonQueue
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. put repeat common txs failed
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
}

// Test_fetchTxBatch test fetchTxBatch
func Test_fetchTxBatch(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	confTxs, _ := generateMemTxs(10, 0, true)
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common txs to commonQueue
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. put config tx to configQueue
	queue.addConfigTx(confTxs[0])
	// 3. fetch config tx batch
	fetchedTxs := queue.fetchTxBatch()
	require.EqualValues(t, 1, len(fetchedTxs))
	require.True(t, isConfigTx(fetchedTxs[0].getTx(), chainConf))
	// 4. fetch common tx batch
	fetchedTxs = queue.fetchTxBatch()
	require.EqualValues(t, queueNum*perNumInQueue, len(fetchedTxs))
	require.False(t, isConfigTx(fetchedTxs[0].getTx(), chainConf))
}

// Test_getCommonTxsByTxIds test getCommonTxsByTxIds
func Test_getCommonTxsByTxIds(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	_, txs, txIds, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common txs to commonQueue
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. get common txs success
	txIdsTable := disp.DistTxIds(txIds, queueNum)
	mtxsRet, txsMis := queue.getCommonTxsByTxIds(txIdsTable)
	require.EqualValues(t, queueNum*perNumInQueue, len(mtxsRet))
	require.EqualValues(t, 0, len(txsMis))
}

// Test_addConfigTxToPendingCache test addConfigTxToPendingCache
func Test_addConfigTxToPendingCache(t *testing.T) {
	// 0.init source
	confTxs, _ := generateMemTxs(10, 0, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	queue := newTxQueue(10, chainConf, store.store, log)
	// 1. put config tx to configQueue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 2. add config tx to pending
	queue.addConfigTxToPendingCache(confTxs[0])
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	queueLen, pendingLen := queue.configQueue.Size()
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 1, pendingLen)
	// 3. add a second config tx to pending
	queue.addConfigTxToPendingCache(confTxs[1])
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	queueLen, pendingLen = queue.configQueue.Size()
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 2, pendingLen)
}

// Test_addCommonTxsToPendingCache test addCommonTxsToPendingCache
func Test_addCommonTxsToPendingCache(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common txs to commonQueue
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. put common txs to pending
	queue.addCommonTxsToPendingCache(mtxsTable)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, perNumInQueue, pendingLen)
	}
}

// Test_retryConfigTxs test retryConfigTxs
func Test_retryConfigTxs(t *testing.T) {
	// 0.init source
	confTxs, _ := generateMemTxs(10, 0, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	queue := newTxQueue(10, chainConf, store.store, log)
	// 1. put config tx to configQueue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 2. retry tx
	queue.retryConfigTxs([]*memTx{confTxs[0]})
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 3. add config tx to pending
	queue.addConfigTxToPendingCache(confTxs[0])
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	queueLen, pendingLen := queue.configQueue.Size()
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 1, pendingLen)
	// 4. retry tx again
	queue.retryConfigTxs([]*memTx{confTxs[0]})
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	queueLen, pendingLen = queue.configQueue.Size()
	require.EqualValues(t, 1, queueLen)
	require.EqualValues(t, 1, pendingLen)
	// 5. add a second config tx to pending
	queue.addConfigTxToPendingCache(confTxs[1])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	queueLen, pendingLen = queue.configQueue.Size()
	require.EqualValues(t, 1, queueLen)
	require.EqualValues(t, 2, pendingLen)
	// 6. retry tx again
	queue.retryConfigTxs([]*memTx{confTxs[1]})
	require.EqualValues(t, int32(2), atomic.LoadInt32(queue.configTxCountAtomic))
	queueLen, pendingLen = queue.configQueue.Size()
	require.EqualValues(t, 2, queueLen)
	require.EqualValues(t, 2, pendingLen)
}

// Test_retryCommonTxs test retryCommonTxs
func Test_retryCommonTxs(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common txs to commonQueue
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. retry common txs
	queue.retryCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 3. put common txs to pending
	queue.addCommonTxsToPendingCache(mtxsTable)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, perNumInQueue, pendingLen)
	}
	// 4. retry common txs again
	queue.retryCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, perNumInQueue, pendingLen)
	}
}

// Test_removeConfigTxs test removeConfigTxs
func Test_removeConfigTxs(t *testing.T) {
	// 0.init source
	confTxs, txIds := generateMemTxs(10, 0, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	queue := newTxQueue(10, chainConf, store.store, log)
	// 1. put config tx to configQueue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 2. remove tx in pending
	queue.removeConfigTxs(txIds[:1], true)
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 3. remove tx in queue and pending
	queue.removeConfigTxs(txIds[:1], false)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	// 4. add config tx to pending
	queue.addConfigTxToPendingCache(confTxs[0])
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	queueLen, pendingLen := queue.configQueue.Size()
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 1, pendingLen)
	// 5. remove tx in pending
	queue.removeConfigTxs(txIds[:1], true)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
}

// Test_removeCommonTxs test removeCommonTxs
func Test_removeCommonTxs(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	_, txs, txIds, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common txs to commonQueue
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. remove common txs in pending
	queue.removeCommonTxs(disp.DistTxIds(txIds, queueNum), true)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 3. remove common txs in queue and pending
	queue.removeCommonTxs(disp.DistTxIds(txIds, queueNum), false)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 4. put common txs to pending
	queue.addCommonTxsToPendingCache(mtxsTable)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, perNumInQueue, pendingLen)
	}
	// 5. remove common txs in queue and pending
	queue.removeCommonTxs(disp.DistTxIds(txIds, queueNum), false)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
}

// Test_configTxExists test configTxExists
func Test_configTxExists(t *testing.T) {
	// 0.init source
	confTxs, txIds := generateMemTxs(10, 0, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	queue := newTxQueue(10, chainConf, store.store, log)
	// 1. put config tx to queue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 2. config tx exist in queue
	exist := queue.configTxExists(txIds[0])
	require.True(t, exist)
	// 3. put config tx to pending
	queue.addConfigTx(confTxs[1])
	require.EqualValues(t, int32(2), atomic.LoadInt32(queue.configTxCountAtomic))
	queue.addConfigTxToPendingCache(confTxs[1])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	// 4. config tx exist in pending
	exist = queue.configTxExists(txIds[1])
	require.True(t, exist)
	// 5. config tx no exist
	exist = queue.configTxExists(txIds[2])
	require.False(t, exist)
}

// Test_commonTxExists test commonTxExists
func Test_commonTxExists(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put common txs to commonQueue
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. common tx exist in queue
	txId := txs[0].Payload.TxId
	exist := queue.commonTxExists(txId, disp.DistTx(txId, queueNum))
	require.True(t, exist)
	// 3. put common txs to pending
	queue.addCommonTxsToPendingCache(mtxsTable)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, perNumInQueue, pendingLen)
	}
	// 4. common tx exist in pending
	txId = txs[0].Payload.TxId
	exist = queue.commonTxExists(txId, disp.DistTx(txId, queueNum))
	require.True(t, exist)
	// 5. common tx no exist
	txId = utils.GetRandTxId()
	exist = queue.commonTxExists(txId, disp.DistTx(txId, queueNum))
	require.False(t, exist)
}

// Test_getPoolStatus test getPoolStatus
func Test_getPoolStatus(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	TxPoolConfig = &txPoolConfig{
		MaxConfigTxPoolSize: 10,
		MaxTxPoolSize:       100,
	}
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	confTxs, _ := generateMemTxs(10, 0, true)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put config and common txs to commonQueue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. get pool status
	status := queue.getPoolStatus()
	require.NotNil(t, status)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 80, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	// 3. put config and common txs to pending
	queue.addConfigTxToPendingCache(confTxs[0])
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	queue.addCommonTxsToPendingCache(mtxsTable)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, perNumInQueue, pendingLen)
	}
	// 4. get pool status
	status = queue.getPoolStatus()
	require.NotNil(t, status)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 0, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	require.EqualValues(t, 80, status.CommonTxNumInPending)
}

// Test_getTxsByTxTypeAndStage test getTxsByTxTypeAndStage
func Test_getTxsByTxTypeAndStage(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	TxPoolConfig = &txPoolConfig{
		MaxConfigTxPoolSize: 10,
		MaxTxPoolSize:       100,
	}
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	confTxs, _ := generateMemTxs(10, 0, true)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put config and common txs to commonQueue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. get txs
	txsRet, txIdsRet := queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 1, len(txsRet))
	require.EqualValues(t, 1, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 0, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 80, len(txsRet))
	require.EqualValues(t, 80, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 0, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX|txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE|txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 81, len(txsRet))
	require.EqualValues(t, 81, len(txIdsRet))
	// 3. put config and common txs to pending
	queue.addConfigTxToPendingCache(confTxs[0])
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	queue.addCommonTxsToPendingCache(mtxsTable)
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, 0, queueLen)
		require.EqualValues(t, perNumInQueue, pendingLen)
	}
	// 4. get txs
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 0, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 1, len(txsRet))
	require.EqualValues(t, 1, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 0, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 80, len(txsRet))
	require.EqualValues(t, 80, len(txIdsRet))
	txsRet, txIdsRet = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX|txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE|txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 81, len(txsRet))
	require.EqualValues(t, 81, len(txIdsRet))
}

// Test_getTxByTxId test getTxByTxId
func Test_getTxByTxId(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	TxPoolConfig = &txPoolConfig{
		MaxConfigTxPoolSize: 10,
		MaxTxPoolSize:       100,
	}
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	// generate Dispatcher and txs
	queueNum := 8
	perNumInQueue := 10
	disp := newTxDispatcher(log)
	confTxs, _ := generateMemTxs(10, 0, true)
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// generate queue
	queue := newTxQueue(queueNum, chainConf, store.store, log)
	// 1. put config and common txs to commonQueue
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, 0)
	queue.addCommonTxs(mtxsTable)
	require.EqualValues(t, int32(queueNum*perNumInQueue), atomic.LoadInt32(queue.commonTxCountAtomic))
	for _, commQueue := range queue.commonQueues {
		queueLen, pendingLen := commQueue.Size()
		require.EqualValues(t, perNumInQueue, queueLen)
		require.EqualValues(t, 0, pendingLen)
	}
	// 2. get config tx
	mtx, err := queue.getTxByTxId(confTxs[0].getTxId())
	require.NoError(t, err)
	require.NotNil(t, mtx)
	mtx, err = queue.getTxByTxId(confTxs[1].getTxId())
	require.Error(t, err)
	require.Nil(t, mtx)
	// 2. get common tx
	mtx, err = queue.getTxByTxId(txs[0].Payload.TxId)
	require.NoError(t, err)
	require.NotNil(t, mtx)
	mtx, err = queue.getTxByTxId(utils.GetRandTxId())
	require.Error(t, err)
	require.Nil(t, mtx)
}
