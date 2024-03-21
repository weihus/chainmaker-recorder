/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	commonErrors "chainmaker.org/chainmaker/common/v2/errors"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/localconf/v2"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	netPb "chainmaker.org/chainmaker/pb-go/v2/net"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewTxPoolImpl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	chainConf := newMockChainConf(ctrl, false)
	log := newMockLogger()
	txsMap := make(map[string]*commonPb.Transaction, 100)
	txPool, err := NewTxPoolImpl(testNodeId, "",
		newMockTxFilter(ctrl, txsMap).txFilter, newMockBlockChainStore(ctrl, txsMap).store,
		newMockMessageBus(ctrl), chainConf, nil, newMockAccessControlProvider(ctrl), nil,
		log, false, map[string]interface{}{})
	require.Nil(t, txPool)
	require.EqualError(t, fmt.Errorf("no chainId in create txpool"), err.Error())

	txPool, err = NewTxPoolImpl(testNodeId, testChainId,
		newMockTxFilter(ctrl, txsMap).txFilter, newMockBlockChainStore(ctrl, txsMap).store,
		newMockMessageBus(ctrl), chainConf, nil, newMockAccessControlProvider(ctrl), nil,
		log, false, map[string]interface{}{})
	require.NotNil(t, txPool)
	require.NoError(t, err)
}

type testPool struct {
	txPool protocol.TxPool
	extTxs map[string]*commonPb.Transaction
}

func newTestPool(t *testing.T, txCount uint32) (*testPool, func()) {
	// 创建gomock控制器，用来记录后续操作信息
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// 生成mock实例
	chainConf := newMockChainConf(ctrl, false)
	log := newMockLogger()
	txsMap := make(map[string]*commonPb.Transaction, 100)
	mockFilter := newMockTxFilter(ctrl, txsMap)
	mockStore := newMockBlockChainStore(ctrl, txsMap)
	txPool, _ := NewTxPoolImpl(testNodeId, testChainId, mockFilter.txFilter, mockStore.store,
		newMockMessageBus(ctrl), chainConf, nil, newMockAccessControlProvider(ctrl), nil, log,
		true, map[string]interface{}{})
	TxPoolConfig.MaxTxPoolSize = txCount
	TxPoolConfig.MaxConfigTxPoolSize = 1000
	TxPoolConfig.CacheFlushTicker = 1
	TxPoolConfig.CacheThresholdCount = 1
	TxPoolConfig.IsDumpTxsInQueue = true
	// 默认存储路径
	localconf.ChainMakerConfig.StorageConfig["store_path"] = "./data"
	// 默认删除dump_tx_wal
	dumpDir := path.Join(localconf.ChainMakerConfig.GetStorePath(), testChainId, dumpDir)
	os.RemoveAll(dumpDir)
	_ = txPool.Start()
	return &testPool{
			txPool: txPool,
			extTxs: txsMap,
		}, func() {
			ctrl.Finish()
			err := txPool.Stop()
			if err != nil {
				t.Log(err.Error())
			}
		}
}

func Test_OnMessage(t *testing.T) {
	// 0.init source
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)

	txs := generateTxs(100, false)
	txIds := getTxIdsByTxs(txs)

	// 1. test TxPoolMsgType_BATCH_TX
	txBz, err1 := proto.Marshal(txs[0])
	require.NoError(t, err1)
	imlPool.OnMessage(newMessage(txBz, txpoolPb.TxPoolMsgType_SINGLE_TX))

	// 2. test TxPoolMsgType_RECOVER_REQ
	txRecoverReq := &txpoolPb.TxRecoverRequest{
		NodeId: "node1",
		Height: 9,
		TxIds:  txIds,
	}
	txRecoverReqBz, err2 := proto.Marshal(txRecoverReq)
	require.NoError(t, err2)
	imlPool.OnMessage(newMessage(txRecoverReqBz, txpoolPb.TxPoolMsgType_RECOVER_REQ))

	// 3. test TxPoolMsgType_RECOVER_RESP
	txRecoverRes := &txpoolPb.TxRecoverResponse{
		NodeId: "node1",
		Height: 9,
		Txs:    txs,
	}
	txRecoverResBz, err3 := proto.Marshal(txRecoverRes)
	require.NoError(t, err3)
	imlPool.OnMessage(newMessage(txRecoverResBz, txpoolPb.TxPoolMsgType_RECOVER_RESP))
}

func newMessage(poolMsgBz []byte, msgType txpoolPb.TxPoolMsgType) *msgbus.Message {
	txPoolMsg := &txpoolPb.TxPoolMsg{
		Type:    msgType,
		Payload: poolMsgBz,
	}
	txPoolMsgBz, _ := proto.Marshal(txPoolMsg)
	netMsg := &netPb.NetMsg{
		Payload: txPoolMsgBz,
		Type:    netPb.NetMsg_TX,
	}
	return &msgbus.Message{
		Topic:   msgbus.RecvTxPoolMsg,
		Payload: netMsg,
	}
}

func TestTxPoolImpl_AddTx(t *testing.T) {
	commonTxs := generateTxs(30, false)
	configTxs := generateTxs(30, true)
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	TxPoolConfig.MaxConfigTxPoolSize = 10

	// 2. add config txs
	for _, tx := range configTxs[:10] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}

	// 1. add common txs
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}

	// 3. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)

	// 4. check pool is full
	fmt.Printf("------------------------------ test txpool is full ------------------------------\n")
	require.EqualError(t, commonErrors.ErrTxPoolLimit, txPool.AddTx(commonTxs[20], protocol.RPC).Error())
	require.EqualError(t, commonErrors.ErrTxPoolLimit, txPool.AddTx(configTxs[10], protocol.P2P).Error())
	fmt.Printf("------------------------------ test txpool is full ------------------------------\n")
	imPool := txPool.(*txPoolImpl)
	require.EqualValues(t, 20, imPool.queue.commonTxsCount())
	require.EqualValues(t, 10, imPool.queue.configTxsCount())

	// 5. repeat add same txs
	TxPoolConfig.MaxTxPoolSize = 30
	TxPoolConfig.MaxConfigTxPoolSize = 11
	require.EqualError(t, commonErrors.ErrTxIdExist, txPool.AddTx(commonTxs[0], protocol.RPC).Error())
	require.EqualError(t, commonErrors.ErrTxIdExist, txPool.AddTx(configTxs[0], protocol.RPC).Error())

	// 6. add txs to blockchain
	for _, tx := range commonTxs[20:25] {
		testPool.extTxs[tx.Payload.TxId] = tx
	}

	// 7. add txs[20:25] failed due to txs exist in blockchain
	for _, tx := range commonTxs[20:25] {
		// here because not check existence in blockchain, The check will
		// only be performed when the flush transaction reaches the db
		require.EqualValues(t, commonErrors.ErrTxIdExistDB, txPool.AddTx(tx, protocol.RPC))
	}
	//  sleep to wait for the flush
	time.Sleep(time.Second)
	require.EqualValues(t, 20, imPool.queue.commonTxsCount())
}

func TestFlushOrAddTxsToCache(t *testing.T) {
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	rpcConfigTxs, _, _ := generateTxsBySource(10, true)
	rpcCommonTxs, p2pCommonTxs, _ := generateTxsBySource(10, false)
	imlPool := txPool.(*txPoolImpl)

	// 1. add config txs
	imlPool.flushOrAddTxsToCache(rpcConfigTxs)
	require.EqualValues(t, len(rpcConfigTxs.mtxs), imlPool.queue.configTxsCount())

	// 2. repeat add config txs
	imlPool.flushOrAddTxsToCache(rpcConfigTxs)
	require.EqualValues(t, len(rpcConfigTxs.mtxs), imlPool.queue.configTxsCount())

	// 3. add common txs
	imlPool.flushOrAddTxsToCache(rpcCommonTxs)
	require.EqualValues(t, len(rpcCommonTxs.mtxs), imlPool.queue.commonTxsCount())
	require.EqualValues(t, 0, imlPool.cache.totalCount)

	// 4. repeat add common txs due to not flush, so size will be *2
	imlPool.flushOrAddTxsToCache(rpcCommonTxs)
	require.EqualValues(t, len(rpcCommonTxs.mtxs), imlPool.queue.commonTxsCount())
	require.EqualValues(t, 0, imlPool.cache.totalCount)

	// 5. modify flushThreshold in cache and add common txs to queue
	imlPool.cache.flushThreshold = 0
	p2pCommonTxs.source = protocol.RPC
	imlPool.flushOrAddTxsToCache(p2pCommonTxs)
	fmt.Println(imlPool.cache.isFlushByTxCount(p2pCommonTxs), imlPool.queue.configTxsCount(), imlPool.queue.commonTxsCount())
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())
}

func TestTxPoolImpl_FetchTxs(t *testing.T) {
	testPool, fn := newTestPool(t, 2000)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	commonTxs := generateTxs(100, false)

	// 1. add common txs
	for _, tx := range commonTxs[:50] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}

	// 2. sleep to wait txs flush
	time.Sleep(time.Millisecond * 100)
	txsInPool := txPool.FetchTxs(99)
	require.EqualValues(t, commonTxs[:50], txsInPool)
	require.EqualValues(t, 0, imlPool.queue.commonTxsCount())
}

func TestTxPoolImpl_GetTxsByTxIds(t *testing.T) {
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	commonTxs := generateTxs(50, false)
	confTxs := generateTxs(50, true)

	// 1. add common txs
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())

	// 2. check txs[:20] existence
	txsInPool, txsMis := txPool.GetTxsByTxIds(getTxIdsByTxs(commonTxs[:20]))
	require.EqualValues(t, 0, len(txsMis))
	for _, tx := range commonTxs[:20] {
		require.EqualValues(t, tx, txsInPool[tx.Payload.TxId])
	}

	// 3. check txs[20:50] not existence
	txsInPool, txsMis = txPool.GetTxsByTxIds(getTxIdsByTxs(commonTxs[20:]))
	require.EqualValues(t, 30, len(txsMis))
	for _, tx := range commonTxs[20:50] {
		require.Nil(t, txsInPool[tx.Payload.TxId])
	}

	// 4. add txs[20:30] to pendingCache
	for _, tx := range commonTxs[20:30] {
		imlPool.queue.commonTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}

	// 5. check txs[:20] existence
	txsInPool, txsMis = txPool.GetTxsByTxIds(getTxIdsByTxs(commonTxs[20:30]))
	require.EqualValues(t, 0, len(txsMis))
	for _, tx := range commonTxs[20:30] {
		require.EqualValues(t, tx, txsInPool[tx.Payload.TxId])
	}

	// 6.put and get config tx
	require.NoError(t, txPool.AddTx(confTxs[0], protocol.RPC))
	time.Sleep(time.Millisecond * 1500)
	txsInPool, txsMis = txPool.GetTxsByTxIds(getTxIdsByTxs(confTxs[:1]))
	require.EqualValues(t, 1, len(txsInPool))
	require.EqualValues(t, 0, len(txsMis))
}

func TestTxPoolImpl_GetAllTxsByTxIds(t *testing.T) {
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	commonTxs := generateTxs(50, false)

	// 1. add common txs
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())

	// 2. check txs[:20] existence
	txsInPool, err := txPool.GetAllTxsByTxIds(getTxIdsByTxs(commonTxs[:20]), "node1", 99, 200)
	require.NoError(t, err)
	for _, tx := range commonTxs[:20] {
		require.EqualValues(t, tx, txsInPool[tx.Payload.TxId])
	}

	// 3. check txs[20:50] not existence
	txsInPool, err = txPool.GetAllTxsByTxIds(getTxIdsByTxs(commonTxs[20:]), "node1", 99, 200)
	require.Error(t, err)
	require.Nil(t, txsInPool)

	// 4. add txs[20:30] to pendingCache
	for _, tx := range commonTxs[20:30] {
		imlPool.queue.commonTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}

	// 5. check txs[:20] existence
	txsInPool, err = txPool.GetAllTxsByTxIds(getTxIdsByTxs(commonTxs[20:30]), "node1", 99, 200)
	require.NoError(t, err)
	for _, tx := range commonTxs[20:30] {
		require.EqualValues(t, tx, txsInPool[tx.Payload.TxId])
	}
}

func TestTxPoolImpl_AddTxsToPendingCache(t *testing.T) {
	testPool, fn := newTestPool(t, 200)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	commonTxs := generateTxs(50, false)

	// 1. add common txs
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	// wait time to flush txs to queue，execute in order by adding txs to queue and adding txs to cache
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())
	// 1.1 add txs[0:20] to pending cache
	txPool.AddTxsToPendingCache(commonTxs[:20], 99)
	require.EqualValues(t, 0, imlPool.queue.commonTxsCount())

	// 2. add common txs
	for _, tx := range commonTxs[20:45] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	// 2.1 add txs[20:40] to pending cache
	txPool.AddTxsToPendingCache(commonTxs[20:40], 100)
	// wait time to flush txs to queue with failed due to txs has exist in pending cache，
	// parallel execution by adding txs to queue and adding txs to pending cache
	time.Sleep(time.Second * 3)
	require.EqualValues(t, 5, imlPool.queue.commonTxsCount())

	// 3. only add txs to pending cache
	txPool.AddTxsToPendingCache(commonTxs[45:], 101)
	require.EqualValues(t, 5, imlPool.queue.commonTxsCount())
}

func TestTxPoolImpl_RetryAndRemoveTxs(t *testing.T) {
	testPool, fn := newTestPool(t, 2000)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	commonTxs := generateTxs(100, false)
	configTxs := generateTxs(100, true)

	// 1. add common txs
	for _, tx := range commonTxs[:50] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 100)
	require.EqualValues(t, 50, imlPool.queue.commonTxsCount())

	// 2. retry nil and remove txs[50:60]
	txPool.RetryAndRemoveTxs(nil, commonTxs[50:60])
	require.EqualValues(t, 50, imlPool.queue.commonTxsCount())

	// 3. retry nil and remove txs[0:50]
	txPool.RetryAndRemoveTxs(nil, commonTxs[:50])
	require.EqualValues(t, 0, imlPool.queue.commonTxsCount())

	// 4. retry txs[0:50] and remove txs[0:50]
	txPool.RetryAndRemoveTxs(commonTxs[:50], commonTxs[:50])
	require.EqualValues(t, 0, imlPool.queue.commonTxsCount())

	// 5. retry txs[0:80] and remove txs[0:50]
	txPool.RetryAndRemoveTxs(commonTxs[:80], commonTxs[:50])
	require.EqualValues(t, 30, imlPool.queue.commonTxsCount())
	txsInPool, _ := txPool.GetTxsByTxIds(getTxIdsByTxs(commonTxs[50:80]))
	require.EqualValues(t, 30, len(txsInPool))

	// 6. Add txs[:50] to pendingCache, and retry txs[:50] and delRetry = true
	for _, tx := range commonTxs[:50] {
		imlPool.queue.commonTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 999})
	}
	txPool.RetryAndRemoveTxs(commonTxs[:50], nil)
	require.EqualValues(t, 80, imlPool.queue.commonTxsCount())

	// 7. add config txs
	for _, tx := range configTxs[:50] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 100)
	require.EqualValues(t, 50, imlPool.queue.configTxsCount())
	// 8. retry txs[:80] and remove txs[:50]
	txPool.RetryAndRemoveTxs(configTxs[:80], configTxs[:50])
	require.EqualValues(t, 30, imlPool.queue.configTxsCount())
}

func TestTxPoolImpl_TxExists(t *testing.T) {
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	confTxs := generateTxs(50, true)
	commonTxs := generateTxs(50, false)

	// 1. add 20 txs to the queue of configTxQueue and commonTxQueue
	for _, tx := range confTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.configTxsCount())
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())

	// 2. add txs[20:30] to the pendingCache of configTxQueue and commonTxQueue
	for _, tx := range confTxs[20:30] {
		imlPool.queue.configTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}
	for _, tx := range commonTxs[20:30] {
		imlPool.queue.commonTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}

	// 3. get pool status
	status := txPool.GetPoolStatus()
	require.EqualValues(t, 1000, status.ConfigTxPoolSize)
	require.EqualValues(t, 20, status.CommonTxPoolSize)
	require.EqualValues(t, 20, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.CommonTxNumInPending)

	// 4. tx exists in the queue of configTxQueue
	require.True(t, imlPool.TxExists(confTxs[10]))

	// 5. tx exists in the pendingCache of configTxQueue
	require.True(t, imlPool.TxExists(confTxs[25]))

	// 6. tx exists in the queue of commonTxQueue
	require.True(t, imlPool.TxExists(commonTxs[10]))

	// 7. tx exists in the pendingCache of commonTxQueue
	require.True(t, imlPool.TxExists(commonTxs[25]))
}

func TestTxPoolImpl_GetPoolStatus(t *testing.T) {
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	confTxs := generateTxs(50, true)
	commonTxs := generateTxs(50, false)

	// 1. add 20 txs to the queue of configTxQueue and commonTxQueue
	for _, tx := range confTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.configTxsCount())
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())

	// 2. add txs[20:30] to the pendingCache of configTxQueue and commonTxQueue
	for _, tx := range confTxs[20:30] {
		imlPool.queue.configTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}
	for _, tx := range commonTxs[20:30] {
		imlPool.queue.commonTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}

	// 3. get pool status
	status := txPool.GetPoolStatus()
	require.EqualValues(t, 1000, status.ConfigTxPoolSize)
	require.EqualValues(t, 20, status.CommonTxPoolSize)
	require.EqualValues(t, 20, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.CommonTxNumInPending)
}

func TestTxPoolImpl_GetTxIdsByTypeAndStage(t *testing.T) {
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	confTxs := generateTxs(50, true)
	commonTxs := generateTxs(50, false)

	// 1. add 20 txs to the queue of configTxQueue and commonTxQueue
	for _, tx := range confTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.configTxsCount())
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())

	// 2. add txs[20:30] to the pendingCache of configTxQueue and commonTxQueue
	for _, tx := range confTxs[20:30] {
		imlPool.queue.configTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}
	for _, tx := range commonTxs[20:30] {
		imlPool.queue.commonTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}

	// 3. get pool status
	status := txPool.GetPoolStatus()
	require.EqualValues(t, 1000, status.ConfigTxPoolSize)
	require.EqualValues(t, 20, status.CommonTxPoolSize)
	require.EqualValues(t, 20, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 20, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.CommonTxNumInPending)

	// 4. get txs by type and stage
	txIds := txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 20, len(txIds))
	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 10, len(txIds))
	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 30, len(txIds))

	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 20, len(txIds))
	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 10, len(txIds))
	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 30, len(txIds))

	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 40, len(txIds))
	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 20, len(txIds))
	txIds = txPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 60, len(txIds))
}

func TestTxPoolImpl_GetTxsInPoolByTxIds(t *testing.T) {
	testPool, fn := newTestPool(t, 20)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	commonTxs := generateTxs(50, false)

	// 1. add common txs
	for _, tx := range commonTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.commonTxsCount())

	// 2. check txs[:20] existence
	txsRet, txsMis, _ := txPool.GetTxsInPoolByTxIds(getTxIdsByTxs(commonTxs[:20]))
	require.EqualValues(t, 20, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))

	// 3. check txs[20:50] not existence
	txsRet, txsMis, _ = txPool.GetTxsInPoolByTxIds(getTxIdsByTxs(commonTxs[20:]))
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 30, len(txsMis))

	// 4. add txs[20:30] to pendingCache
	for _, tx := range commonTxs[20:30] {
		imlPool.queue.commonTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}

	// 5. check txs[20:30] existence
	txsRet, txsMis, _ = txPool.GetTxsInPoolByTxIds(getTxIdsByTxs(commonTxs[20:30]))
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
}

func TestTxPoolImpl_DumpTxs(t *testing.T) {
	testPool, fn := newTestPool(t, 100)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*txPoolImpl)
	commonTxs := generateTxs(50, false)
	confTxs := generateTxs(50, true)

	// 1. add common txs to queue, and fetch a batch
	for _, tx := range commonTxs[:50] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 50, imlPool.queue.commonTxsCount())

	mtxs, _ := imlPool.queue.commonTxQueue.Fetch(10, nil)
	require.EqualValues(t, 10, len(mtxs))
	require.EqualValues(t, 40, imlPool.queue.commonTxsCount())
	imlPool.recover.CacheFetchedTxs(0, getTxsByMemTxs(mtxs))
	require.EqualValues(t, 1, len(imlPool.recover.txsCache))
	// commonQueue: commonTxs[10:50], recover:0->commonTxs[:10], commonPending:commonTxs[:10]

	// 2. add config txs to queue and pending
	for _, tx := range confTxs[20:30] {
		imlPool.queue.configTxQueue.pendingCache.Store(tx.Payload.TxId, &memTx{tx: tx, dbHeight: 99})
	}
	for _, tx := range confTxs[:20] {
		require.NoError(t, txPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Millisecond * 1500)
	require.EqualValues(t, 20, imlPool.queue.configTxsCount())
	// configQueue: confTxs[:20], configPending:confTxs[20:30]

	// 3. dump txs
	fmt.Println("---------------------------- test dump ----------------------------")
	err := imlPool.dumpTxs()
	require.NoError(t, err)
	fmt.Println("---------------------------- test replay ----------------------------")
	err = imlPool.replayTxs()
	require.NoError(t, err)
	fmt.Println("---------------------------- test finish ----------------------------")
}

//func TestPoolImplConcurrencyInvoke(t *testing.T) {
//	testPool, fn := newTestPool(t, 2000000)
//	txPool := testPool.txPool
//	defer fn()
//	imlPool := txPool.(*txPoolImpl)
//	commonTxs := generateTxs(500000, false)
//	txIds := make([]string, 0, len(commonTxs))
//	for _, tx := range commonTxs {
//		txIds = append(txIds, tx.Payload.TxId)
//	}
//
//	// 1. Concurrent Adding Transactions to txPool
//	addBegin := utils.CurrentTimeMillisSeconds()
//	workerNum := 100
//	peerWorkerTxNum := len(commonTxs) / workerNum
//	wg := sync.WaitGroup{}
//	for i := 0; i < workerNum; i++ {
//		wg.Add(1)
//		go func(i int, mtxs []*commonPb.Transaction) {
//			for _, tx := range mtxs {
//				err := txPool.AddTx(tx, protocol.RPC)
//				if err != nil {
//					t.Log(err.Error())
//					//t.Fail()
//				}
//			}
//			wg.Done()
//			imlPool.log.Debugf("add mtxs done")
//		}(i, commonTxs[i*peerWorkerTxNum:(i+1)*peerWorkerTxNum])
//	}
//
//	// 2. Simulate the logic for generating blocks
//	fetchTimes := make([]int64, 0, 100)
//	go func() {
//		height := uint64(100)
//		fetchTicker := time.NewTicker(time.Millisecond * 100)
//		fetchTimer := time.NewTimer(2 * time.Minute)
//		defer func() {
//			fetchTimer.Stop()
//			fetchTicker.Stop()
//		}()
//
//	Loop:
//		for {
//			select {
//			case <-fetchTicker.C:
//				begin := utils.CurrentTimeMillisSeconds()
//				mtxs := txPool.FetchTxs(height)
//				fetchTimes = append(fetchTimes, utils.CurrentTimeMillisSeconds()-begin)
//				imlPool.log.Debugf("fetch mtxs num: ", len(mtxs))
//			case <-fetchTimer.C:
//				break Loop
//			}
//		}
//		imlPool.log.Debugf("time used: fetch mtxs: %v ", fetchTimes)
//	}()
//
//	// 3. Simulation validates the logic of the block
//	getTimes := make([]int64, 0, 100)
//	go func() {
//		getTicker := time.NewTicker(time.Millisecond * 80)
//		getTimer := time.NewTimer(2 * time.Minute)
//		defer func() {
//			getTimer.Stop()
//			getTicker.Stop()
//		}()
//
//	Loop:
//		for {
//			select {
//			case <-getTicker.C:
//				start, _ := rand.Int(rand.Reader, new(big.Int).SetInt64(int64((len(txIds) - 1000))))
//				begin := utils.CurrentTimeMillisSeconds()
//				getTxs, _ := txPool.GetTxsByTxIds(txIds[int(start.Int64()) : int(start.Int64())+1000])
//				getTimes = append(getTimes, utils.CurrentTimeMillisSeconds()-begin)
//				imlPool.log.Debugf("get mtxs num: ", len(getTxs))
//			case <-getTimer.C:
//				break Loop
//			}
//		}
//		imlPool.log.Debugf("time used: get mtxs: %v ", getTimes)
//	}()
//
//	wg.Wait()
//	addEnd := utils.CurrentTimeMillisSeconds()
//	imlPool.log.Debugf("time used: add mtxs: %d, txPool state: %s\n", addEnd-addBegin, imlPool.queue.status())
//}
