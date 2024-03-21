/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	commonErr "chainmaker.org/chainmaker/common/v2/errors"
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

// testTxPool contains txPool and txs store
type testTxPool struct {
	// txPool
	txPool protocol.TxPool
	// store
	extTxs map[string]*commonPb.Transaction
}

// newTestPool create testTxPool
func newTestPool(ctrl *gomock.Controller, commonPoolSize uint32) (*testTxPool, func()) {
	txsMap := make(map[string]*commonPb.Transaction, 100)
	// create txFilter
	txFilter := newMockTxFilter(ctrl, txsMap)
	// create store
	store := newMockBlockChainStore(ctrl, txsMap)
	// create msgBus
	msgBus := newMockMessageBus(ctrl)
	// create chainConf
	chainConf := newMockChainConf(ctrl, false)
	// create ac
	ac := newMockAccessControlProvider(ctrl)
	// create log
	log := newMockLogger()
	// create pool
	pool, _ := NewNormalPool(testNodeId, testChainId, txFilter.txFilter, store.store,
		msgBus, chainConf, nil, ac, nil, log, false, nil)
	TxPoolConfig.MaxTxPoolSize = commonPoolSize
	TxPoolConfig.MaxConfigTxPoolSize = 10
	TxPoolConfig.IsDumpTxsInQueue = true
	// 默认存储路径
	localconf.ChainMakerConfig.StorageConfig["store_path"] = "./data"
	// 默认删除dump_tx_wal
	os.RemoveAll(path.Join(localconf.ChainMakerConfig.GetStorePath(), testChainId, dumpDir))
	_ = pool.Start()
	return &testTxPool{
			txPool: pool,
			extTxs: txsMap,
		}, func() {
			ctrl.Finish()
			err := pool.Stop()
			if err != nil {
				log.Error(err.Error())
			}
		}
}

// Test_OnMessage test OnMessage
func Test_OnMessage(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool
	// 1. test TxPoolMsgType_SINGLE_TX
	tx, _ := generateTxs(1, false)
	pool.(*normalPool).OnMessage(newMessage(mustMarshal(tx[0]), txpoolPb.TxPoolMsgType_SINGLE_TX))
	// 2. test TxPoolMsgType_BATCH_TX
	txs, txIds := generateTxs(100, false)
	txBatch := &txpoolPb.TxBatch{
		BatchId: "node1_1",
		Txs:     txs,
	}
	pool.(*normalPool).OnMessage(newMessage(mustMarshal(txBatch), txpoolPb.TxPoolMsgType_BATCH_TX))
	// 3. test TxPoolMsgType_RECOVER_REQ
	txRecoverReq := &txpoolPb.TxRecoverRequest{
		NodeId: testNodeId,
		Height: 9,
		TxIds:  txIds,
	}
	pool.(*normalPool).OnMessage(newMessage(mustMarshal(txRecoverReq), txpoolPb.TxPoolMsgType_RECOVER_REQ))
	// 4. test TxPoolMsgType_RECOVER_RESP
	txRecoverRes := &txpoolPb.TxRecoverResponse{
		NodeId: testNodeId,
		Height: 9,
		Txs:    txs,
	}
	pool.(*normalPool).OnMessage(newMessage(mustMarshal(txRecoverRes), txpoolPb.TxPoolMsgType_RECOVER_RESP))
}

// newMessage create a Message
func newMessage(poolMsgBz []byte, msgType txpoolPb.TxPoolMsgType) *msgbus.Message {
	// create txPoolMsg
	txPoolMsg := &txpoolPb.TxPoolMsg{
		Type:    msgType,
		Payload: poolMsgBz,
	}
	// marshal txPoolMsg
	txPoolMsgBz, _ := proto.Marshal(txPoolMsg)
	// create netMsg
	netMsg := &netPb.NetMsg{
		Payload: txPoolMsgBz,
		Type:    netPb.NetMsg_TX,
	}
	// return netMsg
	return &msgbus.Message{
		Topic:   msgbus.RecvTxPoolMsg,
		Payload: netMsg,
	}
}

// Test_AddTx test AddTx
func Test_AddTx(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 10)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(30, false)
	configTxs, _ := generateTxs(20, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 10, pool.queue.configTxCount())
	// 3. check pool is full
	fmt.Printf("------------------------------ test txpool is full ------------------------------\n")
	require.EqualError(t, commonErr.ErrTxPoolLimit, pool.AddTx(configTxs[10], protocol.RPC).Error())
	require.EqualError(t, commonErr.ErrTxPoolLimit, pool.AddTx(commonTxs[10], protocol.P2P).Error())
	fmt.Printf("------------------------------ test txpool is full ------------------------------\n")
	// 5. repeat add same txs
	TxPoolConfig.MaxTxPoolSize = 20
	TxPoolConfig.MaxConfigTxPoolSize = 20
	_ = pool.AddTx(commonTxs[0], protocol.RPC)
	_ = pool.AddTx(configTxs[0], protocol.RPC)
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 10, pool.queue.configTxCount())
	// 6. add txs to blockchain and add txs failed
	for _, tx := range commonTxs[20:30] {
		testPool.extTxs[tx.Payload.TxId] = tx
	}
	for _, tx := range commonTxs[20:30] {
		require.Error(t, commonErr.ErrTxIdExistDB, pool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 10, pool.queue.configTxCount())
}

// Test_FetchTxBatch test FetchTxBatch
func Test_FetchTxBatch(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(100, false)
	configTxs, _ := generateTxs(10, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:1] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 1, pool.queue.configTxCount())
	// 3. fetch config tx first
	fetchedTxs := pool.FetchTxs(99)
	require.EqualValues(t, 1, len(fetchedTxs))
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 0, pool.queue.configTxCount())
	// 4. fetch common txs
	fetchedTxs = pool.FetchTxs(99)
	require.EqualValues(t, 10, len(fetchedTxs))
	require.EqualValues(t, 0, pool.queue.commonTxCount())
	require.EqualValues(t, 0, pool.queue.configTxCount())
}

// Test_GetTxsByTxIds_Pool test GetTxsByTxIds_Pool
func Test_GetTxsByTxIds_Pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, commonTxIds := generateTxs(100, false)
	configTxs, configTxIds := generateTxs(10, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:1] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 1, pool.queue.configTxCount())
	// 3. fetch config tx first
	fetchedTxs := pool.FetchTxs(99)
	require.EqualValues(t, 1, len(fetchedTxs))
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 0, pool.queue.configTxCount())
	// 4. get config tx
	txsRet, txsMis := pool.GetTxsByTxIds(configTxIds[:1])
	require.EqualValues(t, 1, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
	// 5. fetch common txs
	fetchedTxs = pool.FetchTxs(99)
	require.EqualValues(t, 10, len(fetchedTxs))
	require.EqualValues(t, 0, pool.queue.commonTxCount())
	require.EqualValues(t, 0, pool.queue.configTxCount())
	// 6. get common txs
	txsRet, txsMis = pool.GetTxsByTxIds(commonTxIds[:10])
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
	// 7. add common txs to pool
	for _, tx := range commonTxs[10:20] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 8. get common txs
	time.Sleep(time.Second)
	txsRet, txsMis = pool.GetTxsByTxIds(commonTxIds[10:20])
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
}

// Test_GetAllTxsByTxIds_Pool test GetAllTxsByTxIds_Pool
func Test_GetAllTxsByTxIds_Pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, commonTxIds := generateTxs(100, false)
	configTxs, configTxIds := generateTxs(10, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:1] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 1, pool.queue.configTxCount())
	// 3. fetch config tx first
	fetchedTxs := pool.FetchTxs(99)
	require.EqualValues(t, 1, len(fetchedTxs))
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 0, pool.queue.configTxCount())
	// 4. get config tx
	txsRet, err := pool.GetAllTxsByTxIds(configTxIds[:1], testNodeId, 99, 100)
	require.EqualValues(t, 1, len(txsRet))
	require.NoError(t, err)
	// 5. fetch common txs
	fetchedTxs = pool.FetchTxs(99)
	require.EqualValues(t, 10, len(fetchedTxs))
	require.EqualValues(t, 0, pool.queue.commonTxCount())
	require.EqualValues(t, 0, pool.queue.configTxCount())
	// 6. get common txs
	txsRet, err = pool.GetAllTxsByTxIds(commonTxIds[:10], testNodeId, 99, 100)
	require.EqualValues(t, 10, len(txsRet))
	require.NoError(t, err)
	// 7. add common txs to pool
	for _, tx := range commonTxs[10:20] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 8. get common txs success
	txsRet, err = pool.GetAllTxsByTxIds(commonTxIds[10:20], testNodeId, 99, 100)
	require.EqualValues(t, 10, len(txsRet))
	require.NoError(t, err)
	// 9. get common txs failed
	txsRet, err = pool.GetAllTxsByTxIds(commonTxIds[10:30], testNodeId, 99, 100)
	require.Nil(t, txsRet)
	require.Error(t, err)
}

// Test_AddTxsToPendingCache_Pool test AddTxsToPendingCache
func Test_AddTxsToPendingCache_Pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(100, false)
	configTxs, _ := generateTxs(10, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:1] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 1, pool.queue.configTxCount())
	// 3. add config tx in queue to pending
	pool.AddTxsToPendingCache(configTxs[:1], 99)
	queueLen, pendLen := pool.queue.configQueue.Size()
	require.EqualValues(t, 0, pool.queue.configTxCount())
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 1, pendLen)
	// 4. add common txs in queue to pending
	pool.AddTxsToPendingCache(commonTxs[:10], 100)
	require.EqualValues(t, 0, pool.queue.commonTxCount())
	status := pool.GetPoolStatus()
	require.EqualValues(t, 0, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.CommonTxNumInPending)
	// 5. add repeat config txs to pending
	pool.AddTxsToPendingCache(configTxs[:1], 101)
	queueLen, pendLen = pool.queue.configQueue.Size()
	require.EqualValues(t, 0, pool.queue.configTxCount())
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 1, pendLen)
	// 6. add other config txs to pending
	pool.AddTxsToPendingCache(configTxs[1:2], 101)
	queueLen, pendLen = pool.queue.configQueue.Size()
	require.EqualValues(t, 0, pool.queue.configTxCount())
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 2, pendLen)
}

// Test_RetryAndRemoveTxs_Pool test RetryAndRemoveTxs
func Test_RetryAndRemoveTxs_Pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(100, false)
	configTxs, _ := generateTxs(10, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:1] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 1, pool.queue.configTxCount())
	// 3. retry config tx when tx in queue
	pool.RetryAndRemoveTxs(configTxs[:1], nil)
	queueLen, pendLen := pool.queue.configQueue.Size()
	require.EqualValues(t, 1, pool.queue.configTxCount())
	require.EqualValues(t, 1, queueLen)
	require.EqualValues(t, 0, pendLen)
	// 4. retry config tx when tx in pending
	pool.AddTxsToPendingCache(configTxs[:1], 99)
	queueLen, pendLen = pool.queue.configQueue.Size()
	require.EqualValues(t, 0, pool.queue.configTxCount())
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 1, pendLen)
	pool.RetryAndRemoveTxs(configTxs[:1], nil)
	queueLen, pendLen = pool.queue.configQueue.Size()
	require.EqualValues(t, 1, pool.queue.configTxCount())
	require.EqualValues(t, 1, queueLen)
	require.EqualValues(t, 0, pendLen)
	// 5. remove config tx
	pool.RetryAndRemoveTxs(nil, configTxs[:1])
	queueLen, pendLen = pool.queue.configQueue.Size()
	require.EqualValues(t, 0, pool.queue.configTxCount())
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 0, pendLen)
	// 6. retry common txs when txs in queue
	pool.RetryAndRemoveTxs(commonTxs[:10], nil)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	time.Sleep(time.Second)
	status := pool.GetPoolStatus()
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	// 7. retry common txs when txs in pending
	pool.AddTxsToPendingCache(commonTxs[:10], 100)
	require.EqualValues(t, 0, pool.queue.commonTxCount())
	time.Sleep(time.Second)
	status = pool.GetPoolStatus()
	require.EqualValues(t, 0, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.CommonTxNumInPending)
	pool.RetryAndRemoveTxs(commonTxs[:10], nil)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	status = pool.GetPoolStatus()
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	// 8. remove common txs
	pool.RetryAndRemoveTxs(nil, commonTxs[:10])
	require.EqualValues(t, 0, pool.queue.commonTxCount())
	time.Sleep(time.Second)
	status = pool.GetPoolStatus()
	require.EqualValues(t, 0, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
}

// Test_TxExists_pool test TxExists
func Test_TxExists_pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 10)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(30, false)
	configTxs, _ := generateTxs(20, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 10, pool.queue.configTxCount())
	// 3. config tx exist
	require.True(t, pool.TxExists(configTxs[0]))
	// 4. config tx no exist
	require.False(t, pool.TxExists(configTxs[10]))
	// 5. common tx exist
	require.True(t, pool.TxExists(commonTxs[0]))
	// 6. common tx no exist
	require.False(t, pool.TxExists(commonTxs[10]))
}

// Test_GetPoolStatus_Pool test GetPoolStatus
func Test_GetPoolStatus_Pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(100, false)
	configTxs, _ := generateTxs(10, true)
	// 1. get status
	status := pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 0, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	require.EqualValues(t, 0, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	// 2. add common and config txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:1] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Second)
	status = pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	// 3. move config tx to pending
	pool.AddTxsToPendingCache(configTxs[:1], 99)
	time.Sleep(time.Second)
	status = pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	require.EqualValues(t, 0, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	// 4. move common txs to pending
	pool.AddTxsToPendingCache(commonTxs[:10], 99)
	time.Sleep(time.Second)
	status = pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 0, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.CommonTxNumInPending)
	require.EqualValues(t, 0, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
}

// Test_GetTxIdsByTypeAndStage_Pool test GetTxIdsByTypeAndStage
func Test_GetTxIdsByTypeAndStage_Pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(100, false)
	configTxs, _ := generateTxs(10, true)
	// 1. add common and config txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:2] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Second)
	status := pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	require.EqualValues(t, 2, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	// 2. move a half of config and common tx to pending
	pool.AddTxsToPendingCache(configTxs[:1], 99)
	pool.AddTxsToPendingCache(commonTxs[:5], 99)
	time.Sleep(time.Second)
	status = pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 5, status.CommonTxNumInQueue)
	require.EqualValues(t, 5, status.CommonTxNumInPending)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	// 3. get config in queue
	txIds := pool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 1, len(txIds))
	txIds = pool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 1, len(txIds))
	txIds = pool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE|txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 2, len(txIds))
	txIds = pool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 5, len(txIds))
	txIds = pool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 5, len(txIds))
	txIds = pool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE|txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 10, len(txIds))
	txIds = pool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX|txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE|txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 12, len(txIds))
}

// Test_GetTxByTxId_pool test GetTxByTxId
func Test_GetTxByTxId_pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 10)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, commonTxIds := generateTxs(30, false)
	configTxs, configTxIds := generateTxs(20, true)
	// 1. add common and config  txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	// 2. sleep to wait for the txPool to fill up
	time.Sleep(time.Second)
	require.EqualValues(t, 10, pool.queue.commonTxCount())
	require.EqualValues(t, 10, pool.queue.configTxCount())
	// 3. config tx exist
	txsRet, txsMis, _ := pool.GetTxsInPoolByTxIds(configTxIds[:10])
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
	// 4. config tx no exist
	txsRet, txsMis, _ = pool.GetTxsInPoolByTxIds(configTxIds[10:20])
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 10, len(txsMis))
	// 5. common tx exist
	txsRet, txsMis, _ = pool.GetTxsInPoolByTxIds(commonTxIds[:10])
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
	// 6. common tx no exist
	txsRet, txsMis, _ = pool.GetTxsInPoolByTxIds(commonTxIds[10:20])
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 10, len(txsMis))
}

// Test_DumpTxs_Pool test DumpTxs
func Test_DumpTxs_Pool(t *testing.T) {
	// 0.init source
	ctrl := gomock.NewController(t)
	testPool, fn := newTestPool(ctrl, 100)
	defer fn()
	pool := testPool.txPool.(*normalPool)
	commonTxs, _ := generateTxs(100, false)
	configTxs, _ := generateTxs(10, true)
	// 1. add common and config txs to pool
	for _, tx := range commonTxs[:10] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	for _, tx := range configTxs[:2] {
		require.NoError(t, pool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(time.Second)
	status := pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 0, status.CommonTxNumInPending)
	require.EqualValues(t, 2, status.ConfigTxNumInQueue)
	require.EqualValues(t, 0, status.ConfigTxNumInPending)
	// 3. move a half of config and common tx to pending
	pool.AddTxsToPendingCache(configTxs[:1], 99)
	pool.recover.CacheFetchedTxs(99, configTxs[:1])
	pool.AddTxsToPendingCache(commonTxs[:5], 100)
	pool.recover.CacheFetchedTxs(100, commonTxs[:5])
	time.Sleep(time.Second)
	status = pool.GetPoolStatus()
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 5, status.CommonTxNumInQueue)
	require.EqualValues(t, 5, status.CommonTxNumInPending)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	require.EqualValues(t, 2, len(pool.recover.txsCache))
	// 4. dump txs
	fmt.Println("---------------------------- test dump ----------------------------")
	err := pool.dumpTxs()
	require.NoError(t, err)
	fmt.Println("---------------------------- test replay ----------------------------")
	err = pool.replayTxs()
	require.NoError(t, err)
	fmt.Println("---------------------------- test finish ----------------------------")
}
