/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"chainmaker.org/chainmaker/common/v2/crypto"
	"chainmaker.org/chainmaker/common/v2/crypto/hash"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/localconf/v2"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	netPb "chainmaker.org/chainmaker/pb-go/v2/net"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewTxPoolImpl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchTxPool
	chainConf := newMockChainConf(ctrl, 4, false)
	log := newMockLogger()
	txsMap := make(map[string]*commonPb.Transaction, 100)
	mockFilter := newMockTxFilter(ctrl, txsMap)
	mockStore := newMockBlockChainStore(ctrl, txsMap)
	txPool, err := NewBatchTxPool(testNodeId, "", mockFilter.txFilter, mockStore.store, newMockMessageBus(ctrl),
		chainConf, newMockSigningMember(ctrl), newMockAccessControlProvider(ctrl), newMockNetService(ctrl), log, false, map[string]interface{}{})
	require.EqualError(t, fmt.Errorf("no chainId in create txpool"), err.Error())
	require.Nil(t, txPool)

	txPool, err = NewBatchTxPool(testNodeId, testChainId, mockFilter.txFilter, mockStore.store, newMockMessageBus(ctrl),
		chainConf, newMockSigningMember(ctrl), newMockAccessControlProvider(ctrl), newMockNetService(ctrl), log, false, map[string]interface{}{})
	require.NoError(t, err)
	require.NotNil(t, txPool)
}

type testPool struct {
	txPool protocol.TxPool
	extTxs map[string]*commonPb.Transaction
}

func newTestPool(t *testing.T, blockTxCapacity uint32, batchCreateTime int64) (*testPool, func()) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// 生成mock实例
	chainConf := newMockChainConf(ctrl, blockTxCapacity, false)
	log := newMockLogger()
	txsMap := make(map[string]*commonPb.Transaction, 100)
	mockFilter := newMockTxFilter(ctrl, txsMap)
	mockStore := newMockBlockChainStore(ctrl, txsMap)
	txPool, err := NewBatchTxPool(testNodeId, testChainId, mockFilter.txFilter, mockStore.store, newMockMessageBus(ctrl),
		chainConf, newMockSigningMember(ctrl), newMockAccessControlProvider(ctrl), newMockNetService(ctrl), log,
		false, map[string]interface{}{
			"batch_create_timeout": batchCreateTime,
		})
	require.NoError(t, err)
	require.NotNil(t, txPool)

	// 默认存储路径
	localconf.ChainMakerConfig.StorageConfig["store_path"] = "./data"
	// 默认删除dump_tx_wal
	os.RemoveAll(path.Join(localconf.ChainMakerConfig.GetStorePath(), testChainId, dumpDir))
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

func TestBatchTxPool_OnMessage(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add forwarded tx
	fmt.Println("---------------------------- test 1 ----------------------------")
	commonTxs := generateTxs(2, false)
	for _, tx := range commonTxs {
		imlPool.OnMessage(newMessage(mustMarshal(tx), txpoolPb.TxPoolMsgType_SINGLE_TX))
	}
	time.Sleep(2 * time.Second)
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 2. add batch succeed
	fmt.Println("---------------------------- test 2 ----------------------------")
	configTxs := sortTxs(generateTxs(1, true))
	batch := &txpoolPb.TxBatch{
		Txs: configTxs,
	}
	batchHash, _ := hash.GetByStrType(crypto.CRYPTO_ALGO_SHA256, mustMarshal(batch))
	batch.BatchId = generateTimestamp() + cutoutNodeId(testNodeId) + cutoutBatchHash(batchHash)
	batch.Size_ = int32(len(configTxs))
	batch.TxIdsMap = createTxId2IndexMap(configTxs)
	batch.Endorsement = &commonPb.EndorsementEntry{Signer: nil, Signature: []byte("sign")}

	imlPool.OnMessage(newMessage(mustMarshal(batch), txpoolPb.TxPoolMsgType_BATCH_TX))
	time.Sleep(2 * time.Second)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	fetchedBatchIds, fetchedTxs := imlPool.FetchTxBatches(1)
	require.EqualValues(t, 1, len(fetchedBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(fetchedTxs))

	// 3. mismatch txsNum with txs, should not added in pool
	fmt.Println("---------------------------- test 3 ----------------------------")
	batch.Size_ = 2
	imlPool.OnMessage(newMessage(mustMarshal(batch), txpoolPb.TxPoolMsgType_BATCH_TX))
	time.Sleep(time.Second)
	require.EqualValues(t, 0, imlPool.queue.configTxCount())

	// 4. process recover request success
	fmt.Println("---------------------------- test 4 ----------------------------")
	batch.Size_ = 1
	batchReq := &txpoolPb.TxBatchRecoverRequest{
		NodeId:   requestBatchesNodeId,
		Height:   1,
		BatchIds: []string{batch.BatchId},
	}
	imlPool.OnMessage(newMessage(mustMarshal(batchReq), txpoolPb.TxPoolMsgType_RECOVER_REQ))

	// 5. process recover response success
	fmt.Println("---------------------------- test 5 ----------------------------")
	batchRes := &txpoolPb.TxBatchRecoverResponse{
		NodeId:    testNodeId,
		Height:    1,
		TxBatches: []*txpoolPb.TxBatch{batch},
	}
	imlPool.OnMessage(newMessage(mustMarshal(batchRes), txpoolPb.TxPoolMsgType_RECOVER_RESP))
}

func newMessage(poolMsgBz []byte, msgType txpoolPb.TxPoolMsgType) *msgbus.Message {
	txPoolMsg := &txpoolPb.TxPoolMsg{
		Type:    msgType,
		Payload: poolMsgBz,
	}
	netMsg := &netPb.NetMsg{
		Payload: mustMarshal(txPoolMsg),
		Type:    netPb.NetMsg_TX,
	}
	return &msgbus.Message{
		Topic:   msgbus.RecvTxPoolMsg,
		Payload: netMsg,
	}
}

func TestBatchTxPool_FetchTxBatches(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 1 config tx to build 1 config tx batch and 4 common txs to build 2 common tx batches
	configTxs := generateTxs(1, true)
	for _, tx := range configTxs {
		require.NoError(t, imlPool.AddTx(tx, protocol.RPC))
	}
	commonTxs := generateTxs(4, false)
	for _, tx := range commonTxs {
		require.NoError(t, imlPool.AddTx(tx, protocol.RPC))
	}
	time.Sleep(2 * time.Second)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. fetch 1 config batch for build block 1, fetch 2 common batches for build block 2
	fetchedBatchIds, fetchedTxs := imlPool.FetchTxBatches(1)
	require.EqualValues(t, 1, len(fetchedBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	fetchedBatchIds, fetchedTxs = imlPool.FetchTxBatches(2)
	require.EqualValues(t, 2, len(fetchedBatchIds))
	require.EqualValues(t, 4, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 0, imlPool.queue.commonTxCount())
}

func TestBatchTxPool_ReGenTxBatchesWithRetryTxs(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 1 config tx to build 1 config tx batch and 4 common txs to build 2 common tx batches
	fmt.Println("---------------------------- test 1 ----------------------------")
	confTxs := sortTxs(generateTxs(1*1, true))
	confMBatches := generateMBatchesByTxs(1, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. fetch 1 config batch to build block 1
	// and execute tx timeout
	fmt.Println("---------------------------- test 2 ----------------------------")
	fetchedBatchIds, fetchedTxs := imlPool.FetchTxBatches(3)
	require.EqualValues(t, 1, len(fetchedBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, confBatchIds[0], fetchedBatchIds[0])
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())
	newBatchIds, newTxsTable := imlPool.ReGenTxBatchesWithRetryTxs(3, fetchedBatchIds, confTxs)
	require.EqualValues(t, 1, len(newBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(newTxsTable))
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	val, ok := imlPool.queue.configBatchQueue.pendingCache.Load(newBatchIds[0])
	require.True(t, ok)
	newBatch, ok := val.(*memTxBatch)
	require.True(t, ok)
	require.NotNil(t, newBatch)
	// consumes old config batch to build block 2
	fetchedBatchIds, fetchedTxs = imlPool.FetchTxBatches(2)
	require.EqualValues(t, 1, len(fetchedBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 3. fetch 2 common batches to build block 3
	// and execute tx1-3 timeout
	fmt.Println("---------------------------- test 3 ----------------------------")
	fetchedBatchIds, fetchedTxs = imlPool.FetchTxBatches(1)
	require.EqualValues(t, 2, len(fetchedBatchIds))
	require.EqualValues(t, 4, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, 0, imlPool.queue.commonTxCount())

	newBatchIds, newTxsTable = imlPool.ReGenTxBatchesWithRetryTxs(1, fetchedBatchIds, commTxs)
	require.EqualValues(t, 2, len(newBatchIds))
	require.EqualValues(t, 4, calcTxNumInTxsTable(newTxsTable))
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())
	for _, batchId := range newBatchIds {
		commonBatchQueue := imlPool.queue.getCommonBatchQueue(batchId)
		val, ok = commonBatchQueue.pendingCache.Load(batchId)
		require.True(t, ok)
		newBatch, ok = val.(*memTxBatch)
		require.True(t, ok)
		require.NotNil(t, newBatch)
	}
}

func TestBatchTxPool_ReGenTxBatchesWithRemoveTxs(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 1 config tx to build 1 config tx batch and 4 common txs to build 2 common tx batches
	fmt.Println("---------------------------- test 1 ----------------------------")
	confTxs := sortTxs(generateTxs(1*1, true))
	confMBatches := generateMBatchesByTxs(1, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. fetch 1 config batch to build block 1
	// and tx is timeout remove it
	fmt.Println("---------------------------- test 2 ----------------------------")
	fetchedBatchIds, fetchedTxs := imlPool.FetchTxBatches(1)
	require.EqualValues(t, 1, len(fetchedBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, confBatchIds[0], fetchedBatchIds[0])
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())
	newBatchIds, newTxsTable := imlPool.ReGenTxBatchesWithRemoveTxs(1, fetchedBatchIds, confTxs)
	require.EqualValues(t, 0, len(newBatchIds))
	require.EqualValues(t, 0, calcTxNumInTxsTable(newTxsTable))
	require.EqualValues(t, 0, imlPool.queue.configTxCount())

	// 3. fetch 2 common batches to build block 3
	// and tx1-3 is timeout remove these
	fmt.Println("---------------------------- test 3 ----------------------------")
	fetchedBatchIds, fetchedTxs = imlPool.FetchTxBatches(3)
	require.EqualValues(t, 2, len(fetchedBatchIds))
	require.EqualValues(t, 4, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, 0, imlPool.queue.commonTxCount())

	newBatchIds, newTxsTable = imlPool.ReGenTxBatchesWithRemoveTxs(3, fetchedBatchIds, commTxs[:3])
	require.EqualValues(t, 1, len(newBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(newTxsTable))
	require.EqualValues(t, 0, imlPool.queue.commonTxCount())
	for _, batchId := range newBatchIds {
		commonBatchQueue := imlPool.queue.getCommonBatchQueue(batchId)
		val, ok := commonBatchQueue.pendingCache.Load(batchId)
		require.True(t, ok)
		newBatch, ok := val.(*memTxBatch)
		require.True(t, ok)
		require.NotNil(t, newBatch)
	}
}

func TestBatchTxPool_RemoveTxInTxBatches(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 1 config tx to build 1 config tx batch and 4 common txs to build 2 common tx batches
	fmt.Println("---------------------------- test 1 ----------------------------")
	confTxs := sortTxs(generateTxs(1*1, true))
	confMBatches := generateMBatchesByTxs(1, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. fetch 1 config batch to build block 1
	// and tx is a rand transaction, remove it
	fmt.Println("---------------------------- test 2 ----------------------------")
	fetchedBatchIds, fetchedTxs := imlPool.FetchTxBatches(1)
	require.EqualValues(t, 1, len(fetchedBatchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, confBatchIds[0], fetchedBatchIds[0])
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	imlPool.RemoveTxsInTxBatches(fetchedBatchIds, confTxs)
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 0, imlPool.queue.configBatchQueue.TxCountInPending())

	// 3. fetch 2 common batches to build block 3
	// and tx1-3 are rand transaction, remove these
	fmt.Println("---------------------------- test 3 ----------------------------")
	fetchedBatchIds, fetchedTxs = imlPool.FetchTxBatches(3)
	require.EqualValues(t, 2, len(fetchedBatchIds))
	require.EqualValues(t, 4, calcTxNumInTxsTable(fetchedTxs))
	require.EqualValues(t, 0, imlPool.queue.commonTxCount())

	imlPool.RemoveTxsInTxBatches(fetchedBatchIds, commTxs[:3])
	require.EqualValues(t, 1, imlPool.queue.commonTxCount())
}

func TestBatchTxPool_GetAllTxsByBatchIds(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 1 config tx to build 1 config tx batch and 4 common txs to build 2 common tx batches
	_, confMBatches := generateBatchesAndMBatches(1, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	_, cmmMBatches := generateBatchesAndMBatches(2, false, 2)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache
	imlPool.AddTxBatchesToPendingCache(confBatchIds, 1)
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 3. get config batch in pendingCache
	txsTable, err := imlPool.GetAllTxsByBatchIds(confBatchIds, testNodeId, 1, 1000)
	require.NoError(t, err)
	require.EqualValues(t, 1, calcTxNumInTxsTable(txsTable))

	// 4. get common batch in queue
	txsTable, err = imlPool.GetAllTxsByBatchIds(commBatchIds, testNodeId, 2, 1000)
	require.NoError(t, err)
	require.EqualValues(t, 4, calcTxNumInTxsTable(txsTable))

	// 5. get common batch no in pool
	batchIds := generateBatchIdsByBatches(generateMBatches(4, false, 2))
	txsTable, err = imlPool.GetAllTxsByBatchIds(batchIds, testNodeId, 2, 1000)
	require.Error(t, err)
	require.EqualValues(t, 0, calcTxNumInTxsTable(txsTable))
}

func TestBatchTxPool_AddTxBatchesToPendingCache(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 1 config tx to build 1 config tx batch and 4 common txs to build 2 common tx batches
	_, confMBatches := generateBatchesAndMBatches(1, true, 1)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	_, cmmMBatches := generateBatchesAndMBatches(2, false, 2)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache
	imlPool.AddTxBatchesToPendingCache(confBatchIds, 1)
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 3. add 2 common batches to pendingCache
	imlPool.AddTxBatchesToPendingCache(commBatchIds, 2)
	require.EqualValues(t, 0, imlPool.queue.configTxCount())
	require.EqualValues(t, 0, imlPool.queue.commonTxCount())
}

func TestBatchTxPool_RetryTxBatches(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 2 config tx to build 2 config tx batch and 4 common txs to build 2 common tx batches
	fmt.Println("---------------------------- test 1 ----------------------------")
	confTxs := sortTxs(generateTxs(2*1, true))
	confMBatches := generateMBatchesByTxs(2, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache, add 1 common batches to pendingCache
	fmt.Println("---------------------------- test 2 ----------------------------")
	imlPool.AddTxBatchesToPendingCache(confBatchIds[:1], 1)
	imlPool.AddTxBatchesToPendingCache(commBatchIds[:1], 2)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 3. retry 1 config batch in pendingCache
	fmt.Println("---------------------------- test 3 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(confBatchIds[:1], nil)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())

	// 4. retry 2 config batch in queue
	fmt.Println("---------------------------- test 4 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(confBatchIds, nil)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())

	// 5. retry 1 common batch in pendingCache
	fmt.Println("---------------------------- test 5 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(commBatchIds[:1], nil)
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 6. retry 2 common batches in queue
	fmt.Println("---------------------------- test 6 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(commBatchIds, nil)
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())
}

func TestBatchTxPool_RemoveTxBatches(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 3 config tx to build 3 config tx batch and 6 common txs to build 3 common tx batches
	fmt.Println("---------------------------- test 1 ----------------------------")
	confTxs := sortTxs(generateTxs(3*1, true))
	confMBatches := generateMBatchesByTxs(3, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(3*2, false))
	cmmMBatches := generateMBatchesByTxs(3, 2, commTxs)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 3, imlPool.queue.configTxCount())
	require.EqualValues(t, 6, imlPool.queue.commonTxCount())

	// 2. add 2 config batch to pendingCache, add 2 common batches to pendingCache
	fmt.Println("---------------------------- test 2 ----------------------------")
	imlPool.AddTxBatchesToPendingCache(confBatchIds[:2], 1)
	imlPool.AddTxBatchesToPendingCache(commBatchIds[:2], 2)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 3. remove 1 config batch in pendingCache
	fmt.Println("---------------------------- test 3 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(nil, confBatchIds[:1])
	require.EqualValues(t, 1, imlPool.queue.configTxCount())

	// 4. remove 2 config batch in queue
	fmt.Println("---------------------------- test 4 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(nil, confBatchIds)
	require.EqualValues(t, 0, imlPool.queue.configTxCount())

	// 5. remove 1 common batch in pendingCache
	fmt.Println("---------------------------- test 5 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(nil, commBatchIds[:1])
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 6. remove 2 common batch in queue
	fmt.Println("---------------------------- test 6 ----------------------------")
	imlPool.RetryAndRemoveTxBatches(nil, commBatchIds)
	require.EqualValues(t, 0, imlPool.queue.commonTxCount())
}

func TestBatchTxPool_TxExists(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 2 config tx to build 2 config tx batch and 4 common txs to build 2 common tx batches
	fmt.Println("---------------------------- test 1 ----------------------------")
	confTxs := sortTxs(generateTxs(2*1, true))
	confMBatches := generateMBatchesByTxs(2, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache, add 1 common batch to pendingCache
	fmt.Println("---------------------------- test 2 ----------------------------")
	imlPool.AddTxBatchesToPendingCache(confBatchIds[:1], 1)
	imlPool.AddTxBatchesToPendingCache(commBatchIds[:1], 2)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 3. get 1 config tx in queue or pendingCache
	fmt.Println("---------------------------- test 3 ----------------------------")
	require.EqualValues(t, true, imlPool.TxExists(confTxs[1]))
	require.EqualValues(t, true, imlPool.TxExists(confTxs[0]))

	// 4. get 1 common tx in queue or pendingCache
	fmt.Println("---------------------------- test 4 ----------------------------")
	require.EqualValues(t, true, imlPool.TxExists(commTxs[2]))
	require.EqualValues(t, true, imlPool.TxExists(confTxs[0]))

	// 5. tx on exist in pool
	fmt.Println("---------------------------- test 5 ----------------------------")
	require.EqualValues(t, false, imlPool.TxExists(generateTxs(1, false)[0]))
	require.EqualValues(t, false, imlPool.TxExists(generateTxs(1, true)[0]))
}

func TestBatchTxPool_GetPoolStatus(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 2 config tx to build 2 config tx batch and 4 common txs to build 2 common tx batches
	confTxs := sortTxs(generateTxs(2*1, true))
	confMBatches := generateMBatchesByTxs(2, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache, add 1 common batch to pendingCache
	imlPool.AddTxBatchesToPendingCache(confBatchIds[:1], 1)
	imlPool.AddTxBatchesToPendingCache(commBatchIds[:1], 2)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 3. get pool status
	status := imlPool.GetPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxPoolSize)
	require.EqualValues(t, 20, status.CommonTxPoolSize)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	require.EqualValues(t, 2, status.CommonTxNumInQueue)
	require.EqualValues(t, 2, status.CommonTxNumInPending)
}

func TestBatchTxPool_GetTxIdsByTypeAndStage(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 2 config tx to build 2 config tx batch and 4 common txs to build 2 common tx batches
	confTxs := sortTxs(generateTxs(2*1, true))
	confMBatches := generateMBatchesByTxs(2, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache, add 1 common batch to pendingCache
	imlPool.AddTxBatchesToPendingCache(confBatchIds[:1], 1)
	imlPool.AddTxBatchesToPendingCache(commBatchIds[:1], 2)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 3. get pool status
	status := imlPool.GetPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxPoolSize)
	require.EqualValues(t, 20, status.CommonTxPoolSize)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	require.EqualValues(t, 2, status.CommonTxNumInQueue)
	require.EqualValues(t, 2, status.CommonTxNumInPending)

	// 4. get all txs
	txIds := imlPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 3, len(txIds))
	txIds = imlPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 3, len(txIds))
	txIds = imlPool.GetTxIdsByTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 6, len(txIds))
}

func TestBatchTxPool_GetTxsInPoolByTxIds(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")

	// 1. add 2 config tx to build 2 config tx batch and 4 common txs to build 2 common tx batches
	confTxs := sortTxs(generateTxs(2*1, true))
	confMBatches := generateMBatchesByTxs(2, 1, confTxs)
	confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache, add 1 common batch to pendingCache
	imlPool.AddTxBatchesToPendingCache(confBatchIds[:1], 1)
	imlPool.AddTxBatchesToPendingCache(commBatchIds[:1], 2)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 3. get pool status
	status := imlPool.GetPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxPoolSize)
	require.EqualValues(t, 20, status.CommonTxPoolSize)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	require.EqualValues(t, 2, status.CommonTxNumInQueue)
	require.EqualValues(t, 2, status.CommonTxNumInPending)

	// 4. get txs in pool
	rxsRet, txsMis, _ := imlPool.GetTxsInPoolByTxIds(generateTxIdsByTxs(confTxs))
	require.EqualValues(t, 2, len(rxsRet))
	require.EqualValues(t, 0, len(txsMis))

	rxsRet, txsMis, _ = imlPool.GetTxsInPoolByTxIds(generateTxIdsByTxs(commTxs))
	require.EqualValues(t, 4, len(rxsRet))
	require.EqualValues(t, 0, len(txsMis))
}

func TestBatchTxPool_dumpTxs(t *testing.T) {
	// create  testPool
	testPool, fn := newTestPool(t, 4, 500)
	txPool := testPool.txPool
	defer fn()
	imlPool := txPool.(*batchTxPool)
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, true, "")

	// 1. add 2 config tx to build 2 config tx batch and 4 common txs to build 2 common tx batches
	confTxs := sortTxs(generateTxs(2*1, true))
	confMBatches := generateMBatchesByTxs(2, 1, confTxs)
	//confBatchIds := generateBatchIdsByBatches(confMBatches)
	for _, batch := range confMBatches {
		imlPool.queue.addConfigTxBatch(batch)
	}
	commTxs := sortTxs(generateTxs(2*2, false))
	cmmMBatches := generateMBatchesByTxs(2, 2, commTxs)
	commBatchIds := generateBatchIdsByBatches(cmmMBatches)
	imlPool.queue.addCommonTxBatches(cmmMBatches)
	require.EqualValues(t, 2, imlPool.queue.configTxCount())
	require.EqualValues(t, 4, imlPool.queue.commonTxCount())

	// 2. add 1 config batch to pendingCache, add 1 common batch to pendingCache
	batchIds, txsTale := imlPool.FetchTxBatches(1)
	require.EqualValues(t, 1, len(batchIds))
	require.EqualValues(t, 1, calcTxNumInTxsTable(txsTale))
	imlPool.AddTxBatchesToPendingCache(commBatchIds[:1], 2)
	require.EqualValues(t, 1, imlPool.queue.configTxCount())
	require.EqualValues(t, 2, imlPool.queue.commonTxCount())

	// 3. get pool status
	status := imlPool.GetPoolStatus()
	require.EqualValues(t, 20, status.ConfigTxPoolSize)
	require.EqualValues(t, 20, status.CommonTxPoolSize)
	require.EqualValues(t, 1, status.ConfigTxNumInQueue)
	require.EqualValues(t, 1, status.ConfigTxNumInPending)
	require.EqualValues(t, 2, status.CommonTxNumInQueue)
	require.EqualValues(t, 2, status.CommonTxNumInPending)

	// 4. dump txs
	fmt.Println("---------------------------- test dump ----------------------------")
	err := imlPool.dumpTxs()
	require.NoError(t, err)
	fmt.Println("---------------------------- test replay ----------------------------")
	err = imlPool.replayTxs()
	require.NoError(t, err)
	fmt.Println("---------------------------- test finish ----------------------------")
}
