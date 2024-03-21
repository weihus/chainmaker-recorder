/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"testing"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestAddTxsToConfigQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, internalTxs := generateTxsBySource(10, true)

	// 1. put mtxs to config queue
	queue.addTxsToConfigQueue(rpcTxs)
	queue.addTxsToConfigQueue(p2pTxs)
	queue.addTxsToConfigQueue(internalTxs)
	require.EqualValues(t, 30, queue.configTxsCount())

	// 2. repeat put mtxs to config queue failed when source = [RPC,P2P]
	queue.addTxsToConfigQueue(rpcTxs)
	queue.addTxsToConfigQueue(p2pTxs)
	queue.addTxsToConfigQueue(internalTxs)
	require.EqualValues(t, 30, queue.configTxsCount())
	require.EqualValues(t, 0, queue.commonTxsCount())

	// 3. repeat put mtxs to common queue failed due to txIds exist in config queue
	for _, mtx := range rpcTxs.mtxs {
		mtx.tx.Payload.TxType = commonPb.TxType_INVOKE_CONTRACT
	}
	queue.addTxsToCommonQueue(rpcTxs)
	queue.addTxsToCommonQueue(p2pTxs)
	require.EqualValues(t, 30, queue.configTxsCount())
	require.EqualValues(t, 20, queue.commonTxsCount())
}
func changeTx2ConfigTx(tx *commonPb.Transaction) {
	payload := tx.Payload
	payload.ContractName = syscontract.SystemContract_CHAIN_CONFIG.String()
}
func TestAddTxsToCommonQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, internalTxs := generateTxsBySource(10, false)

	// 1. put mtxs to queue
	queue.addTxsToCommonQueue(rpcTxs)
	queue.addTxsToCommonQueue(p2pTxs)
	queue.addTxsToCommonQueue(internalTxs)
	require.EqualValues(t, 30, queue.commonTxsCount())

	// 2. repeat put mtxs to queue failed when source = [RPC,P2P]
	queue.addTxsToCommonQueue(rpcTxs)
	queue.addTxsToCommonQueue(p2pTxs)
	queue.addTxsToCommonQueue(internalTxs)
	require.EqualValues(t, 30, queue.commonTxsCount())
	require.EqualValues(t, 0, queue.configTxsCount())

	// 3. repeat put mtxs to config queue failed due to txIds exist in common queue
	for _, mtx := range rpcTxs.mtxs {
		//tx.Payload.TxType = commonPb.TxType_INVOKE_CONTRACT
		changeTx2ConfigTx(mtx.tx)
	}
	queue.addTxsToConfigQueue(rpcTxs)
	queue.addTxsToConfigQueue(p2pTxs)
	require.EqualValues(t, 20, queue.configTxsCount())
	require.EqualValues(t, 30, queue.commonTxsCount())
}

func TestGetInQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, internalTxs := generateTxsBySource(10, false)

	// 1. put mtxs to queue and check existence
	queue.addTxsToCommonQueue(rpcTxs)
	for _, mtx := range rpcTxs.mtxs {
		txInPool, err := queue.get(mtx.getTxId())
		require.NoError(t, err)
		require.EqualValues(t, txInPool, mtx)
	}

	// 2. check not existence
	for _, mtx := range internalTxs.mtxs {
		txInPool, err := queue.get(mtx.getTxId())
		require.Error(t, err)
		require.Nil(t, txInPool)
	}
	for _, mtx := range p2pTxs.mtxs {
		txInPool, err := queue.get(mtx.getTxId())
		require.Error(t, err)
		require.Nil(t, txInPool)
	}

	// 3. modify p2pTxs txType to commonPb.TxType_INVOKE_CONTRACT
	for _, mtx := range p2pTxs.mtxs {
		changeTx2ConfigTx(mtx.tx)
	}

	// 4. put mtxs to config queue and check existence
	queue.addTxsToConfigQueue(p2pTxs)
	for _, mtx := range p2pTxs.mtxs {
		txInPool, err := queue.get(mtx.getTxId())
		require.NoError(t, err)
		require.EqualValues(t, txInPool, mtx)
	}
}

func TestHasInQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, internalTxs := generateTxsBySource(10, false)

	// 1. put mtxs to queue and check existence
	queue.addTxsToCommonQueue(rpcTxs)
	for _, mtx := range rpcTxs.mtxs {
		require.True(t, queue.has(mtx.tx, true))
	}

	// 2. check not existence
	for _, mtx := range internalTxs.mtxs {
		require.False(t, queue.has(mtx.tx, true))
	}
	for _, mtx := range p2pTxs.mtxs {
		require.False(t, queue.has(mtx.tx, true))
	}

	// 3. modify p2pTxs txType to commonPb.TxType_INVOKE_CONTRACT
	for _, mtx := range p2pTxs.mtxs {
		changeTx2ConfigTx(mtx.tx)
	}

	// 4. put mtxs to config queue and check existence
	queue.addTxsToConfigQueue(p2pTxs)
	for _, mtx := range p2pTxs.mtxs {
		require.True(t, queue.has(mtx.tx, true))
	}
}

func TestDeleteConfigTxs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, _ := generateTxsBySource(10, true)

	// 1. put mtxs to queue
	queue.addTxsToConfigQueue(rpcTxs)

	// 2. delete mtxs in common queue and check existence
	queue.deleteCommonTxs(getTxIdsByMemTxs(rpcTxs.mtxs))
	for _, mtx := range rpcTxs.mtxs {
		require.True(t, queue.has(mtx.tx, true))
	}
	require.EqualValues(t, 10, queue.configTxsCount())

	// 3. delete mtxs in config queue and check existence
	queue.deleteConfigTxs(getTxIdsByMemTxs(rpcTxs.mtxs))
	for _, mtx := range rpcTxs.mtxs {
		require.False(t, queue.has(mtx.tx, true))
	}
	require.EqualValues(t, 0, queue.configTxsCount())

	// 4. delete not exist mtxs and check existence
	queue.deleteConfigTxs(getTxIdsByMemTxs(p2pTxs.mtxs))
	require.EqualValues(t, 0, queue.configTxsCount())
}

func TestDeleteCommonTxs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, _ := generateTxsBySource(10, false)

	// 1. put mtxs to queue and check existence
	queue.addTxsToCommonQueue(rpcTxs)

	// 2. delete mtxs in common queue and check existence
	queue.deleteConfigTxs(getTxIdsByMemTxs(rpcTxs.mtxs))
	for _, mtx := range rpcTxs.mtxs {
		require.True(t, queue.has(mtx.tx, true))
	}
	require.EqualValues(t, 10, queue.commonTxsCount())

	// 3. delete mtxs in config queue and check existence
	queue.deleteCommonTxs(getTxIdsByMemTxs(rpcTxs.mtxs))
	for _, mtx := range rpcTxs.mtxs {
		require.False(t, queue.has(mtx.tx, true))
	}
	require.EqualValues(t, 0, queue.commonTxsCount())

	// 4. delete not exist mtxs and check existence
	queue.deleteConfigTxs(getTxIdsByMemTxs(p2pTxs.mtxs))
	require.EqualValues(t, 0, queue.commonTxsCount())
}

func TestAppendTxsToPendingCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, _ := generateTxsBySource(10, false)

	// 1. put mtxs to queue and check appendTxsToPendingCache
	queue.addTxsToCommonQueue(rpcTxs)
	queue.appendCommonTxsToPendingCache(getTxsByMTxs(rpcTxs.mtxs), 100)

	// 3. repeat appendTxsToPendingCache mtxs
	queue.appendCommonTxsToPendingCache(getTxsByMTxs(rpcTxs.mtxs), 100)

	// 4. modify p2pTxs txType to commonPb.TxType_INVOKE_CONTRACT
	for _, mtx := range p2pTxs.mtxs {
		changeTx2ConfigTx(mtx.tx)
	}

	// 5. add mtxs to config queue and check appendTxsToPendingCache
	queue.addTxsToCommonQueue(rpcTxs)
	queue.appendCommonTxsToPendingCache(getTxsByMTxs(p2pTxs.mtxs[:1]), 101)

	// 6. append >1 config mtxs
	queue.appendCommonTxsToPendingCache(getTxsByMTxs(p2pTxs.mtxs[1:]), 101)
	//require.EqualValues(t, 11, queue.configTxQueue.pendingCache.queueSize())
}

func TestGetTxsByTxTypeAndStage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	commonRPCTxs, commonP2PTxs, _ := generateTxsBySource(10, false)
	confRPCTxs, confP2PTxs, _ := generateTxsBySource(10, true)

	// 1. put txs to queue and put a half txs to appendTxsToPendingCache
	queue.addTxsToCommonQueue(commonRPCTxs)
	queue.appendCommonTxsToPendingCache(getTxsByMTxs(commonP2PTxs.mtxs), 100)

	queue.addTxsToConfigQueue(confRPCTxs)
	queue.appendConfigTxsToPendingCache(getTxsByMTxs(confP2PTxs.mtxs), 100)

	// 2. get txs by tx type and stage
	txs, txIds := queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_ALL_TYPE), int32(txpoolPb.TxStage_ALL_STAGE))
	require.EqualValues(t, 40, len(txs))
	require.EqualValues(t, 40, len(txIds))

	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 10, len(txs))
	require.EqualValues(t, 10, len(txIds))

	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 10, len(txs))
	require.EqualValues(t, 10, len(txIds))

	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE))
	require.EqualValues(t, 10, len(txs))
	require.EqualValues(t, 10, len(txIds))

	txs, txIds = queue.getTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING))
	require.EqualValues(t, 10, len(txs))
	require.EqualValues(t, 10, len(txIds))
}

func TestGetPoolStatue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	TxPoolConfig.MaxTxPoolSize = 100
	TxPoolConfig.MaxConfigTxPoolSize = 10
	commonRPCTxs, commonP2PTxs, _ := generateTxsBySource(10, false)
	confRPCTxs, confP2PTxs, _ := generateTxsBySource(10, true)

	// 1. put txs to queue and put a half txs to appendTxsToPendingCache
	queue.addTxsToCommonQueue(commonRPCTxs)
	queue.appendCommonTxsToPendingCache(getTxsByMTxs(commonP2PTxs.mtxs), 100)

	queue.addTxsToConfigQueue(confRPCTxs)
	queue.appendConfigTxsToPendingCache(getTxsByMTxs(confP2PTxs.mtxs), 100)

	// 2. get txs by tx type and stage
	status := queue.getPoolStatus()
	require.EqualValues(t, 10, status.ConfigTxPoolSize)
	require.EqualValues(t, 100, status.CommonTxPoolSize)
	require.EqualValues(t, 10, status.ConfigTxNumInQueue)
	require.EqualValues(t, 10, status.ConfigTxNumInPending)
	require.EqualValues(t, 10, status.CommonTxNumInQueue)
	require.EqualValues(t, 10, status.CommonTxNumInPending)
}

func TestFetchInQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	rpcTxs, p2pTxs, _ := generateTxsBySource(10, false)

	// 1. put mtxs to queue and check appendTxsToPendingCache
	queue.addTxsToCommonQueue(rpcTxs)
	fetchTxs := queue.fetch(100)
	require.EqualValues(t, rpcTxs.mtxs, fetchTxs)

	// 2. fetch mtxs nil
	fetchTxs = queue.fetch(100)
	require.EqualValues(t, 0, len(fetchTxs))

	// 3. modify p2pTxs txType to commonPb.TxType_INVOKE_CONTRACT and push mtxs to config queue
	for _, mtx := range p2pTxs.mtxs {
		changeTx2ConfigTx(mtx.tx)
	}
	queue.addTxsToConfigQueue(p2pTxs)

	// 4. fetch config tx
	fetchTxs = queue.fetch(100)
	require.EqualValues(t, p2pTxs.mtxs[:1], fetchTxs)

	// 5. next fetch
	fetchTxs = queue.fetch(100)
	require.EqualValues(t, p2pTxs.mtxs[1:2], fetchTxs)
}
