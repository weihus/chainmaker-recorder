/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"os"
	"testing"
	"time"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/stretchr/testify/require"
)

func initTxPoolConfig() {
	TxPoolConfig = &txPoolConfig{}
}

func generateTxsBySource(num int, isConfig bool) (rpcTxs, p2pTxs, internalTxs *mempoolTxs) {
	rpcTxs = &mempoolTxs{isConfigTxs: isConfig, source: protocol.RPC}
	p2pTxs = &mempoolTxs{isConfigTxs: isConfig, source: protocol.P2P}
	internalTxs = &mempoolTxs{isConfigTxs: isConfig, source: protocol.INTERNAL}
	txType := commonPb.TxType_INVOKE_CONTRACT

	for i := 0; i < num; i++ {
		contractName := syscontract.SystemContract_CHAIN_CONFIG.String()
		if !isConfig {
			contractName = testContract
		}
		rpcTxs.mtxs = append(rpcTxs.mtxs, &memTx{tx: &commonPb.Transaction{Payload: &commonPb.Payload{TxId: utils.GetRandTxId(), TxType: txType, Method: "SetConfig", ContractName: contractName}}})
		p2pTxs.mtxs = append(p2pTxs.mtxs, &memTx{tx: &commonPb.Transaction{Payload: &commonPb.Payload{TxId: utils.GetRandTxId(), TxType: txType, Method: "SetConfig", ContractName: contractName}}})
		internalTxs.mtxs = append(internalTxs.mtxs, &memTx{tx: &commonPb.Transaction{Payload: &commonPb.Payload{TxId: utils.GetRandTxId(), TxType: txType, Method: "SetConfig", ContractName: contractName}}})
	}
	return
}

func getTxsByMTxs(mTxs []*memTx) []*commonPb.Transaction {
	txs := make([]*commonPb.Transaction, 0, len(mTxs))
	for _, mtx := range mTxs {
		txs = append(txs, mtx.tx)
	}
	return txs
}

func TestMain(m *testing.M) {
	initTxPoolConfig()
	code := m.Run()
	os.Exit(code)
}

func TestAddMemoryTxs(t *testing.T) {
	cache := newTxCache()
	rpcTxs, p2pTxs, internalTxs := generateTxsBySource(10, false)
	cache.addMemoryTxs(rpcTxs)
	require.EqualValues(t, 10, cache.txCount())
	cache.addMemoryTxs(p2pTxs)
	require.EqualValues(t, 20, cache.txCount())
	cache.addMemoryTxs(internalTxs)
	require.EqualValues(t, 30, cache.txCount())
}

func TestMergeAndSplitTxsBySource(t *testing.T) {
	cache := newTxCache()
	rpcTxs, p2pTxs, internalTxs := generateTxsBySource(30, false)
	cache.addMemoryTxs(rpcTxs)
	cache.addMemoryTxs(rpcTxs)
	cache.addMemoryTxs(p2pTxs)
	cache.addMemoryTxs(internalTxs)

	tmpRpcTxs, tmpP2PTxs, tmpInternalTxs := cache.mergeAndSplitTxsBySource(nil)
	require.EqualValues(t, append(rpcTxs.mtxs, rpcTxs.mtxs...), tmpRpcTxs)
	require.EqualValues(t, p2pTxs.mtxs, tmpP2PTxs)
	require.EqualValues(t, internalTxs.mtxs, tmpInternalTxs)
}

func TestIsFlushByTxCount(t *testing.T) {
	cache := newTxCache()
	cache.flushThreshold = 20
	rpcTxs, _, _ := generateTxsBySource(10, false)
	cache.addMemoryTxs(rpcTxs)
	require.False(t, cache.isFlushByTxCount(nil))
	require.True(t, cache.isFlushByTxCount(rpcTxs))

	cache.addMemoryTxs(rpcTxs)
	require.True(t, cache.isFlushByTxCount(nil))
}

func TestIsFlushByTime(t *testing.T) {
	cache := newTxCache()
	cache.flushTimeOut = 200 * time.Microsecond
	require.True(t, cache.isFlushByTime())
	cache.reset()
	require.False(t, cache.isFlushByTime())
	time.Sleep(time.Millisecond * 200)
	require.True(t, cache.isFlushByTime())
}

func TestReset(t *testing.T) {
	cache := newTxCache()
	rpcTxs, _, _ := generateTxsBySource(10, false)
	cache.addMemoryTxs(rpcTxs)
	require.EqualValues(t, 10, cache.txCount())
	cache.reset()
	require.EqualValues(t, 0, cache.txCount())
	require.EqualValues(t, 0, len(cache.txs))
}
