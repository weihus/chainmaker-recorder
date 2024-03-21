/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"sync"
	"testing"
	"time"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_TxRecover(t *testing.T) {
	// 0.init source
	txs := generateTxs(200, false)
	txIds := getTxIdsByTxs(txs)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	msgBus := newMockMessageBus(ctrl)
	log := newMockLogger()
	queue := newQueue(newMockChainConf(ctrl, false), newMockLogger())
	recover := newTxRecover("node1", queue, msgBus, log)

	// 1. cache 20 block txs, only cache the block from [10,19]
	perTxs := 10
	for i := 0; i < len(txs)/perTxs; i++ {
		recover.CacheFetchedTxs(uint64(i), txs[i*perTxs:(i+1)*perTxs])
		if i < 10 {
			require.EqualValues(t, i+1, len(recover.txsCache))
		} else {
			require.EqualValues(t, defaultTxBatchCacheSize, len(recover.txsCache))
		}
	}

	// 2. process recover request in block 9 failed
	txsReq := &txpoolPb.TxRecoverRequest{
		NodeId: "node1",
		Height: 9,
		TxIds:  txIds[90:100],
	}
	recover.ProcessRecoverReq(txsReq)

	// 3. process recover request in block 10 success
	txsReq.Height = 10
	txsReq.TxIds = txIds[100:110]
	recover.ProcessRecoverReq(txsReq)

	// 4. process recover request in block 50 from pending success
	rpcTxs, _, _ := generateTxsBySource(10, false)
	queue.addTxsToCommonQueue(rpcTxs)
	fetchTxs := queue.fetch(100)
	require.EqualValues(t, rpcTxs.mtxs, fetchTxs)

	txsReq.Height = 50
	txsReq.TxIds = getTxIdsByMemTxs(rpcTxs.mtxs)
	recover.ProcessRecoverReq(txsReq)

	// 5. recover txs in block 9 failed
	txsMis := generateTxIdsToMap(txIds[90:100])
	txsRet := make(map[string]*commonPb.Transaction, 10)
	var wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		txsRet, txsMis = recover.RecoverTxs(txsMis, txsRet, "node1", 9, 5000)
	}()
	time.Sleep(1 * time.Second)
	txsRes := &txpoolPb.TxRecoverResponse{
		NodeId: "node1",
		Height: 10,
		Txs:    txs[100:110],
	}
	recover.ProcessRecoverRes(txsRes)
	wg.Wait()
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 10, len(txsMis))

	// 6. recover txs in block 10 success
	txsMis = generateTxIdsToMap(txIds[100:110])
	txsRet = make(map[string]*commonPb.Transaction, 10)
	wg.Add(1)
	go func() {
		defer wg.Done()
		txsRet, txsMis = recover.RecoverTxs(txsMis, txsRet, "node1", 10, 5000)
	}()
	time.Sleep(1 * time.Second)
	recover.ProcessRecoverRes(txsRes)
	wg.Wait()
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
}
