/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// Test_TxRecover test TxRecover
func Test_TxRecover(t *testing.T) {
	// 0.init source
	txs, txIds := generateTxs(200, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	msgBus := newMockMessageBus(ctrl)
	log := newMockLogger()
	disp := newTxDispatcher(log)
	queue := newTxQueue(10, chainConf, store.store, log)
	recover := newTxRecover(testNodeId, disp, queue, msgBus, log)
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
		NodeId: testNodeId,
		Height: 9,
		TxIds:  txIds[90:100],
	}
	recover.ProcessRecoverReq(txsReq)
	// 3. process recover request in block 10 success
	txsReq.Height = 10
	txsReq.TxIds = txIds[100:110]
	recover.ProcessRecoverReq(txsReq)
	// 4. process recover request in block 50 from pending success
	// config tx
	confTxs, _ := generateMemTxs(10, 0, true)
	queue.addConfigTx(confTxs[0])
	require.EqualValues(t, int32(1), atomic.LoadInt32(queue.configTxCountAtomic))
	queue.addConfigTxToPendingCache(confTxs[0])
	require.EqualValues(t, int32(0), atomic.LoadInt32(queue.configTxCountAtomic))
	txsReq.Height = 50
	txsReq.TxIds = []string{confTxs[0].getTxId()}
	recover.ProcessRecoverReq(txsReq)
	// common txs
	_, commTxs, commTxIds, _ := genTxsByQueueNum(disp, 8, 10, false)
	mtxsTable := disp.DistTxsToMemTxs(commTxs, 8, 0)
	queue.addCommonTxsToPendingCache(mtxsTable)
	txsReq.Height = 50
	txsReq.TxIds = commTxIds
	recover.ProcessRecoverReq(txsReq)
	// 5. recover txs in block 9 failed
	txsMis := generateTxIdsToMap(txIds[90:100])
	txsRet := make(map[string]*commonPb.Transaction, 10)
	var wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		txsRet, txsMis = recover.RecoverTxs(txsMis, txsRet, testNodeId, 9, 5000)
	}()
	time.Sleep(1 * time.Second)
	txsRes := &txpoolPb.TxRecoverResponse{
		NodeId: testNodeId,
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
		txsRet, txsMis = recover.RecoverTxs(txsMis, txsRet, testNodeId, 10, 5000)
	}()
	time.Sleep(1 * time.Second)
	recover.ProcessRecoverRes(txsRes)
	wg.Wait()
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
}
