/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"fmt"
	"sync"
	"testing"
	"time"

	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_TxRecover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// init TxPoolConfig
	initTxPoolConfig(20, 20, 2, false, "")
	// create batchQueue and batchRecover
	log := newMockLogger()
	queue := newBatchQueue(newMockChainConf(ctrl, 4, false), log)
	recover := newBatchRecover(testNodeId, queue, newMockMessageBus(ctrl), log)

	// 1. cache 20 block batches, only cache the block from [10,19]
	fmt.Println("---------------------------- test 1 ----------------------------")
	batches, mBatches := generateBatchesAndMBatches(200, false, 2)
	batchIds := generateBatchIdsByBatches(mBatches)
	perBatchesInBlock := 10
	for i := 0; i < len(batches)/perBatchesInBlock; i++ {
		recover.CacheFetchedTxBatches(uint64(i), batches[i*perBatchesInBlock:(i+1)*perBatchesInBlock])
		if i < 10 {
			require.EqualValues(t, i+1, len(recover.batchesCache))
		} else {
			require.EqualValues(t, defaultTxBatchCacheSize, len(recover.batchesCache))
		}
	}
	// 2. process recover request in block 9 failed
	fmt.Println("---------------------------- test 2 ----------------------------")
	batchReq := &txpoolPb.TxBatchRecoverRequest{
		NodeId:   requestBatchesNodeId,
		Height:   9,
		BatchIds: batchIds[90:100],
	}
	recover.ProcessRecoverReq(batchReq)
	// 3. process recover request in block 10 success
	fmt.Println("---------------------------- test 3 ----------------------------")
	batchReq.Height = 10
	batchReq.BatchIds = batchIds[100:110]
	recover.ProcessRecoverReq(batchReq)
	// 4. process recover request in block 50 from pending success
	// config tx batches
	fmt.Println("---------------------------- test 4 ----------------------------")
	_, configMBatches := generateBatchesAndMBatches(10, true, 1)
	confBatchIds := generateBatchIdsByBatches(configMBatches)
	queue.addConfigTxBatch(configMBatches[0])
	require.EqualValues(t, 1, queue.configTxCount())
	queue.moveBatchesToPending(confBatchIds[:1])
	require.EqualValues(t, 0, queue.configTxCount())
	batchReq.Height = 50
	batchReq.BatchIds = confBatchIds[:1]
	recover.ProcessRecoverReq(batchReq)
	// common txs
	_, commMBatches := generateBatchesAndMBatches(10, false, 2)
	commBatchIds := generateBatchIdsByBatches(commMBatches)
	queue.addCommonTxBatch(commMBatches[0])
	require.EqualValues(t, 2, queue.commonTxCount())
	queue.moveBatchesToPending(commBatchIds[:1])
	require.EqualValues(t, 0, queue.commonTxCount())
	batchReq.Height = 50
	batchReq.BatchIds = commBatchIds[:1]
	recover.ProcessRecoverReq(batchReq)
	// 5. recover tx batches in block 9 failed
	fmt.Println("---------------------------- test 5 ----------------------------")
	batchesMis := make(map[string]struct{})
	for _, batchId := range batchIds[90:100] {
		batchesMis[batchId] = struct{}{}
	}
	var batchesRet map[string]*memTxBatch
	var wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		batchesRet, batchesMis = recover.RecoverTxBatches(batchesRet, batchesMis, testNodeId, 9, 5000)
	}()
	time.Sleep(1 * time.Second)
	batchRes := &txpoolPb.TxBatchRecoverResponse{
		NodeId:    testNodeId,
		Height:    10,
		TxBatches: batches[100:110],
	}
	recover.ProcessRecoverRes(batchRes.NodeId, batchRes.Height, nil)
	wg.Wait()
	require.EqualValues(t, 0, len(batchesRet))
	require.EqualValues(t, 10, len(batchesMis))
	// 6. recover tx batches in block 10 success
	fmt.Println("---------------------------- test 6 ----------------------------")
	batchesMis = make(map[string]struct{})
	for _, batchId := range batchIds[100:110] {
		batchesMis[batchId] = struct{}{}
	}
	batchesRet = make(map[string]*memTxBatch)

	wg.Add(1)
	go func() {
		defer wg.Done()
		batchesRet, batchesMis = recover.RecoverTxBatches(batchesRet, batchesMis, testNodeId, 10, 5000)
	}()
	time.Sleep(1 * time.Second)
	recover.ProcessRecoverRes(batchRes.NodeId, batchRes.Height, mBatches[100:110])
	wg.Wait()
	require.EqualValues(t, 10, len(batchesRet))
	require.EqualValues(t, 0, len(batchesMis))
}
