/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"fmt"
	"testing"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// Test_DistTx test DistTx
func Test_DistTx(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	disp := newTxDispatcher(newMockLogger())
	queueNum := 8
	txId0 := "08a"
	// (0+8+97)/8=1
	require.Equal(t, 1, disp.DistTx(txId0, queueNum))
}

// Test_DistTxByNormalWords test DistTxByNormalWords
func Test_DistTxByNormalWords(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create dispatcher
	disp := newTxDispatcher(newMockLogger())
	queueNum := 8
	txId0 := "m"
	//109/8=5
	require.Equal(t, 5, disp.DistTx(txId0, queueNum))
	txId0 = "m1"
	//109+1/8=6
	require.Equal(t, 6, disp.DistTx(txId0, queueNum))
}

// Test_DistTxs test DistTxs
func Test_DistTxs(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create dispatcher
	disp := newTxDispatcher(newMockLogger())
	queueNum := 8
	perNumInQueue := 10
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// test DistTxs
	txsTable := disp.DistTxs(txs, queueNum)
	require.Equal(t, queueNum, len(txsTable))
	for _, txsCache := range txsTable {
		require.Equal(t, perNumInQueue, len(txsCache))
	}
}

// Test_DistTxsToMemTxs test DistTxsToMemTxs
func Test_DistTxsToMemTxs(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create dispatcher
	disp := newTxDispatcher(newMockLogger())
	queueNum := 8
	perNumInQueue := 10
	_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txs))
	// test
	height := uint64(0)
	// test DistTxsToMemTxs
	mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, height)
	require.Equal(t, queueNum, len(mtxsTable))
	for _, txsCache := range mtxsTable {
		require.Equal(t, perNumInQueue, len(txsCache))
		require.Equal(t, height, txsCache[0].dbHeight)
	}
}

func Test_DistTxIds(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create dispatcher
	disp := newTxDispatcher(newMockLogger())
	queueNum := 8
	perNumInQueue := 10
	_, _, txIds, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
	require.Equal(t, queueNum*perNumInQueue, len(txIds))
	// test DistTxIds
	txIdsTable := disp.DistTxIds(txIds, queueNum)
	require.Equal(t, queueNum, len(txIdsTable))
	for _, txIdsCache := range txIdsTable {
		require.Equal(t, perNumInQueue, len(txIdsCache))
	}
}

// 1w笔txs, 分发需要1.5ms
func Test_DistTxsBench(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create dispatcher
	disp := newTxDispatcher(newMockLogger())
	totalTime := int64(0)
	count := 10
	queueNum := 8
	perNumInQueue := 1000
	// test DistTxs
	for i := 0; i < count; i++ {
		_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
		beginTime := utils.CurrentTimeMillisSeconds()
		txsTable := disp.DistTxs(txs, queueNum)
		totalTime += utils.CurrentTimeMillisSeconds() - beginTime
		for _, txsCache := range txsTable {
			require.Equal(t, perNumInQueue, len(txsCache))
		}
	}
	fmt.Printf("test DistTxs, total time:%d, count:%d, avg time:%d", totalTime, count, totalTime/int64(count))
}

// 1w笔txs, 分发需要1.3ms
func Test_DistTxsToMemTxsBench(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create dispatcher
	disp := newTxDispatcher(newMockLogger())
	totalTime := int64(0)
	count := 10
	queueNum := 8
	perNumInQueue := 1000
	height := uint64(0)
	// test DistTxsToMemTxs
	for i := 0; i < count; i++ {
		_, txs, _, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
		beginTime := utils.CurrentTimeMillisSeconds()
		mtxsTable := disp.DistTxsToMemTxs(txs, queueNum, height)
		totalTime += utils.CurrentTimeMillisSeconds() - beginTime
		for _, txsCache := range mtxsTable {
			require.Equal(t, perNumInQueue, len(txsCache))
			require.Equal(t, height, txsCache[0].dbHeight)
		}
	}
	fmt.Printf("test DistTxsToMemTxs, total time:%d, count:%d, avg time:%d", totalTime, count, totalTime/int64(count))
}

// 1w笔txIds, 分发需要1.2ms
func Test_DistTxIdsBench(t *testing.T) {
	// init source
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create dispatcher
	disp := newTxDispatcher(newMockLogger())
	totalTime := int64(0)
	count := 10
	queueNum := 8
	perNumInQueue := 1000
	// test DistTxIds
	for i := 0; i < count; i++ {
		_, _, txIds, _ := genTxsByQueueNum(disp, queueNum, perNumInQueue, false)
		beginTime := utils.CurrentTimeMillisSeconds()
		txIdsTable := disp.DistTxIds(txIds, queueNum)
		totalTime += utils.CurrentTimeMillisSeconds() - beginTime
		for _, txIdsCache := range txIdsTable {
			require.Equal(t, perNumInQueue, len(txIdsCache))
		}
	}
	fmt.Printf("test DistTxIds, total time:%d, count:%d, avg time:%d", totalTime, count, totalTime/int64(count))
}

// genTxsByQueueNum generate txs by queueNum and perNumInQueue
func genTxsByQueueNum(dis Dispatcher, queueNum int, perNumInQueue int, isConfig bool) (
	mtxs []*memTx, txs []*commonPb.Transaction, txIdsSlice []string, txIdsMap map[string]struct{}) {
	// generate txs
	perNum := make([]int, queueNum)
	mtxs = make([]*memTx, 0, queueNum*perNumInQueue)
	txs = make([]*commonPb.Transaction, 0, queueNum*perNumInQueue)
	txIdsSlice = make([]string, 0, queueNum*perNumInQueue)
	txIdsMap = make(map[string]struct{}, queueNum*perNumInQueue)
	totalCount := 0
	// generate queueNum*perNumInQueue txs
	for {
		txId := utils.GetRandTxId()
		idx := dis.DistTx(txId, queueNum)
		if perNum[idx] != perNumInQueue {
			tx := generateTx(txId, isConfig)
			mtxs = append(mtxs, &memTx{tx: tx})
			txs = append(txs, tx)
			txIdsSlice = append(txIdsSlice, txId)
			txIdsMap[txId] = struct{}{}
			perNum[idx] = perNum[idx] + 1
			totalCount++
		}
		if totalCount == queueNum*perNumInQueue {
			return
		}
	}
}

// generateTx generate a config or common tx
func generateTx(txId string, isConfig bool) *commonPb.Transaction {
	// generate config or common tx
	contractName := syscontract.SystemContract_CHAIN_CONFIG.String()
	if !isConfig {
		contractName = testContract
	}
	return &commonPb.Transaction{
		Payload: &commonPb.Payload{
			TxId:         txId,
			TxType:       commonPb.TxType_INVOKE_CONTRACT,
			ContractName: contractName,
			Method:       "SetConfig",
		},
	}
}

func Test_nextNearestPow2uint64(t *testing.T) {
	// test -1
	require.EqualValues(t, 0, nextNearestPow2int(-1))
	// test 0
	require.EqualValues(t, 0, nextNearestPow2int(0))
	// test 1
	require.EqualValues(t, 1, nextNearestPow2int(1))
	// test 2
	require.EqualValues(t, 2, nextNearestPow2int(2))
	// test 3
	require.EqualValues(t, 4, nextNearestPow2int(3))
	// test 5
	require.EqualValues(t, 8, nextNearestPow2int(5))
}
