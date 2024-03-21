/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"fmt"
	"testing"

	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestTxList_Put(t *testing.T) {
	// 0. init source
	mtxs := generateMemTxs(100, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	list := newTxList(newMockChainConf(ctrl, false), newMockLogger())

	// 1. put [RPC,P2P,INTERNAL] txs and check num in txList
	list.Put(mtxs[:10], protocol.RPC, nil)
	require.EqualValues(t, 10, list.queueSize())
	list.Put(mtxs[10:20], protocol.P2P, nil)
	require.EqualValues(t, 20, list.queueSize())
	list.Put(mtxs[20:30], protocol.INTERNAL, nil)
	require.EqualValues(t, 30, list.queueSize())

	// 2. repeat put tx failed due to the txs has exist in pool
	list.Put(mtxs[:10], protocol.RPC, nil)
	require.EqualValues(t, 30, list.queueSize())
	list.Put(mtxs[10:20], protocol.P2P, nil)
	require.EqualValues(t, 30, list.queueSize())
	list.Put(mtxs[20:30], protocol.INTERNAL, nil)
	require.EqualValues(t, 30, list.queueSize())

	// 5. put txs to pendingCache, so the txs from [RPC,P2P] can not put into pool,
	//but the txs from [INTERNAL] can put into pool
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	list.Put(mtxs[30:40], protocol.RPC, nil)
	require.EqualValues(t, 30, list.queueSize())
	list.Put(mtxs[30:40], protocol.P2P, nil)
	require.EqualValues(t, 30, list.queueSize())
	list.Put(mtxs[30:40], protocol.INTERNAL, nil)
	require.EqualValues(t, 40, list.queueSize())
}

func TestTxList_Get(t *testing.T) {
	mtxs := generateMemTxs(100, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	list := newTxList(newMockChainConf(ctrl, false), newMockLogger())

	// 1. put mtxs[:30] into pool
	list.Put(mtxs[:30], protocol.RPC, nil)
	for _, mtx := range mtxs[:30] {
		tx, err := list.Get(mtx.getTxId())
		require.NoError(t, err)
		require.NotNil(t, tx)
	}

	// 2. check mtxs[30:100] not exist in queue
	for _, mtx := range mtxs[30:100] {
		tx, err := list.Get(mtx.getTxId())
		require.Error(t, err)
		require.Nil(t, tx)
	}

	// 3. put mtxs[30:40] to pending cache and check mtxs[30:40] exist in pendingCache
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	for _, mtx := range mtxs[30:40] {
		txInPool, err := list.Get(mtx.getTxId())
		require.NoError(t, err)
		require.EqualValues(t, mtx, txInPool)
	}
}

func TestTxList_GetTxs(t *testing.T) {
	mtxs := generateMemTxs(100, false)
	txIds := getTxIdsByMemTxs(mtxs)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	list := newTxList(newMockChainConf(ctrl, false), newMockLogger())

	// 1. put mtxs[:30] mtxs and check existence
	list.Put(mtxs[:30], protocol.RPC, nil)
	txsRet, txsMis := list.GetTxs(txIds[:30])
	require.EqualValues(t, 30, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))

	// 2. check mtxs[30:100] not exist in txList
	txsRet, txsMis = list.GetTxs(txIds[30:100])
	require.EqualValues(t, 0, len(txsRet))
	require.EqualValues(t, 70, len(txsMis))

	// 3. put mtxs[30:40] to pending cache and check mtxs[30:40] exist in pendingCache in the txList
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	txsRet, txsMis = list.GetTxs(txIds[30:40])
	require.EqualValues(t, 10, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
}

func TestTxList_Has(t *testing.T) {
	mtxs := generateMemTxs(100, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	list := newTxList(newMockChainConf(ctrl, false), newMockLogger())

	// 1. put mtxs[:30] and check existence
	list.Put(mtxs[:30], protocol.RPC, nil)
	for _, mtx := range mtxs[:30] {
		require.True(t, list.Has(mtx.getTxId(), true))
		require.True(t, list.Has(mtx.getTxId(), false))
	}

	// 2. put mtxs[30:40] in pendingCache in txList and check existence
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	for _, mtx := range mtxs[30:40] {
		require.True(t, list.Has(mtx.getTxId(), true))
		require.False(t, list.Has(mtx.getTxId(), false))
	}

	// 3. check not existence in txList
	for _, mtx := range mtxs[40:] {
		require.False(t, list.Has(mtx.getTxId(), true))
		require.False(t, list.Has(mtx.getTxId(), false))
	}
}

func TestTxList_Delete(t *testing.T) {
	mtxs := generateMemTxs(100, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	list := newTxList(newMockChainConf(ctrl, false), newMockLogger())

	// 1. put mtxs[:30]
	list.Put(mtxs[:30], protocol.RPC, nil)

	// 2. delete mtxs[:10] and check correctness
	list.Delete(getTxIdsByMemTxs(mtxs[:10]))
	require.EqualValues(t, 20, list.queueSize())
	for _, mtx := range mtxs[:10] {
		require.False(t, list.Has(mtx.getTxId(), true))
		require.False(t, list.Has(mtx.getTxId(), false))
	}

	// 2. put mtxs[30:50] in the pendingCache in txList
	for _, mtx := range mtxs[30:50] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}

	// 3. put mtxs[40:50] succeed due to not check existence when source = [INTERNAL]
	list.Put(mtxs[40:50], protocol.INTERNAL, nil)
	require.EqualValues(t, 30, list.queueSize())

	// 4. delete mtxs[40:50], check pendingCache size and queue size
	list.Delete(getTxIdsByMemTxs(mtxs[40:50]))
	require.EqualValues(t, 20, list.queueSize())
}

func TestTxList_Fetch(t *testing.T) {
	mtxs := generateMemTxs(100, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	list := newTxList(newMockChainConf(ctrl, false), newMockLogger())

	// 1. put mtxs[:30] and Fetch mtxs
	list.Put(mtxs[:30], protocol.RPC, nil)

	fetchTxs, fetchTxIds := list.Fetch(100, nil)
	require.EqualValues(t, 30, len(fetchTxs))
	require.EqualValues(t, 30, len(fetchTxIds))
	require.EqualValues(t, 0, list.queueSize())

	// 2. put mtxs[:30] failed due to exist in pendingCache
	list.Put(mtxs[:30], protocol.RPC, nil)
	require.EqualValues(t, 0, list.queueSize())

	// 3. fetch mtxs nil due to not exist mtxs in txList
	fetchTxs, fetchTxIds = list.Fetch(100, nil)
	require.EqualValues(t, 0, len(fetchTxs))
	require.EqualValues(t, 0, len(fetchTxIds))

	// 4. put mtxs[30:100] and Fetch mtxs with less number
	list.Put(mtxs[30:], protocol.RPC, nil)
	fetchTxs, fetchTxIds = list.Fetch(10, nil)
	require.EqualValues(t, 10, len(fetchTxs))
	require.EqualValues(t, 10, len(fetchTxIds))

	// 5. fetch all remaining mtxs
	fetchTxs, fetchTxIds = list.Fetch(100, nil)
	require.EqualValues(t, 60, len(fetchTxs))
	require.EqualValues(t, 60, len(fetchTxIds))

	// 6. repeat put mtxs[30:100] with source = [INTERNAL] and fetch mtxs
	for _, mtx := range mtxs[30:] {
		list.pendingCache.Delete(mtx.getTxId())
	}
	list.Put(mtxs[30:], protocol.INTERNAL, nil)
	require.EqualValues(t, 70, list.queueSize())

	fetchTxs, fetchTxIds = list.Fetch(100, nil)
	require.EqualValues(t, 70, len(fetchTxs))
	require.EqualValues(t, 70, len(fetchTxIds))
}

func TestTxList_Fetch_Bench(t *testing.T) {
	mtxs := generateMemTxs(1000000, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	list := newTxList(newMockChainConf(ctrl, false), newMockLogger())

	// 1. put mtxs
	beginPut := utils.CurrentTimeMillisSeconds()
	list.Put(mtxs, protocol.RPC, nil)
	fmt.Printf("put mtxs:%d, elapse time: %d\n", len(mtxs), utils.CurrentTimeMillisSeconds()-beginPut)

	// 2. fetch
	fetchNum := 100000
	for i := 0; i < len(mtxs)/fetchNum; i++ {
		beginFetch := utils.CurrentTimeMillisSeconds()
		fetchTxs, _ := list.Fetch(fetchNum, nil)
		fmt.Printf("fetch mtxs:%d, elapse time: %d\n", len(fetchTxs), utils.CurrentTimeMillisSeconds()-beginFetch)
	}
	require.EqualValues(t, 0, list.queue.Size())
}
