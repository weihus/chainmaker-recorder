/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"sync/atomic"
	"testing"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// Test_PutTxs test PutTxs
func Test_PutTxs(t *testing.T) {
	// 0.init source
	mtxs, _ := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs and check num in txList
	list.PutTxs(mtxs[:10], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(10), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 10, queueLen)
	require.EqualValues(t, 0, pendingLen)
	list.PutTxs(mtxs[10:20], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(20), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 20, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. repeat put tx failed due to the txs has exist in pool
	list.PutTxs(mtxs[:10], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(20), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 20, queueLen)
	require.EqualValues(t, 0, pendingLen)
	list.PutTxs(mtxs[10:20], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, 20, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 5. put txs to pendingCache, so the txs from [RPC,P2P] can not put into pool,
	//but the txs from [INTERNAL] can put into pool
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	list.PutTxs(mtxs[30:40], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(20), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 20, queueLen)
	require.EqualValues(t, 10, pendingLen)
	list.PutTxs(mtxs[30:40], list.retryTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 10, pendingLen)
}

// Test_PutTx test PutTx
func Test_PutTx(t *testing.T) {
	// 0.init source
	mtxs, _ := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put tx and check num in txList
	list.PutTx(mtxs[0], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(1), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 1, queueLen)
	require.EqualValues(t, 0, pendingLen)
	list.PutTx(mtxs[1], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(2), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 2, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. repeat put tx failed due to the tx has exist in pool
	list.PutTx(mtxs[0], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(2), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 2, queueLen)
	require.EqualValues(t, 0, pendingLen)
	list.PutTx(mtxs[1], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(2), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 2, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 5. put tx to pendingCache
	// so the txs from [RPC,P2P] can not put into pool,
	//but the tx from [INTERNAL] can put into pool
	list.pendingCache.Store(mtxs[2].getTxId(), mtxs[2])
	list.PutTx(mtxs[2], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(2), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 2, queueLen)
	require.EqualValues(t, 1, pendingLen)
	list.PutTx(mtxs[2], list.retryTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(3), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 3, queueLen)
	require.EqualValues(t, 1, pendingLen)
}

// Test_FetchTxs test FetchTxs
func Test_FetchTxs(t *testing.T) {
	// 0.init source
	mtxs, _ := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30] and Fetch txs
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	fetchTxs, fetchTxIds := list.FetchTxs(100, list.fetchTxValidate)
	require.EqualValues(t, 30, len(fetchTxs))
	require.EqualValues(t, 30, len(fetchTxIds))
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(0), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 30, pendingLen)
	// 2. put txs[:30] failed due to exist in pendingCache
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(0), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 30, pendingLen)
	// 3. fetch txs nil due to not exist txs in txList
	fetchTxs, fetchTxIds = list.FetchTxs(100, list.fetchTxValidate)
	require.EqualValues(t, 0, len(fetchTxs))
	require.EqualValues(t, 0, len(fetchTxIds))
	// 4. put txs[30:100] and Fetch txs with less number
	list.PutTxs(mtxs[30:], list.addTxValidate)
	fetchTxs, fetchTxIds = list.FetchTxs(10, list.fetchTxValidate)
	require.EqualValues(t, 10, len(fetchTxs))
	require.EqualValues(t, 10, len(fetchTxIds))
	// 5. fetch all remaining txs
	fetchTxs, fetchTxIds = list.FetchTxs(100, list.fetchTxValidate)
	require.EqualValues(t, 60, len(fetchTxs))
	require.EqualValues(t, 60, len(fetchTxIds))
}

// Test_GetTxsByTxIds test GetTxsByTxIds
func Test_GetTxsByTxIds(t *testing.T) {
	// 0.init source
	mtxs, txIds := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30]
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. get txs[:30] from queue
	txsRet, txsMis := list.GetTxsByTxIds(txIds[:30])
	require.EqualValues(t, 30, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
	// 3. get txs[:40] from queue
	txsRet, txsMis = list.GetTxsByTxIds(txIds[:40])
	require.EqualValues(t, 30, len(txsRet))
	require.EqualValues(t, 10, len(txsMis))
	// 4. fetch txs[:30] from queue
	fetchTxs, fetchTxIds := list.FetchTxs(100, list.fetchTxValidate)
	require.EqualValues(t, 30, len(fetchTxs))
	require.EqualValues(t, 30, len(fetchTxIds))
	// 5. put txs[30:40] to pending
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	// 6. get txs[:40] from queue and pending
	txsRet, txsMis = list.GetTxsByTxIds(txIds[:40])
	require.EqualValues(t, 40, len(txsRet))
	require.EqualValues(t, 0, len(txsMis))
}

// Test_GetTxByTxId test getTxByTxId
func Test_GetTxByTxId(t *testing.T) {
	// 0.init source
	mtxs, txIds := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30]
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. get tx[0] from queue
	mtx, err := list.GetTxByTxId(txIds[0])
	require.NotNil(t, mtx)
	require.NoError(t, err)
	// 3. get tx[30] from queue failed
	mtx, err = list.GetTxByTxId(txIds[30])
	require.Nil(t, mtx)
	require.Error(t, err)
	// 4. put txs[30:40] to pending
	for _, tx := range mtxs[30:40] {
		list.pendingCache.Store(tx.getTxId(), tx)
	}
	// 5. get tx[30] from pending
	mtx, err = list.GetTxByTxId(txIds[30])
	require.NotNil(t, mtx)
	require.NoError(t, err)
	// 6. get tx[40] from pending failed
	mtx, err = list.GetTxByTxId(txIds[40])
	require.Nil(t, mtx)
	require.Error(t, err)
}

// Test_AddTxsToPending test AddTxsToPending
func Test_AddTxsToPending(t *testing.T) {
	// 0.init source
	mtxs, _ := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30] to list
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. add txs[:30] to pending
	list.AddTxsToPending(mtxs[:30])
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(0), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 30, pendingLen)
	// 3. put txs[30:40] to pending
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	// 4. add txs[30:40] to pending
	list.AddTxsToPending(mtxs[30:40])
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(0), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 40, pendingLen)
}

// Test_HasTx test HasTx
func Test_HasTx(t *testing.T) {
	// 0.init source
	mtxs, txIds := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30] to list
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. has txs[0]
	exist := list.HasTx(txIds[0])
	require.True(t, exist)
	// 3. has not txs[30]
	exist = list.HasTx(txIds[30])
	require.False(t, exist)
	// 4. put txs[30:40] to pending
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	// 5. has txs[30]
	exist = list.HasTx(txIds[30])
	require.True(t, exist)
	// 6. has not txs[40]
	exist = list.HasTx(txIds[40])
	require.False(t, exist)
}

// Test_RemoveTxs test RemoveTxs
func Test_RemoveTxs(t *testing.T) {
	// 0.init source
	mtxs, txIds := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30] and remove txs[:20]
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	list.RemoveTxs(txIds[:20])
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(10), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 10, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. put txs[:20] successfully
	list.PutTxs(mtxs[:20], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 3. put txs[30:40] to pending
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 10, pendingLen)
	// 4. remove all
	list.RemoveTxs(txIds[:100])
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(0), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 0, queueLen)
	require.EqualValues(t, 0, pendingLen)
}

// Test_RemoveTxsInPending test RemoveTxsInPending
func Test_RemoveTxsInPending(t *testing.T) {
	// 0.init source
	mtxs, txIds := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, false)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30] to queue and remove in pending failed
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	list.RemoveTxsInPending(txIds[:20])
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. put txs[:20] failed
	list.PutTxs(mtxs[:20], list.addTxValidate)
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 3. put txs[30:40] to pending
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 10, pendingLen)
	// 4. remove all txs in pending
	list.RemoveTxsInPending(txIds[:100])
	queueLen, pendingLen = list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
}

// Test_GetTxsByStage test GetTxsByStage
func Test_GetTxsByStage(t *testing.T) {
	// 0.init source
	mtxs, _ := generateMemTxs(100, 0, false)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	queueSizeAtomic := int32(0)
	chainConf := newMockChainConf(ctrl, true)
	store := newMockBlockChainStore(ctrl, make(map[string]*commonPb.Transaction, 100))
	log := newMockLogger()
	list := newNormalTxList(&queueSizeAtomic, chainConf, store.store, log)
	// 1. put txs[:30] to queue
	list.PutTxs(mtxs[:30], list.addTxValidate)
	queueLen, pendingLen := list.Size()
	require.EqualValues(t, int32(30), atomic.LoadInt32(list.queueSizeAtomic))
	require.EqualValues(t, 30, queueLen)
	require.EqualValues(t, 0, pendingLen)
	// 2. put txs[30:40] to pending
	for _, mtx := range mtxs[30:40] {
		list.pendingCache.Store(mtx.getTxId(), mtx)
	}
	// 3. get txs in queue
	txs, txIds := list.GetTxsByStage(true, false)
	require.EqualValues(t, 30, len(txs))
	require.EqualValues(t, 30, len(txIds))
	// 4. get txs in pending
	txs, txIds = list.GetTxsByStage(false, true)
	require.EqualValues(t, 10, len(txs))
	require.EqualValues(t, 10, len(txIds))
	// 5. get txs in queue and pending
	txs, txIds = list.GetTxsByStage(true, true)
	require.EqualValues(t, 40, len(txs))
	require.EqualValues(t, 40, len(txIds))
}
