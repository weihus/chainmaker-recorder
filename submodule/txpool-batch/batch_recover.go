/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	netPb "chainmaker.org/chainmaker/pb-go/v2/net"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
)

const (
	defaultTxBatchCacheSize = 10
)

// batchRecover cache txs after fetched and process recover request or response
type batchRecover struct {
	nodeId string

	cacheMtx     sync.RWMutex
	batchesCache map[uint64]map[string]*txpoolPb.TxBatch // key: blockHeight, val: map[batchId]*txBatch

	listenerMtx     sync.RWMutex
	listenerChCache map[string]chan []*memTxBatch // key: proposerId+height, val: resCh

	queue *batchQueue

	msgBus msgbus.MessageBus
	log    protocol.Logger
}

// newBatchRecover create batchRecover
func newBatchRecover(nodeId string, queue *batchQueue, msgBus msgbus.MessageBus, log protocol.Logger) *batchRecover {
	return &batchRecover{
		nodeId:          nodeId,
		cacheMtx:        sync.RWMutex{},
		batchesCache:    make(map[uint64]map[string]*txpoolPb.TxBatch),
		listenerMtx:     sync.RWMutex{},
		listenerChCache: make(map[string]chan []*memTxBatch),
		queue:           queue,
		msgBus:          msgBus,
		log:             log,
	}
}

// CacheFetchedTxBatches cache batches after fetching
func (r *batchRecover) CacheFetchedTxBatches(height uint64, batches []*txpoolPb.TxBatch) {
	// should not be nil
	if len(batches) == 0 {
		return
	}
	// wait lock
	r.cacheMtx.Lock()
	defer r.cacheMtx.Unlock()
	// first gc
	r.gc(height)
	// add batches to cache
	if _, ok := r.batchesCache[height]; !ok {
		r.batchesCache[height] = make(map[string]*txpoolPb.TxBatch, len(batches))
	}
	batchesCache := r.batchesCache[height]
	for _, batch := range batches {
		// avoid vm add execution result to cached tx,
		// and reduce batch serialization time when sending batch to backups
		batchesCache[batch.BatchId] = copyTxBatch(batch)
	}
	r.log.Debugf("CacheFetchedTxBatches, recover cache batches, height:%d, batches:%d", height, len(batches))
}

// RecoverTxBatches request lacked batches and wait response
func (r *batchRecover) RecoverTxBatches(mBatchesRet map[string]*memTxBatch, batchesMis map[string]struct{},
	proposerId string, height uint64, timeoutMs int) (map[string]*memTxBatch, map[string]struct{}) {
	if len(batchesMis) == 0 {
		return mBatchesRet, batchesMis
	}
	if len(mBatchesRet) == 0 {
		mBatchesRet = make(map[string]*memTxBatch, len(batchesMis))
	}
	// add lister chan
	r.addListener(proposerId, height)
	defer r.removeListener(proposerId, height)
	// request missing txs
	misBatchIds := make([]string, 0, len(batchesMis))
	for batchId := range batchesMis {
		misBatchIds = append(misBatchIds, batchId)
	}
	if err := r.requestMissTxBatches(misBatchIds, height, proposerId); err != nil {
		r.log.Errorf("requestMissTxs failed, err:%v", err)
		return mBatchesRet, batchesMis
	}
	// capture txs from lister chan
	if timeoutMs <= 0 {
		timeoutMs = defaultRecoverTimeMs
	}
	select {
	case mBatchesRes := <-r.getListenerCh(proposerId, height):
		for _, mBatch := range mBatchesRes {
			batchId := mBatch.batch.BatchId
			if _, ok := batchesMis[batchId]; ok {
				mBatchesRet[batchId] = mBatch
				delete(batchesMis, batchId)
				if len(batchesMis) == 0 {
					return mBatchesRet, batchesMis
				}
			}
		}
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		break
	}
	return mBatchesRet, batchesMis
}

// ProcessRecoverReq process recover request and response txs
func (r *batchRecover) ProcessRecoverReq(batchReq *txpoolPb.TxBatchRecoverRequest) {
	// should not be nil
	if batchReq == nil || len(batchReq.BatchIds) == 0 {
		return
	}
	// wait lock
	r.cacheMtx.RLock()
	defer r.cacheMtx.RUnlock()
	batchesRet := make([]*txpoolPb.TxBatch, 0, len(batchReq.BatchIds))
	// get batches from batchRecover
	batchesMap, ok := r.batchesCache[batchReq.Height]
	if !ok {
		r.log.Infof("no cache batches in batchRecover, block height:%d", batchReq.Height)
		var err error
		// if tx batches not in batchRecover,
		// then get tx batches from the pendingCache of configBatchQueue or common BatchQueue
		// because of Locke logic in TBFT consensus
		if batchesMap, err = r.getRequestedTxBatchesFromPending(batchReq); err != nil {
			return
		}
	}
	// get txBatches successfully
	for _, batchId := range batchReq.BatchIds {
		if batch, ok1 := batchesMap[batchId]; ok1 {
			batchesRet = append(batchesRet, batch)
		}
	}
	if len(batchesRet) != len(batchReq.BatchIds) {
		r.log.Errorf("node:%s have no all requested batches, want:%d, get:%d",
			r.nodeId, len(batchReq.BatchIds), len(batchesRet))
		return
	}
	// create txRecoverResponse
	batchRes := &txpoolPb.TxBatchRecoverResponse{
		NodeId:    r.nodeId,
		Height:    batchReq.Height,
		TxBatches: batchesRet,
	}
	// send txRecoverResponse
	if err := r.sendRecoverMsg(txpoolPb.TxPoolMsgType_RECOVER_RESP, mustMarshal(batchRes), batchReq.NodeId); err != nil {
		r.log.Errorf("send txBatchRecoverResponse to:%s failed, err:%v", batchReq.NodeId, err)
		return
	}
	r.log.Infof("send txBatchRecoverResponse, to:%s, height:%d, batches:%d",
		batchReq.NodeId, batchReq.Height, len(batchesRet))
}

// getRequestedTxBatchesFromPending get requested txBatches from pendingCache of configBatchQueue or commonBatchQueues
func (r *batchRecover) getRequestedTxBatchesFromPending(batchReq *txpoolPb.TxBatchRecoverRequest) (
	map[string]*txpoolPb.TxBatch, error) {
	// get mBatches
	mBatchesMap, _ := r.queue.getTxBatches(batchReq.BatchIds, StageInPending)
	if len(mBatchesMap) != len(batchReq.BatchIds) {
		r.log.Errorf("can not find batches in pending, block height:%d, want:%d, get:%d",
			batchReq.Height, len(batchReq.BatchIds), len(mBatchesMap))
		return nil, fmt.Errorf("can not find batches in pending, block height:%d, want:%d, get:%d",
			batchReq.Height, len(batchReq.BatchIds), len(mBatchesMap))
	}
	r.log.Infof("find batches in pending, block height:%d, want:%d, get:%d",
		batchReq.Height, len(batchReq.BatchIds), len(mBatchesMap))
	// return batches
	return createBatchMap(mBatchesMap), nil
}

// ProcessRecoverRes process recover response and put mBatches into channel
func (r *batchRecover) ProcessRecoverRes(from string, height uint64, mBatches []*memTxBatch) {
	// should not be nil
	if len(mBatches) == 0 {
		return
	}
	go func(from string, height uint64, mBatches []*memTxBatch) {
		if ch := r.getListenerCh(from, height); ch != nil {
			ch <- mBatches
		} else {
			r.log.Warnf("%s no waiting recover response height:%d, from:%s, batches:%d",
				r.nodeId, height, from, len(mBatches))
		}
	}(from, height, mBatches)
}

// gc clear overdue txs
func (r *batchRecover) gc(height uint64) {
	for h := range r.batchesCache {
		if (h + uint64(defaultTxBatchCacheSize)) <= height {
			r.log.Debugw("delete recover cache, gc params", "h", h, "curr height", height)
			delete(r.batchesCache, h)
		}
	}
}

// requestMissTxBatches send recover request to proposer
func (r *batchRecover) requestMissTxBatches(batchIds []string, height uint64, to string) error {
	// create txBatchRecoverReq
	txBatchRecoverReq := &txpoolPb.TxBatchRecoverRequest{
		NodeId:   r.nodeId,
		Height:   height,
		BatchIds: batchIds,
	}
	// send txBatchRecoverReq
	if err := r.sendRecoverMsg(txpoolPb.TxPoolMsgType_RECOVER_REQ, mustMarshal(txBatchRecoverReq), to); err != nil {
		r.log.Errorf("%s send txBatchRecoverRequest to:%s failed, %v", r.nodeId, to, err)
		return err
	}
	r.log.Debugf("send txBatchRecoverRequest, to:%s, height:%d, batches:%d", to, height, len(batchIds))
	return nil
}

// sendRecoverMsg send recover net msg to other node
func (r *batchRecover) sendRecoverMsg(txPoolMsgType txpoolPb.TxPoolMsgType, recoverBz []byte, to string) error {
	// create txPoolMsg
	txPoolMsg := &txpoolPb.TxPoolMsg{
		Type:    txPoolMsgType,
		Payload: recoverBz,
	}
	// create netMsg
	netMsg := &netPb.NetMsg{
		Payload: mustMarshal(txPoolMsg),
		Type:    netPb.NetMsg_TX,
		To:      to,
	}
	// broadcast netMsg
	r.msgBus.Publish(msgbus.SendTxPoolMsg, netMsg)
	return nil
}

// addListener register a channel to receive recover response
func (r *batchRecover) addListener(proposerId string, height uint64) {
	// wait lock
	r.listenerMtx.Lock()
	defer r.listenerMtx.Unlock()
	// get listener
	key := proposerId + strconv.FormatUint(height, 10)
	if _, ok := r.listenerChCache[key]; !ok {
		r.listenerChCache[key] = make(chan []*memTxBatch, 1)
	}
}

// removeListener remove registered channel
func (r *batchRecover) removeListener(proposerId string, height uint64) {
	// wait lock
	r.listenerMtx.Lock()
	defer r.listenerMtx.Unlock()
	// remove listener
	key := proposerId + strconv.FormatUint(height, 10)
	if ch, ok := r.listenerChCache[key]; ok {
		close(ch)
	}
	delete(r.listenerChCache, key)
}

// getListenerCh get registered channel
func (r *batchRecover) getListenerCh(proposerId string, height uint64) chan []*memTxBatch {
	// wait lock
	r.listenerMtx.RLock()
	defer r.listenerMtx.RUnlock()
	// get listener
	key := proposerId + strconv.FormatUint(height, 10)
	return r.listenerChCache[key]
}

// batchesCacheToProto get batches in batchesCache and generate TxBatchRecoverResponse
func (r *batchRecover) batchesCacheToProto() []*txpoolPb.TxBatchRecoverResponse {
	// wait lock
	r.cacheMtx.RLock()
	defer r.cacheMtx.RUnlock()
	// get txs in recover
	batchesRecRes := make([]*txpoolPb.TxBatchRecoverResponse, 0, len(r.batchesCache))
	for h, batchMap := range r.batchesCache {
		batches := make([]*txpoolPb.TxBatch, 0, len(batchMap))
		for _, batch := range batchMap {
			batches = append(batches, batch)
		}
		batchesRecRes = append(batchesRecRes, &txpoolPb.TxBatchRecoverResponse{
			NodeId:    r.nodeId,
			Height:    h,
			TxBatches: batches,
		})
	}
	return batchesRecRes
}

// createBatchMap create TxBatch map by memTxBatch map
func createBatchMap(mBatchesMap map[string]*memTxBatch) (batchesMap map[string]*txpoolPb.TxBatch) {
	batchesMap = make(map[string]*txpoolPb.TxBatch, len(mBatchesMap))
	for batchId, mBatch := range mBatchesMap {
		batchesMap[batchId] = mBatch.getBatch()
	}
	return
}
