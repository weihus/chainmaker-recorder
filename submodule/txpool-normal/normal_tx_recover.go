/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	netPb "chainmaker.org/chainmaker/pb-go/v2/net"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
)

const (
	defaultTxBatchCacheSize = 10
)

// txRecover cache txs after fetched and process recover request or response
type txRecover struct {
	nodeId          string
	cacheMtx        sync.RWMutex
	txsCache        map[uint64]map[string]*commonPb.Transaction // key: blockHeight, val: map[txId]*tx
	listenerMtx     sync.RWMutex
	listenerChCache map[string]chan *txpoolPb.TxRecoverResponse // key: proposerId+height, val: resCh

	dispatcher Dispatcher
	txQueue    *txQueue // the queue for store transactions
	msgBus     msgbus.MessageBus
	log        protocol.Logger
}

// newTxRecover create txRecover
func newTxRecover(nodeId string, dispatcher Dispatcher, txQueue *txQueue,
	msgBus msgbus.MessageBus, log protocol.Logger) *txRecover {
	return &txRecover{
		nodeId:          nodeId,
		cacheMtx:        sync.RWMutex{},
		txsCache:        make(map[uint64]map[string]*commonPb.Transaction),
		listenerMtx:     sync.RWMutex{},
		listenerChCache: make(map[string]chan *txpoolPb.TxRecoverResponse),
		dispatcher:      dispatcher,
		txQueue:         txQueue,
		msgBus:          msgBus,
		log:             log,
	}
}

// CacheFetchedTxs cache txs after fetching
func (r *txRecover) CacheFetchedTxs(height uint64, txs []*commonPb.Transaction) {
	r.cacheMtx.Lock()
	defer r.cacheMtx.Unlock()
	// first gc
	r.gc(height)
	// add txs to cache
	if _, ok := r.txsCache[height]; !ok {
		r.txsCache[height] = make(map[string]*commonPb.Transaction, len(txs))
	}
	txsCache := r.txsCache[height]
	for _, tx := range txs {
		// avoid adding execution result to cached tx,
		// and reduce tx serialization time when sending tx to backups
		txsCache[tx.Payload.TxId] = copyTx(tx)
	}
	r.log.Debugf("CacheFetchedTxs, recover cache txs, height:%d, txs:%d", height, len(txs))
}

// RecoverTxs request lacked txs and wait response
func (r *txRecover) RecoverTxs(txsMis map[string]struct{}, txsRet map[string]*commonPb.Transaction,
	proposerId string, height uint64, timeoutMs int) (map[string]*commonPb.Transaction, map[string]struct{}) {
	if len(txsMis) == 0 {
		return txsRet, txsMis
	}
	if len(txsRet) == 0 {
		txsRet = make(map[string]*commonPb.Transaction, len(txsMis))
	}
	// add lister chan
	r.addListener(proposerId, height)
	defer r.removeListener(proposerId, height)
	// request missing txs
	misIdTxs := make([]string, 0, len(txsMis))
	for txId := range txsMis {
		misIdTxs = append(misIdTxs, txId)
	}
	// send recover request to proposer
	r.requestMissTxs(misIdTxs, height, proposerId)
	// capture tx from lister chan
	if timeoutMs <= 0 {
		timeoutMs = defaultRecoverTimeMs
	}
	select {
	case txsResp := <-r.getListenerCh(proposerId, height):
		for _, tx := range txsResp.Txs {
			txId := tx.Payload.TxId
			if _, ok := txsMis[txId]; ok {
				txsRet[txId] = tx
				delete(txsMis, txId)
				if len(txsMis) == 0 {
					return txsRet, txsMis
				}
			}
		}
	case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
		break
	}
	return txsRet, txsMis
}

// ProcessRecoverReq process recover request and response txs
func (r *txRecover) ProcessRecoverReq(txsReq *txpoolPb.TxRecoverRequest) {
	if txsReq == nil || len(txsReq.TxIds) == 0 {
		return
	}
	r.cacheMtx.RLock()
	defer r.cacheMtx.RUnlock()
	txs := make([]*commonPb.Transaction, 0, len(txsReq.TxIds))
	// get txs from txRecover
	txsMap, ok := r.txsCache[txsReq.Height]
	if !ok {
		r.log.Warnf("no cache txs in txRecover, block height:%d", txsReq.Height)
		var err error
		// get txs from config or common pendingCache
		// because of Locke logic in TBFT consensus
		if txsMap, err = r.getRequestedTxsFromPending(txsReq); err != nil {
			return
		}
	}
	// get txs backup requested
	for _, txId := range txsReq.TxIds {
		if tx, ok1 := txsMap[txId]; ok1 {
			txs = append(txs, tx)
		}
	}
	// it should get all txs
	if len(txs) != len(txsReq.TxIds) {
		r.log.Errorf("node:%s have no all request txs, want:%d, get:%d", r.nodeId, len(txsReq.TxIds), len(txs))
		return
	}
	// create txRecoverResponse
	txRecoverRes := &txpoolPb.TxRecoverResponse{
		NodeId: r.nodeId,
		Height: txsReq.Height,
		Txs:    txs,
	}
	// send txRecoverResponse
	r.sendRecoverMsg(txpoolPb.TxPoolMsgType_RECOVER_RESP, mustMarshal(txRecoverRes), txsReq.NodeId)
	r.log.Infof("send txRecoverResponse, to:%s, height:%d, txs:%d", txsReq.NodeId, txsReq.Height, len(txs))
}

// getRequestedTxsFromPending get requested txs from config ore common pool PendingCache
func (r *txRecover) getRequestedTxsFromPending(txsReq *txpoolPb.TxRecoverRequest) (
	map[string]*commonPb.Transaction, error) {
	findMsg := "find txs in pending, block height:%d, want:%d, get:%d"
	errMsg := "can not find txs in pending, block height:%d, want:%d, get:%d"
	txsRet := make(map[string]*commonPb.Transaction, len(txsReq.TxIds))
	// may be config tx
	if len(txsReq.TxIds) == 1 {
		txId := txsReq.TxIds[0]
		if mtx, err := r.txQueue.getTxByTxId(txId); err == nil {
			txsRet[txId] = mtx.getTx()
			r.log.Infof(fmt.Sprintf(findMsg, txsReq.Height, 1, 1))
			return txsRet, nil
		}
		r.log.Errorf(fmt.Sprintf(errMsg, txsReq.Height, 1, 0))
		return nil, fmt.Errorf(errMsg, txsReq.Height, 1, 0)
	}
	// common txs
	txIdsTable := r.dispatcher.DistTxIds(txsReq.TxIds, r.txQueue.commonQueuesNum())
	mtxsRet, txsMis := r.txQueue.getCommonTxsByTxIds(txIdsTable)
	if len(mtxsRet) != len(txsReq.TxIds) || len(txsMis) != 0 {
		r.log.Errorf(fmt.Sprintf(errMsg, txsReq.Height, len(txsReq.TxIds), len(mtxsRet)))
		return nil, fmt.Errorf(errMsg, txsReq.Height, len(txsReq.TxIds), len(mtxsRet))
	}
	for txId, mtx := range mtxsRet {
		txsRet[txId] = mtx.getTx()
	}
	r.log.Infof(fmt.Sprintf(findMsg, txsReq.Height, len(txsReq.TxIds), len(mtxsRet)))
	return txsRet, nil
}

// ProcessRecoverRes process recover response and put txs to Channel
func (r *txRecover) ProcessRecoverRes(txsRes *txpoolPb.TxRecoverResponse) {
	// should not be nil
	if txsRes == nil || len(txsRes.Txs) == 0 {
		r.log.Warnf("TxRecoverResponse should not be nil")
		return
	}
	go func() {
		if ch := r.getListenerCh(txsRes.NodeId, txsRes.Height); ch != nil {
			ch <- txsRes
		} else {
			r.log.Warnf("recover no waiting response height:%d, from:%s, txs:%d",
				txsRes.Height, txsRes.NodeId, len(txsRes.Txs))
		}
	}()
}

// gc clear overdue txs
func (r *txRecover) gc(height uint64) {
	for h := range r.txsCache {
		if (h + uint64(defaultTxBatchCacheSize)) <= height {
			r.log.Debugw("delete recover cache, gc params", "h", h, "curr height", height)
			delete(r.txsCache, h)
		}
	}
}

// requestMissTxs send recover request to proposer
func (r *txRecover) requestMissTxs(txIds []string, height uint64, to string) {
	// create txRecoverRequest
	txRecoverReq := &txpoolPb.TxRecoverRequest{
		NodeId: r.nodeId,
		Height: height,
		TxIds:  txIds,
	}
	// send txRecoverRequest
	r.sendRecoverMsg(txpoolPb.TxPoolMsgType_RECOVER_REQ, mustMarshal(txRecoverReq), to)
	r.log.Infof("send txRecoverRequest, to:%s, height:%d, txs:%d", to, height, len(txIds))
}

// sendRecoverMsg send recover msg to other node
func (r *txRecover) sendRecoverMsg(txPoolMsgType txpoolPb.TxPoolMsgType, recoverBz []byte, to string) {
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
}

// addListener register a channel to receive recover response
func (r *txRecover) addListener(proposerId string, height uint64) {
	// wait lock
	r.listenerMtx.Lock()
	defer r.listenerMtx.Unlock()
	// add a listener
	key := proposerId + strconv.FormatUint(height, 10)
	if _, ok := r.listenerChCache[key]; !ok {
		r.listenerChCache[key] = make(chan *txpoolPb.TxRecoverResponse, 1)
	}
}

// removeListener remove registered channel
func (r *txRecover) removeListener(proposerId string, height uint64) {
	// wait lock
	r.listenerMtx.Lock()
	defer r.listenerMtx.Unlock()
	// get and remove the listener
	key := proposerId + strconv.FormatUint(height, 10)
	if ch, ok := r.listenerChCache[key]; ok {
		close(ch)
	}
	delete(r.listenerChCache, key)
}

// getListenerCh get registered channel
func (r *txRecover) getListenerCh(proposerId string, height uint64) chan *txpoolPb.TxRecoverResponse {
	// wait lock
	r.listenerMtx.RLock()
	defer r.listenerMtx.RUnlock()
	// get the listener
	key := proposerId + strconv.FormatUint(height, 10)
	return r.listenerChCache[key]
}

// txCacheToProto get txs in  txCache and generate TxRecoverResponse
func (r *txRecover) txCacheToProto() []*txpoolPb.TxRecoverResponse {
	// waite lock
	r.cacheMtx.RLock()
	defer r.cacheMtx.RUnlock()
	// get all txs in recover
	txsRecRes := make([]*txpoolPb.TxRecoverResponse, 0, len(r.txsCache))
	for h, txsMap := range r.txsCache {
		txs := make([]*commonPb.Transaction, 0, len(r.txsCache))
		for _, tx := range txsMap {
			txs = append(txs, tx)
		}
		txsRecRes = append(txsRecRes, &txpoolPb.TxRecoverResponse{
			NodeId: r.nodeId,
			Height: h,
			Txs:    txs,
		})
	}
	return txsRecRes
}
