/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"chainmaker.org/chainmaker/common/v2/crypto/hash"
	commonErrors "chainmaker.org/chainmaker/common/v2/errors"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/localconf/v2"
	"chainmaker.org/chainmaker/lws"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	netPb "chainmaker.org/chainmaker/pb-go/v2/net"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/mitchellh/mapstructure"
)

const (
	// TxPoolType is tx_pool type
	TxPoolType = "BATCH"
	// BatchPoolAddtionalDataKey is the key for AddtionalData
	BatchPoolAddtionalDataKey = "BatchPoolAddtionalDataKey"
)

const (
	stopped  = 0
	started  = 1
	starting = 2
	stopping = 3
)

var _ protocol.TxPool = (*batchTxPool)(nil)

// batchTxPool Another implementation of tx pool
type batchTxPool struct {
	stat    int32  // Identification of module service startup, 0 is stopped, 1 is started, 2 is starting, 3 is stopping
	nodeId  string // The ID of node
	chainId string // The ID of chain

	addTxCh chan *memTx // channel that receive the transaction
	ctx     context.Context
	cancel  context.CancelFunc

	forwarder          Forwarder     // forward tx to a node
	queue              *batchQueue   // queue for store batches, contains configBatchQueue and commonBatchQueues
	recover            *batchRecover // batch recover to cache packed batches, and sync batches for node
	cache              *txCache      // tx cache to cache transactions from RPC
	batchTimer         *time.Timer   // build batch timer
	batchCreateTimeout time.Duration // build batch time threshold. uint:ms
	wal                *lws.Lws      // wal is used to store dumped batches

	singer     protocol.SigningMember         // Sign batch
	ac         protocol.AccessControlProvider // Verify transaction and batch signature
	netService protocol.NetService            // Verify nodeId is not forged
	msgBus     msgbus.MessageBus              // Receive messages from other modules
	chainConf  protocol.ChainConf             // Chain Config
	txFilter   protocol.TxFilter              // TxFilter is cuckoo filter
	blockStore protocol.BlockchainStore       // Access information on the chain
	log        protocol.Logger                // Log
}

// NewBatchTxPool creates batch tx_pool
func NewBatchTxPool(
	nodeId string,
	chainId string,
	txFilter protocol.TxFilter,
	chainStore protocol.BlockchainStore,
	msgBus msgbus.MessageBus,
	chainConf protocol.ChainConf,
	singer protocol.SigningMember,
	ac protocol.AccessControlProvider,
	netService protocol.NetService,
	log protocol.Logger,
	monitorEnabled bool,
	poolConfig map[string]interface{}) (protocol.TxPool, error) {
	// chainId should not be nil
	if len(chainId) == 0 {
		return nil, fmt.Errorf("no chainId in create txpool")
	}
	// nodeId should not be nil
	if len(nodeId) == 0 {
		return nil, fmt.Errorf("no nodeId in create txpool")
	}
	// set config
	TxPoolConfig = &txPoolConfig{}
	if err := mapstructure.Decode(poolConfig, TxPoolConfig); err != nil {
		return nil, err
	}
	MonitorEnabled = monitorEnabled
	var (
		addChSize         = defaultChannelSize
		batchCreatTimeout = defaultBatchCreateTimeout
		forwardMod        = defaultForwardMod
	)
	// set addTxChannelSize
	if TxPoolConfig.AddTxChannelSize > 0 {
		addChSize = int(TxPoolConfig.AddTxChannelSize)
	}
	// set batchCreatTimeout
	if TxPoolConfig.BatchCreateTimeout > 0 {
		batchCreatTimeout = int(TxPoolConfig.BatchCreateTimeout)
	}
	// set forwardMod
	if len(TxPoolConfig.ForwardMod) != 0 {
		forwardMod = TxPoolConfig.ForwardMod
	}
	// create forwarder
	forwarder, err := newTxForwarder(nodeId, ForwardMod(forwardMod), chainConf, log)
	if err != nil {
		return nil, err
	}

	bcTime := time.Duration(batchCreatTimeout) * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	queue := newBatchQueue(chainConf, log)
	// return batchTxPool
	return &batchTxPool{
		nodeId:             nodeId,
		chainId:            chainId,
		addTxCh:            make(chan *memTx, addChSize),
		ctx:                ctx,
		cancel:             cancel,
		forwarder:          forwarder,
		queue:              queue,
		recover:            newBatchRecover(nodeId, queue, msgBus, log),
		cache:              newTxCache(BatchMaxSize(chainConf)),
		batchCreateTimeout: bcTime,
		batchTimer:         time.NewTimer(bcTime),
		singer:             singer,
		ac:                 ac,
		netService:         netService,
		msgBus:             msgBus,
		chainConf:          chainConf,
		txFilter:           txFilter,
		blockStore:         chainStore,
		log:                log,
	}, nil
}

//UpdateTxPoolConfig including BatchMaxSize
func (pool *batchTxPool) UpdateTxPoolConfig(param string, value string) error {
	v, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	switch param {
	case "BatchMaxSize":
		pool.cache = newTxCache(BatchMaxSize(pool.chainConf))
		pool.log.Infof("[BatchTxPool] update BatchMaxSize success, value :%v", BatchMaxSize(pool.chainConf))
	case "BatchCreateTimeout":
		bcTime := time.Duration(v) * time.Millisecond
		pool.batchCreateTimeout = bcTime
		pool.batchTimer.Reset(pool.batchCreateTimeout)
		pool.log.Infof("[BatchTxPool] update BatchCreateTimeout success, value :%v", pool.batchCreateTimeout)
	default:
		return nil
	}
	return nil
}

// Start start the txPool service
func (pool *batchTxPool) Start() (err error) {
	// start logic: stopped => starting => started
	// should not start again
	// should not start when stopping
	if !atomic.CompareAndSwapInt32(&pool.stat, stopped, starting) {
		pool.log.Errorf(commonErrors.ErrTxPoolStartFailed.String())
		return commonErrors.ErrTxPoolStartFailed
	}
	// register pool msg
	if pool.msgBus != nil {
		pool.msgBus.Register(msgbus.RecvTxPoolMsg, pool)
	} else {
		panic("should not be happen, msgBus in single pool should not be nil")
	}
	// replay tx batches in wal
	if err = pool.replayTxs(); err != nil {
		// if start failed, set stat to 0 that mean pool is stopped
		atomic.StoreInt32(&pool.stat, stopped)
		pool.log.Errorf("replay dumped txs failed, err:%v", err)
		return err
	}
	// start createBatchLoop
	go pool.createBatchLoop()
	// set stat to started
	if !atomic.CompareAndSwapInt32(&pool.stat, starting, started) {
		pool.log.Errorf(commonErrors.ErrTxPoolStartFailed.String())
		return commonErrors.ErrTxPoolStartFailed
	}
	pool.log.Infof("start batch pool success")
	return
}

// OnMessage Process messages from MsgBus
func (pool *batchTxPool) OnMessage(msg *msgbus.Message) { // nolint
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if msg == nil {
		pool.log.Errorf("OnMessage received msg is empty")
		return
	}
	// should be RecvTxPoolMsg type
	if msg.Topic != msgbus.RecvTxPoolMsg {
		pool.log.Errorf("OnMessage received msg topic(%v) is not msgbus.RecvTxPoolMsg", msg.Topic)
		return
	}
	// unMarshal txPoolMsg
	txPoolBz := msg.Payload.(*netPb.NetMsg).Payload
	txPoolMsg := &txpoolPb.TxPoolMsg{}
	if err := proto.Unmarshal(txPoolBz, txPoolMsg); err != nil {
		pool.log.Warnf("OnMessage unmarshal txPoolMsg failed, err:%v", err)
		return
	}
	switch txPoolMsg.Type {
	case txpoolPb.TxPoolMsgType_SINGLE_TX:
		// unMarshal tx being forwarded
		tx := &commonPb.Transaction{}
		if err := proto.Unmarshal(txPoolMsg.Payload, tx); err != nil {
			pool.log.Warnf("OnMessage unmarshal transaction failed, err:%v", err)
			return
		}
		// add tx to pool
		if err := pool.AddTx(tx, protocol.P2P); err != nil {
			return
		}
		pool.log.Debugf("OnMessage receive tx being forwarded, txId:%s", tx.Payload.TxId)
	case txpoolPb.TxPoolMsgType_BATCH_TX:
		// unMarshal txBatch
		batch := &txpoolPb.TxBatch{}
		if err := proto.Unmarshal(txPoolMsg.Payload, batch); err != nil {
			pool.log.Warnf("OnMessage unmarshal txBatch failed, err:%v", err)
			return
		}
		// should not be nil
		if batch == nil || len(batch.Txs) == 0 {
			return
		}
		// verify whether tx pool is full
		if pool.queue.isFull(batch.Txs[0]) {
			return
		}
		// verify whether batch is valid, contain format, signature, exist in pool and the validity of txs.
		mBatch, err := pool.validateTxBatch(batch, TxBatchByBroadcast)
		if err != nil {
			return
		}
		// if there is invalid tx, it will rebuild a new batch
		if mBatch.getValidTxNum() < mBatch.getTxNum() {
			if mBatch, err = pool.reBuildMBatch(mBatch); err != nil {
				return
			}
		}
		// tell core engine SignalType_BLOCK_PROPOSE signal that it can package txs to build block
		defer pool.publishSignal()
		pool.log.Debugf("OnMessage receive txBatch, batchId:%x(%s), bytes:%d, txs:%d, valid txs:%d",
			mBatch.getBatchId(), getNodeId(mBatch.batch.BatchId), mBatch.batch.Size(), mBatch.getTxNum(), mBatch.getValidTxNum())
		// add batch to configBatchPool or commonBatchPool
		if len(mBatch.batch.Txs) == 1 && isConfigTx(mBatch.batch.Txs[0], pool.chainConf) {
			pool.queue.addConfigTxBatch(mBatch)
			return
		}
		pool.queue.addCommonTxBatch(mBatch)
	case txpoolPb.TxPoolMsgType_RECOVER_REQ:
		// unMarshal txRecoverRequest
		batchReq := &txpoolPb.TxBatchRecoverRequest{}
		if err := proto.Unmarshal(txPoolMsg.Payload, batchReq); err != nil {
			pool.log.Warnf("OnMessage unmarshal txBatchRecoverRequest failed, err:%v", err)
			return
		}
		pool.log.Debugf("OnMessage receive txBatchRecoverRequest, from:%s, height:%d, batches:%d",
			batchReq.NodeId, batchReq.Height, len(batchReq.BatchIds))
		// process batch recover request and response batches
		pool.recover.ProcessRecoverReq(batchReq)
	case txpoolPb.TxPoolMsgType_RECOVER_RESP:
		// unMarshal txRecoverResponse
		batchRes := &txpoolPb.TxBatchRecoverResponse{}
		if err := proto.Unmarshal(txPoolMsg.Payload, batchRes); err != nil {
			pool.log.Warnf("OnMessage unmarshal txBatchRecoverResponse failed, err:%v", err)
			return
		}
		// validate txRecoverRes
		if batchRes == nil || len(batchRes.TxBatches) == 0 {
			return
		}
		// attention: even if the pool is full, put it into pool !!!
		// verify batches
		mBatches := pool.validateTxBatches(batchRes.TxBatches, TxBatchByRecover)
		pool.log.Debugf("OnMessage receive txBatchRecoverResponse, "+
			"from:%s, height:%d, bytes:%d, batches:%d, valid batches:%d",
			batchRes.NodeId, batchRes.Height, batchRes.Size(), len(batchRes.TxBatches), len(mBatches))
		if len(mBatches) == 0 {
			return
		}
		// tell core engine SignalType_BLOCK_PROPOSE signal that it can package txs to build block
		defer pool.publishSignal()
		// add batches to batchRecover
		pool.recover.ProcessRecoverRes(batchRes.NodeId, batchRes.Height, mBatches)
		// add batches to configBatchQueue or commonBatchQueues
		if len(mBatches) == 1 && len(mBatches[0].batch.Txs) == 1 &&
			isConfigTx(mBatches[0].batch.Txs[0], pool.chainConf) {
			pool.queue.addConfigTxBatch(mBatches[0])
			return
		}
		pool.queue.addCommonTxBatches(mBatches)
	default:
		pool.log.Warnf("OnMessage receive invalid message type %s", txPoolMsg.Type)
	}
}

// AddTx add config or common tx from RPC or P2P(forward) to txCache
// these txs will be built into txBatch, put into configBatchQueue or commonBatchQueue and broadcast to other nodes
func (pool *batchTxPool) AddTx(tx *commonPb.Transaction, source protocol.TxSource) error {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return commonErrors.ErrTxPoolHasStopped
	}
	// should not be nil
	if tx == nil || tx.Payload == nil || len(tx.Payload.TxId) == 0 {
		return commonErrors.ErrStructEmpty
	}
	// verify tx source
	if source == protocol.INTERNAL {
		return commonErrors.ErrTxSource
	}
	// verify whether forward tx
	if source == protocol.RPC {
		if to := pool.forwarder.ForwardTx(tx); to != pool.nodeId {
			pool.sendTx(mustMarshal(tx), to)
			pool.log.Debugw("ForwardTx", "txId", tx.Payload.TxId, "to", to)
			return nil
		}
	}
	// verify whether tx exist in pool
	if exist := pool.queue.txExist(tx); exist {
		pool.log.Warnf("transaction exists in pool, txId:%s", tx.Payload.TxId)
		return commonErrors.ErrTxIdExist
	}
	// verify whether timestamp is overdue, exist in db and (signature and format is valid from p2p)
	mTx, err := pool.validateTx(tx, VerifyTxTime, VerifyTxAuth, VerifyTxInDB)
	if err != nil {
		return err
	}
	// verify whether the tx pool is full, if tx pool is full, send valid tx from RPC to a random node
	if pool.queue.isFull(tx) {
		if source == protocol.RPC {
			if to := pool.selectNode(); len(to) > 0 {
				pool.sendTx(mustMarshal(tx), to)
				pool.log.Debugw("SendTx", "txId", tx.Payload.TxId, "to", to)
			}
		}
		return commonErrors.ErrTxPoolLimit
	}
	pool.log.Debugw("AddTx", "txId", tx.Payload.GetTxId(), "source", source)
	// attempt add tx to addTxCh
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case pool.addTxCh <- mTx:
	case <-t.C:
		pool.log.Warnf("add transaction timeout, txId:%s, source:%v", mTx.getTxId(), source)
		return fmt.Errorf("add transaction timeout")
	}
	return nil
}

// createBatchLoop create config and common batch
func (pool *batchTxPool) createBatchLoop() {
	for {
		select {
		// receive tx from addTxCh
		case mtx, ok := <-pool.addTxCh:
			if !ok {
				pool.log.Warnf("addTxCh has been closed")
				return
			}
			if isConfigTx(mtx.tx, pool.chainConf) {
				// config tx
				pool.buildConfigTxBatch(mtx)
			} else {
				// common tx
				pool.putCommonTx(mtx)
			}
		// batch create timer
		case <-pool.batchTimer.C:
			pool.buildCommonTxBatch()
		// stop goroutine
		case <-pool.ctx.Done():
			return
		}
	}
}

// buildConfigTxBatch build and broadcast config tx batch
func (pool *batchTxPool) buildConfigTxBatch(mtx *memTx) {
	// tell core engine SignalType_BLOCK_PROPOSE signal that it can package txs to build block
	defer pool.publishSignal()
	// create config tx batch
	txs := []*commonPb.Transaction{mtx.getTx()}
	batch, err := pool.buildTxBatch(txs)
	if err != nil {
		return
	}
	// note: first marshal batch, then add batch to batchQueue,
	// to avoid vm add result to tx
	batchBz := mustMarshal(batch)
	// put batch to configBatchQueue
	pool.queue.addConfigTxBatch(newMemTxBatch(batch, mtx.dbHeight, createFilter(batch.Txs)))
	// broadcast batch to other nodes
	if localconf.ChainMakerConfig.DebugConfig.IsNoBroadcastTx {
		// simulate no broadcast batch to test recover mechanism
		pool.log.Warnf("simulate no broadcast batch, batchId:%x(%s), txs:%d",
			batch.BatchId, getNodeId(batch.BatchId), len(batch.Txs))
	} else {
		pool.broadcastTxBatch(batchBz)
		pool.log.Debugf("broadcast config txBatch success, batchId:%x(%s), bytes:%d, txs:%d",
			batch.BatchId, getNodeId(batch.BatchId), batch.Size(), len(batch.Txs))
	}
}

// putCommonTx put common tx to txCache
func (pool *batchTxPool) putCommonTx(mtx *memTx) {
	// add common tx to txCache
	pool.cache.PutTx(mtx)
	// if need to build txBatch
	// then build and broadcast common tx batch
	if pool.cache.Size() >= BatchMaxSize(pool.chainConf) {
		pool.buildCommonTxBatch()
	}
}

// buildConfigTxBatch build and broadcast common tx batch
func (pool *batchTxPool) buildCommonTxBatch() {
	defer func() {
		// reset txCache
		pool.cache.Reset()
		// reset batchTimer
		pool.batchTimer.Reset(pool.batchCreateTimeout)
		// tell core engine SignalType_BLOCK_PROPOSE signal that it can package txs to build block
		pool.publishSignal()
	}()
	// get common txs
	txs, dbHeight := pool.cache.GetTxs()
	if len(txs) == 0 || dbHeight == math.MaxUint64 {
		return
	}
	// create batch
	batch, err := pool.buildTxBatch(txs)
	if err != nil {
		return
	}
	// note: first marshal batch, then add batch to batchQueue,
	// to avoid vm add result to tx
	batchBz := mustMarshal(batch)
	// put batch to commonBatchQueue
	pool.queue.addCommonTxBatch(newMemTxBatch(batch, dbHeight, createFilter(batch.Txs)))
	// broadcast batch to other nodes
	if localconf.ChainMakerConfig.DebugConfig.IsNoBroadcastTx {
		// simulate no broadcast batch to test recover mechanism
		pool.log.Warnf("simulate no broadcast batch, batchId:%x(%s), txs:%d",
			batch.BatchId, getNodeId(batch.BatchId), len(batch.Txs))
	} else {
		pool.broadcastTxBatch(batchBz)
		pool.log.Debugf("broadcast common txBatch success, batchId:%x(%s), bytes:%d, txs:%d",
			batch.BatchId, getNodeId(batch.BatchId), batch.Size(), len(batch.Txs))
	}
}

func (pool *batchTxPool) reBuildMBatch(mBatch *memTxBatch) (*memTxBatch, error) {
	// no need rebuild a new batch
	if mBatch.getValidTxNum() == mBatch.getTxNum() {
		return mBatch, nil
	}
	if mBatch.getValidTxNum() == 0 {
		return nil, fmt.Errorf("the number of valid txs should not be 0")
	}
	// rebuild a nwe batch only contains valid txs
	newBatch, err := pool.buildTxBatch(mBatch.getValidTxs())
	if err != nil {
		return nil, err
	}
	newMBatch := newMemTxBatch(newBatch, mBatch.dbHeight, createFilter(newBatch.Txs))
	pool.log.Debugf("reBuildMBatch, [oldBatch:%x(%s)(%d), newBatch:%x(%s)(%d)]",
		mBatch.getBatchId(), getNodeId(mBatch.getBatchId()), mBatch.getTxNum(),
		newMBatch.getBatchId(), getNodeId(newMBatch.getBatchId()), newMBatch.getTxNum())
	return newMBatch, nil
}

// FetchTxs Get some transactions from single or normal txPool by block height to generate new block.
// return transactions.
func (pool *batchTxPool) FetchTxs(blockHeight uint64) (txs []*commonPb.Transaction) {
	pool.log.Warn("FetchTxs is no implementation for batch pool")
	return
}

// FetchTxBatches Get some transactions from batch txPool by block height to generate new block.
// return transactions table and batchId list.
func (pool *batchTxPool) FetchTxBatches(blockHeight uint64) (batchIds []string, txsTable [][]*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// fetch batches from configBatchQueue or commonBatchQueue
	startFetchTime := utils.CurrentTimeMillisSeconds()
	fetchedBatches := pool.queue.fetchTxBatches()
	//process invalid txs in batch
	mBatches := pool.processInvalidTxs(fetchedBatches)
	if len(mBatches) == 0 {
		return
	}
	// prune txs
	startPruneTime := utils.CurrentTimeMillisSeconds()
	var errBatchIds []string
	txsTable, batchIds, errBatchIds = pool.pruneTxBatches(mBatches)
	// cache valid batches into recover
	startCacheTime := utils.CurrentTimeMillisSeconds()
	if len(batchIds) > 0 {
		validBatches := make([]*txpoolPb.TxBatch, 0, len(batchIds))
		batchIdsMap := createBatchIdsMap(batchIds)
		for _, mBatch := range mBatches {
			if _, ok := batchIdsMap[mBatch.getBatchId()]; ok {
				validBatches = append(validBatches, mBatch.getBatch())
			}
		}
		pool.recover.CacheFetchedTxBatches(blockHeight, validBatches)
	}
	// delete invalid batches in pendingCache
	startRemoveTime := utils.CurrentTimeMillisSeconds()
	if len(errBatchIds) > 0 {
		pool.queue.removeBatches(errBatchIds)
	}
	endTime := utils.CurrentTimeMillisSeconds()
	pool.log.DebugDynamic(func() string {
		return fmt.Sprintf("FetchTxBatches in detail, height:%d, batchIds:[valid:%v, err:%v], txIds:%v",
			blockHeight, printBatchIds(batchIds), printBatchIds(errBatchIds), printTxIdsInTxsTable(txsTable))
	})
	pool.log.Infof("FetchTxBatches, height:%d, batch:[fetch:%d,prune:%d], txs:%d, "+
		"time:[fetch:%d,prune:%d,cache:%d,remove:%d,total:%dms]",
		blockHeight, len(mBatches), len(errBatchIds), calcTxNumInTxsTable(txsTable),
		startPruneTime-startFetchTime, startCacheTime-startPruneTime,
		startRemoveTime-startCacheTime, endTime-startRemoveTime, endTime-startFetchTime)
	// return valid txs and batchIds
	return
}

// processInvalidTxs delete invalid txs in batch and create a new batch
func (pool *batchTxPool) processInvalidTxs(batches []*memTxBatch) []*memTxBatch {
	// no need process
	if len(batches) == 0 {
		return nil
	}
	// if there is invalid tx, rebuild a new batch
	newBatches := make([]*memTxBatch, 0, len(batches))
	invalidBatchIds := make([]string, 0, len(batches))
	oldBatchIds := make(map[string]struct{})
	for _, batch := range batches {
		if batch.getValidTxNum() < batch.getTxNum() {
			if newBatch, err := pool.reBuildMBatch(batch); err == nil {
				newBatches = append(newBatches, newBatch)
			}
			// if new batch create failed, we think it is invalid batch and remove it
			invalidBatchIds = append(invalidBatchIds, batch.getBatchId())
		} else {
			newBatches = append(newBatches, batch)
		}
		oldBatchIds[batch.getBatchId()] = struct{}{}
	}
	// if there is invalid tx in some batch
	// remove old batches
	if len(invalidBatchIds) > 0 {
		pool.queue.removeBatches(invalidBatchIds)
	}
	// and add new batches to pending
	for _, newBatch := range newBatches {
		if _, ok := oldBatchIds[newBatch.getBatchId()]; !ok {
			if len(newBatch.batch.Txs) == 1 && isConfigTx(newBatch.batch.Txs[0], pool.chainConf) {
				pool.queue.addConfigTxBatchToPending(newBatch)
			} else {
				pool.queue.addCommonTxBatchToPending(newBatch)
			}
		}
	}
	// return newBatches
	return newBatches
}

// ReGenTxBatchesWithRetryTxs Generate new batches by retryTxBatches and return batchIds of new batches for batch txPool
// then, put new batches into the pendingCache of pool
// and retry old batches retrieved by the batchIds into the queue of pool.
func (pool *batchTxPool) ReGenTxBatchesWithRetryTxs(blockHeight uint64, batchIds []string,
	retryTxs []*commonPb.Transaction) (newBatchIds []string, newTxsTable [][]*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(batchIds) == 0 || len(retryTxs) == 0 {
		pool.log.Warnf("ReGenTxBatchesWithRetryTxs, batchIds:%d or retryTxs:%d should not be nil",
			len(batchIds), len(retryTxs))
		return
	}
	// create new batches by retryTxs and put new batches into the pendingCache of configBatchPool or commonBatchPool
	// 1.create new batches and put new batches into the pendingCache
	var (
		retryTxsTable = pool.generateTxsForBatch(retryTxs)
		newMBatches   = make([]*memTxBatch, len(retryTxsTable))
		wg            sync.WaitGroup
	)
	for i, perTxs := range retryTxsTable {
		wg.Add(1)
		go func(i int, perTxs []*commonPb.Transaction) {
			defer wg.Done()
			// create new batch
			if newBatch, err := pool.buildTxBatch(perTxs); err == nil {
				// verify new batch
				if validMBatch, err := pool.validateTxBatch(newBatch, TxBatchByRetry); err == nil {
					// add valid batch to the pendingCache of configBatchPool or commonBatchPool
					if len(validMBatch.batch.Txs) == 1 && isConfigTx(validMBatch.batch.Txs[0], pool.chainConf) {
						pool.queue.addConfigTxBatchToPending(validMBatch)
					} else {
						pool.queue.addCommonTxBatchToPending(validMBatch)
					}
					// add new mBatch to newMBatches
					newMBatches[i] = validMBatch
				}
			}
		}(i, perTxs)
	}
	wg.Wait()
	// eliminate nil batch in newMBatches
	validMBatches := eliminateNilMBatches(newMBatches)
	if len(validMBatches) > 0 {
		// 2.put newBatches to recover
		pool.recover.CacheFetchedTxBatches(blockHeight, createBatchesByMBatches(validMBatches))
		// 3.calc new txsTable and batchIds
		newBatchIds = make([]string, 0, len(validMBatches))
		newTxsTable = make([][]*commonPb.Transaction, 0, len(validMBatches))
		for _, mBatch := range validMBatches {
			newBatchIds = append(newBatchIds, mBatch.getBatchId())
			newTxsTable = append(newTxsTable, mBatch.getValidTxs())
		}
		// 4.retry old batches to the queue of pool
		pool.retryTxBatches(batchIds)
	}
	pool.log.DebugDynamic(func() string {
		return fmt.Sprintf("ReGenTxBatchesWithRetryTxs in detail, height:%d, retryTxIds:%v, newBatchIds:%v, newTxIds:%v",
			blockHeight, printTxIdsInTxs(retryTxs), printBatchIds(batchIds), printTxIdsInTxsTable(newTxsTable))
	})
	pool.log.Infof("ReGenTxBatchesWithRetryTxs, height:%d, batchIds:%d, retryTxs:%d, newBatches:%d, newTxs:%d",
		blockHeight, len(batchIds), len(retryTxs), len(validMBatches), calcTxNumInTxsTable(newTxsTable))
	return
}

// ReGenTxBatchesWithRemoveTxs Remove removeTxBatches in batches that retrieved by the batchIds
// to create new batches for batch txPool and return batchIds of new batches
// then put new batches into the pendingCache of pool
// and delete old batches retrieved by the batchIds in pool.
func (pool *batchTxPool) ReGenTxBatchesWithRemoveTxs(blockHeight uint64, batchIds []string,
	removeTxs []*commonPb.Transaction) (newBatchIds []string, newTxsTable [][]*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(batchIds) == 0 || len(removeTxs) == 0 {
		pool.log.Warnf("ReGenTxBatchesWithRemoveTxs, batchIds:%d or removeTxs:%d should not be nil",
			len(batchIds), len(removeTxs))
		return
	}
	// remove batches that retrieved by the batchIds to create new batches.
	// then add new batches into the pendingCache of configBatchQueue and commonBatchQueues.
	// 1.get all batches by batchIds
	oldBatchesMap, _ := pool.queue.getTxBatches(batchIds, StageInQueueAndPending)
	// 2.remove old batches from configBatchQueue and commonBatchQueues
	pool.queue.removeBatches(batchIds)
	// 3.create new batch and put into the pendingCache of configBatchQueue and commonBatchQueues
	var (
		errTxIdsMap = createTxIdsMap(removeTxs)
		newMBatches = make([]*memTxBatch, len(oldBatchesMap))
		i           = -1
		wg          sync.WaitGroup
	)
	for _, mBatch := range oldBatchesMap {
		i++
		wg.Add(1)
		go func(i int, batch *txpoolPb.TxBatch) {
			defer wg.Done()
			// create new batch by eliminated txs, txs in batch has been ordered
			if newBatch, err := pool.buildTxBatch(eliminateErrTxs(batch.Txs, errTxIdsMap)); err == nil {
				// verify new batch
				if validMBatch, err := pool.validateTxBatch(newBatch, TxBatchByRetry); err == nil {
					// add valid batch to the pendingCache of configBatchPool or commonBatchPool
					if len(validMBatch.batch.Txs) == 1 && isConfigTx(validMBatch.batch.Txs[0], pool.chainConf) {
						pool.queue.addConfigTxBatchToPending(validMBatch)
					} else {
						pool.queue.addCommonTxBatchToPending(validMBatch)
					}
					// add new mBatch to newMBatches
					newMBatches[i] = validMBatch
				}
			}
		}(i, mBatch.batch)
	}
	wg.Wait()
	// eliminate nil batch in newMBatches
	validMBatches := eliminateNilMBatches(newMBatches)
	if len(validMBatches) > 0 {
		// 4.put newBatches to recover
		pool.recover.CacheFetchedTxBatches(blockHeight, createBatchesByMBatches(validMBatches))
		// 5.calc new batchIds
		newBatchIds = make([]string, 0, len(validMBatches))
		newTxsTable = make([][]*commonPb.Transaction, 0, len(validMBatches))
		for _, mBatch := range validMBatches {
			newBatchIds = append(newBatchIds, mBatch.getBatchId())
			newTxsTable = append(newTxsTable, mBatch.getValidTxs())
		}
	}
	pool.log.DebugDynamic(func() string {
		return fmt.Sprintf("ReGenTxBatchesWithRemoveTxs in detail, height:%d, removeTxIds:%v, newBatchIds:%v, newTxIds:%v",
			blockHeight, printTxIdsInTxs(removeTxs), printBatchIds(batchIds), printTxIdsInTxsTable(newTxsTable))
	})
	pool.log.Infof("ReGenTxBatchesWithRemoveTxs, height:%d, batchIds:%d, removeTxs:%d, newBatches:%d, newTxs:%d",
		blockHeight, len(batchIds), len(removeTxs), len(newBatchIds), calcTxNumInTxsTable(newTxsTable))
	return
}

// RemoveTxsInTxBatches Remove removeTxBatches in batches that retrieved by the batchIds
// to create new batches for batch txPool.
// then, put new batches into the queue of pool
// and delete old batches retrieved by the batchIds in pool.
func (pool *batchTxPool) RemoveTxsInTxBatches(batchIds []string, removeTxs []*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(batchIds) == 0 || len(removeTxs) == 0 {
		pool.log.Warnf("RemoveTxInTxBatches, batchIds:%d or removeTxs:%d should not be nil",
			len(batchIds), len(removeTxs))
		return
	}
	// remove removeTxBatches in batches that retrieved by the batchIds to create new batches.
	// then add new batches into the queue of configBatchQueue and commonBatchQueues.
	// 1.get all batches by batchIds
	oldBatchesMap, _ := pool.queue.getTxBatches(batchIds, StageInQueueAndPending)
	// 2.remove old batches from configBatchQueue and commonBatchQueues
	pool.queue.removeBatches(batchIds)
	// 3.create new batch and put into the queue of configBatchQueue and commonBatchQueues
	var (
		errTxIdsMap = createTxIdsMap(removeTxs)
		newMBatches = make([]*memTxBatch, len(oldBatchesMap))
		i           = -1
		wg          sync.WaitGroup
	)
	for _, mBatch := range oldBatchesMap {
		i++
		wg.Add(1)
		go func(i int, batch *txpoolPb.TxBatch) {
			defer wg.Done()
			// create new batch by eliminated txs, txs in batch has been ordered
			if newBatch, err := pool.buildTxBatch(eliminateErrTxs(batch.Txs, errTxIdsMap)); err == nil {
				// verify new batch
				if validMBatch, err := pool.validateTxBatch(newBatch, TxBatchByRetry); err == nil {
					// add valid batch to the queue of configBatchPool or commonBatchPool
					if len(validMBatch.batch.Txs) == 1 && isConfigTx(validMBatch.batch.Txs[0], pool.chainConf) {
						pool.queue.addConfigTxBatch(validMBatch)
					} else {
						pool.queue.addCommonTxBatch(validMBatch)
					}
					// add new mBatch to newMBatches
					newMBatches[i] = validMBatch
				}
			}
		}(i, mBatch.batch)
	}
	wg.Wait()
	pool.log.DebugDynamic(func() string {
		return fmt.Sprintf("RemoveTxsInTxBatches in detail, batchIds:%v, removeTxIds:%v",
			printBatchIds(batchIds), printTxIdsInTxs(removeTxs))
	})
	pool.log.Infof("RemoveTxsInTxBatches, batchIds:%d, removeTxs:%d, newBatches:%d",
		len(batchIds), len(removeTxs), len(eliminateNilMBatches(newMBatches)))
}

// GetTxsByTxIds Retrieves the transaction by the txIds from single or normal tx pool,
// and only return the txs it has.
// txsRet is the transaction in the tx pool, txsMis is the transaction not in the tx pool.
func (pool *batchTxPool) GetTxsByTxIds(txIds []string) (
	txsRet map[string]*commonPb.Transaction, txsMis map[string]struct{}) {
	// no implementation for batch pool
	pool.log.Warn("GetTxsByTxIds is no implementation for batch pool")
	return
}

// GetAllTxsByTxIds Retrieves all transactions by the txIds from single or normal tx pool synchronously.
// if there are some transactions lacked, it need to obtain them from the proposer.
// if tx pool get all transactions before timeout return txsRet, otherwise, return error.
func (pool *batchTxPool) GetAllTxsByTxIds(txIds []string, proposerId string, height uint64, timeoutMs int) (
	txsRet map[string]*commonPb.Transaction, err error) {
	// no implementation for batch pool
	pool.log.Warn("GetAllTxsByTxIds is no implementation for batch pool")
	return
}

// GetAllTxsByBatchIds Retrieves all transactions by the batchIds from batch tx pool synchronously.
// if there are some batches lacked, it need to obtain them from the proposer.
// if tx pool get all batches before timeout return txsRet, otherwise, return error.
func (pool *batchTxPool) GetAllTxsByBatchIds(batchIds []string, proposerId string, height uint64, timeoutMs int) (
	txsTable [][]*commonPb.Transaction, err error) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return nil, commonErrors.ErrTxPoolHasStopped
	}
	// should not be nil
	if len(batchIds) == 0 {
		return nil, fmt.Errorf("txIds should not be nil")
	}
	var (
		batchesRet  map[string]*memTxBatch
		batchMis    map[string]struct{}
		errBatchIds []string
	)
	// first get batches
	startFirstTime := utils.CurrentTimeMillisSeconds()
	batchesRet, batchMis = pool.queue.getTxBatches(batchIds, StageInQueueAndPending)
	if len(batchesRet) == len(batchIds) && len(batchMis) == 0 {
		startFirstPruneTime := utils.CurrentTimeMillisSeconds()
		txsTable, _, errBatchIds = pool.pruneTxBatches(createBatchSlice(batchesRet, batchIds))
		startFirstRemoveTime := utils.CurrentTimeMillisSeconds()
		if len(errBatchIds) > 0 {
			pool.queue.removeBatches(errBatchIds)
		}
		endFirstTime := utils.CurrentTimeMillisSeconds()
		pool.log.DebugDynamic(func() string {
			return fmt.Sprintf("GetAllTxsByBatchIds in detail, height:%d, first, batchIds:[want:%v, err:%v], txIds:%v",
				height, printBatchIds(batchIds), printBatchIds(errBatchIds), printTxIdsInTxsTable(txsTable))
		})
		pool.log.Infof("GetAllTxsByBatchIds, height:%d, first, batch:[want:%d,get:%d], txs:%d, "+
			"time:[get:%d,prune:%d,remove:%d,total:%dms]",
			height, len(batchIds), len(batchesRet), calcTxNumInTxsTable(txsTable),
			startFirstPruneTime-startFirstTime, startFirstRemoveTime-startFirstPruneTime,
			endFirstTime-startFirstRemoveTime, endFirstTime-startFirstTime)
		return txsTable, nil
	}
	// second get batches after 10ms
	startSecondTime := utils.CurrentTimeMillisSeconds()
	var (
		secBatches     map[string]*memTxBatch
		secBatchIdsMis = make([]string, 0, len(batchMis))
	)
	for batchId := range batchMis {
		secBatchIdsMis = append(secBatchIdsMis, batchId)
	}
	select { // nolint
	case <-time.After(defaultSecGetTime * time.Millisecond):
		secBatches, batchMis = pool.queue.getTxBatches(secBatchIdsMis, StageInQueueAndPending)
	}
	if len(batchesRet) == 0 {
		batchesRet = make(map[string]*memTxBatch, len(batchIds))
	}
	for batchId, batch := range secBatches {
		batchesRet[batchId] = batch
	}
	if len(batchesRet) == len(batchIds) && len(batchMis) == 0 {
		startSecondPruneTime := utils.CurrentTimeMillisSeconds()
		txsTable, _, errBatchIds = pool.pruneTxBatches(createBatchSlice(batchesRet, batchIds))
		startSecondRemoveTime := utils.CurrentTimeMillisSeconds()
		if len(errBatchIds) > 0 {
			pool.queue.removeBatches(errBatchIds)
		}
		endSecondTime := utils.CurrentTimeMillisSeconds()
		pool.log.DebugDynamic(func() string {
			return fmt.Sprintf("GetAllTxsByBatchIds in detail, height:%d, second, batchIds:[want:%v, err:%v], txIds:%v",
				height, printBatchIds(batchIds), printBatchIds(errBatchIds), printTxIdsInTxsTable(txsTable))
		})
		pool.log.Infof("GetAllTxsByBatchIds, height:%d, second, batch:[want:%d,get:%d], txs:%d, "+
			"time:[first:%dms] [get:%d,prune:%d,remove:%d,total:%dms]",
			height, len(batchIds), len(batchesRet), calcTxNumInTxsTable(txsTable), startSecondTime-startFirstTime,
			startSecondPruneTime-startSecondTime-defaultSecGetTime, startSecondRemoveTime-startSecondPruneTime,
			endSecondTime-startSecondRemoveTime, endSecondTime-startSecondTime)
		return txsTable, nil
	}
	// recover synchronously
	startRecoverTime := utils.CurrentTimeMillisSeconds()
	batchesRet, batchMis = pool.recover.RecoverTxBatches(batchesRet, batchMis, proposerId, height, timeoutMs)
	if len(batchesRet) == len(batchIds) && len(batchMis) == 0 {
		startRecoverPruneTime := utils.CurrentTimeMillisSeconds()
		txsTable, _, errBatchIds = pool.pruneTxBatches(createBatchSlice(batchesRet, batchIds))
		startRecoverRemoveTime := utils.CurrentTimeMillisSeconds()
		if len(errBatchIds) > 0 {
			pool.queue.removeBatches(errBatchIds)
		}
		endRecoverTime := utils.CurrentTimeMillisSeconds()
		pool.log.DebugDynamic(func() string {
			return fmt.Sprintf("GetAllTxsByBatchIds in detail, height:%d, recover, batchIds:[want:%v, err:%v], txIds:%v",
				height, printBatchIds(batchIds), printBatchIds(errBatchIds), printTxIdsInTxsTable(txsTable))
		})
		pool.log.Infof("GetAllTxsByBatchIds, height:%d, recover, batch:[want:%d,get:%d], txs:%d, "+
			"time:[first:%dms,second:%dms] [recover:%d,prune:%d,remove:%d,total:%dms]",
			height, len(batchIds), len(batchesRet), calcTxNumInTxsTable(txsTable),
			startSecondTime-startFirstTime, startRecoverTime-startSecondTime,
			startRecoverPruneTime-startRecoverTime, startRecoverRemoveTime-startRecoverPruneTime,
			endRecoverTime-startRecoverRemoveTime, endRecoverTime-startRecoverTime)
		return txsTable, nil
	}
	pool.log.Warnf("GetAllTxsByBatchIds failed, height:%d, can not get all batches in %dms, batch:[want:%d,get:%d]",
		height, maxValue(uint64(timeoutMs), defaultRecoverTimeMs), len(batchIds), len(batchesRet))
	return nil, fmt.Errorf("can not get all batches, want:%d, get:%d", len(batchIds), len(batchesRet))
}

// AddTxsToPendingCache These transactions will be added to single or normal tx pool to avoid the transactions
// are fetched again and re-filled into the new block. Because of the chain confirmation
// rule in the HotStuff consensus algorithm.
func (pool *batchTxPool) AddTxsToPendingCache(txs []*commonPb.Transaction, blockHeight uint64) {
	// no implementation for batch pool
	pool.log.Warn("AddTxsToPendingCache is no implementation for batch pool")
}

// AddTxBatchesToPendingCache These transactions will be added to batch tx pool to avoid the transactions
// are fetched again and re-filled into the new block. Because of the chain confirmation
// rule in the HotStuff consensus algorithm.
func (pool *batchTxPool) AddTxBatchesToPendingCache(batchIds []string, blockHeight uint64) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(batchIds) == 0 {
		return
	}
	startTime := utils.CurrentTimeMillisSeconds()
	pool.queue.moveBatchesToPending(batchIds)
	pool.log.Infof("AddTxBatchesToPendingCache, height:%d, batchIds:%d,time:%dms",
		blockHeight, len(batchIds), utils.CurrentTimeMillisSeconds()-startTime)
}

// RetryAndRemoveTxs Process transactions within multiple proposed blocks at the same height to
// ensure that these transactions are not lost for single or normal tx pool
// re-add valid txs which that are not on local node.
// remove txs in the commit block.
func (pool *batchTxPool) RetryAndRemoveTxs(retryTxs []*commonPb.Transaction, removeTxs []*commonPb.Transaction) {
	// no implementation for batch pool
	pool.log.Warn("RetryAndRemoveTxs is no implementation for batch pool")
}

// RetryAndRemoveTxBatches Process transactions within multiple proposed blocks at the same height to
// ensure that these transactions are not lost for batch tx pool
// re-add valid txs which that are not on local node, if retryTxBatches is nil, only retry batches by retryBatchIds,
// otherwise, remove retryTxBatches in batches, create and add a new batches to pool.
// remove txs in the commit block, if removeTxBatches is nil, only remove batches by removeBatchIds,
// otherwise, remove removeTxBatches in batches, create and add a new batches to pool.
func (pool *batchTxPool) RetryAndRemoveTxBatches(retryBatchIds []string, removeBatchIds []string) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(retryBatchIds) == 0 && len(removeBatchIds) == 0 {
		return
	}
	// tell core engine SignalType_BLOCK_PROPOSE signal that it can package txs to build block
	defer pool.publishSignal()
	startTime := utils.CurrentTimeMillisSeconds()
	// first retry
	pool.retryTxBatches(retryBatchIds)
	// then remove
	pool.removeTxBatches(removeBatchIds)
	pool.log.Infof("RetryAndRemoveTxBatches, retryBatchIds:%d, removeBatchIds:%d,time:%dms",
		len(retryBatchIds), len(removeBatchIds), utils.CurrentTimeMillisSeconds()-startTime)
}

// retryTxBatch re-add txs to batchPool after validation
func (pool *batchTxPool) retryTxBatches(batchIds []string) {
	if len(batchIds) == 0 {
		return
	}
	// move batches from pendingCache to queue
	pool.queue.moveBatchesToQueue(batchIds, pool.retryTxBatchValidate)
}

// removeTxBatch remove txBatch after commit block
func (pool *batchTxPool) removeTxBatches(batchIds []string) {
	if len(batchIds) == 0 {
		return
	}
	// delete batches in pool
	pool.queue.removeBatches(batchIds)
}

// TxExists verifies whether the transaction exists in the tx pool.
func (pool *batchTxPool) TxExists(tx *commonPb.Transaction) (exist bool) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	if tx == nil {
		return
	}
	exist = pool.queue.txExist(tx)
	pool.log.Infof("TxExists, txId:%s, exist:%v", tx.Payload.TxId, exist)
	return
}

// GetPoolStatus return the max size of config transaction pool and common transaction pool,
// the num of config transaction in queue and pendingCache,
// and the the num of common transaction in queue and pendingCache.
func (pool *batchTxPool) GetPoolStatus() (txPoolStatus *txpoolPb.TxPoolStatus) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		txPoolStatus = &txpoolPb.TxPoolStatus{
			ConfigTxPoolSize: int32(MaxConfigTxPoolSize()),
			CommonTxPoolSize: int32(MaxCommonTxPoolSize()),
		}
		return
	}
	txPoolStatus = pool.queue.getPoolStatus()
	//pool.log.Infof("GetPoolStatus, status:%v", txPoolStatus)
	return
}

// GetTxIdsByTypeAndStage returns config or common txIds in different stage.
// TxType may be TxType_CONFIG_TX, TxType_COMMON_TX, (TxType_CONFIG_TX|TxType_COMMON_TX)
// TxStage may be TxStage_IN_QUEUE, TxStage_IN_PENDING, (TxStage_IN_QUEUE|TxStage_IN_PENDING)
func (pool *batchTxPool) GetTxIdsByTypeAndStage(txType, txStage int32) (txIds []string) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	_, txIds = pool.queue.getTxsByTxTypeAndStage(txType, txStage)
	pool.log.Infof("GetTxIdsByTypeAndStage, params:[type:%s,stage:%s], txs:%d",
		txpoolPb.TxType(txType), txpoolPb.TxStage(txStage), len(txIds))
	return
}

// GetTxsInPoolByTxIds Retrieve the transactions by the txIds from the tx pool.
func (pool *batchTxPool) GetTxsInPoolByTxIds(txIds []string) (
	txsRet []*commonPb.Transaction, txsMis []string, err error) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return nil, txIds, commonErrors.ErrTxPoolHasStopped
	}
	// should not be nil
	if len(txIds) == 0 {
		return nil, nil, fmt.Errorf("txIds shoule not be nil")
	}
	// should not be more than QueryUpperLimit
	if len(txIds) > QueryUpperLimit() {
		return nil, txIds, fmt.Errorf("the number of queried transactions cannot exceed %d",
			QueryUpperLimit())
	}
	txsRet = make([]*commonPb.Transaction, 0, len(txIds))
	txsMis = make([]string, 0, len(txIds))
	for _, txId := range txIds {
		if tx, exist := pool.queue.getTx(txId); exist {
			txsRet = append(txsRet, tx)
		} else {
			txsMis = append(txsMis, txId)
		}
	}
	pool.log.Infof("GetTxsInPoolByTxIds, want:%d, get:%d", len(txIds), len(txsRet))
	return
}

// publishSignal tell core engine to package
func (pool *batchTxPool) publishSignal() {
	if pool.msgBus == nil {
		return
	}
	// if there is config tx
	// or common tx more than BlockTxCapacity
	// put SignalType_BLOCK_PROPOSE to core
	if pool.queue.configTxCount() > 0 || int(pool.queue.commonTxCount()) > MaxTxCount(pool.chainConf) {
		pool.msgBus.Publish(msgbus.TxPoolSignal, &txpoolPb.TxPoolSignal{
			SignalType: txpoolPb.SignalType_BLOCK_PROPOSE,
			ChainId:    pool.chainId,
		})
	}
}

func (pool *batchTxPool) OnQuit() {
	pool.log.Infof("batch pool on quit message bus")
}

// Stop stop tx_pool service
func (pool *batchTxPool) Stop() error {
	// stop logic: started => stopping => stopped
	// should not stop again
	// should not stop when starting
	if !atomic.CompareAndSwapInt32(&pool.stat, started, stopping) {
		pool.log.Errorf(commonErrors.ErrTxPoolStopFailed.String())
		return commonErrors.ErrTxPoolStopFailed
	}
	// dump tx batches in wal
	if err := pool.dumpTxs(); err != nil {
		// even if dump txs failed, it should stop tx_pool service
		pool.log.Warnf("dump txs failed, err:%v", err)
	}
	// unregister pool msg
	if pool.msgBus != nil {
		pool.msgBus.UnRegister(msgbus.RecvTxPoolMsg, pool)
	}
	// cancel context, stop createBatchLoop goroutine
	pool.cancel()
	// set stat to stopped
	if !atomic.CompareAndSwapInt32(&pool.stat, stopping, stopped) {
		pool.log.Errorf(commonErrors.ErrTxPoolStopFailed.String())
		return commonErrors.ErrTxPoolStopFailed
	}
	pool.log.Infof("stop batch pool success")
	return nil
}

// buildTxBatch build batch by txs
func (pool *batchTxPool) buildTxBatch(txs []*commonPb.Transaction) (*txpoolPb.TxBatch, error) {
	if len(txs) == 0 {
		return nil, fmt.Errorf("txs should not be nil, when build txBatch")
	}
	// sort txs
	sortedTxs := sortTxs(txs)
	// create batch
	batch := &txpoolPb.TxBatch{
		Txs: sortedTxs,
	}
	// generate batchId for batch
	if err := pool.generateBatchId(batch); err != nil {
		pool.log.Errorf("append batchId failed, err:%v", err)
		return nil, err
	}
	// add Size_ and TxIdsMap
	batch.Size_ = int32(len(sortedTxs))
	batch.TxIdsMap = createTxId2IndexMap(sortedTxs)
	// add signature for batch
	if err := pool.signBatch(batch); err != nil {
		pool.log.Errorf("sign batch failed, err:%v", err)
		return nil, err
	}
	return batch, nil
}

// generateBatchId generate batchId for batch
func (pool *batchTxPool) generateBatchId(batch *txpoolPb.TxBatch) error {
	// only contains Txs
	batchHash, err := hash.GetByStrType(pool.chainConf.ChainConfig().Crypto.Hash, mustMarshal(batch))
	if err != nil {
		pool.log.Errorf("calc batch hash failed, err:%v", err)
		return err
	}
	// batchId=timestamp(8)+nodeId(8)+batchHash(8)
	batch.BatchId = generateTimestamp() + cutoutNodeId(pool.nodeId) + cutoutBatchHash(batchHash)
	return nil
}

// signBatch add signature for batch
func (pool *batchTxPool) signBatch(batch *txpoolPb.TxBatch) error {
	batchIdBz := []byte(batch.BatchId)
	sig, err := pool.singer.Sign(pool.chainConf.ChainConfig().Crypto.Hash, batchIdBz)
	if err != nil {
		pool.log.Errorf("sign batch failed, err:%v", err)
		return err
	}
	serializeMember, err := pool.singer.GetMember()
	if err != nil {
		pool.log.Errorf("get serialize member failed, err:%v", err)
		return err
	}
	batch.Endorsement = &commonPb.EndorsementEntry{
		Signer:    serializeMember,
		Signature: sig,
	}
	return nil
}

// broadcastTxBatch broadcast batch to other nodes
func (pool *batchTxPool) broadcastTxBatch(batchMsg []byte) {
	if pool.msgBus == nil {
		return
	}
	// create txPoolMsg
	txPoolMsg := &txpoolPb.TxPoolMsg{
		Type:    txpoolPb.TxPoolMsgType_BATCH_TX,
		Payload: batchMsg,
	}
	// create netMsg
	netMsg := &netPb.NetMsg{
		Type:    netPb.NetMsg_TX,
		Payload: mustMarshal(txPoolMsg),
	}
	// broadcast netMsg
	pool.msgBus.Publish(msgbus.SendTxPoolMsg, netMsg)
}

// select a random node to send tx when tx pool is full
func (pool *batchTxPool) selectNode() string {
	// get consensus node list no contain self
	nodeIds := make([]string, 0, len(pool.chainConf.ChainConfig().Consensus.Nodes))
	for _, orgConfig := range pool.chainConf.ChainConfig().Consensus.Nodes {
		for _, nodeId := range orgConfig.NodeId {
			if nodeId != pool.nodeId {
				nodeIds = append(nodeIds, nodeId)
			}
		}
	}
	// only one consensus node
	if len(nodeIds) == 0 {
		return ""
	}
	// select a random node
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(nodeIds))))
	if err != nil {
		n = big.NewInt(0)
	}
	return nodeIds[n.Int64()]
}

// sendTx forward tx to a node
func (pool *batchTxPool) sendTx(txMsg []byte, to string) {
	if pool.msgBus == nil {
		return
	}
	// create txPoolMsg
	txPoolMsg := &txpoolPb.TxPoolMsg{
		Type:    txpoolPb.TxPoolMsgType_SINGLE_TX,
		Payload: txMsg,
	}
	// create netMsg
	netMsg := &netPb.NetMsg{
		Type:    netPb.NetMsg_TX,
		Payload: mustMarshal(txPoolMsg),
		To:      to,
	}
	// broadcast netMsg
	pool.msgBus.Publish(msgbus.SendTxPoolMsg, netMsg)
}

// generateTxsForBatch generate txs for  build batch by BatchMaxSize
func (pool *batchTxPool) generateTxsForBatch(txs []*commonPb.Transaction) (txsTable [][]*commonPb.Transaction) {
	// should not be nil
	if len(txs) == 0 {
		return
	}
	// split to config or common txs
	confTxs, commTxs := pool.splitTxsByType(txs)
	sortConfTxs := sortTxs(confTxs)
	sortCommTxs := sortTxs(commTxs)
	batchSize := BatchMaxSize(pool.chainConf)
	commonBatchNum := len(sortCommTxs) / batchSize
	txsTable = make([][]*commonPb.Transaction, 0, len(sortConfTxs)+commonBatchNum)
	// create txs for config batch
	for _, confTx := range sortConfTxs {
		txsTable = append(txsTable, []*commonPb.Transaction{confTx})
	}
	// create txs for common batch
	if len(sortCommTxs) == 0 {
		return
	}
	if commonBatchNum == 0 {
		txsTable = append(txsTable, sortCommTxs)
		return
	}
	for i := 0; i < commonBatchNum; i++ {
		var perTxs []*commonPb.Transaction
		if i != commonBatchNum-1 {
			perTxs = sortCommTxs[i*batchSize : (i+1)*batchSize]
		} else {
			perTxs = sortCommTxs[i*batchSize:]
		}
		txsTable = append(txsTable, perTxs)
	}
	return
}

// splitTxsByType split txs by type
func (pool *batchTxPool) splitTxsByType(txs []*commonPb.Transaction) (
	confTxs []*commonPb.Transaction, comTxs []*commonPb.Transaction) {
	confTxs = make([]*commonPb.Transaction, 0, len(txs))
	comTxs = make([]*commonPb.Transaction, 0, len(txs))
	for _, tx := range txs {
		if isConfigTx(tx, pool.chainConf) {
			// config tx
			// note: no need result in tx
			confTxs = append(confTxs, copyTx(tx))
		} else {
			// common tx
			// note: no need result in tx
			comTxs = append(comTxs, copyTx(tx))
		}
	}
	return
}

// dumpTxs dump all txs to file
func (pool *batchTxPool) dumpTxs() (err error) { // nolint
	dumpPath := path.Join(localconf.ChainMakerConfig.GetStorePath(), pool.chainId, dumpDir)
	// dump tx batches in recover
	batchRes := pool.recover.batchesCacheToProto()
	if len(batchRes) > 0 {
		for _, batchRec := range batchRes {
			// dump txBatchRecoverResponse
			if err = pool.dumpMsg(mustMarshal(batchRec), path.Join(dumpPath, recoverTxDir)); err != nil {
				pool.log.Errorf("dump batches failed, fetched in recover, err:%v", err)
				return err
			}
		}
	}
	pool.log.Infof("dump batches success, fetched in recover, batches:%d", len(batchRes))
	// if IsDumpTxsInQueue is true, dump config and common transaction in queue and pending
	if IsDumpTxsInQueue() {
		// dump config tx batches in queue
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE),
			dumpPath, confTxInQueDir); err != nil {
			return err
		}
		// dump common tx in batches queue
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE),
			dumpPath, commTxInQueDir); err != nil {
			return err
		}
		// dump config tx batches in pending
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING),
			dumpPath, confTxInPenDir); err != nil {
			return err
		}
		// dump common tx batches in pending
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING),
			dumpPath, commTxInPenDir); err != nil {
			return err
		}
		return
	}
	// no dump tx batches in queue and pending
	pool.log.Infof("no dump config and common batches in queue")
	return
}

// dumpTxsByTxTypeAndStage dump config or common tx batches in queue or pendingCache
func (pool *batchTxPool) dumpTxsByTxTypeAndStage(txType, txStage int32, dumpPath, txDir string) error {
	// get tx batches by type and stage
	var mBatches []*memTxBatch
	if txType == int32(txpoolPb.TxType_CONFIG_TX) {
		mBatches = pool.queue.getConfigTxBatchesByTxStage(txStage)
	} else if txType == int32(txpoolPb.TxType_COMMON_TX) {
		mBatches = pool.queue.getCommonTxBatchesByTxStage(txStage)
	} else {
		return fmt.Errorf("txType should be TxType_CONFIG_TX(1) or TxType_COMMON_TX(2)")
	}
	if len(mBatches) == 0 {
		return nil
	}
	for _, mBatch := range mBatches {
		// dump batches to wal
		if err := pool.dumpMsg(mustMarshal(mBatch.batch), path.Join(dumpPath, txDir)); err != nil {
			pool.log.Errorf("dump batches failed, err:%v", err)
			return err
		}
	}
	pool.log.Infof("dump batches success, type:%s, stage:%s, batches:%d",
		txpoolPb.TxType(txType), txpoolPb.TxStage(txStage), len(mBatches))
	return nil
}

// dumpMsg dump msg to dir
func (pool *batchTxPool) dumpMsg(bz []byte, dir string) (err error) {
	// open wal
	if pool.wal, err = lws.Open(dir); err != nil {
		return err
	}
	// write log entry
	if err = pool.wal.Write(0, bz); err != nil {
		return err
	}
	// flush to file
	return pool.wal.Flush()
}

// replayDumpedTx replay tx batches in batchRecover, pendingCache and queue
func (pool *batchTxPool) replayTxs() error { // nolint
	dumpPath := path.Join(localconf.ChainMakerConfig.GetStorePath(), pool.chainId, dumpDir)
	// replay tx batches in recover
	bzs, err := pool.replayMsg(path.Join(dumpPath, recoverTxDir))
	if err != nil {
		pool.log.Errorf("replay fetched batches in recover failed, err:%v", err)
		return err
	}
	heightRange := make([]uint64, 0, defaultTxBatchCacheSize)
	for _, bz := range bzs {
		// unMarshal txBatchRecoverResponse
		batchRes := &txpoolPb.TxBatchRecoverResponse{}
		if err = proto.Unmarshal(bz, batchRes); err != nil {
			pool.log.Errorf("unmarshal *txpoolPb.TxBatchRecoverResponse failed "+
				"(please delete dump_tx_wal when change txPool type), err:%v", err)
			return err
		}
		if batchRes.NodeId != pool.nodeId {
			pool.log.Warnf("replay txs failed, fetched in recover, nodeIdInPool:%s, nodeIdInWal:%s",
				pool.nodeId, batchRes.NodeId)
			continue
		}
		// put tx batches to recover
		heightRange = append(heightRange, batchRes.Height)
		pool.recover.CacheFetchedTxBatches(batchRes.Height, batchRes.TxBatches)
	}
	pool.log.Infof("replay batches success, fetched in recover, blocks:%d, height:%v", len(bzs), heightRange)
	// replay config tx batches in pending
	txCount := 0
	batches, err := pool.replayTxsByTxDir(dumpPath, confTxInPenDir)
	if err != nil {
		pool.log.Errorf("replay batches failed, config in pending, err:%v", err)
		return err
	}
	for _, batch := range batches {
		if len(batch.Txs) == 1 && isConfigTx(batch.Txs[0], pool.chainConf) {
			pool.queue.addConfigTxBatchToPending(newMemTxBatch(batch, 0, createFilter(batch.Txs)))
			txCount += len(batch.Txs)
		}
	}
	pool.log.Infof("replay batches success, config in pending, batches:%d, txs:%d", len(batches), txCount)
	// replay common tx batches in pending
	txCount = 0
	batches, err = pool.replayTxsByTxDir(dumpPath, commTxInPenDir)
	if err != nil {
		pool.log.Errorf("replay batches failed, common in pending, err:%v", err)
		return err
	}
	for _, batch := range batches {
		if len(batch.Txs) != 0 && !isConfigTx(batch.Txs[0], pool.chainConf) {
			pool.queue.addCommonTxBatchToPending(newMemTxBatch(batch, 0, createFilter(batch.Txs)))
			txCount += len(batch.Txs)
		}
	}
	pool.log.Infof("replay batches success, common in pending, batches:%d, txs:%d", len(batches), txCount)
	// no dump tx batches in queue, no replay tx batches
	if !IsDumpTxsInQueue() {
		pool.log.Infof("no dump and replay config and common batches in queue")
		return nil
	}
	// replay config tx batches in queue
	txCount = 0
	txValid := 0
	batches, err = pool.replayTxsByTxDir(dumpPath, confTxInQueDir)
	if err != nil {
		pool.log.Errorf("replay batches failed, config in queue, err:%v", err)
		return err
	}
	for _, batch := range batches {
		// should not be nil
		if batch == nil || len(batch.Txs) == 0 {
			continue
		}
		// verify whether tx pool is full
		if pool.queue.isFull(batch.Txs[0]) {
			break
		}
		if mBatch, err1 := pool.validateTxBatch(batch, TxBatchByRePlay); err1 == nil {
			// there is only one config tx in a batch
			// so it is impossible to exist other invalid txs
			if len(mBatch.batch.Txs) == 1 && isConfigTx(mBatch.batch.Txs[0], pool.chainConf) {
				pool.queue.addConfigTxBatch(mBatch)
				txValid += int(mBatch.getValidTxNum())
			}
		}
		txCount += len(batch.Txs)
	}
	pool.log.Infof("replay batches success, config in queue, batches:%d, txs:%d, valid txs:%d",
		len(batches), txCount, txValid)
	// replay common tx batches in queue
	txCount = 0
	txValid = 0
	batches, err = pool.replayTxsByTxDir(dumpPath, commTxInQueDir)
	if err != nil {
		pool.log.Errorf("replay batches failed, common in queue, err:%v", err)
		return err
	}
	for _, batch := range batches {
		// should not be nil
		if batch == nil || len(batch.Txs) == 0 {
			continue
		}
		// verify whether tx pool is full
		if pool.queue.isFull(batch.Txs[0]) {
			break
		}
		if mBatch, err1 := pool.validateTxBatch(batch, TxBatchByRePlay); err1 == nil {
			// delete invalid txs in batch
			if mBatch.getValidTxNum() < mBatch.getTxNum() {
				if mBatch, err = pool.reBuildMBatch(mBatch); err != nil {
					pool.log.Errorf("rebuild batch failed, err:%v", err)
					return err
				}
			}
			// add batch into queue
			if len(mBatch.batch.Txs) != 0 && !isConfigTx(mBatch.batch.Txs[0], pool.chainConf) {
				pool.queue.addCommonTxBatch(mBatch)
				txValid += int(mBatch.getValidTxNum())
			}
		}
		txCount += len(batch.Txs)
	}
	pool.log.Infof("replay batches success, common in queue, batches:%d, txs:%d, valid txs:%d",
		len(batches), txCount, txValid)
	// remove dir
	os.RemoveAll(dumpPath)
	return nil
}

// replayTxsByTxDir replay tx batches by txDir
func (pool *batchTxPool) replayTxsByTxDir(dumpPath, txDir string) (batches []*txpoolPb.TxBatch, err error) {
	// replay tx batches in txDir
	var batchBzs [][]byte
	batchBzs, err = pool.replayMsg(path.Join(dumpPath, txDir))
	if err != nil {
		pool.log.Errorf("replay batches failed, txDir:%s, err:%v", txDir, err)
		return
	}
	if len(batchBzs) == 0 {
		return
	}
	// return tx batches
	batches = make([]*txpoolPb.TxBatch, 0, len(batchBzs))
	for _, batchBz := range batchBzs {
		// unMarshal txBatch
		batch := &txpoolPb.TxBatch{}
		if err = proto.Unmarshal(batchBz, batch); err != nil {
			pool.log.Errorf("unmarshal *txpoolPb.TxBatch failed"+
				"(please delete dump_tx_wal when change txPool type), err:%v", err)
			return
		}
		// it is nodeId for the batchId of dumped batch in single or normal pool
		if len(batch.BatchId) == defaultBatchIdLen && len(batch.Txs) != 0 {
			batches = append(batches, batch)
		}
	}
	return
}

// replayMsg upload msp from dir
func (pool *batchTxPool) replayMsg(dir string) (bzs [][]byte, err error) {
	if pool.wal, err = lws.Open(dir); err != nil {
		return nil, err
	}
	bzs = make([][]byte, 0, 10)
	it := pool.wal.NewLogIterator()
	for it.HasNext() {
		data, err1 := it.Next().Get()
		if err1 != nil {
			return nil, err1
		}
		bzs = append(bzs, data)
	}
	return bzs, nil
}

// =====================================================================================================================
//                                                tool method
// =====================================================================================================================
// createTxId2IndexMap create txId to index map by txs
func createTxId2IndexMap(txs []*commonPb.Transaction) map[string]int32 {
	txIdsMap := make(map[string]int32, len(txs))
	for idx, tx := range txs {
		txIdsMap[tx.Payload.GetTxId()] = int32(idx)
	}
	return txIdsMap
}

// createTxIdsMap create txIds map by txs
func createTxIdsMap(txs []*commonPb.Transaction) map[string]struct{} {
	txIdsMap := make(map[string]struct{}, len(txs))
	for _, tx := range txs {
		txIdsMap[tx.Payload.GetTxId()] = struct{}{}
	}
	return txIdsMap
}

// createTxIdsByTxs create txIds by txs
func createTxIdsByTxs(txs []*commonPb.Transaction) (txIds []string) {
	txIds = make([]string, len(txs))
	for i, tx := range txs {
		txIds[i] = tx.Payload.TxId
	}
	return
}

// createBatchIdsMap create batchId map by batchIds
func createBatchIdsMap(batchIds []string) map[string]struct{} {
	batchIdsMap := make(map[string]struct{}, len(batchIds))
	for _, batchId := range batchIds {
		batchIdsMap[batchId] = struct{}{}
	}
	return batchIdsMap
}

// createBatchSlice create memTxBatch slice by memTxBatch map and ordered by batchIds
func createBatchSlice(mBatchesMap map[string]*memTxBatch, batchIds []string) (mBatches []*memTxBatch) {
	mBatches = make([]*memTxBatch, 0, len(mBatchesMap))
	for _, batchId := range batchIds {
		if mBatch, ok := mBatchesMap[batchId]; ok {
			mBatches = append(mBatches, mBatch)
		}
	}
	return
}

// createFilter create filter by txs
func createFilter(txs []*commonPb.Transaction) []bool {
	filter := make([]bool, len(txs))
	for i := 0; i < len(txs); i++ {
		filter[i] = true
	}
	return filter
}

// eliminateErrTxs eliminate err txs and return new txs
func eliminateErrTxs(txs []*commonPb.Transaction, errTxIdsMap map[string]struct{}) (newTxs []*commonPb.Transaction) {
	if len(txs) == 0 {
		return
	}
	if len(errTxIdsMap) == 0 {
		return txs
	}
	newTxs = make([]*commonPb.Transaction, 0, len(txs))
	for _, tx := range txs {
		if _, ok := errTxIdsMap[tx.Payload.TxId]; !ok {
			newTxs = append(newTxs, tx)
		}
	}
	return
}

// eliminateNilMBatches eliminate nil mBatch and return a new mBatches
func eliminateNilMBatches(batches []*memTxBatch) (newBatches []*memTxBatch) {
	if len(batches) == 0 {
		return
	}
	newBatches = make([]*memTxBatch, 0, len(batches))
	for _, batch := range batches {
		if batch != nil {
			newBatches = append(newBatches, batch)
		}
	}
	return
}

// createBatchesByMBatches create batches by mBatches
func createBatchesByMBatches(mBatches []*memTxBatch) (newBatches []*txpoolPb.TxBatch) {
	if len(mBatches) == 0 {
		return
	}
	newBatches = make([]*txpoolPb.TxBatch, 0, len(mBatches))
	for _, mBatch := range mBatches {
		if mBatch != nil {
			newBatches = append(newBatches, mBatch.batch)
		}
	}
	return
}
