/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	commonErrors "chainmaker.org/chainmaker/common/v2/errors"
	"chainmaker.org/chainmaker/common/v2/monitor"
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
	"github.com/prometheus/client_golang/prometheus"
)

// TxPoolType is tx_pool type
const TxPoolType = "SINGLE"

const (
	stopped  = 0
	started  = 1
	starting = 2
	stopping = 3
)

var _ protocol.TxPool = (*txPoolImpl)(nil)

type txPoolImpl struct {
	stat    int32  // Identification of module service startup, 0 is stopped, 1 is started, 2 is starting, 3 is stopping
	nodeId  string // The ID of node
	chainId string // The ID of chain

	queue    *txQueue         // the queue for store transactions
	cache    *txCache         // the cache to temporarily cache transactions
	addTxsCh chan *mempoolTxs // channel that receive the common transactions
	ctx      context.Context
	cancel   context.CancelFunc

	flushTicker int        // ticker to check whether the cache needs to be refreshed
	recover     *txRecover // transaction recover to cache packed txs, and sync txs for node
	wal         *lws.Lws   // the wal is used to store dumped transaction

	ac         protocol.AccessControlProvider // Access control provider to verify transaction signature and authority
	msgBus     msgbus.MessageBus              // Information interaction between modules
	chainConf  protocol.ChainConf             // ChainConfig contains chain config
	txFilter   protocol.TxFilter              // TxFilter is cuckoo filter
	blockStore protocol.BlockchainStore       // Store module implementation
	log        protocol.Logger                // Log

	metricTxExistInDBTime  *prometheus.HistogramVec // TxExistInDB verify time prometheus metric
	metricTxVerifySignTime *prometheus.HistogramVec // TxVerifySign verify time prometheus metric
}

// NewTxPoolImpl create single tx pool txPoolImpl
func NewTxPoolImpl(
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
	// chainId and nodeId should not be nil
	if len(chainId) == 0 {
		return nil, fmt.Errorf("no chainId in create txpool")
	}
	if len(nodeId) == 0 {
		return nil, fmt.Errorf("no nodeId in create txpool")
	}
	// set pool config
	TxPoolConfig = &txPoolConfig{}
	if err := mapstructure.Decode(poolConfig, TxPoolConfig); err != nil {
		return nil, err
	}
	MonitorEnabled = monitorEnabled
	var (
		ticker    = defaultFlushTicker
		addChSize = defaultChannelSize
	)
	// set addChSize
	if TxPoolConfig.AddTxChannelSize > 0 {
		addChSize = int(TxPoolConfig.AddTxChannelSize)
	}
	// set cacheFlushTicker
	if TxPoolConfig.CacheFlushTicker > 0 {
		ticker = int(TxPoolConfig.CacheFlushTicker)
	}
	// create txPoolImpl
	queue := newQueue(chainConf, log)
	ctx, cancel := context.WithCancel(context.Background())
	txPoolQueue := &txPoolImpl{
		nodeId:      nodeId,
		chainId:     chainId,
		queue:       queue,
		cache:       newTxCache(),
		addTxsCh:    make(chan *mempoolTxs, addChSize),
		ctx:         ctx,
		cancel:      cancel,
		flushTicker: ticker,
		recover:     newTxRecover(nodeId, queue, msgBus, log),
		ac:          ac,
		log:         log,
		msgBus:      msgBus,
		chainConf:   chainConf,
		txFilter:    txFilter,
		blockStore:  chainStore,
	}
	// if enable monitor create metric
	if MonitorEnabled {
		txPoolQueue.metricTxExistInDBTime = monitor.NewHistogramVec(
			monitor.SUBSYSTEM_TXPOOL,
			monitor.MetricValidateTxInDBTime,
			monitor.MetricValidateTxInDBTimeMetric,
			[]float64{0, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100},
			monitor.ChainId,
		)
		txPoolQueue.metricTxVerifySignTime = monitor.NewHistogramVec(
			monitor.SUBSYSTEM_TXPOOL,
			monitor.MetricValidateTxSignTime,
			monitor.MetricValidateTxSignTimeMetric,
			[]float64{0, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100},
			monitor.ChainId,
		)
	}
	return txPoolQueue, nil
}

// Start start the txPool service
func (pool *txPoolImpl) Start() (err error) {
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
	// start addTxLoop
	go pool.addTxLoop()
	// set stat to started
	if !atomic.CompareAndSwapInt32(&pool.stat, starting, started) {
		pool.log.Errorf(commonErrors.ErrTxPoolStartFailed.String())
		return commonErrors.ErrTxPoolStartFailed
	}
	pool.log.Infof("start single pool success")
	return
}

//UpdateTxPoolConfig including BatchMaxSize
func (pool *txPoolImpl) UpdateTxPoolConfig(param string, value string) error{
	pool.log.Warn("[SingleTxPool] update %v is no implementation for single pool", param)
	return nil
}

// addTxLoop listen addTxsCh and add transaction to txCache
func (pool *txPoolImpl) addTxLoop() {
	flushTicker := time.NewTicker(time.Duration(pool.flushTicker) * time.Second)
	defer flushTicker.Stop()
	for {
		select {
		// receive tx from addTxsCh
		case memTxs, ok := <-pool.addTxsCh:
			if !ok {
				pool.log.Warnf("addTxsCh has been closed")
				return
			}
			pool.flushOrAddTxsToCache(memTxs)
		// flush cache ticker
		case <-flushTicker.C:
			if pool.cache.isFlushByTime() && pool.cache.txCount() > 0 {
				pool.flushCommonTxToQueue(nil)
			}
		// stop goroutine
		case <-pool.ctx.Done():
			return
		}
	}
}

// flushOrAddTxsToCache flush and add transaction to txCache
func (pool *txPoolImpl) flushOrAddTxsToCache(memTxs *mempoolTxs) {
	// should not be nil
	if memTxs == nil || len(memTxs.mtxs) == 0 {
		return
	}
	defer func() {
		pool.log.Debugf("txPool status: %s, cache txs num: %d", pool.queue.status(), pool.cache.txCount())
	}()
	// config tx
	if memTxs.isConfigTxs {
		pool.flushConfigTxToQueue(memTxs)
		return
	}
	// common tx
	if pool.cache.isFlushByTxCount(memTxs) {
		pool.flushCommonTxToQueue(memTxs)
	} else {
		pool.cache.addMemoryTxs(memTxs)
	}
}

// flushConfigTxToQueue flush config transaction to txCache
func (pool *txPoolImpl) flushConfigTxToQueue(memTxs *mempoolTxs) {
	// put single to core
	defer pool.publishSignal()
	// add config tx to queue
	pool.queue.addTxsToConfigQueue(memTxs)
}

// flushCommonTxToQueue flush common transaction to txCache
func (pool *txPoolImpl) flushCommonTxToQueue(memTxs *mempoolTxs) {
	// put single to core
	// reset txCache
	defer func() {
		pool.publishSignal()
		pool.cache.reset()
	}()
	// split txs by source
	rpcTxs, p2pTxs, internalTxs := pool.cache.mergeAndSplitTxsBySource(memTxs)
	// add common tx to queue
	pool.queue.addTxsToCommonQueue(&mempoolTxs{mtxs: rpcTxs, source: protocol.RPC})
	pool.queue.addTxsToCommonQueue(&mempoolTxs{mtxs: p2pTxs, source: protocol.P2P})
	pool.queue.addTxsToCommonQueue(&mempoolTxs{mtxs: internalTxs, source: protocol.INTERNAL})
}

// OnMessage Process messages from MsgBus
func (pool *txPoolImpl) OnMessage(msg *msgbus.Message) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if msg == nil {
		pool.log.Errorf("receiveOnMessage msg OnMessage msg is empty")
		return
	}
	// topic should be RecvTxPoolMsg
	if msg.Topic != msgbus.RecvTxPoolMsg {
		pool.log.Errorf("receiveOnMessage msg topic is not msgbus.RecvTxPoolMsg")
		return
	}
	// unMarshal to txPoolMsg
	txPoolBz := msg.Payload.(*netPb.NetMsg).Payload
	txPoolMsg := &txpoolPb.TxPoolMsg{}
	if err := proto.Unmarshal(txPoolBz, txPoolMsg); err != nil {
		pool.log.Warnf("OnMessage unmarshal txPoolMsg failed, warn:%v", err)
		return
	}
	switch txPoolMsg.Type {
	case txpoolPb.TxPoolMsgType_SINGLE_TX:
		// unMarshal to tx
		tx := &commonPb.Transaction{}
		if err := proto.Unmarshal(txPoolMsg.Payload, tx); err != nil {
			pool.log.Warnf("OnMessage unmarshal tx failed, err:%v", err)
			return
		}
		// add tx from p2p to pool
		_ = pool.AddTx(tx, protocol.P2P)
	case txpoolPb.TxPoolMsgType_RECOVER_REQ:
		// unMarshal to txRecoverRequest
		txRecoverReq := &txpoolPb.TxRecoverRequest{}
		if err := proto.Unmarshal(txPoolMsg.Payload, txRecoverReq); err != nil {
			pool.log.Warnf("OnMessage unmarshal txRecoverRequest failed, err:%v", err)
			return
		}
		pool.log.Debugw("OnMessage receive txRecoverRequest", "from", txRecoverReq.NodeId,
			"height", txRecoverReq.Height, "txs", len(txRecoverReq.TxIds))
		// process txRecoverRequest
		pool.recover.ProcessRecoverReq(txRecoverReq)
	case txpoolPb.TxPoolMsgType_RECOVER_RESP:
		// unMarshal to txRecoverResponse
		txRecoverRes := &txpoolPb.TxRecoverResponse{}
		if err := proto.Unmarshal(txPoolMsg.Payload, txRecoverRes); err != nil {
			pool.log.Warnf("OnMessage unmarshal txRecoverResponse failed, err:%v", err)
			return
		}
		pool.log.Debugw("OnMessage receive txRecoverResponse", "from", txRecoverRes.NodeId,
			"height", txRecoverRes.Height, "txs", len(txRecoverRes.Txs))
		// put single to core
		defer pool.publishSignal()
		txs := txRecoverRes.Txs
		// txs should not be nil
		if len(txs) == 0 {
			return
		}
		// attention: even if the pool is full, put it into pool !!!
		// validate txs
		mTxs, validTxs := pool.validateTxs(txs, protocol.P2P)
		if len(mTxs) == 0 || len(validTxs) == 0 {
			pool.log.Warnw("OnMessage receive txRecoverResponse", "from", txRecoverRes.NodeId,
				"height", txRecoverRes.Height, "txs", len(txRecoverRes.Txs), "validTxs", 0)
			return
		}
		memTxs := &mempoolTxs{isConfigTxs: false, mtxs: mTxs, source: protocol.P2P}
		if isConfigTx(validTxs[0], pool.chainConf) {
			memTxs.isConfigTxs = true
		}
		// add txs to pool
		pool.flushOrAddTxsToCache(memTxs)
		txRecoverRes.Txs = validTxs
		// process recover response
		pool.recover.ProcessRecoverRes(txRecoverRes)
	default:
		pool.log.Warnf("OnMessage receive invalid message type %s", txPoolMsg.Type)
	}
}

// AddTx add tx form RPC or P2P to pool
func (pool *txPoolImpl) AddTx(tx *commonPb.Transaction, source protocol.TxSource) error {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return commonErrors.ErrTxPoolHasStopped
	}
	// should not be nil
	if tx == nil || tx.Payload == nil || len(tx.Payload.TxId) == 0 {
		return commonErrors.ErrStructEmpty
	}
	// the transaction from RPC or P2P can put into pool by AddTx
	if source == protocol.INTERNAL {
		return commonErrors.ErrTxSource
	}
	// verify whether the tx exist in pool
	if pool.queue.has(tx, true) {
		pool.log.Warnf("transaction exists in pool, txId:%s", tx.Payload.TxId)
		return commonErrors.ErrTxIdExist
	}
	// verify whether the tx is out of date, signature/format is right(P2P), tx exist in db
	mtx, err := pool.validateTx(tx, source)
	if err != nil {
		return err
	}
	// verify whether the tx pool is full, even if tx pool is full, broadcast valid tx from RPC to other nodes
	if pool.isFull(tx) {
		if source == protocol.RPC {
			pool.broadcastTx(tx.Payload.TxId, mustMarshal(tx))
		}
		return commonErrors.ErrTxPoolLimit
	}
	pool.log.Debugw("AddTx", "txId", tx.Payload.TxId, "source", source)
	// attempt to add transaction to pool
	memTxs := &mempoolTxs{isConfigTxs: false, mtxs: []*memTx{mtx}, source: source}
	if isConfigTx(tx, pool.chainConf) {
		memTxs.isConfigTxs = true
	}
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case pool.addTxsCh <- memTxs:
	case <-t.C:
		pool.log.Warnf("add transaction timeout, txId:%s, source:%v", tx.Payload.TxId, source)
		return fmt.Errorf("add transaction timeout, txId:%s", tx.Payload.TxId)
	}
	// broadcast the transaction to other nodes
	if source == protocol.RPC {
		// simulate no broadcast transaction to test recover mechanism
		if localconf.ChainMakerConfig.DebugConfig.IsNoBroadcastTx {
			pool.log.Warnf("simulate no broadcast transaction, txId:%s", tx.Payload.TxId)
		} else {
			// broadcast transaction
			// attention: must shallow copy transaction!!!
			// While marshaling transaction, VM module may be adding execution result to the transaction for proposer,
			// which can cause panic.
			pool.broadcastTx(tx.Payload.TxId, mustMarshal(copyTx(tx)))
		}
	}
	return nil
}

// FetchTxs Get some transactions from single or normal txPool by block height to generate new block.
// return transactions.
func (pool *txPoolImpl) FetchTxs(blockHeight uint64) (txs []*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// fetch memTxs
	startFetchTime := utils.CurrentTimeMillisSeconds()
	mtxs := pool.queue.fetch(MaxTxCount(pool.chainConf))
	if len(mtxs) == 0 {
		return
	}
	// prune memTxs and return txs
	startPruneTime := utils.CurrentTimeMillisSeconds()
	txs = pool.pruneFetchedTxsExistDB(mtxs)
	// cache txs in tx recover
	startCacheTime := utils.CurrentTimeMillisSeconds()
	if IsMessageTurbo(pool.chainConf) && len(txs) > 0 {
		pool.recover.CacheFetchedTxs(blockHeight, txs)
	}
	endTime := utils.CurrentTimeMillisSeconds()
	pool.log.DebugDynamic(func() string {
		return fmt.Sprintf("FetchTxs in detail, height:%d, txIds:%v",
			blockHeight, printTxIdsInTxs(txs))
	})
	pool.log.Infof("FetchTxs, height:%d, txs:[fetch:%d,prune:%d], "+
		"time:[fetch:%d,prune:%d,cache:%d,total:%dms]",
		blockHeight, len(txs), len(mtxs)-len(txs),
		startPruneTime-startFetchTime, startCacheTime-startPruneTime, endTime-startCacheTime, endTime-startFetchTime)
	return
}

// FetchTxBatches Get some transactions from batch txPool by block height to generate new block.
// return transactions table and batchId list.
func (pool *txPoolImpl) FetchTxBatches(blockHeight uint64) (batchIds []string, txsTable [][]*commonPb.Transaction) {
	pool.log.Warn("FetchTxBatches is no implementation for single pool")
	return
}

// pruneFetchedTxsExistDB prune txs in incremental db after fetch a batch of txs
func (pool *txPoolImpl) pruneFetchedTxsExistDB(mtxs []*memTx) (txsRet []*commonPb.Transaction) {
	// should not be nil
	if len(mtxs) == 0 {
		return
	}
	// verify whether txs exist in the increment db
	mTxsTable := segmentMTxs(mtxs)
	// result table
	txsRetTable := make([][]*commonPb.Transaction, len(mTxsTable))
	txsInDBTable := make([][]*commonPb.Transaction, len(mTxsTable))
	// verify concurrently
	var wg sync.WaitGroup
	for i, perMtxs := range mTxsTable {
		if len(perMtxs) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int, perMtxs []*memTx) {
			defer wg.Done()
			txs := make([]*commonPb.Transaction, 0, len(perMtxs))
			txsInDB := make([]*commonPb.Transaction, 0)
			for _, mtx := range perMtxs {
				tx := mtx.getTx()
				if !pool.isTxExistsInIncrementDB(tx, mtx.dbHeight) {
					txs = append(txs, tx)
				} else {
					txsInDB = append(txsInDB, tx)
				}
			}
			// put result to table
			txsRetTable[i] = txs
			txsInDBTable[i] = txsInDB
		}(i, perMtxs)
	}
	wg.Wait()
	// delete tx in db and return txs not in db
	txsRet = make([]*commonPb.Transaction, 0, len(mtxs))
	for i := 0; i < len(txsRetTable); i++ {
		txsRet = append(txsRet, txsRetTable[i]...)
		// delete txs in the pool that have been commit
		// because the current verify whether tx exist in db is not executed in the queue.lock,
		// the proposer maybe pack txs that have been commit.
		pool.removeTxs(txsInDBTable[i])
	}
	return txsRet
}

// ReGenTxBatchesWithRetryTxs Generate new batches by retryTxs and return batchIds of new batches for batch txPool,
// then, put new batches into the pendingCache of pool
// and retry old batches retrieved by the batchIds into the queue of pool.
func (pool *txPoolImpl) ReGenTxBatchesWithRetryTxs(blockHeight uint64, batchIds []string,
	retryTxs []*commonPb.Transaction) (newBatchIds []string, newTxsTable [][]*commonPb.Transaction) {
	pool.log.Warn("ReGenTxBatchesWithRetryTxs is no implementation for single pool")
	return
}

// ReGenTxBatchesWithRemoveTxs Remove removeTxs in batches that retrieved by the batchIds
// to create new batches for batch txPool and return batchIds of new batches
// then put new batches into the pendingCache of pool
// and delete old batches retrieved by the batchIds in pool.
func (pool *txPoolImpl) ReGenTxBatchesWithRemoveTxs(blockHeight uint64, batchIds []string,
	removeTxs []*commonPb.Transaction) (newBatchIds []string, newTxsTable [][]*commonPb.Transaction) {
	pool.log.Warn("ReGenTxBatchesWithRemoveTxs is no implementation for single pool")
	return
}

// RemoveTxsInTxBatches Remove removeTxs in batches that retrieved by the batchIds
// to create new batches for batch txPool.
// then, put new batches into the queue of pool
// and delete old batches retrieved by the batchIds in pool.
func (pool *txPoolImpl) RemoveTxsInTxBatches(batchIds []string, removeTxs []*commonPb.Transaction) {
	pool.log.Warn("RemoveTxsInTxBatches is no implementation for single pool")
}

// GetTxsByTxIds Retrieve transactions by the txIds from single or normal txPool,
// and only return transactions it has.
// txsRet is the transaction in the txPool, txsMis is the transaction not in the txPool.
func (pool *txPoolImpl) GetTxsByTxIds(txIds []string) (
	txsRet map[string]*commonPb.Transaction, txsMis map[string]struct{}) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return nil, convertTxIdsSliceToMap(txIds)
	}
	// should not be nil
	if len(txIds) == 0 {
		return
	}
	startGetTime := utils.CurrentTimeMillisSeconds()
	// may be config tx
	if len(txIds) == 1 {
		txsRet = make(map[string]*commonPb.Transaction, 1)
		txsMis = make(map[string]struct{}, 1)
		txId := txIds[0]
		indb := 0
		if mtx, err := pool.queue.get(txId); err == nil {
			if !pool.isTxExistsInIncrementDB(mtx.getTx(), mtx.dbHeight) {
				txsRet[txId] = mtx.getTx()
			} else {
				indb++
				txsMis[txId] = struct{}{}
				pool.removeTxs([]*commonPb.Transaction{mtx.getTx()})
			}
		} else {
			txsMis[txId] = struct{}{}
		}
		pool.log.DebugDynamic(func() string {
			return fmt.Sprintf("GetTxsByTxIds in detail, txIds:%v",
				printTxIdsInTxsMap(txsRet))
		})
		pool.log.Infof("GetTxsByTxIds, txs:[want:%d,get:%d,prune:%d], time:[%dms]",
			len(txIds), len(txsRet), indb, utils.CurrentTimeMillisSeconds()-startGetTime)
		return txsRet, txsMis
	}
	// get common txs
	mtxsRet, mtxsMis := pool.queue.getCommonTxs(txIds)
	// prune txs and return txsRet and txsMis
	startPruneTime := utils.CurrentTimeMillisSeconds()
	txsRet, txsMis = pool.pruneGetTxsExistDB(mtxsRet, mtxsMis)
	endTime := utils.CurrentTimeMillisSeconds()
	pool.log.DebugDynamic(func() string {
		return fmt.Sprintf("GetTxsByTxIds in detail, txIds:%v",
			printTxIdsInTxsMap(txsRet))
	})
	pool.log.Infof("GetTxsByTxIds, txs:[want:%d,get:%d,prune:%d], time:[get:%d,prune:%d,total:%dms]",
		len(txIds), len(txsRet), len(mtxsRet)-len(txsRet),
		startPruneTime-startGetTime, endTime-startPruneTime, endTime-startGetTime)
	return txsRet, txsMis
}

// GetAllTxsByTxIds Retrieve all transactions by the txIds from single or normal txPool synchronously.
// if there are some transactions lacked, it need to obtain them by height from the proposer.
// if txPool get all transactions before timeout return txsRet, otherwise, return error.
func (pool *txPoolImpl) GetAllTxsByTxIds(txIds []string, proposerId string, height uint64, timeoutMs int) (
	txsRet map[string]*commonPb.Transaction, err error) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return nil, commonErrors.ErrTxPoolHasStopped
	}
	// should not be nil
	if len(txIds) == 0 {
		return nil, fmt.Errorf("txIds should not be nil")
	}
	var txsMis map[string]struct{}
	// first get txs
	startFirstGetTime := utils.CurrentTimeMillisSeconds()
	txsRet, txsMis = pool.GetTxsByTxIds(txIds)
	if len(txsRet) == len(txIds) && len(txsMis) == 0 {
		pool.log.Infof("GetAllTxsByTxIds, height:%d, first, want:%d, get:%d, time:%dms",
			height, len(txIds), len(txsRet), utils.CurrentTimeMillisSeconds()-startFirstGetTime)
		return txsRet, nil
	}
	// second get txs after 10ms
	startSecondGetTime := utils.CurrentTimeMillisSeconds()
	var secTxsRet map[string]*commonPb.Transaction
	txIdsMis := make([]string, 0, len(txsMis))
	for txId := range txsMis {
		txIdsMis = append(txIdsMis, txId)
	}
	select { // nolint
	case <-time.After(defaultSecGetTime * time.Millisecond):
		secTxsRet, txsMis = pool.GetTxsByTxIds(txIdsMis)
	}
	if len(txsRet) == 0 {
		txsRet = make(map[string]*commonPb.Transaction, len(txsMis))
	}
	for txId, tx := range secTxsRet {
		txsRet[txId] = tx
	}
	if len(txsRet) == len(txIds) && len(txsMis) == 0 {
		pool.log.Infof("GetAllTxsByTxIds, height:%d, second, want:%d, get:%d, time:[first:%d,second:%d,total:%dms]",
			height, len(txIds), len(txsRet), startSecondGetTime-startFirstGetTime,
			utils.CurrentTimeMillisSeconds()-startSecondGetTime, utils.CurrentTimeMillisSeconds()-startFirstGetTime)
		return txsRet, nil
	}
	// recover synchronously
	startRecoverTime := utils.CurrentTimeMillisSeconds()
	txsRet, txsMis = pool.recover.RecoverTxs(txsMis, txsRet, proposerId, height, timeoutMs)
	endTime := utils.CurrentTimeMillisSeconds()
	if len(txsRet) == len(txIds) && len(txsMis) == 0 {
		pool.log.Infof("GetAllTxsByTxIds, height:%d, recover, want:%d, get:%d, "+
			"time:[first:%d,second:%d,recover:%d, total:%dms]",
			height, len(txIds), len(txsRet), startSecondGetTime-startFirstGetTime,
			startRecoverTime-startSecondGetTime, endTime-startRecoverTime, endTime-startFirstGetTime)
		return txsRet, nil
	}
	pool.log.Warnf("GetAllTxsByTxIds failed, height:%d, can not get all txs in %dms, want:%d, get:%d, "+
		"time:[first:%d,second:%d,recover:%d,total:%dms]",
		height, maxVal(timeoutMs, defaultRecoverTimeMs), len(txIds), len(txsRet), startSecondGetTime-startFirstGetTime,
		startRecoverTime-startSecondGetTime, endTime-startRecoverTime, endTime-startFirstGetTime)
	return nil, fmt.Errorf("can not get all txs, want:%d, only get:%d", len(txIds), len(txsRet))
}

// pruneGetTxsExistDB prune txs in incremental db after get a batch of txs
func (pool *txPoolImpl) pruneGetTxsExistDB(mtxsMap map[string]*memTx, txsMis map[string]struct{}) (
	map[string]*commonPb.Transaction, map[string]struct{}) {
	// should not be nil
	if len(mtxsMap) == 0 {
		return nil, txsMis
	}
	// verify whether txs exist in db
	mTxsTable := segmentMTxs(convertMapToSlice(mtxsMap))
	// result table
	txsTable := make([][]*commonPb.Transaction, len(mTxsTable))
	txsInDBTable := make([][]*commonPb.Transaction, len(mTxsTable))
	// verify concurrently
	var wg sync.WaitGroup
	for i, perMtxs := range mTxsTable {
		if len(perMtxs) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int, perMtxs []*memTx) {
			defer wg.Done()
			txs := make([]*commonPb.Transaction, 0, len(perMtxs))
			txsInDB := make([]*commonPb.Transaction, 0)
			for _, mtx := range perMtxs {
				tx := mtx.getTx()
				if pool.isTxExistsInIncrementDB(tx, mtx.dbHeight) {
					txsInDB = append(txsInDB, tx)
				} else {
					txs = append(txs, tx)
				}
			}
			// put result to table
			txsTable[i] = txs
			txsInDBTable[i] = txsInDB
		}(i, perMtxs)
	}
	wg.Wait()
	// return result
	txsRet := make(map[string]*commonPb.Transaction, len(mtxsMap))
	// delete tx in db and return txs not in db
	for i := 0; i < len(txsTable); i++ {
		for _, tx := range txsTable[i] {
			txsRet[tx.Payload.TxId] = tx
		}
		// delete txs in the pool that have been commit
		// because the current verify whether tx exist in db is not executed in the queue.lock,
		// the proposer maybe pack txs that have been commit.
		txsInDB := txsInDBTable[i]
		for _, tx := range txsInDB {
			txsMis[tx.Payload.TxId] = struct{}{}
		}
		pool.removeTxs(txsInDB)
	}
	return txsRet, txsMis
}

// GetAllTxsByBatchIds Retrieve all transactions by the batchIds from batch txPool synchronously.
// if there are some batches lacked, it need to obtain them by height from the proposer.
// if txPool get all batches before timeout return txsRet, otherwise, return error.
func (pool *txPoolImpl) GetAllTxsByBatchIds(batchIds []string, proposerId string, height uint64, timeoutMs int) (
	txsTable [][]*commonPb.Transaction, err error) {
	// no implementation for single pool
	pool.log.Warn("GetAllTxsByBatchIds is no implementation for single pool")
	return
}

// AddTxsToPendingCache These transactions will be added to the cache to avoid the transactions
// are fetched again and re-filled into the new block. Because of the chain confirmation
// rule in the HotStuff consensus algorithm.
func (pool *txPoolImpl) AddTxsToPendingCache(txs []*commonPb.Transaction, blockHeight uint64) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(txs) == 0 {
		return
	}
	startTime := utils.CurrentTimeMillisSeconds()
	// config tx
	if len(txs) == 1 && isConfigTx(txs[0], pool.chainConf) {
		pool.queue.appendConfigTxsToPendingCache(txs, blockHeight)
		//common tx
	} else {
		pool.queue.appendCommonTxsToPendingCache(txs, blockHeight)
	}
	pool.log.Infof("AddTxsToPendingCache, txs:%d, time:[%dms]",
		len(txs), utils.CurrentTimeMillisSeconds()-startTime)
}

// AddTxBatchesToPendingCache These transactions will be added to batch txPool to avoid the transactions
// are fetched again and re-filled into the new block. Because of the chain confirmation
// rule in the HotStuff consensus algorithm.
func (pool *txPoolImpl) AddTxBatchesToPendingCache(batchIds []string, blockHeight uint64) {
	// no implementation for single pool
	pool.log.Warn("AddTxBatchesToPendingCache is no implementation for single pool")
}

// RetryAndRemoveTxs Process transactions within multiple proposed blocks at the same height to
// ensure that these transactions are not lost, re-add valid txs which that are not on local node.
// remove txs in the commit block.
func (pool *txPoolImpl) RetryAndRemoveTxs(retryTxs []*commonPb.Transaction, removeTxs []*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(retryTxs) == 0 && len(removeTxs) == 0 {
		return
	}
	start := utils.CurrentTimeMillisSeconds()
	// first retry
	pool.retryTxs(retryTxs)
	// then remove
	pool.removeTxs(removeTxs)
	pool.log.Infof("RetryAndRemoveTxs, retryTxs:%d, removeTxs:%d, time:[%dms]",
		len(retryTxs), len(removeTxs), utils.CurrentTimeMillisSeconds()-start)
}

// RetryAndRemoveTxBatches Process transactions within multiple proposed blocks at the same height to
// ensure that these transactions are not lost for batch txPool
// re-add valid transactions which that are not on local node, if retryTxs is nil, only retry batches by retryBatchIds,
// otherwise, create and add new batches by retryTxs to pool, and then retry batches by retryBatchIds.
// remove txs in the commit block, if removeTxs is nil, only remove batches by removeBatchIds,
// otherwise, remove removeTxs in batches, create and add a new batches to pool, and remove batches by removeBatchIds.
func (pool *txPoolImpl) RetryAndRemoveTxBatches(retryBatchIds []string, removeBatchIds []string) {
	// no implementation for single pool
	pool.log.Warn("RetryAndRemoveTxBatches is no implementation for single pool")
}

// retryTxs Re-add the txs to txPool
func (pool *txPoolImpl) retryTxs(txs []*commonPb.Transaction) {
	// should not be nil
	if len(txs) == 0 {
		return
	}
	// split txs by type
	configTxs, _, commonTxs, _ := pool.splitTxsByType(txs)
	// config txs
	if len(configTxs) > 0 {
		// first delete txs in pending
		pool.queue.deleteConfigTxsInPending(configTxs)
		// validate txs
		mTxs, _ := pool.validateTxs(configTxs, protocol.INTERNAL)
		pool.log.Debugf("retry config txs:%d, after validate:%d", len(configTxs), len(mTxs))
		if len(mTxs) > 0 {
			// add validate txs to pending
			pool.queue.addTxsToConfigQueue(&mempoolTxs{isConfigTxs: true, mtxs: mTxs, source: protocol.INTERNAL})
		}
	}
	// common txs
	if len(commonTxs) > 0 {
		// first delete txs in pending
		pool.queue.deleteCommonTxsInPending(commonTxs)
		// validate txs
		mTxs, _ := pool.validateTxs(commonTxs, protocol.INTERNAL)
		pool.log.Debugf("retry common txs:%d, after validate:%d", len(commonTxs), len(mTxs))
		if len(mTxs) > 0 {
			// add validate txs to pending
			pool.queue.addTxsToCommonQueue(&mempoolTxs{isConfigTxs: false, mtxs: mTxs, source: protocol.INTERNAL})
		}
	}
}

// removeTxs delete the txs from the pool
func (pool *txPoolImpl) removeTxs(txs []*commonPb.Transaction) {
	// should not be nil
	if len(txs) == 0 {
		return
	}
	defer pool.publishSignal()
	// split txs by type
	_, configTxIds, _, commonTxIds := pool.splitTxsByType(txs)
	// config txs
	if len(configTxIds) > 0 {
		pool.log.Debugf("remove config txs:%d", len(configTxIds))
		pool.queue.deleteConfigTxs(configTxIds)
	}
	// common txs
	if len(commonTxIds) > 0 {
		pool.log.Debugf("remove common txs:%d", len(commonTxIds))
		pool.queue.deleteCommonTxs(commonTxIds)
	}
}

// TxExists verifies whether the transaction exists in the tx_pool
func (pool *txPoolImpl) TxExists(tx *commonPb.Transaction) (exist bool) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		exist = false
		return
	}
	exist = pool.queue.has(tx, true)
	pool.log.Infof("TxExists, txId:%s, exist:%v", tx.Payload.TxId, exist)
	return
}

// GetPoolStatus return the max size of config tx pool and common tx pool,
// the num of config tx in normal queue and pending queue,
// and the the num of config tx in normal queue and pending queue.
func (pool *txPoolImpl) GetPoolStatus() (poolStatus *txpoolPb.TxPoolStatus) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		poolStatus = &txpoolPb.TxPoolStatus{
			ConfigTxPoolSize: int32(MaxConfigTxPoolSize()),
			CommonTxPoolSize: int32(MaxCommonTxPoolSize()),
		}
		return
	}
	poolStatus = pool.queue.getPoolStatus()
	//pool.log.Infof("GetPoolStatus, status:%v", poolStatus)
	return
}

// GetTxIdsByTypeAndStage returns config or common txs in different stage.
// TxType are TxType_CONFIG_TX, TxType_COMMON_TX, (TxType_CONFIG_TX|TxType_COMMON_TX)
// TxStage are TxStage_IN_QUEUE, TxStage_IN_PENDING, (TxStage_IN_QUEUE|TxStage_IN_PENDING)
func (pool *txPoolImpl) GetTxIdsByTypeAndStage(txType, txStage int32) (txIds []string) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	_, txIds = pool.queue.getTxsByTxTypeAndStage(txType, txStage)
	pool.log.Infof("GetTxIdsByTypeAndStage, [type:%s,stage:%s], txs:%d",
		txpoolPb.TxType(txType), txpoolPb.TxStage(txStage), len(txIds))
	return
}

// GetTxsInPoolByTxIds Retrieve the transactions by the txIds from the txPool,
// return transactions in the txPool and txIds not in txPool.
// default query upper limit is 1w transaction, and error is returned if the limit is exceeded.
func (pool *txPoolImpl) GetTxsInPoolByTxIds(txIds []string) (
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
	// get txs in pool byt txIds
	txsRet = make([]*commonPb.Transaction, 0, len(txIds))
	txsMis = make([]string, 0, len(txIds))
	for _, txId := range txIds {
		if mtx, err := pool.queue.get(txId); err == nil {
			txsRet = append(txsRet, mtx.getTx())
		} else {
			txsMis = append(txsMis, txId)
		}
	}
	pool.log.Infof("GetTxsInPoolByTxIds, want:%d, get:%d", len(txIds), len(txsRet))
	return
}

// isFull Check whether the transaction pool is full
func (pool *txPoolImpl) isFull(tx *commonPb.Transaction) bool {
	// config tx
	if isConfigTx(tx, pool.chainConf) {
		if pool.queue.configTxsCount() >= MaxConfigTxPoolSize() {
			pool.log.Warnf("AddTx configTxPool is full, txId:%s, configQueueSize:%d", tx.Payload.GetTxId(),
				pool.queue.configTxsCount())
			return true
		}
		return false
	}
	// common tx
	if pool.queue.commonTxsCount() >= MaxCommonTxPoolSize() {
		pool.log.Warnf("AddTx commonTxPool is full, txId:%s, commonTxQueueSize:%d", tx.Payload.GetTxId(),
			pool.queue.commonTxsCount())
		return true
	}
	return false
}

// broadcastTx broadcast transaction to other nodes
func (pool *txPoolImpl) broadcastTx(txId string, txMsg []byte) {
	if pool.msgBus != nil {
		// create txPoolMsg
		txPoolMsg := &txpoolPb.TxPoolMsg{
			Type:    txpoolPb.TxPoolMsgType_SINGLE_TX,
			Payload: txMsg,
		}
		// create netMsg
		netMsg := &netPb.NetMsg{
			Payload: mustMarshal(txPoolMsg),
			Type:    netPb.NetMsg_TX,
		}
		// broadcast netMsg
		pool.msgBus.Publish(msgbus.SendTxPoolMsg, netMsg)
		pool.log.Debugf("broadcastTx, txId:%s", txId)
	}
}

// splitTxsByType split txs by type
func (pool *txPoolImpl) splitTxsByType(txs []*commonPb.Transaction) (
	confTxs []*commonPb.Transaction, confTxIds []string, commTxs []*commonPb.Transaction, commTxIds []string) {
	if len(txs) == 0 {
		return
	}
	confTxs = make([]*commonPb.Transaction, 0, len(txs))
	confTxIds = make([]string, 0, len(txs))
	commTxs = make([]*commonPb.Transaction, 0, len(txs))
	commTxIds = make([]string, 0, len(txs))
	for _, tx := range txs {
		if isConfigTx(tx, pool.chainConf) {
			confTxs = append(confTxs, tx)
			confTxIds = append(confTxIds, tx.Payload.TxId)
		} else {
			commTxs = append(commTxs, tx)
			commTxIds = append(commTxIds, tx.Payload.TxId)
		}
	}
	return
}

// publishSignal When the number of transactions in the transaction pool is greater
// than or equal to the block can contain, update the status of the tx pool to block
// propose, otherwise update the status of tx pool to TRANSACTION_INCOME.
func (pool *txPoolImpl) publishSignal() {
	if pool.msgBus == nil {
		return
	}
	// if there is config txs
	// or there is more common txs than MaxTxCount
	// put SignalType_BLOCK_PROPOSE to core
	if pool.queue.configTxsCount() > 0 || pool.queue.commonTxsCount() >= MaxTxCount(pool.chainConf) {
		pool.msgBus.Publish(msgbus.TxPoolSignal, &txpoolPb.TxPoolSignal{
			SignalType: txpoolPb.SignalType_BLOCK_PROPOSE,
			ChainId:    pool.chainId,
		})
	}
}

// dumpTxs dump all txs to file
func (pool *txPoolImpl) dumpTxs() (err error) {
	dumpPath := path.Join(localconf.ChainMakerConfig.GetStorePath(), pool.chainId, dumpDir)
	// if block clipping is enabled, dump txs in recover
	if IsMessageTurbo(pool.chainConf) {
		// dump tx in recover
		txsRecRes := pool.recover.txCacheToProto()
		if len(txsRecRes) > 0 {
			for _, txsRes := range txsRecRes {
				bz, err1 := proto.Marshal(txsRes)
				if err1 != nil {
					pool.log.Errorf("marshal *txpoolPb.TxRecoverResponse failed, err:%v", err1)
					return err1
				}
				if err = pool.dumpMsg(bz, path.Join(dumpPath, recoverTxDir)); err != nil {
					pool.log.Errorf("dump fetched tx in recover failed, err:%v", err)
					return err
				}
			}
		}
		pool.log.Infof("dump txs success, fetched in recover, batches:%d", len(txsRecRes))
	}

	// if IsDumpTxsInQueue is true, dump config and common transaction in queue and pending
	if IsDumpTxsInQueue() {
		// dump config tx in queue
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_QUEUE),
			dumpPath, confTxInQueDir); err != nil {
			return err
		}
		// dump common tx in queue
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_QUEUE),
			dumpPath, commTxInQueDir); err != nil {
			return err
		}
		// dump config tx in pending
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_CONFIG_TX), int32(txpoolPb.TxStage_IN_PENDING),
			dumpPath, confTxInPenDir); err != nil {
			return err
		}
		// dump common tx in pending
		if err = pool.dumpTxsByTxTypeAndStage(int32(txpoolPb.TxType_COMMON_TX), int32(txpoolPb.TxStage_IN_PENDING),
			dumpPath, commTxInPenDir); err != nil {
			return err
		}
		return
	}
	pool.log.Infof("no dump config and common txs in queue and pending")
	return nil
}

// dumpTxsByTxTypeAndStage dump config or common txs in queue or pending
func (pool *txPoolImpl) dumpTxsByTxTypeAndStage(txType, txStage int32, dumpPath, txDir string) error {
	// get txs by tx type and stage
	txs, _ := pool.queue.getTxsByTxTypeAndStage(txType, txStage)
	if len(txs) == 0 {
		return nil
	}
	batchSize := maxVal(MaxTxCount(pool.chainConf), defaultMaxTxCount)
	batchNum := maxVal(len(txs)/batchSize, 1)
	for i := 0; i < batchNum; i++ {
		var txsInBatch []*commonPb.Transaction
		if i != batchNum-1 {
			txsInBatch = txs[i*batchSize : (i+1)*batchSize]
		} else {
			txsInBatch = txs[i*batchSize:]
		}
		// create txBatch
		txBatch := &txpoolPb.TxBatch{
			BatchId: pool.nodeId,
			Size_:   int32(len(txsInBatch)),
			Txs:     txsInBatch,
		}
		// marshal txBatch
		bz, err := proto.Marshal(txBatch)
		if err != nil {
			pool.log.Errorf("marshal *txpoolPb.TxBatch failed, err:%v", err)
			return err
		}
		// dump txBatch to wal
		if err = pool.dumpMsg(bz, path.Join(dumpPath, txDir)); err != nil {
			pool.log.Errorf("dump tx failed, err:%v", err)
			return err
		}
	}
	pool.log.Infof("dump txs success, type:%s, stage:%s, txs:%d",
		txpoolPb.TxType(txType), txpoolPb.TxStage(txStage), len(txs))
	return nil
}

// dumpMsg dump msg to dir
func (pool *txPoolImpl) dumpMsg(bz []byte, dir string) (err error) {
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

// replayDumpedTx replay tx in txRecover、pending and queue
func (pool *txPoolImpl) replayTxs() error { // nolint
	dumpPath := path.Join(localconf.ChainMakerConfig.GetStorePath(), pool.chainId, dumpDir)
	// replay txs in recover
	bzs, err := pool.replayMsg(path.Join(dumpPath, recoverTxDir))
	if err != nil {
		pool.log.Warnf("replay txs failed, fetched in recover, err:%v", err)
		return err
	}
	heightRange := make([]uint64, 0, defaultTxBatchCacheSize)
	for _, bz := range bzs {
		txRecRes := &txpoolPb.TxRecoverResponse{}
		if err = proto.Unmarshal(bz, txRecRes); err != nil {
			pool.log.Errorf("unmarshal *txpoolPb.TxRecoverResponse failed"+
				"(please delete dump_tx_wal when change txPool type), err:%v", err)
			return err
		}
		if txRecRes.NodeId != pool.nodeId {
			pool.log.Warnf("replay txs failed, fetched in recover, nodeIdInPool:%s, nodeIdInWal:%s",
				pool.nodeId, txRecRes.NodeId)
			continue
		}
		// put txs to recover
		heightRange = append(heightRange, txRecRes.Height)
		pool.recover.CacheFetchedTxs(txRecRes.Height, txRecRes.Txs)
	}
	pool.log.Infof("replay txs success, fetched in recover, blocks:%d, height:%v", len(bzs), heightRange)
	// replay config txs in pending
	txCount := 0
	txBatches, err := pool.replayTxsByTxDir(dumpPath, confTxInPenDir)
	if err != nil {
		pool.log.Errorf("replay txs failed, config in pending, err:%v", err)
		return err
	}
	for _, txBatch := range txBatches {
		// put tx to pending without verifying
		// and set dbHeight to 0
		pool.queue.appendConfigTxsToPendingCache(txBatch.Txs, 0)
		txCount += len(txBatch.Txs)
	}
	pool.log.Infof("replay txs success, config in pending, txs:%d", txCount)
	// replay common txs in pending
	txCount = 0
	txBatches, err = pool.replayTxsByTxDir(dumpPath, commTxInPenDir)
	if err != nil {
		pool.log.Errorf("replay txs failed, common in pending, err:%v", err)
		return err
	}
	for _, txBatch := range txBatches {
		// put txs to pending no verifying
		// and set dbHeight to 0
		pool.queue.appendCommonTxsToPendingCache(txBatch.Txs, 0)
		txCount += len(txBatch.Txs)
	}
	pool.log.Infof("replay txs success, common in pending, txs:%d", txCount)
	// no dump txs in queue, no replay txs
	if !IsDumpTxsInQueue() {
		pool.log.Infof("no dump and replay config and common txs in queue")
		return nil
	}
	// replay config txs in queue
	txCount = 0
	txValid := 0
	txBatches, err = pool.replayTxsByTxDir(dumpPath, confTxInQueDir)
	if err != nil {
		pool.log.Errorf("replay txs failed, config in queue, err:%v", err)
		return err
	}
	for _, txBatch := range txBatches {
		memTxs := &mempoolTxs{
			isConfigTxs: true,
			source:      protocol.P2P,
			mtxs:        make([]*memTx, 0, len(txBatch.Txs)),
		}
		for _, tx := range txBatch.Txs {
			if pool.isFull(tx) {
				break
			}
			if pool.queue.has(tx, true) {
				continue
			}
			if mtx, err2 := pool.validateTx(tx, protocol.P2P); err2 == nil {
				memTxs.mtxs = append(memTxs.mtxs, mtx)
				txValid++
			}
		}
		// put txs to queue with verifying, containing signature and format
		pool.queue.addTxsToConfigQueue(memTxs)
		txCount += len(txBatch.Txs)
	}
	pool.log.Infof("replay txs success, config in queue, txs:%d, valid txs:%d", txCount, txValid)
	// replay common txs in queue
	txCount = 0
	txValid = 0
	txBatches, err = pool.replayTxsByTxDir(dumpPath, commTxInQueDir)
	if err != nil {
		pool.log.Errorf("replay txs failed, common in queue, err:%v", err)
		return err
	}
	for _, txBatch := range txBatches {
		memTxs := &mempoolTxs{
			isConfigTxs: false,
			source:      protocol.P2P,
			mtxs:        make([]*memTx, 0, len(txBatch.Txs)),
		}
		for _, tx := range txBatch.Txs {
			if pool.isFull(tx) {
				break
			}
			if pool.queue.has(tx, true) {
				continue
			}
			if mtx, err2 := pool.validateTx(tx, protocol.P2P); err2 == nil {
				memTxs.mtxs = append(memTxs.mtxs, mtx)
				txValid++
			}
		}
		// put txs to queue with verifying, containing signature and format
		pool.queue.addTxsToCommonQueue(memTxs)
		txCount += len(txBatch.Txs)
	}
	pool.log.Infof("replay txs success, common in queue, txs:%d, valid txs:%d", txCount, txValid)
	// remove dir
	os.RemoveAll(dumpPath)
	return nil
}

// replayTxsByTxDir replay txs by txDir
func (pool *txPoolImpl) replayTxsByTxDir(dumpPath, txDir string) (txBatches []*txpoolPb.TxBatch, err error) {
	// replay txs in txDir
	var txBatchBzs [][]byte
	txBatchBzs, err = pool.replayMsg(path.Join(dumpPath, txDir))
	if err != nil {
		pool.log.Errorf("replay txs failed, txDir:%s, err:%v", txDir, err)
		return
	}
	if len(txBatchBzs) == 0 {
		return
	}
	// return txBatches
	txBatches = make([]*txpoolPb.TxBatch, 0, len(txBatchBzs))
	for _, batchBz := range txBatchBzs {
		txBatch := &txpoolPb.TxBatch{}
		if err = proto.Unmarshal(batchBz, txBatch); err != nil {
			pool.log.Errorf("unmarshal *txpoolPb.TxBatch failed"+
				"(please delete dump_tx_wal when change txPool type), err:%v", err)
			return
		}
		if txBatch.BatchId == pool.nodeId && len(txBatch.Txs) != 0 {
			txBatches = append(txBatches, txBatch)
		}
	}
	return
}

// replayMsg upload msg from dir
func (pool *txPoolImpl) replayMsg(dir string) (bzs [][]byte, err error) {
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

func (pool *txPoolImpl) OnQuit() {
	pool.log.Infof("single pool on quit message bus")
}

// Stop stop the txPool service
func (pool *txPoolImpl) Stop() error {
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
	// stop addTxLoop goroutine
	pool.cancel()
	// set stat to stopped
	if !atomic.CompareAndSwapInt32(&pool.stat, stopping, stopped) {
		pool.log.Errorf(commonErrors.ErrTxPoolStopFailed.String())
		return commonErrors.ErrTxPoolStopFailed
	}
	pool.log.Infof("stop single pool success")
	return nil
}

// monitorValidateTxInDB count the time of validate tx in db
func (pool *txPoolImpl) monitorValidateTxInDB(validateTimeMs int64) {
	if MonitorEnabled {
		go pool.metricTxExistInDBTime.WithLabelValues(pool.chainId).Observe(float64(validateTimeMs))
	}
}

// monitorValidateTxSign count the time of validate tx signature and format
func (pool *txPoolImpl) monitorValidateTxSign(validateTimeMs int64) {
	if MonitorEnabled {
		go pool.metricTxVerifySignTime.WithLabelValues(pool.chainId).Observe(float64(validateTimeMs))
	}
}

// metrics count validate tx time
func (pool *txPoolImpl) metrics(msg string, startTime int64, endTime int64) {
	if IsMetrics() {
		pool.log.Infof(msg, "internal", endTime-startTime, "startTime", startTime, "endTime", endTime)
	}
}

// convertMapToSlice convert txs map to txs slice
func convertMapToSlice(mtxs map[string]*memTx) []*memTx {
	mtxsRet := make([]*memTx, 0, len(mtxs))
	for _, mtx := range mtxs {
		mtxsRet = append(mtxsRet, mtx)
	}
	return mtxsRet
}

// convertTxIdsSliceToMap convert txIds slice to map
func convertTxIdsSliceToMap(txIds []string) map[string]struct{} {
	txIdsMap := make(map[string]struct{}, len(txIds))
	for _, txId := range txIds {
		txIdsMap[txId] = struct{}{}
	}
	return txIdsMap
}
