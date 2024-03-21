/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	commonErr "chainmaker.org/chainmaker/common/v2/errors"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/localconf/v2"
	"chainmaker.org/chainmaker/lws"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	netPb "chainmaker.org/chainmaker/pb-go/v2/net"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/recorderfile"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/mitchellh/mapstructure"
	"golang.org/x/sync/semaphore"
)

// TxPoolType is tx_pool type
const TxPoolType = "NORMAL"

const (
	stopped  = 0
	started  = 1
	starting = 2
	stopping = 3
)

var _ protocol.TxPool = (*normalPool)(nil)

type normalPool struct {
	stat    int32  // Identification of module service startup, 0 is stopped, 1 is started, 2 is starting, 3 is stopping
	nodeId  string // The ID of node
	chainId string // The ID of chain

	addTxCh chan *memTx // channel that receive the transaction
	ctx     context.Context
	cancel  context.CancelFunc

	workersSem *semaphore.Weighted // The semaphore of worker to add tx to queue
	wal        *lws.Lws            // the wal is used to store dumped transaction

	dispatcher   Dispatcher      // The dispatcher is used to distribute transaction to different common queue
	queue        *txQueue        // The queue for store transactions, contains configQueue and commonQueues
	recover      *txRecover      // The recover to cache packed txs, and sync txs for node
	batchBuilder *txBatchBuilder // The batchBuilder to build and broadcast txBatch

	ac         protocol.AccessControlProvider // Access control provider to verify transaction signature and authority
	msgBus     msgbus.MessageBus              // Information interaction between modules
	chainConf  protocol.ChainConf             // ChainConfig contains chain config
	txFilter   protocol.TxFilter              // TxFilter is cuckoo filter
	blockStore protocol.BlockchainStore       // Store module implementation
	log        protocol.Logger
}

// NewNormalPool create normal tx pool normalPool
func NewNormalPool(
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
	// chaiId and nodeId should not be nil
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
		addChSize       = defaultChannelSize
		commonQueueNum  = defaultCommonQueueNum
		batchCreateTime = defaultBatchCreateTime
		batchMaxSize    = MaxTxCount(chainConf)
	)
	// set addChSize
	if TxPoolConfig.AddTxChannelSize > 0 {
		addChSize = int(TxPoolConfig.AddTxChannelSize)
	}
	// set commonQueueNum
	// the commonQueueNum must be an exponent of 2 and less than 256
	// such as, 1, 2, 4, 8, ..., 256
	if TxPoolConfig.CommonQueueNum > 0 {
		commonQueueNum = minVal(nextNearestPow2int(int(TxPoolConfig.CommonQueueNum)), 256)
	}
	// commonQueueNum must be less than MaxTxCount
	if commonQueueNum > MaxTxCount(chainConf) {
		commonQueueNum = maxVal(nextNearestPow2int(MaxTxCount(chainConf))>>2, 1)
	}
	// set batchCreateTime
	if TxPoolConfig.BatchCreateTimeout > 0 {
		batchCreateTime = int(TxPoolConfig.BatchCreateTimeout)
	}
	// set batchMaxSize
	if TxPoolConfig.BatchMaxSize > 0 {
		batchMaxSize = int(TxPoolConfig.BatchMaxSize)
	}
	// create dispatcher
	dispatcher := newTxDispatcher(log)
	// create txQueue
	queue := newTxQueue(commonQueueNum, chainConf, chainStore, log)
	ctx, cancel := context.WithCancel(context.Background())
	// return normalPool
	return &normalPool{
		nodeId:       nodeId,
		chainId:      chainId,
		addTxCh:      make(chan *memTx, addChSize),
		ctx:          ctx,
		cancel:       cancel,
		workersSem:   semaphore.NewWeighted(int64(commonQueueNum + 1)),
		dispatcher:   dispatcher,
		queue:        queue,
		recover:      newTxRecover(nodeId, dispatcher, queue, msgBus, log),
		batchBuilder: newTxBatchBuilder(ctx, nodeId, 2*addChSize, batchMaxSize, batchCreateTime, msgBus, chainConf, log),
		ac:           ac,
		chainConf:    chainConf,
		txFilter:     txFilter,
		blockStore:   chainStore,
		msgBus:       msgBus,
		log:          log,
	}, nil
}

//UpdateTxPoolConfig including BatchMaxSize
func (pool *normalPool) UpdateTxPoolConfig(param string, value string) error {
	v, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	switch param {
	case "BatchMaxSize":
		pool.batchBuilder.updateBatchMaxSize(v)
	case "BatchCreateTimeout":
		pool.batchBuilder.updateBatchCreateTimeout(v)
	default:
		return nil
	}
	return nil
}

// Start start the txPool service
func (pool *normalPool) Start() (err error) {
	// start logic: stopped => starting => started
	// should not start again
	// should not start when stopping
	if !atomic.CompareAndSwapInt32(&pool.stat, stopped, starting) {
		pool.log.Errorf(commonErr.ErrTxPoolStartFailed.String())
		return commonErr.ErrTxPoolStartFailed
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
	// start batchBuilder
	pool.batchBuilder.Start()
	// start addTxLoop
	pool.addTxLoop()
	// set stat to started
	if !atomic.CompareAndSwapInt32(&pool.stat, starting, started) {
		pool.log.Errorf(commonErr.ErrTxPoolStartFailed.String())
		return commonErr.ErrTxPoolStartFailed
	}
	pool.log.Infof("start normal pool success")
	return
}

// addTxLoop listen addTxCh and add transaction to txQueue
func (pool *normalPool) addTxLoop() {
	go func() {
		for {
			select {
			case tx, ok := <-pool.addTxCh:
				if !ok {
					pool.log.Warnf("addTxCh has been closed")
					return
				}
				pool.addTx(tx)
			case <-pool.ctx.Done():
				return
			}
		}
	}()
}

// addTx add transaction to config or common queue
func (pool *normalPool) addTx(mtx *memTx) {
	// require semaphore
	if err := pool.workersSem.Acquire(context.Background(), 1); err != nil {
		pool.log.Errorf("get add tx semaphore failed! err:%v", err)
	}
	// add tx to queue
	go func() {
		defer func() {
			pool.workersSem.Release(1)
			pool.publishSignal()
		}()
		pool.log.Infof("Add a Trabsaction, txid:%s", mtx.tx.Payload.TxId)
		//......teststart by gzy
		// File, err := os.OpenFile("/home/yaoye/projects/chainmaker-csv/temp/chainmaker-csv/log/Txconfirmdelay_start.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		// if err != nil {
		// 	pool.log.Errorf("File open failed!")
		// }
		// defer File.Close()
		//创建写入接口
		// WriterCsv := csv.NewWriter(File)
		str := fmt.Sprintf("%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), mtx.tx.Payload.TxId) //需要写入csv的数据，切片类型
		_ = recorderfile.Record(str, "tx_delay_start")
		// if err != nil {
		// 	if err == errors.New("close") {
		// 		pool.log.Errorf("[tx_delay_start] switch closed")
		// 	} else {
		// 		pool.log.Errorf("[tx_delay_start] record failed")
		// 	}
		// }
		// pool.log.Infof("[tx_delay_start] record succeed")
		// txconfirmdelay_start := &recorder.Txconfirmdelay_start{
		// 	Tx_timestamp:          time.Now(),
		// 	Txid_confirmdelay_sta: mtx.tx.Payload.TxId,
		// 	Txsendtime:            time.Now(),
		// }
		// resultC := make(chan error, 1)
		// recorder.Record(txconfirmdelay_start, resultC)
		// select {
		// case err := <-resultC:
		// 	if err != nil {
		// 		// 记录数据出现错误
		// 		pool.log.Errorf("[Txconfirmdelay_start] record failed:submodule/txpool-normal/v2@v2.3.0/normal_tx_pool.go:addTx()")
		// 	} else {
		// 		// 记录数据成功
		// 		pool.log.Infof("[Txconfirmdelay_start] record succeed:submodule/txpool-normal/v2@v2.3.0/normal_tx_pool.go:addTx()")
		// 	}
		// case <-time.NewTimer(time.Second).C:
		// 	// 记录数据超时
		// 	pool.log.Errorf("[Txconfirmdelay_start] record timeout:submodule/txpool-normal/v2@v2.3.0/normal_tx_pool.go:addTx()")
		// }
		// if performance.Txconfirmdelay_start() {
		// 	txconfirmdelay_start := &model.Txconfirmdelay_start{
		// 		Tx_timestamp:          time.Now(),
		// 		Txid_confirmdelay_sta: mtx.tx.Payload.TxId,
		// 		Txsendtime:            time.Now(),
		// 	}
		// 	signal := make(chan error, 1)
		// 	performance.Record(txconfirmdelay_start, signal)
		// 	err := <-signal
		// 	if err != nil {
		// 		pool.log.Errorf("写入数据库出现错误， do something", err)
		// 	}
		// }

		//......testsend by gzy
		// config tx
		if isConfigTx(mtx.getTx(), pool.chainConf) {
			pool.queue.addConfigTx(mtx)
			// common tx
		} else {
			idx := pool.dispatcher.DistTx(mtx.getTxId(), pool.queue.commonQueuesNum())
			pool.queue.addCommonTx(mtx, idx)
		}
	}()
}

// OnMessage Process messages from MsgBus
// nolint
func (pool *normalPool) OnMessage(msg *msgbus.Message) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if msg == nil {
		pool.log.Warnf("OnMessage msg should not be empty")
		return
	}
	// should be RecvTxPoolMsg type
	if msg.Topic != msgbus.RecvTxPoolMsg {
		pool.log.Errorf("OnMessage msg topic should be RecvTxPoolMsg")
		return
	}
	// unMarshal to txPoolMsg
	txPoolBz := msg.Payload.(*netPb.NetMsg).Payload
	txPoolMsg := &txpoolPb.TxPoolMsg{}
	if err := proto.Unmarshal(txPoolBz, txPoolMsg); err != nil {
		pool.log.Warnf("OnMessage unmarshal txPoolMsg failed, err:%v", err)
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
	case txpoolPb.TxPoolMsgType_BATCH_TX:
		// unMarshal to txBatch
		txBatch := &txpoolPb.TxBatch{}
		if err := proto.Unmarshal(txPoolMsg.Payload, txBatch); err != nil {
			pool.log.Warnf("OnMessage unmarshal txBatch failed, err:%v", err)
			return
		}
		pool.log.Debugf("OnMessage receive a txBatch, batchId:%s, txs:%d", txBatch.BatchId, len(txBatch.Txs))
		// put signal to core
		defer pool.publishSignal()
		txs := convertBatchToTxs(txBatch)
		strs := ""
		for _, tx := range txs {
			// File, err := os.OpenFile("/home/yaoye/projects/chainmaker-csv/temp/chainmaker-csv/log/TransactionThroughput.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
			// if err != nil {
			// 	pool.log.Errorf("File open failed!")
			// }
			// defer File.Close()
			//创建写入接口
			// WriterCsv := csv.NewWriter(File)
			str := fmt.Sprintf("%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), tx.Payload.GetTxId(), strconv.Itoa(int(1))) //需要写入csv的数据，切片类型
			strs = strs + str
			// err := recorderfile.Record(str, "transaction_pool_input_throughput")
			// if err != nil {
			// 	if err == errors.New("close") {
			// 		pool.log.Errorf("[transaction_pool_input_throughput] switch closed")
			// 	} else {
			// 		pool.log.Errorf("[transaction_pool_input_throughput] record failed")
			// 	}
			// }
			// pool.log.Infof("[transaction_pool_input_throughput] record succeed")
			// transaction := &recorder.TransactionThroughput{
			// 	Tx_timestamp:      time.Now(),
			// 	TxID_tranthr:      tx.Payload.GetTxId(),
			// 	OccurTime_tranthr: time.Now(),
			// 	Source_tranthr:    int(1),
			// }
			// resultC := make(chan error, 1)
			// // recorder.RecordBatch(TransactionThroughputs, TransactionThroughputs[0], resultC)
			// recorder.Record(transaction, resultC) // 异步执行，记录到数据库
			// select {
			// case err := <-resultC:
			// 	if err != nil {
			// 		// 记录数据出现错误
			// 		pool.log.Errorf("[TransactionThroughput] record failed:submodule/txpool-normal/v2@v2.3.0/normal_tx_pool.go:OnMessage()")
			// 	} else {
			// 		// 记录数据成功
			// 		pool.log.Infof("[TransactionThroughput] record succeed:submodule/txpool-normal/v2@v2.3.0/normal_tx_pool.go:OnMessage()")
			// 	}
			// case <-time.NewTimer(time.Second).C:
			// 	// 记录数据超时
			// 	pool.log.Errorf("[TransactionThroughput] record timeout:submodule/txpool-normal/v2@v2.3.0/normal_tx_pool.go:OnMessage()")
			// }

			//pool.log.Infof("Add a txs, txId:%s, source:%v", tx.Payload.GetTxId(), 1)
			// TransactionThroughputs = append(TransactionThroughputs, &recorder.TransactionThroughput{
			// 	Tx_timestamp:      time.Now(),
			// 	TxID_tranthr:      tx.Payload.GetTxId(),
			// 	OccurTime_tranthr: time.Now(),
			// 	Source_tranthr:    int(1),
			// })
		}
		_ = recorderfile.Record(strs, "transaction_pool_input_throughput")
		// if err != nil {
		// 	if err == errors.New("close") {
		// 		pool.log.Errorf("[transaction_pool_input_throughput] switch closed")
		// 	} else {
		// 		pool.log.Errorf("[transaction_pool_input_throughput] record failed")
		// 	}
		// }
		// pool.log.Infof("[transaction_pool_input_throughput] record succeed")
		// recorder
		// if performance.TransactionThroughput() {
		// 	for _, tx := range txs {
		// 		//pool.log.Infof("Add a txs, txId:%s, source:%v", tx.Payload.GetTxId(), 1)
		// 		transaction := &model.TransactionThroughput{
		// 			Tx_timestamp:      time.Now(),
		// 			TxID_tranthr:      tx.Payload.GetTxId(),
		// 			OccurTime_tranthr: time.Now(),
		// 			Source_tranthr:    int(1),
		// 		}
		// 		signal := make(chan error, 1)
		// 		performance.Record(transaction, signal)
		// 		err := <-signal
		// 		if err != nil {
		// 			pool.log.Infof("mysql write error: %s", err)
		// 		}
		// 	}
		// }
		// txs is nil
		if len(txs) == 0 {
			return
		}
		// pool is full
		if pool.queue.isFull(txs[0]) {
			pool.log.Warn("tx pool is full")
			return
		}
		// config tx
		if len(txs) == 1 && isConfigTx(txs[0], pool.chainConf) {
			if mtx, err := pool.validateTx(txs[0], protocol.P2P); err == nil {
				pool.queue.addConfigTx(mtx)
			}
			// common txs
		} else {
			_, mtxsTable := pool.validateTxs(txs, protocol.P2P)
			pool.queue.addCommonTxs(mtxsTable)
		}
	case txpoolPb.TxPoolMsgType_RECOVER_REQ:
		// unMarshal to txRecoverRequest
		txRecoverReq := &txpoolPb.TxRecoverRequest{}
		if err := proto.Unmarshal(txPoolMsg.Payload, txRecoverReq); err != nil {
			pool.log.Warnf("OnMessage unmarshal txRecoverRequest failed, err:%v", err)
			return
		}
		pool.log.Debugf("OnMessage receive txRecoverRequest, from:%s, height:%d, txs:%d",
			txRecoverReq.NodeId, txRecoverReq.Height, len(txRecoverReq.TxIds))
		// process recover request
		pool.recover.ProcessRecoverReq(txRecoverReq)
	case txpoolPb.TxPoolMsgType_RECOVER_RESP:
		// unMarshal to txRecoverResponse
		txRecoverRes := &txpoolPb.TxRecoverResponse{}
		if err := proto.Unmarshal(txPoolMsg.Payload, txRecoverRes); err != nil {
			pool.log.Warnf("OnMessage unmarshal txRecoverResponse failed, err:%v", err)
			return
		}
		pool.log.Debugf("OnMessage receive txRecoverResponse, from:%s, height:%d, txs:%d",
			txRecoverRes.NodeId, txRecoverRes.Height, len(txRecoverRes.Txs))
		// put signal to core
		defer pool.publishSignal()
		txs := txRecoverRes.Txs
		// txs is nil
		// attention: even if the pool is full, put it into pool !!!
		if len(txs) == 0 {
			return
		}
		// config tx
		if len(txs) == 1 && isConfigTx(txs[0], pool.chainConf) {
			if mtx, err := pool.validateTx(txs[0], protocol.P2P); err == nil {
				// put valid tx to recover
				pool.recover.ProcessRecoverRes(txRecoverRes)
				// put valid tx to config tx pool
				pool.queue.addConfigTx(mtx)
			}
			// common txs
		} else {
			validTxs, mtxsTable := pool.validateTxs(txs, protocol.P2P)
			// put valid txs to recover
			txRecoverRes.Txs = validTxs
			pool.recover.ProcessRecoverRes(txRecoverRes)
			// put valid txs to common tx pool
			pool.queue.addCommonTxs(mtxsTable)
		}
	default:
		pool.log.Warnf("OnMessage receive invalid message type %s", txPoolMsg.Type)
	}
}

// AddTx only add rpc tx into normal tx_pool
func (pool *normalPool) AddTx(tx *commonPb.Transaction, source protocol.TxSource) error {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return commonErr.ErrTxPoolHasStopped
	}
	// should not be nil
	if tx == nil || tx.Payload == nil || len(tx.Payload.TxId) == 0 {
		return commonErr.ErrStructEmpty
	}
	// verify tx source
	if source == protocol.INTERNAL {
		return commonErr.ErrTxSource
	}
	// verify whether timestamp is out of date, signature and format(from P2P) is valid, and exist in db
	// attention: whether tx exist in pool is verified when add to txList
	mTx, err := pool.validateTx(tx, source)
	if err != nil {
		return err
	}
	
	str := fmt.Sprintf("%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), tx.Payload.GetTxId(), strconv.Itoa(int(1))) //需要写入csv的数据，切片类型
	_ = recorderfile.Record(str, "transaction_pool_input_throughput")
	
	// verify whether the tx pool is full, even if tx pool is full, broadcast valid tx from RPC to other nodes
	if pool.queue.isFull(tx) {
		if source == protocol.RPC {
			pool.broadcastTx(tx.Payload.TxId, mustMarshal(tx))
		}
		return commonErr.ErrTxPoolLimit
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
	// notify txBatchBuilder to build and broadcast tx batch
	// attention: must shallow copy transaction!!!
	// While batchBuilder serializing transaction, VM module may be adding execution result to the transaction
	// for proposer, which can cause panic.
	if source == protocol.RPC {
		pool.batchBuilder.getBuildBatchCh() <- copyTx(tx)
	}
	return nil
}

// FetchTxs Get some transactions from single or normal txPool by block height to generate new block.
// return transactions.
func (pool *normalPool) FetchTxs(blockHeight uint64) (txs []*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// fetch txs
	startFetchTime := utils.CurrentTimeMillisSeconds()
	mtxs := pool.queue.fetchTxBatch()
	if len(mtxs) == 0 {
		return
	}
	// prune txs in increment db
	startPruneTime := utils.CurrentTimeMillisSeconds()
	txs = pool.pruneFetchedTxsExistDB(mtxs)
	// if the number of fetched tx is lager than maxCount
	// retry extra tx to pool
	if maxCount := MaxTxCount(pool.chainConf); len(txs) > maxCount {
		retryTxs := txs[maxCount:]
		pool.retryTxs(retryTxs)
		txs = txs[:maxCount]
	}
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
	pool.log.Infof("FetchTxs, height:%d, txs:[fetch:%d,prune:%d], time[fetch:%d,prune:%d,cache:%d,total:%dms]",
		blockHeight, len(txs), len(mtxs)-len(txs), startPruneTime-startFetchTime, startCacheTime-startPruneTime,
		endTime-startCacheTime, endTime-startFetchTime)
	return
}

// FetchTxBatches Get some transactions from batch txPool by block height to generate new block.
// return transactions table and batchId list.
func (pool *normalPool) FetchTxBatches(blockHeight uint64) (batchIds []string, txsTable [][]*commonPb.Transaction) {
	pool.log.Warn("FetchTxBatches is no implementation for normal pool")
	return
}

// pruneFetchedTxsExistDB prune txs in incremental db after fetch a batch of txs
func (pool *normalPool) pruneFetchedTxsExistDB(mtxs []*memTx) []*commonPb.Transaction {
	// should not be nil
	if len(mtxs) == 0 {
		return nil
	}
	// verify whether txs exist in increment db
	mtxsTable := segmentMTxs(mtxs)
	// verify result table
	txsRetTable := make([][]*commonPb.Transaction, len(mtxsTable))
	txsInDBTable := make([][]*commonPb.Transaction, len(mtxsTable))
	// verify concurrently
	var wg sync.WaitGroup
	for i, perMTxs := range mtxsTable {
		wg.Add(1)
		go func(i int, perMTxs []*memTx) {
			defer wg.Done()
			txs := make([]*commonPb.Transaction, 0, len(perMTxs))
			txsInDB := make([]*commonPb.Transaction, 0)
			for _, mtx := range perMTxs {
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
		}(i, perMTxs)
	}
	wg.Wait()
	// delete tx in db and return txs not in db
	txsRet := make([]*commonPb.Transaction, 0, len(mtxs))
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
func (pool *normalPool) ReGenTxBatchesWithRetryTxs(blockHeight uint64, batchIds []string,
	retryTxs []*commonPb.Transaction) (newBatchIds []string, newTxsTable [][]*commonPb.Transaction) {
	pool.log.Warn("ReGenTxBatchesWithRetryTxs is no implementation for normal pool")
	return
}

// ReGenTxBatchesWithRemoveTxs Remove removeTxs in batches that retrieved by the batchIds
// to create new batches for batch txPool and return batchIds of new batches
// then put new batches into the pendingCache of pool
// and delete old batches retrieved by the batchIds in pool.
func (pool *normalPool) ReGenTxBatchesWithRemoveTxs(blockHeight uint64, batchIds []string,
	removeTxs []*commonPb.Transaction) (newBatchIds []string, newTxsTable [][]*commonPb.Transaction) {
	pool.log.Warn("ReGenTxBatchesWithRemoveTxs is no implementation for normal pool")
	return
}

// RemoveTxsInTxBatches Remove removeTxs in batches that retrieved by the batchIds
// to create new batches for batch txPool.
// then, put new batches into the queue of pool
// and delete old batches retrieved by the batchIds in pool.
func (pool *normalPool) RemoveTxsInTxBatches(batchIds []string, removeTxs []*commonPb.Transaction) {
	pool.log.Warn("RemoveTxsInTxBatches is no implementation for normal pool")
}

// GetTxsByTxIds Retrieve transactions by the txIds from single or normal txPool,
// and only return transactions it has.
// txsRet is the transaction in the txPool, txsMis is the transaction not in the txPool.
func (pool *normalPool) GetTxsByTxIds(txIds []string) (
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
		txId := txIds[0]
		txsRet = make(map[string]*commonPb.Transaction, 1)
		txsMis = make(map[string]struct{}, 1)
		if mtx, err := pool.queue.getTxByTxId(txId); err == nil {
			txsRet, txsMis = pool.pruneGotTxsExistDB(map[string]*memTx{mtx.getTxId(): mtx}, map[string]struct{}{})
		} else {
			txsMis[txId] = struct{}{}
		}
		pool.log.DebugDynamic(func() string {
			return fmt.Sprintf("GetTxsByTxIds in detail, txIds:%v",
				printTxIdsInTxsMap(txsRet))
		})
		pool.log.Infof("GetTxsByTxIds, want:%d, get:%d, time:[%dms]",
			len(txIds), len(txsRet), utils.CurrentTimeMillisSeconds()-startGetTime)
		return
	}
	// get common txs
	txIdsTable := pool.dispatcher.DistTxIds(txIds, pool.queue.commonQueuesNum())
	var mtxsRet map[string]*memTx
	mtxsRet, txsMis = pool.queue.getCommonTxsByTxIds(txIdsTable)
	// prune common txs in increment db
	startPruneTime := utils.CurrentTimeMillisSeconds()
	txsRet, txsMis = pool.pruneGotTxsExistDB(mtxsRet, txsMis)
	endTime := utils.CurrentTimeMillisSeconds()
	pool.log.DebugDynamic(func() string {
		return fmt.Sprintf("GetTxsByTxIds in detail, txIds:%v",
			printTxIdsInTxsMap(txsRet))
	})
	pool.log.Infof("GetTxsByTxIds, want:%d, get:%d, prune:%d, time[get:%d,prune:%d,total:%dms]",
		len(txIds), len(txsRet), len(mtxsRet)-len(txsRet),
		startPruneTime-startGetTime, endTime-startPruneTime, endTime-startGetTime)
	return txsRet, txsMis
}

// pruneGetTxsExistDB prune txs in incremental db after get a batch of txs
func (pool *normalPool) pruneGotTxsExistDB(mtxsMap map[string]*memTx, txsMis map[string]struct{}) (
	map[string]*commonPb.Transaction, map[string]struct{}) {
	if len(mtxsMap) == 0 {
		return nil, txsMis
	}
	// verify whether txs exist in increment db
	mtxsTable := segmentMTxs(convertTxsMapToSlice(mtxsMap))
	// verify result table
	txsTable := make([][]*commonPb.Transaction, len(mtxsTable))
	txsInDBTable := make([][]*commonPb.Transaction, len(mtxsTable))
	// verify concurrently
	var wg sync.WaitGroup
	for i, perMTxs := range mtxsTable {
		wg.Add(1)
		go func(i int, perMTxs []*memTx) {
			defer wg.Done()
			txs := make([]*commonPb.Transaction, 0, len(perMTxs))
			txsInDB := make([]*commonPb.Transaction, 0)
			for _, mtx := range perMTxs {
				tx := mtx.getTx()
				if !pool.isTxExistsInIncrementDB(tx, mtx.dbHeight) {
					txs = append(txs, tx)
				} else {
					txsInDB = append(txsInDB, tx)
				}
			}
			// put result to table
			txsTable[i] = txs
			txsInDBTable[i] = txsInDB
		}(i, perMTxs)
	}
	wg.Wait()
	// return txsRet
	txsRet := make(map[string]*commonPb.Transaction, len(mtxsMap))
	if txsMis == nil {
		txsMis = make(map[string]struct{})
	}
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

// GetAllTxsByTxIds Retrieve all transactions by the txIds from single or normal txPool synchronously.
// if there are some transactions lacked, it need to obtain them by height from the proposer.
// if txPool get all transactions before timeout return txsRet, otherwise, return error.
func (pool *normalPool) GetAllTxsByTxIds(txIds []string, proposerId string, height uint64, timeoutMs int) (
	txsRet map[string]*commonPb.Transaction, err error) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return nil, commonErr.ErrTxPoolHasStopped
	}
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
	// second get common txs
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
		pool.log.Infof("GetAllTxsByTxIds, height:%d, second, want:%d, get:%d, time:[first:%d,second:%d, total:%dms]",
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
		"time:[first:%d,second:%d,recover:%d, total:%dms]",
		height, maxVal(timeoutMs, defaultRecoverTimeMs), len(txIds), len(txsRet), startSecondGetTime-startFirstGetTime,
		startRecoverTime-startSecondGetTime, endTime-startRecoverTime, endTime-startFirstGetTime)
	return nil, fmt.Errorf("can not get all txs, want:%d, only get:%d", len(txIds), len(txsRet))
}

// GetAllTxsByBatchIds Retrieve all transactions by the batchIds from batch txPool synchronously.
// if there are some batches lacked, it need to obtain them by height from the proposer.
// if txPool get all batches before timeout return txsRet, otherwise, return error.
func (pool *normalPool) GetAllTxsByBatchIds(batchIds []string, proposerId string, height uint64, timeoutMs int) (
	txsTable [][]*commonPb.Transaction, err error) {
	// no implementation for normal pool
	pool.log.Warn("GetAllTxsByBatchIds is no implementation for normal pool")
	return
}

// AddTxsToPendingCache These transactions will be added to the cache to avoid the transactions
// are fetched again and re-filled into the new block. Because of the chain confirmation
// rule in the HotStuff consensus algorithm.
func (pool *normalPool) AddTxsToPendingCache(txs []*commonPb.Transaction, blockHeight uint64) {
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
		pool.queue.addConfigTxToPendingCache(&memTx{tx: txs[0], dbHeight: blockHeight})
		return
	}
	// common txs
	mtxsTable := pool.dispatcher.DistTxsToMemTxs(txs, pool.queue.commonQueuesNum(), blockHeight)
	pool.queue.addCommonTxsToPendingCache(mtxsTable)
	pool.log.Infof("AddTxsToPendingCache, height:%d, txs:%d, time:%dms",
		blockHeight, len(txs), utils.CurrentTimeMillisSeconds()-startTime)
}

// AddTxBatchesToPendingCache These transactions will be added to batch txPool to avoid the transactions
// are fetched again and re-filled into the new block. Because of the chain confirmation
// rule in the HotStuff consensus algorithm.
func (pool *normalPool) AddTxBatchesToPendingCache(batchIds []string, blockHeight uint64) {
	// no implementation for normal pool
	pool.log.Warn("AddTxBatchesToPendingCache is no implementation for normal pool")
}

// RetryAndRemoveTxs Process transactions within multiple proposed blocks at the same height to
// ensure that these transactions are not lost, re-add valid txs which that are not on local node.
// remove txs in the commit block.
func (pool *normalPool) RetryAndRemoveTxs(retryTxs []*commonPb.Transaction, removeTxs []*commonPb.Transaction) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// should not be nil
	if len(retryTxs) == 0 && len(removeTxs) == 0 {
		return
	}
	startTime := utils.CurrentTimeMillisSeconds()
	// first retry
	pool.retryTxs(retryTxs)
	// then remove
	pool.removeTxs(removeTxs)
	pool.log.Infof("RetryAndRemoveTxs, retryTxs:%d, removeTxs:%d, time:%dms",
		len(retryTxs), len(removeTxs), utils.CurrentTimeMillisSeconds()-startTime)
}

// RetryAndRemoveTxBatches Process transactions within multiple proposed blocks at the same height to
// ensure that these transactions are not lost for batch txPool
// re-add valid transactions which that are not on local node, if retryTxs is nil, only retry batches by retryBatchIds,
// otherwise, create and add new batches by retryTxs to pool, and then retry batches by retryBatchIds.
// remove txs in the commit block, if removeTxs is nil, only remove batches by removeBatchIds,
// otherwise, remove removeTxs in batches, create and add a new batches to pool, and remove batches by removeBatchIds.
func (pool *normalPool) RetryAndRemoveTxBatches(retryBatchIds []string, removeBatchIds []string) {
	// no implementation for normal pool
	pool.log.Warn("RetryAndRemoveTxBatches is no implementation for normal pool")
}

// retryTxs Re-add the txs to txPool
func (pool *normalPool) retryTxs(txs []*commonPb.Transaction) {
	// should not be nil
	if len(txs) == 0 {
		return
	}
	// distinguish config or common tx
	confTxs, confTxIds, comTxs, comTxIds := pool.splitTxsByType(txs)
	// config tx
	if len(confTxs) > 0 {
		// first remove txs in pending
		pool.queue.removeConfigTxs(confTxIds, true)
		// verify internal config tx
		mtxs := make([]*memTx, 0, len(confTxs))
		for _, tx := range confTxs {
			if mtx, err := pool.validateTx(tx, protocol.INTERNAL); err == nil {
				mtxs = append(mtxs, mtx)
			}
		}
		// add txs to queue
		pool.queue.retryConfigTxs(mtxs)
	}
	// common txs
	if len(comTxs) > 0 {
		// first remove txs in pending
		txIdsTable := pool.dispatcher.DistTxIds(comTxIds, pool.queue.commonQueuesNum())
		pool.queue.removeCommonTxs(txIdsTable, true)
		// verify internal txs
		_, mtxsTable := pool.validateTxs(comTxs, protocol.INTERNAL)
		// add txs to queue
		pool.queue.retryCommonTxs(mtxsTable)
	}
}

// removeTxs delete the txs from the pool
func (pool *normalPool) removeTxs(txs []*commonPb.Transaction) {
	// should not be nil
	if len(txs) == 0 {
		return
	}
	// put signal to core
	defer pool.publishSignal()
	// distinguish config or common tx
	_, confTxIds, _, comTxIds := pool.splitTxsByType(txs)
	// remove config tx
	pool.queue.removeConfigTxs(confTxIds, false)
	// remove common tx
	comTxIdsTable := pool.dispatcher.DistTxIds(comTxIds, pool.queue.commonQueuesNum())
	pool.queue.removeCommonTxs(comTxIdsTable, false)
}

// splitTxsByType split txs to config txs and common txs by type
func (pool *normalPool) splitTxsByType(txs []*commonPb.Transaction) (
	confTxs []*commonPb.Transaction, confTxIds []string, comTxs []*commonPb.Transaction, comTxIds []string) {
	confTxs = make([]*commonPb.Transaction, 0, len(txs))
	comTxs = make([]*commonPb.Transaction, 0, len(txs))
	confTxIds = make([]string, 0, len(txs))
	comTxIds = make([]string, 0, len(txs))
	for _, tx := range txs {
		// config tx
		if isConfigTx(tx, pool.chainConf) {
			confTxs = append(confTxs, tx)
			confTxIds = append(confTxIds, tx.Payload.TxId)
			// common tx
		} else {
			comTxs = append(comTxs, tx)
			comTxIds = append(comTxIds, tx.Payload.TxId)
		}
	}
	return
}

// TxExists verifies whether the transaction exists in the tx_pool
func (pool *normalPool) TxExists(tx *commonPb.Transaction) (exist bool) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return false
	}
	// should not be nil
	if tx == nil {
		return false
	}
	// config tx
	if isConfigTx(tx, pool.chainConf) {
		exist = pool.queue.configTxExists(tx.Payload.TxId)
		pool.log.Debugf("TxExists, exist:%v", exist)
		return
	}
	// common tx
	idx := pool.dispatcher.DistTx(tx.Payload.TxId, pool.queue.commonQueuesNum())
	exist = pool.queue.commonTxExists(tx.Payload.TxId, idx)
	pool.log.Infof("TxExists, txId:%s, exist:%v", tx.Payload.TxId, exist)
	return
}

// GetPoolStatus return the max size of config tx pool and common tx pool,
// the num of config tx in normal queue and pending queue,
// and the the num of config tx in normal queue and pending queue.
func (pool *normalPool) GetPoolStatus() (poolStatus *txpoolPb.TxPoolStatus) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		poolStatus = &txpoolPb.TxPoolStatus{
			ConfigTxPoolSize: int32(MaxConfigTxPoolSize()),
			CommonTxPoolSize: int32(MaxCommonTxPoolSize()),
		}
		return
	}
	// get status
	poolStatus = pool.queue.getPoolStatus()
	//pool.log.Infof("GetPoolStatus, status:%v", poolStatus)
	return
}

// GetTxIdsByTypeAndStage returns config or common txs in different stage.
// TxType are TxType_CONFIG_TX, TxType_COMMON_TX, (TxType_CONFIG_TX|TxType_COMMON_TX)
// TxStage are TxStage_IN_QUEUE, TxStage_IN_PENDING, (TxStage_IN_QUEUE|TxStage_IN_PENDING)
func (pool *normalPool) GetTxIdsByTypeAndStage(txType, txStage int32) (txIds []string) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return
	}
	// get txs
	_, txIds = pool.queue.getTxsByTxTypeAndStage(txType, txStage)
	pool.log.Infof("GetTxIdsByTypeAndStage, [type:%s,stage:%s], txs:%d",
		txpoolPb.TxType(txType), txpoolPb.TxStage(txStage), len(txIds))
	return
}

// GetTxsInPoolByTxIds Retrieve the transaction by the txIds from the txPool
func (pool *normalPool) GetTxsInPoolByTxIds(txIds []string) (
	txsRet []*commonPb.Transaction, txsMis []string, err error) {
	// should be started
	if atomic.LoadInt32(&pool.stat) != started {
		return nil, txIds, commonErr.ErrTxPoolHasStopped
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
		if mtx, err := pool.queue.getTxByTxId(txId); err == nil {
			txsRet = append(txsRet, mtx.getTx())
		} else {
			txsMis = append(txsMis, txId)
		}
	}
	pool.log.Infof("GetTxsInPoolByTxIds, want:%d, get:%d", len(txIds), len(txsRet))
	return
}

func (pool *normalPool) OnQuit() {
	pool.log.Infof("normal pool on quit message bus")
}

// Stop stop txPool service
func (pool *normalPool) Stop() error {
	// stop logic: started => stopping => stopped
	// should not stop again
	// should not stop when starting
	if !atomic.CompareAndSwapInt32(&pool.stat, started, stopping) {
		pool.log.Errorf(commonErr.ErrTxPoolStopFailed.String())
		return commonErr.ErrTxPoolStopFailed
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
	// cancel context
	// stop addTxLoop and batchBuilder goroutine
	pool.cancel()
	// set stat to stopped
	if !atomic.CompareAndSwapInt32(&pool.stat, stopping, stopped) {
		pool.log.Errorf(commonErr.ErrTxPoolStopFailed.String())
		return commonErr.ErrTxPoolStopFailed
	}
	pool.log.Infof("stop normal pool success")
	return nil
}

// broadcastTx broadcast transaction to other nodes
func (pool *normalPool) broadcastTx(txId string, txMsg []byte) {
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

// publishSignal When the number of transactions in the transaction pool is greater
// than or equal to the block can contain, update the status of the tx pool to block
// propose, otherwise update the status of tx pool to TRANSACTION_INCOME.
func (pool *normalPool) publishSignal() {
	if pool.msgBus == nil {
		return
	}
	// if there is config txs
	// or there is more common txs than MaxTxCount
	// put SignalType_BLOCK_PROPOSE to core
	if pool.queue.configTxCount() > 0 || int(pool.queue.commonTxCount()) >= MaxTxCount(pool.chainConf) {
		pool.msgBus.Publish(msgbus.TxPoolSignal, &txpoolPb.TxPoolSignal{
			SignalType: txpoolPb.SignalType_BLOCK_PROPOSE,
			ChainId:    pool.chainId,
		})
	}
}

// dumpTxs dump all txs to file
func (pool *normalPool) dumpTxs() (err error) {
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
					pool.log.Errorf("dump txs failed, fetched in recover, err:%v", err)
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
	pool.log.Infof("no dump config and common txs in queue")
	return nil
}

// dumpTxsByTxTypeAndStage dump config or common txs in queue or pending
func (pool *normalPool) dumpTxsByTxTypeAndStage(txType, txStage int32, dumpPath, txDir string) error {
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
func (pool *normalPool) dumpMsg(bz []byte, dir string) (err error) {
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
func (pool *normalPool) replayTxs() error { // nolint
	dumpPath := path.Join(localconf.ChainMakerConfig.GetStorePath(), pool.chainId, dumpDir)
	// replay txs in recover
	bzs, err := pool.replayMsg(path.Join(dumpPath, recoverTxDir))
	if err != nil {
		pool.log.Errorf("replay txs failed, fetched in recover, err:%v", err)
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
		for _, tx := range txBatch.Txs {
			// put tx to pending without verifying
			// and set dbHeight to 0
			pool.queue.addConfigTxToPendingCache(newMemTx(tx, protocol.P2P, 0))
		}
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
		mtxsTable := pool.dispatcher.DistTxsToMemTxs(txBatch.Txs, pool.queue.commonQueuesNum(), 0)
		pool.queue.addCommonTxsToPendingCache(mtxsTable)
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
		for _, tx := range txBatch.Txs {
			// put tx to queue with verifying
			if pool.queue.isFull(tx) {
				break
			}
			// put txs to queue with verifying, containing signature and format
			if mtx, err1 := pool.validateTx(tx, protocol.P2P); err1 == nil {
				txValid++
				pool.queue.addConfigTx(mtx)
			}
		}
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
		// put txs to queue with verifying
		if pool.queue.isFull(txBatch.Txs[0]) {
			return commonErr.ErrTxPoolLimit
		}
		// put txs to queue with verifying, containing signature and format
		validTxs, mtxsTable := pool.validateTxs(txBatch.Txs, protocol.P2P)
		pool.queue.addCommonTxs(mtxsTable)
		txCount += len(txBatch.Txs)
		txValid += len(validTxs)
	}
	pool.log.Infof("replay txs success, common in queue, txs:%d, valid txs:%d", txCount, txValid)
	// remove dir
	os.RemoveAll(dumpPath)
	return nil
}

// replayTxsByTxDir replay txs by txDir
func (pool *normalPool) replayTxsByTxDir(dumpPath, txDir string) (txBatches []*txpoolPb.TxBatch, err error) {
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
func (pool *normalPool) replayMsg(dir string) (bzs [][]byte, err error) {
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

// convertBatchToTxs convert txBatch to txs slice
func convertBatchToTxs(txBatch *txpoolPb.TxBatch) []*commonPb.Transaction {
	if txBatch == nil || len(txBatch.Txs) == 0 {
		return nil
	}
	txIds := make(map[string]struct{}, len(txBatch.Txs))
	filterTxs := make([]*commonPb.Transaction, 0, len(txBatch.Txs))
	for _, tx := range txBatch.Txs {
		txId := tx.Payload.TxId
		// no duplicate
		if _, ok := txIds[txId]; !ok {
			txIds[txId] = struct{}{}
			filterTxs = append(filterTxs, tx)
		}
	}
	return filterTxs
}

// convertTxsMapToSlice convert txs Map to txs slice
func convertTxsMapToSlice(mtxs map[string]*memTx) []*memTx {
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
