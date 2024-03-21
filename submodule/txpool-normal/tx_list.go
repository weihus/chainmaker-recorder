/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"chainmaker.org/chainmaker/common/v2/linkedhashmap"
	"chainmaker.org/chainmaker/common/v2/monitor"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/recorderfile"
	"github.com/prometheus/client_golang/prometheus"
)

// txList Structure of store transactions in memory
type txList struct {
	sync.RWMutex
	queue           *linkedhashmap.LinkedHashMap // key:txId val:*memTx
	queueSizeAtomic *int32

	pendingCache *sync.Map // key:txId val:*memTx

	chainConf        protocol.ChainConf
	blockchainStore  protocol.BlockchainStore
	metricTxPoolSize *prometheus.GaugeVec
	log              protocol.Logger
}

// newNormalTxList create txList
func newNormalTxList(queueSizeAtomic *int32, chainConf protocol.ChainConf, blockchainStore protocol.BlockchainStore,
	log protocol.Logger) *txList {
	// create txList
	list := &txList{
		queue:           linkedhashmap.NewLinkedHashMap(),
		queueSizeAtomic: queueSizeAtomic,
		pendingCache:    &sync.Map{},
		chainConf:       chainConf,
		blockchainStore: blockchainStore,
		log:             log,
	}
	// if open monitor
	// then new GaugeVec Metric
	if MonitorEnabled {
		list.metricTxPoolSize = monitor.NewGaugeVec(
			monitor.SUBSYSTEM_TXPOOL,
			monitor.MetricTxPoolSize,
			monitor.HelpTxPoolSizeMetric,
			monitor.ChainId, monitor.PoolType)
	}
	return list
}

// PutTxs put txs to txList
func (l *txList) PutTxs(mtxs []*memTx, validate txValidateFunc) {
	// should not be nil
	if len(mtxs) == 0 {
		return
	}
	// wait lock
	l.Lock()
	defer l.Unlock()
	// put txs to txList
	for _, mtx := range mtxs {
		l.addTx(mtx, validate)
	}
	// update metric
	l.metricMonitor(mtxs[0], atomic.LoadInt32(l.queueSizeAtomic))
}

// PutTx put tx to txList
func (l *txList) PutTx(mtx *memTx, validate txValidateFunc) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	// put a tx to txList
	l.addTx(mtx, validate)
	// update metric
	l.metricMonitor(mtx, atomic.LoadInt32(l.queueSizeAtomic))
}

// FetchTxs fetch a batch of txs from queue and move these txs to pendingCache
func (l *txList) FetchTxs(count int, validate txValidateFunc) ([]*memTx, []string) {
	// update count
	queueLen := l.queue.Size()
	if queueLen < count {
		count = queueLen
	}
	var (
		mTxs    []*memTx
		txIds   []string
		errKeys []string
	)
	// wait lock
	l.Lock()
	// process fetched txs
	defer func() {
		// remove err txs
		if len(errKeys) >= 0 {
			for _, txId := range errKeys {
				l.queue.Remove(txId)
			}
			l.addQueueSize(int32(-1 * len(errKeys)))
		}
		// remove valid txs from queue to pendingCache
		if len(mTxs) > 0 {
			for _, mtx := range mTxs {
				l.queue.Remove(mtx.getTxId())
				l.pendingCache.Store(mtx.getTxId(), mtx)
			}
			l.addQueueSize(int32(-1 * len(mTxs)))
		}
		// update metric
		if len(mTxs) > 0 {
			l.metricMonitor(mTxs[0], atomic.LoadInt32(l.queueSizeAtomic))
		}
		l.Unlock()
	}()
	// if there is tx in txList
	// then get some txs from txList to generate block
	if queueLen > 0 {
		mTxs, txIds, errKeys = l.popTxsFromQueue(count, validate)
	}
	l.log.Debugw("txList fetch txs", "count", count, "queueSize", queueLen,
		"txs", len(mTxs), "errKeys", len(errKeys))
	return mTxs, txIds
}

// GetTxsByTxIds get a batch of txs by txIds
func (l *txList) GetTxsByTxIds(txIds []string) (txsRet map[string]*memTx, txsMis map[string]struct{}) {
	// new value
	txsRet = make(map[string]*memTx, len(txIds))
	txsMis = make(map[string]struct{}, len(txIds))
	txIdsNoPending := make([]string, 0, len(txIds))
	var mtx *memTx
	// get txs from pendingCache
	for _, txId := range txIds {
		if val, ok := l.pendingCache.Load(txId); ok && val != nil {
			if mtx, ok = val.(*memTx); ok {
				txsRet[txId] = mtx
			} else {
				txIdsNoPending = append(txIdsNoPending, txId)
			}
		} else {
			txIdsNoPending = append(txIdsNoPending, txId)
		}
	}
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// get txs from queue
	var ok bool
	for _, txId := range txIdsNoPending {
		if val := l.queue.Get(txId); val != nil {
			if mtx, ok = val.(*memTx); ok {
				txsRet[txId] = mtx
			} else {
				txsMis[txId] = struct{}{}
			}
		} else {
			txsMis[txId] = struct{}{}
		}
	}
	l.log.Debugw("txList Get txs", "len(txIds)", len(txIds),
		"len(txsRet)", len(txsRet), "len(txsMis)", len(txsMis))
	return txsRet, txsMis
}

// GetTxByTxId a tx by txId
func (l *txList) GetTxByTxId(txId string) (*memTx, error) {
	// get tx from pendingCache
	if val, ok := l.pendingCache.Load(txId); ok && val != nil {
		l.log.Debugw("txList get tx in pending", "txId", txId, "exist", true)
		return val.(*memTx), nil
	}
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// get tx from queue
	if val := l.queue.Get(txId); val != nil {
		l.log.Debugw("txList get tx in queue", "txId", txId, "exist", true)
		return val.(*memTx), nil
	}
	l.log.Debugw("txList get tx failed", "txId", txId, "exist", false)
	return nil, fmt.Errorf("tx no exist in tx pool")
}

// AddTxsToPending remove txs in queue and move txs to pendingCache
func (l *txList) AddTxsToPending(mtxs []*memTx) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	// delete txs in queue
	// add txs to pendingCache
	count := int32(0)
	for _, mtx := range mtxs {
		txId := mtx.getTxId()
		if ok, val := l.queue.Remove(txId); ok && val != nil {
			count++
		}
		l.pendingCache.Store(txId, mtx)
	}
	// update queue size
	l.addQueueSize(-1 * count)
}

// HasTx verify whether tx exist in queue or pendingCache
func (l *txList) HasTx(txId string) bool {
	// get tx from pendingCache
	if val, ok := l.pendingCache.Load(txId); ok && val != nil {
		return true
	}
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// get tx in queue
	return l.queue.Get(txId) != nil
}

// RemoveTxs remove txs from queue or pending by txIds
func (l *txList) RemoveTxs(txIds []string) {
	// wait lock
	l.Lock()
	defer l.Unlock()
	// delete txs from queue and pendingCache
	count := int32(0)
	// QueueDelays := make([]*recorder.QueueDelay, 0)
	strs := ""
	for _, txId := range txIds {
		if ok, val := l.queue.Remove(txId); ok && val != nil {
			count++
		}
		// recorder
		// File, err := os.OpenFile("/home/yaoye/projects/chainmaker-csv/temp/chainmaker-csv/log/QueueDelay.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		// if err != nil {
		// 	l.log.Errorf("File open failed!")
		// }
		// defer File.Close()
		//创建写入接口
		// WriterCsv := csv.NewWriter(File)
		str := fmt.Sprintf("%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), txId, "out") //需要写入csv的数据，切片类型
		strs = strs + str
		// err := recorderfile.Record(str, "tx_queue_delay")
		// if err != nil {
		// 	if err == errors.New("close") {
		// 		l.log.Errorf("[tx_queue_delay] switch closed")
		// 	} else {
		// 		l.log.Errorf("[tx_queue_delay] record failed")
		// 	}
		// }
		// l.log.Infof("[tx_queue_delay] record succeed")

		// queueDelay := &recorder.QueueDelay{
		// 	Tx_timestamp: time.Now(),
		// 	TxID_qude:    txId,
		// 	InTime:       t,
		// 	OutTime:      time.Now(),
		// }
		// resultC := make(chan error, 1)
		// // recorder.RecordBatch(QueueDelays, QueueDelays[0], resultC)
		// recorder.Record(queueDelay, resultC) // 异步执行，记录到数据库

		// select {
		// case err := <-resultC:
		// 	if err != nil {
		// 		// 记录数据出现错误
		// 		l.log.Errorf("[QueueDelay] record failed:submodule/txpool-normal/v2@v2.3.0/tx_list.go:RemoveTxs()")
		// 	} else {
		// 		// 记录数据成功
		// 		l.log.Infof("[QueueDelay] record succeed:submodule/txpool-normal/v2@v2.3.0/tx_list.go:RemoveTxs()")
		// 	}
		// case <-time.NewTimer(time.Second).C:
		// 	// 记录数据超时
		// 	l.log.Errorf("[QueueDelay] record timeout:submodule/txpool-normal/v2@v2.3.0/tx_list.go:RemoveTxs()")
		// }
		// QueueDelays = append(QueueDelays, &recorder.QueueDelay{
		// 	Tx_timestamp: time.Now(),
		// 	TxID_qude:    txId,
		// 	InTime:       t,
		// 	OutTime:      time.Now(),
		// })

		// if performance.QueueDelay() {
		// 	t, _ := time.Parse(time.RFC3339, "1890-01-02T15:04:05Z")
		// 	queueDelay := &model.QueueDelay{
		// 		Tx_timestamp: time.Now(),
		// 		TxID_qude:    txId,
		// 		InTime:       t,
		// 		OutTime:      time.Now(),
		// 	}
		// 	signal := make(chan error, 1)
		// 	performance.Record(queueDelay, signal)
		// 	err := <-signal
		// 	if err != nil {
		// 		l.log.Infof("mysql write error: %s", err)
		// 	}
		// }
		l.pendingCache.Delete(txId)
	}
	_ = recorderfile.Record(strs, "tx_queue_delay")
	// if err != nil {
	// 	if err == errors.New("close") {
	// 		l.log.Errorf("[tx_queue_delay] switch closed")
	// 	} else {
	// 		l.log.Errorf("[tx_queue_delay] record failed")
	// 	}
	// }
	// l.log.Infof("[tx_queue_delay] record succeed")
	
	// update queue size
	l.addQueueSize(-1 * count)
}

// RemoveTxsInPending remove txs from pending by txIds
func (l *txList) RemoveTxsInPending(txIds []string) {
	// delete txs from pendingCache
	// QueueDelays := make([]*recorder.QueueDelay, 0)
	strs := ""
	for _, txId := range txIds {
		// recorder
		// QueueDelays = append(QueueDelays, &recorder.QueueDelay{
		// 	Tx_timestamp: time.Now(),
		// 	TxID_qude:    txId,
		// 	InTime:       t,
		// 	OutTime:      time.Now(),
		// })
		// File, err := os.OpenFile("/home/yaoye/projects/chainmaker-csv/temp/chainmaker-csv/log/QueueDelay.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		// if err != nil {
		// 	l.log.Errorf("File open failed!")
		// }
		// defer File.Close()
		//创建写入接口
		// WriterCsv := csv.NewWriter(File)
		str := fmt.Sprintf("%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), txId, "out") //需要写入csv的数据，切片类型
		strs = strs + str

		// queueDelay := &recorder.QueueDelay{
		// 	Tx_timestamp: time.Now(),
		// 	TxID_qude:    txId,
		// 	InTime:       t,
		// 	OutTime:      time.Now(),
		// }
		// resultC := make(chan error, 1)
		// recorder.Record(queueDelay, resultC) // 异步执行，记录到数据库
		// // recorder.RecordBatch(QueueDelays, QueueDelays[0], resultC)

		// select {
		// case err := <-resultC:
		// 	if err != nil {
		// 		// 记录数据出现错误
		// 		l.log.Errorf("[QueueDelay] record failed:submodule/txpool-normal/v2@v2.3.0/tx_list.go:RemoveTxsInPending()")
		// 	} else {
		// 		// 记录数据成功
		// 		l.log.Infof("[QueueDelay] record succeed:submodule/txpool-normal/v2@v2.3.0/tx_list.go:RemoveTxsInPending()")
		// 	}
		// case <-time.NewTimer(time.Second).C:
		// 	// 记录数据超时
		// 	l.log.Errorf("[QueueDelay] record timeout:submodule/txpool-normal/v2@v2.3.0/tx_list.go:RemoveTxsInPending()")
		// }
		l.pendingCache.Delete(txId)
	}
	_ = recorderfile.Record(strs, "tx_queue_delay")
	// if err != nil {
	// 	if err == errors.New("close") {
	// 		l.log.Errorf("[tx_queue_delay] switch closed")
	// 	} else {
	// 		l.log.Errorf("[tx_queue_delay] record failed")
	// 	}
	// }
	// l.log.Infof("[tx_queue_delay] record succeed")
	
}

// Size return the size of queue and pendingCache
func (l *txList) Size() (queueSize, pendingSize int) {
	// get pendingCache size
	l.pendingCache.Range(func(k, v interface{}) bool {
		pendingSize++
		return true
	})
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// get queue size
	queueSize = l.queue.Size()
	return
}

// QueueSize return the size of queue
func (l *txList) QueueSize() (queueSize int) {
	// wait lock
	l.RLock()
	defer l.RUnlock()
	// get queue size
	queueSize = l.queue.Size()
	return
}

// GetTxsByStage get txs in queue or pendingCache
func (l *txList) GetTxsByStage(inQueue, inPending bool) (txs []*commonPb.Transaction, txIds []string) {
	// new value
	queueSize, pendingSize := l.Size()
	totalSize := queueSize + pendingSize
	txs = make([]*commonPb.Transaction, 0, totalSize)
	txIds = make([]string, 0, totalSize)
	// get txs in pending
	if inPending {
		l.pendingCache.Range(func(k, v interface{}) bool {
			txs = append(txs, v.(*memTx).getTx())
			txIds = append(txIds, k.(string))
			return true
		})
	}
	// get txs in queue
	if inQueue {
		l.RLock()
		defer l.RUnlock()
		node := l.queue.GetLinkList().Front()
		for node != nil {
			txId, ok := node.Value.(string)
			if !ok {
				l.log.Errorf("interface value transfer into string failed")
			}
			mtx, ok1 := l.queue.Get(txId).(*memTx)
			if !ok1 {
				l.log.Errorf("interface val transfer into *memTx failed")
			}
			// append tx
			txs = append(txs, mtx.getTx())
			// append txIds
			txIds = append(txIds, txId)
			node = node.Next()
		}
	}
	return
}

// addTx add tx to txList
func (l *txList) addTx(mtx *memTx, validate txValidateFunc) {
	// validate tx if validate is not nil
	if validate == nil || validate(mtx) == nil {
		// recorder
		// File, err := os.OpenFile("/home/yaoye/projects/chainmaker-csv/temp/chainmaker-csv/log/QueueDelay.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		// if err != nil {
		// 	l.log.Errorf("File open failed!")
		// }
		// defer File.Close()
		//创建写入接口
		// WriterCsv := csv.NewWriter(File)
		str := fmt.Sprintf("%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), mtx.getTxId(), "in") //需要写入csv的数据，切片类型
		_ = recorderfile.Record(str, "tx_queue_delay")
		// if err != nil {
		// 	if err == errors.New("close") {
		// 		l.log.Errorf("[QueueDelay] switch closed")
		// 	} else {
		// 		l.log.Errorf("[QueueDelay] record failed")
		// 	}
		// }
		// queueDelay := &recorder.QueueDelay{
		// 	Tx_timestamp: time.Now(),
		// 	TxID_qude:    mtx.getTxId(),
		// 	InTime:       time.Now(),
		// 	OutTime:      t,
		// }
		// resultC := make(chan error, 1)
		// recorder.Record(queueDelay, resultC) // 异步执行，记录到数据库

		// select {
		// case err := <-resultC:
		// 	if err != nil {
		// 		// 记录数据出现错误
		// 		l.log.Errorf("[QueueDelay] record failed:submodule/txpool-normal/v2@v2.3.0/tx_list.go:addTx()")
		// 	} else {
		// 		// 记录数据成功
		// 		l.log.Infof("[QueueDelay] record succeed:submodule/txpool-normal/v2@v2.3.0/tx_list.go:addTx()")

		// 	}
		// case <-time.NewTimer(time.Second).C:
		// 	// 记录数据超时
		// 	l.log.Errorf("[QueueDelay] record timeout:submodule/txpool-normal/v2@v2.3.0/tx_list.go:addTx()")

		// }

		// if performance.QueueDelay() {
		// 	t, _ := time.Parse(time.RFC3339, "1890-01-02T15:04:05Z")
		// 	queueDelay := &model.QueueDelay{
		// 		Tx_timestamp: time.Now(),
		// 		TxID_qude:    mtx.getTxId(),
		// 		InTime:       time.Now(),
		// 		OutTime:      t,
		// 	}
		// 	signal := make(chan error, 1)
		// 	performance.Record(queueDelay, signal)
		// 	err := <-signal
		// 	if err != nil {
		// 		l.log.Infof("mysql write error")
		// 	}
		// 	//l.log.Infof("mysql write queue delay intime")
		// }
		// update queue size
		l.addQueueSize(1)
		// add tx to txList
		l.queue.Add(mtx.getTxId(), mtx)
	}
}

// popTxsFromQueue get txs from queue
func (l *txList) popTxsFromQueue(count int, validate txValidateFunc) (
	mTxs []*memTx, txIds []string, errKeys []string) {
	// new value
	mTxs = make([]*memTx, 0, count)
	txIds = make([]string, 0, count)
	errKeys = make([]string, 0, count)
	// get tx from the front of txList
	node := l.queue.GetLinkList().Front()
	for node != nil && count > 0 {
		txId, ok := node.Value.(string)
		if !ok {
			l.log.Errorf("interface value transfer into string failed")
		}
		mtx, ok1 := l.queue.Get(txId).(*memTx)
		if !ok1 {
			l.log.Errorf("interface val transfer into *memTx failed")
		}
		// verify whether tx is timeout
		if validate != nil && validate(mtx) != nil {
			errKeys = append(errKeys, txId)
		} else {
			// ensure tx no in pending
			if _, ok2 := l.pendingCache.Load(txId); ok2 {
				l.log.Warnf("tx:%s can not duplicate to package block", txId)
				errKeys = append(errKeys, txId)
			} else {
				mTxs = append(mTxs, mtx)
				txIds = append(txIds, txId)
				// need to get all valid txs
				count--
			}
		}
		node = node.Next()
	}
	return
}

// addQueueSize modify queueSizeAtomic
func (l *txList) addQueueSize(num int32) (queueSize int32) {
	return atomic.AddInt32(l.queueSizeAtomic, num)
}

// metricMonitor metric txList queue size
func (l *txList) metricMonitor(mtx *memTx, len int32) {
	if MonitorEnabled {
		// config tx
		if isConfigTx(mtx.getTx(), l.chainConf) {
			go l.metricTxPoolSize.WithLabelValues(mtx.getChainId(), "config").Set(float64(len))
			// common tx
		} else {
			go l.metricTxPoolSize.WithLabelValues(mtx.getChainId(), "normal").Set(float64(len))
		}
	}
}
