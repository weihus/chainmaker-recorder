/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
)

// batchQueue is tx batch pool contains config batch queue and common batch queues
type batchQueue struct {
	configBatchQueue    *batchList // config batch queue, only one
	configTxCountAtomic *int32     // config tx num

	sync.RWMutex
	commonBatchQueues   map[string]*batchList // common batch queues, key:nodeId, val:*batchList
	commonTxCountAtomic *int32                // common tx num

	chainConf protocol.ChainConf
	log       protocol.Logger
}

// newBatchQueue create batchQueue
func newBatchQueue(chainConf protocol.ChainConf, log protocol.Logger) *batchQueue {
	// init tx count
	configTxCountAtomic := int32(0)
	commonTxCountAtomic := int32(0)
	// create batchQueue
	return &batchQueue{
		configBatchQueue:    newBatchList(&configTxCountAtomic, chainConf, log),
		configTxCountAtomic: &configTxCountAtomic,
		commonBatchQueues:   make(map[string]*batchList, len(chainConf.ChainConfig().Consensus.Nodes)),
		commonTxCountAtomic: &commonTxCountAtomic,
		chainConf:           chainConf,
		log:                 log,
	}
}

// addConfigTxBatch add a config batch to the queue of configBatchQueue
func (queue *batchQueue) addConfigTxBatch(mBatch *memTxBatch) {
	defer func() {
		queue.log.Debugf("txPool status: %s", queue.status())
	}()
	confBatchQue := queue.configBatchQueue
	confBatchQue.PutTxBatch(mBatch, confBatchQue.addTxBatchValidate)
}

// addConfigTxBatchToPending add a config batch to the pendingCache of configBatchQueue
func (queue *batchQueue) addConfigTxBatchToPending(mBatch *memTxBatch) {
	confBatchQue := queue.configBatchQueue
	confBatchQue.PutTxBatchToPendingCache(mBatch, nil)
}

// addCommonTxBatch add a common batch to the queue of commonBatchQueues
func (queue *batchQueue) addCommonTxBatch(mBatch *memTxBatch) {
	defer func() {
		queue.log.Debugf("txPool status: %s", queue.status())
	}()
	comBatchQue := queue.getCommonBatchQueue(mBatch.getBatchId())
	comBatchQue.PutTxBatch(mBatch, comBatchQue.addTxBatchValidate)
}

// addCommonTxBatchToPending add a common batch to the pendingCache of commonBatchQueues
func (queue *batchQueue) addCommonTxBatchToPending(mBatch *memTxBatch) {
	comBatchQue := queue.getCommonBatchQueue(mBatch.getBatchId())
	comBatchQue.PutTxBatchToPendingCache(mBatch, nil)
}

// addCommonTxBatches add common batches to commonBatchQueues concurrently
func (queue *batchQueue) addCommonTxBatches(mBatches []*memTxBatch) {
	defer func() {
		queue.log.Debugf("txPool status: %s", queue.status())
	}()
	var (
		segMBatches = segmentMemBatches(mBatches)
		wg          sync.WaitGroup
	)
	for _, perBatches := range segMBatches {
		if len(perBatches) == 0 {
			continue
		}
		wg.Add(1)
		go func(perBatches []*memTxBatch) {
			defer wg.Done()
			comBatchQue := queue.getCommonBatchQueue(perBatches[0].getBatchId())
			comBatchQue.PutTxBatches(perBatches, comBatchQue.addTxBatchValidate)
		}(perBatches)
	}
	wg.Wait()
}

// fetchTxBatches fetch batches from configBatchQueue and commonBatchQueues
func (queue *batchQueue) fetchTxBatches() (batchesRet []*memTxBatch) {
	// first fetch batch from configBatchQueue
	if configTxCount := queue.configTxCount(); configTxCount > 0 {
		confBatchQue := queue.configBatchQueue
		batchesRet = confBatchQue.FetchTxBatches(1, confBatchQue.fetchTxBatchValidate)
		queue.log.Debugw("batchQueue fetch config batches", "configTxCount", configTxCount,
			"batches", len(batchesRet), "txs", calcTxNumInMemBatches(batchesRet))
		return
	}
	// second fetch batch from commonBatchQueues concurrently
	if commonTxCount := queue.commonTxCount(); commonTxCount > 0 {
		comBatchQueues := queue.getAllCommonBatchQueues()
		txCounts := queue.calcFetchTxCount(comBatchQueues)
		mBatchesTable := make([][]*memTxBatch, len(comBatchQueues))
		var wg sync.WaitGroup
		for i, comBatchQue := range comBatchQueues {
			wg.Add(1)
			go func(i int, comBatchQue *batchList) {
				defer wg.Done()
				mBatchesTable[i] = comBatchQue.FetchTxBatches(txCounts[i], comBatchQue.fetchTxBatchValidate)
			}(i, comBatchQue)
		}
		wg.Wait()
		batchesRet = mergeMBatches(mBatchesTable)

		// if the num of txs in pool is greater than block capacity
		// and fetched valid txs is not block capacity, it will fetch again.
		fetchTxCount := calcValidTxNumInMemBatches(batchesRet)
		blockCapacity := int32(MaxTxCount(queue.chainConf))
		if commonTxCount >= blockCapacity && fetchTxCount <= blockCapacity-int32(BatchMaxSize(queue.chainConf)) {
			secTxCount := blockCapacity - fetchTxCount
			secComBatchQue := comBatchQueues[maxValueIndex(txCounts)]
			secFetchedRet := secComBatchQue.FetchTxBatches(int(secTxCount), secComBatchQue.fetchTxBatchValidate)
			batchesRet = append(batchesRet, secFetchedRet...)
		}
		queue.log.Debugw("batchQueue fetch common batches",
			"batches", len(batchesRet), "first fetch txs", fetchTxCount, "total txs", calcTxNumInMemBatches(batchesRet))
		return
	}
	// there is no txs in pool
	return
}

// getTxBatches get batches from configBatchQueue and commonBatchQueues by batchIds
func (queue *batchQueue) getTxBatches(batchIds []string, stage StageMod) (
	batchesRet map[string]*memTxBatch, batchesMis map[string]struct{}) {
	// get config batches from configBatchQueue
	batchesRet, batchesMis = queue.configBatchQueue.GetTxBatches(batchIds, stage)
	if len(batchesRet) == len(batchIds) && len(batchesMis) == 0 {
		queue.log.Debugw("batchQueue get config batches", "batchIds", len(batchIds),
			"got", len(batchesRet), "mis", len(batchesMis))
		return
	}
	// get common batches from commonBatchQueues concurrently
	var (
		segBatchIds     = segmentBatchIds(batchIds)
		batchesRetTable = make([]map[string]*memTxBatch, len(segBatchIds))
		i               = -1
		wg              sync.WaitGroup
	)
	for _, perBatchIds := range segBatchIds {
		i++
		if len(perBatchIds) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int, perBatchIds []string) {
			defer wg.Done()
			comBatchQue := queue.getCommonBatchQueue(perBatchIds[0])
			batchesCache, _ := comBatchQue.GetTxBatches(perBatchIds, stage)
			batchesRetTable[i] = batchesCache
		}(i, perBatchIds)
	}
	wg.Wait()
	// return result
	if len(batchesRet) == 0 {
		// note: may be batches are in configBatchQueue or commonBatchQueue
		// when retry or remove batches
		batchesRet = make(map[string]*memTxBatch, len(batchIds))
	}
	batchesMis = make(map[string]struct{}, len(batchIds))
	for _, batchesMap := range batchesRetTable {
		for batchId, mBatch := range batchesMap {
			batchesRet[batchId] = mBatch
		}
	}
	for _, batchId := range batchIds {
		if _, ok := batchesRet[batchId]; !ok {
			batchesMis[batchId] = struct{}{}
		}
	}
	queue.log.Debugw("batchQueue get common batches", "batchIds", len(batchIds),
		"got", len(batchesRet), "mis", len(batchesMis))
	return
}

// moveBatchesToQueue move batches to the queue of configBatchQueue or commonBatchQueues
func (queue *batchQueue) moveBatchesToQueue(batchIds []string, validate batchValidateFunc) {
	// add config batch to the queue of configBatchQueue
	if ok := queue.configBatchQueue.MoveTxBatchesToQueue(batchIds, validate); ok {
		queue.log.Debugw("batchQueue move config batches to queue", "batchIds", len(batchIds),
			"result", ok)
		return
	}
	// add common batches to queue of commonBatchQueues concurrently
	var wg sync.WaitGroup
	for _, perBatchIds := range segmentBatchIds(batchIds) {
		if len(perBatchIds) == 0 {
			continue
		}
		wg.Add(1)
		go func(perBatchIds []string) {
			defer wg.Done()
			comBatchQue := queue.getCommonBatchQueue(perBatchIds[0])
			comBatchQue.MoveTxBatchesToQueue(perBatchIds, validate)
		}(perBatchIds)
	}
	wg.Wait()
	queue.log.Debugw("batchQueue move common batches to queue", "batchIds", len(batchIds))
}

// moveBatchesToPending move batches to the pendingCache of configBatchQueue or commonBatchQueues
func (queue *batchQueue) moveBatchesToPending(batchIds []string) {
	// add config batch to the pendingCache of configBatchQueue
	if ok := queue.configBatchQueue.MoveTxBatchesToPending(batchIds); ok {
		queue.log.Debugw("batchQueue move config batches to pending", "batchIds", len(batchIds),
			"result", ok)
		return
	}
	// add common batches to pendingCache of commonBatchQueues concurrently
	var wg sync.WaitGroup
	for _, perBatchIds := range segmentBatchIds(batchIds) {
		if len(perBatchIds) == 0 {
			continue
		}
		wg.Add(1)
		go func(perBatchIds []string) {
			defer wg.Done()
			comBatchQue := queue.getCommonBatchQueue(perBatchIds[0])
			comBatchQue.MoveTxBatchesToPending(perBatchIds)
		}(perBatchIds)
	}
	wg.Wait()
	queue.log.Debugw("batchQueue move common batches to pending", "batchIds", len(batchIds))
}

// removeBatches remove batches in configBatchQueue and commonBatchQueues by batchIds
func (queue *batchQueue) removeBatches(batchIds []string) {
	// remove config batch from configBatchQueue
	if ok := queue.configBatchQueue.RemoveTxBatches(batchIds, StageInQueueAndPending); ok {
		return
	}
	// remove common batches from commonBatchQueues concurrently
	var wg sync.WaitGroup
	for _, perBatchIds := range segmentBatchIds(batchIds) {
		if len(perBatchIds) == 0 {
			continue
		}
		wg.Add(1)
		go func(perBatchIds []string) {
			defer wg.Done()
			comBatchQue := queue.getCommonBatchQueue(perBatchIds[0])
			comBatchQue.RemoveTxBatches(perBatchIds, StageInQueueAndPending)
		}(perBatchIds)
	}
	wg.Wait()
}

// txExist verify whether transaction exist in pool
func (queue *batchQueue) txExist(tx *commonPb.Transaction) (exist bool) {
	// config tx
	if isConfigTx(tx, queue.chainConf) {
		_, exist = queue.configBatchQueue.GetTx(tx.Payload.TxId)
		return
	}
	// common tx
	for _, batchQue := range queue.getAllCommonBatchQueues() {
		if _, exist = batchQue.GetTx(tx.Payload.TxId); exist {
			return
		}
	}
	return
}

// txBatchExist verify whether batch exist in pool
func (queue *batchQueue) txBatchExist(txBatch *txpoolPb.TxBatch) (exist bool) {
	// config tx batch
	if len(txBatch.Txs) == 1 && isConfigTx(txBatch.Txs[0], queue.chainConf) {
		return queue.configBatchQueue.HasTxBatchWithoutTimestamp(getNodeIdAndBatchHash(txBatch.BatchId))
	}
	// common tx batch
	comBatchQue := queue.getCommonBatchQueue(txBatch.BatchId)
	return comBatchQue.HasTxBatchWithoutTimestamp(getNodeIdAndBatchHash(txBatch.BatchId))
}

// isFull verify whether configBatchQueue or commonBatchQueues is full
func (queue *batchQueue) isFull(tx *commonPb.Transaction) bool {
	// config tx batch
	if isConfigTx(tx, queue.chainConf) {
		configTxSize := queue.configTxCount()
		if configTxSize >= int32(MaxConfigTxPoolSize()) {
			queue.log.Warnf("AddTx configTxPool is full, txId:%s, configQueueSize:%d",
				tx.Payload.GetTxId(), configTxSize)
			return true
		}
		return false
	}
	// common tx batch
	commonTxSize := queue.commonTxCount()
	if commonTxSize >= int32(MaxCommonTxPoolSize()) {
		queue.log.Warnf("AddTx commonTxPool is full, txId:%s, commonQueueSize:%d",
			tx.Payload.GetTxId(), commonTxSize)
		return true
	}
	return false
}

// getCommonBatchQueue return commonQueue by the nodeId in batchId
func (queue *batchQueue) getCommonBatchQueue(batchId string) *batchList {
	nodeId := getNodeId(batchId)
	// get commonBatchQueue by nodeId
	queue.RLock()
	if comBatchQue, ok := queue.commonBatchQueues[nodeId]; ok {
		queue.RUnlock()
		return comBatchQue
	}
	queue.RUnlock()

	// create commonBatchQueue for this node
	queue.Lock()
	defer queue.Unlock()
	if comBatchQue, ok := queue.commonBatchQueues[nodeId]; ok {
		return comBatchQue
	}
	comBatchQue := newBatchList(queue.commonTxCountAtomic, queue.chainConf, queue.log)
	queue.commonBatchQueues[nodeId] = comBatchQue
	return comBatchQue
}

// getAllCommonBatchQueues return all commonBatchQueues
func (queue *batchQueue) getAllCommonBatchQueues() []*batchList {
	queue.RLock()
	defer queue.RUnlock()
	comBatchQueues := make([]*batchList, 0, len(queue.commonBatchQueues))
	for _, batchQue := range queue.commonBatchQueues {
		comBatchQueues = append(comBatchQueues, batchQue)
	}
	return comBatchQueues
}

// calcFetchTxCount calculate fetch txCount for different commonBatchQueue
func (queue *batchQueue) calcFetchTxCount(comBatchQueues []*batchList) (txCountTable []int) {
	var (
		txNumCache = make([]int, len(comBatchQueues))
		txTotal    int32
		wg         sync.WaitGroup
	)
	for i, comBatchQue := range comBatchQueues {
		wg.Add(1)
		go func(i int, comBatchQue *batchList) {
			defer wg.Done()
			txNum := comBatchQue.TxCountInQue()
			txNumCache[i] = txNum
			atomic.AddInt32(&txTotal, int32(txNum))
		}(i, comBatchQue)
	}
	wg.Wait()
	// calc txCount in different commonBatchQueue
	txCountTable = make([]int, len(txNumCache))
	for i, txNum := range txNumCache {
		txCount := (float64(txNum) / float64(txTotal)) * float64(MaxTxCount(queue.chainConf))
		txCountTable[i] = int(math.Ceil(txCount))
	}
	// avoid getting more transactions than BlockTxCapacity
	var count int
	for i := 0; i < len(txCountTable); i++ {
		if count+txCountTable[i] > MaxTxCount(queue.chainConf) {
			txCountTable[i] = MaxTxCount(queue.chainConf) - count
		}
		count += txCountTable[i]
	}
	return
}

// configTxCount return config tx queue size
func (queue *batchQueue) configTxCount() int32 {
	return atomic.LoadInt32(queue.configTxCountAtomic)
}

// commonTxCount return common tx queue size
func (queue *batchQueue) commonTxCount() int32 {
	return atomic.LoadInt32(queue.commonTxCountAtomic)
}

// secondFetchedCommonQueue return the second fetched commonBatchQueue
func (queue *batchQueue) secondFetchedCommonQueue() *batchList { // nolint
	queue.RLock()
	defer queue.RUnlock()
	var (
		maxBatchQue   *batchList
		maxBatchCount int
	)
	for _, batchQue := range queue.commonBatchQueues {
		if batchCount := batchQue.BatchSize(); batchCount >= maxBatchCount {
			maxBatchCount = batchCount
			maxBatchQue = batchQue
		}
	}
	return maxBatchQue
}

// getPoolStatus get pool status
func (queue *batchQueue) getPoolStatus() (txPoolStatus *txpoolPb.TxPoolStatus) {
	var configTxInQueue, configTxInPending, commonTxInQueue, commonTxInPending int
	// get config tx count
	configTxInQueue = queue.configBatchQueue.TxCountInQue()
	configTxInPending = queue.configBatchQueue.TxCountInPending()
	// get common tx count
	for _, comBatchQue := range queue.getAllCommonBatchQueues() {
		qn := comBatchQue.TxCountInQue()
		pn := comBatchQue.TxCountInPending()
		commonTxInQueue += qn
		commonTxInPending += pn
	}
	// return result
	return &txpoolPb.TxPoolStatus{
		ConfigTxPoolSize:     int32(MaxConfigTxPoolSize()),
		CommonTxPoolSize:     int32(MaxCommonTxPoolSize()),
		ConfigTxNumInQueue:   int32(configTxInQueue),
		ConfigTxNumInPending: int32(configTxInPending),
		CommonTxNumInQueue:   int32(commonTxInQueue),
		CommonTxNumInPending: int32(commonTxInPending),
	}
}

// getTxsByTxTypeAndStage get txs by txType and stage
func (queue *batchQueue) getTxsByTxTypeAndStage(txType, txStage int32) (txs []*commonPb.Transaction, txIds []string) {
	// get config txs
	if txType == int32(txpoolPb.TxType_CONFIG_TX) {
		txs, txIds = createTxsAndTxIdsByMBatch(queue.getConfigTxBatchesByTxStage(txStage), queue.chainConf)
		return
		// get common txs
	} else if txType == int32(txpoolPb.TxType_COMMON_TX) {
		txs, txIds = createTxsAndTxIdsByMBatch(queue.getCommonTxBatchesByTxStage(txStage), queue.chainConf)
		return
		// get config and common txs
	} else if txType == int32(txpoolPb.TxType_ALL_TYPE) {
		confTxsQue, confTxIdsQue := createTxsAndTxIdsByMBatch(queue.getConfigTxBatchesByTxStage(txStage), queue.chainConf)
		txs = append(txs, confTxsQue...)
		txIds = append(txIds, confTxIdsQue...)
		commTxsPen, commTxIds := createTxsAndTxIdsByMBatch(queue.getCommonTxBatchesByTxStage(txStage), queue.chainConf)
		txs = append(txs, commTxsPen...)
		txIds = append(txIds, commTxIds...)
		return
	} else {
		queue.log.Errorf("txType:%v or txStage:%v is invalid", txpoolPb.TxType(txType), txpoolPb.TxStage(txStage))
	}
	return
}

// getConfigTxBatchesByTxStage get config txs in queue or pendingCache
func (queue *batchQueue) getConfigTxBatchesByTxStage(txStage int32) (mBatchesRet []*memTxBatch) {
	// get config txs in queue
	if txStage == int32(txpoolPb.TxStage_IN_QUEUE) {
		mBatchesRet, _ = queue.configBatchQueue.GetTxBatchesByStage(StageInQueue)
		return
		// get config txs in pendingCache
	} else if txStage == int32(txpoolPb.TxStage_IN_PENDING) {
		mBatchesRet, _ = queue.configBatchQueue.GetTxBatchesByStage(StageInPending)
		return
		// get config txs in queue and pendingCache
	} else if txStage == int32(txpoolPb.TxStage_ALL_STAGE) {
		mBatchesRet, _ = queue.configBatchQueue.GetTxBatchesByStage(StageInQueueAndPending)
		return
	} else {
		queue.log.Errorf("txStage:%v is invalid", txpoolPb.TxStage(txStage))
	}
	return
}

// getCommonTxBatchesByTxStage get common txs in queue or pendingCache
func (queue *batchQueue) getCommonTxBatchesByTxStage(txStage int32) (mBatchesRet []*memTxBatch) {
	// get common txs in queue
	if txStage == int32(txpoolPb.TxStage_IN_QUEUE) {
		for _, comQueue := range queue.getAllCommonBatchQueues() {
			mBatches, _ := comQueue.GetTxBatchesByStage(StageInQueue)
			mBatchesRet = append(mBatchesRet, mBatches...)
		}
		return
		// get common txs in pendingCache
	} else if txStage == int32(txpoolPb.TxStage_IN_PENDING) {
		for _, comQueue := range queue.getAllCommonBatchQueues() {
			mBatches, _ := comQueue.GetTxBatchesByStage(StageInPending)
			mBatchesRet = append(mBatchesRet, mBatches...)
		}
		return
		// get common txs in queue and pendingCache
	} else if txStage == int32(txpoolPb.TxStage_ALL_STAGE) {
		for _, comQueue := range queue.getAllCommonBatchQueues() {
			mBatches, _ := comQueue.GetTxBatchesByStage(StageInQueueAndPending)
			mBatchesRet = append(mBatchesRet, mBatches...)
		}
		return
	} else {
		queue.log.Errorf("txStage:%v is invalid", txpoolPb.TxStage(txStage))
	}
	return
}

// getTx get tx by txId
func (queue *batchQueue) getTx(txId string) (tx *commonPb.Transaction, exist bool) {
	// config tx
	if tx, exist = queue.configBatchQueue.GetTx(txId); exist {
		return
	}
	// common tx
	for _, batchQue := range queue.getAllCommonBatchQueues() {
		if tx, exist = batchQue.GetTx(txId); exist {
			return
		}
	}
	return
}

// status print tx pool status containing the number of config txs and common txs in queue
func (queue *batchQueue) status() string {
	return fmt.Sprintf("common txs count:%d, config txs count:%d",
		queue.commonTxCount(), queue.configTxCount())
}

// segmentBatchIds segment batchIds to a group by same nodeId
func segmentBatchIds(batchIds []string) (nodeIdToBatchIds map[string][]string) {
	nodeIdToBatchIds = make(map[string][]string, len(batchIds))
	for _, batchId := range batchIds {
		nodeId := getNodeId(batchId)
		batchIdsRet, ok := nodeIdToBatchIds[nodeId]
		if !ok {
			batchIdsRet = make([]string, 0, len(batchIds))
		}
		batchIdsRet = append(batchIdsRet, batchId)
		nodeIdToBatchIds[nodeId] = batchIdsRet
	}
	return
}

// segmentMemBatches segment mBatches to a group by same nodeId
func segmentMemBatches(mBatches []*memTxBatch) (nodeIdToBatches map[string][]*memTxBatch) {
	nodeIdToBatches = make(map[string][]*memTxBatch, len(mBatches))
	for _, mBatch := range mBatches {
		nodeId := getNodeId(mBatch.getBatchId())
		batchesRet, ok := nodeIdToBatches[nodeId]
		if !ok {
			batchesRet = make([]*memTxBatch, 0, len(mBatches))
		}
		batchesRet = append(batchesRet, mBatch)
		nodeIdToBatches[nodeId] = batchesRet
	}
	return
}

// mergeMBatches merge batches table to batches slice
func mergeMBatches(mBatchesTable [][]*memTxBatch) (mBatchesRet []*memTxBatch) {
	// should not be nil
	if len(mBatchesTable) == 0 {
		return
	}
	mBatchesRet = make([]*memTxBatch, 0, len(mBatchesTable)*len(mBatchesTable[0]))
	for _, batches := range mBatchesTable {
		mBatchesRet = append(mBatchesRet, batches...)
	}
	return
}

// segmentBatches segment batches to a group by same nodeId
func segmentBatches(batches []*txpoolPb.TxBatch) (nodeIdToBatches map[string][]*txpoolPb.TxBatch) {
	nodeIdToBatches = make(map[string][]*txpoolPb.TxBatch, len(batches))
	for _, batch := range batches {
		nodeId := getNodeId(batch.BatchId)
		batchesRet, ok := nodeIdToBatches[nodeId]
		if !ok {
			batchesRet = make([]*txpoolPb.TxBatch, 0, len(batches))
		}
		batchesRet = append(batchesRet, batch)
		nodeIdToBatches[nodeId] = batchesRet
	}
	return
}

// createTxsAndTxIdsByMBatch create txs and txIds by mBatches
func createTxsAndTxIdsByMBatch(memBatches []*memTxBatch, chainConf protocol.ChainConf) (
	txs []*commonPb.Transaction, txIds []string) {
	batchSize := BatchMaxSize(chainConf)
	txs = make([]*commonPb.Transaction, 0, len(memBatches)*batchSize)
	txIds = make([]string, 0, len(memBatches)*batchSize)
	for _, mBatch := range memBatches {
		txs = append(txs, mBatch.getTxs()...)
		txIds = append(txIds, createTxIdsByTxs(mBatch.getTxs())...)
	}
	return
}
