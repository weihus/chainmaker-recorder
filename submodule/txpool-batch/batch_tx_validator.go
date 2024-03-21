/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"fmt"
	"math"
	"sync"

	"chainmaker.org/chainmaker/common/v2/birdsnest"
	"chainmaker.org/chainmaker/common/v2/crypto/hash"
	commonErrors "chainmaker.org/chainmaker/common/v2/errors"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
)

// TxBatchSourceMod is the source of batch
type TxBatchSourceMod int

const (
	// TxBatchSourceUnknown is unknown
	TxBatchSourceUnknown TxBatchSourceMod = iota
	// TxBatchByBroadcast identify the batch is from other node by broadcasting
	TxBatchByBroadcast
	// TxBatchByRecover identify the batch is from proposer by recovering
	TxBatchByRecover
	// TxBatchByRetry identify the batch is from core by pruning
	TxBatchByRetry
	// TxBatchByRePlay identify the batch is from wal by replaying
	TxBatchByRePlay
)

// TxVerifyMod is the verify mod of tx
type TxVerifyMod int

const (
	// VerifyTxTime verify timestamp
	VerifyTxTime TxVerifyMod = iota
	// VerifyTxAuth verify tx format and signature
	VerifyTxAuth
	// VerifyTxInDB verify in full db
	VerifyTxInDB
)

func genTxVerifyMods(batchSource TxBatchSourceMod) []TxVerifyMod {
	switch batchSource {
	case TxBatchByBroadcast:
		return []TxVerifyMod{VerifyTxTime, VerifyTxAuth, VerifyTxInDB}
	case TxBatchByRecover:
		return []TxVerifyMod{VerifyTxAuth, VerifyTxInDB}
	case TxBatchByRetry:
		return []TxVerifyMod{VerifyTxAuth, VerifyTxInDB}
	case TxBatchByRePlay:
		return []TxVerifyMod{VerifyTxTime, VerifyTxAuth, VerifyTxInDB}
	default:
		return []TxVerifyMod{VerifyTxTime, VerifyTxAuth, VerifyTxInDB}
	}
}

// =====================================================================================================================
//   TxBatch verification in pool
// =====================================================================================================================
// validateTxBatches verify the validity of batches
func (pool *batchTxPool) validateTxBatches(batches []*txpoolPb.TxBatch, batchSource TxBatchSourceMod) []*memTxBatch {
	var (
		segBatches    = segmentBatches(batches)
		mBatchesTable = make([][]*memTxBatch, len(segBatches))
		i             = -1
		wg            sync.WaitGroup
	)
	for _, perBatches := range segBatches {
		i++
		wg.Add(1)
		go func(i int, perBatches []*txpoolPb.TxBatch) {
			defer wg.Done()
			mBatchesCache := make([]*memTxBatch, 0, len(perBatches))
			for _, batch := range perBatches {
				if mBatch, err := pool.validateTxBatch(batch, batchSource); err == nil {
					mBatchesCache = append(mBatchesCache, mBatch)
				}
			}
			mBatchesTable[i] = mBatchesCache
		}(i, perBatches)
	}
	wg.Wait()
	return mergeMBatches(mBatchesTable)
}

// validateTxBatch verify the validity of the batch
// batchSource may be TxBatchByBroadcast, TxBatchByRecover, TxBatchByRetry and TxBatchByRePlay
func (pool *batchTxPool) validateTxBatch(batch *txpoolPb.TxBatch, batchSource TxBatchSourceMod) (
	mBatch *memTxBatch, err error) {
	// verify format, contains batchId len, txs num, txIdsMap and txs order
	if len(batch.BatchId) != defaultBatchIdLen {
		pool.log.Warnf("batchId len is not %d, batchId:%x", defaultBatchIdLen, batch.BatchId)
		return nil, fmt.Errorf("batchId len is not %d, batchId:%x", defaultBatchIdLen, batch.BatchId)
	}
	if len(batch.Txs) != int(batch.Size_) || len(batch.Txs) != len(batch.TxIdsMap) {
		pool.log.Warnf("batch txs num is not equal, batchId:%x(%s), txs:%d, size_:%d, txsMap:%d",
			batch.BatchId, getNodeId(batch.BatchId), len(batch.Txs), batch.Size_, len(batch.TxIdsMap))
		return nil, fmt.Errorf("batch txs num is not equal, batchId:%x(%s)",
			batch.BatchId, getNodeId(batch.BatchId))
	}
	for i, tx := range batch.Txs {
		txId := tx.Payload.TxId
		if idx, ok := batch.TxIdsMap[txId]; !ok || int(idx) != i {
			pool.log.Warnf("batch TxIdsMap is invalid, batchId:%x(%s), txId:%s, ok:%v, idxInTxs:%d,idxInMap:%d",
				batch.BatchId, getNodeId(batch.BatchId), txId, ok, i, idx)
			return nil, fmt.Errorf("batch TxIdsMap is invalid, batchId:%x(%s), txId:%s, "+
				"ok:%v, idxInTxs:%d,idxInMap:%d",
				batch.BatchId, getNodeId(batch.BatchId), txId, ok, i, idx)
		}
	}
	if len(batch.Txs) > 1 {
		for _, tx := range batch.Txs {
			if isConfigTx(tx, pool.chainConf) {
				pool.log.Warnf("common batch should not contain config tx, batchId:%x(%s), txId:%s",
					batch.BatchId, getNodeId(batch.BatchId), tx.Payload.TxId)
				return nil, fmt.Errorf("common batch should not contain config tx, "+
					"batchId:%x(%s), txId:%s", batch.BatchId, getNodeId(batch.BatchId), tx.Payload.TxId)
			}
		}
	}
	if !isSorted(batch.Txs) {
		pool.log.Warnf("txs in batch are not ordered, batchId:%x(%s)",
			batch.BatchId, getNodeId(batch.BatchId))
		return nil, fmt.Errorf("txs in batch are not ordered, batchId:%x(%s)",
			batch.BatchId, getNodeId(batch.BatchId))
	}
	// BatchMaxSize may be not equal in different nodes
	// so batch size should not be greater than BlockTxCapacity
	blockCapacity := MaxTxCount(pool.chainConf)
	if len(batch.Txs) > blockCapacity {
		pool.log.Warnf("batch txs num:%d more than blockCapacity:%d, batchId:%x(%s)",
			len(batch.Txs), blockCapacity, batch.BatchId, getNodeId(batch.BatchId))
		return nil, fmt.Errorf("batch txs num:%d more than blockCapacity:%d, batchId:%x(%s)",
			len(batch.Txs), blockCapacity, batch.BatchId, getNodeId(batch.BatchId))
	}
	// verify whether batch signature is valid
	if err = pool.validateBatchSign(batch); err != nil {
		return nil, err
	}
	// verify whether batch that received from other nodes exist in pool
	if batchSource == TxBatchByBroadcast ||
		batchSource == TxBatchByRePlay {
		if exist := pool.queue.txBatchExist(batch); exist {
			pool.log.Warnf("batch exist in pool, batchId:%x(%s)",
				batch.BatchId, getNodeId(batch.BatchId))
			return nil, fmt.Errorf("batch exist in pool, batchId:%x(%s)",
				batch.BatchId, getNodeId(batch.BatchId))
		}
	}

	// verify whether txs in batch are valid concurrently
	dbHeight, filter := pool.validateTxs(batch.Txs, genTxVerifyMods(batchSource)...)
	// create memTxBatch
	mBatch = newMemTxBatch(batch, dbHeight, filter)
	// verify whether txs in batch are all invalid
	if mBatch.getValidTxNum() == 0 {
		pool.log.Warnf("txs in batch are all invalid, batchId:%x(%s)",
			batch.BatchId, getNodeId(batch.BatchId))
		return nil, fmt.Errorf("txs in batch are all invalid, batchId:%x(%s)",
			batch.BatchId, getNodeId(batch.BatchId))
	}
	return
}

// validateBatchSign verify batch signature
func (pool *batchTxPool) validateBatchSign(batch *txpoolPb.TxBatch) error {
	// only contains txs, and no result in any tx.
	batchCopy := &txpoolPb.TxBatch{
		Txs: copyTxs(batch.Txs),
	}
	// verify whether batchHash is valid
	batchHash, err := hash.GetByStrType(pool.chainConf.ChainConfig().Crypto.Hash, mustMarshal(batchCopy))
	if err != nil {
		pool.log.Errorf("calc batch hash failed, err:%v", err)
		return err
	}
	batchHashInId := getBatchHash(batch.BatchId)
	batchHashCalc := cutoutBatchHash(batchHash)
	if batchHashInId != batchHashCalc {
		pool.log.Warnf("batchHash is not equal, batchId:%x(%s), hash in batchId:%x, calc hash:%x",
			batch.BatchId, getNodeId(batch.BatchId), batchHashInId, batchHashCalc)
		return fmt.Errorf("batchHash is not equal, batchId:%x(%s), hash in batchId:%x, calc hash:%x",
			batch.BatchId, getNodeId(batch.BatchId), batchHashInId, batchHashCalc)
	}
	// verify whether signature is valid
	principal, err := pool.ac.CreatePrincipal(
		protocol.ResourceNameP2p,
		[]*commonPb.EndorsementEntry{batch.Endorsement},
		[]byte(batch.BatchId),
	)
	if err != nil {
		pool.log.Warnf("validateBatchSign new policy failed, err:%v", err)
		return err
	}
	result, err := pool.ac.VerifyPrincipal(principal)
	if err != nil {
		pool.log.Warnf("validateBatchSign verify principal failed, err:%v", err)
		return err
	}
	if !result {
		pool.log.Warnf("validateBatchSign verify principal result:%v", result)
		return fmt.Errorf("validateBatchSign result:%v", result)
	}
	// verify whether nodeId is forged
	member, err := pool.ac.NewMember(batch.Endorsement.Signer)
	if err != nil {
		pool.log.Warnf("validateBatchSign new member failed %v", err)
		return err
	}
	uid, err := pool.netService.GetNodeUidByCertId(member.GetMemberId())
	if err != nil {
		pool.log.Warnf("validateBatchSign get nodeUid by certId failed err:%v", err)
		return err
	}
	if cutoutNodeId(uid) != getNodeId(batch.BatchId) {
		pool.log.Warnf("nodeId in invalid, uid:%s nodeId:%s ", cutoutNodeId(uid), getNodeId(batch.BatchId))
		return fmt.Errorf("nodeId in invalid, uid:%s nodeId:%s ", cutoutNodeId(uid), getNodeId(batch.BatchId))
	}
	return nil
}

// retryTxBatchValidate verify whether txs in batch is valid when retry batch to batchList
func (pool *batchTxPool) retryTxBatchValidate(mBatch *memTxBatch) error {
	// verify whether txs in batch are valid concurrently
	dbHeight, filter := pool.validateTxs(mBatch.getTxs(), genTxVerifyMods(TxBatchByRetry)...)
	// update dbHeight and filter
	mBatch.dbHeight = dbHeight
	mBatch.filter = filter
	// verify whether txs in batch are all invalid
	if mBatch.getValidTxNum() == 0 {
		pool.log.Warnf("txs in batch are all invalid, batchId:%x(%s)",
			mBatch.getBatchId(), getNodeId(mBatch.getBatchId()))
		return fmt.Errorf("txs in batch are all invalid, batchId:%x(%s)",
			mBatch.getBatchId(), getNodeId(mBatch.getBatchId()))
	}
	return nil
}

// =====================================================================================================================
//   Transaction verification in pool
// =====================================================================================================================
// validateTxs verify whether txs is valid concomitantly
// and return min dbHeight and filter table
func (pool *batchTxPool) validateTxs(txs []*commonPb.Transaction, verifyMods ...TxVerifyMod) (
	uint64, []bool) {
	// should not be nil
	if len(txs) == 0 {
		return math.MaxUint64, nil
	}
	// verify whether txs in batch are valid concurrently
	var (
		txsTable      = segmentTxs(txs)
		filterTable   = make([][]bool, len(txsTable))
		dbHeightTable = make([]uint64, len(txsTable))
	)
	for i := range dbHeightTable {
		dbHeightTable[i] = math.MaxUint64
	}
	// verify perTxs concurrently
	var wg sync.WaitGroup
	for i, perTxs := range txsTable {
		wg.Add(1)
		go func(idx int, perTxs []*commonPb.Transaction) {
			defer wg.Done()
			perFilter := make([]bool, len(perTxs))
			for j, tx := range perTxs {
				if mTx, err := pool.validateTx(tx, verifyMods...); err == nil {
					if mTx.dbHeight < dbHeightTable[idx] {
						dbHeightTable[idx] = mTx.dbHeight
					}
					perFilter[j] = true
				}
			}
			// put filter result to table
			filterTable[idx] = perFilter
		}(i, perTxs)
	}
	wg.Wait()
	return minValue(dbHeightTable...), mergeFilters(filterTable)
}

// validateTx verify whether timestamp is overdue, exist in full db,
// and (signature and format is valid from p2p)
func (pool *batchTxPool) validateTx(tx *commonPb.Transaction, verifyMods ...TxVerifyMod) (*memTx, error) {
	var (
		dbHeight = uint64(0)
		err      error
	)
	for _, verifyMod := range verifyMods {
		switch verifyMod {
		// verify timestamp
		case VerifyTxTime:
			if err = validateTxTime(tx, pool.chainConf); err != nil {
				pool.log.Warnf("transaction timestamp is timeout, txId:%s", tx.Payload.GetTxId())
				return nil, err
			}
			// verify signature and format
		case VerifyTxAuth:
			if err = utils.VerifyTxWithoutPayload(tx, pool.chainId, pool.ac); err != nil {
				pool.log.Warnf("validate transaction without payload failed, txId:%s, err:%v",
					tx.Payload.GetTxId(), err)
				return nil, err
			}
			// judge whether tx is duplicate within full db
		case VerifyTxInDB:
			if dbHeight, err = pool.isTxExistInFullDB(tx); err != nil {
				return nil, err
			}
		}
	}
	if dbHeight == 0 {
		dbHeight = pool.txFilter.GetHeight()
	}
	return newMemTx(tx, dbHeight), nil
}

// validateTxTime verify whether timestamp is overdue
func validateTxTime(tx *commonPb.Transaction, conf protocol.ChainConf) error {
	if IsTxTimeVerify(conf) {
		txTimestamp := tx.Payload.Timestamp
		chainTime := utils.CurrentTimeSeconds()
		if math.Abs(float64(chainTime-txTimestamp)) > MaxTxTimeTimeout(conf) {
			return commonErrors.ErrTxTimeout
		}
	}
	return nil
}

// isTxExistInFullDB verify whether transaction exist in full db
func (pool *batchTxPool) isTxExistInFullDB(tx *commonPb.Transaction) (dbHeight uint64, err error) {
	// should not be nil
	if pool.txFilter == nil {
		return math.MaxUint64, fmt.Errorf("txFilter should not be nil")
	}
	var exist bool
	txId := tx.Payload.GetTxId()
	exist, dbHeight, _, err = pool.txFilter.IsExistsAndReturnHeight(txId, birdsnest.RuleType_AbsoluteExpireTime)
	// if txFilter failed, return err to client
	if err != nil {
		pool.log.Warnf("txFilter failed, txId:%s, err:%v", txId, err)
		return math.MaxUint64, err
	}
	// if tx Exist in db, return ErrTxIdExistDB
	if exist {
		pool.log.Warnf("transaction exists in DB, txId:%s", txId)
		return dbHeight, commonErrors.ErrTxIdExistDB
	}
	return
}

// isTxExistInIncrementDB verify whether transaction exist in increment db
func (pool *batchTxPool) isTxExistInIncrementDB(tx *commonPb.Transaction, dbHeight uint64) (exist bool) {
	// should not be nil
	if pool.blockStore == nil {
		pool.log.Warnf("blockStore should not be nil")
		return true
	}
	var err error
	txId := tx.Payload.GetTxId()
	// if chainStore failed, return err to client
	exist, err = pool.blockStore.TxExistsInIncrementDB(txId, dbHeight+1)
	if err != nil {
		pool.log.Warnf("blockStore failed, txId:%s, err:%v", txId, err)
		return true
	}
	// if tx Exist in db, return ErrTxIdExistDB
	if exist {
		pool.log.Warnf("transaction exists in DB, txId:%s", txId)
	}
	return
}

// =====================================================================================================================
//   TxBatch verification in batchList
// =====================================================================================================================
// addTxBatchValidate verify whether batch exist in pool when put batch to batchList
func (l *batchList) addTxBatchValidate(mBatch *memTxBatch) error {
	// verify whether the batch exist in queue and pendingCache
	if l.txBatchExistInPool(mBatch) {
		l.log.Warnf("batch exists in pool, batchId:%x(%s)", mBatch.getBatchId(), getNodeId(mBatch.getBatchId()))
		return commonErrors.ErrTxIdExist
	}
	return nil
}

// fetchTxBatchValidate verify whether txs in batch is expired when fetch txs to build block from batchList
func (l *batchList) fetchTxBatchValidate(mBatch *memTxBatch) error {
	// verify whether the transaction timestamp is expired
	for i, tx := range mBatch.batch.Txs {
		if mBatch.filter[i] {
			if err := validateTxTime(tx, l.chainConf); err != nil {
				l.log.Warnf("transaction timestamp is timeout, batchId:%x(%s), txId:%s",
					mBatch.getBatchId(), getNodeId(mBatch.getBatchId()), tx.Payload.GetTxId())
				mBatch.filter[i] = false
			}
		}
	}
	if mBatch.getValidTxNum() == 0 {
		l.log.Warnf("txs in batch are all timeout, when fetching, batchId:%x(%s)",
			mBatch.getBatchId(), getNodeId(mBatch.getBatchId()))
		return fmt.Errorf("txs in batch are all timeout, when fetching, batchId:%x(%s)",
			mBatch.getBatchId(), getNodeId(mBatch.getBatchId()))
	}
	return nil
}

// txBatchExistInPool verify whether batch exist in queue and pendingCache
func (l *batchList) txBatchExistInPool(mBatch *memTxBatch) bool {
	batchId := mBatch.getBatchId()
	if val, ok := l.pendingCache.Load(batchId); ok && val != nil {
		return true
	}
	if l.queue.Get(batchId) != nil {
		return true
	}
	return false
}
