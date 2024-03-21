/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"fmt"
	"math"
	"sync"

	"chainmaker.org/chainmaker/common/v2/birdsnest"
	commonErr "chainmaker.org/chainmaker/common/v2/errors"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
)

// validateTxs verify whether txs timestamp is out of date, signature is valid and exist in db.
func (pool *normalPool) validateTxs(txs []*commonPb.Transaction, txSource protocol.TxSource) (
	validTxs []*commonPb.Transaction, mtxsTable [][]*memTx) {
	if len(txs) == 0 {
		return nil, nil
	}
	// distribute common txs to different commonQueue
	txsTable := pool.dispatcher.DistTxs(txs, pool.queue.commonQueuesNum())
	// validate common txs, verify timestamp、signature(format)、not in db
	mtxsTable = make([][]*memTx, len(txsTable))
	validTxsTable := make([][]*commonPb.Transaction, len(txsTable))
	var wg sync.WaitGroup
	for i, perTxs := range txsTable {
		if len(perTxs) == 0 {
			continue
		}
		wg.Add(1)
		go func(i int, perTxs []*commonPb.Transaction) {
			defer wg.Done()
			mtxsCache := make([]*memTx, 0, len(perTxs))
			validTxsCache := make([]*commonPb.Transaction, 0, len(perTxs))
			for _, tx := range perTxs {
				if mtx, err := pool.validateTx(tx, txSource); err == nil {
					mtxsCache = append(mtxsCache, mtx)
					validTxsCache = append(validTxsCache, tx)
				}
			}
			mtxsTable[i] = mtxsCache
			validTxsTable[i] = validTxsCache
		}(i, perTxs)
	}
	wg.Wait()
	// return all valid txs and mtxs table
	return mergeTxs(validTxsTable), mtxsTable
}

// validateTx verify whether a tx timestamp is out of date, signature is valid and exist in db.
// tx may be config or common tx, and txSource may be RPC/P2P/INTERNAL
func (pool *normalPool) validateTx(tx *commonPb.Transaction, txSource protocol.TxSource) (*memTx, error) {
	if err := validateTxTime(tx, pool.chainConf); err != nil {
		pool.log.Warnf("transaction timestamp is timeout, txId:%s", tx.Payload.GetTxId())
		return nil, err
	}
	if txSource == protocol.P2P {
		if err := utils.VerifyTxWithoutPayload(tx, tx.Payload.ChainId, pool.ac); err != nil {
			pool.log.Warnf("validate transaction without payload failed, txId:%s, err:%v",
				tx.Payload.GetTxId(), err)
			return nil, err
		}
	}
	dbHeight, err := pool.isTxExistsInFullDB(tx)
	if err != nil {
		return nil, err
	}
	return newMemTx(tx, txSource, dbHeight), nil
}

// isTxExistsInFullDB verify whether tx is duplicate with full db
func (pool *normalPool) isTxExistsInFullDB(tx *commonPb.Transaction) (dbHeight uint64, err error) {
	if pool.txFilter == nil {
		return math.MaxUint64, fmt.Errorf("txFilter should not be nil")
	}
	var exist bool
	txId := tx.Payload.GetTxId()
	exist, dbHeight, _, err = pool.txFilter.IsExistsAndReturnHeight(txId, birdsnest.RuleType_AbsoluteExpireTime)
	if err != nil {
		pool.log.Warnf("txFilter failed, txId:%s, err:%v", txId, err)
		return math.MaxUint64, err
	}
	if exist {
		pool.log.Warnf("transaction exists in DB, txId: %s", txId)
		return dbHeight, commonErr.ErrTxIdExistDB
	}
	return
}

// isTxExistsInIncrementDB verify whether tx is duplicate with increment db
func (pool *normalPool) isTxExistsInIncrementDB(tx *commonPb.Transaction, dbHeight uint64) (exist bool) {
	if pool.blockStore == nil {
		pool.log.Warnf("blockStore should not be nil")
		return true
	}
	var err error
	txId := tx.Payload.GetTxId()
	exist, err = pool.blockStore.TxExistsInIncrementDB(txId, dbHeight+1)
	if err != nil {
		pool.log.Warnf("blockStore failed, txId:%s, err:%v", txId, err)
		return true
	}
	if exist {
		pool.log.Warnf("transaction exists in DB, txId: %s", txId)
	}
	return
}

// addTxValidate verify the validity of the transaction when put tx to txList
func (l *txList) addTxValidate(memTx *memTx) error {
	// verify whether the transaction is duplicate with queue and pendingCache
	if l.txExistInPool(memTx) {
		l.log.Warnf("transaction exists in pool, txId: %s", memTx.getTxId())
		return commonErr.ErrTxIdExist
	}
	return nil
}

// fetchTxValidate verify the validity of the transaction when fetch txs to build block from txList
func (l *txList) fetchTxValidate(memTx *memTx) error {
	// verify whether the transaction timestamp is expired
	if err := validateTxTime(memTx.getTx(), l.chainConf); err != nil {
		l.log.Warnf("transaction timestamp is timeout, txId:%s", memTx.getTxId())
		return err
	}
	return nil
}

// retryTxValidate verify the validity of the transaction when retry tx to txList.queue.
func (l *txList) retryTxValidate(memTx *memTx) error {
	// verify whether the transaction is duplicate with queue
	if l.txExistInQueueCache(memTx) {
		l.log.Warnf("transaction exists in pool, txId:%s", memTx.getTxId())
		return commonErr.ErrTxIdExist
	}
	return nil
}

// txExistInPool verify whether transaction exist in queue or pending
func (l *txList) txExistInPool(memTx *memTx) bool {
	if val, ok := l.pendingCache.Load(memTx.getTxId()); ok && val != nil {
		return true
	}
	if l.queue.Get(memTx.getTxId()) != nil {
		return true
	}
	return false
}

// txExistInQueueCache verify whether transaction exist in queue
func (l *txList) txExistInQueueCache(memTx *memTx) bool {
	return l.queue.Get(memTx.getTxId()) != nil
}

// validateTxTime verify whether transaction is overdue
func validateTxTime(tx *commonPb.Transaction, conf protocol.ChainConf) error {
	if IsTxTimeVerify(conf) {
		timestamp := tx.Payload.Timestamp
		chainTime := utils.CurrentTimeSeconds()
		if math.Abs(float64(chainTime-timestamp)) > MaxTxTimeTimeout(conf) {
			return commonErr.ErrTxTimeout
		}
	}
	return nil
}
