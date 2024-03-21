/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"fmt"
	"math"
	"sync"

	"chainmaker.org/chainmaker/common/v2/birdsnest"
	commonErrors "chainmaker.org/chainmaker/common/v2/errors"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
)

// validateTxs verify the validity of the transactions
// when the source type is P2P, additional certificate and tx header checks will be performed
// return valid memTxs and valid txs
func (pool *txPoolImpl) validateTxs(txs []*commonPb.Transaction, source protocol.TxSource) (
	[]*memTx, []*commonPb.Transaction) {
	// segment txs to some group
	txsTable := segmentTxs(txs)
	// result table
	mtxsTable := make([][]*memTx, len(txsTable))
	validTxsTable := make([][]*commonPb.Transaction, len(txsTable))
	// verify tx synchronously
	var wg sync.WaitGroup
	for i, perTxs := range txsTable {
		wg.Add(1)
		go func(i int, perTxs []*commonPb.Transaction) {
			defer wg.Done()
			mtxs := make([]*memTx, 0, len(perTxs))
			validTxs := make([]*commonPb.Transaction, 0, len(perTxs))
			for _, tx := range perTxs {
				if mtx, err := pool.validateTx(tx, source); err == nil {
					mtxs = append(mtxs, mtx)
					validTxs = append(validTxs, tx)
				}
			}
			mtxsTable[i] = mtxs
			validTxsTable[i] = validTxs
		}(i, perTxs)
	}
	wg.Wait()
	return mergeMTxs(mtxsTable), mergeTxs(validTxsTable)
}

// validateTx verify the validity of the transaction
// when the source type is P2P, additional certificate and tx header checks will be performed
func (pool *txPoolImpl) validateTx(tx *commonPb.Transaction, source protocol.TxSource) (*memTx, error) {
	startVerifySignTime := utils.CurrentTimeMillisSeconds()
	defer func() {
		pool.metrics(fmt.Sprintf("validate tx time, txId:%s, source:%d", tx.Payload.TxId, source),
			startVerifySignTime, utils.CurrentTimeMillisSeconds())
	}()
	if err := validateTxTime(tx, pool.chainConf); err != nil {
		pool.log.Warnf("transaction timestamp is timeout, txId:%s", tx.Payload.GetTxId())
		return nil, err
	}
	if source == protocol.P2P {
		if err := utils.VerifyTxWithoutPayload(tx, pool.chainId, pool.ac); err != nil {
			pool.log.Warnf("validate transaction without payload failed, txId:%s, err:%v",
				tx.Payload.GetTxId(), err)
			return nil, err
		}
	}
	startVerifyDBTime := utils.CurrentTimeMillisSeconds()
	dbHeight, err := pool.isTxExistInFullDB(tx)
	if err != nil {
		return nil, err
	}
	endTime := utils.CurrentTimeMillisSeconds()
	pool.monitorValidateTxSign(startVerifyDBTime - startVerifySignTime)
	pool.monitorValidateTxInDB(endTime - startVerifyDBTime)
	return newMemTx(tx, dbHeight), nil
}

// isTxExistInDB verifies whether the transaction exists in the full db
func (pool *txPoolImpl) isTxExistInFullDB(tx *commonPb.Transaction) (dbHeight uint64, err error) {
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
		return dbHeight, commonErrors.ErrTxIdExistDB
	}
	return
}

// isTxExistsInIncrementDB verifies whether the transaction exists in the increment db
func (pool *txPoolImpl) isTxExistsInIncrementDB(tx *commonPb.Transaction, dbHeight uint64) (exist bool) {
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

// fetchTxValidate verify the validity of the transaction when fetch txs to build block from txList
func (l *txList) fetchTxValidate(mtx *memTx) error {
	// verify whether the transaction timestamp is expired
	if err := validateTxTime(mtx.getTx(), l.chainConf); err != nil {
		l.log.Warnf("transaction timestamp is timeout, txId:%s", mtx.getTxId())
		return err
	}
	return nil
}

// validateTxTime verify whether tx is timeout
func validateTxTime(tx *commonPb.Transaction, conf protocol.ChainConf) error {
	if IsTxTimeVerify(conf) {
		timestamp := tx.Payload.Timestamp
		chainTime := utils.CurrentTimeSeconds()
		if math.Abs(float64(chainTime-timestamp)) > MaxTxTimeTimeout(conf) {
			return commonErrors.ErrTxTimeout
		}
	}
	return nil
}
