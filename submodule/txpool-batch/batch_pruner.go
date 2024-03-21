/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"sync"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
)

// pruneTxBatches prune txs in batches that are duplicated and exist in increment DB
func (pool *batchTxPool) pruneTxBatches(mTxBatches []*memTxBatch) (
	txsRet [][]*commonPb.Transaction, batchIdsRet, errBatchIds []string) {
	if len(mTxBatches) == 0 {
		return
	}
	// prune txs in batch that are in increment DB
	var (
		txsRetTable = make([][]*commonPb.Transaction, len(mTxBatches))
		wg          sync.WaitGroup
	)
	for i, mBatch := range mTxBatches {
		wg.Add(1)
		go func(i int, mBatch *memTxBatch) {
			defer wg.Done()
			txsRetTable[i] = pool.pruneTxBatch(mBatch)
		}(i, mBatch)
	}
	wg.Wait()
	// prune txs are duplicated among batches
	txNum := len(mTxBatches) * BatchMaxSize(pool.chainConf)
	txIdsMap := make(map[string]struct{}, txNum)
	txsRet = make([][]*commonPb.Transaction, 0, len(mTxBatches))
	batchIdsRet = make([]string, 0, txNum)
	errBatchIds = make([]string, 0, txNum)
	for i, perTxs := range txsRetTable {
		hasValidTx := false
		txsCache := make([]*commonPb.Transaction, 0, len(perTxs))
		for _, tx := range perTxs {
			txId := tx.Payload.TxId
			if _, ok := txIdsMap[txId]; !ok {
				txIdsMap[txId] = struct{}{}
				txsCache = append(txsCache, tx)
				hasValidTx = true
			} else {
				pool.log.Warnf("transaction is duplicated in different batches, txId:%s", txId)
			}
		}
		// if all txs in batch are duplicated with txs in before batches
		// it will be invalid batch
		if hasValidTx {
			txsRet = append(txsRet, txsCache)
			batchIdsRet = append(batchIdsRet, mTxBatches[i].getBatchId())
		} else {
			errBatchIds = append(errBatchIds, mTxBatches[i].getBatchId())
		}
	}
	return
}

// pruneTxBatch prune txs in txBatch and return txs are not in increment DB
func (pool *batchTxPool) pruneTxBatch(mTxBatch *memTxBatch) []*commonPb.Transaction {
	// should not be nil
	if mTxBatch == nil {
		return nil
	}
	// verify whether txs exist in the increment db
	txsTable := segmentTxs(mTxBatch.getValidTxs())
	if len(txsTable) == 0 {
		return nil
	}
	// verify tx concurrently
	var (
		validTxsTable = make([][]*commonPb.Transaction, len(txsTable))
		wg            sync.WaitGroup
	)
	for i, perTxs := range txsTable {
		wg.Add(1)
		go func(i int, perTxs []*commonPb.Transaction) {
			defer wg.Done()
			txsCache := make([]*commonPb.Transaction, 0, len(perTxs))
			for _, tx := range perTxs {
				if !pool.isTxExistInIncrementDB(tx, mTxBatch.dbHeight) {
					txsCache = append(txsCache, tx)
				}
			}
			// put result to table
			validTxsTable[i] = txsCache
		}(i, perTxs)
	}
	wg.Wait()
	return mergeTxs(validTxsTable)
}
