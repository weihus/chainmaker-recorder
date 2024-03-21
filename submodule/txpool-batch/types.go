/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
)

// batchValidateFunc is memTxBatch validation func
type batchValidateFunc func(memBatch *memTxBatch) error

// memTx contains transaction and dbHeight
type memTx struct {
	tx       *commonPb.Transaction
	dbHeight uint64 // db height when verify whether tx exist in full db
}

// newMemTx create memTx
func newMemTx(tx *commonPb.Transaction, dbHeight uint64) *memTx {
	return &memTx{
		tx:       tx,
		dbHeight: dbHeight,
	}
}

// getTx get tx from memTx
func (mtx *memTx) getTx() *commonPb.Transaction {
	return mtx.tx
}

// getDBHeight get dbHeight from memTx
func (mtx *memTx) getDBHeight() uint64 {
	return mtx.dbHeight
}

// getTxId get txId from memTx
func (mtx *memTx) getTxId() string {
	return mtx.tx.Payload.TxId
}

// memTxBatch contains tx batch, dbHeight, tx filter
type memTxBatch struct {
	batch    *txpoolPb.TxBatch
	dbHeight uint64
	filter   []bool // flag bit that indicates whether tx that in batch.Txs is valid
}

// newMemTxBatch creates memTxBatch
func newMemTxBatch(batch *txpoolPb.TxBatch, dbHeight uint64, filter []bool) *memTxBatch {
	return &memTxBatch{
		batch:    batch,
		dbHeight: dbHeight,
		filter:   filter,
	}
}

// getBatch return txBatch in memTxBatch
func (mbc *memTxBatch) getBatch() *txpoolPb.TxBatch {
	return mbc.batch
}

// getBatchId return batchId in memTxBatch
func (mbc *memTxBatch) getBatchId() string {
	return mbc.batch.BatchId
}

// getTxs return txs in memTxBatch
func (mbc *memTxBatch) getTxs() []*commonPb.Transaction {
	return mbc.batch.Txs
}

// getValidTxs return valid txs in memTxBatch
func (mbc *memTxBatch) getValidTxs() []*commonPb.Transaction {
	validTxs := make([]*commonPb.Transaction, 0, len(mbc.batch.Txs))
	for i := 0; i < len(mbc.batch.Txs) && i < len(mbc.filter); i++ {
		if mbc.filter[i] {
			validTxs = append(validTxs, mbc.batch.Txs[i])
		}
	}
	return validTxs
}

// getValidTxNum return tx num in batch
func (mbc *memTxBatch) getTxNum() int32 {
	return int32(len(mbc.batch.Txs))
}

// getValidTxNum return valid tx num in batch
func (mbc *memTxBatch) getValidTxNum() int32 {
	var validTxNum int32
	for _, valid := range mbc.filter {
		if valid {
			validTxNum++
		}
	}
	return validTxNum
}
