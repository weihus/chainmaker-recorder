/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
)

type txValidateFunc func(tx *memTx) error

// mempoolTxs contains some mtxs to put a batch of txs to txList
type mempoolTxs struct {
	isConfigTxs bool
	mtxs        []*memTx
	source      protocol.TxSource
}

// memTx is the transaction in pool
type memTx struct {
	tx       *commonPb.Transaction // transaction
	dbHeight uint64                // db height when validate tx exist in full db
}

// create memTx
func newMemTx(tx *commonPb.Transaction, dbHeight uint64) *memTx {
	return &memTx{
		tx:       tx,
		dbHeight: dbHeight,
	}
}

// getChainId get chainId in memTx
func (mtx *memTx) getChainId() string {
	return mtx.tx.Payload.ChainId
}

// getTxId get txId in memTx
func (mtx *memTx) getTxId() string {
	return mtx.tx.Payload.TxId
}

// getTxId get tx in memTx
func (mtx *memTx) getTx() *commonPb.Transaction {
	return mtx.tx
}
