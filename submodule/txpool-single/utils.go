/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// isConfigTx verify whether tx is config tx
func isConfigTx(tx *commonPb.Transaction, chainConf protocol.ChainConf) bool {
	if utils.IsConfigTx(tx) ||
		utils.IsManageContractAsConfigTx(tx, chainConf.ChainConfig().Contract.EnableSqlSupport) ||
		utils.IsManagementTx(tx) {
		return true
	}
	return false
}

// copyTx shallow copy to create a new transaction
func copyTx(tx *commonPb.Transaction) *commonPb.Transaction {
	return &commonPb.Transaction{
		Payload:   tx.Payload,
		Sender:    tx.Sender,
		Endorsers: tx.Endorsers,
	}
}

// maxVal return the larger value
func maxVal(val1, val2 int) int {
	if val1 > val2 {
		return val1
	}
	return val2
}

// mustMarshal marshals protobuf message to byte slice or panic when marshal failed
func mustMarshal(msg proto.Message) (data []byte) {
	var err error
	data, err = proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return
}

// mustUnmarshal unmarshals from byte slice to protobuf message or panic
func mustUnmarshal(b []byte, msg proto.Message) { // nolint
	if err := proto.Unmarshal(b, msg); err != nil {
		panic(err)
	}
}

// printTxIdsInTxs get txIds in txs
func printTxIdsInTxs(txs []*commonPb.Transaction) []string {
	var txIds = make([]string, 0, len(txs))
	for _, tx := range txs {
		txIds = append(txIds, getLast8TxId(tx.Payload.TxId))
	}
	return txIds
}

// printTxIdsInTxsMap get txIds in txs
func printTxIdsInTxsMap(txs map[string]*commonPb.Transaction) []string {
	var txIds = make([]string, 0, len(txs))
	for txId := range txs {
		txIds = append(txIds, getLast8TxId(txId))
	}
	return txIds
}

// getLast8TxId  get last 8 bytes txId
func getLast8TxId(txId string) string {
	if len(txId) > 8 {
		return txId[len(txId)-8:]
	}
	return txId
}

// =====================================================================================================================
//   Business tools
// =====================================================================================================================
// segmentTxs segment txs to some group
func segmentTxs(txs []*commonPb.Transaction) (txsRetTable [][]*commonPb.Transaction) {
	// should not be nil
	if len(txs) == 0 {
		return
	}
	// segment txs to workers group
	workers := utils.CalcTxVerifyWorkers(len(txs))
	perWorkerTxsNum := len(txs) / workers
	txsRetTable = make([][]*commonPb.Transaction, workers)
	for i := 0; i < workers; i++ {
		txsPerWorker := txs[i*perWorkerTxsNum : (i+1)*perWorkerTxsNum]
		if i == workers-1 {
			txsPerWorker = txs[i*perWorkerTxsNum:]
		}
		txsRetTable[i] = txsPerWorker
	}
	return
}

// segmentMTxs segment mTxs to some group
func segmentMTxs(txs []*memTx) (txsRetTable [][]*memTx) {
	// should not be nil
	if len(txs) == 0 {
		return
	}
	// segment txs to workers group
	workers := utils.CalcTxVerifyWorkers(len(txs))
	perWorkerTxsNum := len(txs) / workers
	txsRetTable = make([][]*memTx, workers)
	for i := 0; i < workers; i++ {
		txsPerWorker := txs[i*perWorkerTxsNum : (i+1)*perWorkerTxsNum]
		if i == workers-1 {
			txsPerWorker = txs[i*perWorkerTxsNum:]
		}
		txsRetTable[i] = txsPerWorker
	}
	return
}

// mergeTxs merge some txs slice to a txs slice
func mergeTxs(txsTable [][]*commonPb.Transaction) (txsRet []*commonPb.Transaction) {
	// should not be nil
	if len(txsTable) == 0 {
		return
	}
	txsRet = make([]*commonPb.Transaction, 0, len(txsTable)*len(txsTable[0]))
	for _, txs := range txsTable {
		txsRet = append(txsRet, txs...)
	}
	return txsRet
}

// mergeMTxs merge some mTxs slice to a mTxs slice
func mergeMTxs(txsTable [][]*memTx) (txsRet []*memTx) {
	// should not be nil
	if len(txsTable) == 0 {
		return
	}
	txsRet = make([]*memTx, 0, len(txsTable)*len(txsTable[0]))
	for _, txs := range txsTable {
		txsRet = append(txsRet, txs...)
	}
	return txsRet
}
