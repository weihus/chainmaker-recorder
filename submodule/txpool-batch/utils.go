/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// =====================================================================================================================
//   Basic tools
// =====================================================================================================================
// isConfigTx verify whether tx is config tx
func isConfigTx(tx *commonPb.Transaction, chainConf protocol.ChainConf) bool {
	if utils.IsConfigTx(tx) ||
		utils.IsManageContractAsConfigTx(tx, chainConf.ChainConfig().Contract.EnableSqlSupport) ||
		utils.IsManagementTx(tx) {
		return true
	}
	return false
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

// nolint
// mustUnmarshal unmarshals from byte slice to protobuf message or panic
func mustUnmarshal(b []byte, msg proto.Message) {
	if err := proto.Unmarshal(b, msg); err != nil {
		panic(err)
	}
}

// minValue get min value in vales
func minValue(vales ...uint64) (minVal uint64) {
	minVal = math.MaxUint64
	for _, val := range vales {
		if val < minVal {
			minVal = val
		}
	}
	return
}

// maxValue get max value in vales
func maxValue(vales ...uint64) (maxVal uint64) {
	maxVal = 0
	for _, val := range vales {
		if val > maxVal {
			maxVal = val
		}
	}
	return
}

// maxValueIndex get the index of max value in vales
func maxValueIndex(vales []int) (index int) {
	index = 0
	maxVal := math.MinInt32
	for i, val := range vales {
		if val > maxVal {
			maxVal = val
			index = i
		}
	}
	return
}

// =====================================================================================================================
//   Shallow copy tools
// =====================================================================================================================
// copyTxBatch shallow copy txs to create a new txBatch
func copyTxBatch(batch *txpoolPb.TxBatch) *txpoolPb.TxBatch {
	txsCopy := make([]*commonPb.Transaction, 0, len(batch.Txs))
	for _, tx := range batch.Txs {
		txsCopy = append(txsCopy, copyTx(tx))
	}
	return &txpoolPb.TxBatch{
		BatchId:     batch.BatchId,
		Txs:         txsCopy,
		Size_:       batch.Size_,
		TxIdsMap:    batch.TxIdsMap,
		Endorsement: batch.Endorsement,
	}
}

// copyTxs shallow copy to create a new transaction slice
func copyTxs(txs []*commonPb.Transaction) []*commonPb.Transaction {
	txsCopy := make([]*commonPb.Transaction, 0, len(txs))
	for _, tx := range txs {
		txsCopy = append(txsCopy, copyTx(tx))
	}
	return txsCopy
}

// copyTx shallow copy to create a new transaction, no need result
func copyTx(tx *commonPb.Transaction) *commonPb.Transaction {
	return &commonPb.Transaction{
		Payload:   tx.Payload,
		Sender:    tx.Sender,
		Endorsers: tx.Endorsers,
	}
}

// =====================================================================================================================
//   Calc tools
// =====================================================================================================================
// calcTxNumInMemBatches get txNum in memBatches
func calcTxNumInMemBatches(memBatches []*memTxBatch) int32 {
	var txNum int32
	for _, mBatch := range memBatches {
		txNum += mBatch.getTxNum()
	}
	return txNum
}

// calcValidTxNumInMemBatches get valid txNum in memBatches
func calcValidTxNumInMemBatches(memBatches []*memTxBatch) int32 {
	var txNum int32
	for _, mBatch := range memBatches {
		txNum += mBatch.getValidTxNum()
	}
	return txNum
}

// calcTxNumInMemBatches get txNum in memBatches
func calcTxNumInMemBatchesMap(memBatches map[string]*memTxBatch) int32 {
	var txNum int32
	for _, mBatch := range memBatches {
		txNum += mBatch.getTxNum()
	}
	return txNum
}

// calcTxNumInTxsTable get txNum in txsTable
func calcTxNumInTxsTable(txsTable [][]*commonPb.Transaction) int {
	var txNum int
	for _, perTxs := range txsTable {
		txNum += len(perTxs)
	}
	return txNum
}

// printTxIdsInTxsTable get txIds in txsTable
func printTxIdsInTxsTable(txsTable [][]*commonPb.Transaction) [][]string {
	var txIds = make([][]string, 0, len(txsTable))
	for _, perTxs := range txsTable {
		perTxIds := make([]string, 0, len(perTxs))
		for _, tx := range perTxs {
			perTxIds = append(perTxIds, getLast8TxId(tx.Payload.TxId))
		}
		txIds = append(txIds, perTxIds)
	}
	return txIds
}

// printTxIdsInTxs get txIds in txs
func printTxIdsInTxs(txs []*commonPb.Transaction) []string {
	var txIds = make([]string, 0, len(txs))
	for _, tx := range txs {
		txIds = append(txIds, getLast8TxId(tx.Payload.TxId))
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

// printBatchIds print batchId by readable form %x(%s)
func printBatchIds(batchIds []string) []string {
	var ids = make([]string, 0, len(batchIds))
	for _, batchId := range batchIds {
		id := fmt.Sprintf("%x(%s)", batchId, getNodeId(batchId))
		ids = append(ids, id)
	}
	return ids
}

// getTimestamp get timestamp in batchId
func getTimestamp(batchId string) string { // nolint
	// batchId=timestamp(8)+nodeId(8)+batchHash(8)
	return batchId[:8]
}

// getNodeIdAndBatchHash get nodeId and batchHash in batchId
func getNodeIdAndBatchHash(batchId string) string {
	// batchId=timestamp(8)+nodeId(8)+batchHash(8)
	return batchId[8:]
}

// getNodeId get nodeId in batchId
func getNodeId(batchId string) string {
	// batchId=timestamp(8)+nodeId(8)+batchHash(8)
	return batchId[8:16]
}

// getBatchHash get batchHash in batchId
func getBatchHash(batchId string) string {
	// batchId=timestamp(8)+nodeId(8)+batchHash(8)
	return batchId[16:]
}

// generateTimestamp returns nanosecond string
func generateTimestamp() string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(time.Now().UnixNano()))
	return string(b)
}

// parseTimestampToNano parse timestamp string to nanosecond
func parseTimestampToNano(timestamp string) (nano int64, err error) { // nolint
	if len(timestamp) != 8 {
		return -1, fmt.Errorf("fmt err, len(b)=%d", len(timestamp))
	}
	timestampBz := []byte(timestamp)
	nano = int64(binary.BigEndian.Uint64(timestampBz[:8]))
	return
}

// cutoutNodeId returns the last 8 bytes nodeId as string
func cutoutNodeId(nodeId string) string {
	return nodeId[len(nodeId)-8:]
}

// cutoutBatchHash returns the last 8 bytes batch hash as string
func cutoutBatchHash(batchHash []byte) string {
	return string(batchHash[len(batchHash)-8:])
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

// mergeFilters merge some filter slice to a filter slice
func mergeFilters(filterTable [][]bool) (filterRet []bool) {
	// should not be nil
	if len(filterTable) == 0 {
		return
	}
	filterRet = make([]bool, 0, len(filterTable)*len(filterTable[0]))
	for _, filter := range filterTable {
		filterRet = append(filterRet, filter...)
	}
	return filterRet
}

// sortTxs sort txs by timestamp from smallest to largest
// if timestamp are same, then sort txs by txId by lexicographical order
func sortTxs(txs []*commonPb.Transaction) []*commonPb.Transaction {
	sort.SliceStable(txs, func(i, j int) bool {
		if txs[i].Payload.Timestamp != txs[j].Payload.Timestamp {
			return txs[i].Payload.Timestamp < txs[j].Payload.Timestamp
		}
		return strings.Compare(txs[i].Payload.TxId, txs[j].Payload.TxId) == -1
	})
	return txs
}

// isSorted verify whether txs are sorted by timestamp from smallest to largest
// or lexicographical order when timestamp are same
func isSorted(txs []*commonPb.Transaction) bool {
	for i, tx := range txs {
		if i == 0 {
			continue
		}
		// timestamp is not from smallest to largest
		if txs[i-1].Payload.Timestamp > tx.Payload.Timestamp {
			return false
		} else if txs[i-1].Payload.Timestamp == tx.Payload.Timestamp {
			// txId is not lexicographical order when timestamp are same
			if strings.Compare(txs[i-1].Payload.TxId, tx.Payload.TxId) != -1 {
				return false
			}
		}
	}
	return true
}
