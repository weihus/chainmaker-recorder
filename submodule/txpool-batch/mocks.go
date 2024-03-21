/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"math/rand"
	"time"

	"chainmaker.org/chainmaker/common/v2/birdsnest"
	"chainmaker.org/chainmaker/common/v2/crypto"
	"chainmaker.org/chainmaker/common/v2/crypto/hash"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	msgbusmock "chainmaker.org/chainmaker/common/v2/msgbus/mock"
	pbac "chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	configPb "chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/pb-go/v2/txfilter"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"chainmaker.org/chainmaker/protocol/v2/test"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/golang/mock/gomock"
)

const (
	testContract         = "userContract1"
	testChainId          = "chain1"
	testNodeId           = "QmV7N4W1itYc7tykjnJtyzJ1ANR7XsrY3VmKbEybNODEA1" // node1
	requestBatchesNodeId = "QmPT2s6wrQ1STvSYEz5tBPspYGz6xzBJgkU524L8NODEC3" // node3

)

var (
	testNodeIdsSet = []string{
		"QmV7N4W1itYc7tykjnJtyzJ1ANR7XsrY3VmKbEybNODEA1",
		"QmNhumtsH8n3LP7RJ3xKtLqiw646gtn9dwqfcKn1NODEB2",
		"QmPT2s6wrQ1STvSYEz5tBPspYGz6xzBJgkU524L8NODEC3",
		"QmVCeTiDjdAM2r95U6poWmRsVmPPkdRA54AWArcuNODED4",
	}
)

func newMockLogger() protocol.Logger {
	return &test.GoLogger{}
}

func newMockChainConf(ctrl *gomock.Controller, blockTxCapacity uint32, timeVerify bool) protocol.ChainConf {
	mockCC := mock.NewMockChainConf(ctrl)
	mockCC.EXPECT().ChainConfig().AnyTimes().DoAndReturn(func() *configPb.ChainConfig {
		return &configPb.ChainConfig{
			Block: &configPb.BlockConfig{
				TxTimestampVerify: timeVerify,
				TxTimeout:         1, // s
				BlockTxCapacity:   blockTxCapacity,
			},
			Crypto: &configPb.CryptoConfig{
				Hash: "SHA256",
			},
			Contract: &configPb.ContractConfig{},
			Consensus: &configPb.ConsensusConfig{
				Nodes: []*configPb.OrgConfig{
					{NodeId: []string{"QmV7N4W1itYc7tykjnJtyzJ1ANR7XsrY3VmKbEybdkcx1K"}},
					{NodeId: []string{"QmNhumtsH8n3LP7RJ3xKtLqiw646gtn9dwqfcKn1k647Gt"}},
					{NodeId: []string{"QmPT2s6wrQ1STvSYEz5tBPspYGz6xzBJgkU524L8usqn4B"}},
					{NodeId: []string{"QmVCeTiDjdAM2r95U6poWmRsVmPPkdRA54AWArcugw5Esh"}},
				},
			},
			Core: &configPb.CoreConfig{
				ConsensusTurboConfig: &configPb.ConsensusTurboConfig{
					ConsensusMessageTurbo: true,
				},
			},
		}
	})
	return mockCC
}

type mockTxFilter struct {
	blockHeight uint64
	txs         map[string]*commonPb.Transaction
	txFilter    protocol.TxFilter
}

func newMockTxFilter(ctrl *gomock.Controller, txsMap map[string]*commonPb.Transaction) *mockTxFilter {
	filter := mock.NewMockTxFilter(ctrl)
	mockFilter := &mockTxFilter{txFilter: filter, txs: txsMap}

	filter.EXPECT().IsExistsAndReturnHeight(gomock.Any(), gomock.Any()).DoAndReturn(
		func(txId string, ruleType ...birdsnest.RuleType) (bool, uint64, *txfilter.Stat, error) {
			_, exist := mockFilter.txs[txId]
			return exist, 0, nil, nil
		}).AnyTimes()

	filter.EXPECT().GetHeight().DoAndReturn(
		func() uint64 {
			return mockFilter.blockHeight
		}).AnyTimes()
	return mockFilter
}

type mockBlockChainStore struct {
	blockHeight uint64
	txs         map[string]*commonPb.Transaction
	store       protocol.BlockchainStore
}

func newMockBlockChainStore(ctrl *gomock.Controller, txsMap map[string]*commonPb.Transaction) *mockBlockChainStore {
	store := mock.NewMockBlockchainStore(ctrl)
	mockStore := &mockBlockChainStore{store: store, txs: txsMap}

	store.EXPECT().GetTx(gomock.Any()).DoAndReturn(func(txId string) (*commonPb.Transaction, error) {
		tx := mockStore.txs[txId]
		return tx, nil
	}).AnyTimes()
	store.EXPECT().TxExists(gomock.Any()).DoAndReturn(func(txId string) (bool, error) {
		_, exist := mockStore.txs[txId]
		return exist, nil
	}).AnyTimes()
	store.EXPECT().TxExistsInFullDB(gomock.Any()).DoAndReturn(func(txId string) (bool, uint64, error) {
		_, exist := mockStore.txs[txId]
		return exist, mockStore.blockHeight, nil
	}).AnyTimes()

	store.EXPECT().TxExistsInIncrementDB(gomock.Any(), gomock.Any()).DoAndReturn(
		func(txId string, startHeight uint64) (bool, error) {
			_, exist := mockStore.txs[txId]
			return exist, nil
		}).AnyTimes()

	return mockStore
}

func newMockMessageBus(ctrl *gomock.Controller) msgbus.MessageBus {
	mockMsgBus := msgbusmock.NewMockMessageBus(ctrl)
	mockMsgBus.EXPECT().Register(gomock.Any(), gomock.Any()).AnyTimes()
	mockMsgBus.EXPECT().UnRegister(gomock.Any(), gomock.Any()).AnyTimes()
	mockMsgBus.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	return mockMsgBus
}

func newMockAccessControlProvider(ctrl *gomock.Controller) protocol.AccessControlProvider {
	// new MockMember
	mockMem := mock.NewMockMember(ctrl)
	mockMem.EXPECT().GetMemberId().Return("certId").AnyTimes()
	// new MockAccessControlProvider
	mockAc := mock.NewMockAccessControlProvider(ctrl)
	mockAc.EXPECT().NewMember(gomock.Any()).Return(mockMem, nil).AnyTimes()
	mockAc.EXPECT().LookUpExceptionalPolicy(gomock.Any()).Return(nil, nil).AnyTimes()
	mockAc.EXPECT().LookUpPolicy(gomock.Any()).Return(&pbac.Policy{
		Rule: string(protocol.RuleSelf),
	}, nil).AnyTimes()
	mockAc.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockAc.EXPECT().CreatePrincipalForTargetOrg(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(
		nil, nil).AnyTimes()
	mockAc.EXPECT().VerifyPrincipal(gomock.Any()).Return(true, nil).AnyTimes()
	return mockAc
}

func newMockSigningMember(ctrl *gomock.Controller) protocol.SigningMember {
	mockSigner := mock.NewMockSigningMember(ctrl)
	mockSigner.EXPECT().Sign(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockSigner.EXPECT().GetMember().Return(nil, nil).AnyTimes()

	return mockSigner
}

func newMockNetService(ctrl *gomock.Controller) protocol.NetService {
	mockNet := mock.NewMockNetService(ctrl)
	mockNet.EXPECT().GetNodeUidByCertId(gomock.Any()).Return(testNodeId, nil).AnyTimes()

	return mockNet
}

func initTxPoolConfig(configTxSize, commonTxSize, batchSize uint32, isDumpTxsInQueue bool, forwardMod string) {
	TxPoolConfig = &txPoolConfig{
		MaxConfigTxPoolSize: configTxSize,
		MaxTxPoolSize:       commonTxSize,
		BatchMaxSize:        batchSize,
		IsDumpTxsInQueue:    isDumpTxsInQueue,
		ForwardMod:          forwardMod,
	}
}

func generateTxs(num int, isConfig bool) []*commonPb.Transaction {
	txs := make([]*commonPb.Transaction, 0, num)
	txType := commonPb.TxType_INVOKE_CONTRACT
	for i := 0; i < num; i++ {
		contractName := syscontract.SystemContract_CHAIN_CONFIG.String()

		if !isConfig {
			contractName = testContract
		}
		txs = append(txs, &commonPb.Transaction{
			Payload: &commonPb.Payload{
				ChainId:      testChainId,
				TxId:         utils.GetRandTxId(),
				Timestamp:    time.Now().Unix(),
				TxType:       txType,
				Method:       "SetConfig",
				ContractName: contractName,
			},
			Endorsers: []*commonPb.EndorsementEntry{{
				Signer:    nil,
				Signature: []byte("sign"),
			}},
		},
		)
	}
	return txs
}

func generateTxIdsByTxs(txs []*commonPb.Transaction) []string {
	txIds := make([]string, 0, len(txs))
	for _, tx := range txs {
		txIds = append(txIds, tx.Payload.TxId)
	}
	return txIds
}

// generateMBatchesByTxs generate mBatches by sorted txs and nodeId is testNodeId
func generateMBatchesByTxs(batchNum, batchSize int, txs []*commonPb.Transaction) []*memTxBatch {
	if batchNum*batchSize != len(txs) {
		panic("batchNum * batchSize should be equal len(txs)")
	}
	mBatches := make([]*memTxBatch, 0, batchNum)
	for i := 0; i < batchNum; i++ {
		// create batch
		perTxs := txs[i*batchSize : (i+1)*batchSize]
		batch := &txpoolPb.TxBatch{
			Txs: perTxs,
		}
		// create batchId
		batchHash, _ := hash.GetByStrType("SHA256", mustMarshal(batch))
		batch.BatchId = generateTimestamp() + cutoutNodeId(testNodeId) + cutoutBatchHash(batchHash)
		// add Size_, TxIdsMap and Endorsement
		batch.Size_ = int32(len(perTxs))
		batch.TxIdsMap = createTxId2IndexMap(perTxs)
		batch.Endorsement = &commonPb.EndorsementEntry{Signer: nil, Signature: []byte("sign")}
		// create mBatch
		mBatches = append(mBatches, newMemTxBatch(batch, 0, createFilter(perTxs)))
	}
	return mBatches
}

// generateBatchesAndMBatches generate batches and mBatches, nodeId is random
func generateBatchesAndMBatches(batchNum int, isConfig bool, batchSize int) ([]*txpoolPb.TxBatch, []*memTxBatch) {
	batches := make([]*txpoolPb.TxBatch, 0, batchNum)
	mBatches := make([]*memTxBatch, 0, batchNum)
	for i := 0; i < batchNum; i++ {
		// create batch
		txs := sortTxs(generateTxs(batchSize, isConfig))
		batch := &txpoolPb.TxBatch{
			Txs: txs,
		}
		// create batchId
		batchHash, _ := hash.GetByStrType(crypto.CRYPTO_ALGO_SHA256, mustMarshal(batch))
		// nolint
		batch.BatchId = generateTimestamp() + cutoutNodeId(testNodeIdsSet[rand.Int()%len(testNodeIdsSet)]) + cutoutBatchHash(batchHash)
		// add Size_, TxIdsMap and Endorsement
		batch.Size_ = int32(len(txs))
		batch.TxIdsMap = createTxId2IndexMap(txs)
		batch.Endorsement = &commonPb.EndorsementEntry{Signer: nil, Signature: []byte("sign")}
		// create mBatch
		batches = append(batches, batch)
		mBatches = append(mBatches, newMemTxBatch(batch, 0, createFilter(txs)))
	}
	return batches, mBatches
}

func generateMBatches(batchNum int, isConfig bool, batchSize int) []*memTxBatch {
	mBatches := make([]*memTxBatch, 0, batchNum)
	for i := 0; i < batchNum; i++ {
		// create batch
		txs := sortTxs(generateTxs(batchSize, isConfig))
		batch := &txpoolPb.TxBatch{
			Txs: txs,
		}
		// calc batchId
		batchHash, _ := hash.GetByStrType(crypto.CRYPTO_ALGO_SHA256, mustMarshal(batch))
		// nolint
		batch.BatchId = generateTimestamp() + cutoutNodeId(testNodeIdsSet[rand.Int()%len(testNodeIdsSet)]) + cutoutBatchHash(batchHash)
		// add Size_, TxIdsMap and Endorsement
		batch.Size_ = int32(len(txs))
		batch.TxIdsMap = createTxId2IndexMap(txs)
		batch.Endorsement = &commonPb.EndorsementEntry{Signer: nil, Signature: []byte("sign")}
		// create mBatch
		mBatches = append(mBatches, newMemTxBatch(batch, 0, createFilter(txs)))
	}
	return mBatches
}

func generateBatchIdsByBatches(mBatches []*memTxBatch) []string {
	batchIds := make([]string, 0, len(mBatches))
	for _, mBatch := range mBatches {
		batchIds = append(batchIds, mBatch.getBatchId())
	}
	return batchIds
}
