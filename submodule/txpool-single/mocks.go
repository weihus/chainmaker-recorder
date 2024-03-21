/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"chainmaker.org/chainmaker/common/v2/birdsnest"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	msgbusmock "chainmaker.org/chainmaker/common/v2/msgbus/mock"
	pbac "chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	configPb "chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/pb-go/v2/txfilter"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"chainmaker.org/chainmaker/protocol/v2/test"
	"chainmaker.org/chainmaker/utils/v2"
	"github.com/golang/mock/gomock"
)

const (
	testContract = "userContract1"
	testChainId  = "chain1"
	testNodeId   = "QmV7N4W1itYc7tykjnJtyzJ1ANR7XsrY3VmKbEybNODEA1" // node1
)

type mockTxFilter struct {
	txs      map[string]*commonPb.Transaction
	txFilter protocol.TxFilter
}

func newMockTxFilter(ctrl *gomock.Controller, txsMap map[string]*commonPb.Transaction) *mockTxFilter {
	filter := mock.NewMockTxFilter(ctrl)
	mockFilter := &mockTxFilter{txFilter: filter, txs: txsMap}

	filter.EXPECT().IsExistsAndReturnHeight(gomock.Any(), gomock.Any()).DoAndReturn(
		func(txId string, ruleType ...birdsnest.RuleType) (bool, uint64, *txfilter.Stat, error) {
			_, exist := mockFilter.txs[txId]
			return exist, 0, nil, nil
		}).AnyTimes()
	return mockFilter
}

type mockBlockChainStore struct {
	txs   map[string]*commonPb.Transaction
	store protocol.BlockchainStore
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
		return exist, 0, nil
	}).AnyTimes()

	store.EXPECT().TxExistsInIncrementDB(gomock.Any(), gomock.Any()).DoAndReturn(
		func(txId string, startHeight uint64) (bool, error) {
			_, exist := mockStore.txs[txId]
			return exist, nil
		}).AnyTimes()

	return mockStore
}

func newMockLogger() protocol.Logger {
	return &test.GoLogger{}
}

func newMockChainConf(ctrl *gomock.Controller, verifyTime bool) protocol.ChainConf {
	mockCC := mock.NewMockChainConf(ctrl)
	mockCC.EXPECT().ChainConfig().AnyTimes().DoAndReturn(func() *configPb.ChainConfig {
		return &configPb.ChainConfig{
			Block: &configPb.BlockConfig{
				TxTimestampVerify: verifyTime,
				TxTimeout:         1,
				BlockTxCapacity:   defaultMaxTxCount,
			},
			Contract: &configPb.ContractConfig{},
			Core: &configPb.CoreConfig{
				ConsensusTurboConfig: &configPb.ConsensusTurboConfig{
					ConsensusMessageTurbo: true,
				},
			},
		}
	})

	return mockCC
}

func newMockMessageBus(ctrl *gomock.Controller) msgbus.MessageBus {
	mockMsgBus := msgbusmock.NewMockMessageBus(ctrl)
	mockMsgBus.EXPECT().Register(gomock.Any(), gomock.Any()).AnyTimes()
	mockMsgBus.EXPECT().UnRegister(gomock.Any(), gomock.Any()).AnyTimes()
	mockMsgBus.EXPECT().Publish(gomock.Any(), gomock.Any()).AnyTimes()
	return mockMsgBus
}

func newMockAccessControlProvider(ctrl *gomock.Controller) protocol.AccessControlProvider {
	mockAc := mock.NewMockAccessControlProvider(ctrl)
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

func generateMemTxs(num int, isConfig bool) []*memTx {
	mtxs := make([]*memTx, 0, num)
	txType := commonPb.TxType_INVOKE_CONTRACT
	for i := 0; i < num; i++ {
		contractName := syscontract.SystemContract_CHAIN_CONFIG.String()

		if !isConfig {
			contractName = testContract
		}
		mtx := &memTx{
			tx: &commonPb.Transaction{
				Payload: &commonPb.Payload{
					ChainId:      testChainId,
					TxId:         utils.GetRandTxId(),
					TxType:       txType,
					Method:       "SetConfig",
					ContractName: contractName,
				},
				Endorsers: []*commonPb.EndorsementEntry{{
					Signer:    nil,
					Signature: []byte("sign"),
				}},
			},
			dbHeight: 0,
		}
		mtxs = append(mtxs, mtx)
	}
	return mtxs
}

func getTxsByMemTxs(mtxs []*memTx) []*commonPb.Transaction {
	txs := make([]*commonPb.Transaction, 0, len(mtxs))
	for _, mtx := range mtxs {
		txs = append(txs, mtx.getTx())
	}
	return txs
}

func getTxIdsByTxs(txs []*commonPb.Transaction) []string {
	txIds := make([]string, 0, len(txs))
	for _, tx := range txs {
		txIds = append(txIds, tx.Payload.TxId)
	}
	return txIds
}

func getTxIdsByMemTxs(mtxs []*memTx) []string {
	txIds := make([]string, 0, len(mtxs))
	for _, mtx := range mtxs {
		txIds = append(txIds, mtx.getTxId())
	}
	return txIds
}

func generateTxIdsToMap(txIds []string) (txIdsMap map[string]struct{}) {
	txIdsMap = make(map[string]struct{}, len(txIds))
	for _, txId := range txIds {
		txIdsMap[txId] = struct{}{}
	}
	return txIdsMap
}
