/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"fmt"
	"strconv"
	"strings"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	systemPb "chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/gogo/protobuf/proto"
)

const (
	// DefaultViewNumsPerEpoch defines the default views number per epoch
	DefaultViewNumsPerEpoch uint64 = 100

	// MinimumViewNumsPerEpoch defines the minimum views number per epoch
	MinimumViewNumsPerEpoch uint64 = 10

	// ViewNumsPerEpoch defines the key of the minimum views number per epoch in chainConfig
	ViewNumsPerEpoch string = "MaxBftViewNumsPerEpoch"
)

// GetGovernanceContractTxRWSet get the transaction rwSet from the given governance contract object
func GetGovernanceContractTxRWSet(governanceContract *maxbft.GovernanceContract) (*common.TxRWSet, error) {
	txRWSet := &common.TxRWSet{
		TxId:     systemPb.SystemContract_GOVERNANCE.String(),
		TxReads:  make([]*common.TxRead, 0),
		TxWrites: make([]*common.TxWrite, 1),
	}

	var (
		err          error
		pbccPayload  []byte
		contractName = systemPb.SystemContract_GOVERNANCE.String()
	)
	//1. check for changes
	if pbccPayload, err = proto.Marshal(governanceContract); err != nil {
		return nil, fmt.Errorf("proto marshal pbcc failed, %s", err.Error())
	}

	// 2. create txRWSet for the new government contract
	txRWSet.TxWrites[0] = &common.TxWrite{
		Key:          []byte(contractName),
		Value:        pbccPayload,
		ContractName: contractName,
	}
	return txRWSet, nil
}

// GetChainConfigFromChainStore get the current chain configurations from the chain storage
func GetChainConfigFromChainStore(store protocol.BlockchainStore) (*config.ChainConfig, error) {
	contractName := systemPb.SystemContract_CHAIN_CONFIG.String()
	bz, err := store.ReadObject(contractName, []byte(contractName))
	if err != nil {
		return nil, err
	}
	var chainConfig config.ChainConfig
	if err = proto.Unmarshal(bz, &chainConfig); err != nil {
		return nil, err
	}
	return &chainConfig, nil
}

// GetGovernanceContractFromChainStore get the current governance contract information from the chain storage
func GetGovernanceContractFromChainStore(store protocol.BlockchainStore) (*maxbft.GovernanceContract, error) {
	contractName := systemPb.SystemContract_GOVERNANCE.String()
	bz, err := store.ReadObject(contractName, []byte(contractName))
	if err != nil {
		return nil, fmt.Errorf("get contractName=%s from db failed, reason: %s", contractName, err)
	}
	if len(bz) == 0 {
		return nil, nil
	}
	governanceContract := &maxbft.GovernanceContract{}
	if err = proto.Unmarshal(bz, governanceContract); err != nil {
		return nil, fmt.Errorf("unmarshal contractName=%s failed, reason: %s", contractName, err)
	}
	return governanceContract, nil
}

// ConstructContractTxRwSet construct rwSet for governance contract transaction
// by the current chain configurations from the chain storage
func ConstructContractTxRwSet(
	view, epochEndView, epochId uint64, config *config.ChainConfig,
	store protocol.BlockchainStore, log protocol.Logger) (*common.TxRWSet, error) {

	var (
		err    error
		create bool
	)
	// epochId == 0 && store != nil; 生成第一个区块后，就进行一次世代切换，用于兼容当前通过
	if epochId == 0 && store != nil {
		config, err = GetChainConfigFromChainStore(store)
		if err != nil {
			return nil, err
		}
		create = true
	}
	// 正常情况下触发世代切换的时机
	if view >= epochEndView {
		create = true
	}
	if !create {
		return nil, nil
	}

	// 创建新世代配置
	var (
		endView          uint64
		viewNumsPerEpoch = GetViewsPerEpochFromChainConf(config)
		nodeIds          = GetConsensusNodes(config)
	)
	// if the end view of the next epoch is passed, then extend the current epoch,
	// avoid switching epoch every block that has been committed
	endView = epochEndView + viewNumsPerEpoch
	if view >= endView {
		endView = (view + 2*viewNumsPerEpoch) * viewNumsPerEpoch / viewNumsPerEpoch
	}

	originAuthType := config.AuthType
	defer func() {
		config.AuthType = originAuthType
	}()
	config.AuthType = strings.ToLower(config.AuthType)
	contract := &maxbft.GovernanceContract{
		EpochId:        epochId + 1,
		EndView:        endView,
		Validators:     nodeIds,
		ConfigSequence: config.Sequence,
		ChainConfig:    config,
	}
	log.Debugf("new governance contract: epochId %d, endView %d validators %+v,"+
		" configSequence %d", contract.EpochId, contract.EndView, contract.Validators, contract.ConfigSequence)
	txRwSet, err := GetGovernanceContractTxRWSet(contract)
	return txRwSet, err
}

// GetViewsPerEpochFromChainConf get viewsPerEpoch configuration from the given chain configurations
func GetViewsPerEpochFromChainConf(config *config.ChainConfig) uint64 {
	conConf := config.Consensus.ExtConfig
	for _, oneConf := range conConf {
		switch oneConf.Key {
		case ViewNumsPerEpoch:
			if v, err := strconv.ParseUint(oneConf.Value, 10, 64); err == nil {
				if v < MinimumViewNumsPerEpoch {
					return MinimumViewNumsPerEpoch
				}
				return v
			}
		}
	}
	return DefaultViewNumsPerEpoch
}
