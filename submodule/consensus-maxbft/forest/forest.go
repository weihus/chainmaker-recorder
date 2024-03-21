/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

import (
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
)

// Forester defines a nodes forest to maintain proposals that have been preliminary processed
type Forester interface {
	// Stop exit goroutine inside the component
	Stop()
	Start()

	// RegisterFinalEventFunc 注册final事件函数
	RegisterFinalEventFunc(fn FinalEventFunc)

	// RegisterAddedEventFunc 注册添加的事件函数
	RegisterAddedEventFunc(fn AddedEventFunc)

	// GetGenericQC 返回GenericQC
	GetGenericQC() *maxbft.QuorumCert

	// GetGenericHeight 返回当前区块高度，正常情况下当前区块高度等于GenericQC的高度+1
	// 在以下两种情况下，当前区块高度与GenericQC的高度一致：
	// 1）初始化的时候
	// 2）随GenericQC一起收到的proposal没有被验证通过的时候
	GetGenericHeight() uint64

	// GetLockedQC 返回LockedQC
	GetLockedQC() *BlockContainer

	// GetFinalQC 返回FinalQC
	GetFinalQC() *BlockContainer

	// FinalView 返回Final视图ID
	FinalView() uint64

	// HasProposal 判断blockID对应提案是否存在
	HasProposal(blockID string) bool

	// GetProposalByView 根据视图获取提案
	GetProposalByView(view uint64) *maxbft.ProposalData

	// GetLatest3Ancestors Gets the last three uncommitted ancestor blocks of the node
	// The number of ancestors may be less than 3 or there are no uncommitted ancestor
	// blocks for current proposal
	GetLatest3Ancestors(proposal *maxbft.ProposalData) []ForestNoder

	// Node 返回节点
	Node(key string) ForestNoder

	// Root 返回根节点
	Root() ForestNoder

	// UpdateStatesByProposal 仅使用提案携带的QC更新共识状态，并不将提案添加到forest中，
	// 更新genericQC、lockedQC、 提交符合three-chain的区块，更新finalQC，返回被提交的区块数据
	// 若ProposalContainer数组为空表示没有提交任何区块，不为空表示提交了部分区块。
	// 或当提案的父块不存在时，返回错误
	UpdateStatesByProposal(proposal *maxbft.ProposalData, validBlock bool) ([]*ProposalContainer, error)

	// AddProposal 添加提案,
	// replay true表示提案加载自wal，false表示提案从共识流程中获得
	AddProposal(proposal *maxbft.ProposalData, replay bool) error

	// UpdateStatesByQC 将QC添加至它对应的区块中，更新genericQC
	UpdateStatesByQC(qc *maxbft.QuorumCert) error

	//// CommitProposal 提交提案
	//CommitProposal(proposal *maxbft.ProposalData)
}
