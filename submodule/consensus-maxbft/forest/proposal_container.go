/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

import (
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
)

// ProposalContainer used to assemble a proposal and its corresponding BlockContainer
type ProposalContainer struct {
	fromOtherNodes bool                 // 该提案来自于从其它节点接收到的resp
	proposal       *maxbft.ProposalData // 接收到的proposal数据，该值不会被修改
	block          *BlockContainer      // 根据proposal生成的block，block中的QC为该区块自身的，共识流程中实际包含在下一个proposal中
}

// NewProposalContainer 创建ProposalContainer
// useQC表示是否使用该proposal中的QC，通常是不使用的，仅仅当该proposal已经在账本中，并且是第一次加载时使用
func NewProposalContainer(proposal *maxbft.ProposalData, useQC bool, fromOtherNodes bool) *ProposalContainer {
	container := &ProposalContainer{
		proposal:       proposal,
		block:          NewBlockContainer(proposal.Block),
		fromOtherNodes: fromOtherNodes,
	}
	if useQC {
		container.block.SetQC(proposal.JustifyQc)
	}
	return container
}

// ProposalData returns the proposal of the container
func (p *ProposalContainer) ProposalData() *maxbft.ProposalData {
	return p.proposal
}

// BlockContainer returns the block container of the proposal container
func (p *ProposalContainer) BlockContainer() *BlockContainer {
	return p.block
}

// Key returns the block hash string
func (p *ProposalContainer) Key() string {
	return string(p.proposal.Block.Header.BlockHash)
}

// ParentKey returns the pre block hash string
func (p *ProposalContainer) ParentKey() string {
	return string(p.proposal.Block.Header.PreBlockHash)
}

// Height returns the block height of the container
func (p *ProposalContainer) Height() uint64 {
	return p.proposal.Block.Header.BlockHeight
}

// View returns the view of the proposal in the container
func (p *ProposalContainer) View() uint64 {
	return p.proposal.View
}

// UpdateChildProposal updates qc by a child proposal's justify qc
func (p *ProposalContainer) UpdateChildProposal(childProposal *ProposalContainer) {
	p.UpdateQC(childProposal.proposal.JustifyQc)
}

// UpdateQC updates qc in the container
func (p *ProposalContainer) UpdateQC(qc *maxbft.QuorumCert) {
	// 判断当前block是否有QC，没有的话进行设置，存在则不处理
	if p.block.GetQC() == nil {
		p.block.SetQC(qc)
	}
}

// Proposal returns the proposal of the container
func (p *ProposalContainer) Proposal() *maxbft.ProposalData {
	return p.proposal
}

// GetProposalQC returns the justify qc of the proposal in the container
func (p *ProposalContainer) GetProposalQC() *maxbft.QuorumCert {
	return p.proposal.JustifyQc
}

// GetProposalQCView returns the qc view of the container
func (p *ProposalContainer) GetProposalQCView() uint64 {
	return utils.GetQCView(p.proposal)
}

// GetProposalQCHeight returns the qc height of the container
func (p *ProposalContainer) GetProposalQCHeight() uint64 {
	return utils.GetQCHeight(p.proposal)
}
