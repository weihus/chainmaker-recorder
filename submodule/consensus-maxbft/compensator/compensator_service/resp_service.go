/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package compensator_service

import (
	"fmt"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/pb-go/v2/store"
	"chainmaker.org/chainmaker/protocol/v2"
)

// RespService implements a service for processing requests from other consensus nodes
type RespService struct {
	nodeId string
	ac     protocol.AccessControlProvider
	log    protocol.Logger
	net    protocol.NetService
	msgBus msgbus.MessageBus
	store  protocol.BlockchainStore
	forest forest.Forester
}

// NewRespService initials and returns a RespService object
func NewRespService(forest forest.Forester, nodeId string, msgBus msgbus.MessageBus,
	store protocol.BlockchainStore, log protocol.Logger, net protocol.NetService,
	ac protocol.AccessControlProvider) *RespService {
	return &RespService{
		forest: forest,
		nodeId: nodeId,
		msgBus: msgBus,
		store:  store,
		net:    net,
		log:    log,
		ac:     ac,
	}
}

// HandleRequest handle requests from other consensus nodes
func (s *RespService) HandleRequest(msg *maxbft.ProposalFetchMsg) {
	s.log.DebugDynamic(func() string {
		return fmt.Sprintf("handle proposal request: [%d:%d:%x] from %s",
			msg.Height, msg.View, msg.BlockId, string(msg.Requester))
	})
	var (
		err       error
		proposal  *maxbft.ProposalData
		qc        *maxbft.QuorumCert
		proposals = make([]*maxbft.ProposalData, 0)
		qcs       = make([]*maxbft.QuorumCert, 0)
	)
	if len(msg.BlockId) != 0 {
		proposal, qc, err = s.loadProposal(msg.BlockId, msg.Height)
	} else if msg.Height != 0 {
		proposals, qcs, err = s.loadProposalsByHeight(msg.Height)
	} else {
		err = fmt.Errorf("blockId and blockHeight cannot both be 0")
	}
	if err != nil {
		s.log.Warnf("load proposal[%d:%d:%x] failed, reason:: %s", msg.Height, msg.View, msg.BlockId, err)
		return
	}
	if proposal != nil {
		proposals = append(proposals, proposal)
		qcs = append(qcs, qc)
	}
	for i, p := range proposals {
		bz := utils.MustMarshal(&maxbft.ConsensusMsg{
			Type: maxbft.MessageType_PROPOSAL_RESP_MESSAGE,
			Payload: utils.MustMarshal(&maxbft.ProposalRespMsg{
				Proposal:  p,
				Qc:        qcs[i],
				Responser: []byte(s.nodeId), // todo. replace byte with string in the Responser
			}),
		})
		netMsg := &net.NetMsg{
			To:      string(msg.Requester),
			Type:    net.NetMsg_CONSENSUS_MSG,
			Payload: bz,
		}
		s.log.Debugf("send proposal to %s: [%d:%d:%x]",
			string(msg.Requester), p.Block.Header.BlockHeight, p.View, p.Block.Header.BlockHash)
		s.msgBus.Publish(msgbus.SendConsensusMsg, netMsg)
	}
}

func (s *RespService) loadProposal(blockID []byte, height uint64) (*maxbft.ProposalData, *maxbft.QuorumCert, error) {
	node := s.forest.Node(string(blockID))
	if node != nil && node.Data() != nil && node.Data().ProposalData().Proposer != "" {
		// 使用节点中缓存的信息
		return node.Data().ProposalData(), node.Data().BlockContainer().GetQC(), nil
	}
	s.log.Infof("load block[%x] from forest is nil", blockID)

	// 从存储中获取
	// 首先获取当前区块的信息
	blkWithSets, err := s.store.GetBlockWithRWSets(height)
	if err != nil {
		return nil, nil, err
	}
	if blkWithSets == nil {
		err = fmt.Errorf("block [height:%d] was not found in storage", height)
		return nil, nil, err
	}
	return s.constructHistoryProposal(blkWithSets)
}

func (s *RespService) loadProposalsByHeight(height uint64) ([]*maxbft.ProposalData, []*maxbft.QuorumCert, error) {
	node := s.forest.Root()
	var (
		err       error
		proposals []forest.ForestNoder
		proposal  *maxbft.ProposalData
	)
	proposals, err = s.searchProposalByHeight(height, node)
	if err != nil {
		s.log.Warnf("search proposal by height from forest failed. error: %+v", err)
	}
	ps := make([]*maxbft.ProposalData, 0, len(proposals))
	qcs := make([]*maxbft.QuorumCert, 0, len(proposals))
	for _, p := range proposals {
		if p != node {
			proposal = p.Data().ProposalData()
			// send proposal with txRwSet if there is a qc for the proposal,
			// otherwise we will send the proposal without txRwSet to avoid
			// that Vm result by a Byzantine node covered the Vm result by
			// an honest node.
			qc := p.Data().BlockContainer().GetQC()
			if qc != nil {
				ps = append(ps, proposal)
				qcs = append(qcs, qc)
			} else {
				tmpProposal := &maxbft.ProposalData{
					Block:     proposal.Block,
					View:      proposal.View,
					Proposer:  proposal.Proposer,
					JustifyQc: proposal.JustifyQc,
					EpochId:   proposal.EpochId,
				}
				ps = append(ps, tmpProposal)
				qcs = append(qcs, nil)
			}
		}
	}
	if len(ps) != 0 {
		return ps, qcs, nil
	}

	s.log.Infof("load block[%d] from forest by height is nil", height)

	var qc *maxbft.QuorumCert
	proposal, qc, err = s.loadProposalFromStore(height)
	if err != nil {
		s.log.Warnf("load proposal from storage failed. error:%+v", err)
		return ps, qcs, nil
	}
	ps = append(ps, proposal)
	qcs = append(qcs, qc)
	return ps, qcs, nil
}

func (s *RespService) loadProposalFromStore(height uint64) (*maxbft.ProposalData, *maxbft.QuorumCert, error) {
	// 从存储中获取
	// 首先获取当前区块的信息
	var (
		err            error
		blockWithRwSet *store.BlockWithRWSet
		p              *maxbft.ProposalData
		qc             *maxbft.QuorumCert
	)
	blockWithRwSet, err = s.store.GetBlockWithRWSets(height)
	if err != nil {
		return nil, nil, err
	}
	if blockWithRwSet == nil {
		err = fmt.Errorf("block [height:%d] was not found in storage", height)
		return nil, nil, err
	}
	p, qc, err = s.constructHistoryProposal(blockWithRwSet)
	if err != nil {
		s.log.Warnf("construct history proposal failed. error:%+v", err)
		return nil, nil, err
	}
	return p, qc, nil
}

func (s *RespService) constructHistoryProposal(blkWithSets *store.BlockWithRWSet) (
	*maxbft.ProposalData, *maxbft.QuorumCert, error) {
	if blkWithSets.Block == nil {
		return nil, nil, fmt.Errorf("block is nil")
	}
	preBlockHash := blkWithSets.Block.Header.PreBlockHash
	// 查询其父区块
	preBlock, err := s.store.GetBlockByHash(preBlockHash)
	if err != nil || preBlock == nil {
		return nil, nil, fmt.Errorf("load parent block[%x] from db error : %v", preBlockHash, err)
	}
	// 拼装一下proposal
	if preBlock.AdditionalData != nil && len(preBlock.AdditionalData.ExtraData) > 0 ||
		preBlock.Header.BlockHeight == 0 {
		preQc := utils.GetQCFromBlock(preBlock)
		if preQc == nil {
			return nil, nil, fmt.Errorf("parent block[%x] qc is nil", preBlockHash)
		}

		// 获取当前区块的QC，为了拿到当前区块的世代标识
		qc := utils.GetQCFromBlock(blkWithSets.Block)
		if qc == nil {
			return nil, nil, fmt.Errorf("block[%x] qc is nil", blkWithSets.Block.Header.BlockHash)
		}

		nodeId, err := utils.GetNodeIdFromSigner(blkWithSets.Block.Header.Proposer, s.ac, s.net)
		if err != nil {
			return nil, nil, err
		}
		proposal := &maxbft.ProposalData{
			Block:     blkWithSets.Block,
			View:      utils.GetBlockView(blkWithSets.Block),
			Proposer:  nodeId,
			JustifyQc: preQc,
			EpochId:   qc.Votes[0].EpochId,
			TxRwSet:   blkWithSets.TxRWSets,
		}
		return proposal, qc, nil
	}
	return nil, nil, fmt.Errorf("parent block[%x] additional data is nil", preBlockHash)
}

// searchProposalByHeight have to find all proposals at the specified height
func (s *RespService) searchProposalByHeight(height uint64, root forest.ForestNoder) ([]forest.ForestNoder, error) {
	nodes := make([]forest.ForestNoder, 0)
	if root.Height() == height {
		nodes = append(nodes, root)
		return nodes, nil
	}
	if root.Height() > height {
		return []forest.ForestNoder{}, fmt.Errorf("proposal not found. height: %d", height)
	}
	for _, node := range root.Children() {
		results, err := s.searchProposalByHeight(height, node)
		if err != nil {
			return nil, err
		}
		if len(results) != 0 {
			nodes = append(nodes, results...)
		}
	}
	return nodes, nil
}
