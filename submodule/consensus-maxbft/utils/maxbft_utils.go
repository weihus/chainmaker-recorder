/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"fmt"
	"sort"
	"strings"

	"chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"

	//systemPb "chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/gogo/protobuf/proto"
)

const (
	// MaxbftQC defines the key in block.AdditionalData.ExtraData
	MaxbftQC = "maxbftQC"
)

// MustMarshal marshals protobuf message to byte slice or panic when marshal twice both failed
func MustMarshal(msg proto.Message) (data []byte) {
	var err error
	defer func() {
		// while first marshal failed, retry marshal again
		if recover() != nil {
			data, err = proto.Marshal(msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	data, err = proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return
}

// GetBlockView returns the view of the specified block
func GetBlockView(block *common.Block) uint64 {
	if block.Header.BlockHeight == 0 {
		return 0
	}
	args := &consensus.BlockHeaderConsensusArgs{}
	if err := proto.Unmarshal(block.Header.ConsensusArgs, args); err != nil {
		panic(fmt.Sprintf("unmarshal BlockHeaderConsensusArgs "+
			"from block.Header failed, reason: %s", err))
	}
	return args.View
}

// AddArgsToBlock adds txRwSet and view to the specified block
func AddArgsToBlock(view uint64, block *common.Block, txRwSet *common.TxRWSet) {
	args := &consensus.BlockHeaderConsensusArgs{
		View:          view,
		ConsensusType: int64(consensus.ConsensusType_MAXBFT),
		ConsensusData: txRwSet,
	}

	bz, err := proto.Marshal(args)
	if err != nil {
		panic(fmt.Sprintf("marshal BlockHeaderConsensusArgs failed, reason: %s", err))
	}
	block.Header.ConsensusArgs = bz
}

// GetQCFromBlock return the qc in the specified block
func GetQCFromBlock(block *common.Block) *maxbft.QuorumCert {
	// genesis block
	if block.Header.BlockHeight == 0 {
		return initGenesisBlock(block)
	}

	// other height block
	qc := &maxbft.QuorumCert{}
	if _, ok := block.AdditionalData.ExtraData[MaxbftQC]; !ok {
		panic("not qc content in block")
	}
	if err := proto.Unmarshal(block.AdditionalData.ExtraData[MaxbftQC], qc); err != nil {
		panic(fmt.Sprintf("unmarshal qc failed, reason: %s", err))
	}
	return qc
}

func initGenesisBlock(block *common.Block) *maxbft.QuorumCert {
	qcForGenesis := &maxbft.QuorumCert{
		Votes: []*maxbft.VoteData{
			{
				Height:  0,
				View:    0,
				BlockId: block.Header.BlockHash,
			},
		},
	}
	InsertQCToBlock(block, qcForGenesis)
	AddArgsToBlock(0, block, nil)
	return qcForGenesis
}

// InsertQCToBlock inserts the specified qc into the specified block
func InsertQCToBlock(block *common.Block, qc *maxbft.QuorumCert) {
	bz, err := proto.Marshal(qc)
	if err != nil {
		panic(fmt.Sprintf("marshal QuorumCert msg failed, reason: %s", err))
	}
	if block.AdditionalData == nil {
		block.AdditionalData = &common.AdditionalData{
			ExtraData: make(map[string][]byte),
		}
	}
	if block.AdditionalData.ExtraData == nil {
		block.AdditionalData.ExtraData = make(map[string][]byte)
	}
	block.AdditionalData.ExtraData[MaxbftQC] = bz
}

// GetQCView returns the view of the specified proposal
func GetQCView(proposal *maxbft.ProposalData) uint64 {
	return proposal.JustifyQc.Votes[0].View
}

// GetViewFromQC returns the view of the specified qc
func GetViewFromQC(qc *maxbft.QuorumCert) uint64 {
	return qc.Votes[0].View
}

// GetQCHeight returns the qc height in the specified proposal
func GetQCHeight(proposal *maxbft.ProposalData) uint64 {
	return proposal.JustifyQc.Votes[0].Height
}

// GetHeightFromQC returns the block height in the specified quorum certification
func GetHeightFromQC(qc *maxbft.QuorumCert) uint64 {
	return qc.Votes[0].Height
}

// GetBlockIdFromQC returns the block hash in the specified quorum certification
func GetBlockIdFromQC(qc *maxbft.QuorumCert) []byte {
	return qc.Votes[0].BlockId
}

// GetConsensusNodes returns consensus nodes in the chain configuration
func GetConsensusNodes(conf *config.ChainConfig) []string {
	nodes := make([]string, 0, 10)
	for _, org := range conf.Consensus.Nodes {
		nodes = append(nodes, org.NodeId...)
	}
	sort.SliceStable(nodes, func(i, j int) bool {
		return strings.Compare(nodes[i], nodes[j]) < 0
	})
	return nodes
}

// GetQuorum Counts the number of votes needed to reach a consensus 2f+1
func GetQuorum(consensusNodesNum int) uint {
	return uint(consensusNodesNum)*2/3 + 1
}

// GetMiniNodes Calculates the number of nodes for f+1, means that there
// is at least one honest node in the set.
func GetMiniNodes(consensusNodesNum int) int {
	return consensusNodesNum/3 + 1
}

// GetNodeIdFromSigner returns the node to which the specified signer is mapped in Ac module
func GetNodeIdFromSigner(signer *accesscontrol.Member,
	ac protocol.AccessControlProvider, net protocol.NetService) (nodeId string, err error) {
	proposer, err := ac.NewMember(signer)
	if err != nil {
		return "", err
	}
	nodeId, err = net.GetNodeUidByCertId(proposer.GetMemberId())
	return
}
