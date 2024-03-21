/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

import (
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
)

// NewProposalData constructs and returns a new mock proposal
func NewProposalData(parentBlkView, currBlkView, blkHeight uint64, blkHash, preBlkHash string) *maxbft.ProposalData {
	proposal := &maxbft.ProposalData{
		Block: &common.Block{
			Header: &common.BlockHeader{
				ChainId:      "chain1",
				BlockHeight:  blkHeight,
				BlockHash:    []byte(blkHash),
				PreBlockHash: []byte(preBlkHash),
			},
		},
		View:     currBlkView,
		Proposer: "leader",
		JustifyQc: &maxbft.QuorumCert{
			Votes: []*maxbft.VoteData{
				{
					BlockId: []byte(preBlkHash),
					Height:  blkHeight - 1,
					View:    parentBlkView,
				},
				{
					BlockId: []byte(preBlkHash),
					Height:  blkHeight - 1,
					View:    parentBlkView,
				},
				{
					BlockId: []byte(preBlkHash),
					Height:  blkHeight - 1,
					View:    parentBlkView,
				},
			},
		},
	}
	return proposal
}

// GenesisProposalContainer genesis a mock proposal container
func GenesisProposalContainer() *ProposalContainer {
	proposal := &maxbft.ProposalData{
		Block: &common.Block{
			Header: &common.BlockHeader{
				ChainId:     "chain1",
				BlockHeight: 0,
				BlockHash:   []byte("0"),
			},
		},
		View:     0,
		Proposer: "leader",
		JustifyQc: &maxbft.QuorumCert{
			Votes: []*maxbft.VoteData{
				{
					BlockId: []byte("0"),
					Height:  0,
					View:    0,
				},
				{
					BlockId: []byte("0"),
					Height:  0,
					View:    0,
				},
				{
					BlockId: []byte("0"),
					Height:  0,
					View:    0,
				},
			},
		},
	}
	return NewProposalContainer(proposal, true, false)
}
