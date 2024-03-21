/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verifier

import (
	"fmt"
	"testing"

	"chainmaker.org/chainmaker/pb-go/v2/consensus"

	"chainmaker.org/chainmaker/pb-go/v2/config"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	forest_mock "chainmaker.org/chainmaker/consensus-maxbft/v2/forest/mock"
	"chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"chainmaker.org/chainmaker/protocol/v2/test"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	ChainId = "test_chain"
	BlockId = "0"
	View    = uint64(0)
)

var Validators = []string{
	"test_validator_1",
	"test_validator_2",
	"test_validator_3",
	"test_validator_4",
}

func TestValidator_ValidateVote(t *testing.T) {
	ctrl := gomock.NewController(t)

	vote := &maxbftpb.VoteData{
		View:    View,
		Height:  View,
		BlockId: []byte(BlockId),
		Author:  []byte(Validators[0]),
	}

	proposal := &maxbftpb.ProposalData{
		Block: &common.Block{
			Header: &common.BlockHeader{
				BlockHeight: View,
			},
		},
		View:     View,
		Proposer: Validators[3],
	}

	ac, acErr, signEntries := getMockAc(ctrl, []*maxbftpb.VoteData{vote})

	net := mock.NewMockNetService(ctrl)
	for i := 0; i < 4; i++ {
		net.EXPECT().GetNodeUidByCertId(Validators[i]).AnyTimes().Return(Validators[i], nil)
	}

	cache := mock.NewMockLedgerCache(ctrl)
	cache.EXPECT().CurrentHeight().AnyTimes().Return(uint64(0), nil)

	v := NewValidator(Validators, nil, ac, net, Validators[1],
		nil, nil, cache, 0, 100, test.NewTestLogger(t), nil).(*Validator)

	// validate vote success
	vote.Signature = signEntries[0]
	err := v.ValidateVote(vote, proposal)
	require.Empty(t, err)

	// wrong view
	vote.View = View + 1
	err = v.ValidateVote(vote, proposal)
	require.NotEmpty(t, err)

	// wrong signature in vote
	vote.View = View
	v.ac = acErr
	err = v.ValidateVote(vote, proposal)
	require.NotEmpty(t, err)
}

func TestValidator_ValidateProposal(t *testing.T) {
	testValidateProposal(t, View)
	testValidateProposal(t, View+1)
}

func testValidateProposal(t *testing.T, view uint64) {
	ctrl := gomock.NewController(t)
	log := test.NewTestLogger(t)

	// prepare votes
	votes := make([]*maxbftpb.VoteData, 3)
	for i := 0; i < 3; i++ {
		votes[i] = &maxbftpb.VoteData{
			View:    view,
			Height:  view,
			BlockId: []byte(BlockId),
			Author:  []byte(Validators[i]),
		}
	}

	// mock ac and signer
	ac, acErr, signEntries := getMockAc(ctrl, votes)
	args := &consensus.BlockHeaderConsensusArgs{
		ConsensusType: int64(consensus.ConsensusType_MAXBFT),
		View:          view + 1,
		ConsensusData: nil,
	}

	data, err := proto.Marshal(args)
	require.NoError(t, err)
	block := &common.Block{
		Header: &common.BlockHeader{
			BlockHeight:   view + 1,
			PreBlockHash:  []byte(BlockId),
			Proposer:      signEntries[view+1].Signer,
			ConsensusArgs: data,
		},
		Txs: []*common.Transaction{
			{
				Payload: &common.Payload{},
			},
		},
	}
	// prepare proposal
	proposal := &maxbftpb.ProposalData{
		Block:    block,
		View:     view + 1,
		Proposer: Validators[view+1],
		JustifyQc: &maxbftpb.QuorumCert{
			Votes: votes,
		},
		TxRwSet: make([]*common.TxRWSet, 0),
	}

	// mock verifier
	blockVerifier := mock.NewMockBlockVerifier(ctrl)
	blockVerifier.EXPECT().VerifyBlock(block, protocol.CONSENSUS_VERIFY).AnyTimes().Return(nil)
	blockVerifier.EXPECT().VerifyBlockSync(block, protocol.CONSENSUS_VERIFY).AnyTimes().Return(nil, nil)
	blockVerifier.EXPECT().VerifyBlockWithRwSets(block, proposal.TxRwSet,
		protocol.CONSENSUS_VERIFY).AnyTimes().Return(nil)

	blockVerifierErr := mock.NewMockBlockVerifier(ctrl)
	blockVerifierErr.EXPECT().VerifyBlock(block, protocol.CONSENSUS_VERIFY).AnyTimes().Return(
		fmt.Errorf("boom"))
	blockVerifierErr.EXPECT().VerifyBlockSync(block, protocol.CONSENSUS_VERIFY).AnyTimes().Return(nil,
		fmt.Errorf("boom"))
	blockVerifierErr.EXPECT().VerifyBlockWithRwSets(block, proposal.TxRwSet,
		protocol.CONSENSUS_VERIFY).AnyTimes().Return(
		fmt.Errorf("boom"))
	conf := mock.NewMockChainConf(ctrl)
	conf.EXPECT().ChainConfig().AnyTimes().Return(
		&config.ChainConfig{Consensus: &config.ConsensusConfig{
			Nodes: []*config.OrgConfig{
				{NodeId: Validators},
			},
		}})

	// mock forest
	blockCache := forest_mock.NewBlockCacheMock()
	forest := forest.NewForest(forest.NewCachedForestNode(forest.GenesisProposalContainer()),
		blockCache, log)

	// mock the netService
	net := mock.NewMockNetService(ctrl)
	for i := 0; i < 4; i++ {
		net.EXPECT().GetNodeUidByCertId(Validators[i]).AnyTimes().Return(Validators[i], nil)
	}

	cache := mock.NewMockLedgerCache(ctrl)
	cache.EXPECT().CurrentHeight().AnyTimes().Return(uint64(0), nil)

	v := NewValidator(Validators, blockVerifier, ac, net, Validators[2],
		forest, nil, cache, 0, 100, log, conf).(*Validator)

	for i, vote := range proposal.JustifyQc.Votes {
		vote.Signature = signEntries[i]
	}

	// validate proposal success
	_, err = v.ValidateProposal(proposal, nil, protocol.CONSENSUS_VERIFY)
	require.Empty(t, err)

	// base check success
	err = v.BaseCheck(proposal, nil)
	require.Empty(t, err)

	// verify block error
	v.verifier = blockVerifierErr
	_, err = v.ValidateProposal(proposal, nil, protocol.CONSENSUS_VERIFY)
	require.Error(t, err)

	// wrong signature in votes, but genesis block's sub-blocks not need to verify
	v.verifier = blockVerifier
	v.ac = acErr
	_, err = v.ValidateProposal(proposal, nil, protocol.CONSENSUS_VERIFY)
	if view == 0 {
		require.Empty(t, err)
	} else {
		// because in curr version, will not verify qc in the func ValidateProposal
		require.NoError(t, err)
	}

	// wrong proposer for the view in the proposal
	proposal.Proposer = Validators[3]
	v.ac = ac
	err = v.BaseCheck(proposal, nil)
	require.NotEmpty(t, err)

	// wrong proposal pre hash, but here only verify the block info.
	// the qc info has verified by another func
	proposal.Proposer = Validators[view+1]
	proposal.Block.Header.PreBlockHash = []byte("test_block_1")
	_, err = v.ValidateProposal(proposal, nil, protocol.CONSENSUS_VERIFY)
	require.NoError(t, err)

	// not safe node, but here not verify safeNode rules, which is
	// verified by ValidateQC.
	proposal.Block.Header.PreBlockHash = []byte(BlockId)
	for i := 0; i < 3; i++ {
		proposal.JustifyQc.Votes[i].BlockId = []byte("test_block_1")
	}
	_, err = v.ValidateProposal(proposal, nil, protocol.CONSENSUS_VERIFY)
	require.NoError(t, err)
}

func getMockAc(ctrl *gomock.Controller, votes []*maxbftpb.VoteData) (
	*mock.MockAccessControlProvider, *mock.MockAccessControlProvider, []*common.EndorsementEntry) {
	signEntries := make([]*common.EndorsementEntry, 4)
	ac := mock.NewMockAccessControlProvider(ctrl)
	acErr := mock.NewMockAccessControlProvider(ctrl)
	for i := 0; i < 4; i++ {
		signEntries[i] = &common.EndorsementEntry{
			Signer: &accesscontrol.Member{
				OrgId: fmt.Sprintf("test_org_%d", i+1),
			},
		}

		proposer := mock.NewMockMember(ctrl)
		proposer.EXPECT().GetMemberId().AnyTimes().Return(Validators[i])

		ac.EXPECT().NewMember(signEntries[i].Signer).AnyTimes().Return(proposer, nil)
		acErr.EXPECT().NewMember(signEntries[i].Signer).AnyTimes().Return(proposer, nil)
		principal := mock.NewMockPrincipal(ctrl)

		var tmpVote *maxbftpb.VoteData
		for _, vote := range votes {
			tmpVote = &maxbftpb.VoteData{
				BlockId: vote.BlockId,
				Height:  vote.Height,
				View:    vote.View,
				Author:  vote.Author,
			}
			data, _ := proto.Marshal(tmpVote)

			ac.EXPECT().CreatePrincipal(
				protocol.ResourceNameConsensusNode,
				[]*common.EndorsementEntry{signEntries[i]},
				data,
			).AnyTimes().Return(principal, nil)

			acErr.EXPECT().CreatePrincipal(
				protocol.ResourceNameConsensusNode,
				[]*common.EndorsementEntry{signEntries[i]},
				data,
			).AnyTimes().Return(principal, nil)
		}

		ac.EXPECT().VerifyPrincipal(principal).AnyTimes().Return(true, nil)
		acErr.EXPECT().VerifyPrincipal(principal).AnyTimes().Return(false, fmt.Errorf("boom"))
	}
	return ac, acErr, signEntries
}
