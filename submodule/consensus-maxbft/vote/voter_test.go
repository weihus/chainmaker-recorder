/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vote

import (
	"testing"

	"chainmaker.org/chainmaker/protocol/v2/test"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2/mock"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestVoter_GenerateVote(t *testing.T) {
	var (
		blockId = []byte("test_hash")
		nodeId  = "test_author_1"
		view    = uint64(10)
		height  = uint64(5)
		sig     = []byte("test_sig")
	)
	ctrl := gomock.NewController(t)
	signMemer := mock.NewMockSigningMember(ctrl)
	voteData := &maxbftpb.VoteData{
		Height:  height,
		Author:  []byte(nodeId),
		View:    view,
		BlockId: blockId,
	}
	data, _ := proto.Marshal(voteData)
	signMemer.EXPECT().Sign(HashType, data).AnyTimes().Return(sig, nil)
	signMemer.EXPECT().GetMember().AnyTimes().Return(nil, nil)
	voter := NewVoter(signMemer, view-1, 0, nodeId, HashType, test.NewTestLogger(t))
	proposal := &maxbftpb.ProposalData{
		Block: &common.Block{
			Header: &common.BlockHeader{
				BlockHash:   blockId,
				BlockHeight: height,
			},
		},
		View:     view,
		Proposer: "test_author_2",
	}
	vote, err := voter.GenerateVote(proposal, view)
	require.Nil(t, err)
	require.NotEmpty(t, vote)
	require.Equal(t, vote.BlockId, blockId)
	require.Equal(t, vote.Author, []byte(nodeId))
	require.Equal(t, vote.View, view)
	require.Equal(t, vote.Height, height)
	require.NotEmpty(t, vote.Signature)
	require.Equal(t, vote.Signature.Signature, sig)

	proposal.View = view + 1
	vote, _ = voter.GenerateVote(proposal, view)
	require.Nil(t, vote)

	proposal.View = view - 1
	vote, _ = voter.GenerateVote(proposal, view)
	require.Nil(t, vote)
}
