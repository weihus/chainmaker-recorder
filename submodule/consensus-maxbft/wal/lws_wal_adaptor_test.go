/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package wal

import (
	"fmt"
	"os"
	"testing"
	"time"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	forest_mock "chainmaker.org/chainmaker/consensus-maxbft/v2/forest/mock"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/vote"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"chainmaker.org/chainmaker/protocol/v2/test"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestLwsWalAdaptor_SaveWalEntry(t *testing.T) {
	blockCache := forest_mock.NewBlockCacheMock()
	// create the forest
	fork := forest.NewForest(forest.NewCachedForestNode(forest.GenesisProposalContainer()),
		blockCache, test.NewTestLogger(t))

	// create the collector
	collector := vote.NewVotesCollector(nil, 3, test.NewTestLogger(t))

	testPath := "test_wal_01"

	// delete the remaining files from the last test run
	if _, err := os.Stat(testPath); err == nil {
		os.RemoveAll(testPath)
	}

	// prepare proposals
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proposalBlk1 := forest.NewProposalData(0, 1, 1, "1", "0")
	proposalBlk2 := forest.NewProposalData(1, 2, 2, "2", "1")
	proposalBlk3 := forest.NewProposalData(2, 3, 3, "3", "2")

	verifier := mock.NewMockBlockVerifier(ctrl)
	verifier.EXPECT().VerifyBlockWithRwSets(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	// create the walAdaptor
	wal, err := NewLwsWalAdaptor(testPath, 0, nil,
		fork, nil, collector, verifier, test.NewTestLogger(t))
	require.Nil(t, err)

	// replay an empty wal file
	updated, err := wal.ReplayWal()
	require.Nil(t, err)
	require.False(t, updated)

	// initial the fork's data
	err = fork.AddProposal(proposalBlk1, true)
	require.Nil(t, err)

	// save a proposal to wal
	AddProposal(t, wal, proposalBlk2, 0)
	wal.AddProposalWalIndex(2, 0)
	status := wal.GetHeightStatus()
	require.Equal(t, status[2], uint64(0))

	// save 4 votes to wal
	AddVotes(t, wal, 2, 2, 1)

	// save the second proposal to wal
	AddProposal(t, wal, proposalBlk3, 5)
	wal.AddProposalWalIndex(3, 5)
	status = wal.GetHeightStatus()
	require.Equal(t, status[3], uint64(5))

	// save the next 4 votes to wal
	AddVotes(t, wal, 3, 3, 6)

	// truncate the wal
	wal.UpdateWalIndexAndTrunc(2)

	iterator := wal.(*LwsWalAdaptor).lws.NewLogIterator()
	defer iterator.Release()
	iterator.SkipToFirst()
	require.Equal(t, iterator.Next().Index(), uint64(1))

	// replay the wal file
	updated, err = wal.ReplayWal()
	require.True(t, updated)
	require.Nil(t, err)

	time.Sleep(100 * time.Millisecond)
	// delete the remaining files from the current test run
	if _, err := os.Stat(testPath); err == nil {
		os.RemoveAll(testPath)
	}
}

func AddProposal(t *testing.T, wal WalAdaptor, data *maxbftpb.ProposalData, start uint64) {
	msg, err := proto.Marshal(data)
	require.Nil(t, err)
	index, err := wal.SaveWalEntry(maxbftpb.MessageType_PROPOSAL_MESSAGE, msg)
	require.Nil(t, err)
	require.Equal(t, index, start+1)
}

func AddVotes(t *testing.T, wal WalAdaptor, height, view uint64, start uint64) {
	var (
		vote  *maxbftpb.VoteData
		msg   []byte
		index uint64
		err   error
	)
	for i := 1; i < 5; i++ {
		vote = &maxbftpb.VoteData{
			Height: height,
			View:   view,
			Author: []byte(fmt.Sprintf("test_author_%d", i)),
		}
		msg, err = proto.Marshal(vote)
		require.Nil(t, err)
		index, _ = wal.SaveWalEntry(maxbftpb.MessageType_VOTE_MESSAGE, msg)
		require.Equal(t, index, uint64(i)+start)
	}
}
