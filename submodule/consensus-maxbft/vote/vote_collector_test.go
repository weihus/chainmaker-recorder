/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vote

import (
	"fmt"
	"testing"

	"chainmaker.org/chainmaker/protocol/v2/test"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/verifier"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"

	"github.com/stretchr/testify/require"
)

const (
	BlockHash = "test_block_hash"
	Author    = "test_author"
	HashType  = "hash"
)

func newVoteCollector(t *testing.T) *Collector {
	collector := NewVotesCollector(&verifier.Validator{}, 3, test.NewTestLogger(t))
	return collector.(*Collector)
}

func TestVoteCollector_AddVote(t *testing.T) {
	collector := newVoteCollector(t)
	vote := &maxbftpb.VoteData{
		BlockId: []byte(BlockHash),
		Height:  5,
		View:    7,
		EpochId: 0,
		Author:  []byte(Author),
	}
	err := collector.AddVote(vote)
	require.Nil(t, err)
	require.Equal(t, collector.block2Votes[BlockHash][Author], vote)
	require.Equal(t, collector.view2Votes[7][Author], vote)
	require.Equal(t, len(collector.pendingVotes), 0)
}

func TestVoteCollector_AddPendingVote(t *testing.T) {
	collector := newVoteCollector(t)
	vote := &maxbftpb.VoteData{
		BlockId:   []byte(BlockHash),
		Height:    5,
		View:      7,
		EpochId:   0,
		Author:    []byte(Author),
		Signature: nil,
	}
	var err error

	vote.Signature = &common.EndorsementEntry{
		Signer:    nil,
		Signature: nil,
	}

	err = collector.AddPendingVote(vote)
	require.Nil(t, err)
	require.Equal(t, len(collector.block2Votes), 0)
	require.Equal(t, len(collector.view2Votes), 0)
	require.Equal(t, collector.pendingVotes[vote.View][Author], vote)
}

func TestVoteCollector_TryBuildQcSuccess(t *testing.T) {
	collector := newVoteCollector(t)
	var (
		i      int
		vote   *maxbftpb.VoteData
		err    error
		hash   = []byte("test_hash")
		height = uint64(5)
		view   = uint64(8)
	)
	for i = 0; i < 2; i++ {
		vote = &maxbftpb.VoteData{
			BlockId: hash,
			Height:  height,
			View:    view,
			Author:  []byte(fmt.Sprintf("test_author_%d", i)),
		}
		err = collector.AddVote(vote)
		require.Nil(t, err)
	}
	proposal := &maxbftpb.ProposalData{
		Block: &common.Block{
			Header: &common.BlockHeader{
				BlockHash: hash,
			},
		},
		View:     view,
		Proposer: "test_author_2",
	}
	qc, err := collector.TryBuildQc(proposal)
	require.Nil(t, err)
	require.Nil(t, qc)
	vote = &maxbftpb.VoteData{
		BlockId: hash,
		Height:  height,
		View:    view,
		Author:  []byte("test_author_3"),
	}
	err = collector.AddVote(vote)
	require.Nil(t, err)
	qc, err = collector.TryBuildQc(proposal)
	require.Nil(t, err)
	require.NotEmpty(t, qc)
	require.Equal(t, len(qc.Votes), 3)
	require.Equal(t, qc.Votes[0].BlockId, hash)
	require.Equal(t, qc.Votes[0].View, view)
	require.Equal(t, qc.Votes[0].Height, height)
}

func TestVoteCollector_PruneView(t *testing.T) {
	collector := newVoteCollector(t)
	var (
		vote               *maxbftpb.VoteData
		hash, author       []byte
		i, j, height, view uint64
		err                error
	)
	for i = 0; i < 5; i++ {
		hash = []byte(fmt.Sprintf("test_hash_%d", i))
		height = i + 5
		view = i + 10
		for j = 0; j < 4; j++ {
			author = []byte(fmt.Sprintf("test_author_%d", j))
			vote = &maxbftpb.VoteData{
				BlockId: hash,
				Height:  height,
				View:    view,
				EpochId: 0,
				Author:  author,
			}
			if i%2 == 1 {
				vote.Signature = &common.EndorsementEntry{
					Signer:    nil,
					Signature: nil,
				}
				err = collector.AddPendingVote(vote)
				require.Nil(t, err)
			} else {
				err = collector.AddVote(vote)
				require.Nil(t, err)
			}
		}
	}
	collector.PruneView(12)
	require.Equal(t, len(collector.pendingVotes), 1)
	require.Equal(t, len(collector.block2Votes), 1)
	require.Equal(t, len(collector.view2Votes), 1)
	require.Equal(t, len(collector.pendingVotes[13]), 4)
	require.Equal(t, len(collector.block2Votes["test_hash_4"]), 4)
	require.Equal(t, len(collector.view2Votes[14]), 4)
	for i = 0; i < 4; i++ {
		require.Equal(t,
			collector.pendingVotes[13][fmt.Sprintf("test_author_%d", i)].BlockId,
			[]byte("test_hash_3"))
		require.Equal(t,
			collector.pendingVotes[13][fmt.Sprintf("test_author_%d", i)].Height,
			uint64(8))
		require.Equal(t,
			collector.pendingVotes[13][fmt.Sprintf("test_author_%d", i)].View,
			uint64(13))
		require.Equal(t,
			collector.pendingVotes[13][fmt.Sprintf("test_author_%d", i)].Author,
			[]byte(fmt.Sprintf("test_author_%d", i)))

		require.Equal(t,
			collector.block2Votes["test_hash_4"][fmt.Sprintf("test_author_%d", i)].BlockId,
			[]byte("test_hash_4"))
		require.Equal(t,
			collector.block2Votes["test_hash_4"][fmt.Sprintf("test_author_%d", i)].Height, uint64(9))
		require.Equal(t,
			collector.block2Votes["test_hash_4"][fmt.Sprintf("test_author_%d", i)].View, uint64(14))
		require.Equal(t,
			collector.block2Votes["test_hash_4"][fmt.Sprintf("test_author_%d", i)].Author,
			[]byte(fmt.Sprintf("test_author_%d", i)))

		require.Equal(t,
			collector.view2Votes[14][fmt.Sprintf("test_author_%d", i)].BlockId,
			[]byte("test_hash_4"))
		require.Equal(t,
			collector.view2Votes[14][fmt.Sprintf("test_author_%d", i)].Height,
			uint64(9))
		require.Equal(t,
			collector.view2Votes[14][fmt.Sprintf("test_author_%d", i)].View,
			uint64(14))
		require.Equal(t,
			collector.view2Votes[14][fmt.Sprintf("test_author_%d", i)].Author,
			[]byte(fmt.Sprintf("test_author_%d", i)))
	}
}
