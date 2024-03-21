/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pacemaker

import (
	"testing"
	"time"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	forest_mock "chainmaker.org/chainmaker/consensus-maxbft/v2/forest/mock"
	timeservice "chainmaker.org/chainmaker/consensus-utils/v2/time_service"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/test"

	"github.com/stretchr/testify/require"
)

func TestPaceMaker_OnTimeout(t *testing.T) {
	var (
		view  = uint64(5)
		round = uint16(15)
	)
	forest := mockForest(t)
	pacemaker := NewPaceMaker(view, forest, 0, 0, 0, mockLogger())
	event := &timeservice.TimerEvent{
		View:     view,
		Duration: time.Second * time.Duration(round),
		Type:     maxbftpb.ConsStateType_PACEMAKER,
	}
	// 第一次超时； baseTimeOut + (view+1-finalView-4)*interval = 17000
	e := pacemaker.OnTimeout(event)
	require.Equal(t, e.View, view+1)
	require.Equal(t, e.Duration.Milliseconds(), int64(17000))
	require.Equal(t, pacemaker.CurView(), view+1)

	// 接着上次的事件继续超时
	e = pacemaker.OnTimeout(e)
	require.Equal(t, e.View, view+2)
	require.Equal(t, e.Duration.Milliseconds(), int64(18000))
	require.Equal(t, pacemaker.CurView(), view+2)

	//RoundTimeout = uint16(120)
	// 再次超时
	e = pacemaker.OnTimeout(e)
	require.Equal(t, e.View, view+3)
	require.Equal(t, e.Duration.Milliseconds(), int64(19000))
	require.Equal(t, pacemaker.CurView(), view+3)
}

func TestPaceMaker_UpdateWithQc(t *testing.T) {
	view := uint64(5)
	forest := mockForest(t)
	pacemaker := NewPaceMaker(view, forest, 0, 0, 0, mockLogger())
	votes := make([]*maxbftpb.VoteData, 3)
	for i := 0; i < 3; i++ {
		votes[i] = &maxbftpb.VoteData{
			View: view,
		}
	}
	qc := &maxbftpb.QuorumCert{
		Votes: votes,
	}
	event, advance := pacemaker.UpdateWithQc(qc)
	require.NotEmpty(t, event)
	require.True(t, advance)
	require.Equal(t, pacemaker.CurView(), view+1)
	event, advance = pacemaker.UpdateWithQc(qc)
	require.Empty(t, event)
	require.False(t, advance)
	require.Equal(t, pacemaker.CurView(), view+1)
}

func TestPaceMaker_UpdateWithProposal(t *testing.T) {
	view := uint64(5)
	forest := mockForest(t)
	pacemaker := NewPaceMaker(view, forest, 0, 0, 0, mockLogger())
	votes := make([]*maxbftpb.VoteData, 3)
	for i := 0; i < 3; i++ {
		votes[i] = &maxbftpb.VoteData{
			View: view + 1,
		}
	}
	qc := &maxbftpb.QuorumCert{
		Votes: votes,
	}
	block := &common.Block{
		Header: &common.BlockHeader{
			BlockHash:   []byte("test_hash"),
			BlockHeight: view + 2,
		},
	}
	proposal := &maxbftpb.ProposalData{
		Block:     block,
		View:      view + 2,
		Proposer:  "test_proposer",
		JustifyQc: qc,
	}
	event, advance := pacemaker.UpdateWithProposal(proposal, false)
	require.NotEmpty(t, event)
	require.True(t, advance)
	require.Equal(t, pacemaker.CurView(), view+3)

	proposal.View = view + 3
	for _, vote := range proposal.JustifyQc.Votes {
		vote.View = view + 2
	}
	event, advance = pacemaker.UpdateWithProposal(proposal, true)
	require.NotEmpty(t, event)
	require.False(t, advance)
	require.Equal(t, pacemaker.CurView(), view+3)
}

func mockForest(t *testing.T) forest.Forester {
	// mock forest
	blockCache := forest_mock.NewBlockCacheMock()
	forest := forest.NewForest(forest.NewCachedForestNode(forest.GenesisProposalContainer()),
		blockCache, test.NewTestLogger(t))
	return forest
}

func mockLogger() protocol.Logger {
	return &test.GoLogger{}
}
