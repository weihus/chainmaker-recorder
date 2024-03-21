/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package engine

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/msgcache"

	"chainmaker.org/chainmaker/common/v2/crypto"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/pacemaker"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/verifier"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/vote"
	timeservice "chainmaker.org/chainmaker/consensus-utils/v2/time_service"
	"chainmaker.org/chainmaker/pb-go/v2/accesscontrol"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"chainmaker.org/chainmaker/protocol/v2/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func getMockSigner(ctrl *gomock.Controller) protocol.SigningMember {
	signer := mock.NewMockSigningMember(ctrl)

	signer.EXPECT().Sign(gomock.Any(), gomock.Any()).AnyTimes().Return([]byte("signature"), nil)
	signer.EXPECT().GetMember().AnyTimes().Return(&accesscontrol.Member{
		OrgId:      "org1",
		MemberInfo: []byte("member info"),
		MemberType: accesscontrol.MemberType_CERT_HASH,
	}, nil)
	return signer
}

type mockMsgBus struct {
	msgbus.MessageBus
	pubContents []interface{}
}

func (m *mockMsgBus) Publish(topic msgbus.Topic, payload interface{}) {
	if topic == msgbus.SendConsensusMsg {
		m.pubContents = append(m.pubContents, payload)
	}
}

type mockPaceMaker struct {
	pacemaker.PaceMaker
	view uint64
}

func (m *mockPaceMaker) CurView() uint64 {
	return m.view
}

func (m *mockPaceMaker) OnTimeout(event *timeservice.TimerEvent) *timeservice.TimerEvent {
	m.view = event.View + 1
	return &timeservice.TimerEvent{
		View: m.view,
	}
}

func (m *mockPaceMaker) UpdateWithQc(qc *maxbft.QuorumCert) (*timeservice.TimerEvent, bool) {
	return nil, false
}

func (m *mockPaceMaker) UpdateWithProposal(
	proposal *maxbft.ProposalData, isNextViewLeader bool) (*timeservice.TimerEvent, bool) {
	return nil, isNextViewLeader
}

type mockValidator struct {
	verifier.Verifier
	leader     string
	leaderView uint64
	otherNode  string

	failedProposal string
}

func (m *mockValidator) ValidateProposal(proposal *maxbft.ProposalData, qc *maxbft.QuorumCert,
	mode protocol.VerifyMode) (*consensus.VerifyResult, error) {
	if bytes.Equal(proposal.Block.Header.BlockHash, []byte(m.failedProposal)) {
		return nil, fmt.Errorf("verify proposal failed")
	}
	return nil, nil
}

func (m *mockValidator) ValidateVote(vote *maxbft.VoteData, proposal *maxbft.ProposalData) error {
	if !bytes.Equal(vote.Author, []byte("failed")) {
		return nil
	}
	return fmt.Errorf("verify vote failed")
}

func (m *mockValidator) ValidateQc(qc *maxbft.QuorumCert) error {
	if !bytes.Equal(qc.Votes[0].Author, []byte("failed")) {
		return nil
	}
	return fmt.Errorf("verify vote failed")
}

func (m *mockValidator) ValidateJustifyQc(proposal *maxbft.ProposalData) error {
	return m.ValidateQc(proposal.JustifyQc)
}

func (m *mockValidator) GetLeader(view uint64) string {
	if view == m.leaderView {
		return m.leader
	}
	return m.otherNode
}

func (m *mockValidator) IsLegalForNilBlock(block *common.Block) bool {
	return false
}

type mockCollector struct {
	vote.VotesCollector

	buildQcFailed string
	// verify success vote info
	votes map[string]*maxbft.VoteData
	// pending vote info
	pendingVotes map[string]*maxbft.VoteData
}

func (m *mockCollector) TryBuildQc(proposal *maxbft.ProposalData) (*maxbft.QuorumCert, error) {
	if strings.Contains(string(proposal.Block.Header.BlockHash), m.buildQcFailed) {
		return nil, fmt.Errorf("build qc failed")
	}
	return &maxbft.QuorumCert{}, nil
}

func (m *mockCollector) AddVote(vote *maxbft.VoteData) error {
	m.votes[string(vote.BlockId)] = vote
	return nil
}

func (m *mockCollector) AddPendingVote(vote *maxbft.VoteData) error {
	if _, ok := m.pendingVotes[string(vote.BlockId)]; ok {
		return fmt.Errorf("has exist same vote")
	}
	m.pendingVotes[string(vote.BlockId)] = vote
	return nil
}

type mockVoter struct {
	vote.Voter
	voteProposal string
}

func (v *mockVoter) GenerateVote(proposal *maxbft.ProposalData, curView uint64) (*maxbft.VoteData, error) {
	blkHash := proposal.Block.Header.BlockHash
	if !bytes.Equal([]byte(v.voteProposal), blkHash) {
		return nil, fmt.Errorf("vote failed")
	}
	return &maxbft.VoteData{View: proposal.View, BlockId: blkHash}, nil
}

type mockForest struct {
	forest.Forester

	qcs               []*maxbft.QuorumCert
	proposals         []*maxbft.ProposalData
	addFailedProposal string

	finalView       uint64
	hasProposalId   string
	hasProposalView uint64
	hasProposal     *maxbft.ProposalData
}

func (m *mockForest) UpdateStatesByProposal(proposal *maxbft.ProposalData, validBlock bool) (
	[]*forest.ProposalContainer, error) {
	if strings.Contains(string(proposal.Block.Header.BlockHash), m.addFailedProposal) {
		return nil, fmt.Errorf("add proposal failed")
	}
	m.proposals = append(m.proposals, proposal)
	return nil, nil
}

func (m *mockForest) UpdateStatesByQC(qc *maxbft.QuorumCert) error {
	if bytes.Equal(qc.Votes[0].BlockId, []byte(m.hasProposalId)) {
		m.qcs = append(m.qcs, qc)
		return nil
	}
	return fmt.Errorf("non proposal")
}

func (m *mockForest) AddProposal(proposal *maxbft.ProposalData, replay bool) error {
	return nil
}

func (m *mockForest) FinalView() uint64 {
	return m.finalView
}

func (m *mockForest) Node(key string) forest.ForestNoder {
	if strings.Contains(key, m.hasProposalId) {
		return forest.NewCachedForestNode(forest.NewProposalContainer(m.hasProposal, false, false))
	}
	return nil
}
func (m *mockForest) GetProposalByView(view uint64) *maxbft.ProposalData {
	if view == m.hasProposalView {
		return m.hasProposal
	}
	return nil
}

func (m *mockForest) HasProposal(blockId string) bool {
	return strings.Contains(blockId, m.hasProposalId)
}

func (m *mockForest) GetGenericQC() *maxbft.QuorumCert {
	return &maxbft.QuorumCert{
		Votes: []*maxbft.VoteData{
			{
				Height:  1,
				View:    1,
				BlockId: []byte("parent block"),
			},
		},
	}
}

func (m *mockForest) GetGenericHeight() uint64 {
	return 2
}

func TestEngine_BuildProposal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e := Engine{
		log:          test.NewTestLogger(t),
		singer:       getMockSigner(ctrl),
		hashType:     crypto.CRYPTO_ALGO_SHA256,
		epochEndView: 100,
	}

	// 1.
	blk := &common.Block{
		Header: &common.BlockHeader{
			BlockHeight: 1,
			ChainId:     "test1",
		},
	}
	proposal, err := e.buildProposal(blk, &maxbft.QuorumCert{}, nil, nil, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, proposal.View)
	require.EqualValues(t, 1, proposal.Block.Header.BlockHeight)
	require.EqualValues(t, "test1", proposal.Block.Header.ChainId)
}

func TestBroadcastProposal(t *testing.T) {
	mockBus := &mockMsgBus{}
	e := Engine{
		log:    test.NewTestLogger(t),
		msgBus: mockBus,

		nodeId: "self",
		nodes:  []string{"self", "node2", "node3", "node4"},
	}
	e.broadcastProposal(&maxbft.ProposalData{
		Block: &common.Block{
			Header: &common.BlockHeader{BlockHeight: 10, BlockHash: []byte("hash")},
		},
		View: 10,
	})
	require.EqualValues(t, 3, len(mockBus.pubContents))
}

func TestEngine_StartNewView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPace := &mockPaceMaker{view: 10}
	mockProposer := mock.NewMockBlockProposer(ctrl)
	mockProposer.EXPECT().ProposeBlock(gomock.Any()).AnyTimes().Return(&consensus.ProposalBlock{}, nil)

	e := Engine{
		log:          test.NewTestLogger(t),
		paceMaker:    mockPace,
		forest:       &mockForest{},
		coreProposer: mockProposer,

		nodeId: "self",
		nodes:  []string{"self", "node2", "node3", "node4"},
	}
	mockVerifier := &mockValidator{
		leaderView: 11,
		leader:     e.nodeId,
		otherNode:  "node2",
	}
	e.validator = mockVerifier

	// 1. self node is not leader for view 10
	e.startNewView()

	//	2. self node is leader for view 10
	mockVerifier.leaderView = 10
	e.startNewView()
}

func TestEngine_HandleTimeout(t *testing.T) {
	mockPace := &mockPaceMaker{view: 10}
	e := Engine{
		log:       test.NewTestLogger(t),
		paceMaker: mockPace,
		forest:    &mockForest{},

		nodeId: "self",
	}

	// 1. event.View != currView
	e.handleTimeout(&timeservice.TimerEvent{
		View: 9,
	})
	require.EqualValues(t, 10, mockPace.view)

	// 2. event.View == currView
	mockVerifier := &mockValidator{
		leaderView: 12,
		leader:     e.nodeId,
		otherNode:  "node2",
	}
	e.validator = mockVerifier
	e.hasReplayed = false
	e.handleTimeout(&timeservice.TimerEvent{
		View: 10,
	})
	require.EqualValues(t, 11, mockPace.view)
}

func TestEngine_HandleVote(t *testing.T) {
	mockPace := &mockPaceMaker{view: 10}
	mcollector := &mockCollector{
		buildQcFailed: "buildqc_failed",
		votes:         make(map[string]*maxbft.VoteData), pendingVotes: make(map[string]*maxbft.VoteData),
	}
	mforest := &mockForest{
		finalView:       8,
		hasProposal:     &maxbft.ProposalData{Block: &common.Block{Header: &common.BlockHeader{BlockHash: []byte(mcollector.buildQcFailed)}}},
		hasProposalView: 11,
		hasProposalId:   "exist_proposal",
	}
	e := Engine{
		log:       test.NewTestLogger(t),
		paceMaker: mockPace,
		forest:    mforest,
		collector: mcollector,
		nodeId:    "self",
	}
	mockVerifier := &mockValidator{
		leaderView: 11,
		leader:     e.nodeId,
		otherNode:  "node2",
	}
	e.validator = mockVerifier

	// do not save wal
	e.hasReplayed = false

	// 1. node is not leader in view 9
	e.handleVote(&maxbft.VoteData{View: 7})

	// 2. not has proposal for the view
	e.handleVote(&maxbft.VoteData{View: 10, BlockId: []byte("non_proposal")})
	require.EqualValues(t, true, mcollector.pendingVotes["non_proposal"] != nil)

	// 3. has proposal with the view and verify vote failed
	e.handleVote(&maxbft.VoteData{View: 10, BlockId: []byte("exist_proposal"), Author: []byte("failed")})
	require.EqualValues(t, 1, len(mcollector.pendingVotes))

	// 4. vote has succeeded process but build qc failed
	e.handleVote(&maxbft.VoteData{View: 10, BlockId: []byte(fmt.Sprintf("%s_%s", mforest.hasProposalId, mcollector.buildQcFailed)), Author: []byte("success")})
	require.EqualValues(t, 1, len(mcollector.votes))
}

func TestEngine_HandleQC(t *testing.T) {
	mockPace := &mockPaceMaker{view: 10}
	mforest := &mockForest{hasProposalId: "exist_proposal"}
	mValidator := &mockValidator{}

	e := Engine{
		log:       test.NewTestLogger(t),
		paceMaker: mockPace,
		forest:    mforest,
		nodeId:    "self",
		validator: mValidator,
	}

	// 1. add qc failed
	e.processQC(&maxbft.QuorumCert{Votes: []*maxbft.VoteData{{BlockId: []byte("non_exist_proposal")}}})
	require.EqualValues(t, 0, len(mforest.qcs))

	// 2. add qc success
	e.processQC(&maxbft.QuorumCert{Votes: []*maxbft.VoteData{{BlockId: []byte("exist_proposal")}}})
	require.EqualValues(t, 1, len(mforest.qcs))
}

func TestEngine_HandleProposal(t *testing.T) {
	mockPace := &mockPaceMaker{view: 10}
	mforest := &mockForest{addFailedProposal: "add_failed"}
	mvalidator := &mockValidator{failedProposal: "verify_failed"}
	e := Engine{
		log:         test.NewTestLogger(t),
		forest:      mforest,
		paceMaker:   mockPace,
		validator:   mvalidator,
		voter:       &mockVoter{voteProposal: "vote_proposal"},
		hasReplayed: false,

		nodeId: "self",
	}

	// 1. verify proposal failed
	e.handleProposal(&msgcache.PendingProposal{
		FromOtherNodes: false,
		Proposal: &maxbft.ProposalData{
			View:     10,
			Proposer: "node1",
			Block: &common.Block{Header: &common.BlockHeader{BlockHeight: 9,
				BlockHash: []byte(mvalidator.failedProposal)}},
			JustifyQc: &maxbft.QuorumCert{Votes: []*maxbft.VoteData{{BlockId: []byte("pre_block")}}}},
	})

	//	2. verify proposal success and add proposal failed
	e.handleProposal(&msgcache.PendingProposal{
		FromOtherNodes: false,
		Proposal: &maxbft.ProposalData{
			View:     10,
			Proposer: "node1",
			Block: &common.Block{Header: &common.BlockHeader{BlockHeight: 9,
				BlockHash: []byte(fmt.Sprintf("%s_%s", mvalidator.failedProposal, mforest.addFailedProposal))}},
			JustifyQc: &maxbft.QuorumCert{Votes: []*maxbft.VoteData{{BlockId: []byte("pre_block")}}}},
	})

	// 3. add proposal success but generate vote failed
	e.handleProposal(&msgcache.PendingProposal{
		FromOtherNodes: false,
		Proposal: &maxbft.ProposalData{
			View:     10,
			Proposer: "node1",
			Block: &common.Block{Header: &common.BlockHeader{BlockHeight: 9,
				BlockHash: []byte(fmt.Sprintf("%s_add_success", mvalidator.failedProposal))}},
			JustifyQc: &maxbft.QuorumCert{Votes: []*maxbft.VoteData{{BlockId: []byte("pre_block")}}}},
	})
}

func TestEngine_HandleCurrProposal(t *testing.T) {
	mBus := &mockMsgBus{}
	mockPace := &mockPaceMaker{view: 10}
	mvoter := &mockVoter{voteProposal: "vote_proposal"}
	e := Engine{
		log:       test.NewTestLogger(t),
		voter:     mvoter,
		paceMaker: mockPace,
		msgBus:    mBus,

		nodeId: "self",
	}
	mvalidator := &mockValidator{
		failedProposal: "verify_failed",
		leaderView:     11,
		leader:         e.nodeId,
	}
	e.validator = mvalidator

	// 1. generate vote and next leader is self
	e.handleCurrViewProposal(&maxbft.ProposalData{
		View:     9,
		Proposer: "node1",
		Block:    &common.Block{Header: &common.BlockHeader{BlockHeight: 9, BlockHash: []byte(mvoter.voteProposal)}}},
	)

	// 2. generate vote and next leader is not self
	e.handleCurrViewProposal(&maxbft.ProposalData{
		View:     10,
		Proposer: "node1",
		Block:    &common.Block{Header: &common.BlockHeader{BlockHeight: 9, BlockHash: []byte(mvoter.voteProposal)}}},
	)
}

func TestEngine_HandleOtherProposal(t *testing.T) {
	mcollector := &mockCollector{buildQcFailed: "failed_qc"}
	e := Engine{
		log:       test.NewTestLogger(t),
		collector: mcollector,
		nodeId:    "self",
	}

	// 1. build qc failed
	e.handleOtherViewProposal(&maxbft.ProposalData{
		View:     10,
		Proposer: "node1",
		Block:    &common.Block{Header: &common.BlockHeader{BlockHeight: 9, BlockHash: []byte(mcollector.buildQcFailed)}}},
	)
}
