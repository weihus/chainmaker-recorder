/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vote

import (
	"fmt"
	"sync"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/verifier"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"
)

// VotesCollector defines a vote collector to collect and process votes, and build qc
type VotesCollector interface {
	PruneView(view uint64)
	AddVote(vote *maxbftpb.VoteData) error
	AddPendingVote(vote *maxbftpb.VoteData) error
	TryBuildQc(proposal *maxbftpb.ProposalData) (*maxbftpb.QuorumCert, error)
}

// Collector is an implementation of VotesCollector interface
type Collector struct {
	mtx sync.Mutex

	// vote pool to cache unverified votes,
	// when no corresponding proposal has been received.
	// formatting: [blockHash][authorIdx]voteData
	pendingVotes map[uint64]map[string]*maxbftpb.VoteData

	// for verify votes
	verifier verifier.Verifier

	// vote pool to cache verified votes
	// formatting: [view][authorIdx]voteData
	view2Votes map[uint64]map[string]*maxbftpb.VoteData

	// vote pool to cache verified votes
	// formatting: [blockHash][authorIdx]voteData
	block2Votes map[string]map[string]*maxbftpb.VoteData

	// current quorum of chain
	quorum uint

	// last pruned view
	lastPrunedView uint64

	logger protocol.Logger
}

// NewVotesCollector initial and return aCollector object
func NewVotesCollector(verifier verifier.Verifier, quorum uint, logger protocol.Logger) VotesCollector {
	logger.Debugf("new votes collector, quorum:%d", quorum)
	return &Collector{
		verifier:     verifier,
		pendingVotes: make(map[uint64]map[string]*maxbftpb.VoteData),
		view2Votes:   make(map[uint64]map[string]*maxbftpb.VoteData),
		block2Votes:  make(map[string]map[string]*maxbftpb.VoteData),
		quorum:       quorum,
		logger:       logger,
		mtx:          sync.Mutex{},
	}
}

// TryBuildQc builds a quorum certification for a proposal when valid votes count reaches the quorum
func (v *Collector) TryBuildQc(proposal *maxbftpb.ProposalData) (*maxbftpb.QuorumCert, error) {
	v.logger.DebugDynamic(func() string {
		header := proposal.Block.Header
		return fmt.Sprintf("tryBuildQc start. proposal:%d:%d:%x from"+
			" leader: %s, quorum:%d", header.BlockHeight, proposal.View, header.BlockHash, proposal.Proposer, v.quorum)
	})
	view := proposal.View
	v.mtx.Lock()
	defer v.mtx.Unlock()

	// if there are some votes that have not been verified,
	// verify them and put them in verified vote cache
	pendingVotes, pendingOk := v.pendingVotes[view]
	if pendingOk {
		for author, vote := range pendingVotes {
			if err := v.verifier.ValidateVote(vote, proposal); err != nil {
				v.logger.Warnf("verify vote failed. error:%+v", err)
			} else {
				if err = v.addVote(vote); err != nil {
					v.logger.Warnf("addVote failed:%+v", err)
				}
			}
			delete(v.pendingVotes[view], author)
		}
	}

	// check there are enough votes for quorum
	votes, votesOk := v.view2Votes[view]
	if !votesOk || uint(len(votes)) < v.quorum {
		v.logger.Debugf("vote for view[%d] not done. "+
			"quorum:%d, has received: %d", view, v.quorum, len(v.view2Votes[view]))
		return nil, nil
	}

	// synthesize a qc for votes
	voteList := make([]*maxbftpb.VoteData, 0, v.quorum)
	i := uint(0)
	for _, vote := range votes {
		voteList = append(voteList, vote)
		i++
		if i == v.quorum {
			break
		}
	}
	qc := &maxbftpb.QuorumCert{
		Votes: voteList,
	}
	v.logger.DebugDynamic(func() string {
		return fmt.Sprintf("tryBuildQc success. "+
			"qc:%d:%d:%x", qc.Votes[0].Height, qc.Votes[0].View, qc.Votes[0].BlockId)
	})

	return qc, nil
}

// AddVote add a vote to votes cache
func (v *Collector) AddVote(vote *maxbftpb.VoteData) error {
	v.mtx.Lock()
	defer v.mtx.Unlock()
	return v.addVote(vote)
}

// AddPendingVote add a vote to pending votes cache
func (v *Collector) AddPendingVote(vote *maxbftpb.VoteData) error {
	var (
		view   = vote.View
		author = string(vote.Author)
	)
	v.logger.DebugDynamic(func() string {
		return fmt.Sprintf("addPendingVote start. "+
			"vote:%d:%d:%x from voter %s", vote.Height, view, vote.BlockId, author)
	})
	v.mtx.Lock()
	defer v.mtx.Unlock()
	// check the vote pool already exists for the view
	if _, ok := v.pendingVotes[view]; !ok {
		v.pendingVotes[view] = make(map[string]*maxbftpb.VoteData)
	}

	// check the vote from this author on this view already exists
	if _, ok := v.pendingVotes[view][author]; ok {
		return fmt.Errorf("AddPendingVote failed. "+
			"vote from author[%s] on block[%x] already exists", author, vote.BlockId)
	}

	// put the view into the unverified vote pool
	v.pendingVotes[view][author] = vote
	return nil
}

// PruneView prune votes that before specified view from votes cache and pending votes cache
func (v *Collector) PruneView(view uint64) {
	v.logger.Debugf("PruneView start. view:%d", view)
	var (
		blockId        string
		toDelBlockHash = make([]string, 0)
	)
	v.mtx.Lock()
	defer v.mtx.Unlock()

	// prune unverified votes
	for i := range v.pendingVotes {
		if i <= view {
			delete(v.pendingVotes, i)
		}
	}

	// prune verified votes
	for i, votes := range v.view2Votes {
		if i <= view {
			if len(votes) > 0 {
				for _, vote := range votes {
					blockId = string(vote.BlockId)
					break
				}
				// record block hash to prune
				toDelBlockHash = append(toDelBlockHash, blockId)
			}
			delete(v.view2Votes, i)
			if view > v.lastPrunedView {
				v.lastPrunedView = view
			}
		}
	}

	// prune block2Votes
	for _, hash := range toDelBlockHash {
		delete(v.block2Votes, hash)
	}
	v.logger.Debugf("PruneView done. lastPrunedView:%d", v.lastPrunedView)
}

func (v *Collector) addVote(vote *maxbftpb.VoteData) error {
	v.logger.Debugf("addVote start. vote:%d:%d:%x "+
		"from voter %s", vote.Height, vote.View, vote.BlockId, vote.Author)

	// check the vote pool already exists for the view
	view := vote.View
	if _, ok := v.view2Votes[view]; !ok {
		v.view2Votes[view] = make(map[string]*maxbftpb.VoteData)
	}

	// check the vote from this author on this view already exists
	author := string(vote.Author)
	if _, ok := v.view2Votes[view][author]; ok {
		return fmt.Errorf("addVote failed. vote from author[%s] on view[%d] already exists", author, view)
	}

	// put the view into the verified vote pool
	v.view2Votes[view][author] = vote

	blockHash := string(vote.BlockId)
	if _, ok := v.block2Votes[blockHash]; !ok {
		v.block2Votes[blockHash] = make(map[string]*maxbftpb.VoteData)
	}
	v.block2Votes[blockHash][author] = vote
	v.logger.Debugf("addVote success. view:%d, author:%s", view, author)
	return nil
}
