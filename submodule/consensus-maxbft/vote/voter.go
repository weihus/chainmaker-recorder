/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vote

import (
	"fmt"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/gogo/protobuf/proto"
)

// Voter used to generate vote
type Voter interface {
	GenerateVote(proposal *maxbft.ProposalData, curView uint64) (*maxbft.VoteData, error)
}

var _ Voter = &VoterImpl{}

// VoterImpl is an implementation of Voter interface
type VoterImpl struct {
	// for sign the vote
	signer protocol.SigningMember

	// last view number that we have voted for
	lastView uint64

	// current local node id
	id string

	// current epoch id
	epochId uint64

	// default: hash
	hashType string

	logger protocol.Logger
}

// NewVoter initial and return a VoterImpl object
func NewVoter(signer protocol.SigningMember, lastView, epochId uint64, id, hashType string,
	logger protocol.Logger) Voter {
	return &VoterImpl{
		signer:   signer,
		lastView: lastView,
		epochId:  epochId,
		id:       id,
		hashType: hashType,
		logger:   logger,
	}
}

// GenerateVote generate vote for a valid proposal with current view
func (v *VoterImpl) GenerateVote(proposal *maxbft.ProposalData, curView uint64) (*maxbft.VoteData, error) {
	var err error
	v.logger.DebugDynamic(func() string {
		header := proposal.Block.Header
		return fmt.Sprintf("start to GenerateVote. curView:%d, "+
			"proposal:%d:%d:%x", curView, header.BlockHeight, proposal.View, header.BlockHash)
	})

	defer func() {
		if err != nil {
			// log the error
			v.logger.Warnf("GenerateVote failed. error: %s", err)
		}
	}()

	// check if already vote for the view, or it's a request out of date
	if proposal.View <= v.lastView {
		err = fmt.Errorf("already vote for view[%d] or it's a request out of date", proposal.View)
		return nil, err
	}

	// check the view match the node state machine
	if proposal.View != curView {
		err = fmt.Errorf("received proposal on wrong view:%d", proposal.View)
		return nil, nil
	}

	// construct a vote for the proposal
	voteData := &maxbft.VoteData{
		Height:  proposal.Block.Header.BlockHeight,
		Author:  []byte(v.id),
		View:    proposal.View,
		BlockId: proposal.Block.Header.BlockHash,
		EpochId: v.epochId,
	}

	var (
		data []byte
		sign []byte
	)

	// marshal the vote without signature information
	if data, err = proto.Marshal(voteData); err != nil {
		err = fmt.Errorf("marshal voteData failed. error:%s", err)
		return nil, err
	}

	// sign the vote
	if sign, err = v.signer.Sign(v.hashType, data); err != nil {
		err = fmt.Errorf("sign data failed, err %v data %v", err, data)
		return nil, err
	}

	// attach the signature information to the vote
	mem, err := v.signer.GetMember()
	if err != nil {
		return nil, err
	}
	voteData.Signature = &common.EndorsementEntry{
		Signer:    mem,
		Signature: sign,
	}

	return voteData, nil
}
