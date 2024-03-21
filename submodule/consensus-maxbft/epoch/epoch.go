/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package epoch

import (
	"fmt"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/compensator"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/compensator/compensator_service"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/engine"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/msgcache"
	consensusUtils "chainmaker.org/chainmaker/consensus-utils/v2"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/gogo/protobuf/proto"
)

var (
	chanCapacity = 64
)

// Epoch defines epoch objects for consensus engine
type Epoch struct {
	// current epoch id
	epochId uint64

	// local node id
	nodeId string

	// the last view number that the current epoch will process
	epochEndView uint64

	// validators of this epoch
	validators []string

	// impl consensus algo
	engine *engine.Engine

	// chain configuration
	config protocol.ChainConf

	// message bus for interacting with other components
	msgBus msgbus.MessageBus

	// cache pending proposals
	msgTransmitter msgcache.MsgTransmitter

	// for send requests
	requester compensator.RequestCompensator

	// for respond other nodes' requests
	responder compensator.ResponseCompensator

	// transfer vote message from Epoch to engine
	voteCh chan *maxbft.VoteData

	// transfer proposal message from msgTransmitter to engine
	proposalCh chan *msgcache.PendingProposal

	// transfer consistent message from consensus to consistent engine
	consistentCh chan *net.NetMsg

	// logger
	log protocol.Logger

	isStarted bool
}

// NewEpoch initials and returns an Epoch object
func NewEpoch(cfg *consensusUtils.ConsensusImplConfig, log protocol.Logger, epochId,
	endView uint64, validators []string, hasReplayed bool, consistentCh chan *net.NetMsg,
	newBlockSignalCh chan bool, isEngineRunning bool) *Epoch {
	epoch := &Epoch{
		epochId:      epochId,
		nodeId:       cfg.NodeId,
		epochEndView: endView,
		validators:   validators,
		msgBus:       cfg.MsgBus,
		log:          log,
		config:       cfg.ChainConf,
		voteCh:       make(chan *maxbft.VoteData, chanCapacity),
		proposalCh:   make(chan *msgcache.PendingProposal, chanCapacity),
		consistentCh: consistentCh,
	}
	epoch.requester = compensator_service.NewReqService(cfg.NodeId, log, cfg.MsgBus)
	consensusEngine := engine.NewEngine(log, cfg, epoch.epochId, epoch.epochEndView,
		validators, epoch.voteCh, epoch.proposalCh, epoch.consistentCh,
		newBlockSignalCh, isEngineRunning, hasReplayed, epoch.requester)
	if consensusEngine == nil {
		return nil
	}
	epoch.responder = compensator_service.NewRespService(
		consensusEngine.GetForest(), cfg.NodeId, cfg.MsgBus, cfg.Store, log, cfg.NetService, cfg.Ac)
	epoch.engine = consensusEngine
	epoch.msgTransmitter = msgcache.NewMsgCacheHandler(
		epoch.engine.GetForest(), epoch.engine.GetBaseCheck(), epoch.proposalCh, epoch.requester, epoch.log)
	return epoch
}

// Start starts the epoch
func (e *Epoch) Start() {
	if e.isStarted {
		return
	}
	e.log.Debugf("Start epoch: [%d:%d], validators:%+v", e.epochId, e.epochEndView, e.validators)
	if err := e.engine.StartEngine(); err != nil {
		e.log.Panic("engine start failed. error:%+v", err)
		return
	}
	e.msgTransmitter.SetForest(e.engine.GetForest())
	e.msgTransmitter.Start()
	e.isStarted = true
}

// Stop stops the epoch
func (e *Epoch) Stop() {
	if !e.isStarted {
		return
	}
	e.log.Debugf("Stop epoch: [%d:%d], validators:%+v", e.epochId, e.epochEndView, e.validators)
	if err := e.engine.StopEngine(); err != nil {
		e.log.Warnf("engine stop failed. error: %+v", err)
	}
	if err := e.msgTransmitter.Stop(); err != nil {
		e.log.Warnf("close MsgTransmitter failed, error: %s", err)
	}
	e.isStarted = false
}

// HandleConsensusMsg handle consensus messages from other consensus nodes
func (e *Epoch) HandleConsensusMsg(netMsg *net.NetMsg) {
	if netMsg.Type != net.NetMsg_CONSENSUS_MSG {
		return
	}
	consensusMsg := &maxbft.ConsensusMsg{}
	if err := proto.Unmarshal(netMsg.Payload, consensusMsg); err != nil {
		e.log.Errorf("unmarshal consensus msg failed, reason: %s", err)
		return
	}
	switch consensusMsg.Type {
	case maxbft.MessageType_PROPOSAL_MESSAGE:
		payload := &maxbft.ProposalData{}
		if err := proto.Unmarshal(consensusMsg.Payload, payload); err != nil {
			e.log.Errorf("unmarshal proposal data failed, reason: %s", err)
			return
		}
		proposal := &msgcache.PendingProposal{
			FromOtherNodes: false,
			Proposal:       payload,
		}
		if err := e.msgTransmitter.HandleProposal(proposal); err != nil {
			e.log.Warnf("handle proposal data failed, reason: %s", err)
			return
		}
	case maxbft.MessageType_VOTE_MESSAGE:
		vote := &maxbft.VoteData{}
		if err := proto.Unmarshal(consensusMsg.Payload, vote); err != nil {
			e.log.Errorf("unmarshal vote data failed, reason: %s", err)
			return
		}
		e.voteCh <- vote
	case maxbft.MessageType_PROPOSAL_FETCH_MESSAGE:
		reqMsg := &maxbft.ProposalFetchMsg{}
		if err := proto.Unmarshal(consensusMsg.Payload, reqMsg); err != nil {
			e.log.Errorf("unmarshal fetch request failed, reason: %s", err)
			return
		}
		e.log.DebugDynamic(func() string {
			return fmt.Sprintf("recv request[%d:%d:%x] response from %s",
				reqMsg.Height, reqMsg.View, reqMsg.BlockId, string(reqMsg.Requester))
		})
		e.responder.HandleRequest(reqMsg)
	case maxbft.MessageType_PROPOSAL_RESP_MESSAGE:
		respMsg := &maxbft.ProposalRespMsg{}
		if err := proto.Unmarshal(consensusMsg.Payload, respMsg); err != nil {
			e.log.Errorf("unmarshal proposal resp data failed, reason: %s", err)
			return
		}
		header := respMsg.Proposal.Block.Header
		e.log.DebugDynamic(func() string {
			return fmt.Sprintf("recv proposal's [%d:%d:%x] response from %s",
				header.BlockHeight, respMsg.Proposal.View, header.BlockHash, string(respMsg.Responser))
		})
		proposal := &msgcache.PendingProposal{
			FromOtherNodes: true,
			Proposal:       respMsg.Proposal,
			Sender:         string(respMsg.Responser),
			Qc:             respMsg.Qc,
		}
		if err := e.msgTransmitter.HandleProposal(proposal); err != nil {
			e.log.Warnf("handle proposal data failed, reason: %s", err)
			return
		}
	}
}

// DiscardBlocks call core module to discard blocks after some blocks have been committed
func (e *Epoch) DiscardBlocks(height uint64) {
	e.engine.DiscardBlocks(height)
}

// GetCurrentView returns the current view of the pacemaker of the local consensus engine
func (e *Epoch) GetCurrentView() uint64 {
	return e.engine.GetCurrentView()
}
