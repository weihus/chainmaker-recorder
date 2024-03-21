/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package engine

import (
	"fmt"

	"chainmaker.org/chainmaker/pb-go/v2/consensus"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/msgcache"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/status"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	timeservice "chainmaker.org/chainmaker/consensus-utils/v2/time_service"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"
	chainUtils "chainmaker.org/chainmaker/utils/v2"

	"github.com/gogo/protobuf/proto"
)

// handleTimeout liveness logic of the consensus, using
// timeout events to push pacemaker into the next view,
func (e *Engine) handleTimeout(event *timeservice.TimerEvent) {
	if event == nil {
		return
	}
	e.log.InfoDynamic(func() string { return fmt.Sprintf("process timeout event: %+v", event) })
	currView := e.paceMaker.CurView()
	if currView != event.View {
		return
	}
	if newEvent := e.paceMaker.OnTimeout(event); newEvent != nil {
		e.log.DebugDynamic(func() string {
			return fmt.Sprintf("enter next timeout event: %v", newEvent)
		})
	}
	// save new view data to the log wal system
	if e.hasReplayed {
		if err := e.wal.AddNewView(); err != nil {
			e.log.Warnf("handleTimeout save new view information to wal failed. error:%+v", err)
		}
	}

	e.startNewView()
}

// handleProposal handles proposals:
// 1) received from another validator, if the local node is a follower in the proposal's view;
// 2) constructed by the local node, if it is the leader in the proposal's view
func (e *Engine) handleProposal(pendingProposal *msgcache.PendingProposal) {
	proposal := pendingProposal.Proposal
	header := proposal.Block.Header
	qc := pendingProposal.Qc
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("handle proposal: [%d:%d:%x] from %s",
			header.BlockHeight, proposal.View, header.BlockHash, proposal.Proposer)
	})
	var (
		validBlock = true
	)
	// 非节点自身生成的提案，进入校验逻辑
	// 1. 校验提案的QC包含的投票是否有效
	// 2. 校验提案
	if e.lastCreateProposal != proposal {
		var mode protocol.VerifyMode
		if !e.hasReplayed || pendingProposal.FromOtherNodes {
			mode = protocol.SYNC_VERIFY
		} else {
			mode = protocol.CONSENSUS_VERIFY
		}
		// If QC verification fails, exit in time
		if err := e.validator.ValidateJustifyQc(proposal); err != nil {
			e.log.Warnf("verify justify qc failed in the proposal [%d:%d:%x], reason: %s",
				header.BlockHeight, proposal.View, header.BlockHash, err)
			return
		}

		var (
			err     error
			results *consensus.VerifyResult
		)
		// QC has verified success and verify the block data carried by proposal
		if results, err = e.validator.ValidateProposal(proposal, qc, mode); err != nil {
			e.log.Warnf("validate proposal failed, reason: %s", err)
			validBlock = false
		}
		// Store wal only when the proposal verification succeeds
		if validBlock && e.hasReplayed {
			// add TxRwSet to proposal before write wal
			// 因为需要支持随机交易，所以存储提案信息时需要包含本次生成的读写集结果
			if results != nil {
				rwSet := make([]*common.TxRWSet, 0, len(results.TxsRwSet))
				for _, s := range results.TxsRwSet {
					rwSet = append(rwSet, s)
				}
				proposal.TxRwSet = rwSet
				proposal.Block = results.VerifiedBlock
			}
			msg, err := proto.Marshal(proposal)
			if err != nil {
				e.log.Warnf("handle proposal failed: marshal proposal error: %+v", err)
				return
			}
			index, err := e.wal.SaveWalEntry(maxbft.MessageType_PROPOSAL_MESSAGE, msg)
			if err != nil {
				e.log.Warnf("save proposal wal failed. error:%+v", err)
				return
			}
			e.wal.AddProposalWalIndex(proposal.Block.Header.BlockHeight, index)
		}
	}
	// 提案由两部分组成：上个提案的QC和当前提案的区块；
	// 可能存在：QC有效但当前的提案的区块无效场景。此时，应当应用提案携带的QC，
	// 更新节点的各中QC状态(genericQC, lockedQC, finalQC)，因为提案携带
	// 的QC是对它父块的确认，与当前提案本身的区块无关。
	committedProposals, err := e.forest.UpdateStatesByProposal(proposal, validBlock)
	if err != nil {
		e.log.Warnf("update states by qc in the proposal failed, reason: %s", err)
		return
	}
	// Update wal lastCommitIndex
	if length := len(committedProposals); length > 0 {
		e.collector.PruneView(committedProposals[length-1].View())
		e.wal.UpdateWalIndexAndTrunc(committedProposals[length-1].Height())
	}
	if !validBlock {
		// 尝试用提案携带的QC推动paceMaker状态，推动成功后进入下一个视图的处理流程
		if _, enterNext := e.paceMaker.UpdateWithQc(proposal.JustifyQc); enterNext {
			e.startNewView()
		}
		return
	}

	// 到达这步时，表示提案携带的QC验证成功，将提案添加到forest中
	if err := e.forest.AddProposal(proposal, false); err != nil {
		e.log.Warnf("addProposal failed, reason: %s", err)
		return
	}
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("add proposal [%d:%d:%x] to"+
			" forest success", header.BlockHeight, proposal.View, header.BlockHash)
	})

	// 依据节点当前视图状态，对提案进行进一步处理
	// 提案视图等于节点视图，则对提案生成投票、发送给下个视图的leader
	// 提案视图不等于节点视图，尝试构建该提案的QC，使用构建成功的QC更新共识状态
	if proposal.View == e.paceMaker.CurView() {
		e.handleCurrViewProposal(proposal)
	} else {
		e.handleOtherViewProposal(proposal)
	}
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("handle proposal success [%d:%d:%x]",
			header.BlockHeight, proposal.View, header.BlockHash)
	})
}

// handleCurrViewProposal Generate the vote of the proposal and send it to the
// leader of the next view. Use the proposal to advance the view state in paceMaker.
// If the current node is the leader of the next view, the voting process will be
// entered, otherwise the node enter the next view, the new view is processed.
func (e *Engine) handleCurrViewProposal(proposal *maxbft.ProposalData) {
	header := proposal.Block.Header
	vote, err := e.voter.GenerateVote(proposal, e.paceMaker.CurView())
	if err != nil {
		// Note. only record the reason for generate vote failed.
		e.log.Errorf("generate vote failed for proposal[%d:%d:%x], "+
			"reason: %s", header.BlockHeight, proposal.View, header.BlockHash, err)
		return
	}

	nextLeader := e.validator.GetLeader(proposal.View + 1)
	if nextLeader == e.nodeId {
		// 节点为下个视图的leader，则以leader角色处理提案，此时pacemaker应该进入收集投票状态，
		// 不应该触发视图切换。
		_, enterNext := e.paceMaker.UpdateWithProposal(proposal, true)
		if enterNext {
			e.log.Errorf("next leader should not enter new view by proposal")
			return
		}
		// 作为下个视图的leader，处理前个视图的投票、聚合QC，使用聚合的QC推进共识进入下个视图.
		e.handleVote(vote)
		return
	}

	e.sendVoteToLeader(vote, nextLeader)
	_, enterNext := e.paceMaker.UpdateWithProposal(proposal, false)
	if !enterNext {
		e.log.Errorf("replicate should enter next view by currView "+
			"proposal[%d:%d:%x]", header.BlockHeight, proposal.View, header.BlockHash)
		return
	}
	if e.hasReplayed {
		if err = e.wal.AddNewView(); err != nil {
			e.log.Warnf("handleCurrViewProposal save new view information to wal failed. error:%+v", err)
		}
	}
	e.startNewView()
}

// sendVoteToLeader packaging a vote to a net.NetMsg and send it to
// the leader node of the next view
func (e *Engine) sendVoteToLeader(vote *maxbft.VoteData, nextLeader string) {
	voteData := utils.MustMarshal(&maxbft.ConsensusMsg{
		Type:    maxbft.MessageType_VOTE_MESSAGE,
		Payload: utils.MustMarshal(vote),
	})
	netMsg := &net.NetMsg{
		To:      nextLeader,
		Type:    net.NetMsg_CONSENSUS_MSG,
		Payload: voteData,
	}
	e.msgBus.Publish(msgbus.SendConsensusMsg, netMsg)
}

// startNewView process the new view by local node, The follower attempts to
// load and process the proposal for the current view from the Forest, and the
// leader generate a new proposal and broadcast it to all followers, processing the new proposal.
// Node If the epoch switch condition is reached, the process for the new view is terminated.
func (e *Engine) startNewView() {
	// signal may be pushed multiple times by the core module, so when
	// processing logic of the newView, check the channel to see if there
	// are more signals waiting to be processed and discard the signal.
	select {
	case <-e.newBlockSignalCh:
	default:
	}
	var (
		err      error
		currView = e.paceMaker.CurView()
		leader   = e.validator.GetLeader(currView)
	)
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("enter new view: %d, leader is %s, currentNode %s", currView, leader, e.nodeId)
	})

	if e.forest.FinalView() >= e.epochEndView {
		e.log.Infof("the current epoch has ended, so the generation of new blocks "+
			"should be stopped, finalView %d, epochEndView %d", e.forest.FinalView(), e.epochEndView)
		return
	}
	if leader != e.nodeId {
		// 当节点由leader ----> 非leader时，发送信号给core模块，停止向共识模块发送定时器信号
		if e.validator.GetLeader(currView-1) == e.nodeId {
			e.msgBus.Publish(msgbus.ProposeState, false)
		}
		if proposal := e.forest.GetProposalByView(currView); proposal != nil {
			e.handleCurrViewProposal(proposal)
		}
		return
	}

	parent := e.forest.GetGenericQC()
	buildMsg := &maxbft.BuildProposal{
		View:    currView,
		Height:  parent.Votes[0].Height + 1,
		PreHash: parent.Votes[0].BlockId,
	}
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("fired block generation: [%d:%d]", buildMsg.Height, buildMsg.View)
	})
	// 1. will generate block by core module and create proposal on it
	newBlock, err := e.coreProposer.ProposeBlock(buildMsg)
	if err != nil {
		e.log.Errorf("generate new block[%d:%d]"+
			" failed, reason: %s", buildMsg.Height, buildMsg.View, err)
		return
	}

	if !e.validator.IsLegalForNilBlock(newBlock.Block) {
		e.log.Warnf("empty block are not allowed to be generated")
		e.lastGiveUpProposeView = currView
		return
	}

	var (
		governanceTxRwSet *common.TxRWSet
		proposalMsg       *maxbft.ProposalData
		rawProposalMsg    *maxbft.ProposalData
	)
	governanceTxRwSet, err = utils.ConstructContractTxRwSet(
		currView, e.epochEndView, e.epochId, e.conf.ChainConfig(), e.store, e.log)
	if err != nil {
		e.log.Errorf("construct contract tx rwset failed, error:%+v", err)
		return
	}

	// 2. broadcast proposal
	proposalMsg, err = e.buildProposal(newBlock.CutBlock,
		parent, make(map[string]*common.TxRWSet), governanceTxRwSet, currView)
	if err != nil {
		e.log.Errorf("buildProposal failed, reason: %s", err)
		return
	}

	// nil block's RwSets is nil, and it has not been cut
	if newBlock.Block.Header.TxCount == 0 {
		rawProposalMsg = proposalMsg
	} else {
		rawProposalMsg, err = e.buildProposal(newBlock.Block, parent, newBlock.TxsRwSet, governanceTxRwSet, currView)
		if err != nil {
			e.log.Errorf("buildProposal to save wal failed, error:%+v", err)
			return
		}
	}
	e.lastCreateProposal = rawProposalMsg

	// save wal, proposal msg
	if e.hasReplayed {
		msg, err := proto.Marshal(rawProposalMsg)
		if err != nil {
			e.log.Warnf("startNewView failed. marshal proposal error:%+v", err)
			return
		}
		index, err := e.wal.SaveWalEntry(maxbft.MessageType_PROPOSAL_MESSAGE, msg)
		if err != nil {
			e.log.Errorf("startNewView failed. error:%+v", err)
			return
		}
		e.wal.AddProposalWalIndex(buildMsg.Height, index)
	}

	// 广播提案给其它共识节点，并通知core模块停止推送生成区块信号.
	e.broadcastProposal(proposalMsg)
	e.msgBus.Publish(msgbus.ProposeState, false)
	// 3. process the proposal
	e.handleProposal(&msgcache.PendingProposal{
		FromOtherNodes: false,
		Proposal:       rawProposalMsg,
	})
}

// buildProposal builds a proposal with a raw block constructed by the core engine,
// and add the consensus information, such as the current view, epoch, proposer, qc,
// governance, and sign the block
func (e *Engine) buildProposal(block *common.Block, preQc *maxbft.QuorumCert,
	rwSet map[string]*common.TxRWSet, governanceTxRwSet *common.TxRWSet, view uint64) (
	*maxbft.ProposalData, error) {
	// 1. add consensusArgs to blockHeader
	utils.AddArgsToBlock(view, block, governanceTxRwSet)
	e.log.DebugDynamic(func() string {
		if len(rwSet) == 0 {
			return fmt.Sprintf("build proposal: %d:%d", block.Header.BlockHeight, view)
		}
		return fmt.Sprintf("build proposal with RwSets for saving: %d:%d", block.Header.BlockHeight, view)
	})
	blockHash, sig, err := chainUtils.SignBlock(e.hashType, e.singer, block)
	if err != nil {
		return nil, fmt.Errorf("sign proposal failed, reason: %s", err)
	}
	signer, err := e.singer.GetMember()
	if err != nil {
		return nil, fmt.Errorf("getMember failed, reason: %s", err)
	}
	block.Header.Proposer = signer
	block.Header.BlockHash = blockHash
	block.Header.Signature = sig
	sets := make([]*common.TxRWSet, 0, len(rwSet))
	for _, s := range rwSet {
		sets = append(sets, s)
	}
	return &maxbft.ProposalData{
		Block:     block,
		EpochId:   e.epochId,
		View:      view,
		Proposer:  e.nodeId,
		JustifyQc: preQc,
		TxRwSet:   sets,
	}, nil
}

// broadcastProposal packaging the specified proposal to net.NetMsg
// and send them to the other validators
func (e *Engine) broadcastProposal(proposal *maxbft.ProposalData) {
	consensusMsg := utils.MustMarshal(&maxbft.ConsensusMsg{
		Type:    maxbft.MessageType_PROPOSAL_MESSAGE,
		Payload: utils.MustMarshal(proposal),
	})
	for _, peer := range e.nodes {
		if peer == e.nodeId {
			continue
		}
		msg := &net.NetMsg{
			To:      peer,
			Payload: consensusMsg,
			Type:    net.NetMsg_CONSENSUS_MSG,
		}
		e.msgBus.Publish(msgbus.SendConsensusMsg, msg)
	}
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("broadcast proposal msg succeed, [%d:%d:%x]",
			proposal.Block.Header.BlockHeight, proposal.View, proposal.Block.Header.BlockHash)
	})
}

// handleOtherViewProposal has blocks with multiple views of the same height, so if forks
// has parented proposal for the future view of the proposal, then will try to
// build the qc for the block and process.
func (e *Engine) handleOtherViewProposal(proposal *maxbft.ProposalData) {
	header := proposal.Block.Header
	qc, err := e.collector.TryBuildQc(proposal)
	if err != nil {
		e.log.Errorf("try build qc failed, reason: %s", err)
		return
	}
	if qc == nil {
		e.log.Debugf("not build qc for proposal [%d:%d:%x]",
			header.BlockHeight, proposal.View, header.BlockHash)
		return
	}
	e.processQC(qc)
}

// processQC iff verify the qc generated by local node
// Use valid qc to update forest status and submit blocks which
// is conformed to the three-chain rule. Update pacemaker status
// with qc. if go to the next view, the new view processing logic is triggered
func (e *Engine) processQC(qc *maxbft.QuorumCert) {
	if err := e.validator.ValidateQc(qc); err != nil {
		e.log.Errorf("verify qc failed, reason: %s", err)
		return
	}
	if err := e.forest.UpdateStatesByQC(qc); err != nil {
		e.log.Errorf("add qc to forest failed, reason: %s", err)
		return
	}

	_, enterNewView := e.paceMaker.UpdateWithQc(qc)
	if enterNewView {
		if e.hasReplayed {
			if err := e.wal.AddNewView(); err != nil {
				e.log.Warnf("processQc save new view information to wal failed. error:%+v", err)
			}
		}
		e.startNewView()
	}
}

// handleVote Process vote and update consensus status with valid vote.
// only the leader of the next view in vote to process. If no proposal
// corresponding to the vote is received, the vote is cached in the
// waiting verification area. After passing the voting verification,
// try to construct QC by vote. will be exited if the qc construction fails.
// Otherwise, process the newly generated qc.
func (e *Engine) handleVote(vote *maxbft.VoteData) {
	if finalView := e.forest.FinalView(); finalView >= vote.View {
		e.log.DebugDynamic(func() string {
			return fmt.Sprintf("receive stale vote[%d:%d:%x], "+
				"final view: %d", vote.Height, vote.View, vote.BlockId, finalView)
		})
		return
	}

	if vote.EpochId != e.epochId {
		e.log.Warnf("receive a vote from other epoch. vote epoch:%d, current epoch:%d", vote.EpochId, e.epochId)
		return
	}

	if leader := e.validator.GetLeader(vote.View + 1); leader != e.nodeId {
		e.log.Errorf("local node is not the leader for %d view ", vote.View)
		return
	}
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("handle vote info: [%d:%d:%x] from %s",
			vote.Height, vote.View, vote.BlockId, vote.Author)
	})
	if !e.forest.HasProposal(string(vote.BlockId)) {
		if err := e.collector.AddPendingVote(vote); err != nil {
			e.log.Errorf("add vote to pending cache failed, reason: %s", err)
		}
		return
	}

	blockNode := e.forest.Node(string(vote.BlockId))
	if err := e.validator.ValidateVote(vote, blockNode.Data().Proposal()); err != nil {
		e.log.Errorf("verify vote failed, reason: %s", err)
		return
	}

	if e.hasReplayed {
		// save vote into the log wal system
		msg, err := proto.Marshal(vote)
		if err != nil {
			e.log.Errorf("handle vote failed. marshal vote error:%+v", err)
			return
		}

		if _, err = e.wal.SaveWalEntry(maxbft.MessageType_VOTE_MESSAGE, msg); err != nil {
			e.log.Errorf("handle vote failed. save vote to the log wal system error:%+v", err)
			return
		}
	}

	if err := e.collector.AddVote(vote); err != nil {
		e.log.Errorf("add vote to collector failed, reason: %s", err)
		return
	}

	qc, err := e.collector.TryBuildQc(blockNode.Data().Proposal())
	if err != nil {
		e.log.Errorf("try build qc failed, reason: %s", err)
		return
	}
	if qc == nil {
		e.log.Debugf("not build qc for proposal [%d:%d:%x]",
			vote.Height, vote.View, vote.BlockId)
		return
	}
	e.processQC(qc)
}

// handleRemoteStatus Using the common state of top F +1 nodes, try to update the view state of this node
func (e *Engine) handleRemoteStatus(rStatus *status.NodeStatus) {
	e.UpdateStatus(NewRemoteInfo(&maxbft.NodeStatus{
		Height: rStatus.Height, View: rStatus.View, Epoch: rStatus.Epoch}, nil))
}

// hasGiveUpProposeInCurView Whether the current view generates invalid empty blocks
func (e *Engine) hasGiveUpProposeInCurView() bool {
	return e.paceMaker.CurView() == e.lastGiveUpProposeView
}
