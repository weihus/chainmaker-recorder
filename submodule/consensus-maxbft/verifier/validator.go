/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verifier

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"runtime"
	"strings"
	"sync"

	commonErrors "chainmaker.org/chainmaker/common/v2/errors"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	maxbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/gogo/protobuf/proto"
	"github.com/panjf2000/ants/v2"
)

// MinQuorumForQc defines the minimum votes count for a quorum certification
const MinQuorumForQc = 3

var (
	certIdNotMappingError = "cert id not mapping"
)

// Verifier defines a verifier used to validate quorum certifications, proposals and blocks
type Verifier interface {
	// BaseCheck implements basic check for proposals
	BaseCheck(proposal *maxbftpb.ProposalData, qc *maxbftpb.QuorumCert) error
	ValidateProposal(proposal *maxbftpb.ProposalData, qc *maxbftpb.QuorumCert,
		mode protocol.VerifyMode) (*consensus.VerifyResult, error)
	// ValidateQc Verify the QC aggregated from the local vote, and the
	// proposal corresponding to the QC has been verified. This behavior
	// occurs on the leader, which collects votes for proposals to aggregate QC.
	ValidateQc(qc *maxbftpb.QuorumCert) error
	// ValidateJustifyQc Verify the QC carried in the proposal
	ValidateJustifyQc(proposal *maxbftpb.ProposalData) error
	// IsLegalForNilBlock  the logic is to verify the nil block whether can be generated.
	IsLegalForNilBlock(block *common.Block) bool
	// ValidateVote validate specified vote with specified proposal
	ValidateVote(vote *maxbftpb.VoteData, proposal *maxbftpb.ProposalData) error
	// GetLeader return the leader for the specified view
	GetLeader(view uint64) string
}

var _ Verifier = &Validator{}

// Validator is an implementation of Verifier interface
type Validator struct {
	// validator nodeIds
	validators []string

	// local nodeId
	nodeId string

	// current epoch id
	epochId uint64

	// the last view number current epoch will process
	epochEndView uint64

	// total count of validators
	amount int

	// quorum of validators: there are at least (2/3)n + 1 validators
	// were active to ensure the chain working
	quorum int

	// ac for calculate the signature of message
	ac protocol.AccessControlProvider

	// net for calculate the nodeId from signer
	net protocol.NetService

	config protocol.ChainConf

	forks    forest.Forester
	ledger   protocol.LedgerCache
	verifier protocol.BlockVerifier
	store    protocol.BlockchainStore
	logger   protocol.Logger
}

// NewValidator initial and return a Validator object
func NewValidator(validators []string, verifier protocol.BlockVerifier,
	ac protocol.AccessControlProvider, net protocol.NetService, nodeId string,
	forks forest.Forester, store protocol.BlockchainStore, cache protocol.LedgerCache,
	epochId, epochEndView uint64, logger protocol.Logger, conf protocol.ChainConf) Verifier {

	n := len(validators)
	quorum := utils.GetQuorum(n)
	validator := &Validator{
		validators:   validators,
		nodeId:       nodeId,
		epochId:      epochId,
		epochEndView: epochEndView,
		amount:       n,
		quorum:       int(quorum),
		ac:           ac,
		net:          net,
		forks:        forks,
		verifier:     verifier,
		config:       conf,
		store:        store,
		ledger:       cache,
		logger:       logger,
	}
	return validator
}

// BaseCheck implements basic check for proposals
func (v *Validator) BaseCheck(proposal *maxbftpb.ProposalData, qc *maxbftpb.QuorumCert) error {
	// 由于提案中包含是用户签名证书、但节点标识是由tls证书生成的
	// 两个证书间的关联关系由Net模块负责，由此出现个问题：当对端节点
	// 掉线后，该节点发送的提案由于无法再通过包含的签名证书从Net模块查询到
	// 节点标识，提案身份验证失败从而导致应该有效的提案校验无法通过。
	// 因此，采取了一种补救方案，当提案是由一致性引擎主动推送，此时有可能携带
	// 该提案的QC，仅需校验QC中包含f+1张有效投票也可认为该提案身份验证通过.
	err := v.validateProposer(proposal)
	if err != nil {
		if !strings.Contains(err.Error(), certIdNotMappingError) || qc == nil {
			return err
		}
		// 在有多个节点先后重启的场景中，可能由于节点掉线而网络不通，导致验证提案者失败的情况。
		// 在这种情况下，验证提案本身的qc，只要qc中有f+1张有效投票，就可以认为该提案的提案者是有效的。
		if err = v.validateMinGroupQc(qc, proposal); err != nil {
			v.logger.Warnf("validateMinGroupQc failed. error: %+v", err)
			return err
		}
	}
	return nil
}

// ValidateQc validate specified quorum certification
func (v *Validator) ValidateQc(qc *maxbftpb.QuorumCert) error {
	v.logger.Debugf("ValidateQc: %d:%d:%x", qc.Votes[0].Height, qc.Votes[0].View, qc.Votes[0].BlockId)

	// check safeNode rules
	if !v.checkSafeNode(qc) {
		return fmt.Errorf("check safeNode failed. proposal[%d:%d:%x] is not a safe node",
			qc.Votes[0].Height, qc.Votes[0].View, qc.Votes[0].BlockId)
	}

	// if this is the first block in the blockchain, in other words
	// its pre-block is the genesis block, it's not needed to verify justifyQc
	if qc.Votes[0].Height == 0 {
		v.logger.Debugf("child-block of genesis block not needed to verify qc")
		return nil
	}

	if total := len(qc.Votes); total < v.quorum {
		return fmt.Errorf("votes total count in qc was error. want %d votes, got %d",
			v.quorum, total)
	}

	// check every vote in qc
	if err := v.checkVotesInQc(qc); err != nil {
		v.logger.Warnf("ValidateQc failed. checkVotesInQc error:%+v", err)
		return err
	}
	return nil
}

// ValidateProposal validate specified proposal
func (v *Validator) ValidateProposal(proposal *maxbftpb.ProposalData, qc *maxbftpb.QuorumCert,
	mode protocol.VerifyMode) (*consensus.VerifyResult, error) {
	header := proposal.Block.Header
	v.logger.Debugf("ValidateProposal: %d:%d:%x from proposer %s",
		header.BlockHeight, proposal.View, header.BlockHash, proposal.Proposer)

	var err error
	defer func() {
		// log the error
		if proposal != nil && err != nil {
			v.logger.Warnf("ValidateProposal failed [%d:%d:%x]. error: %s",
				proposal.Block.Header.BlockHeight, proposal.View, header.BlockHash, err)
		}
	}()

	// 1. check final view
	if proposal.View < v.forks.FinalView() {
		err = fmt.Errorf("old proposal, ignore it. proposalView:[%d], FinalView:[%d]",
			proposal.View, v.forks.FinalView())
		return nil, err
	}

	// 2. check block has existed in forest
	if v.forks.HasProposal(string(proposal.Block.Header.BlockHash)) {
		err = fmt.Errorf("proposal exsits")
		return nil, err
	}

	// 3. verify whether you could be generated nil block
	// 仅在leader生成提案时对空块进行校验，因为存在下述场景：共识节点重启后，通过小同步拿到的
	// 当前提案所有 祖先都已被提交上链，如果当前提案为nil块，会由于forest不存在该提案的祖先
	// 而导致验证失败，但其它未重启的节点在验证该提案时，它的祖先中有一个非空块而导致验证通过；
	// 由此产生分叉。
	//if !v.IsLegalForNilBlock(proposal.Block) {
	//	err = fmt.Errorf("empty block are not allowed to be generated")
	//	return err
	//}

	// validate epoch and proposer
	if err = v.BaseCheck(proposal, qc); err != nil {
		v.logger.Warnf("base check failed. error:%+v", err)
		return nil, err
	}

	// 共识模式下，需校验提案包含的Epoch合约内容
	if mode == protocol.CONSENSUS_VERIFY {
		// 4. check consensus arguments for epoch contract
		if err = v.checkContractTxRwSet(proposal); err != nil {
			err = fmt.Errorf("check contract txRwSet failed. error: %+v", err)
			return nil, err
		}
	}

	// 5. call core engine to verifyBlock
	// ErrBlockHadBeenCommitted should be treated specially.
	var result *consensus.VerifyResult
	if mode == protocol.CONSENSUS_VERIFY {
		result, err = v.verifier.VerifyBlockSync(proposal.Block, mode)
		if err != nil && err != commonErrors.ErrBlockHadBeenCommited {
			err = fmt.Errorf("VerifyBlockSync error:%+v", err)
			return nil, err
		}
	} else {
		if proposal.TxRwSet != nil {
			if err = v.verifier.VerifyBlockWithRwSets(proposal.Block, proposal.TxRwSet, mode); err != nil &&
				err != commonErrors.ErrBlockHadBeenCommited {
				err = fmt.Errorf("VerifyBlockWithRwSets error:%+v", err)
				return nil, err
			}
		} else {
			if err = v.verifier.VerifyBlock(proposal.Block, mode); err != nil &&
				err != commonErrors.ErrBlockHadBeenCommited {
				err = fmt.Errorf("VerifyBlock error:%+v", err)
				return nil, err
			}
		}
	}
	v.logger.DebugDynamic(func() string {
		return fmt.Sprintf("ValidateProposal success [%d:%d:%x], chainConfig after verify: +%v",
			header.BlockHeight, proposal.View, header.BlockHash, v.config.ChainConfig())
	})
	return result, nil
}

// IsLegalForNilBlock the logic is to verify the nil block whether can be generated.
// If the nearest ancestor blocks are empty or none exist,
// then no new empty block are allowed to be generated.
func (v *Validator) IsLegalForNilBlock(block *common.Block) bool {
	if len(block.Txs) > 0 || block.Header.TxCount > 0 {
		return true
	}

	latest3Ancestors := v.forks.GetLatest3Ancestors(&maxbft.ProposalData{
		Block: block,
	})
	if len(latest3Ancestors) == 0 {
		return false
	}
	hasNoNilBlock := false
	for _, ancestor := range latest3Ancestors {
		if len(ancestor.Data().Proposal().Block.Txs) > 0 ||
			ancestor.Data().Proposal().Block.Header.TxCount > 0 {
			hasNoNilBlock = true
			break
		}
	}
	return hasNoNilBlock
}

// ValidateVote validate specified vote with specified proposal
func (v *Validator) ValidateVote(vote *maxbftpb.VoteData, proposal *maxbftpb.ProposalData) error {
	v.logger.Debugf("ValidateVote start. vote:%d:%d:%x ", vote.Height, vote.View, vote.BlockId)

	var err error
	defer func() {
		if err != nil {
			// log the error
			v.logger.Warnf("ValidateVote failed. error: %s", err)
		}
	}()

	// check the author of vote
	if vote.GetAuthor() == nil {
		err = fmt.Errorf("received a vote msg with nil author")
		return err
	}

	// check the vote and the proposal are match
	if vote.View != proposal.View || vote.Height != proposal.Block.Header.BlockHeight ||
		vote.EpochId != proposal.EpochId {
		err = fmt.Errorf("receive vote for wrong proposal[epoch:view:height]:[%d:%d:%d], vote:[%d:%d:%d]",
			proposal.EpochId, proposal.View, proposal.Block.Header.BlockHeight,
			vote.EpochId, vote.View, vote.Height)
		return err
	}

	// check that the local node is the next leader to verify the vote
	if proposer := v.GetLeader(vote.View + 1); proposer != v.nodeId {
		err = fmt.Errorf("received a vote at view[%d],bug I'm not the leader", vote.View)
		return err
	}

	// when a vote comes from another node, its signature certificate
	// verifies the voter's identity to ensure that there is no impostor
	if string(vote.Author) != v.nodeId {
		// check signer and author are the same
		var voteNodeId string
		voteNodeId, err = utils.GetNodeIdFromSigner(vote.Signature.Signer, v.ac, v.net)
		if err != nil {
			err = fmt.Errorf("utils.GetNodeIdFromSigner failed. error: %+v", err)
			return err
		}

		if string(vote.GetAuthor()) != voteNodeId {
			err = fmt.Errorf("wrong signer. signer:[%s], vote author:[%s]", voteNodeId, vote.GetAuthor())
			return err
		}
	}

	// check vote signature
	if err = v.validateSignature(vote); err != nil {
		err = fmt.Errorf("validateSignature error, vote %v, err %v", vote, err)
		return err
	}

	return nil
}

// GetLeader return the leader for the specified view
func (v *Validator) GetLeader(view uint64) string {
	return v.validators[int(view)%v.amount]
}

func (v *Validator) validateProposer(proposal *maxbftpb.ProposalData) error {
	if proposal.EpochId != v.epochId {
		return fmt.Errorf("received a proposal from another epoch: %d, current epoch:%d",
			proposal.EpochId, v.epochId)
	}
	// check the leader of the view is equal to proposal.Proposer
	if proposer := proposal.GetProposer(); v.GetLeader(proposal.View) != proposer {
		return fmt.Errorf("received a proposal at view [%d] from invalid addr [%s]. leader:[%s]",
			proposal.View, proposer, v.GetLeader(proposal.View))
	}

	// check the signer and the proposer are match
	nodeId, err := utils.GetNodeIdFromSigner(proposal.Block.Header.Proposer, v.ac, v.net)
	if err != nil {
		err = fmt.Errorf("utils.GetNodeIdFromSigner failed. error:%+v", err)
		return err
	}
	if nodeId != proposal.Proposer {
		return fmt.Errorf("proposer in proposal is wrong. proposer:[%s], block proposer:[%s]",
			proposal.Proposer, nodeId)
	}

	return nil
}

func (v *Validator) validateSignature(vote *maxbftpb.VoteData) error {
	if err := verifyConsensusMsgSign(vote, v.ac); err != nil {
		return fmt.Errorf("verify signature failed, error: %s, sigHash: %x",
			err, sha256.Sum256(vote.Signature.Signature))
	}
	return nil
}

func (v *Validator) checkVotesInQc(qc *maxbftpb.QuorumCert) error {
	// check every vote in qc, and calculate the valid vote count
	votedBlockNum, err := v.countNumFromVotes(qc)
	if err != nil {
		return err
	}

	// check if there are enough valid votes in qc
	if votedBlockNum < v.quorum {
		return fmt.Errorf(fmt.Sprintf("vote block num [%v] less than expected [%v]",
			votedBlockNum, v.quorum))
	}
	return nil
}

//countNumFromVotes Verify the voting data in QC and count the votes
func (v *Validator) countNumFromVotes(qc *maxbftpb.QuorumCert) (int, error) {
	// calculate the count of votes in qc that for a block
	voteBlkNum, err := CountNumFromVotes(qc)
	if err != nil {
		return 0, err
	}

	// verify the votes
	if _, err := VerifyVotes(
		v.ac, len(qc.Votes), qc.Votes, v.validators); err != nil {
		return 0, err
	}
	return int(voteBlkNum), nil
}

func (v *Validator) checkContractTxRwSet(proposal *maxbft.ProposalData) error {
	var (
		err     error
		txRwSet *common.TxRWSet
		data    []byte
	)

	// TODO,该检查可以优化，仅当达到视图切换条件时，才进行世代合约内容的比较。
	// 因为构建合约读写集、比较属于耗时操作
	if txRwSet, err = utils.ConstructContractTxRwSet(proposal.View,
		v.epochEndView, v.epochId, v.config.ChainConfig(), v.store, v.logger); err != nil {
		return fmt.Errorf("construct contract txRwSet failed. error:%+v", err)
	}

	args := &consensus.BlockHeaderConsensusArgs{
		ConsensusType: int64(consensus.ConsensusType_MAXBFT),
		View:          proposal.View,
		ConsensusData: txRwSet,
	}

	data, err = proto.Marshal(args)
	if err != nil {
		return fmt.Errorf("marshal consensus args failed. error: %+v", err)
	}

	if !bytes.Equal(data, proposal.Block.Header.ConsensusArgs) {
		originArgs := &consensus.BlockHeaderConsensusArgs{}
		if err := proto.Unmarshal(proposal.Block.Header.ConsensusArgs, originArgs); err != nil {
			return fmt.Errorf("unmarshal epoch config from block failed, reason: %s", err)
		}
		return fmt.Errorf("block header consensus args not match. proposal view:%d, "+
			"current epochEndView:%d, current epochId:%d, expected: %+v, actual: %+v,"+
			"expectedHash: %x, actualHash: %x, args in block: %x, generate in verify: %x ",
			proposal.View, v.epochEndView, v.epochId, originArgs, args,
			sha256.Sum256(proposal.Block.Header.ConsensusArgs), sha256.Sum256(data),
			data, proposal.Block.Header.ConsensusArgs)
	}
	return nil
}

// ValidateJustifyQc 验证提案中携带的QC
//
func (v *Validator) ValidateJustifyQc(proposal *maxbft.ProposalData) error {
	if proposal.JustifyQc == nil || len(proposal.JustifyQc.Votes) == 0 {
		return fmt.Errorf("justifyQc is nil or there is no votes in justifyQc")
	}

	// check the justify qc was for the pre-block
	if !bytes.Equal(proposal.Block.Header.PreBlockHash, proposal.JustifyQc.Votes[0].BlockId) {
		return fmt.Errorf("mismatch pre hash [%x] in block, "+
			"justifyQC %x", proposal.Block.GetHeader().PreBlockHash, proposal.JustifyQc.Votes[0].BlockId)
	}

	currentHeight, err := v.ledger.CurrentHeight()
	if err != nil {
		v.logger.Warnf("ValidateJustifyQc failed: get current height from ledgerCache failed. error:%+v", err)
		return err
	}

	if proposal.Block.Header.BlockHeight < currentHeight {
		err = fmt.Errorf("the block in height[%d] has been committed. currentHeight:%d",
			proposal.Block.Header.BlockHeight, currentHeight)
		return err
	}

	// 每个Epoch的第一个提案不再校验QC，因为再three-chain模式下，提案包含的QC
	// 已在上个世代被校验通过，且写入DB.
	if proposal.JustifyQc.Votes[0].EpochId < v.epochId && proposal.Block.Header.BlockHeight == currentHeight+1 {
		v.logger.Debugf("justify qc belong to the first proposal in a new epoch need not to verify. "+
			"blockHeight:%d", proposal.Block.Header.BlockHeight)
		return nil
	}

	// check the justify qc in proposal
	if err := v.ValidateQc(proposal.JustifyQc); err != nil {
		return fmt.Errorf("ValidateQc error: %s", err)
	}
	return nil
}

// validateMinGroupQc check that there are f+1 valid votes in the  qc
func (v *Validator) validateMinGroupQc(qc *maxbft.QuorumCert, proposal *maxbft.ProposalData) error {
	// calculate the count of votes in qc that for a block
	var (
		err        error
		reach      bool
		voteBlkNum uint64
	)
	if len(qc.Votes) != 0 &&
		(!bytes.Equal(qc.Votes[0].BlockId, proposal.Block.Header.BlockHash) ||
			qc.Votes[0].View != proposal.View ||
			qc.Votes[0].Height != proposal.Block.Header.BlockHeight) {
		err = fmt.Errorf("wrong qc[%d:%d:%x] for block[%d:%d:%x]",
			qc.Votes[0].Height, qc.Votes[0].View, qc.Votes[0].BlockId,
			proposal.Block.Header.BlockHeight, proposal.View, proposal.Block.Header.BlockHash)
		return err
	}
	voteBlkNum, err = CountNumFromVotes(qc)
	if err != nil {
		return err
	}
	minNodes := utils.GetMiniNodes(len(v.validators))
	if int(voteBlkNum) < minNodes {
		return fmt.Errorf("not enouph votes for min nodes: need at least %d votes, got %d", minNodes, voteBlkNum)
	}
	// verify the votes
	reach, err = VerifyVotes(v.ac, minNodes, qc.Votes, v.validators)
	if err != nil {
		v.logger.Warnf("verify vote failed. error: %+v", err)
	}
	if !reach {
		return fmt.Errorf("need at least %d valid votes, there is not enouph", minNodes)
	}
	return nil
}

// checkSafeNode check if the given proposal is a safe node or not
// safeNode:
//   1) qc view > locked view
//   2) qc view == locked view && qc hash == locked qc hash
// a node that meets any of these criteria is called a safeNode
func (v *Validator) checkSafeNode(qc *maxbft.QuorumCert) bool {
	var (
		qcView       = qc.Votes[0].View
		lockedView   = v.forks.GetLockedQC().View()
		qcHash       = string(qc.Votes[0].BlockId)
		lockedQcHash = string(v.forks.GetLockedQC().Hash())
	)
	isSafeNode := qcView > lockedView || qcView == lockedView && qcHash == lockedQcHash
	if !isSafeNode {
		v.logger.Warnf("not safe node proposal. QcView:[%d], LockedQcView:[%d],"+
			"QcHash:[%x], LockedQcHash:[%x]", qcView, lockedView, qcHash, lockedQcHash)
	}
	return isSafeNode
}

//verifyConsensusMsgSign verify the vote sign
func verifyConsensusMsgSign(vote *maxbftpb.VoteData, ac protocol.AccessControlProvider) error {
	if vote == nil {
		return fmt.Errorf("vote is nil")
	}
	// copy the vote without the signature information
	tmpVote := &maxbftpb.VoteData{
		BlockId: vote.BlockId,
		EpochId: vote.EpochId,
		Height:  vote.Height,
		View:    vote.View,
		Author:  vote.Author,
	}
	data, err := proto.Marshal(tmpVote)
	if err != nil {
		return fmt.Errorf("marshal vote failed, payload %v , err %v", tmpVote, err)
	}

	return verifyDataSign(data, vote.Signature, ac)
}

//verifyDataSign verify the data with EndorsementEntry, ac, org
func verifyDataSign(data []byte, signEnrty *common.EndorsementEntry,
	ac protocol.AccessControlProvider) error {
	principal, err := ac.CreatePrincipal(
		protocol.ResourceNameConsensusNode,
		[]*common.EndorsementEntry{signEnrty},
		data,
	)
	if err != nil {
		return fmt.Errorf("new principal error %v", err)
	}

	result, err := ac.VerifyPrincipal(principal)
	if err != nil {
		return fmt.Errorf("verify principal failed, error %v", err)
	}
	if !result {
		return fmt.Errorf("verify failed, result %v", result)
	}

	return nil
}

// CountNumFromVotes count votes for a qc
func CountNumFromVotes(
	qc *maxbftpb.QuorumCert) (votedBlockNum uint64, err error) {
	total := len(qc.Votes)
	if total == 0 {
		return 0, fmt.Errorf("there is no votes in qc")
	}
	var (
		author    string
		voteIdxes = make(map[string]bool, total)
		height    = qc.Votes[0].Height
		view      = qc.Votes[0].View
		blockId   = qc.Votes[0].BlockId
	)
	if blockId == nil {
		return 0, fmt.Errorf("blockId was nil")
	}
	votedBlockNum++
	// for each vote, check:
	//   1) they are for the same block height, view, block hash
	//   2) there are no vote was generated by the same author
	for i := 1; i < total; i++ {
		if qc.Votes[i] == nil {
			return 0, fmt.Errorf("nil Commits msg")
		}
		if qc.Votes[i].Height != height || qc.Votes[i].View != view {
			return 0, fmt.Errorf("vote for unequal height:view [%v:%v] with [%v:%v]",
				qc.Votes[i].Height, qc.Votes[0].View, height, view)
		}
		author = string(qc.Votes[i].GetAuthor())
		if ok := voteIdxes[author]; ok {
			return 0, fmt.Errorf("duplicate vote index [%v] at height:view [%v:%v]",
				string(qc.Votes[i].GetAuthor()), height, view)
		}
		voteIdxes[author] = true
		if qc.Votes[i].BlockId == nil || !bytes.Equal(qc.Votes[i].BlockId, blockId) {
			return 0, fmt.Errorf("vote for unequal blockId [%v] with expected [%v]",
				qc.Votes[i].BlockId, blockId)
		}
		votedBlockNum++
	}
	return votedBlockNum, nil
}

// validateVoteData Validate voting data
func validateVoteData(voteData *maxbftpb.VoteData, validators []string,
	ac protocol.AccessControlProvider) error {
	author := voteData.GetAuthor()

	if author == nil {
		return fmt.Errorf("author is nil")
	}
	found := false
	for _, v := range validators {
		if v == string(author) {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("msg index: %s not in validators, members: %v", string(author), validators)
	}

	// check cert id
	if voteData.Signature == nil || voteData.Signature.Signer == nil {
		return fmt.Errorf("signer is nil")
	}

	//check sign
	tmpVote := &maxbftpb.VoteData{
		BlockId: voteData.BlockId,
		EpochId: voteData.EpochId,
		Height:  voteData.Height,
		View:    voteData.View,
		Author:  voteData.Author,
	}
	data, err := proto.Marshal(tmpVote)
	if err != nil {
		return fmt.Errorf("marshal payload failed, err %v", err)
	}
	if err = verifyDataSign(data, voteData.Signature, ac); err != nil {
		return fmt.Errorf("verify signature failed, err %v", err)
	}
	return nil
}

// VerifyVotes validate the votes concurrently
func VerifyVotes(
	ac protocol.AccessControlProvider,
	validThreshold int,
	votes []*maxbftpb.VoteData,
	members []string) (reachThreshold bool, err error) {
	verifyPool, err := ants.NewPool(runtime.NumCPU(), ants.WithPreAlloc(true))
	if err != nil {
		return false, fmt.Errorf("create goroutine pool failed, err: %s", err)
	}
	defer verifyPool.Release()

	var (
		wg   = sync.WaitGroup{}
		errs = make([]error, 0, len(votes))
	)

	// parallel verify all the votes in the qc
	wg.Add(len(votes))
	for i := range votes {
		index := i
		if err = verifyPool.Submit(func() {
			defer wg.Done()
			if err = validateVoteData(votes[index], members, ac); err != nil {
				errs = append(errs, fmt.Errorf("%d vote validate err: %s", index, err))
			}
		}); err != nil {
			return false, fmt.Errorf("goroutine pool submit task failed, err: %s", err)
		}
	}
	wg.Wait()

	// 有效的提案数量是否满足阈值，不满足时本次投票校验失败
	reachThreshold = len(votes)-len(errs) >= validThreshold
	if len(errs) > 0 {
		return reachThreshold, fmt.Errorf(
			"verify votes error, because some vote is wrong; %v", errs)
	}
	return reachThreshold, nil
}
