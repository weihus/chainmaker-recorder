/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_maxbft

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/epoch"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/verifier"
	consensusUtils "chainmaker.org/chainmaker/consensus-utils/v2"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"
	systemPb "chainmaker.org/chainmaker/pb-go/v2/syscontract"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/gogo/protobuf/proto"
)

var (
	isStarted sync.Map
	instances sync.Map

	// to mark the current mode of consensus module
	//   - SYNC_VERIFY: the consensus module is synchronizing blocks from other nodes
	//   - CONSENSUS_VERIFY: the consensus module is participating in the consensus
	currentModes sync.Map

	consistentMsgCapacity = 1024
)

var (
	topics = [3]msgbus.Topic{msgbus.RecvConsensusMsg, msgbus.RecvConsistentMsg, msgbus.ProposeBlock}
)

// Maxbft defines consensus module object used by blockChain
type Maxbft struct {
	epoch *epoch.Epoch

	log protocol.Logger
	// receive consensus msg from network and other modules
	msgBus msgbus.MessageBus

	// when the block synchronization reaches the threshold range, the signal
	// of the sync module is fired and the consensus engine is started
	syncService protocol.SyncService

	// local node id
	nodeId string

	// chain id
	chainId string

	// consensus implement config
	config *consensusUtils.ConsensusImplConfig

	// channel for receiving consistent messages
	consistentCh chan *net.NetMsg

	// sync signal that the engine of the current epoch is running or not
	isEngineRunning bool

	// used to transmit signal that can propose new block by core engine module
	newBlockSignalCh chan bool

	// to avoid concurrent reading and writing to currentMode,
	// and avoid concurrent starting epoch
	modeLock sync.RWMutex
}

// New initials and returns a new Maxbft object
func New(cfg *consensusUtils.ConsensusImplConfig) (protocol.ConsensusEngine, error) {
	log := cfg.Logger
	maxBftKey := fmt.Sprintf("%s_%s", cfg.NodeId, cfg.ChainId)
	if started, ok := isStarted.Load(maxBftKey); ok && started.(bool) {
		if ins, ok := instances.Load(maxBftKey); ok && ins.(*Maxbft) != nil {
			log.Infof("MaxBft[%s] is already exist, need to do nothing. maxBftKey:%s",
				cfg.NodeId, maxBftKey)
			return ins.(*Maxbft), nil
		}
		isStarted.Delete(maxBftKey)
	}

	var (
		consistentCh     = make(chan *net.NetMsg, consistentMsgCapacity)
		isEngineRunning  = false
		newBlockSignalCh = make(chan bool, 1)
		isValidator      bool
	)
	governanceContract, err := utils.GetGovernanceContractFromChainStore(cfg.Store)
	if err != nil {
		return nil, err
	}
	m := &Maxbft{
		log:              log,
		msgBus:           cfg.MsgBus,
		syncService:      cfg.Sync,
		nodeId:           cfg.NodeId,
		chainId:          cfg.ChainId,
		config:           cfg,
		consistentCh:     consistentCh,
		newBlockSignalCh: newBlockSignalCh,
		isEngineRunning:  isEngineRunning,
	}
	currentModes.Store(m.chainId, protocol.SYNC_VERIFY)
	var currEpoch *epoch.Epoch
	if governanceContract == nil {
		nodes := utils.GetConsensusNodes(cfg.ChainConf.ChainConfig())
		for _, id := range nodes {
			if m.nodeId == id {
				isValidator = true
				break
			}
		}
		if isValidator {
			log.Debugf("new epoch:[%d,%d]", 0, 1)
			currEpoch = epoch.NewEpoch(cfg, log, 0, 1,
				nodes, false, consistentCh, newBlockSignalCh, isEngineRunning)
		}
	} else {
		isValidator = m.isLocalNodeInEpoch(governanceContract)
		if isValidator {
			log.Debugf("new epoch:[%d,%d] validators: %+v",
				governanceContract.EpochId, governanceContract.EndView, governanceContract.Validators)
			currEpoch = epoch.NewEpoch(cfg, log, governanceContract.EpochId,
				governanceContract.EndView, governanceContract.Validators,
				false, consistentCh, newBlockSignalCh, isEngineRunning)
		}
	}
	if isValidator && currEpoch == nil {
		return nil, fmt.Errorf("create epoch engine failed")
	}

	m.epoch = currEpoch

	m.msgBus.Register(msgbus.BlockInfo, m)
	instances.Store(maxBftKey, m)
	return m, nil
}

// Start was called by blockChain after the chain configurations were modified,
// we encapsulate the actual startup behavior to avoid repeated startup of MaxBft
func (m *Maxbft) Start() error {
	maxBftKey := fmt.Sprintf("%s_%s", m.nodeId, m.chainId)
	if started, ok := isStarted.Load(maxBftKey); ok && started.(bool) {
		m.log.Infof("MaxBft[%s] is already started, need to do nothing", m.nodeId)
		return nil
	}
	m.start()
	isStarted.Store(maxBftKey, true)
	return nil
}

// Stop was called by blockChain after the chain configurations were modified,
// we do nothing here to give control of the consensus module to itself
func (m *Maxbft) Stop() error {
	// do nothing
	return nil
}

// OnMessage The message received from msgBus is distributed to the
// consensus engine for processing
func (m *Maxbft) OnMessage(message *msgbus.Message) {
	// 1. 接收到网络中其它节点的共识消息，进行处理
	if message.Topic == msgbus.RecvConsensusMsg {
		if msg, ok := message.Payload.(*net.NetMsg); ok {
			m.epoch.HandleConsensusMsg(msg)
		}
		return
	}
	// 2. 接收到core模块发送的区块提交信号，尝试进行世代切换
	if message.Topic == msgbus.BlockInfo {
		if info, ok := message.Payload.(*common.BlockInfo); ok {
			m.log.Debugf("receive a message about BlockInfo [%d:%x]",
				info.Block.Header.BlockHeight, info.Block.Header.BlockHash)
			m.handleBlockInfo(info.Block)
		}
		return
	}
	// 3. 接收到一致性引擎同步的其它节点状态信息
	if message.Topic == msgbus.RecvConsistentMsg {
		m.log.Debugf("receive a message about RecvConsistentMsg")
		if msg, ok := message.Payload.(*net.NetMsg); ok {
			select {
			case m.consistentCh <- msg:
				return
			default:
			}
		}
		return
	}
	// 4. 接收到core模块发送的生成区块信号
	if message.Topic == msgbus.ProposeBlock {
		m.log.Debugf("receive a message about ProposeBlock from core engine")
		if msg, ok := message.Payload.(*maxbft.ProposeBlock); ok && msg.IsPropose {
			select {
			case m.newBlockSignalCh <- msg.IsPropose:
				return
			default:
			}
		} else {
			m.log.Warnf("receive message not a ProposeBlock or IsPropose is not true %+v", message.Payload)
		}
		return
	}
}

// OnQuit implements the Subscriber interface in message bus
func (m *Maxbft) OnQuit() {
	// nil
}

func (m *Maxbft) handleBlockInfo(block *common.Block) {
	var err error

	defer func() {
		if block != nil && err != nil {
			m.log.Warnf("handle BlockInfo from core module failed. error:%+v", err)
		}
	}()

	// get consensus arguments from the block
	args := new(consensus.BlockHeaderConsensusArgs)
	if err = proto.Unmarshal(block.Header.ConsensusArgs, args); err != nil {
		err = fmt.Errorf("unmarshal consensus args failed. error:%+v", err)
		return
	}

	// get the governanceContract from the txWrite
	contractName := systemPb.SystemContract_GOVERNANCE.String()
	if args.ConsensusData == nil || len(args.ConsensusData.TxWrites) == 0 ||
		args.ConsensusData.TxWrites[0].ContractName != contractName {
		// there is no governance contract information in the block, need not to switch epoch
		return
	}

	m.modeLock.Lock()
	defer m.modeLock.Unlock()

	// get governance contract from the block, to get the configurations of the next epoch
	governanceContract := new(maxbft.GovernanceContract)
	if err = proto.Unmarshal(args.ConsensusData.TxWrites[0].GetValue(), governanceContract); err != nil {
		err = fmt.Errorf("unmarshal txWrites value failed. error:%+v", err)
		return
	}

	// Fix: can't do that;
	// Because the new generation needs to be based on the latest chainConfig
	// when it is created, only elements injected by the external module
	// can get the latest chainConfig which will be committed.
	//m.config.ChainConf.(*chainconf.ChainConf).ChainConf = governanceContract.ChainConfig

	// check if the local node is in the new epoch's validators
	isInNewEpoch := m.isLocalNodeInEpoch(governanceContract)

	// local node is a sync node in current epoch
	if m.epoch == nil {
		// still a sync node in the new epoch
		if !isInNewEpoch {
			return
		}

		// switch to a consensus node, so construct the epoch, but we should not start it now,
		// we will start it after got a signal from sync module in m.start().
		m.log.Infof("local node is switching to a consensus node. epochId:%d, endView:%d",
			governanceContract.EpochId, governanceContract.EndView)

		m.epoch = epoch.NewEpoch(m.config,
			m.log,
			governanceContract.EpochId,
			governanceContract.EndView,
			governanceContract.Validators,
			true,
			m.consistentCh,
			m.newBlockSignalCh,
			m.isEngineRunning)
		return
	}

	// local node is a consensus node, so stop the old epoch.
	// discard the blocks after the committed height
	if err = m.stop(); err != nil {
		err = fmt.Errorf("stop maxbft failed. error: %+v", err)
		return
	}
	m.epoch.DiscardBlocks(block.Header.BlockHeight)
	m.epoch = nil

	currentMode, err := getCurrentMode(m.chainId)
	if err != nil {
		m.log.Errorf("start maxBft failed, error: %+v", err)
		return
	}

	// switch to a sync node
	if !isInNewEpoch {
		m.log.Infof("local node is switching to a sync node")
		// if the consensus module is synchronizing blocks, not need to start synchronizing again
		if currentMode == protocol.CONSENSUS_VERIFY {
			currentModes.Store(m.chainId, protocol.SYNC_VERIFY)
			m.syncService.StartBlockSync()
		}
		return
	}

	// construct and start the new epoch
	m.log.Infof("step in new epoch:%+v", governanceContract)
	m.epoch = epoch.NewEpoch(m.config, m.log, governanceContract.EpochId,
		governanceContract.EndView, governanceContract.Validators,
		true, m.consistentCh, m.newBlockSignalCh, m.isEngineRunning)

	// the previous epoch is not nil and the consensus module is synchronizing blocks,
	// we need not to start.
	if currentMode == protocol.CONSENSUS_VERIFY {
		for _, tp := range topics {
			m.msgBus.Register(tp, m)
		}
		m.epoch.Start()
	}
}

func (m *Maxbft) start() {
	go func() {
		m.log.Infof("start maxbft engine, first receive block by sync module")
		// wait for data synchronization from the sync module to be completed
		for range m.syncService.ListenSyncToIdealHeight() {
			m.log.Infof("receive block is done by sync module")
			// If the local node switched to a sync node during
			// synchronizing blocks, the epoch would changed to nil
			m.modeLock.Lock()
			currentMode, err := getCurrentMode(m.chainId)
			if err != nil {
				m.log.Errorf("start maxBft failed, error: %+v", err)
				continue
			}
			//
			if currentMode == protocol.CONSENSUS_VERIFY {
				m.syncService.StopBlockSync()
			}
			//
			if currentMode == protocol.SYNC_VERIFY && m.epoch != nil && !m.isEngineRunning {
				m.syncService.StopBlockSync()
				// start the consensus engine when most blocks have been synchronized to the node
				// by sync module, the consensus module will be responsible for pulling the
				// blocks that have not been synchronized in the remaining critical region.
				m.log.Infof("start the consensus processing process ")
				currentModes.Store(m.chainId, protocol.CONSENSUS_VERIFY)
				for _, tp := range topics {
					m.msgBus.Register(tp, m)
				}
				m.epoch.Start()
			}
			m.modeLock.Unlock()
		}
	}()
}

func (m *Maxbft) stop() error {
	for _, tp := range topics {
		m.msgBus.UnRegister(tp, m)
	}
	m.epoch.Stop()
	return nil
}

func (m *Maxbft) isLocalNodeInEpoch(contract *maxbft.GovernanceContract) bool {
	for _, validator := range contract.Validators {
		if m.nodeId == validator {
			return true
		}
	}
	return false
}

// VerifyBlockSignatures Implement callback functions for core modules is
// used for consensus checking of the block which receive block from the sync module.
func VerifyBlockSignatures(chainConf protocol.ChainConf, ac protocol.AccessControlProvider,
	store protocol.BlockchainStore, block *common.Block, ledger protocol.LedgerCache) error {

	// The validation request is from the consensus module itself, need not to verify.
	// Otherwise, the block may not pass the validation because it has not been
	// committed yet and lack the necessary information, such as ExtraData.
	//
	// On the other hand, in order to avoid the transaction timeout check of the core
	// engine module, some validation requests from the consensus module also need to
	// use synchronous validation mode, such as when replaying WAL and validating
	// history proposals.
	currentMode, err := getCurrentMode(chainConf.ChainConfig().ChainId)
	if err != nil {
		return err
	}
	if currentMode == protocol.CONSENSUS_VERIFY {
		return nil
	}

	// 1. base info check
	if block.AdditionalData == nil ||
		len(block.AdditionalData.ExtraData) <= 0 {
		return errors.New("nil block or nil additionalData or empty extraData")
	}
	if len(block.Header.ConsensusArgs) == 0 {
		return errors.New("nil consensus args in BlockHeader")
	}

	// 2. check view and blockId
	view := utils.GetBlockView(block)
	blockQc := utils.GetQCFromBlock(block)
	if blockQc.Votes[0].View != view {
		return fmt.Errorf("invalid block due to mismatched"+
			" views, header view: %d, qc view: %d", view, blockQc.Votes[0].View)
	}
	if !bytes.Equal(blockQc.Votes[0].BlockId, block.Header.BlockHash) {
		return fmt.Errorf("not equal blockHash in BlockHeader[%x] and qc[%x]",
			block.Header.BlockHash, blockQc.Votes[0].BlockId)
	}

	// get current governance contract from chainStore
	governanceContract, err := utils.GetGovernanceContractFromChainStore(store)
	if err != nil {
		return fmt.Errorf("get governance contract from chainStore failed. error: %+v", err)
	}

	voteBlkNum, err := verifier.CountNumFromVotes(blockQc)
	if err != nil {
		return err
	}
	var validators []string

	// there is no governance contract when the chain was first started
	if governanceContract != nil {
		validators = governanceContract.Validators
	} else {
		validators = utils.GetConsensusNodes(chainConf.ChainConfig())
	}

	// 3. check votes in block
	if uint64(utils.GetQuorum(len(validators))) > voteBlkNum {
		return fmt.Errorf("not enough votes in the qc of the block ")
	}
	_, err = verifier.VerifyVotes(
		ac, len(blockQc.Votes), blockQc.Votes, validators)
	return err
}

func getCurrentMode(chainId string) (protocol.VerifyMode, error) {
	var currentMode protocol.VerifyMode
	if mode, ok := currentModes.Load(chainId); ok {
		if currentMode, ok = mode.(protocol.VerifyMode); !ok {
			return protocol.SYNC_VERIFY, fmt.Errorf("current mode of maxBft[%s] is invalid", chainId)
		}
	} else {
		currentMode = protocol.SYNC_VERIFY
		currentModes.Store(chainId, currentMode)
	}
	return currentMode, nil
}
