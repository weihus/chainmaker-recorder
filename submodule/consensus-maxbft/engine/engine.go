/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package engine

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"time"

	conf "chainmaker.org/chainmaker/chainconf/v2"
	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/compensator"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/msgcache"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/pacemaker"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/verifier"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/vote"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/wal"
	consensusUtils "chainmaker.org/chainmaker/consensus-utils/v2"
	"chainmaker.org/chainmaker/consensus-utils/v2/consistent_service"
	"chainmaker.org/chainmaker/localconf/v2"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"

	"go.uber.org/atomic"
)

// Transceiver implements Message interface of the consistent engine
type Transceiver struct {
	msgbus.MessageBus

	receiver chan *net.NetMsg
}

// NewTransceiver initials and returns a Transceiver object
func NewTransceiver(msgBus msgbus.MessageBus, receiver chan *net.NetMsg) *Transceiver {
	return &Transceiver{
		MessageBus: msgBus,
		receiver:   receiver,
	}
}

// Send sends consistent messages to other consensus nodes
func (t *Transceiver) Send(payload interface{}) {
	t.MessageBus.Publish(msgbus.SendConsistentMsg, payload)
}

// Receive consistent messages from other consensus nodes
func (t *Transceiver) Receive() interface{} {
	return <-t.receiver
}

// Start message
func (t *Transceiver) Start() error {
	return nil
}

// Stop message
func (t *Transceiver) Stop() error {
	return nil
}

// Engine implements a MaxBft consensus engine
type Engine struct {
	// config in consensus
	chainID      string
	nodeId       string
	epochId      uint64
	epochEndView uint64
	nodes        []string
	hashType     string

	// msg channel
	closeCh          chan struct{}
	doneCh           chan struct{}
	voteCh           chan *maxbft.VoteData
	proposalCh       chan *msgcache.PendingProposal
	consistentCh     chan *net.NetMsg
	newBlockSignalCh chan bool

	// state in consensus
	voter     vote.Voter
	forest    forest.Forester
	validator verifier.Verifier
	collector vote.VotesCollector
	paceMaker pacemaker.PaceMaker
	requester compensator.RequestCompensator

	lastCommitBlock    uint64
	lastCreateProposal *maxbft.ProposalData // The latest proposal created by the node self

	// other modules
	ac            protocol.AccessControlProvider
	log           protocol.Logger
	net           protocol.NetService
	conf          protocol.ChainConf
	msgBus        msgbus.MessageBus
	singer        protocol.SigningMember
	ledgerCache   protocol.LedgerCache
	coreVerifier  protocol.BlockVerifier
	coreCommitter protocol.BlockCommitter
	coreProposer  protocol.BlockProposer
	helper        protocol.MaxbftHelper
	store         protocol.BlockchainStore
	syncService   protocol.SyncService
	wal           wal.WalAdaptor

	// wal actor status, bool
	hasReplayed bool

	// the last view in witch we have given up our proposal because of there is no tx in last 3 blocks
	lastGiveUpProposeView uint64

	// status of the consistency engine
	// Consistency engine Service
	service consistent_service.ConsistentEngine

	// indicates whether the service is started
	statusRunning bool

	// the engine loop is now running or not
	isRunning bool

	// broadcast timer set in the consistency engine service
	broadcastTiming time.Duration

	// records the time of the last status broadcast
	lastBroadcastTime map[string]time.Time

	// after the node status is updated by the consistency engine for
	// the first time, the frequency of broadcasting the status is reduced
	broadcastInterval time.Duration

	// after the node status is updated by the consistency engine for
	// the first time, the node status will be updated only when the
	// difference between the local and remotes is greater than the threshold
	updateViewInterval int64

	// indicates whether the status has been updated
	hasFirstSyncedStatus *atomic.Bool

	// remote notes information
	remotes []*RemoteNodeInfo
	// Local event processing is triggered based on the status of the peer node
	statusEventCh chan Event
}

// NewEngine initials and returns an Engine object
func NewEngine(
	log protocol.Logger,
	config *consensusUtils.ConsensusImplConfig,
	epochId uint64,
	epochEndView uint64,
	consensusNodes []string,
	voteCh chan *maxbft.VoteData,
	proposalCh chan *msgcache.PendingProposal,
	consistentCh chan *net.NetMsg,
	newBlockSignalCh chan bool,
	isRunning bool,
	hasReplayed bool,
	requester compensator.RequestCompensator,
) *Engine {
	if config.ChainConf.ChainConfig().Contract.EnableSqlSupport {
		log.Error("maxbft consensus doesn't support sql contract")
		return nil
	}

	engine := &Engine{
		nodes:        consensusNodes,
		nodeId:       config.NodeId,
		chainID:      config.ChainId,
		epochId:      epochId,
		epochEndView: epochEndView,
		hashType:     config.ChainConf.ChainConfig().Crypto.Hash,

		voteCh:           voteCh,
		proposalCh:       proposalCh,
		consistentCh:     consistentCh,
		closeCh:          make(chan struct{}),
		doneCh:           make(chan struct{}),
		newBlockSignalCh: newBlockSignalCh,
		isRunning:        isRunning,

		log:           log,
		ac:            config.Ac,
		net:           config.NetService,
		conf:          config.ChainConf,
		singer:        config.Signer,
		msgBus:        config.MsgBus,
		ledgerCache:   config.LedgerCache,
		coreVerifier:  config.Core.GetBlockVerifier(),
		coreProposer:  config.Core.GetBlockProposer(),
		coreCommitter: config.Core.GetBlockCommitter(),
		helper:        config.Core.GetMaxbftHelper(),
		syncService:   config.Sync,
		store:         config.Store,

		requester:     requester,
		hasReplayed:   hasReplayed,
		statusEventCh: make(chan Event),

		remotes: make([]*RemoteNodeInfo, 0, len(consensusNodes)),
	}
	if err := engine.initState(); err != nil {
		engine.log.Errorf("init engine state failed, reason: %s", err)
		return nil
	}
	engine.initConsistentService()
	engine.log.DebugDynamic(func() string {
		return fmt.Sprintf("last committed block [%d:%d:%x], consensus nodes:%+v",
			engine.lastCommitBlock, engine.forest.FinalView(), engine.forest.Root().Key(), consensusNodes)
	})
	return engine
}

func (e *Engine) initConsistentService() {
	transceiver := NewTransceiver(e.msgBus, e.consistentCh)
	e.service = consistent_service.NewConsistentService(e, transceiver, e.log)
	e.updateViewInterval = 3
	e.broadcastTiming = time.Second * 30
	e.broadcastInterval = time.Minute * 3
	e.lastBroadcastTime = make(map[string]time.Time)
	e.hasFirstSyncedStatus = atomic.NewBool(false)
}

// GetForest returns the forest object of Engine
func (e *Engine) GetForest() forest.Forester {
	return e.forest
}

// GetBaseCheck returns the base check function of the Engine's validator
func (e *Engine) GetBaseCheck() func(data *maxbft.ProposalData, qc *maxbft.QuorumCert) error {
	return e.validator.BaseCheck
}

// Verify Implement verification interface for configuration changes
func (e *Engine) Verify(consensusType consensus.ConsensusType, chainConfig *config.ChainConfig) error {
	if consensusType != consensus.ConsensusType_MAXBFT {
		return fmt.Errorf("only support %s consensus ", consensus.ConsensusType_MAXBFT.String())
	}
	nodes := utils.GetConsensusNodes(chainConfig)
	if len(nodes) < verifier.MinQuorumForQc+1 {
		return fmt.Errorf("the expected minimum number of"+
			" consensus nodes is %d, but will be %d after deletion", verifier.MinQuorumForQc+1, len(nodes))
	}
	return nil
}

// StartEngine starts the local consensus engine
func (e *Engine) StartEngine() error {
	_ = conf.RegisterVerifier(e.chainID, consensus.ConsensusType_MAXBFT, e)
	// 2. if last committed block has been update, will reInit the Engine.
	// because
	lastCommitBlk := e.ledgerCache.GetLastCommittedBlock()
	if lastCommitBlk.Header.BlockHeight != 0 &&
		lastCommitBlk.Header.BlockHeight > e.lastCommitBlock {
		if err := e.initState(); err != nil {
			e.log.Errorf("init engine state failed, reason: %s", err)
			return err
		}
	}

	// start the pacemaker
	e.paceMaker.Start()
	e.forest.Start()

	// replay wal when the local node restarted
	hasEntry, err := e.replayWal()
	if err != nil {
		e.log.Errorf("Start failed. replay wal error:%+v", err)
		return err
	}

	// register consensus nodes to consistent service and will receive status from these nodes.
	var info *RemoteNodeInfo
	for _, nodeId := range e.nodes {
		if nodeId != e.nodeId {
			info = NewRemoteInfo(&maxbft.NodeStatus{NodeId: nodeId}, e)
			err = e.service.PutRemoter(nodeId, info)
			e.lastBroadcastTime[nodeId] = time.Now()
			if err != nil {
				e.log.Errorf("Start failed. put remoter failed. error: %+v", err)
				return err
			}
			e.remotes = append(e.remotes, info)
		}
	}
	_ = e.service.AddBroadcaster(e.nodeId, e)
	_ = e.service.RegisterStatusCoder(int8(msgbus.RecvConsistentMsg), &Decoder{})
	if hasEntry {
		e.log.Infof("replay wal success ...")
		// 节点重启加载wal成功后，依靠replayWal中设置的超时器或其它节点发送的提案将状态向下推动，
		// 如果所有节点都重启，且wal中记录的超时器时间比较长，则长时间节点都不会有共识触发生成新提案.
		// 因此，如果节点成功加载wal后，立即触发当前视图的超时，使共识向下流转，该操作可能会导致
		// 共识节点间对本应达成一致的视图失败，通过Liveness机制在随后的视图中进行共识达成，会极大
		// 减少达成共识的时间。
		e.handleTimeout(e.paceMaker.GetMonitorEvent())
	} else {
		_, updated := e.paceMaker.UpdateWithQc(e.forest.GetGenericQC())
		if updated {
			if err = e.wal.AddNewView(); err != nil {
				e.log.Errorf("save new view information to wal failed. error:%+v", err)
				return err
			}
		} else {
			e.paceMaker.AddEvent(maxbft.ConsStateType_PACEMAKER)
		}
		e.startNewView()
	}

	go e.loop()
	if err = e.service.Start(context.Background()); err != nil {
		e.log.Errorf("start consistent engine failed. error: %+v", err)
		return err
	}

	return nil
}

// initState init Engine state from DB
func (e *Engine) initState() error {
	// 1. load last committed block from DB
	lastCommitBlk := e.ledgerCache.GetLastCommittedBlock()
	if lastCommitBlk == nil {
		return fmt.Errorf("don't get block from the ledgerCache module")
	}
	lastFinalQC := utils.GetQCFromBlock(lastCommitBlk)
	lastFinalView := utils.GetBlockView(lastCommitBlk)

	// 2. init state in the Engine
	e.lastCommitBlock = lastCommitBlk.Header.BlockHeight // new

	forks := forest.NewForest(
		forest.NewCachedForestNode(forest.NewProposalContainer(
			&maxbft.ProposalData{
				Block:     lastCommitBlk,
				View:      lastFinalView,
				JustifyQc: lastFinalQC,
			}, true, false)), e.coreCommitter, e.log)

	voter := vote.NewVoter(e.singer, lastFinalView, e.epochId, e.nodeId, e.hashType, e.log)
	validator := verifier.NewValidator(e.nodes, e.coreVerifier, e.ac, e.net, e.nodeId,
		forks, e.store, e.ledgerCache, e.epochId, e.epochEndView, e.log, e.conf)
	collector := vote.NewVotesCollector(validator, utils.GetQuorum(len(e.nodes)), e.log)
	roundTime, roundInterval, maxTime, err := getRoundTime(e.conf.ChainConfig())
	if err != nil {
		return fmt.Errorf("getRoundTime failed. error:%+v", err)
	}
	paceMaker := pacemaker.NewPaceMaker(lastFinalView, forks, roundTime, roundInterval, maxTime, e.log)
	walPath := path.Join(localconf.ChainMakerConfig.GetStorePath(), e.chainID,
		fmt.Sprintf("%s_%s", "wal", e.nodeId))
	walAdaptor, err := wal.NewLwsWalAdaptor(walPath, e.epochId, voter,
		forks, paceMaker, collector, e.coreVerifier, e.log)
	if err != nil {
		e.log.Errorf("new lws failed. error:%+v", err)
		return err
	}

	e.voter = voter
	e.forest = forks
	e.wal = walAdaptor
	e.collector = collector
	e.paceMaker = paceMaker
	e.validator = validator
	return nil
}

func (e *Engine) replayWal() (bool, error) {
	if e.hasReplayed {
		return false, nil
	}
	e.hasReplayed = true
	return e.wal.ReplayWal()
}

func (e *Engine) loop() {
	e.isRunning = true
	defer func() {
		e.isRunning = false
		close(e.doneCh)
	}()
	e.log.Infof("start event loop to process consensus msg")
	for {
		select {
		case <-e.closeCh:
			e.log.Infof("consensus exit")
			return
		case event := <-e.paceMaker.TimeoutChannel():
			e.HandleStatus()
			e.handleTimeout(event)
		case voteData := <-e.voteCh:
			e.handleVote(voteData)
		case proposal := <-e.proposalCh:
			e.handleProposal(proposal)
		default:
		}

		select {
		case <-e.closeCh:
			e.log.Infof("consensus exit")
			return
		case event := <-e.paceMaker.TimeoutChannel():
			e.HandleStatus()
			e.handleTimeout(event)
		case voteData := <-e.voteCh:
			e.handleVote(voteData)
		case proposal := <-e.proposalCh:
			e.handleProposal(proposal)
		case event := <-e.statusEventCh:
			e.handleStatusEvent(event)
		case <-e.newBlockSignalCh:
			// other signals will be processed in preference
			// to new proposal request signals from core engine
			select {
			case <-e.closeCh:
				e.log.Infof("consensus exit")
				return
			case event := <-e.paceMaker.TimeoutChannel():
				e.HandleStatus()
				e.handleTimeout(event)
			case voteData := <-e.voteCh:
				e.handleVote(voteData)
			case proposal := <-e.proposalCh:
				e.handleProposal(proposal)
			default:
				if e.isProposer() && e.hasGiveUpProposeInCurView() {
					e.log.Debugf("receive a signal from core module"+
						" to fired generate block %d", e.paceMaker.CurView())
					e.startNewView()
				}
			}
		}
	}
}

// handleStatusEvent handles events from the status service,
// when the local node's block height fell behind, we notify
// the sync module to start to sync blocks
func (e *Engine) handleStatusEvent(status Event) {
	switch status.Type() {
	case RestartSyncServiceEvent:
		e.log.Infof("trigger signal to start sync service")
		e.syncService.StartBlockSync()
	}
}

// StopEngine stops the local consensus engine
func (e *Engine) StopEngine() error {
	close(e.closeCh)
	e.forest.Stop()
	e.paceMaker.Stop()
	err := e.service.Stop(context.Background())
	// 保证后台服务已成功退出，不再处理当前世代任何消息，不再向wal中写任何数据后再关闭wal
	<-e.doneCh
	e.wal.Close()
	return err
}

// Start implement the Start function of interface StatusBroadcaster
func (e *Engine) Start() error {
	e.Run()
	return nil
}

// Stop implement the Stop function of interface StatusBroadcaster
func (e *Engine) Stop() error {
	e.statusRunning = false
	return nil
}

// DiscardBlocks discards blocks in the cache of core engine module
func (e *Engine) DiscardBlocks(height uint64) {
	e.helper.DiscardBlocks(height)
}

// GetCurrentView return the current view of the pacemaker of Engine
func (e *Engine) GetCurrentView() uint64 {
	return e.paceMaker.CurView()
}

// isProposer returns that the local node is the leader of the current view or not
func (e *Engine) isProposer() bool {
	return e.validator.GetLeader(e.paceMaker.CurView()) == e.nodeId
}

// getRoundTime loads the configurations of timeouts from the chain configurations,
// return 0 if not exists
func getRoundTime(config *config.ChainConfig) (uint64, uint64, uint64, error) {
	var (
		round, roundInterval, maxRound uint64
		err                            error
	)
	conConf := config.Consensus
	for _, oneConf := range conConf.ExtConfig {
		switch oneConf.Key {
		case utils.RoundTimeoutMill:
			round, err = strconv.ParseUint(oneConf.Value, 10, 64)
			if err != nil {
				return 0, 0, 0, fmt.Errorf("set %s Parse uint error: %s", utils.RoundTimeoutMill, err)
			}
		case utils.RoundTimeoutIntervalMill:
			roundInterval, err = strconv.ParseUint(oneConf.Value, 10, 64)
			if err != nil {
				return 0, 0, 0, fmt.Errorf("set %s Parse uint error: %s", utils.RoundTimeoutIntervalMill, err)
			}
		case utils.MaxTimeoutMill:
			maxRound, err = strconv.ParseUint(oneConf.Value, 10, 64)
			if err != nil {
				return 0, 0, 0, fmt.Errorf("set %s Parse uint error: %s", utils.MaxTimeoutMill, err)
			}
		}
	}
	return round, roundInterval, maxRound, nil
}
