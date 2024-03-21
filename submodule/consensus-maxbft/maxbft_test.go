package consensus_maxbft

import (
	"fmt"
	"os"
	"testing"

	"chainmaker.org/chainmaker/pb-go/v2/accesscontrol"

	utils2 "chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/pb-go/v2/net"

	systemPb "chainmaker.org/chainmaker/pb-go/v2/syscontract"

	"chainmaker.org/chainmaker/common/v2/msgbus"

	"chainmaker.org/chainmaker/common/v2/crypto"

	"chainmaker.org/chainmaker/utils/v2"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"

	"github.com/stretchr/testify/require"

	"chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/protocol/v2/test"

	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"

	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/protocol/v2/mock"
	"github.com/golang/mock/gomock"

	consensusUtils "chainmaker.org/chainmaker/consensus-utils/v2"
)

func newBlocks(num int) []*consensus.ProposalBlock {
	blks := make([]*consensus.ProposalBlock, num)
	parentHash := []byte("genesis block")
	for i := 0; i < num; i++ {
		tx := &common.Transaction{Payload: &common.Payload{TxId: utils.GetRandTxId()}}
		blks[i] = &consensus.ProposalBlock{
			Block: &common.Block{
				Header: &common.BlockHeader{
					BlockHeight:  uint64(i + 1),
					TxCount:      1,
					BlockHash:    []byte(fmt.Sprintf("%d_block", i+1)),
					PreBlockHash: parentHash,
				},
				Txs: []*common.Transaction{tx},
			},
		}
		blks[i].CutBlock = blks[i].Block
		parentHash = blks[i].Block.Header.BlockHash
	}
	return blks
}

type mockHelper struct {
	signalCh chan struct{}
}

func newMockHelper() *mockHelper {
	return &mockHelper{signalCh: make(chan struct{})}
}

func (m *mockHelper) DiscardBlocks(baseHeight uint64) {
	go func() {
		m.signalCh <- struct{}{}
	}()
}

type mockMsgBus struct {
}

func (m *mockMsgBus) Register(topic msgbus.Topic, sub msgbus.Subscriber) {
}
func (m *mockMsgBus) UnRegister(topic msgbus.Topic, sub msgbus.Subscriber) {
}
func (m *mockMsgBus) Publish(topic msgbus.Topic, payload interface{}) {
}
func (m *mockMsgBus) PublishSafe(topic msgbus.Topic, payload interface{}) {
}
func (m *mockMsgBus) PublishSync(topic msgbus.Topic, payload interface{}) {
}
func (m mockMsgBus) Close() {
}

type mockMember struct {
	protocol.Member
	member string
}

func (m *mockMember) GetMemberId() string {
	return m.member
}

func Test_MissLastProposal_InEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer func() {
		ctrl.Finish()
		os.RemoveAll("wal_node1")
	}()
	cfg := &consensusUtils.ConsensusImplConfig{}
	chainConf := mock.NewMockChainConf(ctrl)
	chainConf.EXPECT().ChainConfig().AnyTimes().Return(&config.ChainConfig{
		Consensus: &config.ConsensusConfig{Nodes: []*config.OrgConfig{
			{NodeId: []string{"node1", "node2", "node3", "node4"}},
		}},
		Contract: &config.ContractConfig{EnableSqlSupport: false},
		Crypto:   &config.CryptoConfig{Hash: crypto.CRYPTO_ALGO_SHA256},
	})
	syncCh := make(chan struct{})
	sync := mock.NewMockSyncService(ctrl)
	sync.EXPECT().StartBlockSync().DoAndReturn(func() {
		syncCh <- struct{}{}
	}).AnyTimes()
	sync.EXPECT().StopBlockSync().AnyTimes()
	syncSignalCh := make(chan struct{})
	sync.EXPECT().ListenSyncToIdealHeight().AnyTimes().Return(syncSignalCh)
	store := mock.NewMockBlockchainStore(ctrl)
	store.EXPECT().PutBlock(gomock.Any(), gomock.Any()).AnyTimes()
	// todo. will special return value
	store.EXPECT().ReadObject(gomock.Any(), gomock.Any()).DoAndReturn(func(contractName string,
		key []byte) ([]byte, error) {
		if contractName == systemPb.SystemContract_CHAIN_CONFIG.String() {
			return utils2.MustMarshal(chainConf.ChainConfig()), nil
		}
		return nil, nil
	}).AnyTimes()
	verifyCh := make(chan struct{})
	coreVerifier := mock.NewMockBlockVerifier(ctrl)
	coreVerifier.EXPECT().VerifyBlockSync(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(
		block *common.Block, mode protocol.VerifyMode) (*consensus.VerifyResult, error) {
		fmt.Printf("------verify %d block -------\n", block.Header.BlockHeight)
		verifyCh <- struct{}{}
		return &consensus.VerifyResult{VerifiedBlock: block}, nil
	})
	proposals := newBlocks(4)
	coreProposer := mock.NewMockBlockProposer(ctrl)
	coreProposer.EXPECT().ProposeBlock(gomock.Any()).AnyTimes().DoAndReturn(func(
		proposal *maxbft.BuildProposal) (*consensus.ProposalBlock, error) {
		fmt.Printf("------propose %d block -------\n", proposal.Height)
		return proposals[proposal.Height-1], nil
	})
	//commitCh := make(chan struct{})
	coreCommitter := mock.NewMockBlockCommitter(ctrl)
	coreCommitter.EXPECT().AddBlock(gomock.Any()).DoAndReturn(func(blk *common.Block) error {
		fmt.Printf("---------- commit %d block -------\n", blk.Header.BlockHeight)
		return nil
	}).AnyTimes()
	coreEngine := mock.NewMockCoreEngine(ctrl)
	coreEngine.EXPECT().GetBlockProposer().AnyTimes().Return(coreProposer)
	coreEngine.EXPECT().GetBlockVerifier().AnyTimes().Return(coreVerifier)
	coreEngine.EXPECT().GetBlockCommitter().AnyTimes().Return(coreCommitter)
	helper := newMockHelper()
	coreEngine.EXPECT().GetMaxbftHelper().AnyTimes().Return(helper)
	ledger := mock.NewMockLedgerCache(ctrl)
	genesisBlk := &common.Block{
		Header: &common.BlockHeader{BlockHeight: 0, BlockHash: []byte("genesis block")},
	}
	ledger.EXPECT().GetLastCommittedBlock().AnyTimes().DoAndReturn(func() *common.Block {
		return genesisBlk
	})

	ledger.EXPECT().CurrentHeight().Return(uint64(0), nil).AnyTimes()
	mockAc := mock.NewMockAccessControlProvider(ctrl)
	mockAc.EXPECT().NewMember(gomock.Any()).DoAndReturn(func(info *accesscontrol.Member) (protocol.Member, error) {
		return &mockMember{member: string(info.MemberInfo)}, nil
	}).AnyTimes()
	mockAc.EXPECT().CreatePrincipal(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockAc.EXPECT().VerifyPrincipal(gomock.Any()).Return(true, nil).AnyTimes()
	mockNet := mock.NewMockNetService(ctrl)
	mockNet.EXPECT().GetNodeUidByCertId(gomock.Any()).DoAndReturn(func(certId string) (string, error) {
		return certId, nil
	}).AnyTimes()
	mockSigner := mock.NewMockSigningMember(ctrl)
	mockSigner.EXPECT().Sign(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockSigner.EXPECT().GetMember().Return(nil, nil).AnyTimes()

	cfg.NodeId = "node1"
	cfg.Ac = mockAc
	cfg.Sync = sync
	cfg.Store = store
	cfg.Core = coreEngine
	cfg.Signer = mockSigner
	cfg.NetService = mockNet
	cfg.ChainConf = chainConf
	cfg.LedgerCache = ledger
	cfg.MsgBus = &mockMsgBus{}
	cfg.Logger = test.NewTestLogger(t)

	// 2. 创建、启动共识引擎
	engine, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer func() {
		require.NoError(t, engine.Stop())
	}()
	syncSignalCh <- struct{}{}

	// 3. 接收到区块1进行处理
	implConsensus := engine.(*Maxbft)
	rwSet, err := utils2.GetGovernanceContractTxRWSet(&maxbft.GovernanceContract{
		EpochId:     1,
		EndView:     101,
		Validators:  []string{"node1", "node2", "node3", "node4"},
		ChainConfig: chainConf.ChainConfig(),
	})
	require.NoError(t, err)
	proposals[0].Block.Header.ConsensusArgs = utils2.MustMarshal(&consensus.BlockHeaderConsensusArgs{
		ConsensusType: int64(consensus.ConsensusType_MAXBFT),
		View:          proposals[0].Block.Header.BlockHeight,
		ConsensusData: rwSet,
	})
	proposals[0].Block.Header.PreBlockHash = []byte("genesis block")
	proposals[0].Block.Header.Proposer = &accesscontrol.Member{MemberInfo: []byte("node2")}
	implConsensus.OnMessage(&msgbus.Message{
		Topic: msgbus.RecvConsensusMsg,
		Payload: &net.NetMsg{
			Type: net.NetMsg_CONSENSUS_MSG,
			To:   "node1",
			Payload: utils2.MustMarshal(&maxbft.ConsensusMsg{Type: maxbft.MessageType_PROPOSAL_MESSAGE,
				Payload: utils2.MustMarshal(&maxbft.ProposalData{
					Block:     proposals[0].Block,
					View:      proposals[0].Block.Header.BlockHeight,
					Proposer:  "node2",
					JustifyQc: utils2.GetQCFromBlock(ledger.GetLastCommittedBlock()),
				}),
			}),
		},
	})
	<-verifyCh

	// 4. 接收到区块2与区块1的QC进行处理
	qcForBlock1 := &maxbft.QuorumCert{
		Votes: []*maxbft.VoteData{
			{BlockId: proposals[0].Block.Header.BlockHash, Height: 1, View: 1, Author: []byte("node1"),
				Signature: &common.EndorsementEntry{Signer: &accesscontrol.Member{MemberInfo: []byte("node1")}}},
			{BlockId: proposals[0].Block.Header.BlockHash, Height: 1, View: 1, Author: []byte("node2"),
				Signature: &common.EndorsementEntry{Signer: &accesscontrol.Member{MemberInfo: []byte("node2")}}},
			{BlockId: proposals[0].Block.Header.BlockHash, Height: 1, View: 1, Author: []byte("node3"),
				Signature: &common.EndorsementEntry{Signer: &accesscontrol.Member{MemberInfo: []byte("node3")}}},
		},
	}
	proposals[1].Block.Header.Proposer = &accesscontrol.Member{MemberInfo: []byte("node3")}
	proposals[1].Block.Header.ConsensusArgs = utils2.MustMarshal(&consensus.BlockHeaderConsensusArgs{
		ConsensusType: int64(consensus.ConsensusType_MAXBFT),
		View:          proposals[1].Block.Header.BlockHeight,
		ConsensusData: rwSet,
	})
	implConsensus.OnMessage(&msgbus.Message{
		Topic: msgbus.RecvConsensusMsg,
		Payload: &net.NetMsg{
			Type: net.NetMsg_CONSENSUS_MSG,
			To:   "node1",
			Payload: utils2.MustMarshal(&maxbft.ConsensusMsg{Type: maxbft.MessageType_PROPOSAL_MESSAGE,
				Payload: utils2.MustMarshal(&maxbft.ProposalData{
					Block:     proposals[1].Block,
					View:      proposals[1].Block.Header.BlockHeight,
					Proposer:  "node3",
					JustifyQc: qcForBlock1,
				}),
			}),
		},
	})
	// Note: 此处不再发送其它的区块给节点，模拟一个世代丢失区块导致无法触发世代切换的情况
	<-verifyCh

	// 5. 接收到其它节点处于下个世代E2 的状态
	implConsensus.OnMessage(&msgbus.Message{
		Topic: msgbus.RecvConsistentMsg,
		Payload: &net.NetMsg{
			Type: net.NetMsg_CONSISTENT_MSG,
			Payload: utils2.MustMarshal(&maxbft.NodeStatus{
				Epoch:  1,
				NodeId: "node2",
			}),
		},
	})
	<-syncCh
	// 6. 模拟同步模块提交世代E1的最后一个区块，触发节点进行世代切换
	proposals[0].Block.AdditionalData = &common.AdditionalData{ExtraData: make(map[string][]byte)}
	proposals[0].Block.AdditionalData.ExtraData[utils2.MaxbftQC] = utils2.MustMarshal(qcForBlock1)
	genesisBlk = proposals[0].Block
	implConsensus.OnMessage(&msgbus.Message{
		Topic: msgbus.BlockInfo,
		Payload: &common.BlockInfo{
			Block: proposals[0].Block,
		},
	})
	<-helper.signalCh
}
