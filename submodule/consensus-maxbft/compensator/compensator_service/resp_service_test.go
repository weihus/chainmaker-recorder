/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package compensator_service

import (
	"testing"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	forest_mock "chainmaker.org/chainmaker/consensus-maxbft/v2/forest/mock"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2/test"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

type MockNetSender struct {
	Queue []*net.NetMsg
}

func (m *MockNetSender) OnMessage(message *msgbus.Message) {
	if message.Topic == msgbus.SendConsensusMsg {
		if msg, ok := message.Payload.(*net.NetMsg); ok {
			m.Queue = append(m.Queue, msg)
		}
		return
	}
}

func (m *MockNetSender) OnQuit() {

}

func TestRespService_HandleRequest(t *testing.T) {
	blockCache := forest_mock.NewBlockCacheMock()
	// 创建forest
	fork := forest.NewForest(forest.NewCachedForestNode(forest.GenesisProposalContainer()),
		blockCache, test.NewTestLogger(t))
	// 检查其信息是否准确
	require.Equal(t, "0", fork.Root().Data().Key())

	var (
		parentBlockView = []uint64{0, 1, 1, 2, 3, 3, 4, 5, 5, 6}
		curBlkView      = []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		blkHeight       = []uint64{1, 2, 2, 3, 3, 3, 4, 4, 4, 4}
		blkHash         = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
		preBlkHash      = []string{"0", "1", "1", "2", "3", "3", "4", "5", "5", "6"}
	)

	// add blocks
	for i := 0; i < 10; i++ {
		proposalBlk1 := forest.NewProposalData(parentBlockView[i], curBlkView[i],
			blkHeight[i], blkHash[i], preBlkHash[i])
		_, _ = fork.UpdateStatesByProposal(proposalBlk1, true)
		_ = fork.AddProposal(proposalBlk1, false)
	}

	msgBus := msgbus.NewMessageBus()
	sender := &MockNetSender{
		Queue: make([]*net.NetMsg, 0),
	}
	msgBus.Register(msgbus.SendConsensusMsg, sender)
	respSvr := NewRespService(fork, "node1", msgBus, nil, test.NewTestLogger(t), nil, nil)

	req := &maxbft.ProposalFetchMsg{
		Height:    3,
		View:      0,
		BlockId:   []byte{},
		Requester: []byte("node2"),
	}

	respSvr.HandleRequest(req)

	consensusMsg := new(maxbft.ConsensusMsg)
	resp := new(maxbft.ProposalRespMsg)

	time.Sleep(100 * time.Millisecond)

	for _, toSendMessage := range sender.Queue {
		err := proto.Unmarshal(toSendMessage.Payload, consensusMsg)
		require.Nil(t, err)
		err = proto.Unmarshal(consensusMsg.Payload, resp)
		require.Nil(t, err)
		require.Equal(t, resp.Proposal.Block.Header.BlockHeight, uint64(3))
		require.Equal(t, string(resp.Responser), "node1")
	}

	sender.Queue = make([]*net.NetMsg, 0)
	req = &maxbft.ProposalFetchMsg{
		Height:    0,
		View:      0,
		BlockId:   []byte("7"),
		Requester: []byte("node2"),
	}
	respSvr.HandleRequest(req)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, len(sender.Queue), 1)

	toSendMessage := sender.Queue[0]
	err := proto.Unmarshal(toSendMessage.Payload, consensusMsg)
	require.Nil(t, err)
	err = proto.Unmarshal(consensusMsg.Payload, resp)
	require.Nil(t, err)
	require.Equal(t, string(resp.Proposal.Block.Header.BlockHash), "7")
	require.Equal(t, string(resp.Responser), "node1")
}
