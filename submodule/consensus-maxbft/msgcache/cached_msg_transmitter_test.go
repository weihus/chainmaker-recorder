/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgcache

import (
	"fmt"
	"testing"
	"time"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	forest_mock "chainmaker.org/chainmaker/consensus-maxbft/v2/forest/mock"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2/test"

	"github.com/stretchr/testify/require"
)

func TestMsgCacheHandler(t *testing.T) {
	blockCache := forest_mock.NewBlockCacheMock()
	// 创建forest，内置创世区块
	cachedForest := forest.NewForest(forest.NewCachedForestNode(forest.GenesisProposalContainer()), blockCache, test.NewTestLogger(t))
	// 检查其信息是否准确
	require.Equal(t, "0", cachedForest.Root().Data().Key())
	// 添加区块1
	proposalBlk1 := forest.NewProposalData(0, 1, 1, "1", "0")
	err := cachedForest.AddProposal(proposalBlk1, false)
	require.NoError(t, err)
	commitBlocks, err := cachedForest.UpdateStatesByProposal(proposalBlk1, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块2
	proposalBlk2 := forest.NewProposalData(1, 2, 2, "2", "1")
	_ = cachedForest.AddProposal(proposalBlk2, false)
	commitBlocks, err = cachedForest.UpdateStatesByProposal(proposalBlk2, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块3
	proposalBlk3 := forest.NewProposalData(2, 3, 3, "3", "2")
	_ = cachedForest.AddProposal(proposalBlk3, false)
	commitBlocks, err = cachedForest.UpdateStatesByProposal(proposalBlk3, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 创建msgcache
	engineChan := make(chan *PendingProposal)
	go func() {
		// 监听channel
		for pData := range engineChan {
			// 放入到forest中
			fmt.Println("read data from engine channel")
			pErr := cachedForest.AddProposal(pData.Proposal, false)
			var data []*forest.ProposalContainer
			data, err = cachedForest.UpdateStatesByProposal(pData.Proposal, true)
			require.NoError(t, err)
			fmt.Println(data)
			fmt.Println(pErr)
		}
	}()
	msgCacheHandler := NewMsgCacheHandler(cachedForest, func(data *maxbft.ProposalData, qc *maxbft.QuorumCert) error {
		return nil
	}, engineChan, nil, test.NewTestLogger(t))
	// 添加区块4
	proposalBlk4 := forest.NewProposalData(3, 4, 4, "4", "3")
	err = msgCacheHandler.HandleProposal(&PendingProposal{
		FromOtherNodes: false,
		Proposal:       proposalBlk4,
	})
	if err != nil {
		fmt.Println(err)
	}
	// 添加区块5
	proposalBlk5 := forest.NewProposalData(4, 5, 5, "5", "4")
	err = msgCacheHandler.HandleProposal(&PendingProposal{
		FromOtherNodes: false,
		Proposal:       proposalBlk5,
	})
	if err != nil {
		fmt.Println(err)
	}
	// 最长等待
	te := time.NewTimer(2 * time.Second)
	defer te.Stop()
	<-te.C
	// 期待可以打印两个block
	for _, blk := range blockCache.Blocks() {
		fmt.Printf("Height = %d, Hash = %s, PreHash = %s\n", blk.Header.BlockHeight, blk.Header.BlockHash, blk.Header.PreBlockHash)
	}
}
