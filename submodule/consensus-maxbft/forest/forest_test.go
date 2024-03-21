/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

import (
	"fmt"
	"testing"
	"time"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"

	"chainmaker.org/chainmaker/protocol/v2/test"

	forest_mock "chainmaker.org/chainmaker/consensus-maxbft/v2/forest/mock"

	"github.com/stretchr/testify/require"
)

func TestSampleForest(t *testing.T) {
	blockCache := forest_mock.NewBlockCacheMock()
	// 创建forest
	forest := NewForest(NewCachedForestNode(GenesisProposalContainer()), blockCache, test.NewTestLogger(t))
	// 检查其信息是否准确
	require.Equal(t, "0", forest.Root().Data().Key())
	// 添加区块1
	proposalBlk1 := NewProposalData(0, 1, 1, "1", "0")
	err := forest.AddProposal(proposalBlk1, false)
	require.Nil(t, err) // 返回nil
	commitBlocks, err := forest.UpdateStatesByProposal(proposalBlk1, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块2
	proposalBlk2 := NewProposalData(1, 2, 2, "2", "1")
	_ = forest.AddProposal(proposalBlk2, false)
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk2, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块3
	proposalBlk3 := NewProposalData(2, 3, 3, "3", "2")
	_ = forest.AddProposal(proposalBlk3, false)
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk3, true)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块4
	proposalBlk4 := NewProposalData(3, 4, 4, "4", "3")
	_ = forest.AddProposal(proposalBlk4, false)
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk4, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitBlocks))

	// 添加区块5
	proposalBlk5 := NewProposalData(4, 5, 5, "5", "4")
	_ = forest.AddProposal(proposalBlk5, false)
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk5, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitBlocks))

	// 最长等待
	te := time.NewTimer(2 * time.Second)
	defer te.Stop()
	<-te.C

	for _, blk := range blockCache.Blocks() {
		fmt.Printf("Height = %d, Hash = %s, PreHash = %s\n", blk.Header.BlockHeight, blk.Header.BlockHash, blk.Header.PreBlockHash)
	}
}

func TestComplexForest(t *testing.T) {
	//	 <- blk1(1)	  <- blk2(4)   <- blk3(8)  <- blk4(11)
	// 0 <- blk1'(2)  <- blk2'(5)  <- blk3'(6) <- blk4'(7) <- blk5'(10)
	//   <- blk1''(3) <- blk2''(9) <-end.		  blk4'(7) <- blk5(12) <- blk6(13) <- blk7(14) <- blk8(15)
	blockCache := forest_mock.NewBlockCacheMock()
	// 创建forest
	forest := NewForest(NewCachedForestNode(GenesisProposalContainer()), blockCache, test.NewTestLogger(t))
	// 检查其信息是否准确
	require.Equal(t, "0", forest.Root().Data().Key())
	// 添加区块1
	proposalBlk1 := NewProposalData(0, 1, 1, "1", "0")
	commitBlocks, err := forest.UpdateStatesByProposal(proposalBlk1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk1, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	addProposals1(t, forest)

	for _, blk := range blockCache.Blocks() {
		fmt.Printf("Height = %d, Hash = %s, PreHash = %s\n", blk.Header.BlockHeight, blk.Header.BlockHash, blk.Header.PreBlockHash)
	}

	// 判断状态是否正确
	fmt.Printf("FinalQC hash = %s, height = %d\n", forest.GetFinalQC().block.Header.BlockHash, utils.GetHeightFromQC(forest.GetFinalQC().qc))
	fmt.Printf("LockedQC hash = %s, height = %d\n", forest.GetLockedQC().block.Header.BlockHash, utils.GetHeightFromQC(forest.GetLockedQC().qc))
	fmt.Printf("GenericQC hash = %s, height = %d\n", utils.GetBlockIdFromQC(forest.GetGenericQC()), utils.GetHeightFromQC(forest.GetGenericQC()))

	addProposals2(t, forest)

	fmt.Println("==========================================")
	for _, blk := range blockCache.Blocks() {
		fmt.Printf("Height = %d, Hash = %s, PreHash = %s\n", blk.Header.BlockHeight, blk.Header.BlockHash, blk.Header.PreBlockHash)
	}
	fmt.Println("==========================================")
	forest.nodeCache.hash2Nodes.Range(func(key, value interface{}) bool {
		nd := value.(ForestNoder)
		fmt.Printf("height = %d, hash = %s\n", nd.Data().block.block.Header.BlockHeight, nd.Data().block.Hash())
		return true
	})

	// 写入比之前小的视图
	proposalBlk6_1 := NewProposalData(12, 13, 6, "6'", "5")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk6_1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk6_1, false)
	require.Error(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 最长等待
	te := time.NewTimer(2 * time.Second)
	defer te.Stop()
	<-te.C
}

func addProposals1(t *testing.T, forest *CachedForest) {
	// 添加区块1'
	proposalBlk1_1 := NewProposalData(0, 2, 1, "1'", "0")
	commitBlocks, err := forest.UpdateStatesByProposal(proposalBlk1_1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk1_1, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块1''
	proposalBlk1_1_1 := NewProposalData(0, 3, 1, "1''", "0")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk1_1_1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk1_1_1, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块2
	proposalBlk2 := NewProposalData(1, 4, 2, "2", "1")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk2, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk2, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块2'
	proposalBlk2_1 := NewProposalData(2, 5, 2, "2'", "1'")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk2_1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk2_1, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块3'
	proposalBlk3_1 := NewProposalData(5, 6, 3, "3'", "2'")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk3_1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk3_1, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块4'
	proposalBlk4_1 := NewProposalData(6, 7, 4, "4'", "3'")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk4_1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk4_1, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitBlocks))
	require.EqualValues(t, commitBlocks[0].Key(), "1'")

	// 添加区块3
	proposalBlk3 := NewProposalData(4, 8, 3, "3", "2")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk3, true)
	require.Error(t, err)
	err = forest.AddProposal(proposalBlk3, false)
	require.Error(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块2''
	proposalBlk2_1_1 := NewProposalData(3, 9, 2, "2''", "1''")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk2_1_1, true)
	require.Error(t, err)
	err = forest.AddProposal(proposalBlk2_1_1, false)
	require.Error(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块5'
	proposalBlk5_1 := NewProposalData(7, 10, 5, "5'", "4'")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk5_1, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk5_1, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(commitBlocks))
	require.EqualValues(t, commitBlocks[0].Key(), "2'")
}

func addProposals2(t *testing.T, forest *CachedForest) {
	// 继续添加
	// 添加区块4
	proposalBlk4 := NewProposalData(6, 11, 4, "4", "3")
	commitBlocks, err := forest.UpdateStatesByProposal(proposalBlk4, true)
	require.Error(t, err)
	err = forest.AddProposal(proposalBlk4, false)
	require.Error(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块5
	proposalBlk5 := NewProposalData(7, 12, 5, "5", "4'")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk5, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk5, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(commitBlocks))

	// 添加区块6
	proposalBlk6 := NewProposalData(12, 13, 6, "6", "5")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk6, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk6, false)
	require.NoError(t, err)
	require.EqualValues(t, commitBlocks[0].Key(), "3'")

	// 添加区块7
	proposalBlk7 := NewProposalData(13, 14, 7, "7", "6")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk7, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk7, false)
	require.NoError(t, err)
	require.EqualValues(t, commitBlocks[0].Key(), "4'")

	// 添加区块8
	proposalBlk8 := NewProposalData(14, 15, 8, "8", "7")
	commitBlocks, err = forest.UpdateStatesByProposal(proposalBlk8, true)
	require.NoError(t, err)
	err = forest.AddProposal(proposalBlk8, false)
	require.NoError(t, err)
	require.EqualValues(t, commitBlocks[0].Key(), "5")
}
