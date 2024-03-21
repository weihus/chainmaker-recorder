/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

import (
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
)

// BlockContainer used to assemble a block and its corresponding qc
type BlockContainer struct {
	block *common.Block
	qc    *maxbft.QuorumCert
}

// NewBlockContainer returns a new BlockContainer object
func NewBlockContainer(block *common.Block) *BlockContainer {
	return &BlockContainer{
		block: block,
	}
}

// SetQC sets a qc for the BlockContainer
func (c *BlockContainer) SetQC(justifyQC *maxbft.QuorumCert) {
	c.qc = justifyQC
}

// GetQC gets the qc from the BlockContainer
func (c *BlockContainer) GetQC() *maxbft.QuorumCert {
	return c.qc
}

// GetBlock gets the block from the BlockContainer
func (c *BlockContainer) GetBlock() *common.Block {
	return c.block
}

// View returns the qc view from the BlockContainer
func (c *BlockContainer) View() uint64 {
	if c.qc == nil {
		return 0
	}
	return utils.GetViewFromQC(c.qc)
}

// Height returns the block height
func (c *BlockContainer) Height() uint64 {
	return c.block.Header.BlockHeight
}

// Hash returns the block hash
func (c *BlockContainer) Hash() []byte {
	return c.block.Header.BlockHash
}

// Key returns the string of the block hash
func (c *BlockContainer) Key() string {
	return string(c.block.Header.BlockHash)
}
