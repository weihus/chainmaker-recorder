/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest_mock

import (
	"chainmaker.org/chainmaker/pb-go/v2/common"
)

type BlockCacheMock struct {
	blocks []*common.Block
}

func NewBlockCacheMock() *BlockCacheMock {
	return &BlockCacheMock{
		blocks: make([]*common.Block, 0),
	}
}

func (c *BlockCacheMock) AddBlock(blk *common.Block) error {
	c.blocks = append(c.blocks, blk)
	return nil
}

func (c *BlockCacheMock) Blocks() []*common.Block {
	return c.blocks
}
