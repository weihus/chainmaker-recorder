/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgcache

import "chainmaker.org/chainmaker/consensus-maxbft/v2/forest"

// MsgTransmitter defines a transmitter used to transmit message between consensus engine and forest
type MsgTransmitter interface {
	// FinalView 返回final视图ID
	FinalView() uint64

	// HandleProposal 处理提案数据
	HandleProposal(proposal *PendingProposal) error

	// Stop 释放资源
	Stop() error

	// Start 启动服务
	Start()

	SetForest(forester forest.Forester)
}
