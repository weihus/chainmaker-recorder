/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package compensator

import "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"

// RequestCompensator defines a compensator to get missing proposals from other consensus nodes
type RequestCompensator interface {
	// SendRequest 发送请求
	SendRequest(nodeId string, missProposal *maxbft.ProposalFetchMsg)
}
