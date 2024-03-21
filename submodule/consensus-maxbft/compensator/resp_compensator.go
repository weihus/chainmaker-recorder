/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package compensator

import "chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"

// ResponseCompensator 应答补偿器
// 用于处理需要补偿的maxbft请求
type ResponseCompensator interface {
	// HandleRequest 处理请求
	HandleRequest(msg *maxbft.ProposalFetchMsg)
}
