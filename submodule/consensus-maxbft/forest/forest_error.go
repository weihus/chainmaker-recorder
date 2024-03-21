/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

import "fmt"

// ProposalLivenessAndSafeCheckError defines an error for activity and safety check
type ProposalLivenessAndSafeCheckError struct {
	// 区块高度
	BlockHeight uint64
	// 视图ID
	View uint64
	// 区块Hash
	BlockHash []byte
}

// NewProposalLivenessAndSafeCheckError returns a new ProposalLivenessAndSafeCheckError object
func NewProposalLivenessAndSafeCheckError(blockHeight, view uint64,
	blockHash []byte) *ProposalLivenessAndSafeCheckError {
	return &ProposalLivenessAndSafeCheckError{
		BlockHeight: blockHeight,
		View:        view,
		BlockHash:   blockHash,
	}
}

// Error returns the string of the error
func (l *ProposalLivenessAndSafeCheckError) Error() string {
	return fmt.Sprintf("the proposal[%d:%d:%s] liveness and safe check error", l.BlockHeight, l.View, l.BlockHash)
}
