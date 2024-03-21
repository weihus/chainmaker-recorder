/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import "time"

var (
	// MaxBFTAdditionalQCKey defines the key of qc in block.AdditionalData
	MaxBFTAdditionalQCKey = "MaxBFTAdditionalQCKey"

	// RoundTimeoutMill defines the key of view timeout in chainConf
	RoundTimeoutMill = "MaxBFTRoundTimeoutMill"

	// RoundTimeoutIntervalMill defines the key of view increment timeout in chainConf
	RoundTimeoutIntervalMill = "MaxBFTRoundTimeoutIntervalMill"

	// MaxTimeoutMill defines the key of the max view timeout in chainConf
	MaxTimeoutMill = "MaxBFTMaxTimeoutMill"

	// DefaultRoundTimeout defines the default timeout in a view
	DefaultRoundTimeout time.Duration = 15000

	// DefaultRoundTimeoutInterval defines the default timeout increment in a view
	DefaultRoundTimeoutInterval time.Duration = 1000

	// DefaultMaxRoundTimeout defines the default max timeout in a view 5min
	DefaultMaxRoundTimeout time.Duration = 1000 * 60 * 5
)
