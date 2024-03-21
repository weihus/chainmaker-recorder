/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pacemaker

import (
	"time"

	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
)

// ViewEvent used to describe an time_service event
type ViewEvent struct {
	View      uint64
	Duration  time.Duration
	EventType maxbft.ConsStateType
}
