/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package wal

import (
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
)

// WalAdaptor defines an adapter for wal components
type WalAdaptor interface {
	// save consensus messages that validated to log wal system
	SaveWalEntry(msgType maxbft.MessageType, msg []byte) (uint64, error)

	// load consensus messages from wal system and replay them
	ReplayWal() (hasWalEntry bool, err error)

	// delete related information after committed blocks and update lastCommitWalIndex
	UpdateWalIndexAndTrunc(committedHeight uint64)

	// record wal index when receiving proposal
	AddProposalWalIndex(proposalHeight, index uint64)

	// return height status
	GetHeightStatus() map[uint64]uint64

	// save new view information to wal
	AddNewView() error

	// clear the wal, used in switching epoch
	Clear() error

	// close the wal
	Close()
}
