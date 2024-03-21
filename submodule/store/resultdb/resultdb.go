/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package resultdb

import (
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	storePb "chainmaker.org/chainmaker/pb-go/v2/store"
	"chainmaker.org/chainmaker/store/v2/serialization"
)

// ResultDB provides handle to rwSets instances
type ResultDB interface {
	InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error

	// CommitBlock commits the block rwsets in an atomic operation
	CommitBlock(blockInfo *serialization.BlockWithSerializedInfo, isCache bool) error

	// ShrinkBlocks archive old blocks rwsets in an atomic operation
	ShrinkBlocks(txIdsMap map[uint64][]string) error

	// RestoreBlocks restore blocks from outside serialized block data
	RestoreBlocks(blockInfos []*serialization.BlockWithSerializedInfo) error

	// GetTxRWSet returns an txRWSet for given txId, or returns nil if none exists.
	GetTxRWSet(txid string) (*commonPb.TxRWSet, error)

	// GetRWSetIndex returns the offset of the rwset in the file
	GetRWSetIndex(txId string) (*storePb.StoreInfo, error)

	// GetLastSavepoint returns the last block height
	GetLastSavepoint() (uint64, error)

	// Close is used to close database
	Close()
}
