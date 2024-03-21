/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"math"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
)

// txCache is used to cache memTx
type txCache struct {
	mTxs []*memTx // cache transaction
}

// newTxBatchBuilder create txCache
func newTxCache(cacheSize int) *txCache {
	// return txCache
	return &txCache{
		mTxs: make([]*memTx, 0, cacheSize),
	}
}

// PutTx put common memTx to txCache
func (c *txCache) PutTx(mtx *memTx) {
	// add common txs to txCache
	c.mTxs = append(c.mTxs, mtx)
}

// GetTxs get txs and dbHeight
func (c *txCache) GetTxs() ([]*commonPb.Transaction, uint64) {
	var (
		txIdsMap    = make(map[string]struct{}, len(c.mTxs))
		txs         = make([]*commonPb.Transaction, 0, len(c.mTxs))
		minDBHeight = uint64(math.MaxUint64)
	)
	for _, mtx := range c.mTxs {
		if _, ok := txIdsMap[mtx.getTxId()]; !ok {
			txs = append(txs, mtx.getTx())
			txIdsMap[mtx.getTxId()] = struct{}{}
			if dbHeight := mtx.getDBHeight(); dbHeight < minDBHeight {
				minDBHeight = dbHeight
			}
		}
	}
	return txs, minDBHeight
}

// Size return current txCache size
func (c *txCache) Size() int {
	return len(c.mTxs)
}

// Reset reset txCache
func (c *txCache) Reset() {
	// reset txCache
	c.mTxs = c.mTxs[:0]
}
