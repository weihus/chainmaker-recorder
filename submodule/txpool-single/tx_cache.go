/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package single

import (
	"time"

	"chainmaker.org/chainmaker/protocol/v2"
)

type txCache struct {
	txs        []*mempoolTxs // The transactions in the cache
	totalCount int           // The number of transactions in the cache

	flushThreshold int // The threshold of the number of transactions in the cache. When the number of
	// transactions in the cache is greater than or equal to the threshold, the transactions
	//	will be flushed to the queue.
	flushTimeOut  time.Duration // The timeOut to flush cache
	lastFlushTime time.Time     // Time of the latest cache refresh
}

// newTxCache create txCache
func newTxCache() *txCache {
	// create txCache
	cache := &txCache{flushThreshold: defaultCacheThreshold, flushTimeOut: defaultFlushTimeOut}
	// update CacheThresholdCount
	if TxPoolConfig.CacheThresholdCount > 0 {
		cache.flushThreshold = int(TxPoolConfig.CacheThresholdCount)
	}
	// update CacheFlushTimeOut
	if TxPoolConfig.CacheFlushTimeOut > 0 {
		cache.flushTimeOut = time.Duration(TxPoolConfig.CacheFlushTimeOut) * time.Second
	}
	return cache
}

// isFlushByTxCount Whether the number of transactions in the cache reaches the refresh threshold
func (cache *txCache) isFlushByTxCount(memTxs *mempoolTxs) bool {
	if memTxs != nil {
		return len(memTxs.mtxs)+cache.totalCount >= cache.flushThreshold
	}
	return cache.totalCount >= cache.flushThreshold
}

// addMemoryTxs Add transactions to the cache without any checks at this time.
// When the transactions in the cache is refreshed to the queue, the validity of the transaction will be checked.
func (cache *txCache) addMemoryTxs(memTxs *mempoolTxs) {
	cache.txs = append(cache.txs, memTxs)
	cache.totalCount += len(memTxs.mtxs)
}

// mergeAndSplitTxsBySource Divide the transactions in the cache according to the source.
func (cache *txCache) mergeAndSplitTxsBySource(memTxs *mempoolTxs) (rpcTxs, p2pTxs, internalTxs []*memTx) {
	totalCount := cache.totalCount
	if memTxs != nil {
		totalCount = len(memTxs.mtxs) + cache.totalCount
	}
	rpcTxs = make([]*memTx, 0, totalCount/2)
	p2pTxs = make([]*memTx, 0, totalCount/2)
	internalTxs = make([]*memTx, 0, totalCount/2)
	for _, v := range cache.txs {
		splitTxsBySource(v, &rpcTxs, &p2pTxs, &internalTxs)
	}
	if memTxs != nil {
		splitTxsBySource(memTxs, &rpcTxs, &p2pTxs, &internalTxs)
	}
	return
}

// splitTxsBySource split txs by source
func splitTxsBySource(memTxs *mempoolTxs, rpcMemTxs, p2pMemTxs, internalMemTxs *[]*memTx) {
	if len(memTxs.mtxs) == 0 {
		return
	}
	if memTxs.source == protocol.RPC {
		*rpcMemTxs = append(*rpcMemTxs, memTxs.mtxs...)
	} else if memTxs.source == protocol.P2P {
		*p2pMemTxs = append(*p2pMemTxs, memTxs.mtxs...)
	} else {
		*internalMemTxs = append(*internalMemTxs, memTxs.mtxs...)
	}
}

// reset Reset the cache.
func (cache *txCache) reset() {
	cache.txs = cache.txs[:0]
	cache.totalCount = 0
	cache.lastFlushTime = time.Now()
}

// isFlushByTime Whether the cache refresh time threshold is reached
func (cache *txCache) isFlushByTime() bool {
	return time.Now().After(cache.lastFlushTime.Add(cache.flushTimeOut))
}

// txCount The number of transactions in the cache.
func (cache *txCache) txCount() int {
	return cache.totalCount
}
