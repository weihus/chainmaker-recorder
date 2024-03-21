/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"chainmaker.org/chainmaker/protocol/v2"
)

const (
	defaultChannelSize        = 10000 // The channel size to add the txs
	defaultBatchCreateTimeout = 50    // the default create batch time threshold. uint:ms
	defaultSecGetTime         = 10    // The second time threshold to get txs from pool. unit:ms
	defaultBatchIdLen         = 24    // The batchId length. unit:byte. batchId=timestamp(8)+nodeId(8)+batchHash(8)
	defaultForwardMod         = "0"   // The default forward mod is ForwardModSelf.
	defaultNodIdLen           = 46    // The nodeId length. unit:byte.

	dumpDir        = "dump_tx_wal"
	recoverTxDir   = "fetched_tx_recover"
	confTxInQueDir = "config_tx_queue"
	confTxInPenDir = "config_tx_pending"
	commTxInQueDir = "common_tx_queue"
	commTxInPenDir = "common_tx_pending"
)

const (
	defaultMaxTxCount          = 1000         // Maximum number of transactions in a block
	defaultMaxTxPoolSize       = 5120         // Maximum number of common transaction in the pool
	defaultMaxConfigTxPoolSize = 100          // Maximum number of config transaction in the pool
	defaultMaxTxTimeTimeout    = float64(600) // default transaction overdue time. uint:s
	defaultRecoverTimeMs       = 1000         // default recover batch time. uint:ms
	defaultQueryUpperLimit     = 10000        // default query transaction upper limit
)

var (
	// TxPoolConfig is tx_pool global configuration
	TxPoolConfig *txPoolConfig
	// MonitorEnabled indicates whether monitoring is enabled
	MonitorEnabled bool
)

type txPoolConfig struct {
	PoolType            string `mapstructure:"pool_type"`
	MaxTxPoolSize       uint32 `mapstructure:"max_txpool_size"`
	MaxConfigTxPoolSize uint32 `mapstructure:"max_config_txpool_size"`
	IsMetrics           bool   `mapstructure:"is_metrics"`
	Performance         bool   `mapstructure:"performance"`
	BatchMaxSize        uint32 `mapstructure:"batch_max_size"`
	BatchCreateTimeout  int64  `mapstructure:"batch_create_timeout"` // unit:ms
	CacheFlushTicker    int64  `mapstructure:"cache_flush_ticker"`   // unit:s
	CacheThresholdCount uint32 `mapstructure:"cache_threshold_count"`
	CacheFlushTimeOut   int64  `mapstructure:"cache_flush_timeout"` // unit:s
	AddTxChannelSize    uint32 `mapstructure:"add_tx_channel_size"`
	CommonQueueNum      uint32 `mapstructure:"common_queue_num"`
	IsDumpTxsInQueue    bool   `mapstructure:"is_dump_txs_in_queue"`
	ForwardMod          string `mapstructure:"forward_mod"`
	QueryUpperLimit     uint32 `mapstructure:"query_upper_limit"`
}

// IsTxTimeVerify Whether transactions require validation
func IsTxTimeVerify(chainConf protocol.ChainConf) bool {
	if chainConf != nil {
		config := chainConf.ChainConfig()
		if config != nil && config.Block != nil {
			return config.Block.TxTimestampVerify
		}
	}
	return false
}

// MaxTxTimeTimeout The maximum timeout for a transaction
func MaxTxTimeTimeout(chainConf protocol.ChainConf) float64 {
	if chainConf != nil {
		config := chainConf.ChainConfig()
		if config != nil && config.Block != nil && config.Block.TxTimeout > 0 {
			return float64(config.Block.TxTimeout)
		}
	}
	return defaultMaxTxTimeTimeout
}

// MaxTxCount Maximum number of transactions in a block
func MaxTxCount(chainConf protocol.ChainConf) int {
	if chainConf != nil {
		config := chainConf.ChainConfig()
		if config != nil && config.Block != nil && int(config.Block.BlockTxCapacity) > 0 {
			return int(config.Block.BlockTxCapacity)
		}
	}
	return defaultMaxTxCount
}

// BatchMaxSize Maximum number of common transaction in a batch
func BatchMaxSize(chainConf protocol.ChainConf) int {
	config := TxPoolConfig
	if config.BatchMaxSize > 0 && int(config.BatchMaxSize) < MaxTxCount(chainConf) {
		return int(config.BatchMaxSize)
	}
	return MaxTxCount(chainConf)
}

// MaxCommonTxPoolSize Maximum number of common transaction in the pool
func MaxCommonTxPoolSize() int {
	config := TxPoolConfig
	if int(config.MaxTxPoolSize) > 0 {
		return int(config.MaxTxPoolSize)
	}
	return defaultMaxTxPoolSize
}

// MaxConfigTxPoolSize The maximum number of configure transaction in the pool
func MaxConfigTxPoolSize() int {
	config := TxPoolConfig
	if int(config.MaxConfigTxPoolSize) > 0 {
		return int(config.MaxConfigTxPoolSize)
	}
	return defaultMaxConfigTxPoolSize
}

// IsMetrics Whether to log operation time
func IsMetrics() bool {
	config := TxPoolConfig
	return config.IsMetrics
}

// IsDumpTxsInQueue Whether to dump config and common transaction in queue
func IsDumpTxsInQueue() bool {
	config := TxPoolConfig
	return config.IsDumpTxsInQueue
}

// QueryUpperLimit Maximum number of querying transaction in the pool
func QueryUpperLimit() int {
	config := TxPoolConfig
	if int(config.QueryUpperLimit) > 0 {
		return int(config.QueryUpperLimit)
	}
	return defaultQueryUpperLimit
}
