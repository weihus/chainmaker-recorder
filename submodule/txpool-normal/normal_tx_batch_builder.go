/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"context"
	"strconv"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/localconf/v2"
	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	netPb "chainmaker.org/chainmaker/pb-go/v2/net"
	txpoolPb "chainmaker.org/chainmaker/pb-go/v2/txpool"
	"chainmaker.org/chainmaker/protocol/v2"
)

// txBatchBuilder is used to build and broadcast txBatch
type txBatchBuilder struct {
	nodeId       string
	buildBatchCh chan *commonPb.Transaction // receive transaction to be build and broadcast in txBatch
	ctx          context.Context
	txCache      []*commonPb.Transaction // cache transaction

	batchId            int           // current batch id
	batchMaxSize       int           // batch max size, default is block capacity
	batchTimer         *time.Timer   // build txBatch timer
	batchCreateTimeout time.Duration // build txBatch time threshold. uint:s

	msgBus    msgbus.MessageBus
	chainConf protocol.ChainConf
	log       protocol.Logger
}

// newTxBatchBuilder create txBatchBuilder
func newTxBatchBuilder(ctx context.Context, nodeId string, addChSize, batchMaxSize, batchCreateTimeMs int,
	msgBus msgbus.MessageBus, chainConf protocol.ChainConf, log protocol.Logger) *txBatchBuilder {
	bcTime := time.Duration(batchCreateTimeMs) * time.Millisecond
	// return txBatchBuilder
	return &txBatchBuilder{
		nodeId:             nodeId,
		buildBatchCh:       make(chan *commonPb.Transaction, 2*addChSize),
		ctx:                ctx,
		txCache:            make([]*commonPb.Transaction, 0, batchMaxSize),
		batchId:            -1,
		batchMaxSize:       batchMaxSize,
		batchCreateTimeout: bcTime,
		batchTimer:         time.NewTimer(bcTime),
		msgBus:             msgBus,
		chainConf:          chainConf,
		log:                log,
	}
}

// Start startup txBatchBuilder to listen buildBatchCh and put transaction to txCache
func (b *txBatchBuilder) Start() {
	go func() {
		for {
			select {
			// receive tx from buildBatchCh
			case tx, ok := <-b.buildBatchCh:
				if !ok {
					b.log.Warnf("buildBatchCh has been closed")
					return
				}
				if isConfigTx(tx, b.chainConf) {
					// config tx
					b.buildConfigTxBatch(tx)
				} else {
					// common tx
					b.putCommonTx(tx)
				}
			// batchTimer
			case <-b.batchTimer.C:
				b.buildCommonTxBatch()
			// stop goroutine
			case <-b.ctx.Done():
				return
			}
		}
	}()
	b.log.Debug("normal tx pool start batch builder")
}

func (b *txBatchBuilder) updateBatchMaxSize(maxSize int) {
	b.batchMaxSize = maxSize
	b.txCache = make([]*commonPb.Transaction, 0, maxSize)
	b.log.Infof("[NormalTxPool] update BatchMaxSize success, value: %d", maxSize)
}

func (b *txBatchBuilder) updateBatchCreateTimeout(batchCreateTimeMs int) {
	bcTime := time.Duration(batchCreateTimeMs) * time.Millisecond
	b.batchCreateTimeout = bcTime
	b.batchTimer.Reset(b.batchCreateTimeout)
	b.log.Infof("[NormalTxPool] update BatchCreateTimeout success, value: %d", bcTime)
}

// putCommonTx put common tx to txCache
func (b *txBatchBuilder) putCommonTx(tx *commonPb.Transaction) {
	// add common txs to txCache
	b.txCache = append(b.txCache, tx)
	// if need to build txBatch
	// then build and broadcast txBatch
	b.log.Infof("[NormalTxPool] BatchMaxSize: %d", b.batchMaxSize)
	if len(b.txCache) >= b.batchMaxSize {
		b.buildCommonTxBatch()
	}
}

// buildConfigTxBatch build and broadcast config tx batch
func (b *txBatchBuilder) buildConfigTxBatch(tx *commonPb.Transaction) {
	// create config txBatch
	b.batchId++
	batch := &txpoolPb.TxBatch{
		// batchId := nodeId(8)+seq
		BatchId: cutoutNodeId(b.nodeId) + strconv.Itoa(b.batchId),
		Txs:     []*commonPb.Transaction{tx},
		Size_:   int32(1),
	}
	// simulate no broadcast transaction to test recover mechanism
	if localconf.ChainMakerConfig.DebugConfig.IsNoBroadcastTx {
		b.log.Warnf("simulate no broadcast transaction, batchId:%s, txs num:%d", batch.BatchId, batch.Size_)
		// broadcast batch to other nodes
	} else {
		// marshal txBatch
		b.broadcastTxBatch(mustMarshal(batch))
		b.log.Debugf("broadcast config txBatch success, batchId:%s, txs:%d", batch.BatchId, batch.Size_)
	}
}

// buildCommonTxBatch build and broadcast common tx batch
func (b *txBatchBuilder) buildCommonTxBatch() {
	// reset txCache and batchTimer
	defer b.reset()
	if len(b.txCache) == 0 {
		return
	}
	// create common txBatch
	b.batchId++
	batch := &txpoolPb.TxBatch{
		// batchId := nodeId(8)+seq
		BatchId: cutoutNodeId(b.nodeId) + strconv.Itoa(b.batchId),
		Txs:     b.txCache,
		Size_:   int32(len(b.txCache)),
	}
	// simulate no broadcast transaction to test recover mechanism
	if localconf.ChainMakerConfig.DebugConfig.IsNoBroadcastTx {
		b.log.Warnf("simulate no broadcast transaction, batch Id:%d, txs num:%d", batch.BatchId, batch.Size_)
		// broadcast batch to other nodes
	} else {
		// marshal txBatch
		b.broadcastTxBatch(mustMarshal(batch))
		b.log.Debugf("broadcast common txBatch success, batchId:%s, txs:%d", batch.BatchId, batch.Size_)
	}
}

// broadcastTxBatch broadcast txBatch to other nodes
func (b *txBatchBuilder) broadcastTxBatch(batchBz []byte) {
	// create txPoolMsg
	txPoolMsg := &txpoolPb.TxPoolMsg{
		Type:    txpoolPb.TxPoolMsgType_BATCH_TX,
		Payload: batchBz,
	}
	// create netMsg
	netMsg := &netPb.NetMsg{
		Payload: mustMarshal(txPoolMsg),
		Type:    netPb.NetMsg_TX,
	}
	// broadcast netMsg
	b.msgBus.Publish(msgbus.SendTxPoolMsg, netMsg)
}

// reset txCache
func (b *txBatchBuilder) reset() {
	// reset txCache
	b.txCache = b.txCache[:0]
	// reset batchTimer
	// b.log.Infof("[NormalTxPool] BatchCreateTimeout: %d", b.batchCreateTimeout)
	b.batchTimer.Reset(b.batchCreateTimeout)
}

// getBuildBatchCh get buildBatchCh
func (b *txBatchBuilder) getBuildBatchCh() chan *commonPb.Transaction {
	// return buildBatchCh
	return b.buildBatchCh
}

// Stop stop txBatchBuilder
func (b *txBatchBuilder) Stop() {
	// close buildBatchCh
	close(b.buildBatchCh)
}
