/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
)

// Test_TxBatchBuilder test TxBatchBuilder
func Test_TxBatchBuilder(t *testing.T) {
	// 0.init source
	// generate common txs
	commonTxs, _ := generateTxs(50, false)
	// generate config txs
	confTxs, _ := generateTxs(10, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// create chainConf
	chainConf := newMockChainConf(ctrl, false)
	// create msgBus
	msgBus := newMockMessageBus(ctrl)
	// create log
	log := newMockLogger()
	builder := newTxBatchBuilder(context.Background(), testNodeId, 10000, 2, 1, msgBus, chainConf, log)
	// 1. put txs to buildBatchCh
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, tx := range commonTxs {
			builder.getBuildBatchCh() <- tx
			time.Sleep(1 * time.Millisecond)
		}
		for _, tx := range confTxs {
			builder.getBuildBatchCh() <- tx
		}
	}()
	// 2. get and broadcast txs
	builder.Start()
	time.Sleep(5 * time.Second)
	wg.Done()
	wg.Wait()
}
