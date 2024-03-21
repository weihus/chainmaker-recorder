/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockchain

import (
	"encoding/hex"
	"strconv"
	"chainmaker.org/chainmaker/pb-go/v2/config"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"github.com/gogo/protobuf/proto"
	localconf "chainmaker.org/chainmaker/localconf/v2"
)

var _ msgbus.Subscriber = (*Blockchain)(nil)

// OnMessage contract event data is a []string, hexToString(proto.Marshal(data))
func (bc *Blockchain) OnMessage(msg *msgbus.Message) {
	switch msg.Topic {
	case msgbus.ChainConfig:
		bc.log.Infof("[Blockchain] receive msg, topic: %s", msg.Topic.String())
		bc.updateChainConfig(msg)
		if err := bc.Init(); err != nil {
			bc.log.Errorf("blockchain init failed when the configuration of blockchain updating, %s", err)
			return
		}
		bc.StopOnRequirements()
		if err := bc.Start(); err != nil {
			bc.log.Errorf("blockchain start failed when the configuration of blockchain updating, %s", err)
			return
		}
	}
}

func (bc *Blockchain) OnQuit() {
	// nothing for implement interface msgbus.Subscriber
}

func (bc *Blockchain) updateChainConfig(msg *msgbus.Message) {
	cfg := &config.ChainConfig{}
	dataStr, ok := msg.Payload.([]string)
	if !ok {
		return
	}
	dataBytes, err := hex.DecodeString(dataStr[0])
	if err != nil {
		bc.log.Error(err)
		panic(err)
	}
	err = proto.Unmarshal(dataBytes, cfg)
	if err != nil {
		bc.log.Error(err)
		panic(err)
	}
	_ = bc.chainConf.SetChainConfig(cfg)
	for index := range bc.chainConf.ChainConfig().Consensus.ExtConfig {
		key := bc.chainConf.ChainConfig().Consensus.ExtConfig[index].GetKey()
		value := bc.chainConf.ChainConfig().Consensus.ExtConfig[index].GetValue()
		switch key {
		case "MaxPeerCountAllow":
			err = bc.net.UpdateNetConfig(key, value)
			if err != nil {
				bc.log.Error(err)
				panic(err)
			}
			localconf.ChainMakerConfig.NetConfig.MaxPeerCountAllow, _ = strconv.Atoi(value)
			err = localconf.UpdateLocalConfig("net.max_peer_count_allow", value)
			if err != nil {
				bc.log.Error(err)
				panic(err)
			}
			bc.log.Infof("[localconf] update MaxPeerCountAllow to localconf success, value: %v", localconf.ChainMakerConfig.NetConfig.MaxPeerCountAllow)
		case "BatchMaxSize":
			err = bc.txPool.UpdateTxPoolConfig(key, value)
			if err != nil {
				bc.log.Error(err)
				panic(err)
			}
			err = localconf.UpdateLocalConfig("txpool.batch_max_size", value)
			if err != nil {
				bc.log.Error(err)
				panic(err)
			}
			bc.log.Infof("[localconf] update %s to localconf success, value: %v", key, value)
		case "BatchCreateTimeout":
			err = bc.txPool.UpdateTxPoolConfig(key, value)
			if err != nil {
				bc.log.Error(err)
				panic(err)
			}
			err = localconf.UpdateLocalConfig("txpool.batch_create_timeout", value)
			if err != nil {
				bc.log.Error(err)
				panic(err)
			}
			bc.log.Infof("[localconf] update %s to localconf success, value: %v", key, value)
		case "Connectors", "HeartbeatInterval", "GossipRetransmission", "OpportunisticGraftPeers", "Dout", "Dlazy", "GossipFactor", "OpportunisticGraftTicks", "FloodPublish":
			err = bc.net.UpdateNetConfig(key, value)
			if err != nil {
				bc.log.Error(err)
				panic(err)
			}
		default:
			return
		}
	}
}
