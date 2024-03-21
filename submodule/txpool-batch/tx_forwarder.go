/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batch

import (
	"fmt"

	commonPb "chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/protocol/v2"
)

// ForwardMod identify transaction forward mod
type ForwardMod string

const (
	// ForwardModSelf is no forward (default)
	ForwardModSelf ForwardMod = "0"
	// ForwardModAVG is forward to all consensus nodes on average
	ForwardModAVG ForwardMod = "1"
	// ForwardModNode is forward to a consensus node
	ForwardModNode ForwardMod = "DEFAULT_NODE_ID"
)

// Forwarder forward tx to a node
type Forwarder interface {
	// ForwardTx forward tx to a node
	ForwardTx(tx *commonPb.Transaction) (to string)
}

// txForwarder impl Forwarder
type txForwarder struct {
	nodeId      string
	forwardMod  ForwardMod
	chainConfig protocol.ChainConf
	log         protocol.Logger
}

// newTxForwarder create txForwarder
func newTxForwarder(nodeId string, forwardMod ForwardMod, chainConfig protocol.ChainConf, log protocol.Logger) (
	*txForwarder, error) {
	// verify whether ForwardMod is valid
	if err := checkForwardMod(forwardMod, chainConfig, log); err != nil {
		return nil, err
	}
	// create txForwarder
	return &txForwarder{
		nodeId:      nodeId,
		forwardMod:  forwardMod,
		chainConfig: chainConfig,
		log:         log,
	}, nil
}

// ForwardTx forward tx to a node
func (f *txForwarder) ForwardTx(tx *commonPb.Transaction) (to string) {
	if f.forwardMod == ForwardModSelf {
		return f.nodeId
	} else if f.forwardMod == ForwardModAVG {
		nodeSet := f.calcConsensusNodesSet()
		txId := tx.Payload.TxId
		nodeIdx := int(txId[len(txId)-1]) % len(nodeSet)
		return nodeSet[nodeIdx]
	} else if len(f.forwardMod) == defaultNodIdLen {
		return string(f.forwardMod)
	} else {
		f.log.Warnf("ForwardMod is invalid, ForwardMod:%s, use default ForwardModSelf", string(f.forwardMod))
		return f.nodeId
	}
}

// checkForwardMod verify whether ForwardMod is valid
func checkForwardMod(forwardMod ForwardMod, chainConfig protocol.ChainConf, log protocol.Logger) error {
	if forwardMod == ForwardModSelf {
		log.Info("ForwardMod is ForwardModSelf")
		return nil
	} else if forwardMod == ForwardModAVG {
		log.Info("ForwardMod is ForwardModAVG")
		return nil
	} else if len(forwardMod) == defaultNodIdLen {
		for _, orgConfig := range chainConfig.ChainConfig().Consensus.Nodes {
			for _, uid := range orgConfig.NodeId {
				if string(forwardMod) == uid {
					log.Infof("ForwardMod is ForwardModNode to: %s", string(forwardMod))
					return nil
				}
			}
		}
		log.Errorf("ForwardMod is ForwardModNode, but node being forwarded is not a consensus node, "+
			"nodeId:%s", string(forwardMod))
		return fmt.Errorf("the node being forwarded should be a consensus node, nodeId:%s", string(forwardMod))
	} else {
		log.Errorf("ForwardMod is invalid, ForwardMod:%s", string(forwardMod))
		return fmt.Errorf("ForwardMod is invalid, ForwardMod:%s", string(forwardMod))
	}
}

// calcConsensusNodesSet calc consensus nodes set
func (f *txForwarder) calcConsensusNodesSet() (nodeIds []string) {
	nodeIds = make([]string, 0, len(f.chainConfig.ChainConfig().Consensus.Nodes))
	for _, orgConfig := range f.chainConfig.ChainConfig().Consensus.Nodes {
		nodeIds = append(nodeIds, orgConfig.NodeId...)
	}
	return
}
