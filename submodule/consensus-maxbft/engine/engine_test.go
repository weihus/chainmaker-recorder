/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package engine

import (
	"testing"

	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/consensus"

	"github.com/stretchr/testify/require"
)

func TestEngine_Verify(t *testing.T) {
	e := Engine{
		nodes: []string{"node4", "node3", "node2", "node1"},
	}
	chainConfig := &config.ChainConfig{
		Consensus: &config.ConsensusConfig{
			Nodes: []*config.OrgConfig{
				{NodeId: []string{"node1"}},
				{NodeId: []string{"node3"}},
				{NodeId: []string{"node4"}},
			},
		},
	}

	// 1. consensus type mismatch
	require.Error(t, e.Verify(consensus.ConsensusType_DPOS, chainConfig))

	// 2. nodes number mismatch
	require.Error(t, e.Verify(consensus.ConsensusType_MAXBFT, chainConfig))

	// 3. nodes content mismatch
	chainConfig.Consensus.Nodes = append(chainConfig.Consensus.Nodes, &config.OrgConfig{NodeId: []string{"node5"}})
	require.NoError(t, e.Verify(consensus.ConsensusType_MAXBFT, chainConfig))

	// 4. no change in nodes
	chainConfig.Consensus.Nodes = []*config.OrgConfig{
		{NodeId: []string{"node1"}},
		{NodeId: []string{"node2"}},
		{NodeId: []string{"node3"}},
		{NodeId: []string{"node4"}},
	}
	require.NoError(t, e.Verify(consensus.ConsensusType_MAXBFT, chainConfig))
}
