package utils

import (
	"fmt"
	"testing"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/config"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"

	"github.com/stretchr/testify/require"
)

func TestGetQCFromBlock(t *testing.T) {
	block0 := &common.Block{
		Header: &common.BlockHeader{
			BlockHeight: 0,
			BlockHash:   []byte("hash zero"),
		},
	}

	qc0 := GetQCFromBlock(block0)
	require.EqualValues(t, block0.Header.BlockHash, qc0.Votes[0].BlockId)

	qc1 := &maxbft.QuorumCert{
		Votes: []*maxbft.VoteData{
			{
				BlockId: []byte("first block"),
				Height:  10,
				View:    15,
			},
		},
	}

	block1 := &common.Block{
		Header: &common.BlockHeader{
			BlockHeight: 11,
		},
	}
	InsertQCToBlock(block1, qc1)
	retQC1 := GetQCFromBlock(block1)
	require.EqualValues(t, qc1.Votes[0].View, retQC1.Votes[0].View)
	require.EqualValues(t, qc1.Votes[0].Height, retQC1.Votes[0].Height)
	require.EqualValues(t, qc1.Votes[0].BlockId, retQC1.Votes[0].BlockId)
}

func TestGetConsensusNodes(t *testing.T) {
	cfg := &config.ChainConfig{
		Consensus: &config.ConsensusConfig{
			Nodes: []*config.OrgConfig{
				{NodeId: []string{"QmZy2okHNAtFESoGzjMLc1nmWh8yjBCbZpQcgaDPPj3uv3"}},
				{NodeId: []string{"QmWTJuJiq9GuikKvjwPyeoECczGphALxXoig7rjGKBStD9"}},
				{NodeId: []string{"QmX1qMbAiQsfx3qWBGu7UTXcJomcquSEhTsYSs3yJAdnmu"}},
				{NodeId: []string{"QmWAuPCSSNPqGtWihbC1JskZcYP4mqG26eXo7r9CEV5Qm7"}},
			},
		},
	}
	nodes := GetConsensusNodes(cfg)
	fmt.Printf("origin: %v \n\n sort later:%v\n", cfg.Consensus.Nodes, nodes)
}
