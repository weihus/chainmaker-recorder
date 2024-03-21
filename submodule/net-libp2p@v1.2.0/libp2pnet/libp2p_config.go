/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

var Config *netConfig

type netConfig struct {
	Provider                string            `mapstructure:"provider"`
	ListenAddr              string            `mapstructure:"listen_addr"`
	PeerStreamPoolSize      int               `mapstructure:"peer_stream_pool_size"`
	MaxPeerCountAllow       int               `mapstructure:"max_peer_count_allow"`
	PeerEliminationStrategy int               `mapstructure:"peer_elimination_strategy"`
	Seeds                   []string          `mapstructure:"seeds"`
	TLSConfig               netTlsConfig      `mapstructure:"tls"`
	BlackList               blackList         `mapstructure:"blacklist"`
	CustomChainTrustRoots   []chainTrustRoots `mapstructure:"custom_chain_trust_roots"`
}

type netTlsConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	PrivKeyFile string `mapstructure:"priv_key_file"`
	CertFile    string `mapstructure:"cert_file"`
}

type blackList struct {
	Addresses []string `mapstructure:"addresses"`
	NodeIds   []string `mapstructure:"node_ids"`
}

type chainTrustRoots struct {
	ChainId    string       `mapstructure:"chain_id"`
	TrustRoots []trustRoots `mapstructure:"trust_roots"`
}

type trustRoots struct {
	OrgId string `mapstructure:"org_id"`
	Root  string `mapstructure:"root"`
}
