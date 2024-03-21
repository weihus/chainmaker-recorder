/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package relay

// RelayConfig .
type RelayConfig struct {
	NetConfig NetConfig `mapstructure:"net"` // net config
}

// NetConfig .
type NetConfig struct {
	AuthType                string                   `mapstructure:"auth_type"`
	ListenAddr              string                   `mapstructure:"listen_addr"`
	PeerStreamPoolSize      int                      `mapstructure:"peer_stream_pool_size"`
	MaxPeerCountAllow       int                      `mapstructure:"max_peer_count_allow"`
	PeerEliminationStrategy int                      `mapstructure:"peer_elimination_strategy"` // default LIFO
	Seeds                   []string                 `mapstructure:"seeds"`
	TLSConfig               netTlsConfig             `mapstructure:"tls"`
	BlackList               blackList                `mapstructure:"blacklist"`
	IsNetInsecurity         bool                     `mapstructure:"is_net_insecurity"`
	UseNetMsgCompression    bool                     `mapstructure:"use_net_msg_compression"`
	CustomTrustRootsConfigs []CustomTrustRootsConfig `mapstructure:"custom_trust_roots"` // custom trust root
}

// netTlsConfig
type netTlsConfig struct {
	Enabled     bool   `mapstructure:"enabled"` // whether enable tls
	PrivKeyFile string `mapstructure:"priv_key_file"`
	CertFile    string `mapstructure:"cert_file"`
}

// blackList
type blackList struct {
	Addresses []string `mapstructure:"addresses"`
	NodeIds   []string `mapstructure:"node_ids"`
}

// CustomTrustRootsConfig .
type CustomTrustRootsConfig struct {
	ChainId    string   `mapstructure:"chain_id"`
	TrustRoots []string `mapstructure:"trust_roots"`
}
