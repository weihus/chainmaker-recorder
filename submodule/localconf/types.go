/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package localconf

import (
	"fmt"
	"time"

	"chainmaker.org/chainmaker/common/v2/crypto/pkcs11"
	"chainmaker.org/chainmaker/logger/v2"
)

type nodeConfig struct {
	Type              string         `mapstructure:"type"`
	CertFile          string         `mapstructure:"cert_file"`
	PrivKeyFile       string         `mapstructure:"priv_key_file"`
	CertEncFile       string         `mapstructure:"cert_enc_file"`
	PrivEncKeyFile    string         `mapstructure:"priv_enc_key_file"`
	PrivKeyPassword   string         `mapstructure:"priv_key_password"`
	AuthType          string         `mapstructure:"auth_type"`
	P11Config         pkcs11Config   `mapstructure:"pkcs11"`
	NodeId            string         `mapstructure:"node_id"`
	OrgId             string         `mapstructure:"org_id"`
	SignerCacheSize   int            `mapstructure:"signer_cache_size"`
	CertCacheSize     int            `mapstructure:"cert_cache_size"`
	CertKeyUsageCheck bool           `mapstructure:"cert_key_usage_check"`
	FastSyncConfig    fastSyncConfig `mapstructure:"fast_sync"`
}

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
	StunClient              stunClient        `mapstructure:"stun_client"`
	StunServer              stunServer        `mapstructure:"stun_server"`
	EnablePunch             bool              `mapstructure:"enable_punch"`
}

type netTlsConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	PrivKeyFile    string `mapstructure:"priv_key_file"`
	CertFile       string `mapstructure:"cert_file"`
	CertEncFile    string `mapstructure:"cert_enc_file"`
	PrivEncKeyFile string `mapstructure:"priv_enc_key_file"`
}

type pkcs11Config struct {
	Enabled          bool   `mapstructure:"enabled"`
	Type             string `mapstructure:"type"`
	Library          string `mapstructure:"library"`
	Label            string `mapstructure:"label"`
	Password         string `mapstructure:"password"`
	SessionCacheSize int    `mapstructure:"session_cache_size"`
	Hash             string `mapstructure:"hash"`
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

type stunClient struct {
	Enabled        bool   `mapstructure:"enabled"`
	ListenAddr     string `mapstructure:"listen_addr"`
	StunServerAddr string `mapstructure:"stun_server_addr"`
	NetworkType    string `mapstructure:"network_type"`
}

type stunServer struct {
	Enabled             bool   `mapstructure:"enabled"`
	OtherStunServerAddr string `mapstructure:"other_stun_server_addr"`
	ListenAddr1         string `mapstructure:"listen_addr1"`
	ListenAddr2         string `mapstructure:"listen_addr2"`
	TwoPublicAddress    bool   `mapstructure:"two_public_address"`
	ListenAddr3         string `mapstructure:"listen_addr3"`
	ListenAddr4         string `mapstructure:"listen_addr4"`
	LocalNotifyAddr     string `mapstructure:"local_notify_addr"`
	OtherNotifyAddr     string `mapstructure:"other_notify_addr"`
	NetworkType         string `mapstructure:"network_type"`
}

type rpcConfig struct {
	Provider                               string           `mapstructure:"provider"`
	Host                                   string           `mapstructure:"host"`
	Port                                   int              `mapstructure:"port"`
	TLSConfig                              tlsConfig        `mapstructure:"tls"`
	BlackList                              blackList        `mapstructure:"blacklist"`
	RateLimitConfig                        rateLimitConfig  `mapstructure:"ratelimit"`
	SubscriberConfig                       subscriberConfig `mapstructure:"subscriber"`
	GatewayConfig                          gatewayConfig    `mapstructure:"gateway"`
	CheckChainConfTrustRootsChangeInterval int              `mapstructure:"check_chain_conf_trust_roots_change_interval"`
	MaxSendMsgSize                         int              `mapstructure:"max_send_msg_size"`
	MaxRecvMsgSize                         int              `mapstructure:"max_recv_msg_size"`
}

type tlsConfig struct {
	Mode                  string `mapstructure:"mode"`
	PrivKeyFile           string `mapstructure:"priv_key_file"`
	CertFile              string `mapstructure:"cert_file"`
	PrivEncKeyFile        string `mapstructure:"priv_enc_key_file"`
	CertEncFile           string `mapstructure:"cert_enc_file"`
	TestClientPrivKeyFile string `mapstructure:"test_client_priv_key_file"`
	TestClientCertFile    string `mapstructure:"test_client_cert_file"`
}

type rateLimitConfig struct {
	Enabled         bool `mapstructure:"enabled"`
	Type            int  `mapstructure:"type"`
	TokenPerSecond  int  `mapstructure:"token_per_second"`
	TokenBucketSize int  `mapstructure:"token_bucket_size"`
}

type gatewayConfig struct {
	Enabled         bool `mapstructure:"enabled"`
	MaxRespBodySize int  `mapstructure:"max_resp_body_size"`
}

type subscriberConfig struct {
	RateLimitConfig rateLimitConfig `mapstructure:"ratelimit"`
}

type debugConfig struct {
	IsCliOpen       bool `mapstructure:"is_cli_open"`
	IsHttpOpen      bool `mapstructure:"is_http_open"`
	IsProposer      bool `mapstructure:"is_proposer"`
	IsNotRWSetCheck bool `mapstructure:"is_not_rwset_check"`
	IsConcurPropose bool `mapstructure:"is_concur_propose"`
	IsConcurVerify  bool `mapstructure:"is_concur_verify"`
	IsSolo          bool `mapstructure:"is_solo"`
	IsHaltPropose   bool `mapstructure:"is_halt_propose"`
	// true: minimize access control; false: use full access control
	IsSkipAccessControl bool `mapstructure:"is_skip_access_control"`
	// true for trace memory usage information periodically
	IsTraceMemoryUsage bool `mapstructure:"is_trace_memory_usage"`
	// Simulate a node which would propose duplicate after it has proposed Proposal
	IsProposeDuplicately bool `mapstructure:"is_propose_duplicately"`
	// Simulate a malicious node which would propose duplicate proposals
	IsProposeMultiNodeDuplicately bool `mapstructure:"is_propose_multinode_duplicately"`
	IsProposalOldHeight           bool `mapstructure:"is_proposal_old_height"`
	// Simulate a malicious node which would prevote duplicately
	IsPrevoteDuplicately bool `mapstructure:"is_prevote_duplicately"`
	// Simulate a malicious node which would prevote for oldheight
	IsPrevoteOldHeight bool `mapstructure:"is_prevote_old_height"`
	IsPrevoteLost      bool `mapstructure:"is_prevote_lost"` //prevote vote lost
	//Simulate a malicious node which would propose duplicate precommits
	IsPrecommitDuplicately bool `mapstructure:"is_precommit_duplicately"`
	// Simulate a malicious node which would Precommit a lower height than current height
	IsPrecommitOldHeight bool `mapstructure:"is_precommit_old_height"`

	IsProposeLost    bool `mapstructure:"is_propose_lost"`     //proposal vote lost
	IsProposeDelay   bool `mapstructure:"is_propose_delay"`    //proposal lost
	IsPrevoteDelay   bool `mapstructure:"is_prevote_delay"`    //network problem resulting in preovote lost
	IsPrecommitLost  bool `mapstructure:"is_precommit_lost"`   //precommit vote lost
	IsPrecommitDelay bool `mapstructure:"is_prevcommit_delay"` //network problem resulting in precommit lost
	//if the node committing block without publishing, TRUE；else, FALSE
	IsCommitWithoutPublish bool `mapstructure:"is_commit_without_publish"`
	//simulate a node which sends an invalid prevote(hash=nil)
	IsPrevoteInvalid bool `mapstructure:"is_prevote_invalid"`
	//simulate a node which sends an invalid precommit(hash=nil)
	IsPrecommitInvalid bool `mapstructure:"is_precommit_invalid"`

	IsModifyTxPayload    bool `mapstructure:"is_modify_tx_payload"`
	IsExtreme            bool `mapstructure:"is_extreme"` //extreme fast mode
	UseNetMsgCompression bool `mapstructure:"use_net_msg_compression"`
	IsNetInsecurity      bool `mapstructure:"is_net_insecurity"`

	IsNoBroadcastTx bool `mapstructure:"is_no_broadcast_tx"`
}

type BlockchainConfig struct {
	ChainId string
	Genesis string
}

//type txPoolConfig struct {
//	PoolType            string `mapstructure:"pool_type"`
//	MaxTxPoolSize       uint32 `mapstructure:"max_txpool_size"`
//	MaxConfigTxPoolSize uint32 `mapstructure:"max_config_txpool_size"`
//	IsMetrics           bool   `mapstructure:"is_metrics"`
//	Performance         bool   `mapstructure:"performance"`
//	BatchMaxSize        int    `mapstructure:"batch_max_size"`
//	BatchCreateTimeout  int64  `mapstructure:"batch_create_timeout"`
//	CacheFlushTicker    int64  `mapstructure:"cache_flush_ticker"`
//	CacheThresholdCount int64  `mapstructure:"cache_threshold_count"`
//	CacheFlushTimeOut   int64  `mapstructure:"cache_flush_timeout"`
//	AddTxChannelSize    int64  `mapstructure:"add_tx_channel_size"`
//}

type syncConfig struct {
	BroadcastTime             uint32  `mapstructure:"broadcast_time"`
	BlockPoolSize             uint32  `mapstructure:"block_pool_size"`
	WaitTimeOfBlockRequestMsg uint32  `mapstructure:"wait_time_requested"`
	BatchSizeFromOneNode      uint32  `mapstructure:"batch_Size_from_one_node"`
	ProcessBlockTick          float64 `mapstructure:"process_block_tick"`
	NodeStatusTick            float64 `mapstructure:"node_status_tick"`
	LivenessTick              float64 `mapstructure:"liveness_tick"`
	SchedulerTick             float64 `mapstructure:"scheduler_tick"`
	ReqTimeThreshold          float64 `mapstructure:"req_time_threshold"`
	DataDetectionTick         float64 `mapstructure:"data_detection_tick"`
	BlockRequestTime          float64 `mapstructure:"block_request_time"`
}

type vmConfig struct {
	Go         map[string]interface{} `mapstructure:"go"`
	Java       map[string]interface{} `mapstructure:"java"`
	DockerVMGo map[string]interface{} `mapstructure:"docker_go"`
}

type monitorConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

type pprofConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

type raftConfig struct {
	SnapCount    uint64        `mapstructure:"snap_count"`
	AsyncWalSave bool          `mapstructure:"async_wal_save"`
	Ticker       time.Duration `mapstructure:"ticker"`
}

type ConsensusConfig struct {
	RaftConfig raftConfig `mapstructure:"raft"`
}

//type redisConfig struct {
//	Url          string `mapstructure:"url"`
//	Auth         string `mapstructure:"auth"`
//	DB           int    `mapstructure:"db"`
//	MaxIdle      int    `mapstructure:"max_idle"`
//	MaxActive    int    `mapstructure:"max_active"`
//	IdleTimeout  int    `mapstructure:"idle_timeout"`
//	CacheTimeout int    `mapstructure:"cache_timeout"`
//}

//type clientConfig struct {
//	OrgId           string `mapstructure:"org_id"`
//	UserKeyFilePath string `mapstructure:"user_key_file_path"`
//	UserCrtFilePath string `mapstructure:"user_crt_file_path"`
//	HashType        string `mapstructure:"hash_type"`
//}

type schedulerConfig struct {
	RWSetLog bool `mapstructure:"rwset_log"`
}

type coreConfig struct {
	Evidence bool           `mapstructure:"evidence"`
	TxFilter TxFilterConfig `mapstructure:"tx_filter"`
}

// TxFilterConfig tx filter
type TxFilterConfig struct {
	// Transaction filter type
	Type int32 `mapstructure:"type"`
	// Bird's nest configuration
	BirdsNest BirdsNestConfig `mapstructure:"birds_nest"`
	// Sharding bird's nest configuration
	ShardingBirdsNest ShardingBirdsNestConfig `mapstructure:"sharding"`
}

// BirdsNestConfig birds
type BirdsNestConfig struct {
	Length uint32 `mapstructure:"length"`
	// Cuckoo config
	Cuckoo CuckooConfig `mapstructure:"cuckoo"`
	// rules config
	Rules RulesConfig `mapstructure:"rules"`
	// Snapshot config
	Snapshot SnapshotSerializerConfig `mapstructure:"snapshot"`
}

// RulesConfig birds
type RulesConfig struct {
	AbsoluteExpireTime int64 `mapstructure:"absolute_expire_time"`
}

// CuckooConfig Cuckoo config
type CuckooConfig struct {
	KeyType int32 `mapstructure:"key_type"`
	// num of tags for each bucket, which is b in paper. tag is fingerprint, which is f in paper.
	TagsPerBucket uint32 `mapstructure:"tags_per_bucket"`
	// num of bits for each item, which is length of tag(fingerprint)
	BitsPerItem uint32 `mapstructure:"bits_per_item"`
	// num of keys that filter will store. this value should close to and lower
	//					 nextPow2(maxNumKeys/tagsPerBucket) * maxLoadFactor. cause table.NumBuckets is always a power of two
	MaxNumKeys uint32 `mapstructure:"max_num_keys"`
	// has two constant parameters to choose from:
	// TableTypeSingle normal single table
	// TableTypePacked packed table, use semi-sort to save 1 bit per item
	TableType uint32 `mapstructure:"table_type"`
}

type FilterExtensionConfig struct {
	// id expire time
	AbsoluteExpireTime int64 `mapstructure:"absolute_expire_time"`
}

// SnapshotSerializerConfig Snapshot serializer config
type SnapshotSerializerConfig struct {
	Type        int
	BlockHeight BlockHeightSerializeIntervalConfig `mapstructure:"block_height"`
	Timed       TimedSerializeIntervalConfig       `mapstructure:"timed"`
	// filepath
	Path string `mapstructure:"path"`
}

// TimedSerializeIntervalConfig Timed serialization interval
type TimedSerializeIntervalConfig struct {
	Interval int `mapstructure:"interval"`
}

// BlockHeightSerializeIntervalConfig Timed serialization interval
type BlockHeightSerializeIntervalConfig struct {
	Interval int `mapstructure:"interval"`
}

// ShardingBirdsNestConfig Sharding bird's Nest configuration
type ShardingBirdsNestConfig struct {
	Length  uint32 `mapstructure:"length"`
	Timeout int64  `mapstructure:"timeout"`
	// Bird's Nest configuration
	BirdsNest BirdsNestConfig `mapstructure:"birds_nest"`
	// Snapshot config
	Snapshot SnapshotSerializerConfig `mapstructure:"snapshot"`
}

// CMConfig - Local config struct
type CMConfig struct {
	AuthType         string                 `mapstructure:"auth_type"`
	LogConfig        logger.LogConfig       `mapstructure:"log"`
	NetConfig        netConfig              `mapstructure:"net"`
	NodeConfig       nodeConfig             `mapstructure:"node"`
	RpcConfig        rpcConfig              `mapstructure:"rpc"`
	BlockChainConfig []BlockchainConfig     `mapstructure:"blockchain"`
	ConsensusConfig  ConsensusConfig        `mapstructure:"consensus"`
	StorageConfig    map[string]interface{} `mapstructure:"storage"`
	TxPoolConfig     map[string]interface{} `mapstructure:"txpool"`
	SyncConfig       syncConfig             `mapstructure:"sync"`
	VMConfig         vmConfig               `mapstructure:"vm"`
	CryptoEngine     string                 `mapstructure:"crypto_engine"`

	// 开发调试使用
	DebugConfig     debugConfig     `mapstructure:"debug"`
	PProfConfig     pprofConfig     `mapstructure:"pprof"`
	MonitorConfig   monitorConfig   `mapstructure:"monitor"`
	CoreConfig      coreConfig      `mapstructure:"core"`
	SchedulerConfig schedulerConfig `mapstructure:"scheduler"`
	TxFilter        TxFilterConfig  `mapstructure:"tx_filter"`
	Plugin          Plugin          `mapstructure:"plugin"`

	p11Handle *pkcs11.P11Handle
}
type Plugin struct {
	RecorderFile RecorderFile `mapstructure:"recorder_file"`
}

type RecorderFile struct {
	ConfigServerPort uint16 `mapstructure:"config_server_port"`
}

type fastSyncConfig struct {
	Enable        bool `mapstructure:"enabled"`
	MinFullBlocks int  `mapstructure:"min_full_blocks"`
}

// GetBlockChains - get blockchain config list
func (c *CMConfig) GetBlockChains() []BlockchainConfig {
	return c.BlockChainConfig
}

func (c *CMConfig) GetStorePath() string {
	if path, ok := c.StorageConfig["store_path"]; ok {
		return path.(string)
	}
	return ""
}

func (c *CMConfig) GetP11Handle() (*pkcs11.P11Handle, error) {
	if c.p11Handle == nil {
		var err error
		p11Config := c.NodeConfig.P11Config
		if !p11Config.Enabled {
			return nil, nil //disable p11, return nil error
		}
		if p11Config.Type != "pkcs11" {
			return nil, nil //disable if type is not pkcs11
		}
		c.p11Handle, err = pkcs11.New(p11Config.Library, p11Config.Label, p11Config.Password, p11Config.SessionCacheSize,
			p11Config.Hash)
		if err != nil {
			return nil, fmt.Errorf("fail to initialize organization with HSM: [%v]", err)
		}
	}
	return c.p11Handle, nil
}

// Deal deal and set the default configuration parameters
func (c *CMConfig) Deal() {
	//// RPC ////
	if c.RpcConfig.MaxSendMsgSize > 0 {
		c.RpcConfig.MaxSendMsgSize = c.RpcConfig.MaxSendMsgSize * 1024 * 1024
	} else {
		c.RpcConfig.MaxSendMsgSize = DefaultRpcMaxSendMsgSize
	}

	if c.RpcConfig.MaxRecvMsgSize > 0 {
		c.RpcConfig.MaxRecvMsgSize = c.RpcConfig.MaxRecvMsgSize * 1024 * 1024
	} else {
		c.RpcConfig.MaxRecvMsgSize = DefaultRpcMaxRecvMsgSize
	}
}
