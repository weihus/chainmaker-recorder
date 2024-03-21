/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package liquidnet

import (
	"context"
	"errors"
	"sync"
	"time"
	"strconv"

	"chainmaker.org/chainmaker/common/v2/crypto/asym"
	"chainmaker.org/chainmaker/common/v2/crypto/tls"
	"chainmaker.org/chainmaker/common/v2/helper"
	cmTlsS "chainmaker.org/chainmaker/net-common/cmtlssupport"
	"chainmaker.org/chainmaker/net-common/common"
	"chainmaker.org/chainmaker/net-common/common/priorityblocker"
	"chainmaker.org/chainmaker/net-common/utils"
	"chainmaker.org/chainmaker/net-liquid/core/broadcast"
	"chainmaker.org/chainmaker/net-liquid/core/discovery"
	"chainmaker.org/chainmaker/net-liquid/core/handler"
	"chainmaker.org/chainmaker/net-liquid/core/host"
	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/types"
	"chainmaker.org/chainmaker/net-liquid/core/util"
	"chainmaker.org/chainmaker/net-liquid/discovery/protocoldiscovery"
	"chainmaker.org/chainmaker/net-liquid/exMultiaddr"
	"chainmaker.org/chainmaker/net-liquid/holepunch"
	lHost "chainmaker.org/chainmaker/net-liquid/host"
	"chainmaker.org/chainmaker/net-liquid/host/quic"
	"chainmaker.org/chainmaker/net-liquid/host/tcp"
	"chainmaker.org/chainmaker/net-liquid/pubsub"
	"chainmaker.org/chainmaker/net-liquid/relay"
	"chainmaker.org/chainmaker/net-liquid/stun"
	"chainmaker.org/chainmaker/net-liquid/tlssupport"
	api "chainmaker.org/chainmaker/protocol/v2"
	ma "github.com/multiformats/go-multiaddr"
)

// global log object
var log api.Logger

// the global log object used to create a pubsub
var pubSubLoggerCreator func(chainId string) api.Logger

var (
	// ErrorPubSubNotExist will be returned when pub-sub service not exist.
	ErrorPubSubNotExist = errors.New("pub-sub service not exist")
	// ErrorPubSubExisted will be returned if the pub-sub service exist
	// when calling InitPubSub method.
	ErrorPubSubExisted = errors.New("pub-sub service existed")
	// ErrorTopicSubscribed will be returned if the topic has been
	// subscribed when calling SubscribeWithChainId method.
	ErrorTopicSubscribed = errors.New("topic has been subscribed")
	// ErrorTopicNotSubscribed will be returned if the topic has
	// not been subscribed when calling CancelSubscribeWithChainId method.
	ErrorTopicNotSubscribed = errors.New("topic has not been subscribed")
	// ErrorNotBelongToChain will be returned if the remote node
	// not belong to chain expected when calling SendMsg method.
	ErrorNotBelongToChain = errors.New("node not belong to chain")
	// ErrorWrongAddressOrUnsupported will be returned if the listening
	// address is wrong or unsupported when calling Start method.
	ErrorWrongAddressOrUnsupported = errors.New("wrong address or address unsupported")
	// ErrorNetRunning will be returned if Start method has been called
	// when calling Start method.
	ErrorNetRunning = errors.New("net running")
)

const (
	// DefaultMaxPeerCount is the default value for HostConfig.MaxPeerCountAllowed.
	DefaultMaxPeerCount = 20
	// DefaultMaxConnCountEachPeer is the default value for HostConfig.MaxConnCountEachPeerAllowed.
	DefaultMaxConnCountEachPeer = 1
	// DefaultPeerEliminationStrategy is the default value for HostConfig.ConnEliminationStrategy.
	DefaultPeerEliminationStrategy = 3
	// DefaultInitSendStreamSize is the default value for HostConfig.SendStreamPoolInitSize.
	DefaultInitSendStreamSize = 10
	// DefaultSendStreamMaxCount is the default value for HostConfig.SendStreamPoolCap.
	DefaultSendStreamMaxCount = 100
	// DefaultListenAddress is the default value for HostConfig.ListenAddresses.
	DefaultListenAddress = "/ip4/0.0.0.0/tcp/0"
	// DefaultPubSubMaxMessageSize is the default value for pubSubConfig.MaxPubMessageSize.
	DefaultPubSubMaxMessageSize = 50 * (2 << 20)
	// udp
	udp = "udp"
	// protocolHolePunch
	protocolHolePunch = "protocolHolePunch"
)

// InitLogger .
func InitLogger(globalNetLogger api.Logger, pubSubLogCreator func(chainId string) api.Logger) {
	log = globalNetLogger
	pubSubLoggerCreator = pubSubLogCreator
}

var _ api.Net = (*LiquidNet)(nil)

// LiquidNet is an implementation of Net interface with liquid.
type LiquidNet struct {
	lock sync.Mutex

	context context.Context

	// whether net has been started
	startUp bool
	// network type, currently only supports Libp2p and Liquid
	netType api.NetType

	// contains some configuration information required by the host
	hostCfg *lHost.HostConfig
	// host object
	host host.Host
	// contains some configuration information required by pubsub
	psCfg *pubSubConfig
	//map[string]broadcast.PubSub, map chainId -> broadcast.PubSub
	psMap sync.Map

	// contains some configuration information required for encryptio
	cryptoCfg *cryptoConfig
	// used to verify member, such as whether it belongs to this chain
	memberStatusValidator *common.MemberStatusValidator

	tlsChainTrustRoots *cmTlsS.ChainTrustRoots
	tlsCertValidator   *cmTlsS.CertValidator

	// some simple storage objects
	peerIdChainIdsRecorder *common.PeerIdChainIdsRecorder
	certIdPeerIdMapper     *common.CertIdPeerIdMapper
	peerIdTlsCertStore     *common.PeerIdTlsCertStore
	peerIdPubKeyStore      *common.PeerIdPubKeyStore

	// used to record some topics subscribed by the current peer
	subscribeTopic *types.StringSet

	// service discovery object, which provides services for discovering other peer on the network
	discoveryService discovery.Discovery

	// contains configuration information for protocol extensions
	extensionsCfg *extensionsConfig
	// for cutting larger messages
	pktAdapter *pktAdapter
	// used to control the priority of messages
	priorityController *priorityblocker.Blocker

	// hole punch related
	stunClient    stun.Client
	stunServer    stun.Server
	stunClientCfg *stunClientConfig
	stunServerCfg *stunServerConfig
	holePunch     *holepunch.HolePunch
	holePunchCfg  *holePunchConfig
}

// NewLiquidNet create a new LiquidNet instance.
func NewLiquidNet() (*LiquidNet, error) {
	ctx := context.Background()
	liquidNet := &LiquidNet{
		context: ctx,
		startUp: false,
		netType: api.Liquid,
		hostCfg: &lHost.HostConfig{
			TlsCfg:                      nil,
			LoadPidFunc:                 nil,
			SendStreamPoolInitSize:      DefaultInitSendStreamSize,
			SendStreamPoolCap:           DefaultSendStreamMaxCount,
			PeerReceiveStreamMaxCount:   0,
			MaxPeerCountAllowed:         DefaultMaxPeerCount,
			MaxConnCountEachPeerAllowed: DefaultMaxConnCountEachPeer,
			ConnEliminationStrategy:     DefaultPeerEliminationStrategy,
			ListenAddresses:             []ma.Multiaddr{ma.StringCast(DefaultListenAddress)},
			DirectPeers:                 make(map[peer.ID]ma.Multiaddr),
			BlackNetAddr:                make([]string, 0),
			BlackPeers:                  make([]peer.ID, 0),
			MsgCompress:                 false,
			Insecurity:                  false,
		},
		psCfg: &pubSubConfig{
			MaxPubMessageSize: DefaultPubSubMaxMessageSize,
		},
		cryptoCfg: &cryptoConfig{
			KeyBytes:                       nil,
			CertBytes:                      nil,
			CustomChainTrustRootCertsBytes: make(map[string][][]byte),
		},
		memberStatusValidator: common.NewMemberStatusValidator(),
		subscribeTopic:        &types.StringSet{},
		extensionsCfg:         &extensionsConfig{EnablePkt: false},
		holePunchCfg:          &holePunchConfig{EnablePunch: false},
		stunClientCfg:         &stunClientConfig{},
		stunServerCfg:         &stunServerConfig{},
		pktAdapter:            nil,
		priorityController:    nil,
	}
	liquidNet.peerIdChainIdsRecorder = common.NewPeerIdChainIdsRecorder(log)
	liquidNet.certIdPeerIdMapper = common.NewCertIdPeerIdMapper(log)
	liquidNet.peerIdTlsCertStore = common.NewPeerIdTlsCertStore(log)
	liquidNet.peerIdPubKeyStore = common.NewPeerIdPubKeyStore(log)
	//liquidNet.stunClient, _ = stun.NewDefaultStunClient()
	//liquidNet.stunServer, _ = stun.NewDefaultStunServer()
	return liquidNet, nil
}

// HostConfig is the configuration of liquid host.
func (l *LiquidNet) HostConfig() *lHost.HostConfig {
	return l.hostCfg
}

// PubSubConfig is the configuration of liquid host.
func (l *LiquidNet) PubSubConfig() *pubSubConfig {
	return l.psCfg
}

// CryptoConfig is the configuration for crypto.
func (l *LiquidNet) CryptoConfig() *cryptoConfig {
	return l.cryptoCfg
}

// ExtensionsConfig is the configuration for extensions.
func (l *LiquidNet) ExtensionsConfig() *extensionsConfig {
	return l.extensionsCfg
}

// StunClientConfig is the configuration for stun.
func (l *LiquidNet) StunClientConfig() *stunClientConfig {
	return l.stunClientCfg
}

// StunServerConfig is the configuration for stun.
func (l *LiquidNet) StunServerConfig() *stunServerConfig {
	return l.stunServerCfg
}

// HolePunchConfig is the configuration for extensions.
func (l *LiquidNet) HolePunchConfig() *holePunchConfig {
	return l.holePunchCfg
}

// GetNodeUid get self peer id
func (l *LiquidNet) GetNodeUid() string {
	return l.host.ID().ToString()
}

// InitPubSub will init new PubSub instance with given chainId and maxMessageSize.
func (l *LiquidNet) InitPubSub(chainId string, maxMessageSize int) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if _, ok := l.psMap.Load(chainId); ok {
		return ErrorPubSubExisted
	}
	if maxMessageSize <= 0 {
		maxMessageSize = l.PubSubConfig().MaxPubMessageSize
	}
	var err error

	// create a global log object for pubsub
	pubSubLogger := pubSubLoggerCreator(chainId)
	ps := pubsub.NewChainPubSub(chainId, pubSubLogger, pubsub.WithPubSubMessageMaxSize(int32(maxMessageSize)))
	if l.startUp {
		err = ps.AttachHost(l.host)
		if err != nil {
			return err
		}
	}
	l.psMap.Store(chainId, ps)

	// if Net is already started, start the service discovered by the node
	if l.startUp {
		err = l.attachDiscovery(chainId)
		if err != nil {
			return err
		}
	}

	return err
}

//UpdateNetConfig update only maxPeerCountAllow for now
func (l *LiquidNet) UpdateNetConfig(param string, value string) error {
	switch param {
		case "MaxPeerCountAllow":
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			l.hostCfg.MaxPeerCountAllowed = v
			log.Infof("[LiquidNet] update MaxPeerCountAllowed success, value: %s", value)
		default:
			return nil
	}
	return nil
}

// BroadcastWithChainId publish the message to topic, if not subscribe the topic, will return error
func (l *LiquidNet) BroadcastWithChainId(chainId string, topic string, data []byte) error {
	// whether pub-sub service exist
	ps, ok := l.psMap.Load(chainId)
	if !ok {
		return ErrorPubSubNotExist
	}
	// publish msg
	chainPubSub, _ := ps.(*pubsub.ChainPubSub)
	chainPubSub.Publish(topic, data)
	return nil
}

// createSubMsgHandler used to create a handler for handling subscription messages
func (l *LiquidNet) createSubMsgHandler(chainId string, handler api.PubSubMsgHandler) handler.SubMsgHandler {
	return func(publisher peer.ID, topic string, msg []byte) {
		go func(publisherInner peer.ID, topicInner string, msgInner []byte) {
			// whether published by myself
			if publisherInner.ToString() == l.GetNodeUid() {
				return
			}
			// whether belong to the chain
			if !l.peerIdChainIdsRecorder.IsPeerBelongToChain(publisherInner.ToString(), chainId) {
				log.Debug("[LiquidNet] get sub msg from peer not belong to chain "+
					"(publisher:", publisherInner.ToString(), ", chain:", chainId, ")")
				return
			}

			// call msg handler
			err := handler(publisherInner.ToString(), msgInner)
			if err != nil {
				log.Errorf(
					"[LiquidNet] call subscribe handler failed, %s (publisher: %s,topic: %s,msg: %v)",
					err.Error(), publisherInner.ToString(), topicInner, msgInner)
				return
			}
		}(publisher, topic, msg)
	}
}

// SubscribeWithChainId register a PubSubMsgHandler to a PubSubTopic with the pub-sub service which id is given chainId.
func (l *LiquidNet) SubscribeWithChainId(chainId string, topic string, handler api.PubSubMsgHandler) error {
	// whether pub-sub service exist
	chainPubSub, ok := l.psMap.Load(chainId)
	if !ok {
		return ErrorPubSubNotExist
	}
	// whether has subscribed
	chainTopic := chainAppendTopic(chainId, topic)
	if l.subscribeTopic.Exist(chainTopic) {
		return ErrorTopicSubscribed
	}
	// create sub msg handler
	h := l.createSubMsgHandler(chainId, handler)
	// subscribe topic
	chainPubSub.(*pubsub.ChainPubSub).Subscribe(topic, h)
	l.subscribeTopic.Put(chainTopic)
	return nil
}

// CancelSubscribeWithChainId cancel subscribe a PubSubTopic with the pub-sub service which id is given chainId.
func (l *LiquidNet) CancelSubscribeWithChainId(chainId string, topic string) error {
	// whether pub-sub service exist
	ps, ok := l.psMap.Load(chainId)
	if !ok {
		return ErrorPubSubNotExist
	}
	// whether topic has subscribed
	chainTopic := chainAppendTopic(chainId, topic)
	if !l.subscribeTopic.Exist(chainTopic) {
		return ErrorTopicNotSubscribed
	}
	// unsubscribe topic
	ps.(*pubsub.ChainPubSub).Unsubscribe(topic)
	l.subscribeTopic.Remove(chainTopic)
	return nil
}

// chainAppendTopic splicing topic key
func chainAppendTopic(chain, topic string) string {
	return chain + "-" + topic
}

// SendMsg send msg to the node which id is given string.
// 		msgFlag: is a flag used to distinguish msg type.
func (l *LiquidNet) SendMsg(chainId string, targetPeer string, msgFlag string, data []byte) error {
	if targetPeer == l.GetNodeUid() {
		log.Warn("[LiquidNet] [SendMsg] can not send msg to self")
		return nil
	}

	if l.priorityController != nil {
		l.priorityController.Block(msgFlag)
	}

	targetPeerId := peer.ID(targetPeer)

	// whether peer belong to chain
	if !l.hostCfg.Insecurity && !l.peerIdChainIdsRecorder.IsPeerBelongToChain(targetPeer, chainId) {
		return ErrorNotBelongToChain
	}
	// create protocol id
	netProtocolId := CreateProtocolIdWithChainIdAndMsgFlag(chainId, msgFlag)
	// whether pktAdapter enabled
	if l.pktAdapter != nil {
		e := l.pktAdapter.sendMsg(targetPeerId, netProtocolId, data)
		if e != nil {
			log.Errorf("[LiquidNet] [SendMsg][Pkt] send message failed, %s "+
				"(chain: %s, targetPeer: %s, msg_flag: %s)",
				e.Error(), chainId, targetPeer, msgFlag)
			return e
		}
		return nil
	}

	// send msg
	err := l.host.SendMsg(netProtocolId, targetPeerId, data)
	if err != nil {
		log.Errorf("[LiquidNet] [SendMsg] send message failed, %s (chain: %s, targetPeer: %s, msg_flag: %s)",
			err.Error(), chainId, targetPeer, msgFlag)
		return err
	}
	return nil
}

// createMsgPayloadHandler create a handler for handling direct messages
func (l *LiquidNet) createMsgPayloadHandler(handler api.DirectMsgHandler) handler.MsgPayloadHandler {
	return func(senderPID peer.ID, msgPayload []byte) {
		go func(senderPIDInner peer.ID, msgPayloadInner []byte) {
			// call handler
			err := handler(string(senderPIDInner), msgPayloadInner)
			if err != nil {
				log.Errorf("[LiquidNet] [DirectMsgHandle] call direct msg handler failed, %s "+
					"(sender: %s, message_length: %d)",
					err.Error(), senderPIDInner, len(msgPayloadInner))
				return
			}
		}(senderPID, msgPayload)
	}
}

// DirectMsgHandle register a DirectMsgHandler to the net.
// 		msgFlag: is a flag used to distinguish msg type.
func (l *LiquidNet) DirectMsgHandle(chainId string, msgFlag string, handler api.DirectMsgHandler) error {
	// create net protocol id
	netProtocolId := CreateProtocolIdWithChainIdAndMsgFlag(chainId, msgFlag)
	// create msg payload handler
	h := l.createMsgPayloadHandler(handler)
	// register handler
	err := l.host.RegisterMsgPayloadHandler(netProtocolId, h)
	if err != nil {
		log.Errorf("[LiquidNet] [DirectMsgHandle] register handler failed, %s (chain id:%s, msg_flag:%s)",
			err.Error(), chainId, msgFlag)
		return err
	}
	return nil
}

// CancelDirectMsgHandle unregister a DirectMsgHandler.
// 		msgFlag: is a flag used to distinguish msg type.
func (l *LiquidNet) CancelDirectMsgHandle(chainId string, msgFlag string) error {
	// create net protocol id
	netProtocolId := CreateProtocolIdWithChainIdAndMsgFlag(chainId, msgFlag)
	// unregister handler
	err := l.host.UnregisterMsgPayloadHandler(netProtocolId)
	if err != nil {
		log.Errorf("[LiquidNet] [CancelDirectMsgHandle] caccel handler failed, %s (chain:%s, msg_flag:%s)",
			err.Error(), chainId, msgFlag)
	}
	return err
}

// AddSeed add a seed node addr.
func (l *LiquidNet) AddSeed(seed string) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.startUp {
		dp, err := ma.NewMultiaddr(seed)
		if err != nil {
			return err
		}
		if err := l.host.AddDirectPeer(dp); err != nil {
			return err
		}
	}
	return l.hostCfg.AddDirectPeer(seed)
}

// RefreshSeeds refresh the seed node addr list.
func (l *LiquidNet) RefreshSeeds(seeds []string) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.hostCfg.DirectPeers = make(map[peer.ID]ma.Multiaddr)
	if l.startUp {
		l.host.ClearDirectPeers()
	}
	for _, seed := range seeds {
		if err := l.hostCfg.AddDirectPeer(seed); err != nil {
			return err
		}
		if l.startUp {
			dp, err := ma.NewMultiaddr(seed)
			if err != nil {
				return err
			}
			if err := l.host.AddDirectPeer(dp); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetChainCustomTrustRoots set custom trust roots of chain.
// In cert permission mode, if it is failed when verifying cert by access control of chains,
// the cert will be verified by custom trust root pool again.
func (l *LiquidNet) SetChainCustomTrustRoots(chainId string, roots [][]byte) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.startUp {
		if !l.hostCfg.Insecurity {
			bl := l.tlsChainTrustRoots.RefreshRootsFromPem(chainId, roots)
			if !bl {
				log.Errorf("[LiquidNet] [SetChainCustomTrustRoots] set custom trust roots failed. (chainId: %s)", chainId)
				return
			}
		}
		return
	}
	l.cryptoCfg.SetCustomTrustRootCert(chainId, roots)
}

// setChainPubSubBlackPeer add a peer id into the blacklist of PubSub.
func (l *LiquidNet) setChainPubSubBlackPeer(chainId string, pid peer.ID) {
	v, bl := l.psMap.Load(chainId)
	if bl {
		v.(broadcast.PubSub).SetBlackPeer(pid)
	}
}

// removeChainPubSubBlackPeer remove a peer id from the blacklist of PubSub.
func (l *LiquidNet) removeChainPubSubBlackPeer(chainId string, pid peer.ID) {
	v, bl := l.psMap.Load(chainId)
	if bl {
		v.(broadcast.PubSub).RemoveBlackPeer(pid)
	}
}

// ReVerifyPeers will verify permission of peers existed with the access control module of the chain
// which id is the given chainId.
func (l *LiquidNet) ReVerifyPeers(chainId string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if !l.startUp {
		return
	}
	if l.hostCfg.Insecurity {
		return
	}
	var peerIdTlsCertOrPubKeyMap map[string][]byte
	if l.cryptoCfg.PubKeyMode {
		peerIdTlsCertOrPubKeyMap = l.peerIdPubKeyStore.StoreCopy()
	} else {
		peerIdTlsCertOrPubKeyMap = l.peerIdTlsCertStore.StoreCopy()
	}
	if len(peerIdTlsCertOrPubKeyMap) == 0 {
		return
	}

	// re verify exist peers
	existPeers := l.peerIdChainIdsRecorder.PeerIdsOfChain(chainId)
	for _, existPeerId := range existPeers {
		bytes, ok := peerIdTlsCertOrPubKeyMap[existPeerId]
		if ok {
			var passed bool
			var err error
			// verify member status
			if l.cryptoCfg.PubKeyMode {
				passed, err = utils.ChainMemberStatusValidateWithPubKeyMode(chainId, l.memberStatusValidator, bytes)
			} else {
				passed, err = utils.ChainMemberStatusValidateWithCertMode(chainId, l.memberStatusValidator, bytes)
			}
			if err != nil {
				log.Errorf("[LiquidNet][ReVerifyPeers] chain member status validate failed. %s", err.Error())
				continue
			}
			// if not passed, remove it from chain
			if !passed {
				l.peerIdChainIdsRecorder.RemovePeerChainId(existPeerId, chainId)
				log.Infof("[LiquidNet][ReVerifyPeers] remove peer from chain, (pid: %s, chain id: %s)",
					existPeerId, chainId)
				l.setChainPubSubBlackPeer(chainId, peer.ID(existPeerId))
			}
			delete(peerIdTlsCertOrPubKeyMap, existPeerId)
		} else {
			l.peerIdChainIdsRecorder.RemovePeerChainId(existPeerId, chainId)
			log.Infof("[LiquidNet][ReVerifyPeers] remove peer from chain, (pid: %s, chain id: %s)",
				existPeerId, chainId)
			l.setChainPubSubBlackPeer(chainId, peer.ID(existPeerId))
		}
	}
	// verify other peers
	for pid, bytes := range peerIdTlsCertOrPubKeyMap {
		var passed bool
		var err error
		// verify member status
		if l.cryptoCfg.PubKeyMode {
			passed, err = utils.ChainMemberStatusValidateWithPubKeyMode(chainId, l.memberStatusValidator, bytes)
		} else {
			passed, err = utils.ChainMemberStatusValidateWithCertMode(chainId, l.memberStatusValidator, bytes)
		}
		if err != nil {
			log.Errorf("[LiquidNet][ReVerifyPeers] chain member status validate failed. %s", err.Error())
			continue
		}
		// if passed, add it to chain
		if passed {
			l.peerIdChainIdsRecorder.AddPeerChainId(pid, chainId)
			log.Infof("[LiquidNet] [ReVerifyTrustRoots] add peer to chain, (pid: %s, chain id: %s)",
				pid, chainId)
			l.removeChainPubSubBlackPeer(chainId, peer.ID(pid))
		}
	}

	// close all connections of peers not belong to any chain
	for _, s := range l.peerIdChainIdsRecorder.PeerIdsOfNoChain() {
		pid := peer.ID(s)
		for c := l.host.ConnMgr().GetPeerConn(pid); c != nil; c = l.host.ConnMgr().GetPeerConn(pid) {
			_ = c.Close()
			log.Infof("[LiquidNet] [ReVerifyTrustRoots] close connection of peer %s", s)
			time.Sleep(time.Second)
		}
	}
}

// IsRunning return true when the net instance is running.
func (l *LiquidNet) IsRunning() bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.startUp
}

// confirmConfig check whether there is any problem with the configuration information of net
func (l *LiquidNet) confirmConfig() {
	log.Info("[LiquidNet] config confirming...")
	hc := l.hostCfg
	// check send stream pool init size
	if hc.SendStreamPoolInitSize <= 0 {
		log.Warnf("[LiquidNet] wrong init send stream size set, use default (set: %d, default: %d)",
			hc.SendStreamPoolInitSize, DefaultInitSendStreamSize)
		hc.SendStreamPoolInitSize = DefaultInitSendStreamSize
	}
	// check send stream pool cap
	if hc.SendStreamPoolCap <= 0 {
		log.Warnf("[LiquidNet] wrong send stream max count value set, use default (set: %d, default: %d)",
			hc.SendStreamPoolCap, DefaultSendStreamMaxCount)
		hc.SendStreamPoolCap = DefaultSendStreamMaxCount
	}
	// check max connection count each peer allowed
	if hc.MaxConnCountEachPeerAllowed <= 0 {
		log.Warnf("[LiquidNet] wrong max connection count value of each peer set, use default "+
			"(set: %d, default: %d)",
			hc.MaxConnCountEachPeerAllowed, DefaultMaxConnCountEachPeer)
		hc.MaxConnCountEachPeerAllowed = DefaultSendStreamMaxCount
	}
	// check receive stream max count
	if hc.PeerReceiveStreamMaxCount <= 0 {
		recommended := hc.SendStreamPoolCap * int32(hc.MaxConnCountEachPeerAllowed)
		log.Warnf("[LiquidNet] wrong receive stream max count value of each peer set, use recommended value "+
			"(set: %d, recommended: %d)",
			hc.PeerReceiveStreamMaxCount, recommended)
		hc.PeerReceiveStreamMaxCount = recommended
	}
	// check max peer count allowed to connection
	if hc.MaxPeerCountAllowed <= 0 {
		log.Warnf("[LiquidNet] wrong max peer count allowed value set, use default (set: %d, default: %d)",
			hc.MaxPeerCountAllowed, DefaultMaxPeerCount)
		hc.MaxPeerCountAllowed = DefaultMaxPeerCount
	}
	//  check that the elimination strategy is selected
	if hc.ConnEliminationStrategy <= 0 {
		log.Warnf("[LiquidNet] wrong connection elimination strategy value set, use default "+
			"(set: %d, default: %d)",
			hc.ConnEliminationStrategy, DefaultPeerEliminationStrategy)
		hc.ConnEliminationStrategy = DefaultPeerEliminationStrategy
	}
	log.Info("[LiquidNet] config confirmed.")
}

// setUpChainTrustRoots set chain trust roots
func (l *LiquidNet) setUpChainTrustRoots() error {
	log.Info("[LiquidNet] chain trust roots setting...")
	if !l.hostCfg.Insecurity {
		// if tls enabled
		if len(l.cryptoCfg.CustomChainTrustRootCertsBytes) == 0 {
			log.Warn("[LiquidNet] no trust root certs found. use default security.")
		} else {
			trustRoots, err := cmTlsS.BuildTlsTrustRoots(l.cryptoCfg.CustomChainTrustRootCertsBytes)
			if err != nil {
				log.Errorf("[LiquidNet] build tls trust root failed, %s", err.Error())
				return err
			}
			l.tlsChainTrustRoots = trustRoots
		}
	}
	log.Info("[LiquidNet] chain trust roots set up.")
	return nil
}

// setUpTlsConfig set some configuration information related to TLS
func (l *LiquidNet) setUpTlsConfig(netType lHost.NetworkType) error {
	log.Info("[LiquidNet] tls config setting...")
	// tls cert validator
	tcv := cmTlsS.NewCertValidator(l.cryptoCfg.PubKeyMode, l.memberStatusValidator, l.tlsChainTrustRoots)
	l.tlsCertValidator = tcv

	if l.cryptoCfg.PubKeyMode {
		// get private key
		privateKey, err := asym.PrivateKeyFromPEM(l.cryptoCfg.KeyBytes, nil)
		if err != nil {
			return err
		}
		pid, err := helper.CreateLibp2pPeerIdWithPrivateKey(privateKey)
		if err != nil {
			return err
		}
		// store pub key
		pkPem, err := privateKey.PublicKey().String()
		if err != nil {
			return err
		}
		l.peerIdPubKeyStore.SetPeerPubKey(pid, []byte(pkPem))
		// create tls cert
		if netType == lHost.QuicNetwork {
			l.hostCfg.TlsCfg, err = cmTlsS.NewTlsConfigWithPubKeyMode4Quic(privateKey, l.tlsCertValidator)
		} else {
			l.hostCfg.TlsCfg, err = cmTlsS.NewTlsConfigWithPubKeyMode(privateKey, l.tlsCertValidator)
		}
		if err != nil {
			return err
		}

	} else {
		var (
			tlsCerts  []tls.Certificate
			peerId    string
			err, err1 error

			tlsCert, tlsEncCert *tls.Certificate
		)

		// create tls cert
		if netType == lHost.QuicNetwork {
			tlsCert, peerId, err = cmTlsS.GetCertAndPeerIdWithKeyPair4Quic(l.cryptoCfg.CertBytes, l.cryptoCfg.KeyBytes)
			if l.cryptoCfg.EncCertBytes != nil && l.cryptoCfg.EncKeyBytes != nil {
				tlsEncCert, _, err1 = cmTlsS.GetCertAndPeerIdWithKeyPair4Quic(l.cryptoCfg.EncCertBytes, l.cryptoCfg.EncKeyBytes)
			}
		} else {
			tlsCert, peerId, err = cmTlsS.GetCertAndPeerIdWithKeyPair(l.cryptoCfg.CertBytes, l.cryptoCfg.KeyBytes)
			if l.cryptoCfg.EncCertBytes != nil && l.cryptoCfg.EncKeyBytes != nil {
				tlsEncCert, _, err1 = cmTlsS.GetCertAndPeerIdWithKeyPair(l.cryptoCfg.EncCertBytes, l.cryptoCfg.EncKeyBytes)
			}
		}
		// store tls cert
		if err != nil {
			return err
		}
		tlsCerts = append(tlsCerts, *tlsCert)

		//load gmtls enc keypair
		if err1 == nil && tlsEncCert != nil {
			tlsCerts = append(tlsCerts, *tlsEncCert)
			log.Info("[LiquidNet] tls enc certificate is set, use gmtls")
		}

		l.peerIdTlsCertStore.SetPeerTlsCert(peerId, tlsCert.Certificate[0])
		// create tls config
		l.hostCfg.TlsCfg, err = cmTlsS.NewTlsConfigWithCertMode(tlsCerts, l.tlsCertValidator)
		if err != nil {
			return err
		}
	}
	l.hostCfg.LoadPidFunc = tlssupport.PeerIdFunction()
	log.Info("[LiquidNet] tls config set up.")
	return nil
}

// Start the local net.
func (l *LiquidNet) Start() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	var err error

	if l.startUp {
		return ErrorNetRunning
	}

	// confirm config
	l.confirmConfig()

	// load private key
	l.hostCfg.PrivateKey, err = asym.PrivateKeyFromPEM(l.cryptoCfg.KeyBytes, nil)
	if err != nil {
		return err
	}

	// confirm network type
	var netType lHost.NetworkType
	firstListenAddr := l.hostCfg.ListenAddresses[0]
	netType = lHost.ConfirmNetworkTypeByAddr(firstListenAddr)
	if netType == lHost.UnknownNetwork {
		return ErrorWrongAddressOrUnsupported
	}
	log.Infof("[LiquidNet] network type: %s", netType)

	// set upt chain trust roots
	err = l.setUpChainTrustRoots()
	if err != nil {
		return err
	}

	err = l.setUpTlsConfig(netType)
	if err != nil {
		return err
	}

	// create new host
	newHost, err := l.hostCfg.NewHost(l.context, netType, log)
	if err != nil {
		return err
	}

	l.host = newHost
	// bind notifiee
	l.bindNotifiee()

	// pkt adapter
	if l.extensionsCfg.EnablePkt {
		log.Infof("[LiquidNet] Pkt enabled.")
		l.pktAdapter = newPktAdapter(l.host, log)
		l.pktAdapter.run()
		err = l.host.RegisterMsgPayloadHandler(pktProtocol, l.pktAdapter.msgPayloadFunc)
		if err != nil {
			return err
		}
	}

	// priority blocker
	if l.extensionsCfg.EnablePriorityCtrl {
		log.Infof("[LiquidNet] priority control enabled.")
		l.priorityController = priorityblocker.NewBlocker(nil)
		l.priorityController.Run()
	}

	// start host
	err = newHost.Start()
	if err != nil {
		log.Errorf("[LiquidNet] host start failed, %s", err.Error())
		return err
	}

	// set up discovery service
	l.discoveryService, err = protocoldiscovery.NewProtocolBasedDiscovery(
		l.host,
		protocoldiscovery.WithLogger(log),
		protocoldiscovery.WithMaxQuerySize(3),
	)
	if err != nil {
		log.Errorf("[LiquidNet] set up discovery service failed, %s", err.Error())
		return err
	}

	// stun client
	if l.stunClientCfg.Enable {
		err = l.initStunClient(l.context, netType)
		if err != nil {
			log.Errorf("[LiquidNet] initStunClient failed, %s", err.Error())
			return err
		}
		log.Info("[LiquidNet] initStunClient ok")
	}

	//stun server
	if l.stunServerCfg.EnableServer {
		err = l.initStunServer(l.context, netType)
		if err != nil {
			log.Errorf("[LiquidNet] initStunServer failed, %s", err.Error())
			return err
		}
		log.Info("[LiquidNet] initStunServer ok")
	}

	//hole punch
	if l.holePunchCfg.EnablePunch {
		l.holePunch, _ = holepunch.NewHolePunch(log)
		_ = l.holePunch.SetHost(l.host)
		err = l.host.RegisterMsgPayloadHandler(protocolHolePunch, l.holePunch.HandlePeerDisconnect)
		if err != nil {
			log.Errorf("[LiquidNet] NewHolePunch failed, %s", err.Error())
			return err
		}
		log.Info("[LiquidNet] NewHolePunch ok")
	}

	log.Info("[LiquidNet] discovery service set up.")
	l.startUp = true
	return err
}

// resetChainPubSubBlackPeerWithPid reset some blacklist operations for the specified peer
func (l *LiquidNet) resetChainPubSubBlackPeerWithPid(pidStr string) {
	l.psMap.Range(func(key, value interface{}) bool {
		chainId, ok := key.(string)
		if !ok {
			return false
		}
		ps, _ := value.(broadcast.PubSub)
		if l.peerIdChainIdsRecorder.IsPeerBelongToChain(pidStr, chainId) {
			ps.RemoveBlackPeer(peer.ID(pidStr))
		} else {
			ps.SetBlackPeer(peer.ID(pidStr))
		}
		return true
	})
}

// queryAndStoreDerivedInfoInCertValidator query derived info from cert validator based on peer ID
func (l *LiquidNet) queryAndStoreDerivedInfoInCertValidator(peerIdStr string) {
	if l.hostCfg.Insecurity {
		return
	}
	derivedInfo := l.tlsCertValidator.QueryDerivedInfoWithPeerId(peerIdStr)
	if derivedInfo != nil {
		l.peerIdTlsCertStore.SetPeerTlsCert(derivedInfo.PeerId, derivedInfo.TlsCertBytes)
		l.peerIdPubKeyStore.SetPeerPubKey(derivedInfo.PeerId, derivedInfo.PubKeyBytes)
		for i := range derivedInfo.ChainIds {
			l.peerIdChainIdsRecorder.AddPeerChainId(derivedInfo.PeerId, derivedInfo.ChainIds[i])
		}
		l.certIdPeerIdMapper.Add(derivedInfo.CertId, derivedInfo.PeerId)
		l.resetChainPubSubBlackPeerWithPid(derivedInfo.PeerId)
	} else {
		log.Warnf("[LiquidNet] no derived info found from tls cert validator! (pid: %s)", peerIdStr)
	}
}

// bindNotifiee create a notifiee bundle then register it to the host.
func (l *LiquidNet) bindNotifiee() {
	notifieeBundle := &host.NotifieeBundle{
		PeerConnectedFunc: func(id peer.ID) {
			l.lock.Lock()
			defer l.lock.Unlock()
			peerIdStr := id.ToString()
			log.Debugf("[LiquidNet] peer connected. (pid: %s)", peerIdStr)
			l.queryAndStoreDerivedInfoInCertValidator(peerIdStr)
		},
		PeerDisconnectedFunc: func(peerId peer.ID) {
			l.lock.Lock()
			defer l.lock.Unlock()
			peerIdStr := peerId.ToString()
			log.Debugf("[LiquidNet] peer disconnect. (pid: %s)", peerIdStr)
			l.peerIdChainIdsRecorder.RemoveAllByPeerId(peerIdStr)
			l.peerIdTlsCertStore.RemoveByPeerId(peerIdStr)
			l.peerIdPubKeyStore.RemoveByPeerId(peerIdStr)
			l.certIdPeerIdMapper.RemoveByPeerId(peerIdStr)
			l.handlePeerDisconnect(peerIdStr)
		},
	}
	l.host.Notify(notifieeBundle)
}

// handlePeerDisconnect invoke when peer is disconnect
// guess the peer offline
// notify holePunch should stop try
func (l *LiquidNet) handlePeerDisconnect(peerId string) {
	peerIDs := l.host.ConnMgr().AllPeer()
	log.Debug("notify (", peerIDs, ") ", peerId, " is offline ")
	for _, peer := range peerIDs {
		_ = l.host.SendMsg(protocolHolePunch, peer, []byte(peerId))
	}
}

// listenFindingChanTask a continuously running background task
// that handles peers discovered by the node discovery service.
func (l *LiquidNet) listenFindingChanTask(c <-chan ma.Multiaddr) {
	for {
		select {
		case <-l.context.Done():
			return
		case ai := <-c:
			addr, pid := util.GetNetAddrAndPidFromNormalMultiAddr(ai)
			if pid == "" {
				log.Errorf("[LiquidNet] [Discovery] peer id not contains in addr")
				continue
			}
			if l.host.ConnMgr().PeerCount() >= l.host.ConnMgr().MaxPeerCountAllowed() ||
				l.host.ID() == pid ||
				l.host.ConnMgr().IsConnected(pid) {
				continue
			}
			log.Infof("[LiquidNet] [Discovery] find new peer.(pid: %s, addr: %s)", pid, addr.String())
			_, errD := l.host.Dial(ai)
			if errD == nil {
				continue
			}
			if errD == tcp.ErrConnRejectedByConnHandler || errD == quic.ErrConnRejectedByConnHandler {
				continue
			}
			//get nat type and
			log.Infof("[LiquidNet] [nat] find new peer.(pid: %s, addr: %s) Dial err:%v", pid, addr.String(), errD)
			log.Debugf("[LiquidNet] [nat] peerId:%v  stunClientCfg:%v", l.host.ID().ToString(), l.stunClientCfg)
			localNatType, peerNatType := l.StunCheck(pid)
			//if holePunch is available
			bCanPunch := l.holePunch.HolePunchAvailable(localNatType, peerNatType)
			if !bCanPunch {
				continue
			}
			_ = l.HolePunch(pid, ai)
		}
	}
}

// StunCheck invoke stun func
func (l *LiquidNet) StunCheck(pid peer.ID) (stun.NATDeviceType, stun.NATDeviceType) {
	if !l.stunClientCfg.Enable {
		return stun.NATTypeUnknown, stun.NATTypeUnknown
	}
	localPID := l.host.ID().ToString()
	clientAddr := l.stunClientCfg.ClientListenAddr
	serverAddr := l.stunClientCfg.StunServerAddr
	localNatType, err1 := l.stunClient.CheckNatType(localPID, clientAddr, serverAddr)
	if err1 != nil {
		log.Warn("stunClient.CheckNatType err:", err1.Error())
	}
	peerNatType, err2 := l.stunClient.GetPeerNatType(pid.ToString(), clientAddr, serverAddr)
	if err2 != nil {
		log.Warn("stunClient.GetPeerNatType err:", err2.Error())
	}
	return localNatType, peerNatType
}

// HolePunch try hole punch
func (l *LiquidNet) HolePunch(pid peer.ID, addr ma.Multiaddr) error {
	if !l.holePunchCfg.EnablePunch {
		return errors.New("!EnablePunch")
	}
	if addr == nil {
		return errors.New("addr == nil")
	}
	if relay.IsRelayAddr(addr) {
		return errors.New("IsRelayAddr")
	}
	go func() {
		log.Infof("[LiquidNet] holePunch peer.(pid: %s, addr: %s)", pid, addr.String())
		exAddr := &exMultiaddr.ExInfoMultiaddr{
			Multiaddr: addr,
			Info:      "holePunch",
		}
		conn, holePunchErr := l.holePunch.DirectConnect(exAddr)
		if holePunchErr == nil {
			log.Infof("[LiquidNet] holePunch success (localAddr %v)"+
				"peer.(pid: %s, remoteAddr: %s)", conn.LocalAddr().String(), pid, conn.RemoteAddr().String())
		} else {
			log.Infof("[LiquidNet] holePunch to(%v) err:%v", addr.String(), holePunchErr)
		}
	}()

	return nil
}

// attachDiscovery start the node discovery service
func (l *LiquidNet) attachDiscovery(chainId string) error {
	var err error
	// discovery announce
	err = l.discoveryService.Announce(l.context, chainId)
	if err != nil {
		return err
	}
	log.Infof("[LiquidNet] chain service announced. (chain-id: %s)", chainId)
	// discovery finding
	var findingC <-chan ma.Multiaddr
	findingC, err = l.discoveryService.FindPeers(l.context, chainId, protocoldiscovery.WithQuerySize(3))
	if err != nil {
		return err
	}
	go l.listenFindingChanTask(findingC)
	log.Infof("[LiquidNet] chain peers finding... (chain-id: %s)", chainId)
	return nil
}

// Stop the local net.
func (l *LiquidNet) Stop() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	log.Infof("[LiquidNet] stopping...")
	l.startUp = false

	l.psMap.Range(func(key, value interface{}) bool {
		_ = value.(broadcast.PubSub).Stop()
		return true
	})
	l.psMap = sync.Map{}
	log.Infof("[LiquidNet] pubsub cleaned.")

	log.Infof("[LiquidNet] host stopping...")
	if l.pktAdapter != nil {
		l.pktAdapter.cancel()
	}
	err := l.host.Stop()
	if err != nil {
		log.Infof("[LiquidNet] [Stop] stop host error. err:%v", err)
		return err
	}
	if l.stunServer != nil {
		_ = l.stunServer.CloseListen()
	}
	log.Infof("[LiquidNet] host stopped.")
	log.Infof("[LiquidNet] stopped.")
	return nil
}

// ChainNodesInfo return base node info list of chain which id is the given chainId.
func (l *LiquidNet) ChainNodesInfo(chainId string) ([]*api.ChainNodeInfo, error) {
	result := make([]*api.ChainNodeInfo, 0)
	if !l.hostCfg.Insecurity {
		peerIds := make([]string, 0)
		peerIds = append(peerIds, l.GetNodeUid())
		peerIds = append(peerIds, l.peerIdChainIdsRecorder.PeerIdsOfChain(chainId)...)
		for _, peerId := range peerIds {
			pid := peerId
			addrs := make([]string, 0)
			if pid == l.GetNodeUid() {
				for _, addr := range l.host.LocalAddresses() {
					addrs = append(addrs, addr.String())
				}
			} else {
				for _, addr := range l.host.PeerStore().GetAddrs(peer.ID(pid)) {
					addrs = append(addrs, addr.String())
				}
			}
			cert := l.peerIdTlsCertStore.GetCertByPeerId(peerId)
			result = append(result, &api.ChainNodeInfo{
				NodeUid:     peerId,
				NodeAddress: addrs,
				NodeTlsCert: cert,
			})
		}
	}
	return result, nil
}

// GetNodeUidByCertId return node uid which mapped to the given cert id. If unmapped return error.
func (l *LiquidNet) GetNodeUidByCertId(certId string) (string, error) {
	nodeUid, err := l.certIdPeerIdMapper.FindPeerIdByCertId(certId)
	if err != nil {
		return "", err
	}
	return nodeUid, nil
}

// AddAC add a AccessControlProvider for revoked validator.
func (l *LiquidNet) AddAC(chainId string, ac api.AccessControlProvider) {
	l.memberStatusValidator.AddAC(chainId, ac)
}

// SetMsgPriority set the priority of the msg flag.
// If priority control disabled, it is no-op.
func (l *LiquidNet) SetMsgPriority(msgFlag string, priority uint8) {
	if l.priorityController != nil {
		l.priorityController.SetPriority(msgFlag, priorityblocker.Priority(priority))
	}
}

// initStunClient init stun client by network type
func (l *LiquidNet) initStunClient(ctx context.Context, networkType lHost.NetworkType) error {
	netType := lHost.NetworkType(l.stunClientCfg.NetworkType)

	var err error
	if netType == udp {
		log.Debug("initStunClient udp")
		l.stunClient, err = stun.NewUdpStunClient(log)
	} else {
		log.Debug("initStunClient default")
		l.stunClient, err = stun.NewDefaultStunClient(log)
	}

	if err != nil {
		return err
	}

	iNetwork, err := l.newNetwork(ctx, netType, log)
	if err != nil {
		return err
	}
	err = l.stunClient.SetNetwork(iNetwork)
	return err
}

// initStunServer init stun server by network type
func (l *LiquidNet) initStunServer(ctx context.Context, networkType lHost.NetworkType) error {

	var err error
	if l.stunServerCfg.NetworkType == udp {
		log.Debug("initStunServer udp")
		l.stunServer, err = stun.NewUdpStunServer(log)
	} else {
		log.Debug("initStunServer default")
		l.stunServer, err = stun.NewDefaultStunServer(log)
	}

	if err != nil {
		return err
	}

	iNetwork1, err1 := l.newNetwork(ctx, networkType, log)
	if err1 != nil {
		return err1
	}
	iNetwork2, err2 := l.newNetwork(ctx, networkType, log)
	if err2 != nil {
		return err2
	}
	err = l.stunServer.SetNetwork(iNetwork1, iNetwork2)
	if err != nil {
		return err
	}

	iNetwork3, err3 := l.newNetwork(ctx, networkType, log)
	if err3 != nil {
		return err3
	}
	iNetwork4, err4 := l.newNetwork(ctx, networkType, log)
	if err4 != nil {
		return err4
	}
	if l.stunServerCfg.TwoPublicAddress {
		changeParam := &stun.ChangeParam{
			OtherAddr:             l.stunServerCfg.OtherStunServerAddr,
			OtherMasterListenAddr: l.stunServerCfg.ServerListenAddr3,
			OtherSlaveListenAddr:  l.stunServerCfg.ServerListenAddr4,
			OtherMasterNetwork:    iNetwork3,
			OtherSlaveNetwork:     iNetwork4,
		}
		err := l.stunServer.InitChangeIpLocal(changeParam)
		if err != nil {
			return err
		}
	} else {
		changeParam := &stun.ChangeNotifyParam{
			OtherAddr:       l.stunServerCfg.OtherStunServerAddr,
			NotifyAddr:      l.stunServerCfg.NotifyAddr,
			LocalNotifyAddr: l.stunServerCfg.LocalNotifyAddr,
		}
		err := l.stunServer.InitChangeIpNotify(changeParam)
		if err != nil {
			return err
		}
	}

	return l.stunServer.Listen(l.stunServerCfg.ServerListenAddr1, l.stunServerCfg.ServerListenAddr2)
}

// newNetwork create the corresponding network object according to the network communication type
func (l *LiquidNet) newNetwork(ctx context.Context, networkType lHost.NetworkType,
	logger api.Logger) (network.Network, error) {
	cfg := l.hostCfg
	switch networkType {
	case lHost.TcpNetwork:
		return tcp.NewNetwork(ctx, logger,
			tcp.WithTlsCfg(cfg.TlsCfg),
			tcp.WithLoadPidFunc(cfg.LoadPidFunc),
			tcp.WithEnableTls(cfg.Insecurity),
			tcp.WithLocalPeerId(l.host.ID()),
		)
	default:
		return quic.NewNetwork(ctx, logger,
			quic.WithTlsCfg(cfg.TlsCfg),
			quic.WithLoadPidFunc(cfg.LoadPidFunc),
			quic.WithLocalPeerId(l.host.ID()),
		)
	}
}
