/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"strconv"
	"time"

	"chainmaker.org/chainmaker/common/v2/helper"
	libP2pPubSub "chainmaker.org/chainmaker/libp2p-pubsub"
	"chainmaker.org/chainmaker/net-common/common"

	//"chainmaker.org/chainmaker/net-common/common/pool"
	"chainmaker.org/chainmaker/net-common/common/priorityblocker"
	"chainmaker.org/chainmaker/net-common/utils"
	"chainmaker.org/chainmaker/net-libp2p/datapackage"
	api "chainmaker.org/chainmaker/protocol/v2"
	"chainmaker.org/chainmaker/recorderfile"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const (
	// DefaultLibp2pListenAddress is the default address that libp2p will listen on.
	DefaultLibp2pListenAddress = "/ip4/0.0.0.0/tcp/0"
	// DefaultLibp2pServiceTag is the default service tag for discovery service finding.
	DefaultLibp2pServiceTag = "chainmaker-libp2p-net"
	// MaxReadBuff max []byte to make once
	MaxReadBuff = 1 << 34 // 17G
)

var (
	ErrorPubSubNotExist      = errors.New("pub-sub service not exist")
	ErrorPubSubExisted       = errors.New("pub-sub service existed")
	ErrorTopicSubscribed     = errors.New("topic has been subscribed")
	ErrorSendMsgIncompletely = errors.New("send msg incompletely")
	ErrorNotConnected        = errors.New("node not connected")
	ErrorNotBelongToChain    = errors.New("node not belong to chain")
)

// MsgPID is the protocol.ID of chainmaker net msg.
const MsgPID = protocol.ID("/ChainMakerNetMsg/1.0.0/")

// DefaultMessageSendTimeout is the default timeout for sending msg.
const DefaultMessageSendTimeout = 3 * time.Second

// compressThreshold is the default threshold value for enable compress net msg bytes. Default value is 1M.
const compressThreshold = 1024 * 1024

const pubSubWhiteListChanCap = 50
const pubSubWhiteListChanQuitCheckDelay = 10

// training Interval for refreshing the whitelist
const refreshPubSubWhiteListTickerTime = time.Duration(time.Second * 120)

// ask the neighbor peer for the timeout of the target peer
const findPeerTimout = time.Second * 60

//
const sendDefaultSize = 200
const sendDefaultTime = time.Second * 10

var _ api.Net = (*LibP2pNet)(nil)

// LibP2pNet is an implementation of net.Net interface.
type LibP2pNet struct {
	compressMsgBytes          bool
	lock                      sync.RWMutex
	startUp                   bool
	netType                   api.NetType
	ctx                       context.Context // ctx context.Context
	libP2pHost                *LibP2pHost     // libP2pHost is a LibP2pHost instance.
	messageHandlerDistributor *MessageHandlerDistributor
	pktAdapter                *pktAdapter
	priorityController        *priorityblocker.Blocker

	pubSubs          sync.Map                      // map[string]*LibP2pPubSub , map[chainId]*LibP2pPubSub
	subscribedTopics map[string]*topicSubscription // map[chainId]*topicSubscription
	subscribeLock    sync.Mutex

	reloadChainPubSubWhiteListSignalChanMap sync.Map // map[chainId]chan struct{}

	prepare *LibP2pNetPrepare // prepare contains the base info for the net starting.

	//bytesPool *pool.ByteSlicesPool

	log api.Logger

	alarmBell *bell
}

func (ln *LibP2pNet) SetCompressMsgBytes(enable bool) {
	ln.libP2pHost.compressMsgBytes = enable
}

type topicSubscription struct {
	m map[string]*libP2pPubSub.Subscription
}

func (ln *LibP2pNet) peerChainIdsRecorder() *common.PeerIdChainIdsRecorder {
	return ln.libP2pHost.peerChainIdsRecorder
}

// NewLibP2pNet create a new LibP2pNet instance.
func NewLibP2pNet(log api.Logger) (*LibP2pNet, error) {
	ctx := context.Background()
	host := NewLibP2pHost(ctx, log)
	net := &LibP2pNet{
		startUp:                                 false,
		netType:                                 api.Libp2p,
		ctx:                                     ctx,
		libP2pHost:                              host,
		messageHandlerDistributor:               newMessageHandlerDistributor(),
		pktAdapter:                              nil,
		pubSubs:                                 sync.Map{},
		subscribedTopics:                        make(map[string]*topicSubscription),
		reloadChainPubSubWhiteListSignalChanMap: sync.Map{},

		prepare: &LibP2pNetPrepare{
			listenAddr:              DefaultLibp2pListenAddress,
			bootstrapsPeers:         make(map[string]struct{}),
			maxPeerCountAllow:       DefaultMaxPeerCountAllow,
			peerEliminationStrategy: int(LIFO),

			blackAddresses: make(map[string]struct{}),
			blackPeerIds:   make(map[string]struct{}),
			pktEnable:      false,
		},
		log: log,

		alarmBell: newBell(sendDefaultTime, sendDefaultSize),
		//bytesPool: pool.NewByteSlicesPool(8),
	}
	go net.alarmBell.start(ctx)
	return net, nil
}

func (ln *LibP2pNet) Prepare() *LibP2pNetPrepare {
	return ln.prepare
}

// GetNodeUid is the unique id of node.
func (ln *LibP2pNet) GetNodeUid() string {
	return ln.libP2pHost.Host().ID().Pretty()
}

// isSubscribed return true if the given topic given has subscribed.Otherwise, return false.
func (ln *LibP2pNet) isSubscribed(chainId string, topic string) bool {
	topics, ok := ln.subscribedTopics[chainId]
	if !ok {
		return false
	}
	_, ok = topics.m[topic]
	return ok
}

// getPubSub return the LibP2pPubSub instance which uid equal the given chainId .
func (ln *LibP2pNet) getPubSub(chainId string) (*LibP2pPubSub, bool) {
	ps, ok := ln.pubSubs.Load(chainId)
	var pubsub *LibP2pPubSub = nil
	if ok {
		pubsub = ps.(*LibP2pPubSub)
	}
	return pubsub, ok
}

// InitPubSub will create new LibP2pPubSub instance for LibP2pNet with setting pub-sub uid to the given chainId .
func (ln *LibP2pNet) InitPubSub(chainId string, maxMessageSize int) error {
	if !ln.startUp {
		return errors.New("start net first pls")
	}
	_, ok := ln.getPubSub(chainId)
	if ok {
		return ErrorPubSubExisted
	}
	if maxMessageSize <= 0 {
		maxMessageSize = DefaultLibp2pPubSubMaxMessageSize
	}
	ps, err := NewPubsub(chainId, ln.libP2pHost, maxMessageSize)
	if err != nil {
		ln.log.Errorf("[Net] new pubsub failed, %s", err.Error())
		return err
	}
	ln.pubSubs.Store(chainId, ps)

	if err = ps.Start(); err != nil {
		return err
	}

	go ln.reloadChainPubSubWhiteListLoop(chainId, ps)
	ln.reloadChainPubSubWhiteList(chainId)

	// the loop for refreshing the whitelist
	go ln.checkPubsubWhitelistLoop(chainId, ps)

	return nil
}

//UpdateNetConfig update only maxPeerCountAllow for now
func (ln *LibP2pNet) UpdateNetConfig(param string, value string) error {
	switch param {
	case "MaxPeerCountAllow":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		ln.libP2pHost.connManager.SetMaxSize(v)
	case "Connectors", "HeartbeatInterval", "GossipRetransmission", "OpportunisticGraftPeers",
		"Dout", "Dlazy", "GossipFactor", "OpportunisticGraftTicks", "FloodPublish":
		ln.pubSubs.Range(func(chainId, pub interface{}) bool {
			var pubsub *LibP2pPubSub = nil
			pubsub = pub.(*LibP2pPubSub)
			pubsub.UpdatePubSubConfig(param, value)
			return true
		})
	default:
		return nil
	}
	return nil
}

// BroadcastWithChainId broadcast a msg to the given topic of the target chain which id is the given chainId .
func (ln *LibP2pNet) BroadcastWithChainId(chainId string, topic string, data []byte) error {
	topic = chainId + "_" + topic
	bytes := data
	pubSub, ok := ln.getPubSub(chainId)
	if !ok {
		return ErrorPubSubNotExist
	}
	return pubSub.Publish(topic, bytes) //publish msg
}

// getSubscribeTopicMap
func (ln *LibP2pNet) getSubscribeTopicMap(chainId string) *topicSubscription {
	topics, ok := ln.subscribedTopics[chainId]
	if !ok {
		ln.subscribedTopics[chainId] = &topicSubscription{
			m: make(map[string]*libP2pPubSub.Subscription),
		}
		topics = ln.subscribedTopics[chainId]
	}
	return topics
}

// SubscribeWithChainId subscribe the given topic of the target chain which id is
// the given chainId with the given sub-msg handler function.
func (ln *LibP2pNet) SubscribeWithChainId(chainId string, topic string, handler api.PubSubMsgHandler) error {
	ln.subscribeLock.Lock()
	defer ln.subscribeLock.Unlock()
	topic = chainId + "_" + topic
	// whether pubsub existed
	pubsub, ok := ln.getPubSub(chainId)
	if !ok {
		return ErrorPubSubNotExist
	}
	// whether subscribed
	if ln.isSubscribed(chainId, topic) { //检查topic是否已被订阅
		return ErrorTopicSubscribed
	}
	topicSub, err := pubsub.Subscribe(topic) // subscribe the topic
	if err != nil {
		return err
	}
	// add subscribe info
	topics := ln.getSubscribeTopicMap(chainId)
	topics.m[topic] = topicSub
	// run a new goroutine to handle the msg from the topic subscribed.
	go func() {
		defer func() {
			if err := recover(); err != nil {
				if !ln.isSubscribed(chainId, topic) {
					return
				}
				ln.log.Errorf("[Net] subscribe goroutine recover err, %s", err)
			}
		}()
		ln.topicSubLoop(chainId, topicSub, topic, handler)
	}()
	// reload chain pub-sub whitelist
	ln.reloadChainPubSubWhiteList(chainId)
	return nil
}

func (ln *LibP2pNet) topicSubLoop(
	chainId string,
	topicSub *libP2pPubSub.Subscription,
	topic string,
	handler api.PubSubMsgHandler) {
	for {
		message, err := topicSub.Next(ln.ctx)
		if err != nil {
			if err.Error() == "subscription cancelled" {
				ln.log.Warn("[Net] ", err)
				break
			}
			//logger
			ln.log.Errorf("[Net] subscribe next failed, %s", err.Error())
		}
		if message == nil {
			return
		}

		go func(msg *libP2pPubSub.Message) {
			// if author of the msg is myself , just skip and continue
			if message.ReceivedFrom == ln.libP2pHost.host.ID() || message.GetFrom() == ln.libP2pHost.host.ID() {
				return
			}
			// if author of the msg not belong to this chain, drop it
			// if !ln.peerChainIdsRecorder().IsPeerBelongToChain(message.GetFrom().Pretty(), chainId) {
			// 	return
			// }

			// if sender of the msg not belong to this chain, drop it
			if !ln.peerChainIdsRecorder().IsPeerBelongToChain(message.ReceivedFrom.Pretty(), chainId) {
				return
			}

			bytes := message.GetData()
			ln.log.Debugf("[Net] receive subscribed msg(topic:%s), data size:%d", topic, len(bytes))
			// call handler
			if err = handler(message.GetFrom().Pretty(), bytes); err != nil {
				ln.log.Warnf("[Net] call subscribe handler failed, %s ", err)
			}
		}(message)
	}
}

// CancelSubscribeWithChainId cancel subscribing the given topic of the target chain which id is the given chainId.
func (ln *LibP2pNet) CancelSubscribeWithChainId(chainId string, topic string) error {
	ln.subscribeLock.Lock()
	defer ln.subscribeLock.Unlock()
	topic = chainId + "_" + topic
	_, ok := ln.getPubSub(chainId)
	if !ok {
		return ErrorPubSubNotExist
	}
	topics := ln.getSubscribeTopicMap(chainId)
	if topicSub, ok := topics.m[topic]; ok {
		topicSub.Cancel()
		delete(topics.m, topic)
	}
	return nil
}

func (ln *LibP2pNet) isConnected(node string) (bool, peer.ID, error) {
	isConnected := false
	pid, err := peer.Decode(node) // peerId
	if err != nil {
		return false, pid, err
	}
	isConnected = ln.libP2pHost.HasConnected(pid)
	return isConnected, pid, nil

}

func (ln *LibP2pNet) sendMsg(chainId string, pid peer.ID, msgFlag string, data []byte) error {
	var (
		dataBytes []byte
		err       error
	)
	pkg := datapackage.NewPackage(utils.CreateProtocolWithChainIdAndFlag(chainId, msgFlag), data)

	if ln.libP2pHost.compressMsgBytes && len(pkg.Payload()) > compressThreshold {
		dataBytes, err = pkg.ToBytes(true)
		if err != nil {
			ln.log.Warnf("[PeerSendMsgHandler] marshal the data package failed, err: [%s]", err.Error())
			return err
		}
	} else {
		dataBytes, err = pkg.ToBytes(false)
		if err != nil {
			ln.log.Warnf("[PeerSendMsgHandler] marshal the data package failed, err: [%s]", err.Error())
			return err
		}
	}

	var (
		psh *peerSendMsgHandler
		ok  bool
	)

	//ln.libP2pHost.peersMsgChanLock.RLock()
	//defer ln.libP2pHost.peersMsgChanLock.RUnlock()

	//psh, ok = ln.libP2pHost.peersMsgChanMgr[pid.Pretty()]
	psh, ok = ln.libP2pHost.peersMsgChanMgr.GetStream(pid.Pretty())
	if !ok {
		ln.log.Info("[Net] GetStream return nil, peer:", pid.Pretty())
		return nil
	}

	if psh == nil {
		return errors.New("psh is nil")
	}
	dataChan := psh.getDataChan()
	if dataChan == nil {
		return errors.New("dataChan is nil")
	}

	select {
	case dataChan <- dataBytes:
		ln.log.Debugf("put the msg into the peer stream chan, peer: [%s]", pid.Pretty())
	case <-psh.ctx.Done():
		ln.log.Info("use ctx.Done() chan, peer:", pid.Pretty())
		//default:
		//if ln.alarmBell.needAlarm() {
		//	ln.log.Warnf("the peer stream channel is full, peer: [%s]", pid.Pretty())
		//}
		//ln.log.Debugf("the peer stream channel is full, peer: [%s]", pid.Pretty())
		//return errors.New("the peer stream channel is full")
	}

	return nil
}

// SendMsg send a msg to the given node belong to the given chain.
func (ln *LibP2pNet) SendMsg(chainId string, node string, msgFlag string, data []byte) error {
	if node == ln.GetNodeUid() {
		ln.log.Warn("[Net] can not send msg to self")
		return nil
	}
	if ln.priorityController != nil {
		ln.priorityController.Block(msgFlag)
	}

	isConnected, pid, _ := ln.isConnected(node)
	if !isConnected { // is peer connected
		// If there is no direct connection to the peer, ask the neighbor peer.
		// find the termination condition of the target peer:
		// 1.find timeout.
		// 2.target peer is found.
		// 3.all neighbors have been queried, but the target peer is not found.
		// when the target node is found, it will actively establish a connection
		// and return the peer.AddrInfo corresponding to the node,
		// otherwise an error will be returned.
		// if there are many peers on the network, the query process may be lengthy.
		// It is recommended to set a timeout period.
		//ctx, cancel := context.WithTimeout(context.TODO(), findPeerTimout)
		//defer cancel()
		//_, err := ln.libP2pHost.DHT().FindPeer(ctx, pid)
		//if err != nil {
		//	return ErrorNotConnected // node not connected
		//}
		//
		//// check again
		//isConnected, _, _ = ln.isConnected(node)
		//if !isConnected {
		//	return ErrorNotConnected // node not connected
		//}
		ln.log.Warnf("[Net] send msg failed, node not connected, nodeId: [%s]", node)
		return ErrorNotConnected
	}
	// is peer belong to this chain
	if !ln.prepare.isInsecurity && !ln.libP2pHost.peerChainIdsRecorder.IsPeerBelongToChain(pid.Pretty(), chainId) {
		return ErrorNotBelongToChain
	}
	// whether pkt adapter enable
	if ln.pktAdapter != nil {
		return ln.pktAdapter.sendMsg(chainId, pid, msgFlag, data)
	}
	// record TransmissionLatency
	if msgFlag == "msgbus_consensus_topic::TX" {
		if node != "QmUBvBeX2xD6NTGxN3SagLVsqZAoTcrbuC7WqS1BXaiahr" {
			temp := bytes.Split(data, []byte("chain"))
			temp = bytes.Split(temp[1], []byte("@"))
			temp = bytes.Split(temp[1], []byte(" "))
			t, _ := strconv.Atoi(strconv.FormatInt(time.Now().UnixNano(), 10))

			str := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), string(temp[0]), node, strconv.Itoa(t), strconv.Itoa(0), strconv.Itoa(0), strconv.Itoa(0)) //需要写入csv的数据，切片类型
			_ = recorderfile.Record(str, "net_p2p_transmission_latency")
		}

	}

	str := fmt.Sprintf("%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), string(msgFlag), strconv.Itoa(len(data))) //需要写入csv的数据，切片类型
	_ = recorderfile.Record(str, "peer_message_throughput")

	return ln.sendMsg(chainId, pid, msgFlag, data)
}

func (ln *LibP2pNet) registerMsgHandle() error {
	var streamReadHandler = func(stream network.Stream) {
		streamReadHandlerFunc := NewStreamReadHandlerFunc(ln)
		go streamReadHandlerFunc(stream)
	}
	ln.libP2pHost.host.SetStreamHandler(MsgPID, streamReadHandler) // set stream handler for libP2pHost.
	return nil
}

// NewStreamReadHandlerFunc create new function for listening stream reading.
func NewStreamReadHandlerFunc(ln *LibP2pNet) func(stream network.Stream) {
	return func(stream network.Stream) {
		id := stream.Conn().RemotePeer().Pretty() // sender peer id
		reader := bufio.NewReader(stream)
		for {
			length := ln.readMsgLength(reader, stream)
			if length == -1 {
				break
			} else if length == -2 {
				continue
			}

			if length <= 0 {
				ln.log.Warnf("[Net] NewStreamReadHandlerFunc. length==%v. sender:%s)", length, id)
			}

			data, ret := ln.readMsgReadDataRealWanted(reader, stream, length)
			if ret == -1 {
				break
			} else if ret == -2 {
				continue
			}
			if len(data) == 0 {
				//ln.bytesPool.Put(data)
				pool.Put(data)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			go func() {
				//defer ln.bytesPool.Put(data)
				defer pool.Put(data)
				var pkg datapackage.Package
				if err := pkg.FromBytes(data); err != nil {
					ln.log.Debugf("[Net] unmarshal data package from bytes failed, %s", err.Error())
					return
				}
				chainId, flag := utils.GetChainIdAndFlagWithProtocol(pkg.Protocol())
				if !ln.peerChainIdsRecorder().IsPeerBelongToChain(id, chainId) {
					ln.log.Debugf("[Net] sender not belong to chain. drop message. (chainId:%s, sender:%s)",
						chainId, id)
					return
				}
				handler := ln.messageHandlerDistributor.handler(chainId, flag)
				if handler == nil {
					ln.log.Warnf("[Net] handler not registered. drop message. (chainId:%s, flag:%s)", chainId, flag)
					return
				}
				readMsgCallHandler(id, pkg.Payload(), handler, ln.log)
			}()
		}
	}
}

func (ln *LibP2pNet) readData(reader *bufio.Reader, length int) ([]byte, error) {

	batchSize := 4096
	//result := ln.bytesPool.GetWithLen(length)
	result := pool.Get(length)
	start := 0
	for start < length {
		finalBatchSize := length - start
		if finalBatchSize < batchSize {
			batchSize = finalBatchSize
		}
		bytes := (result)[start : start+batchSize]
		c, err := reader.Read(bytes)
		if err != nil {
			return nil, err
		}
		start += c
	}
	return result, nil
}

func (ln *LibP2pNet) readMsgReadDataErrCheck(err error, stream network.Stream) int {
	if strings.Contains(err.Error(), "stream reset") {
		_ = stream.Reset()
		return -1
	}
	ln.log.Warnf("[Net] read stream failed, %s", err.Error())
	return -2
}

func (ln *LibP2pNet) readMsgLength(reader *bufio.Reader, stream network.Stream) int {
	//lengthBytes := ln.bytesPool.GetWithLen(8)
	//defer ln.bytesPool.Put(lengthBytes)
	lengthBytes := pool.Get(8)
	defer pool.Put(lengthBytes)
	start := 0
	for start < 8 {
		size := 8 - start
		bytes := (lengthBytes)[start : start+size]
		c, err := reader.Read(bytes)
		if err != nil {
			return ln.readMsgReadDataErrCheck(err, stream)
		}
		start += c
	}
	length := utils.BytesToInt(lengthBytes)
	return length
}

func (ln *LibP2pNet) readMsgReadDataRealWanted(reader *bufio.Reader, stream network.Stream, length int) ([]byte, int) {
	if length <= 0 || length > MaxReadBuff {
		ln.log.Warnf("[Net] read length out of range, length(%v)", length)
		//return -2 will continue
		return nil, -2
	}
	data, err := ln.readData(reader, length)
	if err != nil {
		return nil, ln.readMsgReadDataErrCheck(err, stream)
	}
	return data, 0
}

func readMsgCallHandler(id string, data []byte, handler api.DirectMsgHandler, log api.Logger) {
	go func(id string, data []byte, handler api.DirectMsgHandler) {
		err := handler(id, data) // call handler
		if err != nil {
			log.Warnf("[Net] stream read handler func call handler failed, %s", err.Error())
		}
	}(id, data, handler)
}

// DirectMsgHandle register a DirectMsgHandler for handling msg received.
func (ln *LibP2pNet) DirectMsgHandle(chainId string, msgFlag string, handler api.DirectMsgHandler) error {
	return ln.messageHandlerDistributor.registerHandler(chainId, msgFlag, handler)
}

// CancelDirectMsgHandle unregister a DirectMsgHandler for handling msg received.
func (ln *LibP2pNet) CancelDirectMsgHandle(chainId string, msgFlag string) error {
	ln.messageHandlerDistributor.cancelRegisterHandler(chainId, msgFlag) // remove stream handler for libP2pHost.
	return nil
}

// AddSeed add a seed node address. It can be a consensus node address.
func (ln *LibP2pNet) AddSeed(seed string) error {
	newSeedsAddrInfos, err := utils.ParseAddrInfo([]string{seed})
	if err != nil {
		return err
	}
	for _, info := range newSeedsAddrInfos {
		ln.libP2pHost.connManager.AddAsHighLevelPeer(info.ID)
	}

	if ln.startUp {
		seedPid, err := helper.GetNodeUidFromAddr(seed)
		if err != nil {
			return err
		}
		oldSeedsAddrInfos := ln.libP2pHost.connSupervisor.getPeerAddrInfos()
		for _, ai := range oldSeedsAddrInfos {
			if ai.ID.Pretty() == seedPid {
				ln.log.Warn("[Net] seed already exists. ignored.")
				return nil
			}
		}

		oldSeedsAddrInfos = append(oldSeedsAddrInfos, newSeedsAddrInfos...)
		ln.libP2pHost.connSupervisor.refreshPeerAddrInfos(oldSeedsAddrInfos)
		return nil
	}
	ln.prepare.AddBootstrapsPeer(seed)
	return nil
}

// SetChainCustomTrustRoots set custom trust roots of chain.
// In cert permission mode, if it is failed when verifying cert by access control of chains,
// the cert will be verified by custom trust root pool again.
func (ln *LibP2pNet) SetChainCustomTrustRoots(chainId string, roots [][]byte) {
	ln.libP2pHost.customChainTrustRoots.RefreshRootsFromPem(chainId, roots)
}

// RefreshSeeds reset addresses of seed nodes with given.
func (ln *LibP2pNet) RefreshSeeds(seeds []string) error {
	newSeedsAddrInfos, err := utils.ParseAddrInfo(seeds)
	if err != nil {
		return err
	}
	ln.libP2pHost.connManager.ClearHighLevelPeer()
	for _, info := range newSeedsAddrInfos {
		ln.libP2pHost.connManager.AddAsHighLevelPeer(info.ID)
	}
	if ln.startUp {
		ln.libP2pHost.connSupervisor.refreshPeerAddrInfos(newSeedsAddrInfos)
		return nil
	}
	for _, seed := range seeds {
		ln.prepare.AddBootstrapsPeer(seed)
	}
	return nil
}

// ReVerifyPeers will verify permission of peers existed with the access control module of the chain
// which id is the given chainId.
func (ln *LibP2pNet) ReVerifyPeers(chainId string) {
	if !ln.startUp {
		return
	}
	if !ln.libP2pHost.isTls {
		return
	}
	var peerIdTlsCertOrPubKeyMap map[string][]byte
	if ln.prepare.pubKeyMode {
		peerIdTlsCertOrPubKeyMap = ln.libP2pHost.peerIdPubKeyStore.StoreCopy()
	} else {
		peerIdTlsCertOrPubKeyMap = ln.libP2pHost.peerIdTlsCertStore.StoreCopy()
	}
	if len(peerIdTlsCertOrPubKeyMap) == 0 {
		return
	}

	// re verify exist peers
	existPeers := ln.libP2pHost.peerChainIdsRecorder.PeerIdsOfChain(chainId)
	for _, existPeerId := range existPeers {
		bytes, ok := peerIdTlsCertOrPubKeyMap[existPeerId]
		if ok {
			var passed bool
			var err error
			// verify member status
			if ln.prepare.pubKeyMode {
				passed, err = utils.ChainMemberStatusValidateWithPubKeyMode(chainId, ln.libP2pHost.memberStatusValidator, bytes)
			} else {
				passed, err = utils.ChainMemberStatusValidateWithCertMode(chainId, ln.libP2pHost.memberStatusValidator, bytes)
			}
			if err != nil {
				ln.log.Warnf("[Net][ReVerifyPeers] chain member status validate failed, %s", err.Error())
				continue
			}
			// if not passed, remove it from chain
			if !passed {
				ln.libP2pHost.peerChainIdsRecorder.RemovePeerChainId(existPeerId, chainId)
				if err = ln.removeChainPubSubWhiteList(chainId, existPeerId); err != nil {
					ln.log.Warnf("[Net] [ReVerifyPeers] remove chain pub-sub white list failed, %s",
						err.Error())
				}
				ln.log.Infof("[Net] [ReVerifyPeers] remove peer from chain, (pid: %s, chain id: %s)",
					existPeerId, chainId)
			}
			delete(peerIdTlsCertOrPubKeyMap, existPeerId)
		} else {
			ln.libP2pHost.peerChainIdsRecorder.RemovePeerChainId(existPeerId, chainId)
			ln.log.Infof("[Net] [ReVerifyPeers] remove peer from chain, (pid: %s, chain id: %s)",
				existPeerId, chainId)
		}
	}
	// verify other peers
	for pid, bytes := range peerIdTlsCertOrPubKeyMap {
		var passed bool
		var err error
		// verify member status
		if ln.prepare.pubKeyMode {
			passed, err = utils.ChainMemberStatusValidateWithPubKeyMode(chainId, ln.libP2pHost.memberStatusValidator, bytes)
		} else {
			passed, err = utils.ChainMemberStatusValidateWithCertMode(chainId, ln.libP2pHost.memberStatusValidator, bytes)
		}
		if err != nil {
			ln.log.Warnf("[Net][ReVerifyPeers] chain member status validate failed, %s", err.Error())
			continue
		}
		// if passed, add it to chain
		if passed {
			ln.libP2pHost.peerChainIdsRecorder.AddPeerChainId(pid, chainId)
			//if err = ln.addChainPubSubWhiteList(chainId, pid); err != nil {
			//	ln.log.Warnf("[Net][ReVerifyPeers] add chain pub-sub white list failed, %s",
			//		err.Error())
			//}
			ln.log.Infof("[Net][ReVerifyPeers] add peer to chain, (pid: %s, chain id: %s)",
				pid, chainId)
		}
	}

	// close all connections of peers not belong to any chain
	for _, s := range ln.libP2pHost.peerChainIdsRecorder.PeerIdsOfNoChain() {
		pid, err := peer.Decode(s)
		if err != nil {
			continue
		}
		// 主动断掉连接的情况，需要剔除DerivedInfo列表
		ln.libP2pHost.tlsCertValidator.DeleteDerivedInfoWithPeerId(s)

		conns := ln.libP2pHost.connManager.GetConns(pid)
		for _, c := range conns {
			// 由于底层的swarm_conn close只做了一次(once.Do)操作，所以反复调用没用。
			//TODO 什么情况关闭失败，会不会有内存泄露？
			err := c.Close()
			if err != nil {
				// 即使关闭出问题了，也先把上层状态删掉
				ln.libP2pHost.connManager.RemoveConn(pid, c)
				ln.log.Warnf("[Net][ReVerifyPeers] close connection failed, peer[%s], err:[%v], remove this conn, remote multi-addr:[%s]",
					s, err, c.RemoteMultiaddr().String())
			} else {
				ln.log.Infof("[Net][ReVerifyPeers] close connection of peer %s", s)
			}
		}

		// 关闭连接失败不能回调host移除上层其他状态,故检测
		if ln.libP2pHost.host.ID() != pid {
			go ln.checkThePeerConns(s, pid)
		}
	}

	ln.reloadChainPubSubWhiteList(chainId)
}

func (ln *LibP2pNet) checkThePeerConns(peerId string, pid peer.ID) {

	// 延迟两秒，再检测，留出关闭时间
	time.Sleep(time.Second * 2)
	conns := ln.libP2pHost.connManager.GetConns(pid)
	if len(conns) == 0 {

		ln.libP2pHost.peerChainIdsRecorder.RemoveAllByPeerId(peerId)

		ln.libP2pHost.peerIdTlsCertStore.RemoveByPeerId(peerId)

		ln.libP2pHost.peerIdPubKeyStore.RemoveByPeerId(peerId)

		if info := ln.libP2pHost.tlsCertValidator.QueryDerivedInfoWithPeerId(peerId); info == nil {
			ln.libP2pHost.certPeerIdMapper.RemoveByPeerId(peerId)
		}

		ln.libP2pHost.closePeer(peerId)

		ln.log.Infof("[Net][ReVerifyPeers] there is no connection available, remove all peer infos. peer[%s]",
			peerId)
	}
	ln.log.Infof("[Net][ReVerifyPeers] there are available connections, peer[%s],conn num:[%d]",
		peerId, len(conns))
}

func (ln *LibP2pNet) removeChainPubSubWhiteList(chainId, pidStr string) error {
	if ln.startUp {
		v, ok := ln.pubSubs.Load(chainId)
		if ok {
			ps := v.(*LibP2pPubSub)
			pid, err := peer.Decode(pidStr)
			if err != nil {
				ln.log.Infof("[Net] parse peer id string to pid failed. %s", err.Error())
				return err
			}
			return ps.RemoveWhitelistPeer(pid)
		}
	}
	return nil
}

// nolint unused
func (ln *LibP2pNet) addChainPubSubWhiteList(chainId, pidStr string) error {
	if ln.startUp {
		v, ok := ln.pubSubs.Load(chainId)
		if ok {
			ps := v.(*LibP2pPubSub)
			pid, err := peer.Decode(pidStr)
			if err != nil {
				ln.log.Infof("[Net] parse peer id string to pid failed. %s", err.Error())
				return err
			}
			return ps.AddWhitelistPeer(pid)
		}
	}
	return nil
}

func (ln *LibP2pNet) reloadChainPubSubWhiteList(chainId string) {
	if ln.startUp {
		v, ok := ln.reloadChainPubSubWhiteListSignalChanMap.Load(chainId)
		if !ok {
			return
		}
		c := v.(chan struct{})
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

func (ln *LibP2pNet) reloadChainPubSubWhiteListLoop(chainId string, ps *LibP2pPubSub) {
	if ln.startUp {
		v, _ := ln.reloadChainPubSubWhiteListSignalChanMap.LoadOrStore(chainId, make(chan struct{}, 1))
		c := v.(chan struct{})
		for {
			select {
			case <-c:
				for _, pidStr := range ln.libP2pHost.peerChainIdsRecorder.PeerIdsOfChain(chainId) {
					pid, err := peer.Decode(pidStr)
					if err != nil {
						ln.log.Infof("[Net] parse peer id string to pid failed. %s", err.Error())
						continue
					}
					err = ps.AddWhitelistPeer(pid)
					if err != nil {
						ln.log.Infof("[Net] add pub-sub white list failed. %s (pid: %s, chain id: %s)",
							err.Error(), pid, chainId)
						continue
					}
					ln.log.Infof("[Net] add peer to chain pub-sub white list, (pid: %s, chain id: %s)",
						pid, chainId)
				}
			case <-ln.ctx.Done():
				return
			}
		}
	}
}

func (ln *LibP2pNet) checkPubsubWhitelistLoop(chainId string, ps *LibP2pPubSub) {
	// time interval of check the list
	ticker := time.NewTicker(refreshPubSubWhiteListTickerTime)

	for {
		select {
		case <-ticker.C:
			// need to iterate over the PeerIdsOfChain
			for _, pidStr := range ln.libP2pHost.peerChainIdsRecorder.PeerIdsOfChain(chainId) {
				pid, err := peer.Decode(pidStr)
				if err != nil {
					ln.log.Infof("[Net] parse peer id string to pid failed. %s", err.Error())
					continue
				}
				// add to the whitle list again
				err = ps.AddWhitelistPeer(pid)
				if err != nil {
					ln.log.Infof("[Net] add pub-sub white list failed. %s (pid: %s, chain id: %s)",
						err.Error(), pid, chainId)
					continue
				}
				ln.log.Infof("[Net] add peer to chain pub-sub white list, (pid: %s, chain id: %s)",
					pid, chainId)
			}
		case <-ln.ctx.Done():
			return
		}
	}
}

// IsRunning
func (ln *LibP2pNet) IsRunning() bool {
	ln.lock.RLock()
	defer ln.lock.RUnlock()
	return ln.startUp
}

// ChainNodesInfo
func (ln *LibP2pNet) ChainNodesInfo(chainId string) ([]*api.ChainNodeInfo, error) {
	result := make([]*api.ChainNodeInfo, 0)
	if ln.libP2pHost.isTls {
		// 1.find all peerIds of chain
		peerIds := make(map[string]struct{})
		if _, ok := peerIds[ln.libP2pHost.host.ID().Pretty()]; !ok {
			peerIds[ln.libP2pHost.host.ID().Pretty()] = struct{}{}
		}
		ids := ln.libP2pHost.peerChainIdsRecorder.PeerIdsOfChain(chainId)
		for _, id := range ids {
			if _, ok := peerIds[id]; !ok {
				peerIds[id] = struct{}{}
			}
		}
		for peerId := range peerIds {
			// 2.find addr
			pid, _ := peer.Decode(peerId)
			addrs := make([]string, 0)
			if pid == ln.libP2pHost.host.ID() {
				for _, multiaddr := range ln.libP2pHost.host.Addrs() {
					addrs = append(addrs, multiaddr.String())
				}
			} else {
				conns := ln.libP2pHost.connManager.GetConns(pid)
				for _, c := range conns {
					if c == nil || c.RemoteMultiaddr() == nil {
						continue
					}
					addrs = append(addrs, c.RemoteMultiaddr().String())
				}
			}

			// 3.find cert
			cert := ln.libP2pHost.peerIdTlsCertStore.GetCertByPeerId(peerId)
			result = append(result, &api.ChainNodeInfo{
				NodeUid:     peerId,
				NodeAddress: addrs,
				NodeTlsCert: cert,
			})
		}
	}
	return result, nil
}

// GetNodeUidByCertId
func (ln *LibP2pNet) GetNodeUidByCertId(certId string) (string, error) {
	nodeUid, err := ln.libP2pHost.certPeerIdMapper.FindPeerIdByCertId(certId)
	if err != nil {
		return "", err
	}
	return nodeUid, nil
}

func (ln *LibP2pNet) handlePubSubWhiteList() {
	ln.handlePubSubWhiteListOnAddC()
	ln.handlePubSubWhiteListOnRemoveC()
}

func (ln *LibP2pNet) handlePubSubWhiteListOnAddC() {
	go func() {
		onAddC := make(chan string, pubSubWhiteListChanCap)
		ln.libP2pHost.peerChainIdsRecorder.OnAddNotifyC(onAddC)
		go func() {
			for ln.IsRunning() {
				time.Sleep(time.Duration(pubSubWhiteListChanQuitCheckDelay) * time.Second)
			}
			close(onAddC)
		}()

		for str := range onAddC {
			//ln.log.Debugf("[Net] handling pubsub white list on add chan,get %s", str)
			peerIdAndChainId := strings.Split(str, "<-->")
			ps, ok := ln.pubSubs.Load(peerIdAndChainId[1])
			if ok {
				pubsub := ps.(*LibP2pPubSub)
				pid, err := peer.Decode(peerIdAndChainId[0])
				if err != nil {
					ln.log.Errorf("[Net] peer decode failed, %s", err.Error())
				}
				ln.log.Infof("[Net] add to pubsub white list(peer-id:%s, chain-id:%s)",
					peerIdAndChainId[0], peerIdAndChainId[1])
				err = pubsub.AddWhitelistPeer(pid)
				if err != nil {
					ln.log.Errorf("[Net] add to pubsub white list(peer-id:%s, chain-id:%s) failed, %s",
						peerIdAndChainId[0], peerIdAndChainId[1], err.Error())
				}
			}
		}
	}()
}

func (ln *LibP2pNet) handlePubSubWhiteListOnRemoveC() {
	go func() {
		onRemoveC := make(chan string, pubSubWhiteListChanCap)
		ln.libP2pHost.peerChainIdsRecorder.OnRemoveNotifyC(onRemoveC)
		go func() {
			for ln.IsRunning() {
				time.Sleep(time.Duration(pubSubWhiteListChanQuitCheckDelay) * time.Second)
			}
			close(onRemoveC)
		}()
		for str := range onRemoveC {
			peerIdAndChainId := strings.Split(str, "<-->")
			ps, ok := ln.pubSubs.Load(peerIdAndChainId[1])
			if ok {
				pubsub := ps.(*LibP2pPubSub)
				pid, err := peer.Decode(peerIdAndChainId[0])
				if err != nil {
					ln.log.Errorf("[Net] peer decode failed, %s", err.Error())
					continue
				}
				ln.log.Debugf("[Net] remove from pubsub white list(peer-id:%s, chain-id:%s)",
					peerIdAndChainId[0], peerIdAndChainId[1])
				err = pubsub.RemoveWhitelistPeer(pid)
				if err != nil {
					ln.log.Errorf("[Net] remove from pubsub white list(peer-id:%s, chain-id:%s) failed, %s",
						peerIdAndChainId[0], peerIdAndChainId[1], err.Error())
				}
			}
		}
	}()
}

// Start
func (ln *LibP2pNet) Start() error {
	ln.lock.Lock()
	defer ln.lock.Unlock()
	if ln.startUp {
		ln.log.Warn("[Net] net is running.")
		return nil
	}
	var err error
	// prepare blacklist
	err = ln.prepareBlackList()
	if err != nil {
		return err
	}
	// create libp2p options
	ln.libP2pHost.opts, err = ln.createLibp2pOptions()
	if err != nil {
		return err
	}
	// set max size for conn manager
	ln.libP2pHost.connManager.SetMaxSize(ln.prepare.maxPeerCountAllow)
	// set elimination strategy for conn manager
	ln.libP2pHost.connManager.SetStrategy(ln.prepare.peerEliminationStrategy)
	// start libP2pHost
	readyC = ln.prepare.readySignalC
	if err = ln.libP2pHost.Start(); err != nil {
		return err
	}
	if err = ln.registerMsgHandle(); err != nil {
		return err
	}
	// pkt adapter
	if err = ln.initPktAdapter(); err != nil {
		return err
	}
	// priority controller
	ln.initPriorityController()
	ln.startUp = true

	// start handling NewTlsPeerChainIdsNotifyC
	if ln.libP2pHost.isTls && ln.libP2pHost.peerChainIdsRecorder != nil {
		ln.handlePubSubWhiteList()
	}

	// setup discovery
	adis := make([]string, 0)
	for bp := range ln.prepare.bootstrapsPeers {
		adis = append(adis, bp)
	}
	if err = SetupDiscovery(ln.libP2pHost, ln.prepare.readySignalC, true, adis, ln.log); err != nil {
		return err
	}

	return nil
}

// Stop
func (ln *LibP2pNet) Stop() error {
	ln.lock.Lock()
	defer ln.lock.Unlock()
	if !ln.startUp {
		ln.log.Warn("[Net] net is not running.")
		return nil
	}
	if ln.pktAdapter != nil {
		ln.pktAdapter.cancel()
	}
	err := ln.libP2pHost.Stop()
	if err != nil {
		return err
	}
	ln.startUp = false

	return nil
}

func (ln *LibP2pNet) AddAC(chainId string, ac api.AccessControlProvider) {
	ln.libP2pHost.memberStatusValidator.AddAC(chainId, ac)
}

func (ln *LibP2pNet) SetMsgPriority(msgFlag string, priority uint8) {
	if ln.priorityController != nil {
		ln.priorityController.SetPriority(msgFlag, priorityblocker.Priority(priority))
	}
}

func (ln *LibP2pNet) IsConnected(nodeId string) bool {
	isConn, _, _ := ln.isConnected(nodeId)
	return isConn
}
