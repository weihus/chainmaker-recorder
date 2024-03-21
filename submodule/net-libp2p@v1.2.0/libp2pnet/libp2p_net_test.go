package libp2pnet

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"time"

	loggerv2 "chainmaker.org/chainmaker/logger/v2"
	"chainmaker.org/chainmaker/protocol/v2"
)

// nolint
// Tests the connection of two nodes
// > step1: new node01
// > step2: new node02
// > step3: node01 send msg to node02
func TestConn(t *testing.T) {
	var (
		chain1  = "chain1"
		msgFlag = "TEST_MSG_SEND"

		node01Addr = "/ip4/127.0.0.1/tcp/10010"
		node02Addr = "/ip4/127.0.0.1/tcp/10011"

		ca1Path = "../testdata/cert/ca1.crt"
		ca2Path = "../testdata/cert/ca2.crt"

		node01PriKeyPath = "../testdata/cert/key1.key"
		node01CertPath   = "../testdata/cert/cert1.crt"

		node02PriKeyPath = "../testdata/cert/key2.key"
		node02CertPath   = "../testdata/cert/cert2.crt"
	)

	// new logger
	logger := loggerv2.GetLogger(loggerv2.MODULE_NET)

	log.Println("========================node01 make")
	// create a startup flag
	node01ReadySignalC := make(chan struct{})
	node01, err := makeNet(
		logger,
		WithListenAddr(node01Addr),
		WithReadySignalC(node01ReadySignalC),
		WithCrypto(false, node01PriKeyPath, node01CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path),
	)

	if err != nil {
		log.Fatalln(err)
	}
	// node01 start
	log.Println("=>node01 start")
	err = node01.Start()
	if err != nil {
		log.Fatalln("node01 start err", err)
	}

	node01Pid := node01.GetNodeUid()
	node01FullAddr := node01Addr + "/p2p/" + node01Pid
	log.Printf("node01[%v] start\n", node01FullAddr)
	close(node01ReadySignalC)

	log.Println("========================node02 make")
	// create a startup flag
	node02ReadySignalC := make(chan struct{})
	// new net
	node02, err := makeNet(
		logger,
		WithListenAddr(node02Addr),
		WithReadySignalC(node02ReadySignalC),
		WithCrypto(false, node02PriKeyPath, node02CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path),
		WithSeeds(node01FullAddr))

	// node02 start
	log.Println("=>node02 start")
	err = node02.Start()
	if err != nil {
		log.Fatalln("node02 start err", err)
	}

	node02Pid := node02.GetNodeUid()
	node02FullAddr := node02Addr + "/p2p/" + node02Pid
	log.Printf("node02[%v] start\n", node02FullAddr)

	close(node02ReadySignalC)

	// msg handler, used to process incoming topic information
	// the essence is a stream handler
	recvChan := make(chan bool)
	node2MsgHandler := func(peerId string, msg []byte) error {
		log.Printf("[node02][%v] recv a msg from peer[%v], msg:%v", chain1, peerId, string(msg))
		recvChan <- true
		return nil
	}

	// register msg handler
	err = node02.DirectMsgHandle(chain1, msgFlag, node2MsgHandler)
	if err != nil {
		log.Fatalln("node02 register msg handler err,", err)
	}

	// node01 send data to node02
	go func() {
		log.Printf("node01 send msg to node02 in %v\n", chain1)
		for {
			err = node01.SendMsg(chain1, node02Pid, msgFlag, []byte("hello, i am node01"))
			if err != nil {
				log.Printf("node01 send msg to node02 in %v err, %v", chain1, err)
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}()

	select {
	case <-recvChan:
		log.Println("node01 send msg to node02 pass")
	}
	err = node01.Stop()
	if err != nil {
		log.Fatalln("node01 stop err", err)
	}

	err = node02.Stop()
	if err != nil {
		log.Fatalln("node02 stop err", err)
	}
}

// nolint
// Tests the connection of two nodes
// > step1: new node01(enable pkt)
// > step2: new node02(enable pkt)
// > step3: node01 send msg to node02
func TestConnEnablePkt(t *testing.T) {
	var (
		chain1 = "chain1"

		msgFlag = "TEST_MSG_SEND"

		node01Addr = "/ip4/127.0.0.1/tcp/10010"
		node02Addr = "/ip4/127.0.0.1/tcp/10011"

		ca1Path = "../testdata/cert/ca1.crt"
		ca2Path = "../testdata/cert/ca2.crt"

		node01PriKeyPath = "../testdata/cert/key1.key"
		node01CertPath   = "../testdata/cert/cert1.crt"

		node02PriKeyPath = "../testdata/cert/key2.key"
		node02CertPath   = "../testdata/cert/cert2.crt"
	)

	// new logger
	logger := loggerv2.GetLogger(loggerv2.MODULE_NET)

	log.Println("========================node01 make")
	// create a startup flag
	node01ReadySignalC := make(chan struct{})
	node01, err := makeNet(
		logger,
		WithListenAddr(node01Addr),
		WithReadySignalC(node01ReadySignalC),
		WithCrypto(false, node01PriKeyPath, node01CertPath),
		WithPktEnable(true),
		WithTrustRoots(chain1, ca1Path, ca2Path),
		WithTrustRoots(pktChainId, ca1Path, ca2Path))

	if err != nil {
		log.Fatalln(err)
	}
	// node01 start
	log.Println("=>node01 start")
	err = node01.Start()
	if err != nil {
		log.Fatalln("node01 start err", err)
	}

	node01Pid := node01.GetNodeUid()
	node01FullAddr := node01Addr + "/p2p/" + node01Pid
	log.Printf("node01[%v] start\n", node01FullAddr)
	close(node01ReadySignalC)

	log.Println("========================node02 make")
	// create a startup flag
	node02ReadySignalC := make(chan struct{})
	// new net
	node02, err := makeNet(
		logger,
		WithListenAddr(node02Addr),
		WithReadySignalC(node02ReadySignalC),
		WithCrypto(false, node02PriKeyPath, node02CertPath),
		WithPktEnable(true),
		WithTrustRoots(chain1, ca1Path, ca2Path),
		WithTrustRoots(pktChainId, ca1Path, ca2Path),
		WithSeeds(node01FullAddr))
	if err != nil {
		log.Fatalln(err)
	}

	// node02 start
	log.Println("=>node02 start")
	err = node02.Start()
	if err != nil {
		log.Fatalln("node02 start err", err)
	}

	node02Pid := node02.GetNodeUid()
	node02FullAddr := node02Addr + "/p2p/" + node02Pid
	log.Printf("node02[%v] start\n", node02FullAddr)

	close(node02ReadySignalC)

	// msg handler, used to process incoming topic information
	// the essence is a stream handler
	recvChan := make(chan bool)
	node2MsgHandler := func(peerId string, msg []byte) error {
		log.Printf("[node02][%v] recv a msg from peer[%v], msg:%v", chain1, peerId, string(msg))
		recvChan <- true
		return nil
	}

	// register msg handler
	err = node02.DirectMsgHandle(chain1, msgFlag, node2MsgHandler)
	if err != nil {
		log.Fatalln("node02 register msg handler err,", err)
	}

	// node01 send data to node02
	go func() {
		log.Printf("node01 send msg to node02 in %v\n", chain1)
		for {
			err = node01.SendMsg(chain1, node02Pid, msgFlag, []byte("hello, i am node01"))
			if err != nil {
				log.Printf("node01 send msg to node02 in %v err, %v", chain1, err)
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}()

	select {
	case <-recvChan:
		log.Println("node01 send msg to node02 pass")
	}
	err = node01.Stop()
	if err != nil {
		log.Fatalln("node01 stop err", err)
	}

	err = node02.Stop()
	if err != nil {
		log.Fatalln("node02 stop err", err)
	}
}

// test publish subscription
// > step1: new node01
// > step2: new node02
// > step3: new node03
// > step4: node02 subscribe topic
// > step4: node03 subscribe topic
// > step5: node01 publish msg to topic
func TestPubsub(t *testing.T) {

	var (
		chain1     = "chain1"
		msgMaxSize = 50 << 20
		topicName  = "TEST_TOPIC"

		node01Addr = "/ip4/127.0.0.1/tcp/10010"
		node02Addr = "/ip4/127.0.0.1/tcp/10011"
		node03Addr = "/ip4/127.0.0.1/tcp/10013"

		ca1Path = "../testdata/cert/ca1.crt"
		ca2Path = "../testdata/cert/ca2.crt"
		ca3Path = "../testdata/cert/ca3.crt"

		node01PriKeyPath = "../testdata/cert/key1.key"
		node01CertPath   = "../testdata/cert/cert1.crt"

		node02PriKeyPath = "../testdata/cert/key2.key"
		node02CertPath   = "../testdata/cert/cert2.crt"

		node03PriKeyPath = "../testdata/cert/key3.key"
		node03CertPath   = "../testdata/cert/cert3.crt"
	)

	// new logger
	logger := loggerv2.GetLogger(loggerv2.MODULE_NET)

	log.Println("========================node01 new")
	// create a startup flag
	node01ReadySignalC := make(chan struct{})
	node01, err := makeNet(
		logger,
		WithListenAddr(node01Addr),
		WithReadySignalC(node01ReadySignalC),
		WithCrypto(false, node01PriKeyPath, node01CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path, ca3Path))

	if err != nil {
		log.Fatalln(err)
	}
	// node01 start
	err = node01.Start()
	if err != nil {
		log.Fatalln("node01 start err", err)
	}

	//init pubsub
	err = node01.InitPubSub(chain1, msgMaxSize)
	if err != nil {
		log.Fatalln("node01 init pubsub err,", err)
	}

	node01Pid := node01.GetNodeUid()
	node01FullAddr := node01Addr + "/p2p/" + node01Pid
	log.Printf("========================>node1[%v] start\n", node01FullAddr)
	close(node01ReadySignalC)

	log.Println("========================>node02 new")
	// create a startup flag
	node02ReadySignalC := make(chan struct{})
	// new net
	node02, err := makeNet(
		logger,
		WithListenAddr(node02Addr),
		WithReadySignalC(node02ReadySignalC),
		WithCrypto(false, node02PriKeyPath, node02CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path, ca3Path),
		WithSeeds(node01FullAddr))
	if err != nil {
		log.Fatalln(err)
	}

	// node02 start
	err = node02.Start()
	if err != nil {
		log.Fatalln("node02 start err", err)
	}

	// init pubsub
	err = node02.InitPubSub(chain1, msgMaxSize)
	if err != nil {
		log.Fatalln("node02 init pubsub err,", err)
	}
	node02Pid := node02.GetNodeUid()
	node02FullAddr := node02Addr + "/p2p/" + node02Pid
	log.Printf("========================>node02[%v] start\n", node02FullAddr)

	close(node02ReadySignalC)

	log.Println("========================>node03 new")
	// create a startup flag
	node03ReadySignalC := make(chan struct{})
	// new net
	node03, err := makeNet(
		logger,
		WithListenAddr(node03Addr),
		WithReadySignalC(node03ReadySignalC),
		WithCrypto(false, node03PriKeyPath, node03CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path, ca3Path))
	if err != nil {
		log.Fatalln(err)
	}

	// node03 start
	err = node03.Start()
	if err != nil {
		log.Fatalln("node03 start err", err)
	}

	// add a seed [node01] after node start
	err = node03.AddSeed(node01FullAddr)
	if err != nil {
		log.Fatalf("node03 add seed[%v] err, %v\n", node01FullAddr, err)
	}

	// init pubsub
	err = node03.InitPubSub(chain1, msgMaxSize)
	if err != nil {
		log.Fatalln("node03 init pubsub err,", err)
	}
	node03Pid := node03.GetNodeUid()
	node03FullAddr := node03Addr + "/p2p/" + node03Pid
	log.Printf("========================>node03[%v] start\n", node03FullAddr)

	close(node03ReadySignalC)

	passCh := make(chan struct{}, 2)
	// node02、node03 Subscription information
	err = node02.SubscribeWithChainId(chain1, topicName, func(publisherPeerId string, msgData []byte) error {
		log.Printf("========================>node02 get sub info[%v_%v], publiser:[%v], msg:[%v]", chain1, topicName, publisherPeerId, string(msgData))
		passCh <- struct{}{}
		return nil
	})
	if err != nil {
		log.Fatalf("node02 subscribe[%v_%v] err, %v\n", chain1, topicName, err)
	}
	log.Printf("node02 subscribe[%v_%v] success\n", chain1, topicName)

	err = node03.SubscribeWithChainId(chain1, topicName, func(publisherPeerId string, msgData []byte) error {
		log.Printf("========================>node03 get sub info[%v_%v], publiser:[%v], msg:[%v]", chain1, topicName, publisherPeerId, string(msgData))
		passCh <- struct{}{}
		return nil
	})
	if err != nil {
		log.Fatalf("node03 subscribe[%v_%v] err, %v\n", chain1, topicName, err)
	}
	log.Printf("node03 subscribe[%v_%v] success\n", chain1, topicName)

	// waiting to refresh libp2ppubsub white list
	time.Sleep(time.Second)

	// node01 broadcast information
	err = node01.BroadcastWithChainId(chain1, topicName, []byte("i am node01"))
	if err != nil {
		log.Fatalln("node01 broadcast information err, ", err)
	}
	fmt.Println("node01 broadcast information success")

	err = node01.BroadcastWithChainId("chain2", topicName, []byte("i am node01"))
	if err != nil {
		log.Println("node01 broadcast information err, ", err)
	}
	fmt.Println("node01 broadcast information success")

	for len(passCh) < 2 {
		time.Sleep(time.Second)
	}

	err = node01.Stop()
	if err != nil {
		log.Fatalln("node01 stop err", err)
	}

	err = node02.Stop()
	if err != nil {
		log.Fatalln("node02 stop err", err)
	}

	err = node03.Stop()
	if err != nil {
		log.Fatalln("node03 stop err", err)
	}
}

// create libp2p net object
func makeNet(logger protocol.Logger, opts ...NetOption) (*LibP2pNet, error) {
	localNet, err := NewLibP2pNet(logger)
	if err != nil {
		return nil, err
	}
	// 装载配置
	if err := apply(localNet, opts...); err != nil {
		return nil, err
	}
	return localNet, nil
}

// NetOption is a function apply options to net instance.
type NetOption func(ln *LibP2pNet) error

// WithReadySignalC set a ready flag
func WithReadySignalC(signalC chan struct{}) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetReadySignalC(signalC)
		return nil
	}
}

// WithListenAddr set addr that the local net will listen on.
func WithListenAddr(addr string) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetListenAddr(addr)
		return nil
	}
}

// WithCrypto set private key file and tls cert file for the net to create connection.
func WithCrypto(pkMode bool, keyFile string, certFile string) NetOption {
	return func(ln *LibP2pNet) error {
		var (
			err                 error
			keyBytes, certBytes []byte
		)
		keyBytes, err = ioutil.ReadFile(keyFile)
		if err != nil {
			return fmt.Errorf("read private key file[%v] err, %v\n", keyFile, err)
		}
		if !pkMode {
			certBytes, err = ioutil.ReadFile(certFile)
			if err != nil {
				return fmt.Errorf("read cert file[%v] err, %v\n", certFile, err)
			}
		}
		ln.Prepare().SetPubKeyModeEnable(pkMode)
		ln.Prepare().SetKey(keyBytes)
		if !pkMode {
			ln.Prepare().SetCert(certBytes)
		}
		return nil
	}
}

// WithTrustRoots set up custom Trust Roots
func WithTrustRoots(chainId string, caFiles ...string) NetOption {
	return func(ln *LibP2pNet) error {
		var trustRoots [][]byte
		for _, caPath := range caFiles {
			caBytes, err := ioutil.ReadFile(caPath)
			if err != nil {
				return fmt.Errorf("read ca file[%v] err, %v\n", caPath, err)
			}
			trustRoots = append(trustRoots, caBytes)
		}

		ln.SetChainCustomTrustRoots(chainId, trustRoots)
		return nil
	}
}

// WithSeeds set addresses of discovery service node.
func WithSeeds(seeds ...string) NetOption {
	return func(ln *LibP2pNet) error {
		for _, seed := range seeds {
			ln.Prepare().AddBootstrapsPeer(seed)
		}
		return nil
	}
}

// WithPeerStreamPoolSize set the max stream pool size for every node that connected to us.
func WithPeerStreamPoolSize(size int) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetPeerStreamPoolSize(size)
		return nil
	}
}

// WithPubSubMaxMessageSize set max message size (M) for pub/sub.
func WithPubSubMaxMessageSize(size int) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetPubSubMaxMsgSize(size)
		return nil
	}
}

// WithMaxPeerCountAllowed set max count of nodes that connected to us.
func WithMaxPeerCountAllowed(max int) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetMaxPeerCountAllow(max)
		return nil
	}
}

// WithPeerEliminationStrategy set the strategy for eliminating node when the count of nodes
// that connected to us reach the max value.
func WithPeerEliminationStrategy(strategy int) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetPeerEliminationStrategy(strategy)
		return nil
	}
}

// WithBlackAddresses set addresses of the nodes for blacklist.
func WithBlackAddresses(blackAddresses ...string) NetOption {
	return func(ln *LibP2pNet) error {
		for _, ba := range blackAddresses {
			ln.Prepare().AddBlackAddress(ba)
		}
		return nil
	}
}

// WithBlackNodeIds set ids of the nodes for blacklist.
func WithBlackNodeIds(blackNodeIds ...string) NetOption {
	return func(ln *LibP2pNet) error {
		for _, bn := range blackNodeIds {
			ln.Prepare().AddBlackPeerId(bn)
		}
		return nil
	}
}

// WithMsgCompression set whether compressing the payload when sending msg.
func WithMsgCompression(enable bool) NetOption {
	return func(ln *LibP2pNet) error {
		ln.SetCompressMsgBytes(enable)
		return nil
	}
}

func WithInsecurity(isInsecurity bool) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetIsInsecurity(isInsecurity)
		return nil
	}
}

func WithPktEnable(pktEnable bool) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetPktEnable(pktEnable)
		return nil
	}
}

// WithPriorityControlEnable config priority controller
func WithPriorityControlEnable(priorityCtrlEnable bool) NetOption {
	return func(ln *LibP2pNet) error {
		ln.Prepare().SetPriorityCtrlEnable(priorityCtrlEnable)
		return nil
	}
}

// apply options.
func apply(ln *LibP2pNet, opts ...NetOption) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(ln); err != nil {
			return err
		}
	}
	return nil
}
