package liquidnet

import (
	"fmt"
	"io/ioutil"
	stdlog "log"
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
	// init logger
	globalNetLogger := loggerv2.GetLogger(loggerv2.MODULE_NET)
	InitLogger(globalNetLogger, func(chainId string) protocol.Logger {
		return loggerv2.GetLoggerByChain(loggerv2.MODULE_NET, chainId)
	})

	stdlog.Println("========================node01 make")
	node01, err := makeNet(
		WithListenAddr(node01Addr),
		WithCrypto(false, node01PriKeyPath, node01CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path))
	if err != nil {
		stdlog.Fatalln(err)
	}

	// node01 start
	stdlog.Println("=>node01 start")
	err = node01.Start()
	if err != nil {
		stdlog.Fatalln("node01 start err", err)
	}

	node01Pid := node01.GetNodeUid()
	node01FullAddr := node01Addr + "/p2p/" + node01Pid
	stdlog.Printf("node01[%v] start\n", node01FullAddr)

	stdlog.Println("========================node02 make")
	node02, err := makeNet(
		WithListenAddr(node02Addr),
		WithCrypto(false, node02PriKeyPath, node02CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path),
		WithSeeds(node01FullAddr))
	if err != nil {
		stdlog.Fatalln(err)
	}

	// node01 start
	stdlog.Println("=>node02 start")
	err = node02.Start()
	if err != nil {
		stdlog.Fatalln("node02 start err", err)
	}

	node02Pid := node02.GetNodeUid()
	node02FullAddr := node02Addr + "/p2p/" + node02Pid
	stdlog.Printf("node02[%v] start\n", node02FullAddr)

	// msg handler, used to process incoming topic information
	// the essence is a stream handler
	recvChan := make(chan bool)
	node2MsgHandler := func(peerId string, msg []byte) error {
		stdlog.Printf("[node02]-[%v] recv a msg from peer[%v], msg:%v", chain1, peerId, string(msg))
		recvChan <- true
		return nil
	}

	// register msg handler
	err = node02.DirectMsgHandle(chain1, msgFlag, node2MsgHandler)
	if err != nil {
		stdlog.Fatalln("node02 register msg handler err", err)
	}

	// node01 send data to node02
	go func() {
		stdlog.Printf("node01 send msg to node02 in %v\n", chain1)
		for {
			err = node01.SendMsg(chain1, node02Pid, msgFlag, []byte("hello, i am node01"))
			if err != nil {
				stdlog.Printf("node01 send msg to node02 in %v err, %v", chain1, err)
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}()

	select {
	case <-recvChan:
		stdlog.Println("node01 send msg to node02 pass")
	}

	err = node01.Stop()
	if err != nil {
		stdlog.Fatalln("node01 stop err", err)
	}

	err = node02.Stop()
	if err != nil {
		stdlog.Fatalln("node02 stop err", err)
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

	// init logger
	globalNetLogger := loggerv2.GetLogger(loggerv2.MODULE_NET)
	InitLogger(globalNetLogger, func(chainId string) protocol.Logger {
		return loggerv2.GetLoggerByChain(loggerv2.MODULE_NET, chainId)
	})

	stdlog.Println("========================node01 make")
	node01, err := makeNet(
		WithListenAddr(node01Addr),
		WithCrypto(false, node01PriKeyPath, node01CertPath),
		WithPktEnable(true),
		WithTrustRoots(chain1, ca1Path, ca2Path),
		WithTrustRoots(string(pktProtocol), ca1Path, ca2Path))
	if err != nil {
		stdlog.Fatalln(err)
	}

	// node01 start
	stdlog.Println("=>node01 start")
	err = node01.Start()
	if err != nil {
		stdlog.Fatalln("node01 start err", err)
	}

	node01Pid := node01.GetNodeUid()
	node01FullAddr := node01Addr + "/p2p/" + node01Pid
	stdlog.Printf("node01[%v] start\n", node01FullAddr)

	stdlog.Println("========================node02 make")
	node02, err := makeNet(
		WithListenAddr(node02Addr),
		WithCrypto(false, node02PriKeyPath, node02CertPath),
		WithPktEnable(true),
		WithTrustRoots(chain1, ca1Path, ca2Path),
		WithTrustRoots(string(pktProtocol), ca1Path, ca2Path),
		WithSeeds(node01FullAddr))
	if err != nil {
		stdlog.Fatalln(err)
	}

	// node01 start
	stdlog.Println("=>node02 start")
	err = node02.Start()
	if err != nil {
		stdlog.Fatalln("node02 start err", err)
	}

	node02Pid := node02.GetNodeUid()
	node02FullAddr := node02Addr + "/p2p/" + node02Pid
	stdlog.Printf("node02[%v] start\n", node02FullAddr)

	// msg handler, used to process incoming topic information
	// the essence is a stream handler
	recvChan := make(chan bool)
	node2MsgHandler := func(peerId string, msg []byte) error {
		stdlog.Printf("[node02]-[%v] recv a msg from peer[%v], msg:%v", chain1, peerId, string(msg))
		recvChan <- true
		return nil
	}

	// register msg handler
	err = node02.DirectMsgHandle(chain1, msgFlag, node2MsgHandler)
	if err != nil {
		stdlog.Fatalln("node02 register msg handler err", err)
	}

	// node01 send data to node02
	go func() {
		stdlog.Printf("node01 send msg to node02 in %v\n", chain1)
		for {
			err = node01.SendMsg(chain1, node02Pid, msgFlag, []byte("hello, i am node01"))
			if err != nil {
				stdlog.Printf("node01 send msg to node02 in %v err, %v", chain1, err)
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}()

	select {
	case <-recvChan:
		stdlog.Println("node01 send msg to node02 pass")
	}

	err = node01.Stop()
	if err != nil {
		stdlog.Fatalln("node01 stop err", err)
	}

	err = node02.Stop()
	if err != nil {
		stdlog.Fatalln("node02 stop err", err)
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

	// init logger
	globalNetLogger := loggerv2.GetLogger(loggerv2.MODULE_NET)
	InitLogger(globalNetLogger, func(chainId string) protocol.Logger {
		return loggerv2.GetLoggerByChain(loggerv2.MODULE_NET, chainId)
	})

	stdlog.Println("========================node01 make")
	node01, err := makeNet(
		WithListenAddr(node01Addr),
		WithCrypto(false, node01PriKeyPath, node01CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path, ca3Path))
	if err != nil {
		stdlog.Fatalln(err)
	}

	// node01 start
	stdlog.Println("=>node01 start")
	err = node01.Start()
	if err != nil {
		stdlog.Fatalln("node01 start err", err)
	}

	// init pubsub
	err = node01.InitPubSub(chain1, msgMaxSize)
	if err != nil {
		stdlog.Fatalln("node01 init pubsub err", err)
	}

	node01Pid := node01.GetNodeUid()
	node01FullAddr := node01Addr + "/p2p/" + node01Pid
	stdlog.Printf("node01[%v] start\n", node01FullAddr)

	stdlog.Println("========================node02 make")
	node02, err := makeNet(
		WithListenAddr(node02Addr),
		WithCrypto(false, node02PriKeyPath, node02CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path, ca3Path),
		WithSeeds(node01FullAddr))
	if err != nil {
		stdlog.Fatalln(err)
	}

	// node02 start
	stdlog.Println("=>node02 start")
	err = node02.Start()
	if err != nil {
		stdlog.Fatalln("node02 start err", err)
	}

	// init pubsub
	err = node02.InitPubSub(chain1, msgMaxSize)
	if err != nil {
		stdlog.Fatalln("node02 init pubsub err", err)
	}

	node02Pid := node02.GetNodeUid()
	node02FullAddr := node02Addr + "/p2p/" + node02Pid
	stdlog.Printf("node02[%v] start\n", node02FullAddr)

	stdlog.Println("========================node03 make")
	node03, err := makeNet(
		WithListenAddr(node03Addr),
		WithCrypto(false, node03PriKeyPath, node03CertPath),
		WithPktEnable(false),
		WithTrustRoots(chain1, ca1Path, ca2Path, ca3Path),
		WithSeeds(node01FullAddr))
	if err != nil {
		stdlog.Fatalln(err)
	}

	// node03 start
	stdlog.Println("=>node03 start")
	err = node03.Start()
	if err != nil {
		stdlog.Fatalln("node03 start err", err)
	}

	// add a seed [node01] after node start
	err = node03.AddSeed(node01FullAddr)
	if err != nil {
		stdlog.Fatalf("node03 add sees[%v] err, %v\n", node01FullAddr, err)
	}

	// init pubsub
	err = node03.InitPubSub(chain1, msgMaxSize)
	if err != nil {
		stdlog.Fatalln("node03 init pubsub err", err)
	}

	node03Pid := node03.GetNodeUid()
	node03FullAddr := node03Addr + "/p2p/" + node03Pid
	stdlog.Printf("node03[%v] start\n", node03FullAddr)

	passCh := make(chan struct{}, 2)
	// node02、node03 Subscription information
	err = node02.SubscribeWithChainId(chain1, topicName, func(publisherPeerId string, msgData []byte) error {
		stdlog.Printf("========================>node02 get sub info[%v_%v], publisher:[%v], msg:[%v]", chain1, topicName, publisherPeerId, string(msgData))
		passCh <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("node02 subscribe[%v_%v] err, %v\n", chain1, topicName, err)
	}
	stdlog.Printf("node02 subscribe[%v_%v] success\n", chain1, topicName)

	err = node03.SubscribeWithChainId(chain1, topicName, func(publisherPeerId string, msgData []byte) error {
		stdlog.Printf("========================>node03 get sub info[%v_%v], publisher:[%v], msg:[%v]", chain1, topicName, publisherPeerId, string(msgData))
		passCh <- struct{}{}
		return nil
	})
	if err != nil {
		log.Fatalf("node03 subscribe[%v_%v] err, %v\n", chain1, topicName, err)
	}
	stdlog.Printf("node03 subscribe[%v_%v] success\n", chain1, topicName)

	// waiting to refresh libp2ppubsub white list
	time.Sleep(time.Second)

	// node01 broadcast information
	err = node01.BroadcastWithChainId(chain1, topicName, []byte("i am node01"))
	if err != nil {
		stdlog.Fatalln("node01 broadcast information err, ", err)
	}
	fmt.Println("node01 broadcast information success")

	for len(passCh) < 2 {
		time.Sleep(time.Second)
	}

	err = node01.Stop()
	if err != nil {
		stdlog.Fatalln("node01 stop err", err)
	}

	err = node02.Stop()
	if err != nil {
		stdlog.Fatalln("node02 stop err", err)
	}

	err = node03.Stop()
	if err != nil {
		stdlog.Fatalln("node03 stop err", err)
	}
}

// create liquid net object
func makeNet(opts ...NetOption) (*LiquidNet, error) {
	localNet, err := NewLiquidNet()
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
type NetOption func(ln *LiquidNet) error

// WithListenAddr set addr that the local net will listen on.
func WithListenAddr(addr string) NetOption {
	return func(ln *LiquidNet) error {
		return SetListenAddrStr(ln.HostConfig(), addr)
	}
}

// WithCrypto set private key file and tls cert file for the net to create connection.
func WithCrypto(pkMode bool, keyFile string, certFile string) NetOption {
	return func(ln *LiquidNet) error {
		var (
			err                 error
			keyBytes, certBytes []byte
		)
		keyBytes, err = ioutil.ReadFile(keyFile)
		if err != nil {
			return fmt.Errorf("read private key file[%v] err %v\n ", keyFile, err)
		}
		if !pkMode {
			certBytes, err = ioutil.ReadFile(certFile)
			if err != nil {
				return fmt.Errorf("read cert file[%v] err %v\n ", certFile, err)
			}
		}
		ln.CryptoConfig().PubKeyMode = pkMode
		ln.CryptoConfig().KeyBytes = keyBytes
		if !pkMode {
			ln.CryptoConfig().CertBytes = certBytes
		}
		return nil
	}
}

// WithTrustRoots set up custom Trust Roots
func WithTrustRoots(chainId string, caFiles ...string) NetOption {
	return func(ln *LiquidNet) error {
		var trustRoots [][]byte
		for _, caPath := range caFiles {
			caBytes, err := ioutil.ReadFile(caPath)
			if err != nil {
				return fmt.Errorf("read ca file[%v] err %v\n ", caPath, err)
			}
			trustRoots = append(trustRoots, caBytes)
		}

		ln.SetChainCustomTrustRoots(chainId, trustRoots)
		return nil
	}
}

// WithSeeds set addresses of discovery service node.
func WithSeeds(seeds ...string) NetOption {
	return func(ln *LiquidNet) error {
		for _, seed := range seeds {
			err := ln.HostConfig().AddDirectPeer(seed)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// WithPeerStreamPoolSize set the max stream pool size for every node that connected to us.
func WithPeerStreamPoolSize(size int) NetOption {
	return func(ln *LiquidNet) error {
		ln.HostConfig().SendStreamPoolCap = int32(size)
		return nil
	}
}

// WithPubSubMaxMessageSize set max message size (M) for pub/sub.
func WithPubSubMaxMessageSize(size int) NetOption {
	return func(ln *LiquidNet) error {
		ln.PubSubConfig().MaxPubMessageSize = size
		return nil
	}
}

// WithMaxConnCountAllowed set max count of connections for each peer that connected to us.
func WithMaxConnCountAllowed(max int) NetOption {
	return func(ln *LiquidNet) error {
		ln.HostConfig().MaxConnCountEachPeerAllowed = max
		return nil
	}
}

// WithMaxPeerCountAllowed set max count of nodes that connected to us.
func WithMaxPeerCountAllowed(max int) NetOption {
	return func(ln *LiquidNet) error {
		ln.HostConfig().MaxPeerCountAllowed = max
		return nil
	}
}

// WithPeerEliminationStrategy set the strategy for eliminating node when the count of nodes
// that connected to us reach the max value.
func WithPeerEliminationStrategy(strategy int) NetOption {
	return func(ln *LiquidNet) error {
		ln.HostConfig().ConnEliminationStrategy = strategy
		return nil
	}
}

// WithBlackAddresses set addresses of the nodes for blacklist.
func WithBlackAddresses(blackAddresses ...string) NetOption {
	return func(ln *LiquidNet) error {
		ln.HostConfig().BlackNetAddr = blackAddresses
		return nil
	}
}

// WithBlackNodeIds set ids of the nodes for blacklist.
func WithBlackNodeIds(blackNodeIds ...string) NetOption {
	return func(ln *LiquidNet) error {
		return ln.HostConfig().AddBlackPeers(blackNodeIds...)
	}
}

// WithMsgCompression set whether compressing the payload when sending msg.
func WithMsgCompression(enable bool) NetOption {
	return func(ln *LiquidNet) error {
		ln.HostConfig().MsgCompress = enable
		return nil
	}
}

// WithInsecurity decides whether insecurity enable.
func WithInsecurity(isInsecurity bool) NetOption {
	return func(ln *LiquidNet) error {
		ln.HostConfig().Insecurity = isInsecurity
		return nil
	}
}

// WithPktEnable whether to enable pktAdapter
func WithPktEnable(pktEnable bool) NetOption {
	return func(ln *LiquidNet) error {
		ln.ExtensionsConfig().EnablePkt = pktEnable
		return nil
	}
}

// WithPriorityControlEnable config priority controller
func WithPriorityControlEnable(priorityCtrlEnable bool) NetOption {
	return func(ln *LiquidNet) error {
		ln.ExtensionsConfig().EnablePriorityCtrl = priorityCtrlEnable
		return nil
	}
}

// apply options.
func apply(ln *LiquidNet, opts ...NetOption) error {
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
