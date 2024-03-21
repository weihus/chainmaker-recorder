package stun

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	api "chainmaker.org/chainmaker/protocol/v2"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/stun"
)

const (
	timeoutPtr  = 5
	stunMsgSize = 1500
)

var (
	errResponseMessage = errors.New("error reading from response message channel")
	errTimedOut        = errors.New("timed out waiting for response")
	errNoOtherAddress  = errors.New("no OTHER-ADDRESS in message")
)

// defaultStunClient use net work to i/o stun msg
type defaultStunClient struct {
	// network
	network network.Network
	// result nat filteringType
	filteringType BehaviorType
	// result nat mappingType
	mappingType BehaviorType
	// final result
	natType NATDeviceType
	// msg chan
	messageChan chan *stun.Message
	//log
	log api.Logger
}

// GetPeerNatType download a nat type by peer id
// listen and send stun request to server to get the nat type
// waiting for response until timeout.
// peerId: remote peerId
// clientListenAddr: local listen addr
// stunServerAddr: remote stun server addr
func (d *defaultStunClient) GetPeerNatType(peerId string, client, server ma.Multiaddr) (NATDeviceType, error) {
	// adapt log
	log := d.log
	// need listen first
	clientListenAddr := client
	// server addr
	stunServerAddr := server
	log.Debug("GetPeerNatType :", clientListenAddr, stunServerAddr)
	//stun.AttrXORPeerAddress
	if clientListenAddr == nil {
		return NATTypeUnknown, errors.New("clientListenAddr==nil")
	}
	// judge para
	if stunServerAddr == nil {
		return NATTypeUnknown, errors.New("stunServerAddr==nil")
	}
	//listen
	errListen := d.network.Listen(context.Background(), clientListenAddr)
	if errListen != nil {
		log.Warnf("StunClient network.Listen addr(%v) err:%v", clientListenAddr, errListen)
	}
	// get type
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	// des peer
	request.Add(stun.AttrUsername, []byte(peerId))
	log.Info("GetPeerNatType req ", peerId)
	// send msg
	resp1, errSend := d.sendAndWait(request, stunServerAddr)
	if errSend != nil {
		log.Warnf("StunClient sendAndWait addr(%v) err:%v", clientListenAddr, errListen)
	}
	// resp
	if resp1 == nil || resp1.data == nil {
		return NATTypeUnknown, errors.New("resp1.xorAddr == nil || resp1.otherAddr == nil ")
	}
	log.Infof("GetPeerNatType res : %v", resp1.data.String())
	// result
	natType, err := strconv.Atoi(resp1.data.String())
	_ = d.closeListen()
	return NATDeviceType(natType), err
}

// uploadNatType upload local type to stun server
// use turn attr
// natType: local natType
// clientListenAddr: local listen addr
// stunServerAddr: remote stun server addr
func (d *defaultStunClient) uploadNatType(natType NATDeviceType, clientListenAddr, stunServerAddr ma.Multiaddr) error {
	//  adapt log
	log := d.log
	_ = clientListenAddr
	// judge
	if stunServerAddr == nil {
		return errors.New("stunServerAddr==nil")
	}
	// get peerId and natType
	peerId := d.network.LocalPeerID().ToString()
	// local nat type
	strNatType := fmt.Sprintf("%v", natType)
	// packet usrName and natType
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	// add name
	request.Add(stun.AttrUsername, []byte(peerId))
	// add data
	request.Add(stun.AttrData, []byte(strNatType))
	// start upload
	log.Infof("uploadNatType req peer(%v)type(%v)", peerId, strNatType)
	// send msg
	resp1, err := d.sendAndWait(request, stunServerAddr)
	if resp1 == nil {
		return err
	}
	log.Infof("uploadNatType res : %v", resp1.xorAddr)
	return err
}

// handlerNowListenConn handler new conn
// conn:a new conn established
func (d *defaultStunClient) handlerNowListenConn(conn networkConn) (bool, error) {
	if conn == nil {
		return false, errors.New("param == nil")
	}
	log := d.log
	// localAddr str
	localAddr := conn.LocalNetAddr().String()
	// remotePeerID peer.ID
	remotePeerID := conn.RemotePeerID()
	// remoteAddr ma.Multiaddr
	remoteAddr := conn.RemoteAddr()
	//dont block
	go func() {
		for {
			//accept
			readStream, errAccept := conn.AcceptReceiveStream()
			if errAccept != nil {
				log.Errorf("%v AcceptReceiveStream (%v),err:%v", localAddr, remotePeerID, errAccept.Error())
				return
			}
			go func() {
				// avoid new free
				buff := make([]byte, stunMsgSize)
				for {
					//read
					n, errRead := readStream.Read(buff)
					if errRead != nil {
						log.Error("Read err:", errRead)
						return
					}
					log.Infof("Read ok ,len(%v)", n)
					//stun msg
					m := new(stun.Message)
					m.Raw = buff
					//decode msg
					err := m.Decode()
					if err != nil {
						log.Infof("(%v) recv msg from: %v\n", localAddr, remoteAddr)
						continue
					}
					log.Infof("(%v) recv msg from %v: (type %v)", localAddr, remoteAddr, m.Type.String())
					//avoid panic
					if d.messageChan == nil {
						continue
					}
					//send to chan
					d.messageChan <- m
				}

			}()

		}
	}()
	return true, nil
}

// SetNetwork,use listen and dial
// network should make from newNetwork()
// net: a network use to dial or listen
func (d *defaultStunClient) SetNetwork(net network.Network) error {
	if net == nil {
		return errors.New("net is nil")
	}
	//net work
	d.network = net
	//invoke when get a new conn
	net.SetNewConnHandler(d.handlerNowListenConn)
	return nil
}

// CheckNatTypeByDomain include checkNatType
// peerId: local peerId
// listen: local listen addr
// sDomain: remote stun server domain
func (d *defaultStunClient) CheckNatTypeByDomain(listen ma.Multiaddr, peerId, sDomain string) (NATDeviceType, error) {
	//need support www.stun.
	addr, errDns := net.ResolveUDPAddr("udp4", sDomain)
	if errDns != nil {
		return NATTypeUnknown, errDns
	}
	// convert addr
	stunMultiAddr, err := ma.NewMultiaddr(addr.String())
	if err != nil {
		return NATTypeUnknown, errDns
	}
	// check type
	return d.CheckNatType(peerId, listen, stunMultiAddr)
}

// CheckNatType check MapType and FilterType and calculate a nat type
// upload local nat type last
// if MapType or FilterType err ,dont return immediately
// peerId: local peerId
// clientListenAddr: local listen addr
// stunServerAddr: remote stun server addr
// return nat type
func (d *defaultStunClient) CheckNatType(peerId string, listenAddr, serverAddr ma.Multiaddr) (NATDeviceType, error) {
	log := d.log
	defer func() {
		if e := recover(); e != nil {
			stack := debug.Stack()
			_, _ = os.Stderr.Write(stack)
			log.Errorf("panic stack: %s", string(stack))
		}
	}()
	//listen
	log.Debug("StunClient listen on:", listenAddr)
	if listenAddr == nil {
		return NATTypeUnknown, errors.New("clientListenAddr==nil")
	}
	// listen
	errL := d.network.Listen(context.Background(), listenAddr)
	if errL != nil {
		return NATTypeUnknown, errL
	}
	//map test
	mapType, err := d.testMapType(peerId, listenAddr, serverAddr)
	if err != nil {
		log.Warn("testMapType err:", err)
	}
	//filter test
	filterType, err := d.testFilterType(listenAddr, serverAddr)
	if err != nil {
		log.Warn("testFilterType err:", err)
	}
	//result
	var natType NATDeviceType
	switch {
	case mapType == BehaviorTypeAddrAndPort:
		natType = NATTypeSymmetric
	case mapType == BehaviorTypeEndpoint && filterType == BehaviorTypeEndpoint:
		natType = NATTypeFullCone
	case mapType == BehaviorTypeEndpoint && filterType == BehaviorTypeAddr:
		natType = NATTypeRestrictedCone
	case mapType == BehaviorTypeEndpoint && filterType == BehaviorTypeAddrAndPort:
		natType = NATTypePortRestrictedCone
	default:
		natType = NATTypeUnknown
	}
	//upload type to server
	d.natType = natType
	err = d.uploadNatType(natType, listenAddr, serverAddr)
	_ = d.closeListen()
	return natType, err
}

// testMapType send msg to stun server and test nat map type self
// listen for a while to read response
// test map type by respond an addr
// peerId: local peerId
// clientListenAddr: local listen addr
// stunServerAddr: remote stun server addr
// return nat type
func (d *defaultStunClient) testMapType(peerId string, listenAddr, serverAddr ma.Multiaddr) (BehaviorType, error) {
	log := d.log
	_ = peerId
	_ = listenAddr
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	d.mappingType = BehaviorTypeUnknown
	//Mapping Test
	log.Info("Mapping Test I: Regular binding request")
	resp1, errS := d.sendAndWait(request, serverAddr)
	if errS != nil {
		return BehaviorTypeUnknown, errS
	}
	if resp1 == nil || resp1.xorAddr == nil || resp1.otherAddr == nil {
		return BehaviorTypeUnknown, errors.New(" resp1 || resp1.xorAddr || resp1.otherAddr == nil ")
	}
	log.Infof("Received XOR-MAPPED-ADDRESS: %v otherAddr:%v", resp1.xorAddr, resp1.otherAddr)
	// chang ip
	otherStunServerAddr := addrToMAddr("ip4", resp1.otherAddr.String(), "quic")
	// need old port
	_, port := mAddrToIpPort(serverAddr)
	// need other ip
	ip, _ := mAddrToIpPort(otherStunServerAddr)
	// make new addr
	addrOnlyChangeIp := mAddrResetIpPort(serverAddr, ip, port)
	// send msg
	log.Info("Mapping Test II: Send binding request to the other address but primary port:", addrOnlyChangeIp)
	resp2, errS2 := d.sendAndWait(request, addrOnlyChangeIp)
	if errS2 != nil {
		return BehaviorTypeUnknown, errS2
	}
	// error
	if resp2 == nil || resp2.xorAddr == nil {
		return BehaviorTypeUnknown, errors.New(" resp2 || resp2.xorAddr == nil ")
	}
	log.Infof("Received XOR-MAPPED-ADDRESS: %v", resp2.xorAddr)
	// get same addr from diff server
	if resp2.xorAddr.String() == resp1.xorAddr.String() {
		log.Info("=> NAT mapping behavior: endpoint independent ")
		d.mappingType = BehaviorTypeEndpoint
		return d.mappingType, nil
	}
	// chang only port
	log.Info("Mapping Test III: Send binding request to the other address and port")
	resp3, _ := d.sendAndWait(request, otherStunServerAddr)
	if resp3 == nil || resp3.xorAddr == nil {
		return BehaviorTypeUnknown, errors.New(" resp3 || resp3.xorAddr == nil ")
	}
	log.Infof("Received XOR-MAPPED-ADDRESS: %v", resp3.xorAddr)
	// get same addr from diff port
	if resp3.xorAddr.String() == resp2.xorAddr.String() {
		d.mappingType = BehaviorTypeAddr
		log.Info("=> NAT mapping behavior: address dependent")
	} else {
		// get diff addr from diff port
		d.mappingType = BehaviorTypeAddrAndPort
		log.Info("=> NAT mapping behavior: address and port dependent")
	}
	return d.mappingType, nil
}

//testFilterType send msg to stun server and test nat filter type self
// listen for a while to read response
// if timeout to read means nat intercept
// clientListenAddr: local listen addr
// stunServerAddr: remote stun server addr
func (d *defaultStunClient) testFilterType(clientListenAddr, stunServerAddr ma.Multiaddr) (BehaviorType, error) {
	log := d.log
	_ = clientListenAddr
	//Filtering Test I
	log.Info("Filtering Test I: Regular binding request")
	// make msg
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	// send msg
	resp, err := d.sendAndWait(request, stunServerAddr)
	if err != nil {
		return BehaviorTypeUnknown, err
	}
	// something error
	if resp == nil || resp.xorAddr == nil || resp.otherAddr == nil {
		log.Warn("Error: NAT discovery feature not supported by this server")
		return BehaviorTypeUnknown, errNoOtherAddress
	}

	// Test II: Request to change both IP and port
	log.Info("Filtering Test II: Request to change both IP and port")
	// make msg req
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	// change both ip and port
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x06})
	// send msg
	_, err = d.sendAndWait(request, stunServerAddr)
	// get msg from diff addr
	if err == nil {
		d.filteringType = BehaviorTypeEndpoint
		log.Warn("=> NAT filtering behavior: endpoint independent")
		return d.filteringType, nil
	} //else if !errors.Is(err, errTimedOut) {
	//	//return err // something else went wrong
	//}

	// Test III: Request to change port only
	log.Info("Filtering Test III: Request to change port only")
	// make msg req
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	// make msg req only change port
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x02})
	// send msg
	_, err = d.sendAndWait(request, stunServerAddr)
	// get msg from diff port
	if err == nil {
		d.filteringType = BehaviorTypeAddr
		log.Warn("=> NAT filtering behavior: address dependent")
	} else if errors.Is(err, errTimedOut) {
		d.filteringType = BehaviorTypeAddrAndPort
		log.Warn("=> NAT filtering behavior: address and port dependent")
	}

	return d.filteringType, nil
}

// sendAndWait send and wait stun msg from chan
// if timeout return err
// request: stun request msg
// stunServerAddr: remote stun server addr
func (d *defaultStunClient) sendAndWait(request *stun.Message, stunServerAddr ma.Multiaddr) (*stunRes, error) {
	//wait chan
	_ = request.NewTransactionID()
	log := d.log
	//dial
	writeConn, errDial := d.network.Dial(context.Background(), stunServerAddr)
	if errDial != nil {
		return nil, fmt.Errorf("dial addr(%v) err:%v  ", stunServerAddr, errDial)
	}
	if writeConn == nil {
		return nil, errors.New("writeConn == nil")
	}
	log.Infof("dial ok:%v  ", writeConn.RemoteAddr().String())
	//stream
	sendStream, errS := writeConn.CreateSendStream()
	if errS != nil {
		return nil, errors.New("CreateSendStream err:" + errS.Error())
	}
	if sendStream == nil {
		return nil, errors.New("sendStream == nil")
	}
	log.Infof("CreateSendStream ok :", sendStream.Conn().LocalAddr())
	//Write
	nW, errW := sendStream.Write(request.Raw)
	if errW != nil {
		return nil, fmt.Errorf("Write to (%v) err:%v ", sendStream.Conn().RemoteAddr().String(), errW)
	}
	log.Infof("Write to (%v) ok ,len(%v):", sendStream.Conn().RemoteAddr().String(), nW)

	// Wait for response or timeout
	select {
	case m, ok := <-d.messageChan:
		if !ok {
			return nil, errResponseMessage
		}
		res := parseStunMsg(m)
		return &res, nil
	case <-time.After(time.Duration(timeoutPtr) * time.Second):
		log.Infof("Timed out waiting for response from server %v\n", writeConn.RemoteAddr())
		return nil, errTimedOut
	}

}

// GetMappingType
func (d *defaultStunClient) GetMappingType() BehaviorType {
	// return last result
	return d.mappingType
}

// GetFilteringType
func (d *defaultStunClient) GetFilteringType() BehaviorType {
	// return last result
	return d.filteringType
}

// GetNatTypeholePunch to
func (d *defaultStunClient) GetNatType() NATDeviceType {
	// return last result
	return d.natType
}

// closeListen
func (d *defaultStunClient) closeListen() error {
	// close current listen
	return d.network.Close()
}

// NewDefaultStunClient new a client
func NewDefaultStunClient(logger api.Logger) (Client, error) {
	return &defaultStunClient{
		log:         logger,
		messageChan: make(chan *stun.Message),
	}, nil
}
