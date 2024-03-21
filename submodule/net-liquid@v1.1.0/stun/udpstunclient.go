package stun

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	api "chainmaker.org/chainmaker/protocol/v2"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/stun"
)

// NewUdpStunClient new a stun client
func NewUdpStunClient(logger api.Logger) (Client, error) {
	return &udpStunClient{
		log:         logger,
		messageChan: make(chan *stun.Message),
	}, nil
}

//a stun client implement by udp
type udpStunClient struct {
	filteringType BehaviorType
	mappingType   BehaviorType
	natType       NATDeviceType
	log           api.Logger
	messageChan   chan *stun.Message
	peerId        string
}

//udp client use udp
//dont need network
func (u *udpStunClient) SetNetwork(_ network.Network) error {
	return nil
}

//CheckNatType check MapType and FilterType and calculate a nat type
//upload local nat type last
//if MapType or FilterType err ,dont return immediately
//listen: local listen addr
//server: remote stun server addr
//return nat type
func (u *udpStunClient) CheckNatType(peerId string, listen, server ma.Multiaddr) (NATDeviceType, error) {
	serverIp, serverPort := mAddrToIpPort(server)
	serverAddr := &net.UDPAddr{IP: net.ParseIP(serverIp), Port: serverPort}

	return u.checkNatType(peerId, listen, serverAddr)
}

//check nat type include map and filter type by domain
//peerId: remote peerId
//listen: local listen addr
//server: remote stun server domain
func (u *udpStunClient) CheckNatTypeByDomain(listen ma.Multiaddr, peerId, server string) (NATDeviceType, error) {
	//need support www.stun.
	stunMultiAddr, errDns := net.ResolveUDPAddr("udp4", server)
	if errDns != nil {
		return NATTypeUnknown, errDns
	}
	return u.checkNatType(peerId, listen, stunMultiAddr)
}

//check nat type include map and filter type
//if err dont return
//peerId: remote peerId
//listen: local listen addr
//server: remote stun server addr
func (u *udpStunClient) checkNatType(peerId string, listen ma.Multiaddr, server *net.UDPAddr) (NATDeviceType, error) {
	log := u.log
	u.peerId = peerId
	clientListenAddr := listen
	stunServerAddr := server
	//map test
	mapType, err := u.testMapType(clientListenAddr, stunServerAddr)
	if err != nil {
		log.Warn("testMapType err:", err)
	}
	//filter test
	filterType, err := u.testFilterType(clientListenAddr, stunServerAddr)
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

	u.natType = natType
	err = u.uploadNatType(natType, clientListenAddr, stunServerAddr)
	if err != nil {
		log.Warn("uploadNatType err:", err)
	}
	return natType, err

}

// GetPeerNatType
// download a nat type by peer id
// listen and send stun request to server to get the nat type
// waiting for response until timeout.
// peerId: remote peerId
// listen: local listen addr
// server: remote stun server addr
func (u *udpStunClient) GetPeerNatType(peerId string, listen, server ma.Multiaddr) (NATDeviceType, error) {
	log := u.log
	clientListenAddr := listen
	stunServerAddr := server
	log.Debug("GetPeerNatType :", clientListenAddr, stunServerAddr)

	//stun.AttrXORPeerAddress
	if clientListenAddr == nil {
		return NATTypeUnknown, errors.New("clientListenAddr==nil")
	}
	if stunServerAddr == nil {
		return NATTypeUnknown, errors.New("stunServerAddr==nil")
	}
	stunServerIp, stunServerPort := mAddrToIpPort(stunServerAddr)
	serverAddr := &net.UDPAddr{IP: net.ParseIP(stunServerIp), Port: stunServerPort}
	//listen
	mapTestConn, err := u.listen(clientListenAddr, serverAddr)
	if err != nil {
		return NATTypeUnknown, errors.New("listen err:" + err.Error())
	}
	defer func() {
		_ = mapTestConn.Close()
	}()

	//get type
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	request.Add(stun.AttrUsername, []byte(peerId))
	log.Info("GetPeerNatType req ", peerId)
	resp, err := mapTestConn.sendAndWait(request, mapTestConn.RemoteAddr)
	if err != nil {
		log.Warnf("StunClient sendAndWait addr(%v) err:%v", clientListenAddr, err)
		return NATTypeUnknown, err
	}
	//decode stun Msg
	resp1 := parseStunMsg(resp)
	if resp1.data == nil {
		return NATTypeUnknown, errors.New("  resp1.otherAddr == nil ")
	}
	log.Infof("GetPeerNatType res : %v", resp1.data.String())
	natType, err := strconv.Atoi(resp1.data.String())
	return NATDeviceType(natType), err
}

// GetNatType
func (u *udpStunClient) GetNatType() NATDeviceType {
	return u.natType
}

// GetMappingType
func (u *udpStunClient) GetMappingType() BehaviorType {
	return u.mappingType
}

// GetFilteringType
func (u *udpStunClient) GetFilteringType() BehaviorType {
	return u.filteringType
}

// testMapType
// send msg to stun server and test nat map type self
// listen for a while to read response
// test map type by respond an addr
// peerId: local peerId
// clientListenAddr: local listen addr
// stunServerAddr: remote stun server addr
func (u *udpStunClient) testMapType(clientListenAddr ma.Multiaddr, stunServerAddr *net.UDPAddr) (BehaviorType, error) {
	log := u.log
	u.mappingType = BehaviorTypeUnknown
	//listen
	mapTestConn, err := u.listen(clientListenAddr, stunServerAddr)
	if err != nil {
		return u.mappingType, errors.New("listen err:" + err.Error())
	}
	defer func() {
		_ = mapTestConn.Close()
	}()

	// Test I: Regular binding request
	log.Info("Mapping Test I: Regular binding request")
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	log.Debug("" + request.Type.String() + "")
	for _, v := range request.Attributes {
		log.Debug("Attributes: " + v.String() + "")
	}
	resp, err := mapTestConn.sendAndWait(request, mapTestConn.RemoteAddr)
	if err != nil {
		return u.mappingType, err
	}
	// Parse response message for XOR-MAPPED-ADDRESS and make sure OTHER-ADDRESS valid
	resp1 := parseStunMsg(resp)
	if resp1.xorAddr == nil || resp1.otherAddr == nil {
		log.Info("Error: NAT discovery feature not supported by this server, resp1.xorAddr == nil || resp1.otherAddr == nil")
		return u.mappingType, errNoOtherAddress
	}
	//change
	addr, err := net.ResolveUDPAddr("udp4", resp1.otherAddr.String())
	//addr, err := net.ResolveUDPAddr("udp4", otherAddr)
	if err != nil {
		log.Infof("Failed resolving OTHER-ADDRESS: %v", resp1.otherAddr)
		return u.mappingType, err
	}
	mapTestConn.OtherAddr = addr
	log.Infof("Received XOR-MAPPED-ADDRESS: %v ,otherAddr: %v", resp1.xorAddr, resp1.otherAddr)
	// Assert mapping behavior
	if resp1.xorAddr.String() == mapTestConn.LocalAddr.String() {
		log.Info("=> NAT mapping behavior: endpoint independent (no NAT)")
		u.mappingType = BehaviorTypeEndpoint
		return u.mappingType, nil
	}

	// Test II: Send binding request to the other address but primary port
	log.Info("Mapping Test II: Send binding request to the other address but primary port")
	addrOnlyChangeIp := *mapTestConn.OtherAddr
	addrOnlyChangeIp.Port = mapTestConn.RemoteAddr.Port
	resp, err = mapTestConn.sendAndWait(request, &addrOnlyChangeIp)
	if err != nil {
		return u.mappingType, err
	}
	// Assert mapping behavior
	resp2 := parseStunMsg(resp)
	log.Infof("Received XOR-MAPPED-ADDRESS: %v", resp2.xorAddr)
	if resp2.xorAddr.String() == resp1.xorAddr.String() {
		log.Info("=> NAT mapping behavior: endpoint independent")
		u.mappingType = BehaviorTypeEndpoint
		return u.mappingType, nil
	}

	// Test III: Send binding request to the other address and port
	log.Info("Mapping Test III: Send binding request to the other address and port")
	resp, err = mapTestConn.sendAndWait(request, mapTestConn.OtherAddr)
	if err != nil {
		return u.mappingType, err
	}
	resp3 := parseStunMsg(resp)
	log.Infof("Received XOR-MAPPED-ADDRESS: %v", resp3.xorAddr)
	// Assert mapping behavior
	if resp3.xorAddr.String() == resp2.xorAddr.String() {
		u.mappingType = BehaviorTypeAddr
		log.Info("=> NAT mapping behavior: address dependent")
	} else {
		u.mappingType = BehaviorTypeAddrAndPort
		log.Info("=> NAT mapping behavior: address and port dependent")
	}
	return u.mappingType, nil
}

// testFilterType
// send msg to stun server and test nat filter type self
// listen for a while to read response
// if timeout to read means nat intercept
// listen: local listen addr
// server: remote stun server addr
func (u *udpStunClient) testFilterType(listen ma.Multiaddr, server *net.UDPAddr) (BehaviorType, error) {
	log := u.log
	clientListenAddr := listen
	stunServerAddr := server
	u.filteringType = BehaviorTypeUnknown

	mapTestConn, err := u.listen(clientListenAddr, stunServerAddr)
	if err != nil {
		return u.filteringType, errors.New("listen err:" + err.Error())
	}
	defer func() {
		_ = mapTestConn.Close()
	}()

	// Test I: Regular binding request
	log.Info("Filtering Test I: Regular binding request")
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	resp, err := mapTestConn.sendAndWait(request, mapTestConn.RemoteAddr)
	if err != nil || errors.Is(err, errTimedOut) {
		return u.filteringType, err
	}
	respS := parseStunMsg(resp)
	if respS.xorAddr == nil || respS.otherAddr == nil {
		log.Warn("Error: NAT discovery feature not supported by this server")
		return u.filteringType, errNoOtherAddress
	}
	addr, err := net.ResolveUDPAddr("udp4", respS.otherAddr.String())
	if err != nil {
		log.Infof("Failed resolving OTHER-ADDRESS: %v", respS.otherAddr)
		return u.filteringType, err
	}
	mapTestConn.OtherAddr = addr

	// Test II: Request to change both IP and port
	log.Info("Filtering Test II: Request to change both IP and port")
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x06})
	_, err = mapTestConn.sendAndWait(request, mapTestConn.RemoteAddr)
	if err == nil {
		parseStunMsg(resp) // just to print out the resp
		log.Warn("=> NAT filtering behavior: endpoint independent")
		u.filteringType = BehaviorTypeEndpoint
		return u.filteringType, nil
	} else if !errors.Is(err, errTimedOut) {
		return u.filteringType, err // something else went wrong
	}

	// Test III: Request to change port only
	log.Info("Filtering Test III: Request to change port only")
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x02})
	_, err = mapTestConn.sendAndWait(request, mapTestConn.RemoteAddr)
	if err == nil {
		u.filteringType = BehaviorTypeAddr
		log.Info("=> NAT filtering behavior: address dependent")
	} else if errors.Is(err, errTimedOut) {
		u.filteringType = BehaviorTypeAddrAndPort
		log.Info("=> NAT filtering behavior: address and port dependent")
	}

	return u.filteringType, nil
}

// listen udp and return a conn
// loop read and decode to stun msg and send to chan
// return a conn include the chan
func (u *udpStunClient) listen(clientListenAddr ma.Multiaddr, stunServerAddrStr *net.UDPAddr) (*stunServerConn, error) {
	log := u.log
	log.Infof("connecting to STUN server: %v", stunServerAddrStr)
	if stunServerAddrStr == nil {
		return nil, errors.New("param is nil")
	}
	var localAddr *net.UDPAddr
	clientIp, clientPort := mAddrToIpPort(clientListenAddr)
	//need a UDPAddr
	localAddr = &net.UDPAddr{IP: net.ParseIP(clientIp), Port: clientPort}
	//listen
	c, err := net.ListenUDP("udp4", localAddr)
	if err != nil {
		return nil, err
	}
	log.Infof("Local address: %s", c.LocalAddr())
	log.Infof("Remote address: %s", stunServerAddrStr.String())
	//read
	mChan := u.loopRead(c)
	//return
	return &stunServerConn{
		log:         u.log,
		conn:        c,
		LocalAddr:   c.LocalAddr(),
		RemoteAddr:  stunServerAddrStr,
		messageChan: mChan,
	}, nil
}

// stunServerConn
// each stunServerConn for a server's conn
type stunServerConn struct {
	log         api.Logger
	conn        net.PacketConn
	LocalAddr   net.Addr
	RemoteAddr  *net.UDPAddr
	OtherAddr   *net.UDPAddr
	messageChan chan *stun.Message
}

// Close conn
func (c *stunServerConn) Close() error {
	return c.conn.Close()
}

// sendAndWait stun msg from chan
// if timeout or Write err return err
// request: stun request msg
// stunServerAddr: remote stun server addr
func (c *stunServerConn) sendAndWait(msg *stun.Message, addr net.Addr) (*stun.Message, error) {
	log := c.log
	_ = msg.NewTransactionID()
	log.Infof("Sending to %v: (%v bytes)", addr, msg.Length+messageHeaderSize)
	log.Debugf("%v", msg)
	for _, attr := range msg.Attributes {
		log.Debugf("\t%v (l=%v)", attr, attr.Length)
	}
	_, err := c.conn.WriteTo(msg.Raw, addr)
	if err != nil {
		log.Warnf("send request to %v Error:%v", addr, err)
		return nil, err
	}

	// Wait for response or timeout
	select {
	case m, ok := <-c.messageChan:
		if !ok {
			return nil, errResponseMessage
		}
		return m, nil
	case <-time.After(time.Duration(timeoutPtr) * time.Second):
		log.Infof("Timed out waiting for response from server %v", addr)
		return nil, errTimedOut
	}
}

// loopRead
// read until read return err
// close conn will read err
// Decode buf to stun.Message and write to chan
// return the chan
func (u *udpStunClient) loopRead(conn *net.UDPConn) (messages chan *stun.Message) {
	log := u.log
	messages = make(chan *stun.Message)
	go func() {
		for {
			buf := make([]byte, stunMsgSize)
			//read udp no matter who send
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				close(messages)
				log.Infof("conn.ReadFromUDP addr: %v  , err:(%v)", addr, err)
				return
			}
			buf = buf[:n]
			//dont care sender,so new a msg here
			m := new(stun.Message)
			m.Raw = buf
			err = m.Decode()
			if err != nil {
				log.Infof("localAddr(%v) recv other msg from: %v", conn.LocalAddr(), addr)
				//conn.WriteToUDP([]byte(""), addr)
			} else {
				log.Infof("localAddr(%v) recv stun msg from %v: (type %v)", conn.LocalAddr(), addr, m.Type.String())
				messages <- m
			}
		} //for end
	}()
	return
}

// uploadNatType
// upload local type to stun server
// use turn attr
// natType: local natType
// listen: local listen addr
// server: remote stun server addr
// return error
func (u *udpStunClient) uploadNatType(natType NATDeviceType, listen ma.Multiaddr, server *net.UDPAddr) error {
	log := u.log
	clientListenAddr := listen
	stunServerAddr := server
	_ = clientListenAddr
	if stunServerAddr == nil {
		return errors.New("stunServerAddr==nil")
	}
	mapTestConn, err := u.listen(clientListenAddr, stunServerAddr)
	if err != nil {
		return errors.New("listen err:" + err.Error())
	}
	defer func() {
		_ = mapTestConn.Close()
	}()
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	peerId := u.peerId
	strNatType := fmt.Sprintf("%v", natType)
	request.Add(stun.AttrUsername, []byte(peerId))
	request.Add(stun.AttrData, []byte(strNatType))
	//start upload
	log.Infof("uploadNatType req peer(%v)type(%v)", peerId, strNatType)
	resp, err := mapTestConn.sendAndWait(request, stunServerAddr)
	if err != nil {
		return err
	}
	stunResMsg := parseStunMsg(resp)
	log.Infof("uploadNatType res peer(%v)addr(%v)", peerId, stunResMsg.xorAddr)
	return nil
}
