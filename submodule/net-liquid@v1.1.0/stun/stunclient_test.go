package stun

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/logger"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/stun"
)

// haveDns return true if addr looks like /dns/servername/tcp/18301/
func haveDns(addr ma.Multiaddr) bool {
	protocols := addr.Protocols()
	for _, p := range protocols {
		println(p.Name)
		switch p.Code {
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6:
			return true
		}
	}
	return false
}

//use mock func
//no need listen or dial a real addr
//test client only
func TestStunClient(t *testing.T) {

	addr1, err1 := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8081")
	if err1 != nil {
		println(err1)
	}
	addr2, err2 := ma.NewMultiaddr("/dns/cm-node1/tcp/11301/p2p/QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")
	if err2 != nil {
		println(err2)
	}
	println(addr1.String())
	println(addr2.String())
	a1, a2 := ma.SplitFirst(addr2)
	println(a1.String())
	println(a2.String())

	v, _ := a1.ValueForProtocol(a1.Protocol().Code)
	println(v)
	addrs := make([]ma.Multiaddr, 0)
	addrs = append(addrs, addr1, addr2)

	for _, addr := range addrs {
		println("SecureOutbound addr:", addr.String())
		if !haveDns(addr) {
			continue
		}
		println("SecureOutbound addr have dns:", addr.String())
		r := strings.Split(addr.String(), "/")
		if len(r) > 3 {
			serverName := r[2]
			println("SecureOutbound conf.ServerName:", serverName)
			break
		}
	}

	log := logger.NewLogPrinter("stunTest")
	stunServer = &defaultStunServer{log: log}

	pid := peer.ID("QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")
	clientListenAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3333/quic")
	stunServerAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3478/quic")

	var nt network.Network
	pMockNetWork := &mockNetWork{
		localPeerId: pid,
	}
	nt = pMockNetWork
	client, _ := NewDefaultStunClient(log)
	_ = client.SetNetwork(nt)
	natType1, err1 := client.CheckNatType(pid.ToString(), clientListenAddr, stunServerAddr)
	if err1 != nil {
		t.Error(err1)
	}
	fmt.Println(client.GetNatType())
	natType2, err2 := client.GetPeerNatType(pid.ToString(), clientListenAddr, stunServerAddr)
	if err2 != nil {
		t.Error(err2)
	}
	if natType1 != natType2 {
		t.Error("natType1 != natType2")
	}
	client.GetFilteringType()
	client.GetMappingType()
	client.GetMappingType()
}

//mock network
//could dial
type mockNetWork struct {
	localPeerId peer.ID
	listenAddr  []ma.Multiaddr
	Handler     network.ConnHandler
}

func (d *mockNetWork) AddTempListenAddresses(multiaddrs []ma.Multiaddr) {
	panic("implement me")
}

func (d *mockNetWork) GetTempListenAddresses() []ma.Multiaddr {
	panic("implement me")
}

func (d *mockNetWork) DirectListen(ctx context.Context, addrs ...ma.Multiaddr) error {
	panic("implement me")
}

func (d *mockNetWork) RelayListen(listener net.Listener) {
	panic("implement me")
}

func (d *mockNetWork) RelayDial(conn network.Conn, rAddr ma.Multiaddr) (network.Conn, error) {
	panic("implement me")
}

//return a mock conn
//and trig AcceptReceiveStream
func (d *mockNetWork) Dial(ctx context.Context, remoteAddr ma.Multiaddr) (network.Conn, error) {

	m := &mockNetworkConn{
		remoteAddr: remoteAddr,
		localAddr:  d.listenAddr[0],
		netWork:    *d,
	}
	m.ReceiveStream = make(chan bool)
	go func() {
		m.ReceiveStream <- true
	}()
	return m, nil
}

func (d *mockNetWork) Close() error {
	return nil
}

func (d *mockNetWork) Listen(ctx context.Context, addresses ...ma.Multiaddr) error {
	d.listenAddr = append(d.listenAddr, addresses...)
	return nil
}

func (d *mockNetWork) ListenAddresses() []ma.Multiaddr {
	return d.listenAddr
}

func (d *mockNetWork) SetNewConnHandler(handler network.ConnHandler) {
	d.Handler = handler
}

func (d *mockNetWork) Disconnect(conn network.Conn) error {
	return nil
}

func (d *mockNetWork) Closed() bool {
	return true
}

func (d *mockNetWork) LocalPeerID() peer.ID {
	return d.localPeerId
}

//Network.Conn
//implement CreateSendStream and AcceptReceiveStream
type mockNetworkConn struct {
	remoteAddr    ma.Multiaddr
	localAddr     ma.Multiaddr
	remotePeerID  peer.ID
	localPeerId   peer.ID
	netWork       mockNetWork
	ReceiveStream chan bool
}

func (m mockNetworkConn) SetDeadline(t time.Time) error {
	panic("implement me")
}

func (m mockNetworkConn) SetReadDeadline(t time.Time) error {
	panic("implement me")
}

func (m mockNetworkConn) SetWriteDeadline(t time.Time) error {
	panic("implement me")
}

func (m mockNetworkConn) Close() error {
	return nil
}

func (m mockNetworkConn) Direction() network.Direction {
	panic("implement me")
}

func (m mockNetworkConn) EstablishedTime() time.Time {
	return time.Time{}
}

func (m mockNetworkConn) Extra() map[interface{}]interface{} {
	panic("implement me")
}

func (m mockNetworkConn) SetClosed() {
	panic("implement me")
}

func (m mockNetworkConn) IsClosed() bool {
	return true
}

func (m mockNetworkConn) LocalAddr() ma.Multiaddr {
	return m.localAddr
}

func (m mockNetworkConn) LocalNetAddr() net.Addr {
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8000}
	return addr
}

func (m mockNetworkConn) LocalPeerID() peer.ID {
	return m.localPeerId
}

func (m mockNetworkConn) RemoteAddr() ma.Multiaddr {
	return m.remoteAddr
}

func (m mockNetworkConn) RemoteNetAddr() net.Addr {
	panic("implement me")
}

func (m mockNetworkConn) RemotePeerID() peer.ID {
	return m.remotePeerID
}

func (m mockNetworkConn) Network() network.Network {
	panic("implement me")
}

func (m mockNetworkConn) CreateSendStream() (network.SendStream, error) {
	networkStream.networkConn = m
	networkStream.network = m.netWork
	return &networkStream, nil
}

func (m mockNetworkConn) AcceptReceiveStream() (network.ReceiveStream, error) {
	select {
	case <-m.ReceiveStream:
		return &networkStream, nil
	case <-time.After(time.Second * 5):
		return nil, errors.New("time out")
	}
	//return &networkStream, nil
}

func (m mockNetworkConn) CreateBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

func (m mockNetworkConn) AcceptBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

var networkStream mockNetworkStream
var stunServer *defaultStunServer

//mock client NetworkStream
//implement Read and Write,
//Write before Read,
//same as write to a stun server and read response from it
type mockNetworkStream struct {
	network     mockNetWork
	networkConn network.Conn
	msgBus      chan []byte
}

//read from stun server
//pls write before other wise block
func (m *mockNetworkStream) Read(p []byte) (n int, err error) {
	otherStunServerAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3479/quic")
	stunServer.otherStunServerAddr = otherStunServerAddr
	res := new(stun.Message)
	req := new(stun.Message)
	value := 0

	select {
	case buff := <-m.msgBus:
		errMsg := stunServer.genMessage(m.networkConn.RemoteAddr(), buff, req, res, &value)
		copy(p, res.Raw)
		return len(p), errMsg
	case <-time.After(time.Second * 5):
		return 0, errors.New("time out")
	}

}

func (m *mockNetworkStream) Close() error {
	return nil
}

func (m *mockNetworkStream) Direction() network.Direction {
	panic("implement me")
}

func (m *mockNetworkStream) EstablishedTime() time.Time {
	panic("implement me")
}

func (m *mockNetworkStream) Extra() map[interface{}]interface{} {
	panic("implement me")
}

func (m *mockNetworkStream) SetClosed() {
	panic("implement me")
}

func (m *mockNetworkStream) IsClosed() bool {
	panic("implement me")
}

func (m *mockNetworkStream) Conn() network.Conn {
	return m.networkConn
}

//write to stun server
func (m *mockNetworkStream) Write(p []byte) (n int, err error) {

	if m.msgBus == nil {
		m.msgBus = make(chan []byte)
	}
	go func() {
		m.msgBus <- p
	}()

	fmt.Println("call back")
	_, _ = m.network.Handler(m.networkConn)

	//m.msgBus = append(m.msgBus, p)
	return len(p), err
}
