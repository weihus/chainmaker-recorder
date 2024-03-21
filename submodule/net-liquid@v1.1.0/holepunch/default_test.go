package holepunch

import (
	"errors"
	"net"
	"testing"
	"time"

	manet "github.com/multiformats/go-multiaddr/net"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/logger"
	"chainmaker.org/chainmaker/net-liquid/stun"
	ma "github.com/multiformats/go-multiaddr"
)

func TestListenForNat(t *testing.T) {
	a11, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8081/p2p/QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")
	add1, add2 := ma.SplitFunc(a11, func(c ma.Component) bool {
		return c.Protocol().Code == ma.P_TCP || c.Protocol().Code == ma.P_UDP
	})
	println(add1.String(), add2.String())
	portDes, _ := add2.ValueForProtocol(add2.Protocols()[0].Code)
	println(portDes)

	log := logger.NewLogPrinter("TEST")
	hp, _ := NewHolePunch(log)
	var addr1, addr2 []ma.Multiaddr
	a1, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8081")
	a2, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8082")
	a3, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8083")
	a4, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8084")
	addr1 = append(addr1, a1, a2, a4)
	addr2 = append(addr2, a2, a3)
	hp.ListenForNat(addr1, addr2)
}

func TestHolePunchAvailable(t *testing.T) {
	log := logger.NewLogPrinter("TEST")
	hp, _ := NewHolePunch(log)
	b1 := hp.HolePunchAvailable(stun.NATTypeFullCone, stun.NATTypeFullCone)
	if !b1 {
		t.Error()
	}
	b1 = hp.HolePunchAvailable(stun.NATTypeFullCone, stun.NATTypePortRestrictedCone)
	if !b1 {
		t.Error()
	}
	b1 = hp.HolePunchAvailable(stun.NATTypeFullCone, stun.NATTypeSymmetric)
	if !b1 {
		t.Error()
	}

	b2 := hp.HolePunchAvailable(stun.NATTypeSymmetric, stun.NATTypeSymmetric)
	if b2 {
		t.Error()
	}
	b2 = hp.HolePunchAvailable(stun.NATTypeSymmetric, stun.NATTypePortRestrictedCone)
	if b2 {
		t.Error()
	}
	b2 = hp.HolePunchAvailable(stun.NATTypePortRestrictedCone, stun.NATTypeSymmetric)
	if b2 {
		t.Error()
	}
}

func TestHolePunchDirectConnect(t *testing.T) {
	a1, _ := ma.NewMultiaddr("/ip4/82.156.18.47/tcp/11307")
	a2, _ := ma.NewMultiaddr("/ip4/124.126.16.250/tcp/11305")
	a3, _ := ma.NewMultiaddr("/ip4/221.122.77.89/tcp/11303")
	if !manet.IsPublicAddr(a1) {
		t.Error()
	}
	if !manet.IsPublicAddr(a2) {
		t.Error()
	}
	if !manet.IsPublicAddr(a3) {
		t.Error()
	}
}

type mockNetWork struct {
	err int
}

func (d *mockNetWork) Dial(remoteAddr ma.Multiaddr) (network.Conn, error) {
	switch d.err {
	case 1:
		return nil, errors.New("err")
	case 2:
		return nil, nil
	}
	m := &mockNetworkConn{
		remoteAddr: remoteAddr,
		netWork:    *d,
		err:        d.err,
	}
	return m, nil
}

type mockNetworkConn struct {
	err          int
	remoteAddr   ma.Multiaddr
	localAddr    ma.Multiaddr
	remotePeerID peer.ID
	localPeerId  peer.ID
	netWork      mockNetWork
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
	nAddr, _ := manet.ToNetAddr(m.localAddr)
	return nAddr
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

func (m *mockNetworkConn) CreateSendStream() (network.SendStream, error) {
	switch m.err {
	case 3:
		return nil, nil
	case 4:
		return nil, errors.New("")
	}
	networkStream.err = m.err
	networkStream.networkConn = m
	networkStream.network = m.netWork
	return &networkStream, nil
}

func (m mockNetworkConn) AcceptReceiveStream() (network.ReceiveStream, error) {
	return &networkStream, nil
	//return &networkStream, nil
}

func (m mockNetworkConn) CreateBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

func (m mockNetworkConn) AcceptBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

var networkStream mockNetworkStream

type mockNetworkStream struct {
	err         int
	network     mockNetWork
	networkConn network.Conn
}

func (m *mockNetworkStream) Read(p []byte) (n int, err error) {
	return 0, err
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

func (m *mockNetworkStream) Write(p []byte) (n int, err error) {
	if m.err == 5 {
		return 0, errors.New("")
	}
	return len(p), err
}
