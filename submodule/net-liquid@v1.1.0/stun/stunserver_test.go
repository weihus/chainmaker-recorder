package stun

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/logger"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/stun"
)

func TestStunServer(t *testing.T) {
	log := logger.NewLogPrinter("stunTest")
	stunServer = &defaultStunServer{log: log}

	pid := peer.ID("QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")
	stunServerAddr1, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3478/quic")
	stunServerAddr2, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3479/quic")

	stunServerAddr21, _ := ma.NewMultiaddr("/ip4/127.0.0.2/udp/3478/quic")
	stunServerAddr22, _ := ma.NewMultiaddr("/ip4/127.0.0.2/udp/3479/quic")
	var nt1, nt2 network.Network
	pMockNetWork := &mockNetworkServer{
		name:        "s1",
		localPeerId: pid,
	}
	nt1 = pMockNetWork
	pMockNetWork2 := &mockNetworkServer{
		name:        "s2",
		localPeerId: pid,
	}
	nt2 = pMockNetWork2
	server := &defaultStunServer{log: log}
	server.otherStunServerAddr = stunServerAddr2
	changeParam := &ChangeParam{
		OtherAddr:             stunServerAddr2,
		OtherMasterListenAddr: stunServerAddr21,
		OtherSlaveListenAddr:  stunServerAddr22,
		OtherMasterNetwork:    nt1,
		OtherSlaveNetwork:     nt2,
	}
	_ = server.InitChangeIpLocal(changeParam)
	_ = server.SetNetwork(nt1, nt2)
	errL := server.Listen(stunServerAddr1, stunServerAddr2)
	if errL != nil {
		t.Error(errL)
	}

	pid2 := peer.ID("QmeyNRs2DwWjcHTpcVHoUSaDAAif4VQZ2wQDQAUNDP33gH")
	pMockNetWorkClient := &mockNetworkServer{
		name:        "c1",
		localPeerId: pid2,
	}
	clientListenAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3333/quic")
	pMockNetWorkClient.listenAddr = []ma.Multiaddr{clientListenAddr}
	conn, _ := pMockNetWorkClient.dial(pMockNetWork, stunServerAddr1)
	sendStream, _ := conn.CreateSendStream()
	//test 1
	fmt.Println("test 1")
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	sendStream.Extra()
	_, err1 := sendStream.Write(request.Raw)
	if err1 != nil {
		t.Error(err1)
	}
	//natType2, err2 := client.GetPeerNatType(pid.ToString(), clientListenAddr, stunServerAddr)

	//test 2
	fmt.Println("test 2")
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x06})
	sendStream.Extra()
	_, err2 := sendStream.Write(request.Raw)
	if err2 != nil {
		t.Error(err2)
	}

	//test 3
	fmt.Println("test 3")
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x02})
	sendStream.Extra()
	_, err3 := sendStream.Write(request.Raw)
	if err3 != nil {
		t.Error(err3)
	}

	res := new(stun.Message)
	stunServer.HandleChangeIpReq(clientListenAddr, res.Raw)
	time.Sleep(5 * time.Second)
	_ = server.CloseListen()
}

func TestStunServerNotify(t *testing.T) {
	log := logger.NewLogPrinter("stunTest")
	stunServer = &defaultStunServer{log: log}

	pid := peer.ID("QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")
	stunServerAddr1, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3478/quic")
	stunServerAddr2, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3479/quic")

	var nt1, nt2 network.Network
	pMockNetWork := &mockNetworkServer{
		name:        "s1",
		localPeerId: pid,
	}
	nt1 = pMockNetWork
	pMockNetWork2 := &mockNetworkServer{
		name:        "s2",
		localPeerId: pid,
	}
	nt2 = pMockNetWork2
	server := &defaultStunServer{log: log}
	server.otherStunServerAddr = stunServerAddr2
	changeParam := &ChangeNotifyParam{
		stunServerAddr2,
		"127.0.0.1:3344",
		"127.0.0.1:3344",
	}
	_ = server.InitChangeIpNotify(changeParam)
	_ = server.SetNetwork(nt1, nt2)
	errL := server.Listen(stunServerAddr1, stunServerAddr2)
	if errL != nil {
		t.Error(errL)
	}

	pid2 := peer.ID("QmeyNRs2DwWjcHTpcVHoUSaDAAif4VQZ2wQDQAUNDP33gH")
	pMockNetWorkClient := &mockNetworkServer{
		name:        "c1",
		localPeerId: pid2,
	}
	clientListenAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3333/quic")
	pMockNetWorkClient.listenAddr = []ma.Multiaddr{clientListenAddr}
	conn, _ := pMockNetWorkClient.dial(pMockNetWork, stunServerAddr1)
	sendStream, _ := conn.CreateSendStream()
	//test 1
	fmt.Println("test 1")
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	sendStream.Extra()
	_, err1 := sendStream.Write(request.Raw)
	if err1 != nil {
		t.Error(err1)
	}
	//natType2, err2 := client.GetPeerNatType(pid.ToString(), clientListenAddr, stunServerAddr)

	//test 2
	fmt.Println("test 2")
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x06})
	sendStream.Extra()
	_, err2 := sendStream.Write(request.Raw)
	if err2 != nil {
		t.Error(err2)
	}

	//test 3
	fmt.Println("test 3")
	request = stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	request.Add(stun.AttrChangeRequest, []byte{0x00, 0x00, 0x00, 0x02})
	sendStream.Extra()
	_, err3 := sendStream.Write(request.Raw)
	if err3 != nil {
		t.Error(err3)
	}

	time.Sleep(1 * time.Second)
	_ = server.CloseListen()
}

//fade server
//can recv and handle msg
//respond when client read
type mockNetworkServer struct {
	localPeerId   peer.ID
	listenAddr    []ma.Multiaddr
	Handler       network.ConnHandler
	serverNetwork network.Network
	name          string
}

func (d *mockNetworkServer) AddTempListenAddresses(multiaddrs []ma.Multiaddr) {
	panic("implement me")
}

func (d *mockNetworkServer) GetTempListenAddresses() []ma.Multiaddr {
	panic("implement me")
}

func (d *mockNetworkServer) DirectListen(ctx context.Context, addrs ...ma.Multiaddr) error {
	panic("implement me")
}

func (d *mockNetworkServer) RelayListen(listener net.Listener) {
	panic("implement me")
}

func (d *mockNetworkServer) RelayDial(conn network.Conn, rAddr ma.Multiaddr) (network.Conn, error) {
	panic("implement me")
}

func (d *mockNetworkServer) Dial(ctx context.Context, remoteAddr ma.Multiaddr) (network.Conn, error) {

	return &mockNetworkConnClient{
		remoteAddr: remoteAddr,
		localAddr:  d.listenAddr[0],
		netWork:    *d,
	}, nil
}
func (d *mockNetworkServer) dial(serverNetwork *mockNetworkServer, remoteAddr ma.Multiaddr) (network.Conn, error) {
	d.serverNetwork = serverNetwork

	m := &mockNetworkConnClient{
		remoteAddr:    remoteAddr,
		localAddr:     d.listenAddr[0],
		netWork:       *d,
		networkServer: serverNetwork,
	}
	m.ReceiveStream = make(chan bool)
	go func() {
		fmt.Println("dial m.ReceiveStream <- true")
		m.ReceiveStream <- true
	}()

	return m, nil
}
func (d *mockNetworkServer) Close() error {
	return nil
}

func (d *mockNetworkServer) Listen(ctx context.Context, addresses ...ma.Multiaddr) error {
	d.listenAddr = append(d.listenAddr, addresses...)
	return nil
}

func (d *mockNetworkServer) ListenAddresses() []ma.Multiaddr {
	return d.listenAddr
}

func (d *mockNetworkServer) SetNewConnHandler(handler network.ConnHandler) {
	d.Handler = handler
}

func (d *mockNetworkServer) Disconnect(conn network.Conn) error {
	return nil
}

func (d *mockNetworkServer) Closed() bool {
	return true
}

func (d *mockNetworkServer) LocalPeerID() peer.ID {
	return d.localPeerId
}

//network.Conn
type mockNetworkConnClient struct {
	remoteAddr    ma.Multiaddr
	localAddr     ma.Multiaddr
	remotePeerID  peer.ID
	localPeerId   peer.ID
	netWork       mockNetworkServer
	networkServer *mockNetworkServer
	ReceiveStream chan bool
}

func (m mockNetworkConnClient) SetDeadline(t time.Time) error {
	panic("implement me")
}

func (m mockNetworkConnClient) SetReadDeadline(t time.Time) error {
	panic("implement me")
}

func (m mockNetworkConnClient) SetWriteDeadline(t time.Time) error {
	panic("implement me")
}

func (m mockNetworkConnClient) Close() error {
	panic("implement me")
}

func (m mockNetworkConnClient) Direction() network.Direction {
	panic("implement me")
}

func (m mockNetworkConnClient) EstablishedTime() time.Time {
	panic("implement me")
}

func (m mockNetworkConnClient) Extra() map[interface{}]interface{} {
	panic("implement me")
}

func (m mockNetworkConnClient) SetClosed() {
	panic("implement me")
}

func (m mockNetworkConnClient) IsClosed() bool {
	return false
}

func (m mockNetworkConnClient) LocalAddr() ma.Multiaddr {
	return m.localAddr
}

func (m mockNetworkConnClient) LocalNetAddr() net.Addr {

	tmp := m.localAddr.String()
	r := strings.Split(tmp, "/")
	port := r[4]
	nPort, _ := strconv.Atoi(port)

	rAddr := &net.UDPAddr{
		IP:   net.ParseIP(r[2]),
		Port: nPort,
	}
	return rAddr
}

func (m mockNetworkConnClient) LocalPeerID() peer.ID {
	return m.localPeerId
}

func (m mockNetworkConnClient) RemoteAddr() ma.Multiaddr {
	return m.remoteAddr
}

func (m mockNetworkConnClient) RemoteNetAddr() net.Addr {
	panic("implement me")
}

func (m *mockNetworkConnClient) RemotePeerID() peer.ID {
	return m.remotePeerID
}

func (m mockNetworkConnClient) Network() network.Network {
	return m.networkServer
}

var networkStreamC mockNetworkStreamC

func (m *mockNetworkConnClient) CreateSendStream() (network.SendStream, error) {
	networkStreamC.networkConn = m
	networkStreamC.network = m.netWork
	networkStreamC.networkServer = m.networkServer
	networkStreamC.clientMsg = true
	return &networkStreamC, nil
}

func (m *mockNetworkConnClient) AcceptReceiveStream() (network.ReceiveStream, error) {
	select {
	case <-m.ReceiveStream:
		return &networkStreamC, nil
	case <-time.After(time.Second * 5):
		return nil, errors.New("time out")
	}

}

func (m mockNetworkConnClient) CreateBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

func (m mockNetworkConnClient) AcceptBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

type mockNetworkStreamC struct {
	network       mockNetworkServer
	networkServer *mockNetworkServer
	networkConn   network.Conn
	msgBus        chan []byte
	clientMsg     bool //下一个是mock的client调用write，需要穿msgbus
}

func (m mockNetworkStreamC) Close() error {
	panic("implement me")
}

func (m mockNetworkStreamC) Direction() network.Direction {
	panic("implement me")
}

func (m mockNetworkStreamC) EstablishedTime() time.Time {
	panic("implement me")
}

func (m *mockNetworkStreamC) Extra() map[interface{}]interface{} {
	fmt.Println("Extra")
	m.networkServer.Handler(m.networkConn)
	m.clientMsg = true
	return nil
}

func (m mockNetworkStreamC) SetClosed() {
	panic("implement me")
}

func (m mockNetworkStreamC) IsClosed() bool {
	panic("implement me")
}

func (m *mockNetworkStreamC) Conn() network.Conn {
	return m.networkConn
}

func (m *mockNetworkStreamC) Write(p []byte) (n int, err error) {
	if m.msgBus == nil {
		m.msgBus = make(chan []byte)
	}

	fmt.Println(p)
	if networkStreamC.clientMsg {
		networkStreamC.clientMsg = false
		//mock发送，需要服务端处理
		go func() {
			fmt.Println("mockNetworkStreamC m.msgBus <- p")
			m.msgBus <- p
		}()

	} else {
		//服务端回包，直接打印
		fmt.Println("server response")
	}

	return len(p), err
}

func (m *mockNetworkStreamC) Read(p []byte) (n int, err error) {

	fmt.Println("mockNetworkStreamC Read")
	select {
	case buff := <-m.msgBus:
		fmt.Println("mockNetworkStreamC buff := <-m.msgBus")
		copy(p, buff)
		return len(p), nil
		//case <-time.After(10 * time.Second):
		//	fmt.Println("mockNetworkStreamC mock read time out")
		//	return 0, errors.New("mock read time out")
	case <-time.After(time.Second * 5):
		return 0, errors.New("time out")
	}

}
