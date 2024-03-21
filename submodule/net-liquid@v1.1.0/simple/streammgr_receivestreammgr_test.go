package simple

import (
	"net"
	"testing"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestReceiveStreamManager(t *testing.T) {
	var peerReceiveStreamMaxCount = 10
	pid := peer.ID("QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")

	poolMgr := NewReceiveStreamManager(1)

	//set capacity
	poolMgr.SetPeerReceiveStreamMaxCount(peerReceiveStreamMaxCount)

	// add receive stream
	connObj := &connMMock{}
	for i := 0; i < peerReceiveStreamMaxCount-1; i++ {
		err := poolMgr.AddPeerReceiveStream(pid, connObj, &receiveStreamMock{connObj})
		require.Nil(t, err, "add receive stream err")
	}

	tempStream01 := &receiveStreamMock{connObj}
	tempStream02 := &receiveStreamMock{connObj}

	err := poolMgr.AddPeerReceiveStream(pid, connObj, tempStream01)
	require.Nil(t, err, "receive stream pool is full")

	// will err
	err = poolMgr.AddPeerReceiveStream(pid, connObj, tempStream02)
	require.NotNil(t, err, "add receive stream err")

	// get current stream count
	currReceiveStreamCount := poolMgr.GetCurrentPeerReceiveStreamCount(pid)
	require.Equal(t, peerReceiveStreamMaxCount, currReceiveStreamCount, "receive stream count err")

	err = poolMgr.RemovePeerReceiveStream(pid, connObj, tempStream01)
	require.Nil(t, err, "remove receive stream err")

	err = poolMgr.ClosePeerReceiveStreams(pid, connObj)
	require.Nil(t, err, "close receive stream err")

	poolMgr.Reset()

	currReceiveStreamCount = poolMgr.GetCurrentPeerReceiveStreamCount(pid)
	require.Equal(t, 0, currReceiveStreamCount, "receive stream count err")

}

var _ network.Conn = (*connMMock)(nil)

type connMMock struct {
}

func (stub *connMMock) LocalNetAddr() net.Addr {
	panic("implement me")
}

func (stub *connMMock) RemoteNetAddr() net.Addr {
	panic("implement me")
}

func (stub *connMMock) Close() error {
	return nil
}

func (stub *connMMock) Direction() network.Direction {
	panic("implement me")
}

func (stub *connMMock) EstablishedTime() time.Time {
	panic("implement me")
}

func (stub *connMMock) Extra() map[interface{}]interface{} {
	panic("implement me")
}

func (stub *connMMock) SetClosed() {
	panic("implement me")
}

func (stub *connMMock) IsClosed() bool {
	panic("implement me")
}

func (stub *connMMock) LocalAddr() ma.Multiaddr {
	panic("implement me")
}

func (stub *connMMock) LocalPeerID() peer.ID {
	panic("implement me")
}

func (stub *connMMock) RemoteAddr() ma.Multiaddr {
	panic("implement me")
}

func (stub *connMMock) RemotePeerID() peer.ID {
	return ""
}

func (stub *connMMock) Network() network.Network {
	panic("implement me")
}

func (stub *connMMock) CreateSendStream() (network.SendStream, error) {
	return nil, nil
}

func (stub *connMMock) AcceptReceiveStream() (network.ReceiveStream, error) {
	panic("implement me")
}

func (stub *connMMock) CreateBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

func (stub *connMMock) AcceptBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

func (stub *connMMock) NewSendStream() (network.SendStream, error) {
	return nil, nil
}

func (stub *connMMock) SetDeadline(t time.Time) error {
	panic("implement me")
}

// SetReadDeadline used by the relay
func (stub *connMMock) SetReadDeadline(t time.Time) error {
	panic("implement me")
}

// SetWriteDeadline used by the relay
func (stub *connMMock) SetWriteDeadline(t time.Time) error {
	panic("implement me")
}

var _ network.ReceiveStream = (*receiveStreamMock)(nil)

type receiveStreamMock struct {
	conn network.Conn
}

func (r receiveStreamMock) Close() error {
	return nil
}

func (r receiveStreamMock) Direction() network.Direction {
	//TODO implement me
	panic("implement me")
}

func (r receiveStreamMock) EstablishedTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (r receiveStreamMock) Extra() map[interface{}]interface{} {
	//TODO implement me
	panic("implement me")
}

func (r receiveStreamMock) SetClosed() {
	//TODO implement me
	panic("implement me")
}

func (r receiveStreamMock) IsClosed() bool {
	//TODO implement me
	panic("implement me")
}

func (r receiveStreamMock) Conn() network.Conn {
	//TODO implement me
	panic("implement me")
}

func (r receiveStreamMock) Read(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}
