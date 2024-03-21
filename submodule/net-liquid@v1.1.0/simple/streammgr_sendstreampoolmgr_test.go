package simple

import (
	"net"
	"testing"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/logger"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestSendStreamPoolMgr(t *testing.T) {
	pid := peer.ID("QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")
	var poolSize int32 = 10
	var poolCapacity int32 = 20

	connObj := &connMock{}
	log := logger.NewLogPrinter("TEST")

	streamPool, err := NewSimpleStreamPool(poolSize, poolCapacity, connObj, nil, logger.NilLogger)
	require.Nil(t, err, "new stream pool error")
	require.NotNil(t, streamPool, "new stream pool error, stream is nil")

	poolMgr := NewSendStreamPoolManager(nil, log)
	err = poolMgr.AddPeerConnSendStreamPool(pid, connObj, streamPool)
	require.Nil(t, err, "addr peer conn stream pool error")

	pool := poolMgr.GetPeerBestConnSendStreamPool(pid)
	require.NotNil(t, pool, "get conn stream pool error")

	err = poolMgr.RemovePeerConnAndCloseSendStreamPool(pid, connObj)
	require.Nil(t, err, "remove peer conn stream pool error")

	pool = poolMgr.GetPeerBestConnSendStreamPool(pid)
	require.Nil(t, pool, "conn stream pool should be nil")

	poolMgr.Reset()
	pool = poolMgr.GetPeerBestConnSendStreamPool(pid)
	require.Nil(t, pool, "conn stream pool should be nil")

}

var _ network.Conn = (*connMock)(nil)

type connMock struct {
}

func (stub *connMock) LocalNetAddr() net.Addr {
	panic("implement me")
}

func (stub *connMock) RemoteNetAddr() net.Addr {
	panic("implement me")
}

func (stub *connMock) Close() error {
	return nil
}

func (stub *connMock) Direction() network.Direction {
	panic("implement me")
}

func (stub *connMock) EstablishedTime() time.Time {
	panic("implement me")
}

func (stub *connMock) Extra() map[interface{}]interface{} {
	panic("implement me")
}

func (stub *connMock) SetClosed() {
	panic("implement me")
}

func (stub *connMock) IsClosed() bool {
	panic("implement me")
}

func (stub *connMock) LocalAddr() ma.Multiaddr {
	panic("implement me")
}

func (stub *connMock) LocalPeerID() peer.ID {
	panic("implement me")
}

func (stub *connMock) RemoteAddr() ma.Multiaddr {
	panic("implement me")
}

func (stub *connMock) RemotePeerID() peer.ID {
	return ""
}

func (stub *connMock) Network() network.Network {
	panic("implement me")
}

func (stub *connMock) CreateSendStream() (network.SendStream, error) {
	return nil, nil
}

func (stub *connMock) AcceptReceiveStream() (network.ReceiveStream, error) {
	panic("implement me")
}

func (stub *connMock) CreateBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

func (stub *connMock) AcceptBidirectionalStream() (network.Stream, error) {
	panic("implement me")
}

func (stub *connMock) NewSendStream() (network.SendStream, error) {
	return nil, nil
}

func (stub *connMock) SetDeadline(t time.Time) error {
	panic("implement me")
}

// SetReadDeadline used by the relay
func (stub *connMock) SetReadDeadline(t time.Time) error {
	panic("implement me")
}

// SetWriteDeadline used by the relay
func (stub *connMock) SetWriteDeadline(t time.Time) error {
	panic("implement me")
}
