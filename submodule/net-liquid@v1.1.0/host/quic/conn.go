/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package quic

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/types"
	"github.com/lucas-clemente/quic-go"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var _ network.Conn = (*qConn)(nil)

// qConn is an implementation of network.Conn interface.
// It wraps with a quic session as the connection of transport.
type qConn struct {
	// state object, used to store connection metadata
	network.BasicStat
	// the network object to which the connection object belongs
	nw   *qNetwork
	sess quic.Session

	// local address
	laddr ma.Multiaddr
	// remote address
	raddr ma.Multiaddr

	// local peer ID
	lPID peer.ID
	// remote peer ID
	rPID peer.ID

	closeOnce sync.Once
}

// NewQConn create a new qConn.
func newQConn(nw *qNetwork, sess quic.Session,
	direction network.Direction, loadPidFunc types.LoadPeerIdFromCMTlsCertFunc) (*qConn, error) {
	lAddr, rAddr, err := resolveLocalAndRemoteAddrFromQuicSess(sess)
	if err != nil {
		return nil, err
	}
	lPID := nw.lPID
	// resolve remote PID from sess.ConnectionState().TLS.ConnectionState.PeerCertificates
	state := fromClientSessionState(sess.ConnectionState())
	// cmCerts, err := ParseQX509CertsToCMX509Certs(state.TLS.PeerCertificates)
	// if err != nil {
	// 	return nil, err
	// }
	rPID, err := loadPidFunc(state.TLS.PeerCertificates)
	if err != nil {
		return nil, err
	}
	qc := &qConn{
		BasicStat: *network.NewStat(direction, time.Now(), nil),
		nw:        nw,
		sess:      sess,
		laddr:     lAddr,
		raddr:     rAddr,
		lPID:      lPID,
		rPID:      rPID,
	}
	return qc, nil
}

// resolveLocalAndRemoteAddrFromQuicSess resolves local and remote addresses from the quic session object
func resolveLocalAndRemoteAddrFromQuicSess(sess quic.Session) (lAddr, rAddr ma.Multiaddr, err error) {
	// resolve local net address
	lAddr, err = manet.FromNetAddr(sess.LocalAddr())
	if err != nil {
		_ = sess.CloseWithError(ErrCodeCloseConn, ErrMsgCloseConn)
		return nil, nil, fmt.Errorf("parse local addr err: %s", err.Error())
	}
	lAddr = lAddr.Encapsulate(quicMa)
	// resolve remote net address
	rAddr, err = manet.FromNetAddr(sess.RemoteAddr())
	if err != nil {
		_ = sess.CloseWithError(ErrCodeCloseConn, ErrMsgCloseConn)
		return nil, nil, fmt.Errorf("parse remote addr err: %s", err.Error())
	}
	rAddr = rAddr.Encapsulate(quicMa)
	return lAddr, rAddr, nil
}

// LocalAddr is the local net multi-address of the connection.
func (q *qConn) LocalAddr() ma.Multiaddr {
	return q.laddr
}

// LocalNetAddr is the local net address of the connection.
func (q *qConn) LocalNetAddr() net.Addr {
	return q.sess.LocalAddr()
}

// RemoteAddr is the remote net address of the connection.
func (q *qConn) RemoteAddr() ma.Multiaddr {
	return q.raddr
}

// RemoteNetAddr is the remote net address of the connection.
func (q *qConn) RemoteNetAddr() net.Addr {
	return q.sess.RemoteAddr()
}

// Network is the network instance who create this connection.
func (q *qConn) Network() network.Network {
	return q.nw
}

// Close this connection.
func (q *qConn) Close() error {
	var err error
	q.closeOnce.Do(func() {
		q.SetClosed()
		err = q.sess.CloseWithError(ErrCodeCloseConn, ErrMsgCloseConn)
	})
	return err
}

// CreateSendStream try to open a send stream with the connection.
func (q *qConn) CreateSendStream() (network.SendStream, error) {
	qs, err := q.sess.OpenUniStreamSync(context.Background())
	if err != nil {
		return nil, err
	}
	s := NewQSendStream(q, qs)
	return s, nil
}

// AcceptReceiveStream accept a receive stream with the connection.
// It will block until a new receive stream accepted or connection closed.
func (q *qConn) AcceptReceiveStream() (network.ReceiveStream, error) {
	qs, err := q.sess.AcceptUniStream(context.Background())
	if err != nil {
		return nil, err
	}
	s := NewQReceiveStream(q, qs)
	return s, nil
}

// CreateBidirectionalStream try to open a bidirectional stream with the connection.
func (q *qConn) CreateBidirectionalStream() (network.Stream, error) {
	qs, err := q.sess.OpenStreamSync(context.Background())
	if err != nil {
		return nil, err
	}
	s := NewQStream(q, qs, network.Outbound)
	return s, nil
}

// AcceptBidirectionalStream accept a bidirectional stream with the connection.
// It will block until a new bidirectional stream accepted or connection closed.
func (q *qConn) AcceptBidirectionalStream() (network.Stream, error) {
	qs, err := q.sess.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	s := NewQStream(q, qs, network.Inbound)
	return s, nil
}

// RemotePeerID is the remote peer id of the connection.
func (q *qConn) RemotePeerID() peer.ID {
	return q.rPID
}

// LocalPeerID is the local peer id of the connection.
func (q *qConn) LocalPeerID() peer.ID {
	return q.lPID
}

// SetDeadline set a dead line for the connection object
func (c *qConn) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline set the dead line of the connection object read operation
func (c *qConn) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline set the dead line of the connection object write operation
func (c *qConn) SetWriteDeadline(t time.Time) error {
	return nil
}
