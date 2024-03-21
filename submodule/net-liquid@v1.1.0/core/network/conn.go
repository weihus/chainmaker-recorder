/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package network

import (
	"io"
	"net"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// Conn defined a connection with remote peer.
type Conn interface {
	io.Closer
	Stat
	// LocalAddr is the local net multi-address of the connection.
	LocalAddr() ma.Multiaddr
	// LocalNetAddr is the local net address of the connection.
	LocalNetAddr() net.Addr
	// LocalPeerID is the local peer id of the connection.
	LocalPeerID() peer.ID
	// RemoteAddr is the remote net multi-address of the connection.
	RemoteAddr() ma.Multiaddr
	// RemoteNetAddr is the remote net address of the connection.
	RemoteNetAddr() net.Addr
	// RemotePeerID is the remote peer id of the connection.
	RemotePeerID() peer.ID
	// Network is the network instance who create this connection.
	Network() Network
	// CreateSendStream try to open a sending stream with the connection.
	CreateSendStream() (SendStream, error)
	// AcceptReceiveStream accept a receiving stream with the connection.
	// It will block until a new receiving stream accepted or connection closed.
	AcceptReceiveStream() (ReceiveStream, error)
	// CreateBidirectionalStream try to open a bidirectional stream with the connection.
	CreateBidirectionalStream() (Stream, error)
	// AcceptBidirectionalStream accept a bidirectional stream with the connection.
	// It will block until a new bidirectional stream accepted or connection closed.
	AcceptBidirectionalStream() (Stream, error)

	// SetDeadline used by the relay
	SetDeadline(t time.Time) error

	// SetReadDeadline used by the relay
	SetReadDeadline(t time.Time) error

	// SetWriteDeadline used by the relay
	SetWriteDeadline(t time.Time) error
}
