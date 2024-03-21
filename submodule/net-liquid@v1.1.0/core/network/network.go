/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package network

import (
	"context"
	"io"

	ma "github.com/multiformats/go-multiaddr"

	"chainmaker.org/chainmaker/net-liquid/core/peer"
)

// ConnHandler is a function for handling connections.
type ConnHandler func(conn Conn) (bool, error)

// Network is a state machine interface provides a Dialer and a Listener to build a network.
type Network interface {
	Dialer
	Listener
	io.Closer
	// SetNewConnHandler register a ConnHandler to handle the connection established.
	SetNewConnHandler(handler ConnHandler)
	// Disconnect a connection.
	Disconnect(conn Conn) error
	// Closed return whether network closed.
	Closed() bool
	// LocalPeerID return the local peer id.
	LocalPeerID() peer.ID
	// RelayDial using conn and addr of the relay peer, get the relay conn
	RelayDial(conn Conn, rAddr ma.Multiaddr) (Conn, error)
	// GetTempListenAddresses return the local addr dial success
	GetTempListenAddresses() []ma.Multiaddr
	// AddTempListenAddresses add the remote addr accept success
	AddTempListenAddresses([]ma.Multiaddr)
	// DirectListen listen without check and support listen repeat
	DirectListen(ctx context.Context, addrs ...ma.Multiaddr) error
}
