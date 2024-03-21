/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package relay

import (
	"net"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
)

// Conn the relay conn (sAR/sRB)
type Conn struct {
	Stream network.Stream
}

// NetAddr addr
type NetAddr struct {
	Addr string
}

// Network return the "tcp"
func (n *NetAddr) Network() string {
	return "tcp"
}

//String return the string addr
func (n *NetAddr) String() string {
	return n.Addr
}

// Read the relay stream read
func (c *Conn) Read(buf []byte) (int, error) {
	return c.Stream.Read(buf)
}

// Write the relay stream write
func (c *Conn) Write(buf []byte) (int, error) {
	return c.Stream.Write(buf)
}

// LocalAddr return the relay stream LocalNetAddr
func (c *Conn) LocalAddr() net.Addr {
	return c.Stream.Conn().LocalNetAddr()
}

// RemoteAddr return the relay stream RemoteNetAddr
func (c *Conn) RemoteAddr() net.Addr {
	return c.Stream.Conn().RemoteNetAddr()
}

// Close the relay stream close()
func (c *Conn) Close() error {
	return c.Stream.Close()
}

// SetDeadline in order to implement the net.Conn interface
func (c *Conn) SetDeadline(t time.Time) error {
	return c.Stream.Conn().SetDeadline(t)
}

// SetReadDeadline in order to implement the net.Conn interface
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.Stream.Conn().SetReadDeadline(t)
}

// SetWriteDeadline in order to implement the net.Conn interface
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.Stream.Conn().SetWriteDeadline(t)
}
