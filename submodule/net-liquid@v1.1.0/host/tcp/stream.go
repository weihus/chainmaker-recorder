/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tcp

import (
	"sync"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"github.com/libp2p/go-yamux/v2"
)

var _ network.SendStream = (*yamuxSendStream)(nil)

// yamuxSendStream an implementation of the SendStream interface,
// which is a one-way stream and can only perform write operations.
type yamuxSendStream struct {
	// state object, used to store stream metadata
	network.BasicStat
	// the connection object to which the stream object belongs
	c *conn
	// yamux stream object
	ys *yamux.Stream

	closeOnce sync.Once
}

// newSendStream create a sendStream instance
func newSendStream(c *conn, ys *yamux.Stream) *yamuxSendStream {
	ss := &yamuxSendStream{
		BasicStat: *network.NewStat(network.Outbound, time.Now(), nil),
		c:         c,
		ys:        ys,
		closeOnce: sync.Once{},
	}
	return ss
}

// Close close the stream object
func (y *yamuxSendStream) Close() error {
	var err error
	y.closeOnce.Do(func() {
		y.SetClosed()
		err = y.ys.CloseWrite()
	})
	return err
}

// Conn get the connection object to which the qSendStream object belongs
func (y *yamuxSendStream) Conn() network.Conn {
	return y.c
}

// Write write data to the stream object
func (y *yamuxSendStream) Write(p []byte) (n int, err error) {
	return y.ys.Write(p)
}

var _ network.ReceiveStream = (*yamuxReceiveStream)(nil)

// yamuxSendStream an implementation of the ReceiveStream interface,
// which is a one-way stream and can only perform read operations.
type yamuxReceiveStream struct {
	// state object, used to store stream metadata
	network.BasicStat
	// the connection object to which the stream object belongs
	c *conn
	// the stream object that actually does the work
	ys *yamux.Stream

	closeOnce sync.Once
}

// newReceiveStream create a receiveStream instance.
func newReceiveStream(c *conn, ys *yamux.Stream) *yamuxReceiveStream {
	rs := &yamuxReceiveStream{
		BasicStat: *network.NewStat(network.Inbound, time.Now(), nil),
		c:         c,
		ys:        ys,
		closeOnce: sync.Once{},
	}
	return rs
}

// Close close stream object
func (y *yamuxReceiveStream) Close() error {
	var err error
	y.closeOnce.Do(func() {
		y.SetClosed()
		err = y.ys.CloseRead()
	})
	return err
}

// Conn get the connection object to which the qSendStream object belongs
func (y *yamuxReceiveStream) Conn() network.Conn {
	return y.c
}

// Read read data from stream object
func (y *yamuxReceiveStream) Read(p []byte) (n int, err error) {
	return y.ys.Read(p)
}

var _ network.Stream = (*yamuxStream)(nil)

// yamuxSendStream an implementation of the Stream interface is a bidirectional stream
// that can perform read operations and write operations
type yamuxStream struct {
	// state object, used to store stream metadata
	network.BasicStat
	// the connection object to which the stream object belongs
	c *conn
	// the stream object that actually does the work
	ys *yamux.Stream

	closeOnce sync.Once
}

// newStream create a stream instance
func newStream(c *conn, ys *yamux.Stream, dir network.Direction) *yamuxStream {
	s := &yamuxStream{
		BasicStat: *network.NewStat(dir, time.Now(), nil),
		c:         c,
		ys:        ys,
		closeOnce: sync.Once{},
	}
	return s
}

// Close close stream object
func (y *yamuxStream) Close() error {
	var err error
	y.closeOnce.Do(func() {
		y.SetClosed()
		err = y.ys.Close()
	})
	return err
}

// Conn get the connection object to which the qReceiveStream object belongs
func (y *yamuxStream) Conn() network.Conn {
	return y.c
}

// Write write data to the stream object
func (y *yamuxStream) Write(p []byte) (n int, err error) {
	return y.ys.Write(p)
}

// Read read data from stream object
func (y *yamuxStream) Read(p []byte) (n int, err error) {
	return y.ys.Read(p)
}
