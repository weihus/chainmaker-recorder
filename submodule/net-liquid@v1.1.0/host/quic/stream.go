/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package quic

import (
	"sync"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	"github.com/lucas-clemente/quic-go"
)

var _ network.SendStream = (*qSendStream)(nil)

// qSendStream an implementation of the SendStream interface,
// which is a one-way stream and can only perform write operations.
type qSendStream struct {
	// state object, used to store stream metadata
	network.BasicStat
	// the connection object to which the stream object belongs
	qc *qConn
	// send stream object
	qs        quic.SendStream
	closeOnce sync.Once
}

// NewQSendStream create a new SendStream object
func NewQSendStream(qc *qConn, qs quic.SendStream) network.SendStream {
	q := &qSendStream{
		BasicStat: *network.NewStat(network.Outbound, time.Now(), nil),
		qc:        qc,
		qs:        qs,
	}
	return q
}

// Close close the stream object
func (q *qSendStream) Close() error {
	var err error
	q.closeOnce.Do(func() {
		q.SetClosed()
		err = q.qs.Close()
	})
	return err
}

// Write write data to the stream object
func (q *qSendStream) Write(p []byte) (n int, err error) {
	return q.qs.Write(p)
}

// Conn get the connection object to which the qSendStream object belongs
func (q *qSendStream) Conn() network.Conn {
	return q.qc
}

var _ network.ReceiveStream = (*qReceiveStream)(nil)

// qReceiveStream an implementation of the ReceiveStream interface,
// which is a one-way stream and can only perform read operations.
type qReceiveStream struct {
	// state object, used to store stream metadata
	network.BasicStat
	// the connection object to which the stream object belongs
	qc *qConn
	// receive stream object
	qs quic.ReceiveStream

	closeOnce sync.Once
}

// NewQReceiveStream create a new ReceiveStream object
func NewQReceiveStream(qc *qConn, qs quic.ReceiveStream) network.ReceiveStream {
	q := &qReceiveStream{
		BasicStat: *network.NewStat(network.Inbound, time.Now(), nil),
		qc:        qc,
		qs:        qs,
	}
	return q
}

// Close close quic receive stream object
func (q *qReceiveStream) Close() error {
	var err error
	q.closeOnce.Do(func() {
		q.SetClosed()
		q.qs.CancelRead(ErrCodeCloseStream)
	})
	return err
}

// Read read data from stream object
func (q *qReceiveStream) Read(p []byte) (n int, err error) {
	return q.qs.Read(p)
}

// Conn get the connection object to which the qReceiveStream object belongs
func (q *qReceiveStream) Conn() network.Conn {
	return q.qc
}

var _ network.Stream = (*qStream)(nil)

// qStream an implementation of the Stream interface is a bidirectional stream
// that can perform read operations and write operations.
type qStream struct {
	// state object, used to store stream metadata
	network.BasicStat
	// the connection object to which the stream object belongs
	qc *qConn
	// stream object
	qs quic.Stream

	closeOnce sync.Once
}

// NewQStream create a new stream object
func NewQStream(qc *qConn, qs quic.Stream, direction network.Direction) network.Stream {
	q := &qStream{
		BasicStat: *network.NewStat(direction, time.Now(), nil),
		qc:        qc,
		qs:        qs,
	}
	return q
}

// Close close the stream object
func (q *qStream) Close() error {
	var err error
	q.closeOnce.Do(func() {
		q.SetClosed()
		err = q.qs.Close()
		q.qs.CancelRead(ErrCodeCloseStream)
	})
	return err
}

// Read read data from stream object
func (q *qStream) Read(p []byte) (n int, err error) {
	return q.qs.Read(p)
}

// Write write data to the stream object
func (q *qStream) Write(p []byte) (n int, err error) {
	return q.qs.Write(p)
}

// Conn get the connection object to which the qReceiveStream object belongs
func (q *qStream) Conn() network.Conn {
	return q.qc
}
