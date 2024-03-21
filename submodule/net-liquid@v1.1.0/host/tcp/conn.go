/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tcp

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"

	cmTls "chainmaker.org/chainmaker/common/v2/crypto/tls"
	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/util"
	"chainmaker.org/chainmaker/net-liquid/exMultiaddr"
	"github.com/libp2p/go-yamux/v2"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var (
	// ErrConnClosed will be returned if the current connection closed.
	ErrConnClosed = errors.New("connection closed")
	// ErrUnknownDir will be returned if the direction is unknown.
	ErrUnknownDir = errors.New("unknown direction")
	// ErrNextProtoMismatch will be returned if next proto mismatch when tls handshaking.
	ErrNextProtoMismatch = errors.New("next proto mismatch")

	defaultYamuxConfig          = yamux.DefaultConfig()
	defaultYamuxConfigForStream = yamux.DefaultConfig()
)

func init() {
	defaultYamuxConfig.MaxStreamWindowSize = 32 << 20
	defaultYamuxConfig.LogOutput = ioutil.Discard
	defaultYamuxConfig.ReadBufSize = 0
	defaultYamuxConfig.ConnectionWriteTimeout = 1 * time.Second
	defaultYamuxConfig.EnableKeepAlive = true
	defaultYamuxConfig.KeepAliveInterval = 10 * time.Second
	defaultYamuxConfigForStream.MaxStreamWindowSize = 16 << 20
	defaultYamuxConfigForStream.LogOutput = ioutil.Discard
	defaultYamuxConfigForStream.ReadBufSize = 0
	defaultYamuxConfigForStream.EnableKeepAlive = false
}

var _ network.Conn = (*conn)(nil)

// conn is an implementation of network.Conn interface.
// If TLS enabled, the net.Conn will be upgraded to *tls.Conn.
// It wraps a yamux.Session which initialized with the Conn as the connection of transport.
type conn struct {
	// state object, used to store connection metadata
	network.BasicStat
	ctx context.Context

	// the network object to which the connection object belongs
	nw *tcpNetwork

	// the real underlying connection object
	c net.Conn

	sess *yamux.Session

	// used to create a one-way stream object,The stream object can only be read or written.
	sessForUni *yamux.Session

	// used to create bidirectional stream objects
	sessForBi *yamux.Session

	// local peer address
	laddr ma.Multiaddr
	// remote peer address
	raddr ma.Multiaddr
	// local peer ID
	lPID peer.ID
	// remote peer ID
	rPID peer.ID

	// send a value to the channel when the connection needs to be closed,unbuffered
	closeC    chan struct{}
	closeOnce sync.Once
}

// handshakeInbound handshake with remote peer over new inbound connection
func (c *conn) handshakeInbound(conn net.Conn) (net.Conn, error) {
	var err error
	finalConn := conn
	if c.nw.enableTls {
		// tls handshake
		// inbound conn as server
		tlsCfg := c.nw.tlsCfg.Clone()
		tlsConn := cmTls.Server(finalConn, tlsCfg)
		err = tlsConn.Handshake()
		if err != nil {
			_ = tlsConn.Close()
			return nil, err
		}
		connState := tlsConn.ConnectionState()
		if connState.NegotiatedProtocol != tlsCfg.NextProtos[0] {
			return nil, ErrNextProtoMismatch
		}
		c.rPID, err = c.nw.loadPidFunc(connState.PeerCertificates)
		if err != nil {
			_ = tlsConn.Close()
			return nil, err
		}
		finalConn = tlsConn
	} else {
		// exchange PID
		// receive pid
		rpidBytes := make([]byte, 46)
		_, err = finalConn.Read(rpidBytes)
		if err != nil {
			_ = finalConn.Close()
			return nil, err
		}
		c.rPID = peer.ID(rpidBytes)
		// send pid
		_, err = finalConn.Write([]byte(c.lPID))
		if err != nil {
			_ = finalConn.Close()
			return nil, err
		}
	}
	return finalConn, nil
}

// attachYamuxInbound create sessions object that communicates with the remote peer through the inbound connection
func (c *conn) attachYamuxInbound(conn net.Conn) error {
	// inbound conn as server
	sess, err2 := yamux.Server(conn, defaultYamuxConfig)
	if err2 != nil {
		_ = conn.Close()
		return err2
	}
	virtualConnForUni, err3 := sess.Accept()
	if err3 != nil {
		_ = sess.Close()
		_ = conn.Close()
		return err3
	}
	virtualConnForBi, err4 := sess.Accept()
	if err4 != nil {
		_ = virtualConnForUni.Close()
		_ = sess.Close()
		_ = conn.Close()
		return err4
	}

	sessForUni, err5 := yamux.Server(virtualConnForUni, defaultYamuxConfigForStream)
	if err5 != nil {
		_ = virtualConnForBi.Close()
		_ = virtualConnForUni.Close()
		_ = sess.Close()
		_ = conn.Close()
		return err5
	}

	sessForBi, err6 := yamux.Server(virtualConnForBi, defaultYamuxConfigForStream)
	if err6 != nil {
		_ = sessForUni.Close()
		_ = virtualConnForBi.Close()
		_ = virtualConnForUni.Close()
		_ = sess.Close()
		_ = conn.Close()
		return err6
	}
	c.c = conn
	c.sess = sess
	c.sessForUni = sessForUni
	c.sessForBi = sessForBi
	return nil
}

// handshakeOutbound handshake with remote peer over outbound connection
func (c *conn) handshakeOutbound(conn net.Conn, remoteAddr ma.Multiaddr) (net.Conn, error) {
	var err error
	finalConn := conn
	if c.nw.enableTls {
		// tls handshake
		// outbound conn as client
		tlsCfg := c.nw.tlsCfg.Clone()
		if remoteAddr != nil && haveDns(remoteAddr) {
			dnsDomain, _ := ma.SplitFirst(remoteAddr)
			if dnsDomain != nil {
				tlsCfg.ServerName, _ = dnsDomain.ValueForProtocol(dnsDomain.Protocol().Code)
			}
		}

		tlsConn := cmTls.Client(finalConn, tlsCfg)
		err = tlsConn.Handshake()
		if err != nil {
			_ = tlsConn.Close()
			return nil, err
		}
		connState := tlsConn.ConnectionState()
		if connState.NegotiatedProtocol != tlsCfg.NextProtos[0] {
			return nil, ErrNextProtoMismatch
		}
		c.rPID, err = c.nw.loadPidFunc(connState.PeerCertificates)
		if err != nil {
			_ = tlsConn.Close()
			return nil, err
		}
		finalConn = tlsConn
	} else {
		// exchange PID
		// send pid
		_, err = finalConn.Write([]byte(c.lPID))
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		// receive pid
		rpidBytes := make([]byte, 46)
		_, err = finalConn.Read(rpidBytes)
		if err != nil {
			_ = finalConn.Close()
			return nil, err
		}
		c.rPID = peer.ID(rpidBytes)
	}
	return finalConn, nil
}

func haveDns(addr ma.Multiaddr) bool {
	if addr == nil {
		return false
	}
	protocols := addr.Protocols()
	for _, p := range protocols {
		switch p.Code {
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6:
			return true
		}
	}
	return false
}

// attachYamuxOutbound create sessions object that communicates with the remote peer through the outbound connection
func (c *conn) attachYamuxOutbound(conn net.Conn) error {
	// outbound conn as client
	sess, err2 := yamux.Client(conn, defaultYamuxConfig)
	if err2 != nil {
		_ = conn.Close()
		return err2
	}
	virtualConnForUni, err3 := sess.Open(c.ctx)
	if err3 != nil {
		_ = sess.Close()
		_ = conn.Close()
		return err3
	}
	virtualConnForBi, err4 := sess.Open(c.ctx)
	if err4 != nil {
		_ = virtualConnForUni.Close()
		_ = sess.Close()
		_ = conn.Close()
		return err4
	}

	sessForUni, err5 := yamux.Client(virtualConnForUni, defaultYamuxConfigForStream)
	if err5 != nil {
		_ = virtualConnForBi.Close()
		_ = virtualConnForUni.Close()
		_ = sess.Close()
		_ = conn.Close()
		return err5
	}

	sessForBi, err6 := yamux.Client(virtualConnForBi, defaultYamuxConfigForStream)
	if err6 != nil {
		_ = sessForUni.Close()
		_ = virtualConnForBi.Close()
		_ = virtualConnForUni.Close()
		_ = sess.Close()
		_ = conn.Close()
		return err6
	}
	c.c = conn
	c.sess = sess
	c.sessForUni = sessForUni
	c.sessForBi = sessForBi
	return nil
}

// compareDirection compare who is client before tls shake hand
// compare port first,
// if ports are same ,then holePunch info or use c.Direction()
// return new Direction after compare
func (c *conn) compareDirection(conn net.Conn, remoteAddr ma.Multiaddr) network.Direction {
	compare := 0
	lAdd, _ := net.ResolveTCPAddr(conn.LocalAddr().Network(), conn.LocalAddr().String())
	rAdd, _ := net.ResolveTCPAddr(conn.RemoteAddr().Network(), conn.RemoteAddr().String())
	if lAdd == nil || rAdd == nil {
		return c.Direction()
	}

	// compare by port
	if lAdd.Port != rAdd.Port {
		compare = strings.Compare(fmt.Sprintf("%v", rAdd.Port), fmt.Sprintf("%v", lAdd.Port))
	} else {
		// only holePunch get here
		exAddr, _ := remoteAddr.(*exMultiaddr.ExInfoMultiaddr)
		if exAddr != nil && exAddr.GetExInfo() == "holePunch" {
			_, remotePID := util.GetNetAddrAndPidFromNormalMultiAddr(remoteAddr)
			compare = strings.Compare(string(c.lPID), remotePID.ToString())
		}
	}
	// same port and not holePunch
	if compare == 0 {
		return c.Direction()
	}
	var dir network.Direction
	if compare > 0 {
		dir = network.Inbound
	} else {
		dir = network.Outbound
	}
	return dir
}

// handshakeAndAttachYamux Process the connection object, perform the TLS handshake and create a session object
// that interacts with the renmote peer through the connection object.
func (c *conn) handshakeAndAttachYamux(conn net.Conn, remoteAddr ma.Multiaddr) error {
	var err error
	var finalConn net.Conn
	switch c.Direction() {
	case network.Inbound:
		finalConn, err = c.handshakeInbound(conn)
		if err != nil {
			return err
		}
		err = c.attachYamuxInbound(finalConn)
		if err != nil {
			return err
		}
	case network.Outbound:
		finalConn, err = c.handshakeOutbound(conn, remoteAddr)
		if err != nil {
			return err
		}
		err = c.attachYamuxOutbound(finalConn)
		if err != nil {
			return err
		}
	default:
		_ = c.Close()
		return ErrUnknownDir
	}
	return nil
}

// newConn create a new conn instance.
func newConn(ctx context.Context, nw *tcpNetwork, c net.Conn,
	dir network.Direction, needCompare bool, remoteAddr ma.Multiaddr) (*conn, error) {
	res := &conn{
		BasicStat:  *network.NewStat(dir, time.Now(), nil),
		ctx:        ctx,
		nw:         nw,
		c:          nil,
		sess:       nil,
		sessForUni: nil,
		sessForBi:  nil,
		laddr:      nil,
		raddr:      nil,
		lPID:       nw.LocalPeerID(),
		rPID:       "",
		closeC:     make(chan struct{}),
		closeOnce:  sync.Once{},
	}
	var err error
	res.laddr, err = manet.FromNetAddr(c.LocalAddr())
	if err != nil {
		return nil, err
	}

	res.raddr, err = manet.FromNetAddr(c.RemoteAddr())
	if err != nil {
		return nil, err
	}
	if needCompare {
		// compare Direction before handshake
		newDir := res.compareDirection(c, remoteAddr)
		// use new Dir
		res.BasicStat = *network.NewStat(newDir, time.Now(), nil)
	}
	// start tls handshake
	err = res.handshakeAndAttachYamux(c, remoteAddr)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Close this connection.
func (c *conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.SetClosed()
		close(c.closeC)
		err = c.sessForBi.Close()
		if err != nil {
			return
		}
		err = c.sessForUni.Close()
		if err != nil {
			return
		}
		err = c.sess.Close()
		if err != nil {
			return
		}
		//err = c.c.Close()
	})
	return err
}

// LocalAddr is the local net multi-address of the connection.
func (c *conn) LocalAddr() ma.Multiaddr {
	return c.laddr
}

// LocalNetAddr is the local net address of the connection.
func (c *conn) LocalNetAddr() net.Addr {
	return c.c.LocalAddr()
}

// LocalPeerID is the local peer id of the connection.
func (c *conn) LocalPeerID() peer.ID {
	return c.lPID
}

// RemoteAddr is the remote net address of the connection.
func (c *conn) RemoteAddr() ma.Multiaddr {
	return c.raddr
}

// RemoteNetAddr is the remote net address of the connection.
func (c *conn) RemoteNetAddr() net.Addr {
	return c.c.RemoteAddr()
}

// RemotePeerID is the remote peer id of the connection.
func (c *conn) RemotePeerID() peer.ID {
	return c.rPID
}

// Network is the network instance who create this connection.
func (c *conn) Network() network.Network {
	return c.nw
}

// CreateSendStream try to open a sending stream with the connection.
func (c *conn) CreateSendStream() (network.SendStream, error) {
	ys, err := c.sessForUni.OpenStream(c.ctx)
	if err != nil {
		return nil, err
	}
	_ = ys.CloseRead()
	return newSendStream(c, ys), nil
}

// AcceptReceiveStream accept a receiving stream with the connection.
// It will block until a new receiving stream accepted or connection closed.
func (c *conn) AcceptReceiveStream() (network.ReceiveStream, error) {
	select {
	case <-c.closeC:
		return nil, ErrConnClosed
	case <-c.sess.CloseChan():
		_ = c.Close()
		return nil, ErrConnClosed
	case <-c.ctx.Done():
		_ = c.Close()
		return nil, ErrConnClosed
	default:

	}
	rs, err := c.sessForUni.AcceptStream()
	if err != nil {
		return nil, err
	}
	_ = rs.CloseWrite()
	return newReceiveStream(c, rs), nil
}

// CreateBidirectionalStream try to open a bidirectional stream with the connection.
func (c *conn) CreateBidirectionalStream() (network.Stream, error) {
	ys, err := c.sessForBi.OpenStream(c.ctx)
	if err != nil {
		return nil, err
	}
	return newStream(c, ys, network.Outbound), nil
}

// AcceptBidirectionalStream accept a bidirectional stream with the connection.
// It will block until a new bidirectional stream accepted or connection closed.
func (c *conn) AcceptBidirectionalStream() (network.Stream, error) {
	select {
	case <-c.closeC:
		return nil, ErrConnClosed
	case <-c.sess.CloseChan():
		_ = c.Close()
		return nil, ErrConnClosed
	case <-c.ctx.Done():
		_ = c.Close()
		return nil, ErrConnClosed
	default:

	}
	s, err := c.sessForBi.AcceptStream()
	if err != nil {
		return nil, err
	}
	return newStream(c, s, network.Inbound), nil
}

// SetDeadline set the connection object dead line
func (c *conn) SetDeadline(t time.Time) error {
	return c.c.SetDeadline(t)
}

// SetReadDeadline set the dead line of the connection object read operation
func (c *conn) SetReadDeadline(t time.Time) error {
	return c.c.SetReadDeadline(t)
}

// SetWriteDeadline set the dead line of the connection object write operation
func (c *conn) SetWriteDeadline(t time.Time) error {
	return c.c.SetWriteDeadline(t)
}
