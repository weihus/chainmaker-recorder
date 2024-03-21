/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"chainmaker.org/chainmaker/net-liquid/relay"
	"chainmaker.org/chainmaker/net-liquid/relay/pb"

	cmTls "chainmaker.org/chainmaker/common/v2/crypto/tls"
	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/reuse"
	"chainmaker.org/chainmaker/net-liquid/core/types"
	"chainmaker.org/chainmaker/net-liquid/core/util"
	api "chainmaker.org/chainmaker/protocol/v2"
	ma "github.com/multiformats/go-multiaddr"
	mafmt "github.com/multiformats/go-multiaddr-fmt"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	// TCPNetworkVersion is the current version of tcp network.
	TCPNetworkVersion = "v0.0.1"
)

var (
	// ErrNilTlsCfg will be returned if tls config is nil when network starting.
	ErrNilTlsCfg = errors.New("nil tls config")
	// ErrEmptyTlsCerts will be returned if no tls cert given when network starting with tls enabled.
	ErrEmptyTlsCerts = errors.New("empty tls certs")
	// ErrNilAddr will be returned if the listening address is empty.
	ErrNilAddr = errors.New("nil addr")
	// ErrEmptyListenAddress will be returned if no listening address given.
	ErrEmptyListenAddress = errors.New("empty listen address")
	// ErrListenerRequired will be returned if no listener created.
	ErrListenerRequired = errors.New("at least one listener is required")
	// ErrConnRejectedByConnHandler will be returned if connection handler reject a connection when establishing.
	ErrConnRejectedByConnHandler = errors.New("connection rejected by conn handler")
	// ErrNotTheSameNetwork will be returned if the connection disconnected is not created by current network.
	ErrNotTheSameNetwork = errors.New("not the same network")
	// ErrPidMismatch will be returned if the remote peer id is not the expected one.
	ErrPidMismatch = errors.New("pid mismatch")
	// ErrNilLoadPidFunc will be returned if loadPidFunc is nil.
	ErrNilLoadPidFunc = errors.New("load peer id function required")
	// ErrWrongTcpAddr  will be returned if the address is wrong when calling Dial method.
	ErrWrongTcpAddr = errors.New("wrong tcp address format")
	// ErrEmptyLocalPeerId will be returned if load local peer id failed.
	ErrEmptyLocalPeerId = errors.New("empty local peer id")
	// ErrNoUsableLocalAddress will be returned if no usable local address found
	// when the local listening address is an unspecified address.
	ErrNoUsableLocalAddress = errors.New("no usable local address found")
	// ErrLocalPidNotSet will be returned if local peer id not set on insecurity mode.
	ErrLocalPidNotSet = errors.New("local peer id not set")

	listenMatcher      = mafmt.And(mafmt.IP, mafmt.Base(ma.P_TCP))
	dialMatcherNoP2p   = mafmt.TCP
	dialMatcherWithP2p = mafmt.And(mafmt.TCP, mafmt.Base(ma.P_P2P))

	control = reuse.Control
)

// Option is a function to set option value for tcp network.
type Option func(n *tcpNetwork) error

var _ network.Network = (*tcpNetwork)(nil)

// tcpNetwork is an implementation of network.Network interface.
// It uses TCP as transport layer.
// Crypto with TLS supported, and insecurity(TLS disabled) supported.
type tcpNetwork struct {
	mu   sync.RWMutex
	once sync.Once
	ctx  context.Context

	// tlc config object
	tlsCfg *cmTls.Config
	// parse peer ID from ChainMaker tls cert
	loadPidFunc types.LoadPeerIdFromCMTlsCertFunc
	// a flag, whether to enable tls
	enableTls   bool
	connHandler network.ConnHandler

	// local peer ID
	lPID peer.ID
	// local address list
	lAddrList     []ma.Multiaddr
	tempLAddrList []ma.Multiaddr
	// the listener corresponding to the address that has been listened
	tcpListeners []net.Listener
	// Whether the address has been listened
	listening bool

	// send a value to the channel when the connection needs to be closed,unbuffered
	closeChan chan struct{}

	logger api.Logger
}

// apply load configuration items for tcpNetwork object
func (t *tcpNetwork) apply(opt ...Option) error {
	for _, o := range opt {
		if err := o(t); err != nil {
			return err
		}
	}
	return nil
}

// WithTlsCfg set a cmTls.Config option value.
// If enable tls is false, cmTls.Config will not usable.
func WithTlsCfg(tlsCfg *cmTls.Config) Option {
	return func(n *tcpNetwork) error {
		n.tlsCfg = tlsCfg
		return nil
	}
}

// WithLoadPidFunc set a types.LoadPeerIdFromCMTlsCertFunc for loading peer.ID from cmx509 certs when tls handshaking.
func WithLoadPidFunc(f types.LoadPeerIdFromCMTlsCertFunc) Option {
	return func(n *tcpNetwork) error {
		n.loadPidFunc = f
		return nil
	}
}

// WithLocalPeerId will set the local peer.ID for the network.
// If LoadPidFunc option set, the local peer.ID set by this method will be overwritten, probably.
func WithLocalPeerId(pid peer.ID) Option {
	return func(n *tcpNetwork) error {
		n.lPID = pid
		return nil
	}
}

// WithEnableTls set a bool value deciding whether tls enabled.
func WithEnableTls(enable bool) Option {
	return func(n *tcpNetwork) error {
		n.enableTls = enable
		return nil
	}
}

// NewNetwork create a new network instance with TCP transport.
func NewNetwork(ctx context.Context, logger api.Logger, opt ...Option) (network.Network, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	n := &tcpNetwork{
		mu:   sync.RWMutex{},
		once: sync.Once{},
		ctx:  ctx,

		tlsCfg:        nil,
		loadPidFunc:   nil,
		enableTls:     true,
		lAddrList:     make([]ma.Multiaddr, 0, 10),
		tempLAddrList: make([]ma.Multiaddr, 0, 2),
		tcpListeners:  make([]net.Listener, 0, 10),
		closeChan:     make(chan struct{}),
		lPID:          "",

		logger: logger,
	}
	if err := n.apply(opt...); err != nil {
		return nil, err
	}

	if err := n.checkTlsCfg(); err != nil {
		return nil, err
	}

	if n.lPID == "" {
		return nil, ErrLocalPidNotSet
	}

	return n, nil
}

// checkTlsCfg check tls config
func (t *tcpNetwork) checkTlsCfg() error {
	if !t.enableTls {
		return nil
	}
	if t.tlsCfg == nil {
		return ErrNilTlsCfg
	}
	if t.tlsCfg.Certificates == nil || len(t.tlsCfg.Certificates) == 0 {
		return ErrEmptyTlsCerts
	}
	t.tlsCfg.NextProtos = []string{"liquid-network-tcp-" + TCPNetworkVersion}
	return nil
}

// canDial determine whether a given address can be dialed
func (t *tcpNetwork) canDial(addr ma.Multiaddr) bool {
	return dialMatcherNoP2p.Matches(addr) || dialMatcherWithP2p.Matches(addr)
}

// dial dial to a given remote address
func (t *tcpNetwork) dial(ctx context.Context, remoteAddr ma.Multiaddr) (*conn, []error) {
	errs := make([]error, 0)

	dialAddr, _ := util.GetNetAddrAndPidFromNormalMultiAddr(remoteAddr)
	// check dial address
	if !t.canDial(dialAddr) {
		errs = append(errs, ErrWrongTcpAddr)
		return nil, errs
	}

	nAddr, err := manet.ToNetAddr(dialAddr)
	if err != nil {
		errs = append(errs, err)
		return nil, errs
	}
	if ctx == nil {
		ctx = t.ctx
	}
	var tc *conn
	// try to dial to remote with each local address
	for i := range t.lAddrList {
		lAddr := t.lAddrList[i]
		lnAddr, err2 := manet.ToNetAddr(lAddr)
		if err2 != nil {
			errs = append(errs, err2)
			continue
		}
		// dial
		dialer := &net.Dialer{
			LocalAddr: lnAddr,
			Control:   control,
		}
		t.logger.Debugf("[TcpNetwork] local [%s],[%s]try dial to %s",
			lnAddr.String(), lnAddr.Network(), nAddr.String())
		c, err3 := dialer.DialContext(ctx, nAddr.Network(), nAddr.String())
		if err3 != nil {
			errs = append(errs, err3)
			continue
		}
		// create a new conn with net.Conn
		t.logger.Infof("[TcpNetwork] net conn, remote addr string: [%s], remote network string: [%s] remoteAddr %s",
			c.RemoteAddr().String(), c.RemoteAddr().Network(), remoteAddr.String())
		t.logger.Infof("[TcpNetwork] net conn, local addr string: [%s], local network string: [%s]",
			c.LocalAddr().String(), c.LocalAddr().Network())
		tc, err = newConn(ctx, t, c, network.Outbound, true, remoteAddr)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		break
	}
	if tc == nil {
		// all failed, try dial
		dialer := &net.Dialer{
			Control: control,
		}
		c, err3 := dialer.DialContext(ctx, nAddr.Network(), nAddr.String())
		if err3 != nil {
			errs = append(errs, err3)
			return nil, errs
		}
		// create a new conn with net.Conn
		tc, err = newConn(ctx, t, c, network.Outbound, true, remoteAddr)
		if err != nil {
			errs = append(errs, err)
			return nil, errs
		}
		// TODO: temp listener
		t.tempLAddrList = append(t.tempLAddrList, tc.laddr)
		t.logger.Debug("add tempLAddrList:", tc.laddr)
	}
	if tc != nil {
		errs = nil
	}
	return tc, errs
}

// Dial try to establish an outbound connection with the remote address.
func (t *tcpNetwork) Dial(ctx context.Context, remoteAddr ma.Multiaddr) (network.Conn, error) {
	// check network listen state
	if !t.listening {
		return nil, ErrListenerRequired
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	//var remotePID peer.ID
	// check dial address
	//if !t.canDial(remoteAddr) {
	//	return nil, ErrWrongTcpAddr
	//}
	dialAddr, remotePID := util.GetNetAddrAndPidFromNormalMultiAddr(remoteAddr)
	if dialAddr == nil && remotePID == "" {
		return nil, errors.New("wrong addr")
	}
	if dialAddr == nil {
		return nil, ErrWrongTcpAddr
	}
	// check dial address
	if !t.canDial(dialAddr) {
		return nil, ErrWrongTcpAddr
	}

	// try to dial
	tc, errs := t.dial(ctx, remoteAddr)
	if tc == nil {
		err := fmt.Errorf("all dial failed, errors found below:%s", util.ParseErrsToStr(errs))
		return nil, err
	}
	if remotePID != "" && tc.rPID != remotePID {
		_ = tc.Close()
		t.logger.Debugf("[Network][Dial] pid mismatch, expected: %s, got: %s, close the connection.",
			remotePID, tc.rPID)
		return nil, ErrPidMismatch
	}
	// call conn handler
	accept := t.callConnHandler(tc)
	if !accept {
		return nil, ErrConnRejectedByConnHandler
	}
	return tc, nil
}

// Close the network.
func (t *tcpNetwork) Close() error {
	close(t.closeChan)
	//stop listening
	t.mu.Lock()
	defer t.mu.Unlock()
	t.listening = false
	for _, listener := range t.tcpListeners {
		_ = listener.Close()
	}
	return nil
}

// CanListen return whether address can be listened on.
func CanListen(addr ma.Multiaddr) bool {
	return listenMatcher.Matches(addr)
}

// printListeningAddress use the log object to print the address the peer listens on
func (t *tcpNetwork) printListeningAddress(pid peer.ID, addr ma.Multiaddr) error {
	// join net multiaddr with p2p protocol
	// like "/ip4/127.0.0.1/udp/8081/quic" + "/p2p/QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4"
	// -> "/ip4/127.0.0.1/udp/8081/quic/p2p/QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4"
	mAddr := util.CreateMultiAddrWithPidAndNetAddr(pid, addr)
	t.logger.Infof("[Network] listening on address : %s", mAddr.String())
	return nil
}

// reGetListenAddresses get available addresses
func (t *tcpNetwork) reGetListenAddresses(addr ma.Multiaddr) ([]ma.Multiaddr, error) {
	// convert multi addr to net addr
	tcpAddr, err := manet.ToNetAddr(addr)
	if err != nil {
		return nil, err
	}
	// check if ip is an unspecified address
	if tcpAddr.(*net.TCPAddr).IP.IsUnspecified() {
		// if unspecified
		// whether a ipv6 address
		isIp6 := strings.Contains(tcpAddr.(*net.TCPAddr).IP.String(), ":")
		// get local addresses usable
		addrList, e := util.GetLocalAddrs()
		if e != nil {
			return nil, e
		}
		if len(addrList) == 0 {
			return nil, errors.New("no usable local address found")
		}
		// split TCP protocol , like "/tcp/8081"
		_, lastAddr := ma.SplitFunc(addr, func(component ma.Component) bool {
			return component.Protocol().Code == ma.P_TCP
		})
		res := make([]ma.Multiaddr, 0, len(addrList))
		for _, address := range addrList {
			firstAddr, e2 := manet.FromNetAddr(address)
			if e2 != nil {
				return nil, e2
			}
			// join ip protocol with TCP protocol
			// like "/ip4/127.0.0.1" + "/tcp/8081" -> "/ip4/127.0.0.1/tcp/8081"
			temp := ma.Join(firstAddr, lastAddr)
			tempTcpAddr, err := manet.ToNetAddr(temp)
			if err != nil {
				return nil, err
			}
			tempIsIp6 := strings.Contains(tempTcpAddr.(*net.TCPAddr).IP.String(), ":")
			// if both are ipv6 or ipv4, append
			// otherwise continue
			if (isIp6 && !tempIsIp6) || (!isIp6 && tempIsIp6) {
				continue
			}
			if CanListen(temp) {
				res = append(res, temp)
			}
		}
		if len(res) == 0 {
			return nil, ErrNoUsableLocalAddress
		}
		return res, nil
	}
	res, e := manet.FromNetAddr(tcpAddr)
	if e != nil {
		return nil, e
	}
	return []ma.Multiaddr{res}, nil
}

// listenTCPWithAddrList listen to the given address list
func (t *tcpNetwork) listenTCPWithAddrList(ctx context.Context, addrList []ma.Multiaddr) ([]net.Listener, error) {
	if len(addrList) == 0 {
		return nil, ErrEmptyListenAddress
	}
	res := make([]net.Listener, 0, len(addrList))
	for _, mAddr := range addrList {
		// get net address
		nAddr, _ := util.GetNetAddrAndPidFromNormalMultiAddr(mAddr)
		if nAddr == nil {
			return nil, ErrNilAddr
		}
		// parse to net.Addr
		n, err := manet.ToNetAddr(nAddr)
		if err != nil {
			return nil, err
		}
		// try to listen
		var tl net.Listener
		lc := net.ListenConfig{Control: control}
		tl, err = lc.Listen(ctx, n.Network(), n.String())
		if err != nil {
			t.logger.Warnf("[Network] listen on address failed, %s (address: %s)", err.Error(), n.String())
			continue
		}
		res = append(res, tl)
		// print listen address
		err = t.printListeningAddress(t.lPID, nAddr)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// resetCheck reset close channel and once object
func (t *tcpNetwork) resetCheck() {
	select {
	case <-t.closeChan:
		t.closeChan = make(chan struct{})
		t.once = sync.Once{}
	default:

	}
}

// callConnHandler call the connection handler to process the connection
func (t *tcpNetwork) callConnHandler(tc *conn) bool {
	var accept = true
	var err error
	if t.connHandler != nil {
		accept, err = t.connHandler(tc)
		if err != nil {
			t.logger.Errorf("[Network] call connection handler failed, %s", err.Error())
		}
	}
	if !accept {
		_ = tc.Close()
	}
	return accept
}

// listenerAcceptLoop this is an ongoing background task
// that receives new connections from remote peers and processes them.
func (t *tcpNetwork) listenerAcceptLoop(listener net.Listener) {
Loop:
	for {
		select {
		case <-t.ctx.Done():
			break Loop
		case <-t.closeChan:
			break Loop
		default:

		}
		// block waiting for a new connection to come
		c, err := listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "closed network connection") {
				break Loop
			}
			t.logger.Errorf("[Network] listener accept err: %s", err.Error())
			continue
		}
		t.logger.Debugf("[Network] listener accept connection.(remote addr:%s)", c.RemoteAddr().String())

		// handle new connection
		tc, err := newConn(t.ctx, t, c, network.Inbound, true, nil)
		if err != nil {
			t.logger.Errorf("[Network] create new connection failed, %s", err.Error())
			continue
		}
		t.logger.Debugf("[Network] create new connection success.(remote pid: %s)", tc.rPID)
		// call conn handler
		t.callConnHandler(tc)
	}
}

// Listen will run a task that start create listeners with the given addresses waiting
// for accepting inbound connections.
func (t *tcpNetwork) Listen(ctx context.Context, addrs ...ma.Multiaddr) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resetCheck()
	var err error

	// this action of address listening will only be executed once
	t.once.Do(func() {
		t.listening = true
		t.logger.Infof("[Network] local peer id : %s", t.lPID)
		if t.enableTls {
			t.logger.Info("[Network] TLS enabled.")
		} else {
			t.logger.Info("[Network] TLS disabled.")
		}

		// traverse the given address
		for i := range addrs {
			addr := addrs[i]
			// determine whether the address can be monitored
			if !CanListen(addr) {
				err = ErrWrongTcpAddr
				return
			}
			if ctx == nil {
				ctx = t.ctx
			}
			// if the address to be monitored is "0.0.0.0" or "::",
			// it needs to be processed and re-acquire the available connection address
			listenAddrList, err2 := t.reGetListenAddresses(addr)
			if err2 != nil {
				err = err2
				return
			}
			// listen to available addresses and return listener
			tcpListeners, err3 := t.listenTCPWithAddrList(ctx, listenAddrList)
			if err3 != nil {
				err = err3
				return
			}

			if len(tcpListeners) == 0 {
				err = ErrListenerRequired
				return
			}

			// traverse the listeners and start a background task for each listener to wait for a new connection
			for _, tl := range tcpListeners {
				go t.listenerAcceptLoop(tl)

				lAddr, err4 := manet.FromNetAddr(tl.Addr())
				if err4 != nil {
					err = err4
					return
				}
				t.lAddrList = append(t.lAddrList, lAddr)
				t.tcpListeners = append(t.tcpListeners, tl)
			}
		}
	})
	return err
}

// DirectListen to listen the given address, hole punch is required
func (t *tcpNetwork) DirectListen(ctx context.Context, addrs ...ma.Multiaddr) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resetCheck()
	var err error

	t.logger.Infof("[Network] local peer id : %s will listen on %v", t.lPID, addrs)
	if t.enableTls {
		t.logger.Info("[Network] TLS enabled.")
	} else {
		t.logger.Info("[Network] TLS disabled.")
	}

	if ctx == nil {
		ctx = t.ctx
	}
	// traverse the given address
	for i := range addrs {
		addr := addrs[i]
		// determine whether the address can be monitored
		if !CanListen(addr) {
			err = ErrWrongTcpAddr
			continue
		}
		if ctx == nil {
			ctx = t.ctx
		}
		// if the address to be monitored is "0.0.0.0" or "::",
		// it needs to be processed and re-acquire the available connection address
		listenAddrList, err2 := t.reGetListenAddresses(addr)
		if err2 != nil {
			return err2
		}
		// listen to available addresses and return listener
		tcpListeners, err3 := t.listenTCPWithAddrList(ctx, listenAddrList)
		if err3 != nil {
			return err3
		}

		if len(tcpListeners) == 0 {
			return ErrListenerRequired
		}
		t.logger.Infof("[Network] DirectListen local peer id : %s , %v", t.lPID, listenAddrList)

		// traverse the listeners and start a background task for each listener to wait for a new connection
		for _, tl := range tcpListeners {
			go t.listenerAcceptLoop(tl)
			lAddr, err4 := manet.FromNetAddr(tl.Addr())
			if err4 != nil {
				return err4
			}
			t.lAddrList = append(t.lAddrList, lAddr)
			t.tcpListeners = append(t.tcpListeners, tl)
		}
	}

	return err
}

// ListenAddresses return the list of the local addresses for listeners.
func (t *tcpNetwork) ListenAddresses() []ma.Multiaddr {
	t.mu.RLock()
	defer t.mu.RUnlock()
	res := make([]ma.Multiaddr, len(t.lAddrList))
	copy(res, t.lAddrList)
	return res
}

// GetTempListenAddresses get a list of temporarily listening addresses.
func (t *tcpNetwork) GetTempListenAddresses() []ma.Multiaddr {
	t.mu.RLock()
	defer t.mu.RUnlock()
	res := make([]ma.Multiaddr, len(t.tempLAddrList))
	copy(res, t.tempLAddrList)
	return res
}

// AddTempListenAddresses add a list of addresses that need to be temporarily monitored.
func (t *tcpNetwork) AddTempListenAddresses(addr []ma.Multiaddr) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.tempLAddrList = append(t.tempLAddrList, addr...)
}

// SetNewConnHandler register a ConnHandler to handle the connection established.
func (t *tcpNetwork) SetNewConnHandler(handler network.ConnHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connHandler = handler
}

// Disconnect a connection.
func (t *tcpNetwork) Disconnect(conn network.Conn) error {
	if t != conn.Network().(*tcpNetwork) {
		return ErrNotTheSameNetwork
	}
	err := conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// Closed return whether network closed.
func (t *tcpNetwork) Closed() bool {
	if t.closeChan == nil {
		return false
	}
	select {
	case <-t.closeChan:
		return true
	default:
		return false
	}
}

// LocalPeerID return the local peer id.
func (t *tcpNetwork) LocalPeerID() peer.ID {
	return t.lPID
}

// RelayDial dial to the specified address, relay needs
func (t *tcpNetwork) RelayDial(conn network.Conn, dstAddr ma.Multiaddr) (network.Conn, error) {
	// create bidirectional flow to relay node
	t.logger.Infof("[TcpNetwork][RelayDial] start to dial to relay")
	rStream, err := conn.CreateBidirectionalStream()
	if err != nil {
		t.logger.Warnf("[TcpNetwork][RelayDial] fail to create bidirectional stream to relay peer, err: [%s]",
			err.Error())
		return nil, err
	}
	var (
		maAddrs []ma.Multiaddr
	)

	msg := new(pb.RelayMsg)

	msg.Type = pb.RelayMsg_HOP
	maAddrs = make([]ma.Multiaddr, 0)

	maAddrs = append(maAddrs, t.lAddrList...)
	msg.SrcPeer = relay.PeerInfoToRelayPeer(t.lPID, maAddrs)

	maAddrs = maAddrs[0:0]
	maAddrs = append(maAddrs, dstAddr)
	_, dPID := util.GetNetAddrAndPidFromNormalMultiAddr(dstAddr)
	if dPID == "" {
		rStream.Close()
		err = errors.New("wrong dst peer address")
		return nil, err
	}
	msg.DstPeer = relay.PeerInfoToRelayPeer(dPID, maAddrs)

	// write relay message
	err = relay.WriteRelayMsg(rStream, msg)
	if err != nil {
		rStream.Close()
		return nil, fmt.Errorf("write relay msg failed: %s", err.Error())
	}

	msg.Reset()

	// wait for the message to return
	msg, err = relay.ReadRelayMsg(rStream)
	if err != nil {
		rStream.Close()
		return nil, err
	}

	if msg.GetType() != pb.RelayMsg_STATUS {
		rStream.Close()
		return nil, fmt.Errorf("unexpected relay response; not a status message [%d]", msg.GetType())
	}

	if msg.GetCode() != pb.RelayMsg_SUCCESS {
		rStream.Close()
		return nil, fmt.Errorf("unexpected relay response; code is [%d], string: [%s]",
			msg.GetCode(), msg.Code.String())
	}
	netConn := &relay.Conn{
		Stream: rStream,
	}

	// upgrade a normal connection object to an encrypted connection object
	t.logger.Infof("[TcpNetwork][RelayDial] net conn, remote addr string: [%s], remote network string: [%s]",
		netConn.RemoteAddr().String(), netConn.RemoteAddr().Network())
	t.logger.Infof("[TcpNetwork][RelayDial] net conn, local addr string: [%s], local network string: [%s]",
		netConn.LocalAddr().String(), netConn.LocalAddr().Network())
	nwConn, err := newConn(t.ctx, t, netConn, network.Outbound, false, dstAddr)
	if err != nil {
		rStream.Close()
		return nil, fmt.Errorf("new relay network conn failed: %s", err.Error())
	}

	if dPID != "" && nwConn.rPID != dPID {
		_ = nwConn.Close()
		t.logger.Debugf("[Network][Dial] pid mismatch, expected: %s, got: %s, close the connection.",
			dPID, nwConn.rPID)
		return nil, ErrPidMismatch
	}
	// call conn handler
	accept := t.callConnHandler(nwConn)
	if !accept {
		return nil, ErrConnRejectedByConnHandler
	}

	return nwConn, nil
}

// RelayListen waiting for new connections to come in, relay needs
func (t *tcpNetwork) RelayListen(listener net.Listener) {
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.closeChan:
			return
		default:

		}
		// Waiting for new connections to come in
		c, err := listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "closed network connection") {
				return
			}
			t.logger.Warnf("[Network] [relay] listener accept err: %s", err.Error())
			continue
		}
		t.logger.Infof("[Network] [Relaylisten] listener accept connection.(remote addr: [%s]), remote network string: [%s]",
			c.RemoteAddr().String(), c.RemoteAddr().Network())

		t.logger.Infof("[Network] [Relaylisten] listener accept connection.(local addr: [%s]), local network string: [%s]",
			c.LocalAddr().String(), c.LocalAddr().Network())

		// upgrade a normal connection object to an encrypted connection object
		tc, err := newConn(t.ctx, t, c, network.Inbound, false, nil)
		if err != nil {
			t.logger.Warnf("[Network] [relay] create new connection failed, %s", err.Error())
			continue
		}
		t.logger.Debugf("[Network] [relay] create new connection success.(remote pid: %s)", tc.rPID)
		// call conn handler
		t.callConnHandler(tc)
	}
}
