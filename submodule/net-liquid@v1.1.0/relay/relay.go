/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"chainmaker.org/chainmaker/net-common/utils"
	"chainmaker.org/chainmaker/net-liquid/core/host"
	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/protocol"
	"chainmaker.org/chainmaker/net-liquid/core/util"
	"chainmaker.org/chainmaker/net-liquid/relay/pb"
	api "chainmaker.org/chainmaker/protocol/v2"
	"github.com/gogo/protobuf/proto"
	ma "github.com/multiformats/go-multiaddr"
)

// Relay_Protocol_ID the relay protocolID
const Relay_Protocol_ID = "liquid-relay"

var (
	// RelayAcceptTimeout .
	RelayAcceptTimeout = 10 * time.Second

	// HopConnectTimeout the relay hop connect timeout
	HopConnectTimeout = 30 * time.Second

	// StopHandshakeTimeout the relay stop handshake timeout
	StopHandshakeTimeout = 1 * time.Minute

	// HopStreamLimit .
	HopStreamLimit = 1 << 19 // 512K hops for 1M goroutines
)

// Relay the relay struct
type Relay struct {

	// the context of the goroutine
	ctx context.Context

	// the peerId of the relay node
	self peer.ID

	// whether relay is supported
	hop bool

	// the chan of relay conn
	incoming chan *Conn

	// the relay stream count
	streamCount int32

	// the relay count
	liveHopCount int32

	// logger
	logger api.Logger
}

// RelayOpt .
// OptActive/OptHop
type RelayOpt int

var (
	// OptActive (reserved)
	OptActive = RelayOpt(0)

	// OptHop (default)
	OptHop = RelayOpt(1)
)

// NewRelay return the relay struct
func NewRelay(ctx context.Context, peerId peer.ID, logger api.Logger, opts ...RelayOpt) (*Relay, error) {
	if len(peerId) == 0 {
		return nil, fmt.Errorf("peer Id can't be empty")
	}

	if len(opts) == 0 {
		opts = append(opts, OptHop)
	}

	r := &Relay{
		ctx:      ctx,
		self:     peerId,
		incoming: make(chan *Conn),
		logger:   logger,
	}

	// relay opt
	// only HOP is supported.
	// other type is reserved
	for _, opt := range opts {
		switch opt {
		case OptHop:
			r.hop = true
		default:
			return nil, fmt.Errorf("unrecognized option: %d", opt)
		}
	}

	return r, nil
}

// Accept deal with the received relay messages
func (r *Relay) Accept() (net.Conn, error) {
	for {
		select {
		case c := <-r.incoming:
			// response success for the relay stream
			err := r.writeResponse(c.Stream, pb.RelayMsg_SUCCESS)
			if err != nil {
				r.logger.Debugf("error writing relay response: [%s]", err.Error())
				c.Stream.Close()
				continue
			}
			r.logger.Infof("accepted relay connection: [%v]", c)
			return c, nil

		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	}
}

// Addr .
func (r *Relay) Addr() net.Addr {
	return &NetAddr{
		Addr: "any",
	}
}

// Close .
func (r *Relay) Close() error {
	close(r.incoming)
	return nil
}

func (r *Relay) writeResponse(s network.Stream, code pb.RelayMsg_Status) error {

	var msg pb.RelayMsg
	msg.Type = pb.RelayMsg_STATUS
	msg.Code = code

	return WriteRelayMsg(s, &msg)
}

// handleError the error response
func (r *Relay) handleError(s network.Stream, code pb.RelayMsg_Status) {
	r.logger.Warnf("relay error: %s (%d)", code.String(), code)
	err := r.writeResponse(s, code)
	if err != nil {
		s.Close()
		r.logger.Debugf("error writing relay response: %s", err.Error())
	}
}

// WriteRelayMsg 向stream写入pb.RelayMsg格式的信息
func WriteRelayMsg(s network.Stream, msg *pb.RelayMsg) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// create net message package
	pkg := protocol.NewPackage(Relay_Protocol_ID, msgBytes)
	pkgData, err := pkg.ToBytes(false)
	if err != nil {
		return err
	}
	// write data length to stream
	pkgDataLen := len(pkgData)
	pkgDataLenBytes := utils.Uint64ToBytes(uint64(pkgDataLen))

	n, err := s.Write(pkgDataLenBytes)
	if err == nil {
		// write package bytes
		var n1 int
		n1, err = s.Write(pkgData)
		n += n1
	}
	if err != nil {
		return err
	}

	// whether write data completely
	if n < pkgDataLen+8 {
		return errors.New("send msg incompletely")
	}

	return nil
}

// ReadRelayMsg 在stream读出pb.RelayMsg格式的信息
func ReadRelayMsg(stream network.Stream) (*pb.RelayMsg, error) {
	var relayMsg pb.RelayMsg

	if stream.Conn().IsClosed() {
		err := fmt.Errorf("read relay msg failed, err: conn is closed")
		return nil, err
	}

	// read the package length
	dataLength, _, err := util.ReadPackageLength(stream)
	if err != nil {
		err = fmt.Errorf("read relay msg failed, err: %s", err.Error())
		return nil, err
	}

	// read the package data
	dataBytes, err := util.ReadPackageData(stream, dataLength)
	if err != nil {
		err = fmt.Errorf("read relay msg failed, err: %s", err.Error())
		return nil, err
	}

	pkg := &protocol.Package{}
	err = pkg.FromBytes(dataBytes)
	if err != nil {
		err = fmt.Errorf("read relay msg failed, err: %s", err.Error())
		return nil, err
	}

	// Check whether it is the relay protocol ID
	if pkg.ProtocolID() != Relay_Protocol_ID {
		err = fmt.Errorf("read relay msg failed, err: the protocol id is not matched")
		return nil, err
	}

	// unmarshal the packdata
	msgBytes := pkg.Payload()
	err = proto.Unmarshal(msgBytes, &relayMsg)
	if err != nil {
		err = fmt.Errorf("read relay msg failed, err: %s", err.Error())
		return nil, err
	}
	return &relayMsg, nil
}

// IsRelayAddr is the relay's muliaddr addr
func IsRelayAddr(addr ma.Multiaddr) bool {
	protocols := addr.Protocols()

	for _, p := range protocols {
		if p.Code == ma.P_CIRCUIT {
			return true
		}
	}
	return false
}

// GetRelayAddrAndDstPeerAddr cut to obtain the address of the relay source node and destination node
func GetRelayAddrAndDstPeerAddr(addr ma.Multiaddr) (relayAddr,
	dstAddr ma.Multiaddr, err error) {

	// split the "p2p-circuit" from the addr
	// return the relay addr and the destination node addr
	relayAddr, dstAddr = ma.SplitFunc(addr, func(c ma.Component) bool {
		return c.Protocol().Code == ma.P_CIRCUIT
	})

	if dstAddr == nil {
		return nil, nil,
			fmt.Errorf("%s is not a relay address", addr)
	}

	if relayAddr == nil {
		return nil, nil,
			fmt.Errorf("can't dial a p2p-circuit without specifying a relay: %s", addr)
	}

	_, dstAddr = ma.SplitFirst(dstAddr)

	return
}

// PeerInfoToRelayPeer return the pb.RelayMsg_Peer
func PeerInfoToRelayPeer(peerId peer.ID, peerAddr []ma.Multiaddr) *pb.RelayMsg_Peer {
	addrs := make([][]byte, len(peerAddr))
	for i, addr := range peerAddr {
		addrs[i] = addr.Bytes()
	}

	p := new(pb.RelayMsg_Peer)
	p.Id = []byte(peerId)
	p.Addrs = addrs

	return p
}

// RelayPeerToPeerInfo the pb.RelayMsg_Peer to peerId and addr
func RelayPeerToPeerInfo(p *pb.RelayMsg_Peer) (peer.ID, []ma.Multiaddr, error) {
	if p == nil {
		return "", nil, errors.New("nil peer")
	}

	id := peer.ID(p.Id)

	addrs := make([]ma.Multiaddr, 0, len(p.Addrs))

	for _, addrBytes := range p.Addrs {
		a, err := ma.NewMultiaddrBytes(addrBytes)
		if err == nil {
			addrs = append(addrs, a)
		}
	}

	return id, addrs, nil
}

// HandleHopStream *
// 1) dial the target node and send a STOP message
// 2) wait for return and return a success message to the source node
// 3) copy the data from streams on both sides
func (r *Relay) HandleHopStream(stream network.Stream, h host.Host, relayMsg *pb.RelayMsg) {
	if !r.hop {
		r.handleError(stream, pb.RelayMsg_HOP_CANT_SPEAK_RELAY)
		return
	}

	streamCount := atomic.AddInt32(&r.streamCount, 1)
	liveHopCount := atomic.LoadInt32(&r.liveHopCount)
	defer atomic.AddInt32(&r.streamCount, -1)

	if (streamCount + liveHopCount) > int32(HopStreamLimit) {
		r.logger.Warn("hop stream limit exceeded; resetting stream")
		stream.Close()
		return
	}

	// get the srouce node info from pb relay msg
	srcId, _, err := RelayPeerToPeerInfo(relayMsg.SrcPeer)
	if err != nil {
		r.handleError(stream, pb.RelayMsg_HOP_SRC_MULTIADDR_INVALID)
		return
	}

	// Check the source node ID is equal to the remote peer ID
	if srcId != stream.Conn().RemotePeerID() {
		r.handleError(stream, pb.RelayMsg_HOP_SRC_MULTIADDR_INVALID)
		return
	}

	// Get the ID of the destination node
	dstId, _, err := RelayPeerToPeerInfo(relayMsg.DstPeer)
	if err != nil {
		r.handleError(stream, pb.RelayMsg_HOP_DST_MULTIADDR_INVALID)
		return
	}

	// The ID of the destination node cannot be the ID of the relay node
	if dstId == r.self {
		r.handleError(stream, pb.RelayMsg_HOP_CANT_RELAY_TO_SELF)
		return
	}

	//open dstPeer stream
	ctx, cancel := context.WithTimeout(r.ctx, HopConnectTimeout)
	defer cancel()

	var dstConn network.Conn
	if !h.ConnMgr().IsConnected(dstId) {
		// get addrs from the peer store (if the node has been connected before)
		addrs := h.PeerStore().GetAddrs(dstId)
		if len(addrs) == 0 {
			r.handleError(stream, pb.RelayMsg_HOP_CANT_DIAL_DST)
			return
		}

		for _, addr := range addrs {
			// traverse all address to dial
			dstConn, err = h.Network().Dial(ctx, addr)
			if err != nil {
				r.logger.Warnf("[Relay][Hop] connect to dst peer failed, %s (remote pid: %s, addr: %s)",
					err.Error(), dstId, addr.String())
				continue
			}
			break
		}
		if dstConn == nil {
			r.handleError(stream, pb.RelayMsg_HOP_NO_CONN_TO_DST)
			return
		}
	} else {
		// usually, the connection has already been established.
		dstConn = h.ConnMgr().GetPeerConn(dstId)
	}

	// Sends a message to the destination node
	dstStream, err := r.hopSendMsg(dstConn, relayMsg)
	if err != nil {
		r.handleError(stream, pb.RelayMsg_HOP_CANT_OPEN_DST_STREAM)
		r.logger.Warnf("[Relay][Hop] send msg to dst peer failed, %s (remote pid: %s)",
			err.Error(), dstId)
		return
	}

	// Returns success to the source node
	err = r.writeResponse(stream, pb.RelayMsg_SUCCESS)
	if err != nil {
		stream.Close()
		dstStream.Close()
		return
	}

	// relay connection
	r.logger.Infof("relaying connection between %s and %s", srcId, dstId)

	// reset deadline
	err = dstConn.SetDeadline(time.Time{})
	if err != nil {
		r.logger.Warnf("[Relay][Hop] dst conn set the deadline failed, err: %s", err.Error())
	}

	atomic.AddInt32(&r.liveHopCount, 1)

	// Initialize two goroutines
	goroutines := new(int32)
	*goroutines = 2
	done := func() {
		if atomic.AddInt32(goroutines, -1) == 0 {
			atomic.AddInt32(&r.liveHopCount, -1)
		}
	}

	// Data forwarding by Cory
	go func() {
		defer done()
		var buf []byte
		count, err := io.CopyBuffer(stream, dstStream, buf)
		if err != nil {
			r.logger.Debugf("relay copy error: %s", err)
			stream.Close()
			dstStream.Close()
		}

		r.logger.Debugf("relayed %d bytes from %s to %s", count, srcId, dstId)
	}()

	// Data forwarding by Cory
	go func() {
		defer done()
		var buf []byte
		count, err := io.CopyBuffer(dstStream, stream, buf)
		if err != nil {
			r.logger.Debugf("relay copy error: %s", err)
			stream.Close()
			dstStream.Close()
		}
		r.logger.Debugf("relayed %d bytes from %s to %s", count, srcId, dstId)
	}()
}

func (r *Relay) hopSendMsg(conn network.Conn, msg *pb.RelayMsg) (network.Stream, error) {

	// Bidirectional flows are configured for the DST Peer
	dstStream, err := conn.CreateBidirectionalStream()
	if err != nil {
		return nil, err
	}

	// set handshake deadline
	err = conn.SetDeadline(time.Now().Add(StopHandshakeTimeout))
	if err != nil {
		r.logger.Warnf("[Relay][Hop] dst conn set the deadline failed, err: %s", err.Error())
	}

	// Write msg type = STOP

	msg.Type = pb.RelayMsg_STOP

	err = WriteRelayMsg(dstStream, msg)
	if err != nil {
		dstStream.Close()
		return nil, err
	}

	msg.Reset()

	// Waiting for the return
	msg, err = ReadRelayMsg(dstStream)
	if err != nil {
		dstStream.Close()
		return nil, err
	}

	// check the msg type
	if msg.GetType() != pb.RelayMsg_STATUS {
		dstStream.Close()
		return nil, fmt.Errorf("unexpected relay stop response: not a status message (%d), [%s]",
			msg.GetType(), msg.Type.String())
	}

	// check the msg code
	if msg.GetCode() != pb.RelayMsg_SUCCESS {
		dstStream.Close()
		return nil, fmt.Errorf("unexpected relay response; code is [%d], string: [%s]",
			msg.GetCode(), msg.Code.String())
	}

	return dstStream, nil
}

// HandleStopStream wrap the stream created with the trunk as really. Conn and throw it to the upper layer
func (r *Relay) HandleStopStream(stream network.Stream, h host.Host, relayMsg *pb.RelayMsg) {
	// Convert source node information
	srcId, _, err := RelayPeerToPeerInfo(relayMsg.SrcPeer)
	if err != nil {
		r.handleError(stream, pb.RelayMsg_HOP_SRC_MULTIADDR_INVALID)
		return
	}
	// Convert destination node information
	dstId, _, err := RelayPeerToPeerInfo(relayMsg.DstPeer)
	if err != nil && dstId != r.self {
		r.handleError(stream, pb.RelayMsg_HOP_DST_MULTIADDR_INVALID)
		return
	}

	r.logger.Infof("relay connection from: %s", srcId)

	select {
	// Wrap stream as relayConn
	case r.incoming <- &Conn{Stream: stream}:
	// Timeout detection
	case <-time.After(RelayAcceptTimeout):
		r.handleError(stream, pb.RelayMsg_STOP_RELAY_REFUSED)
	}
}
