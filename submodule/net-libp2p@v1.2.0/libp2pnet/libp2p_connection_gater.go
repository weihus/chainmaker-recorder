/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"strconv"
	"strings"

	"chainmaker.org/chainmaker/net-common/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/libp2p/go-libp2p-core/control"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ConnGater is an implementation of ConnectionGater interface.
type ConnGater struct {
	connManager           *PeerConnManager
	blackList             *BlackList
	memberStatusValidator *common.MemberStatusValidator

	log protocol.Logger
}

func NewConnGater(
	connManager *PeerConnManager,
	blackList *BlackList,
	memberStatusValidator *common.MemberStatusValidator,
	log protocol.Logger) *ConnGater {
	return &ConnGater{connManager: connManager, blackList: blackList, memberStatusValidator: memberStatusValidator, log: log}
}

// InterceptPeerDial
func (cg *ConnGater) InterceptPeerDial(p peer.ID) bool {
	return true
}

// InterceptAddrDial
func (cg *ConnGater) InterceptAddrDial(p peer.ID, mu multiaddr.Multiaddr) bool {
	return true
}

// InterceptAccept will be checked first when other peer connect to us.
func (cg *ConnGater) InterceptAccept(cm network.ConnMultiaddrs) bool {
	return true
}

// InterceptSecured
func (cg *ConnGater) InterceptSecured(d network.Direction, p peer.ID, cm network.ConnMultiaddrs) bool {
	remoteAddr := cm.RemoteMultiaddr().String()
	s := strings.Split(remoteAddr, "/")
	ip := s[2]
	port, _ := strconv.Atoi(s[4])

	if !cg.connManager.CanConnect(p) {
		cg.log.Infof("[ConnGater.InterceptSecured] connection not allowed. ignored. (peer-id:%s)", p.Pretty())
		return false
	}

	if cg.blackList.ContainsIPAndPort(ip, port) {
		cg.log.Infof("[ConnGater.InterceptSecured] connection remote address in blacklist. rejected. "+
			"(remote addr:%s)", remoteAddr)
		return false
	}
	if cg.blackList.ContainsPeerId(p) {
		cg.log.Infof("[ConnGater.InterceptSecured] peer in blacklist. rejected. (peer-id:%s)", p.Pretty())
		return false
	}
	if cg.memberStatusValidator.ContainsPeerId(p.Pretty()) {
		cg.log.Infof("[ConnGater.InterceptSecured] peer id in revoked list. rejected. (peer-id:%s)", p.Pretty())
		return false
	}
	if d == network.DirInbound {
		connState := cg.connManager.IsConnected(p)
		if connState {
			cg.log.Infof("[ConnGater.InterceptSecured] peer has connected. ignored. (peer-id:%s)", p.Pretty())
			return false
		}
	}
	return true
}

// InterceptUpgraded
func (cg *ConnGater) InterceptUpgraded(c network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}
