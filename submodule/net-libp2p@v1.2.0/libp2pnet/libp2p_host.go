/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"context"
	"sync"

	"chainmaker.org/chainmaker/net-common/cmtlssupport"
	"chainmaker.org/chainmaker/net-common/common"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

var readyC chan struct{}

type connNotifyUnit struct {
	c      network.Conn
	action bool // true: connected, false: disconnected
}

// networkNotify is an implementation of network.Notifiee.
var networkNotify = func(host *LibP2pHost) network.Notifiee {
	return &network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			select {
			case <-host.ctx.Done():
				return
			case <-readyC:

			}
			host.connHandleOnce.Do(func() {
				go host.connHandleLoop()
			})
			host.connHandleC <- &connNotifyUnit{
				c:      c,
				action: true,
			}
		},
		DisconnectedF: func(_ network.Network, c network.Conn) {
			select {
			case <-host.ctx.Done():
				return
			case <-readyC:

			}
			host.connHandleOnce.Do(func() {
				go host.connHandleLoop()
			})
			host.connHandleC <- &connNotifyUnit{
				c:      c,
				action: false,
			}
		},
	}
}

// LibP2pHost is a libP2pHost which use libp2p as local net provider.
type LibP2pHost struct {
	startUp               bool
	lock                  sync.Mutex
	ctx                   context.Context
	host                  host.Host
	peerDHT               *dht.IpfsDHT
	connManager           *PeerConnManager
	blackList             *BlackList
	memberStatusValidator *common.MemberStatusValidator
	customChainTrustRoots *cmtlssupport.ChainTrustRoots
	connSupervisor        *ConnSupervisor
	isTls                 bool
	peerChainIdsRecorder  *common.PeerIdChainIdsRecorder
	certPeerIdMapper      *common.CertIdPeerIdMapper
	peerIdTlsCertStore    *common.PeerIdTlsCertStore
	peerIdPubKeyStore     *common.PeerIdPubKeyStore
	tlsCertValidator      *cmtlssupport.CertValidator
	peersMsgChanMgr       peerMsgChanManager
	compressMsgBytes      bool
	opts                  []libp2p.Option

	connHandleOnce sync.Once
	connHandleC    chan *connNotifyUnit

	log protocol.Logger
}

func (lh *LibP2pHost) initTlsSubassemblies() {
	lh.peerChainIdsRecorder = common.NewPeerIdChainIdsRecorder(lh.log)
	lh.certPeerIdMapper = common.NewCertIdPeerIdMapper(lh.log)
	lh.peerIdTlsCertStore = common.NewPeerIdTlsCertStore(lh.log)
	lh.peerIdPubKeyStore = common.NewPeerIdPubKeyStore(lh.log)
}

func (lh *LibP2pHost) queryAndStoreDerivedInfoInCertValidator(peerIdStr string) {
	if lh.isTls {
		derivedInfo := lh.tlsCertValidator.QueryDerivedInfoWithPeerId(peerIdStr)
		if derivedInfo != nil {
			if derivedInfo.TlsCertBytes != nil {
				lh.peerIdTlsCertStore.SetPeerTlsCert(derivedInfo.PeerId, derivedInfo.TlsCertBytes)
			}
			if derivedInfo.CertId != "" {
				lh.certPeerIdMapper.Add(derivedInfo.CertId, derivedInfo.PeerId)
			}
			if derivedInfo.PubKeyBytes != nil {
				lh.peerIdPubKeyStore.SetPeerPubKey(derivedInfo.PeerId, derivedInfo.PubKeyBytes)
			}
			for i := range derivedInfo.ChainIds {
				lh.peerChainIdsRecorder.AddPeerChainId(derivedInfo.PeerId, derivedInfo.ChainIds[i])
			}
		} else {
			lh.log.Warnf("[Host] no derived info found from tls cert validator! (pid: %s)", peerIdStr)
		}
		return
	}
}

func (lh *LibP2pHost) connHandleLoop() {
	for {
		select {
		case <-lh.ctx.Done():
			return
		case u := <-lh.connHandleC:
			if u.action {
				lh.log.Infof("[Host] connecting ...")
				// connected notify
				//lh.peerStreamManager.initPeerStream(u.c.RemotePeer())
				pid := u.c.RemotePeer()
				lh.handleNewPeer(pid)
				lh.connManager.AddConn(pid, u.c)
				pidStr := pid.Pretty()
				lh.log.Infof("[Host] new connection connected(remote peer-id:%s, remote multi-addr:%s)",
					pidStr, u.c.RemoteMultiaddr().String())
				lh.queryAndStoreDerivedInfoInCertValidator(pidStr)
				continue
			}
			// disconnected notify
			// 判断连接是否有多个（单机libp2p可能建立多个地址的连接，例如127.0.0.1 192.168.XXX.XXX）
			// 如果连接有一个以上，不能删除节点的上层状态

			lh.log.Infof("[Host] disconnecting ...")
			conn := lh.connManager.GetConns(u.c.RemotePeer())
			if len(conn) > 1 {
				// 不止一个连接
				pid := u.c.RemotePeer().Pretty()
				lh.log.Warnf("[Host] start handle more than one conn(remote peer-id:%s, remote multi-addr:%s)",
					pid, u.c.RemoteMultiaddr().String())
				lh.connManager.RemoveConn(u.c.RemotePeer(), u.c)
				lh.log.Warnf("[Host] RemoveConn done(remote peer-id:%s)", pid)
				lh.closePeer(u.c.RemotePeer().Pretty())
				lh.log.Warnf("[Host] closePeer done(remote peer-id:%s)", pid)
				err := lh.handleNewPeer(u.c.RemotePeer())
				lh.log.Warnf("[Host] handleNewPeer done,(remote peer-id:%s, remote multi-addr:%s) result:%v",
					pid, u.c.RemoteMultiaddr().String(), err)
			} else {
				if len(conn) == 1 && conn[0].RemoteMultiaddr().String() != u.c.RemoteMultiaddr().String() {
					lh.log.Infof("[Host] connection disconnected failed, (remote peer-id:%s, remote multi-addr:%s, connection multi-addr:%s)",
						u.c.RemotePeer().Pretty(), u.c.RemoteMultiaddr().String(), conn[0].RemoteMultiaddr().String())
					return
				}
				lh.log.Infof("[Host] connection disconnected, remove peer from host(remote peer-id:[%s], remote multi-addr:[%s]).",
					u.c.RemotePeer().Pretty(), u.c.RemoteMultiaddr().String())
				pid := u.c.RemotePeer().Pretty()
				lh.connManager.RemoveConn(u.c.RemotePeer(), u.c)
				lh.log.Infof("[Host] remove connection done (remote peer-id:%s)", pid)
				lh.peerChainIdsRecorder.RemoveAllByPeerId(pid)
				lh.log.Infof("[Host] remove peer from peer chain id map done (remote peer-id:%s)", pid)
				lh.peerIdTlsCertStore.RemoveByPeerId(pid)
				lh.log.Infof("[Host] remove peer from peer tls cert map done (remote peer-id:%s)", pid)
				lh.peerIdPubKeyStore.RemoveByPeerId(pid)
				lh.log.Infof("[Host] remove peer from peer pubkey map done (remote peer-id:%s)", pid)

				if info := lh.tlsCertValidator.QueryDerivedInfoWithPeerId(pid); info == nil {
					lh.certPeerIdMapper.RemoveByPeerId(pid)
					lh.log.Infof("[Host] remove peer from peer cert id map done (remote peer-id:%s)", pid)
				} else {
					lh.log.Infof("[Host] the identity of the peer is not removed, no need to remove the cert id map.(remote peer-id:%s)",
						pid)
				}

				lh.closePeer(pid)
				lh.log.Infof("[Host] remove peer from peer stream manager map done (remote peer-id:%s)", pid)
			}
		}
	}
}

// Context
func (lh *LibP2pHost) Context() context.Context {
	return lh.ctx
}

// Host is libp2p.Host.
func (lh *LibP2pHost) Host() host.Host {
	return lh.host
}

// DHT is libp2p.peerDHT
func (lh *LibP2pHost) DHT() *dht.IpfsDHT {
	return lh.peerDHT
}

// HasConnected return true if the peer which id is the peerId given has connected. Otherwise return false.
func (lh *LibP2pHost) HasConnected(peerId peer.ID) bool {
	return lh.connManager.IsConnected(peerId)
}

// IsRunning return true when libp2p has started up.Otherwise return false.
func (lh *LibP2pHost) IsRunning() bool {
	return lh.startUp
}

// NewLibP2pHost create new LibP2pHost instance.
func NewLibP2pHost(ctx context.Context, log protocol.Logger) *LibP2pHost {
	return &LibP2pHost{
		startUp:               false,
		ctx:                   ctx,
		connManager:           NewPeerConnManager(log),
		blackList:             NewBlackList(),
		memberStatusValidator: common.NewMemberStatusValidator(),
		customChainTrustRoots: cmtlssupport.NewChainTrustRoots(),
		opts:                  make([]libp2p.Option, 0),
		connHandleOnce:        sync.Once{},
		connHandleC:           make(chan *connNotifyUnit, 10),
		log:                   log,
		peersMsgChanMgr:       newPeerStreamManager(),
	}
}

// Start libP2pHost.
func (lh *LibP2pHost) Start() error {
	lh.lock.Lock()
	defer lh.lock.Unlock()
	if lh.startUp {
		lh.log.Warn("[Host] host is running. ignored.")
		return nil
	}
	lh.log.Info("[Host] stating host...")
	node, err := libp2p.New(lh.ctx, lh.opts...)
	if err != nil {
		return err
	}
	lh.host = node
	// network notify
	node.Network().Notify(networkNotify(lh))
	lh.log.Info("[Host] host stated.")
	for _, addr := range node.Addrs() {
		lh.log.Infof("[Host] host listening on address:%s/p2p/%s", addr.String(), node.ID().Pretty())
	}
	lh.startUp = true
	return nil
}

// Stop libP2pHost.
func (lh *LibP2pHost) Stop() error {
	lh.connSupervisor.stopSupervising()
	return lh.host.Close()
}

func (lh *LibP2pHost) handleNewPeer(pid peer.ID) error {
	//lh.peersMsgChanLock.Lock()
	//defer lh.peersMsgChanLock.Unlock()
	_, ok := lh.peersMsgChanMgr.GetStream(pid.Pretty())
	if ok {
		return nil
	}

	s, err := lh.host.NewStream(lh.ctx, pid, MsgPID)
	if err != nil {
		lh.log.Warnf("[Host] new the peer stream failed, err: [%s], peer: [%s]",
			err.Error(), pid.Pretty())
		return err
	}
	psh := NewPeerSendMsgHandler(lh.ctx, pid, s, lh.log)
	lh.peersMsgChanMgr.AddStream(pid.Pretty(), psh)
	go psh.handleSendingMessages()
	go psh.handlePeerEOF()
	return nil
}

func (lh *LibP2pHost) closePeer(pid string) {
	//lh.peersMsgChanLock.Lock()
	//defer lh.peersMsgChanLock.Unlock()

	//psh, ok := lh.peersMsgChanMgr[pid]
	psh, ok := lh.peersMsgChanMgr.GetStream(pid)
	if !ok {
		return
	}
	psh.close()
	//delete(lh.peersMsgChanMgr, pid)
	lh.peersMsgChanMgr.DeleteStream(pid)

	lh.log.Infof("[Host] close the peer send msg handler, peer: [%s]", pid)
}
