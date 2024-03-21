/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"sync"
	"time"

	"chainmaker.org/chainmaker/net-common/utils"
	"chainmaker.org/chainmaker/protocol/v2"

	"github.com/libp2p/go-libp2p-core/peer"
)

const DefaultTryTimes = 50

// ConnSupervisor is a connections supervisor.
type ConnSupervisor struct {
	host              *LibP2pHost
	peerAddrInfos     []peer.AddrInfo
	peerAddrInfosLock sync.RWMutex
	signal            bool
	signalLock        sync.RWMutex
	startUp           bool
	tryConnectLock    sync.Mutex
	allConnected      bool

	tryTimes  int
	actuators map[peer.ID]*tryToDialActuator

	log protocol.Logger
}

func (cs *ConnSupervisor) getSignal() bool {
	cs.signalLock.RLock()
	defer cs.signalLock.RUnlock()
	return cs.signal
}

func (cs *ConnSupervisor) setSignal(signal bool) {
	cs.signalLock.Lock()
	defer cs.signalLock.Unlock()
	cs.signal = signal
}

// newConnSupervisor create a new ConnSupervisor.
func newConnSupervisor(host *LibP2pHost, peerAddrInfos []peer.AddrInfo, log protocol.Logger) *ConnSupervisor {
	return &ConnSupervisor{
		host:          host,
		peerAddrInfos: peerAddrInfos,
		startUp:       false,
		allConnected:  false,
		tryTimes:      DefaultTryTimes,
		actuators:     make(map[peer.ID]*tryToDialActuator),
		log:           log,
	}
}

// getPeerAddrInfos get the addr infos of the peers for supervising.
func (cs *ConnSupervisor) getPeerAddrInfos() []peer.AddrInfo {
	cs.peerAddrInfosLock.RLock()
	defer cs.peerAddrInfosLock.RUnlock()
	return cs.peerAddrInfos
}

// refreshPeerAddrInfos refresh the addr infos of the peers for supervising.
func (cs *ConnSupervisor) refreshPeerAddrInfos(peerAddrInfos []peer.AddrInfo) {
	cs.peerAddrInfosLock.Lock()
	defer cs.peerAddrInfosLock.Unlock()
	cs.peerAddrInfos = peerAddrInfos
}

// startSupervising start a goroutine to supervise connections.
func (cs *ConnSupervisor) startSupervising(readySignal chan struct{}) {
	if cs.startUp {
		return
	}
	cs.setSignal(true)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				cs.log.Error(err)
			}
		}()
		cs.startUp = true
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-readySignal:
		case <-timer.C:
		}
		for cs.getSignal() {
			//if cs.host.connManager.ConnCount() < len(cs.getPeerAddrInfos()) {
			cs.try()
			//}
			time.Sleep(5 * time.Second)
		}
		cs.startUp = false
	}()
}

func (cs *ConnSupervisor) try() {
	if len(cs.peerAddrInfos) > 0 {
		cs.tryConnectLock.Lock()
		defer cs.tryConnectLock.Unlock()
		peerAddrInfos := cs.getPeerAddrInfos()
		count := len(peerAddrInfos)
		connectedCount := 0
		for _, peerInfo := range cs.getPeerAddrInfos() {
			if cs.host.host.ID() == peerInfo.ID || cs.host.HasConnected(peerInfo.ID) {
				connectedCount++
				if connectedCount == count && !cs.allConnected {
					cs.log.Infof("[ConnSupervisor] all necessary peers connected.")
					cs.allConnected = true
				}
				_, ok := cs.actuators[peerInfo.ID]
				if ok {
					delete(cs.actuators, peerInfo.ID)
				}
				continue
			}
			cs.allConnected = false
			ac, ok := cs.actuators[peerInfo.ID]
			if !ok || ac.finish {
				cs.actuators[peerInfo.ID] = newTryToDialActuator(peerInfo, cs, cs.tryTimes)
				ac = cs.actuators[peerInfo.ID]
			}
			go ac.run()
		}

	}
}

type tryToDialActuator struct {
	peerInfo  peer.AddrInfo
	fibonacci []int64
	idx       int
	giveUp    bool
	finish    bool
	statC     chan struct{}

	cs *ConnSupervisor
}

func newTryToDialActuator(peerInfo peer.AddrInfo, cs *ConnSupervisor, tryTimes int) *tryToDialActuator {
	return &tryToDialActuator{
		peerInfo:  peerInfo,
		fibonacci: utils.FibonacciArray(tryTimes),
		idx:       0,
		giveUp:    false,
		finish:    false,
		statC:     make(chan struct{}, 1),
		cs:        cs,
	}
}

func (a *tryToDialActuator) run() {
	select {
	case a.statC <- struct{}{}:
		defer func() {
			<-a.statC
		}()
	default:
		return
	}
	if a.giveUp || a.finish {
		return
	}
	for {
		if !a.cs.startUp {
			break
		}
		if a.cs.host.HasConnected(a.peerInfo.ID) {
			a.finish = true
			break
		}
		a.cs.log.Debugf("[ConnSupervisor] try to connect(peer:%s)", a.peerInfo)
		var err error
		if err = a.cs.host.Host().Connect(a.cs.host.Context(), a.peerInfo); err == nil {
			a.finish = true
			break
		}
		a.cs.log.Infof("[ConnSupervisor] try to connect to peer failed(peer: %s, times: %d),%s",
			a.peerInfo, a.idx+1, err.Error())
		a.idx = a.idx + 1
		if a.idx >= len(a.fibonacci) {
			a.cs.log.Warnf("[ConnSupervisor] can not connect to peer, give it up. (peer:%s)", a.peerInfo)
			a.giveUp = true
			break
		}
		timeout := time.Duration(a.fibonacci[a.idx]) * time.Second
		time.Sleep(timeout)
	}
}

// stopSupervising stop supervising.
func (cs *ConnSupervisor) stopSupervising() {
	cs.setSignal(false)
}

// nolint
// handleChanNewPeerFound handle the new peer found which got from discovery.
func (cs *ConnSupervisor) handleChanNewPeerFound(peerChan <-chan peer.AddrInfo) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				cs.log.Errorf("[ConnSupervisor.handleChanNewPeerFound] recover err, %s", err)
			}
		}()
		for p := range peerChan {
			cs.tryConnectLock.Lock()
			if p.ID == cs.host.Host().ID() || cs.host.HasConnected(p.ID) {
				cs.tryConnectLock.Unlock()
				continue
			}
			err := cs.host.Host().Connect(cs.host.Context(), p)
			if err != nil {
				cs.log.Warnf("[ConnSupervisor] new connection connect failed"+
					"(remote peer id:%s, remote addr:%s),%s", p.ID.Pretty(), p.Addrs[0].String(), err.Error())
			} else {
				cs.log.Debug("[ConnSupervisor] new connection connected(remote peer id:%s, remote addr:%s)",
					p.ID.Pretty(), p.Addrs[0].String())
			}
			cs.tryConnectLock.Unlock()
		}
	}()
}
