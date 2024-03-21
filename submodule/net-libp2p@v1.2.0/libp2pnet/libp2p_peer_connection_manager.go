/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

// eliminationStrategy is strategy for eliminating connected peer
type eliminationStrategy int

const (
	// Random
	Random eliminationStrategy = iota + 1
	// FIFO FIRST_IN_FIRST_OUT
	FIFO
	// LIFO LAST_IN_FIRST_OUT
	LIFO
)

var eliminatedHighLevelConnBugError = errors.New("no high level connection will be eliminated bug. pls check why")

// DefaultMaxPeerCountAllow is the default max peer count allow.
const DefaultMaxPeerCountAllow = 100

// DefaultEliminationStrategy is the default strategy for elimination.
const DefaultEliminationStrategy = LIFO

// the interval of time allowed to connect，unit seconds
const allowedConnTimeIntervalLower = 3
const allowedConnTimeIntervalUpper = 10

// connRecorder is a connection recorder.
type peerConnections struct {
	pid  peer.ID
	conn map[network.Conn]struct{}
}

// PeerConnManager is a connection manager of peers.
type PeerConnManager struct {
	cmLock             sync.RWMutex
	maxSize            int
	strategy           eliminationStrategy
	highLevelPeersLock sync.RWMutex
	highLevelPeers     map[peer.ID]struct{}
	highLevelConn      []*peerConnections
	lowLevelConn       []*peerConnections
	connLatestTime     map[string]int64 // records the timestamp of the last connection of the peer
	log                protocol.Logger
}

// SetStrategy set the elimination strategy. If not set, default is LIFO.
func (cm *PeerConnManager) SetStrategy(strategy int) {
	if strategy <= 0 {
		cm.log.Warnf("[PeerConnManager] wrong strategy set(strategy:%d). use default(default:%v)",
			strategy, DefaultEliminationStrategy)
		cm.strategy = DefaultEliminationStrategy
		return
	}
	cm.strategy = eliminationStrategy(strategy)
}

// SetMaxSize set max count of peers allowed. If not set, default is 20.
func (cm *PeerConnManager) SetMaxSize(maxSize int) {
	if maxSize < 1 {
		cm.log.Warnf("[PeerConnManager] wrong max size set(max size:%d). use default(default:%d)",
			maxSize, DefaultMaxPeerCountAllow)
		maxSize = DefaultMaxPeerCountAllow
	}
	cm.maxSize = maxSize
}

// NewPeerConnManager create a new PeerConnManager.
func NewPeerConnManager(log protocol.Logger) *PeerConnManager {
	return &PeerConnManager{
		maxSize:        DefaultMaxPeerCountAllow,
		strategy:       DefaultEliminationStrategy,
		highLevelPeers: make(map[peer.ID]struct{}),
		highLevelConn:  make([]*peerConnections, 0),
		lowLevelConn:   make([]*peerConnections, 0),
		connLatestTime: make(map[string]int64),
		log:            log,
	}
}

// IsHighLevel return true if the peer which is high-level (consensus & seeds) node. Otherwise, return false.
func (cm *PeerConnManager) IsHighLevel(peerId peer.ID) bool {
	cm.highLevelPeersLock.RLock()
	defer cm.highLevelPeersLock.RUnlock()
	_, ok := cm.highLevelPeers[peerId]
	return ok
}

// AddAsHighLevelPeer add a peer id as high level peer.
func (cm *PeerConnManager) AddAsHighLevelPeer(peerId peer.ID) {
	cm.highLevelPeersLock.Lock()
	defer cm.highLevelPeersLock.Unlock()
	cm.highLevelPeers[peerId] = struct{}{}
}

// RemoveHighLevelPeer remove a high level peer id.
func (cm *PeerConnManager) RemoveHighLevelPeer(peerId peer.ID) {
	cm.highLevelPeersLock.Lock()
	defer cm.highLevelPeersLock.Unlock()
	delete(cm.highLevelPeers, peerId)
}

// ClearHighLevelPeer clear all high level peer id records.
func (cm *PeerConnManager) ClearHighLevelPeer() {
	cm.highLevelPeersLock.Lock()
	defer cm.highLevelPeersLock.Unlock()
	cm.highLevelPeers = make(map[peer.ID]struct{})
}

func (cm *PeerConnManager) getHighLevelConnections(pid peer.ID) (map[network.Conn]struct{}, int) {
	for idx, connections := range cm.highLevelConn {
		if pid == connections.pid {
			return connections.conn, idx
		}
	}
	return nil, -1
}

func (cm *PeerConnManager) getLowLevelConnections(pid peer.ID) (map[network.Conn]struct{}, int) {
	for idx, connections := range cm.lowLevelConn {
		if pid == connections.pid {
			return connections.conn, idx
		}
	}
	return nil, -1
}

func (cm *PeerConnManager) eliminateConnections(isHighLevel bool) (peer.ID, error) {
	switch cm.strategy {
	case Random:
		return cm.eliminateConnectionsRandom(isHighLevel)
	case FIFO:
		return cm.eliminateConnectionsFIFO(isHighLevel)
	case LIFO:
		return cm.eliminateConnectionsLIFO(isHighLevel)
	default:
		cm.log.Warnf("[PeerConnManager] unknown elimination strategy[%v], use default[%v]",
			cm.strategy, DefaultEliminationStrategy)
		cm.strategy = DefaultEliminationStrategy
		return cm.eliminateConnections(isHighLevel)
	}
}

func (cm *PeerConnManager) closeLowLevelConnRandom(lowLevelConnCount int) (peer.ID, error) {
	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(lowLevelConnCount)
	eliminatedPid := cm.lowLevelConn[random].pid
	for conn := range cm.lowLevelConn[random].conn {
		go func(connToClose network.Conn) {
			_ = connToClose.Close()
		}(conn)
	}
	if random == lowLevelConnCount-1 {
		cm.lowLevelConn = cm.lowLevelConn[:random]
	} else {
		cm.lowLevelConn = append(cm.lowLevelConn[:random], cm.lowLevelConn[random+1:]...)
	}
	return eliminatedPid, nil
}

func (cm *PeerConnManager) closeHighLevelConnRandom(highLevelConnCount int) (peer.ID, error) {
	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(highLevelConnCount)
	eliminatedPid := cm.highLevelConn[random].pid
	for conn := range cm.highLevelConn[random].conn {
		go func(connToClose network.Conn) {
			_ = connToClose.Close()
		}(conn)
	}
	if random == highLevelConnCount-1 {
		cm.highLevelConn = cm.highLevelConn[:random]
	} else {
		cm.highLevelConn = append(cm.highLevelConn[:random], cm.highLevelConn[random+1:]...)
	}
	return eliminatedPid, nil
}

func (cm *PeerConnManager) eliminateConnectionsRandom(isHighLevel bool) (peer.ID, error) {
	hCount := len(cm.highLevelConn)
	lCount := len(cm.lowLevelConn)
	if hCount+lCount >= cm.maxSize {
		if lCount > 0 {
			eliminatedPid, err := cm.closeLowLevelConnRandom(lCount)
			if err != nil {
				return "", err
			}
			cm.log.Debugf("[PeerConnManager] eliminate connections"+
				"(strategy:Random, is high-level:%v, eliminated pid:%s)", isHighLevel, eliminatedPid)
			return eliminatedPid, nil
		} else {
			if !isHighLevel {
				return "", eliminatedHighLevelConnBugError
			}
			eliminatedPid, err := cm.closeHighLevelConnRandom(hCount)
			if err != nil {
				return "", err
			}
			cm.log.Debugf("[PeerConnManager] eliminate connections"+
				"(strategy:Random, is high-level:%v, eliminated pid:%s)", isHighLevel, eliminatedPid)
			return eliminatedPid, nil
		}
	}
	return "", nil
}

func (cm *PeerConnManager) closeLowLevelConnFirst() (peer.ID, error) {
	eliminatedPid := cm.lowLevelConn[0].pid
	for conn := range cm.lowLevelConn[0].conn {
		go func(connToClose network.Conn) {
			_ = connToClose.Close()
		}(conn)
	}
	cm.lowLevelConn = cm.lowLevelConn[1:]
	return eliminatedPid, nil
}

func (cm *PeerConnManager) closeHighLevelConnFirst() (peer.ID, error) {
	eliminatedPid := cm.highLevelConn[0].pid
	for conn := range cm.highLevelConn[0].conn {
		go func(connToClose network.Conn) {
			_ = connToClose.Close()
		}(conn)
	}
	cm.highLevelConn = cm.highLevelConn[1:]
	return eliminatedPid, nil
}

func (cm *PeerConnManager) eliminateConnectionsFIFO(isHighLevel bool) (peer.ID, error) {
	hCount := len(cm.highLevelConn)
	lCount := len(cm.lowLevelConn)
	if hCount+lCount >= cm.maxSize {
		if lCount > 0 {
			eliminatedPid, err := cm.closeLowLevelConnFirst()
			if err != nil {
				return "", err
			}
			cm.log.Debugf("[PeerConnManager] eliminate connections"+
				"(strategy:FIFO, is high-level:%v, eliminated pid:%s)", isHighLevel, eliminatedPid)
			return eliminatedPid, nil
		} else {
			if !isHighLevel {
				return "", eliminatedHighLevelConnBugError
			}
			eliminatedPid, err := cm.closeHighLevelConnFirst()
			if err != nil {
				return "", err
			}
			cm.log.Debugf("[PeerConnManager] eliminate connections"+
				"(strategy:FIFO, is high-level:%v, eliminated pid:%s)", isHighLevel, eliminatedPid)
			return eliminatedPid, nil
		}
	}
	return "", nil
}

func (cm *PeerConnManager) closeLowLevelConnLast(lowLevelConnCount int) (peer.ID, error) {
	idx := lowLevelConnCount - 1
	eliminatedPid := cm.lowLevelConn[idx].pid
	for conn := range cm.lowLevelConn[idx].conn {
		go func(connToClose network.Conn) {
			_ = connToClose.Close()
		}(conn)
	}
	cm.lowLevelConn = cm.lowLevelConn[0:idx]
	return eliminatedPid, nil
}

func (cm *PeerConnManager) closeHighLevelConnLast(highLevelConnCount int) (peer.ID, error) {
	idx := highLevelConnCount - 1
	eliminatedPid := cm.highLevelConn[idx].pid
	for conn := range cm.highLevelConn[idx].conn {
		go func(connToClose network.Conn) {
			_ = connToClose.Close()
		}(conn)
	}
	cm.highLevelConn = cm.highLevelConn[0:idx]
	return eliminatedPid, nil
}

func (cm *PeerConnManager) eliminateConnectionsLIFO(isHighLevel bool) (peer.ID, error) {
	hCount := len(cm.highLevelConn)
	lCount := len(cm.lowLevelConn)
	if hCount+lCount >= cm.maxSize {
		if lCount > 0 {
			eliminatedPid, err := cm.closeLowLevelConnLast(lCount)
			if err != nil {
				return "", err
			}
			cm.log.Debugf("[PeerConnManager] eliminate connections"+
				"(strategy:LIFO, is high-level:%v, eliminated pid:%s)", isHighLevel, eliminatedPid)
			return eliminatedPid, nil
		} else {
			if !isHighLevel {
				return "", eliminatedHighLevelConnBugError
			}
			eliminatedPid, err := cm.closeHighLevelConnLast(hCount)
			if err != nil {
				return "", err
			}
			cm.log.Debugf("[PeerConnManager] eliminate connections"+
				"(strategy:LIFO, is high-level:%v, eliminated pid:%s)", isHighLevel, eliminatedPid)
			return eliminatedPid, nil
		}
	}
	return "", nil
}

// AddConn add a connection.
func (cm *PeerConnManager) AddConn(pid peer.ID, conn network.Conn) bool {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	cm.connLatestTime[pid.Pretty()] = time.Now().UnixNano()
	cm.log.Debugf("[PeerConnManager] add conn(pid:%s)", pid.Pretty())
	isHighLevel := cm.IsHighLevel(pid)
	var pcs *peerConnections
	if isHighLevel {
		connMap, _ := cm.getHighLevelConnections(pid)
		if connMap != nil {
			if _, ok := connMap[conn]; ok {
				cm.log.Warnf("[PeerConnManager] connection exist(pid:%s). ignored.", pid.Pretty())
				return false
			}
			connMap[conn] = struct{}{}
			return true
		}
		connMap = make(map[network.Conn]struct{})
		connMap[conn] = struct{}{}
		pcs = &peerConnections{
			pid:  pid,
			conn: connMap,
		}
	} else {
		connMap, _ := cm.getLowLevelConnections(pid)
		if connMap != nil {
			if _, ok := connMap[conn]; ok {
				cm.log.Warnf("[PeerConnManager] connection exist(pid:%s). ignored.", pid.Pretty())
				return false
			}
			connMap[conn] = struct{}{}
			return true
		}
		connMap = make(map[network.Conn]struct{})
		connMap[conn] = struct{}{}
		pcs = &peerConnections{
			pid:  pid,
			conn: connMap,
		}
	}

	if pcs != nil {
		// execute the connection elimination policy
		// if the number of connections is full, one will be eliminated
		ePid, err := cm.eliminateConnections(isHighLevel)
		if err != nil {
			cm.log.Warnf("[PeerConnManager] eliminate connection failed, %s", err.Error())
			return false
		} else if ePid != "" {
			cm.log.Infof("[PeerConnManager] eliminate connection ok(pid:%s)", ePid.Pretty())
		}

		// put the connection object into the connection manager
		if isHighLevel {
			cm.highLevelConn = append(cm.highLevelConn, pcs)
		} else {
			cm.lowLevelConn = append(cm.lowLevelConn, pcs)
		}
	}

	return true
}

// RemoveConn remove a connection.
func (cm *PeerConnManager) RemoveConn(pid peer.ID, conn network.Conn) bool {
	cm.cmLock.Lock()
	defer cm.cmLock.Unlock()
	conns, idx := cm.getHighLevelConnections(pid)
	if idx != -1 {
		for c := range conns {
			if c == conn {
				delete(conns, c)
			}
		}

		if len(conns) == 0 {
			if idx == len(cm.highLevelConn)-1 {
				cm.highLevelConn = cm.highLevelConn[:idx]
			} else {
				cm.highLevelConn = append(cm.highLevelConn[:idx], cm.highLevelConn[idx+1:]...)
			}
		} else {
			cm.highLevelConn[idx] = &peerConnections{
				pid:  pid,
				conn: conns,
			}
		}

		return true
	}
	conns2, idx2 := cm.getLowLevelConnections(pid)
	if idx2 != -1 {
		for c := range conns2 {
			if c == conn {
				delete(conns2, c)
			}
		}

		if len(conns2) == 0 {
			if idx2 == len(cm.lowLevelConn)-1 {
				cm.lowLevelConn = cm.lowLevelConn[:idx2]
			} else {
				cm.lowLevelConn = append(cm.lowLevelConn[:idx2], cm.lowLevelConn[idx2+1:]...)
			}
		} else {
			cm.lowLevelConn[idx2] = &peerConnections{
				pid:  pid,
				conn: conns2,
			}
		}

		return true
	}

	return false
}

// GetConn return a connection for peer.
func (cm *PeerConnManager) GetConn(pid peer.ID) network.Conn {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()
	if m, idx := cm.getHighLevelConnections(pid); idx != -1 {
		for conn := range m {
			return conn
		}
	}
	if m, idx := cm.getLowLevelConnections(pid); idx != -1 {
		for conn := range m {
			return conn
		}
	}
	return nil
}

// GetConns return a connection for peer.
func (cm *PeerConnManager) GetConns(pid peer.ID) []network.Conn {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()
	conns := make([]network.Conn, 0)
	if m, idx := cm.getHighLevelConnections(pid); idx != -1 {
		for conn := range m {
			conns = append(conns, conn)
		}
	}
	if m, idx := cm.getLowLevelConnections(pid); idx != -1 {
		for conn := range m {
			conns = append(conns, conn)
		}
	}
	return conns
}

// IsConnected return true if peer has connected. Otherwise, return false.
func (cm *PeerConnManager) IsConnected(pid peer.ID) bool {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()
	if _, idx := cm.getHighLevelConnections(pid); idx != -1 {
		return true
	}
	if _, idx := cm.getLowLevelConnections(pid); idx != -1 {
		return true
	}
	return false
}

// CanConnect return true if peer can connect to self. Otherwise, return false.
func (cm *PeerConnManager) CanConnect(pid peer.ID) bool {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()
	// allow a connection if it has not been made before
	lastestConnTime, ok := cm.connLatestTime[pid.Pretty()]
	if !ok {
		return true
	}

	// if the connection has been made before, check whether the connection interval is within the legal range
	// the interval is left closed and right open
	// [allowedConnTimeIntervalLower,allowedConnTimeIntervalUpper)
	rand.Seed(time.Now().UnixNano())
	allowedConnTimeInterval := (allowedConnTimeIntervalLower + rand.Intn(allowedConnTimeIntervalUpper-allowedConnTimeIntervalLower)) * 1e9
	if time.Now().UnixNano()-lastestConnTime <= int64(allowedConnTimeInterval) {
		return false
	}

	return true
}

// ConnCount return the count num of connections.
func (cm *PeerConnManager) ConnCount() int {
	cm.cmLock.RLock()
	defer cm.cmLock.RUnlock()
	return len(cm.highLevelConn) + len(cm.lowLevelConn)
}
