package libp2pnet

import (
	"sync"
)

type peerMsgChanManager interface {
	GetStream(pid string) (*peerSendMsgHandler, bool)
	DeleteStream(pid string)
	AddStream(pid string, stream *peerSendMsgHandler)
}

func newPeerStreamManager() peerMsgChanManager {
	return &mapPeerStreamManager{}
}

type mapPeerStreamManager struct {
	mapChan sync.Map
}

func (m *mapPeerStreamManager) GetStream(pid string) (*peerSendMsgHandler, bool) {
	v, loaded := m.mapChan.Load(pid)
	if !loaded {
		return nil, false
	}
	r, ok := v.(*peerSendMsgHandler)
	return r, ok
}

func (m *mapPeerStreamManager) DeleteStream(pid string) {
	m.mapChan.Delete(pid)
}

func (m *mapPeerStreamManager) AddStream(pid string, stream *peerSendMsgHandler) {
	m.mapChan.Store(pid, stream)
}
