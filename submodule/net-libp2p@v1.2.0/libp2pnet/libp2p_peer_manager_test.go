package libp2pnet

import (
	"testing"
)

func TestMapPeerStreamManager(t *testing.T) {
	m := newPeerStreamManager()
	p1 := "1111111"
	p2 := "2222222"
	h1 := &peerSendMsgHandler{}
	h2 := &peerSendMsgHandler{}

	if v, ok := m.GetStream(p1); v != nil || ok {
		t.Error()
	}
	if v, ok := m.GetStream(p2); v != nil || ok {
		t.Error()
	}

	m.AddStream(p1, h1)
	m.AddStream(p2, h2)
	if v, ok := m.GetStream(p1); v != h1 || !ok {
		t.Error()
	}
	if v, ok := m.GetStream(p2); v != h2 || !ok {
		t.Error()
	}

	m.DeleteStream(p1)
	if v, ok := m.GetStream(p1); v != nil || ok {
		t.Error()
	}
	if v, ok := m.GetStream(p2); v != h2 || !ok {
		t.Error()
	}
}
