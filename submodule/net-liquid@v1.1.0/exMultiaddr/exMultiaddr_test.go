package exMultiaddr

import (
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

func TestEx(t *testing.T) {
	info := "holePunch"
	newAddr, _ := ma.NewMultiaddr("/ip4/0.0.0.0/udp/9002/quic")
	exAddr := &ExInfoMultiaddr{
		Multiaddr: newAddr,
		Info:      info,
	}
	newAddr = exAddr
	if len(newAddr.String()) == 0 {
		t.Error()
	}
	exMultiaddr, ok := newAddr.(*ExInfoMultiaddr)
	if exMultiaddr == nil || !ok {
		t.Error()
	}
	if info != exMultiaddr.GetExInfo() {
		t.Error()
	}
}
