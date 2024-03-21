package tcp

import (
	"context"
	"fmt"
	"testing"

	"chainmaker.org/chainmaker/net-liquid/core/util"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// TestRemoteAddr test resolving peerId and determine whether the address can be dialed
func TestRemoteAddr(t *testing.T) {
	tcpNet := tcpNetwork{}
	addr0, err0 := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8001")
	if err0 != nil {
		fmt.Println(err0)
	}
	fmt.Println(addr0.String())
	_, remotePID := util.GetNetAddrAndPidFromNormalMultiAddr(addr0)
	fmt.Println(remotePID)

	dialAddr, _ := util.GetNetAddrAndPidFromNormalMultiAddr(addr0)
	addr00, err := manet.ToNetAddr(dialAddr)
	if err != nil {
		t.Error(err)
	}
	if !tcpNet.canDial(dialAddr) {
		t.Error()
	}
	fmt.Println(addr00.String())
}

// TestDialAddr test dialing to the destination address
func TestDialAddr(t *testing.T) {
	tcpNet := tcpNetwork{}
	addr0, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8001/p2p/QmeyNRs2DwWjcHTpcVHoUSaDAAif4VQZ2wQDQAUNDP33gH/dns/holePunch")
	tcpNet.listening = true
	_, err := tcpNet.Dial(context.Background(), addr0)
	switch err {
	case ErrWrongTcpAddr:
		t.Error(err)
	}
	fmt.Println(err)
}
