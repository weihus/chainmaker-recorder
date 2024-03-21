package stun

import (
	"errors"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/stun"
)

// ChangeParam two public addr
type ChangeParam struct {
	OtherAddr             ma.Multiaddr
	OtherMasterListenAddr ma.Multiaddr
	OtherSlaveListenAddr  ma.Multiaddr
	OtherMasterNetwork    network.Network
	OtherSlaveNetwork     network.Network
}

// ChangeNotifyParam  one public addr
type ChangeNotifyParam struct {
	OtherAddr       ma.Multiaddr
	NotifyAddr      string
	LocalNotifyAddr string
}

type (
	networkConn = network.Conn
	//attrType    = stun.AttrType
)

var (
	software          = stun.NewSoftware("stun")
	errNotSTUNMessage = errors.New("not stun message")
	notifyRoutPattern = "/stunNotify/changeSend"
)
