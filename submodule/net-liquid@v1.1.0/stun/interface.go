package stun

import (
	"chainmaker.org/chainmaker/net-liquid/core/host"
	"chainmaker.org/chainmaker/net-liquid/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

// Client :is use to check local nat
type Client interface {
	//SetNetwork: save a network use to dial or listen
	SetNetwork(network.Network) error
	//CheckNatType: send msg to server and recv response to check nat type
	CheckNatType(peerId string, clientListenAddr, stunServerAddr ma.Multiaddr) (NATDeviceType, error)
	//CheckNatTypeByDomain: send msg to server support domain and recv response to check nat type
	CheckNatTypeByDomain(clientListenAddr ma.Multiaddr, peerId, stunServerDomain string) (NATDeviceType, error)
	//GetPeerNatType: send msg to server and recv response to get peer nat type
	GetPeerNatType(peerId string, clientListenAddr, stunServerAddr ma.Multiaddr) (NATDeviceType, error)
	//GetNatType: get nat type last time checked
	GetNatType() NATDeviceType
	//GetMappingType: get mapping type last time checked
	GetMappingType() BehaviorType
	//GetFilteringType: get filter type last time checked
	GetFilteringType() BehaviorType
}

// Server stun server
//help stun client to check its nat type
//InitChangeIpLocal: start listen on second public addr
//InitChangeIpNotify:start server on http serve to handle notify
type Server interface {
	//SetNetwork: save a network use to dial or listen
	SetNetwork(net1, net2 network.Network) error
	//Listen: start listen
	Listen(addr1, addr2 ma.Multiaddr) error
	//local device have two net card and two public address
	InitChangeIpLocal(*ChangeParam) error
	//other stun server locate another device
	InitChangeIpNotify(*ChangeNotifyParam) error
	//HandleChangeIpReq: handle change ip req from client
	HandleChangeIpReq(ma.Multiaddr, []byte) error
	//CloseListen: stop listen and close all conn
	CloseListen() error
}

// HolePunch interface
type HolePunch interface {
	//HolePunchAvailable: calculate a result if punch is available between two nat type
	HolePunchAvailable(type1, type2 NATDeviceType) bool
	//SetHost: set a host use to punch
	SetHost(host host.Host) error
	//DirectConnect: try punch
	DirectConnect(otherPeerAddr ma.Multiaddr) (network.Conn, error)
}
