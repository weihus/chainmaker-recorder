package stun

import (
	"testing"

	"chainmaker.org/chainmaker/net-liquid/core/peer"

	ma "github.com/multiformats/go-multiaddr"

	"chainmaker.org/chainmaker/net-liquid/logger"
)

//test server
//listen and dial a real addr
func TestUdpStunServer(t *testing.T) {
	log := logger.NewLogPrinter("stunTest")
	var stunServer Server
	stunServer, _ = NewUdpStunServer(log)
	//prepare addr
	stunServerAddr1, _ := ma.NewMultiaddr("/ip4/0.0.0.0/udp/3478/quic")
	stunServerAddr2, _ := ma.NewMultiaddr("/ip4/0.0.0.0/udp/3479/quic")
	stunServerAddr3, _ := ma.NewMultiaddr("/ip4/0.0.0.0/udp/3481/quic")
	stunServerAddr4, _ := ma.NewMultiaddr("/ip4/0.0.0.0/udp/3482/quic")
	otherAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3481/quic")
	changeParam := &ChangeParam{
		OtherAddr:             otherAddr,
		OtherMasterListenAddr: stunServerAddr3,
		OtherSlaveListenAddr:  stunServerAddr4,
	}
	err := stunServer.InitChangeIpLocal(changeParam)
	if err != nil {
		t.Error(err)
	}
	//start server
	err = stunServer.Listen(stunServerAddr1, stunServerAddr2)
	if err != nil {
		t.Error(err)
	}
	//client
	pid := peer.ID("QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4")
	clientListenAddr, _ := ma.NewMultiaddr("/ip4/0.0.0.0/udp/3333/quic")
	stunServerAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/udp/3478")
	logC := logger.NewLogPrinter("stunClient")
	var client Client
	client, _ = NewUdpStunClient(logC)
	natType, err := client.CheckNatType(pid.ToString(), clientListenAddr, stunServerAddr)
	if err != nil {
		t.Error(err)
	}
	//server start local ,should return NATTypeFullCone
	if natType != NATTypeFullCone {
		t.Error(err)
	}
	//if upload success
	natType2 := client.GetNatType()
	if natType != natType2 {
		t.Error()
	}
	natType3, err := client.GetPeerNatType(pid.ToString(), clientListenAddr, stunServerAddr)
	if err != nil {
		t.Error(err)
	}
	if natType != natType3 {
		t.Error()
	}
	//close
	err = stunServer.CloseListen()
	if err != nil {
		t.Error(err)
	}
}
