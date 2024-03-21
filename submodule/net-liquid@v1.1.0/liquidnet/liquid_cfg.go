/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package liquidnet

import (
	"chainmaker.org/chainmaker/net-liquid/host"
	ma "github.com/multiformats/go-multiaddr"
)

// SetListenAddrStr set the local address will be listening on fot host.HostConfig.
func SetListenAddrStr(hc *host.HostConfig, listenAddrStr string) error {
	a, err := ma.NewMultiaddr(listenAddrStr)
	if err != nil {
		return err
	}
	hc.ListenAddresses = []ma.Multiaddr{a}
	return nil
}

// cryptoConfig defines some data required for encryption
type cryptoConfig struct {
	PubKeyMode bool
	KeyBytes   []byte
	CertBytes  []byte
	//for gmtls
	EncKeyBytes                    []byte
	EncCertBytes                   []byte
	CustomChainTrustRootCertsBytes map[string][][]byte
}

// SetCustomTrustRootCert set TrustRoot cert for specified chainId
func (cc *cryptoConfig) SetCustomTrustRootCert(chainId string, rootCerts [][]byte) {
	if cc.CustomChainTrustRootCertsBytes == nil {
		cc.CustomChainTrustRootCertsBytes = make(map[string][][]byte)
	}
	if _, ok := cc.CustomChainTrustRootCertsBytes[chainId]; !ok {
		cc.CustomChainTrustRootCertsBytes[chainId] = make([][]byte, 0, 10)
	}
	cc.CustomChainTrustRootCertsBytes[chainId] = rootCerts
}

// pubSubConfig some configuration information required by pubsub
type pubSubConfig struct {
	MaxPubMessageSize int
}

// extensionsConfig Some extended configuration information,
// such as whether to enable PKT, whether to enable message priority control.
type extensionsConfig struct {
	EnablePkt          bool
	EnablePriorityCtrl bool
}

// holePunchConfig some configurations related to hole punch, set whether to enable hole punch.
type holePunchConfig struct {
	EnablePunch bool // whether to enable hole punch
}

// stunClientConfig some configuration information related to stun client
type stunClientConfig struct {
	Enable           bool // whether to enable stun client
	ClientListenAddr ma.Multiaddr
	StunServerAddr   ma.Multiaddr
	NetworkType      string
}

// stunServerConfig some configuration information related to stun server
type stunServerConfig struct {
	OtherStunServerAddr ma.Multiaddr
	// whether to enable stun server
	EnableServer      bool
	ServerListenAddr1 ma.Multiaddr
	ServerListenAddr2 ma.Multiaddr
	TwoPublicAddress  bool
	ServerListenAddr3 ma.Multiaddr
	ServerListenAddr4 ma.Multiaddr
	NotifyAddr        string
	LocalNotifyAddr   string
	NetworkType       string
}

// GenMaAddr converts an address in the form of a string to a multi address object
func GenMaAddr(addr string) ma.Multiaddr {
	a, _ := ma.NewMultiaddr(addr)
	return a
}
