/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"context"
	"errors"
	"fmt"
	"net"

	"chainmaker.org/chainmaker/common/v2/crypto/tls"
	"chainmaker.org/chainmaker/net-libp2p/utils"
	api "chainmaker.org/chainmaker/protocol/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/sec"
	ma "github.com/multiformats/go-multiaddr"
)

// ID is the protocol ID (used when negotiating with multistream)
const ID = "/cmtls/1.0.0"

// Transport constructs secure communication sessions for a peer.
type Transport struct {
	config *tls.Config

	privKey    crypto.PrivKey
	localPeer  peer.ID
	LibP2pHost func() host.Host
	log        api.Logger
}

var _ sec.SecureTransport = &Transport{}

// New return a function can create a new Transport instance.
func New(
	tlsCfg *tls.Config,
	host func() host.Host,
	logger api.Logger,
) func(key crypto.PrivKey) (*Transport, error) {
	return func(key crypto.PrivKey) (*Transport, error) {
		id, err := peer.IDFromPrivateKey(key)
		if err != nil {
			return nil, err
		}
		return &Transport{
			config:     tlsCfg,
			privKey:    key,
			localPeer:  id,
			LibP2pHost: host,
			log:        logger,
		}, nil
	}
}

// SecureInbound runs the TLS handshake as a server.
func (t *Transport) SecureInbound(ctx context.Context, insecure net.Conn) (sec.SecureConn, error) {
	c := tls.Server(insecure, t.config.Clone())
	if err := c.Handshake(); err != nil {
		insecure.Close()
		return nil, err
	}

	remotePubKey, err := t.getPeerPubKey(c)
	if err != nil {
		return nil, err
	}

	return t.setupConn(c, remotePubKey)
}

// SecureOutbound runs the TLS handshake as a client.
func (t *Transport) SecureOutbound(ctx context.Context, insecure net.Conn, p peer.ID) (sec.SecureConn, error) {
	conf := t.config.Clone()

	host := t.LibP2pHost()
	if host != nil && host.Peerstore() != nil {
		peerInfo := host.Peerstore().PeerInfo(p)
		t.log.Info("SecureOutbound peerInfo:", p.String(), ",", peerInfo)
		for _, addr := range peerInfo.Addrs {
			if addr == nil {
				continue
			}
			t.log.Info("SecureOutbound addr:", addr.String())
			if !haveDns(addr) {
				continue
			}
			t.log.Info("SecureOutbound addr have dns:", addr.String())
			dnsDomain, _ := ma.SplitFirst(addr)
			if dnsDomain == nil {
				continue
			}
			conf.ServerName, _ = dnsDomain.ValueForProtocol(dnsDomain.Protocol().Code)
			t.log.Info("SecureOutbound ServerName:", conf.ServerName)
			break
		}
	}

	c := tls.Client(insecure, conf)
	if err := c.Handshake(); err != nil {
		insecure.Close()
		return nil, err
	}

	remotePubKey, err := t.getPeerPubKey(c)
	if err != nil {
		return nil, err
	}

	return t.setupConn(c, remotePubKey)
}

func haveDns(addr ma.Multiaddr) bool {
	protocols := addr.Protocols()
	for _, p := range protocols {
		switch p.Code {
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6:
			return true
		}
	}
	return false
}

func (t *Transport) getPeerPubKey(conn *tls.Conn) (crypto.PubKey, error) {
	state := conn.ConnectionState()
	if len(state.PeerCertificates) <= 0 {
		return nil, errors.New("expected one certificates in the chain")
	}

	pubKey, err := utils.ParsePublicKeyToPubKey(state.PeerCertificates[0].PublicKey.ToStandardKey())
	if err != nil {
		return nil, fmt.Errorf("unmarshalling public key failed: %s", err)
	}
	return pubKey, err
}

func (t *Transport) setupConn(tlsConn *tls.Conn, remotePubKey crypto.PubKey) (sec.SecureConn, error) {
	remotePeerID, err := peer.IDFromPublicKey(remotePubKey)
	if err != nil {
		return nil, err
	}

	return &conn{
		Conn:         tlsConn,
		localPeer:    t.localPeer,
		privKey:      t.privKey,
		remotePeer:   remotePeerID,
		remotePubKey: remotePubKey,
	}, nil
}
