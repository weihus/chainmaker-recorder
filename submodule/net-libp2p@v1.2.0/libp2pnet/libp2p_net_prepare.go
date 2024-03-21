/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"encoding/pem"
	"strconv"
	"strings"
	"sync"

	"chainmaker.org/chainmaker/common/v2/crypto/engine"

	"chainmaker.org/chainmaker/common/v2/crypto/asym"
	"chainmaker.org/chainmaker/common/v2/crypto/tls"
	cmx509 "chainmaker.org/chainmaker/common/v2/crypto/x509"
	"chainmaker.org/chainmaker/common/v2/helper"
	"chainmaker.org/chainmaker/net-common/cmtlssupport"
	"chainmaker.org/chainmaker/net-common/common/priorityblocker"
	"chainmaker.org/chainmaker/net-libp2p/cmtls"
	"github.com/libp2p/go-libp2p"
	circuit "github.com/libp2p/go-libp2p-circuit"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

// LibP2pNetPrepare prepare the config options.
type LibP2pNetPrepare struct {
	listenAddr              string              // listenAddr
	bootstrapsPeers         map[string]struct{} // bootstrapsPeers
	pubSubMaxMsgSize        int                 // pubSubMaxMsgSize
	peerStreamPoolSize      int                 // peerStreamPoolSize
	maxPeerCountAllow       int                 // maxPeerCountAllow
	peerEliminationStrategy int                 // peerEliminationStrategy

	pubKeyMode    bool   // whether using public key mode
	keyBytes      []byte // keyBytes
	certBytes     []byte // certBytes
	encKeyBytes   []byte //fot gmtls if set
	encCertBytes  []byte
	tlsServerName string

	blackAddresses map[string]struct{} // blackAddresses
	blackPeerIds   map[string]struct{} // blackPeerIds

	isInsecurity       bool
	pktEnable          bool
	priorityCtrlEnable bool

	lock sync.Mutex

	readySignalC chan struct{}
}

func (l *LibP2pNetPrepare) SetReadySignalC(readySignalC chan struct{}) {
	l.readySignalC = readySignalC
}

func (l *LibP2pNetPrepare) SetIsInsecurity(isInsecurity bool) {
	l.isInsecurity = isInsecurity
}

func (l *LibP2pNetPrepare) SetPktEnable(pktEnable bool) {
	l.pktEnable = pktEnable
}

func (l *LibP2pNetPrepare) SetPriorityCtrlEnable(priorityCtrlEnable bool) {
	l.priorityCtrlEnable = priorityCtrlEnable
}

// SetPubKeyModeEnable set whether to use public key mode of permission.
func (l *LibP2pNetPrepare) SetPubKeyModeEnable(pkModeEnable bool) {
	l.pubKeyMode = pkModeEnable
}

// SetCert set cert with pem bytes.
func (l *LibP2pNetPrepare) SetCert(certPem []byte) {
	l.certBytes = certPem
}

// SetKey set private key with pem bytes.
func (l *LibP2pNetPrepare) SetKey(keyPem []byte) {
	l.keyBytes = keyPem
}

// SetEncCert set cert with pem bytes.
func (l *LibP2pNetPrepare) SetEncCert(certPem []byte) {
	l.encCertBytes = certPem
}

// SetEncKey set private key with pem bytes.
func (l *LibP2pNetPrepare) SetEncKey(keyPem []byte) {
	l.encKeyBytes = keyPem
}

// SetPubSubMaxMsgSize set max msg size for pub-sub service.(M)
func (l *LibP2pNetPrepare) SetPubSubMaxMsgSize(pubSubMaxMsgSize int) {
	l.pubSubMaxMsgSize = pubSubMaxMsgSize
}

// SetPeerStreamPoolSize set stream pool max size of each peer.
func (l *LibP2pNetPrepare) SetPeerStreamPoolSize(peerStreamPoolSize int) {
	l.peerStreamPoolSize = peerStreamPoolSize
}

// AddBootstrapsPeer add a node address for connecting directly. It can be a seed node address or a consensus node address.
func (l *LibP2pNetPrepare) AddBootstrapsPeer(bootstrapAddr string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.bootstrapsPeers[bootstrapAddr] = struct{}{}
}

// SetListenAddr set address that the net will listen on.
// 		example: /ip4/127.0.0.1/tcp/10001
func (l *LibP2pNetPrepare) SetListenAddr(listenAddr string) {
	l.listenAddr = listenAddr
}

// SetMaxPeerCountAllow set max count of nodes that allow to connect to us.
func (l *LibP2pNetPrepare) SetMaxPeerCountAllow(maxPeerCountAllow int) {
	l.maxPeerCountAllow = maxPeerCountAllow
}

// SetPeerEliminationStrategy set the strategy for eliminating when reach the max count.
func (l *LibP2pNetPrepare) SetPeerEliminationStrategy(peerEliminationStrategy int) {
	l.peerEliminationStrategy = peerEliminationStrategy
}

// AddBlackAddress add a black address to blacklist.
// 		example: 192.168.1.14:8080
//		example: 192.168.1.14
func (l *LibP2pNetPrepare) AddBlackAddress(address string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	address = strings.ReplaceAll(address, "：", ":")
	if _, ok := l.blackAddresses[address]; !ok {
		l.blackAddresses[address] = struct{}{}
	}
}

// AddBlackPeerId add a black node id to blacklist.
// 		example: QmcQHCuAXaFkbcsPUj7e37hXXfZ9DdN7bozseo5oX4qiC4
func (l *LibP2pNetPrepare) AddBlackPeerId(pid string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if _, ok := l.blackPeerIds[pid]; !ok {
		l.blackPeerIds[pid] = struct{}{}
	}
}

func (ln *LibP2pNet) prepareBlackList() error {
	ln.log.Info("[Net] preparing blacklist...")
	for addr := range ln.prepare.blackAddresses {
		s := strings.Split(addr, ":")
		ip := s[0]
		var port = -1
		var err error
		if len(s) > 1 {
			port, err = strconv.Atoi(s[1])
			if err != nil {
				ln.log.Errorf("[Net] parse port failed, %s", err.Error())
				return err
			}
		}
		ln.libP2pHost.blackList.AddIPAndPort(ip, port)
		ln.log.Infof("[Net] black address found[%s]", addr)
	}
	for pid := range ln.prepare.blackPeerIds {
		peerId, err := peer.Decode(pid)
		if err != nil {
			ln.log.Errorf("[Net] decode pid failed(pid:%s), %s", pid, err.Error())
			return err
		}
		ln.libP2pHost.blackList.AddPeerId(peerId)
		ln.log.Infof("[Net] black peer id found[%s]", pid)
	}
	ln.log.Info("[Net] blacklist prepared.")
	return nil
}

// createLibp2pOptions create all necessary options for libp2p.
func (ln *LibP2pNet) createLibp2pOptions() ([]libp2p.Option, error) {
	ln.log.Info("[Net] creating options...")

	//use default crypto engine, TODO optimize
	engine.InitCryptoEngine("tjfoc", true)

	prvKey, err := ln.prepareKey()
	if err != nil {
		ln.log.Errorf("[Net] prepare key failed, %s", err.Error())
		return nil, err
	}
	connGater := NewConnGater(ln.libP2pHost.connManager, ln.libP2pHost.blackList, ln.libP2pHost.memberStatusValidator, ln.log)
	listenAddrs := strings.Split(ln.prepare.listenAddr, ",")
	options := []libp2p.Option{
		libp2p.Identity(prvKey),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.ConnectionGater(connGater),
		libp2p.EnableRelay(circuit.OptHop),
		//libp2p.EnableNATService(),
	}
	if ln.prepare.isInsecurity {
		ln.log.Warn("[Net] use insecurity option.")
		options = append(options, libp2p.NoSecurity)
		ln.libP2pHost.isTls = false
	} else {
		if prvKey.Type().String() == "SM2" {
			ln.log.Info("[Net] the private key type found[sm2]. use gm tls security.")
			ln.libP2pHost.isTls = true
		} else {
			ln.log.Info("[Net] the private key type found[not sm2]. use normal tls security.")
			ln.libP2pHost.isTls = true
		}
		// tls cert validator
		ln.libP2pHost.tlsCertValidator = cmtlssupport.NewCertValidator(
			ln.prepare.pubKeyMode,
			ln.libP2pHost.memberStatusValidator,
			ln.libP2pHost.customChainTrustRoots,
		)
		ln.libP2pHost.initTlsSubassemblies()

		var tlsCfg *tls.Config
		if ln.prepare.pubKeyMode {
			// public key mode
			ln.log.Info("[Net] public key mode confirmed.")
			// get private key
			privateKey, err2 := asym.PrivateKeyFromPEM(ln.prepare.keyBytes, nil)
			if err2 != nil {
				return nil, err2
			}
			// get public key bytes
			pubKeyPem, err3 := privateKey.PublicKey().String()
			if err3 != nil {
				return nil, err3
			}
			// get peer id
			peerId, err4 := helper.CreateLibp2pPeerIdWithPrivateKey(privateKey)
			if err4 != nil {
				return nil, err4
			}
			// store peer id
			ln.libP2pHost.peerIdPubKeyStore.SetPeerPubKey(peerId, []byte(pubKeyPem))
			// store certIdMap
			ln.libP2pHost.certPeerIdMapper.Add(pubKeyPem, peerId)
			// create tls config
			tlsCfg, err = cmtlssupport.NewTlsConfigWithPubKeyMode(privateKey, ln.libP2pHost.tlsCertValidator)
			if err != nil {
				return nil, err
			}
		} else {
			// cert mode
			ln.log.Info("[Net] certificate mode confirmed.")
			// create tls certificate
			var tlsCerts []tls.Certificate
			tlsCert, peerId, e := cmtlssupport.GetCertAndPeerIdWithKeyPair(ln.prepare.certBytes, ln.prepare.keyBytes)
			if e != nil {
				return nil, e
			}
			tlsCerts = append(tlsCerts, *tlsCert)

			tlsEncCert, _, e := cmtlssupport.GetCertAndPeerIdWithKeyPair(ln.prepare.encCertBytes, ln.prepare.encKeyBytes)
			if e == nil && tlsEncCert != nil {
				tlsCerts = append(tlsCerts, *tlsEncCert)
				ln.log.Info("[Net] tls enc certificate is set, use gmtls")
			}

			// store tls cert
			ln.libP2pHost.peerIdTlsCertStore.SetPeerTlsCert(peerId, tlsCert.Certificate[0])
			// store certIdMap
			var tlsCertificate *cmx509.Certificate
			certBlock, rest := pem.Decode(ln.prepare.certBytes)
			if certBlock == nil {
				tlsCertificate, err = cmx509.ParseCertificate(rest)
				if err != nil {
					ln.log.Warnf("[Net] [prepare] set cert id map failed, %s", err.Error())
					return nil, err
				}
			} else {
				tlsCertificate, err = cmx509.ParseCertificate(certBlock.Bytes)
				if err != nil {
					ln.log.Warnf("[Net] [prepare] set cert id map failed, %s", err.Error())
					return nil, err
				}
			}

			var certIdBytes []byte
			certIdBytes, err = cmx509.GetNodeIdFromSm2Certificate(cmx509.OidNodeId, *tlsCertificate)
			if err != nil {
				ln.log.Warn("[Net] [prepare] set cert id map failed, %s", err.Error())
				return nil, err
			}
			ln.libP2pHost.certPeerIdMapper.Add(string(certIdBytes), peerId)

			// create tls config
			tlsCfg, err = cmtlssupport.NewTlsConfigWithCertMode(tlsCerts, ln.libP2pHost.tlsCertValidator)
			if err != nil {
				return nil, err
			}
		}

		tmp := func() host.Host {
			return ln.libP2pHost.Host()
		}
		tpt := cmtls.New(tlsCfg, tmp, ln.log)
		options = append(options, libp2p.Security(cmtls.ID, tpt))
	}
	ln.log.Info("[Net] options created.")
	return options, nil
}

func (ln *LibP2pNet) prepareKey() (crypto.PrivKey, error) {
	ln.log.Info("[Net] node key preparing...")
	var privKey crypto.PrivKey
	var err error
	// read file
	skPemBytes := ln.prepare.keyBytes
	privateKey, err := asym.PrivateKeyFromPEM(skPemBytes, nil)
	if err != nil {
		ln.log.Errorf("[Net] parse pem to private key failed, %s", err.Error())
		return nil, err
	}
	privKey, _, err = crypto.KeyPairFromStdKey(privateKey.ToStandardKey())
	if err != nil {
		ln.log.Errorf("[Net] parse private key to priv key failed, %s", err.Error())
		return nil, err
	}
	ln.log.Info("[Net] node key prepared ok.")
	return privKey, err
}

func (ln *LibP2pNet) initPktAdapter() error {
	if ln.prepare.pktEnable {
		ln.pktAdapter = newPktAdapter(ln)
		e := ln.messageHandlerDistributor.registerHandler(pktChainId, pktMsgFlag, ln.pktAdapter.directMsgHandler)
		if e != nil {
			return e
		}
		ln.pktAdapter.run()
	}
	return nil
}

func (ln *LibP2pNet) initPriorityController() {
	if ln.prepare.priorityCtrlEnable {
		ln.priorityController = priorityblocker.NewBlocker(nil)
		ln.priorityController.Run()
	}
}
