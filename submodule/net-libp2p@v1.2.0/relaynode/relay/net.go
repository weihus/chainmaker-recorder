/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package relay

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"chainmaker.org/chainmaker/net-libp2p/libp2pnet"
	"chainmaker.org/chainmaker/protocol/v2"
)

// NetOption is a function apply options to net instance.
type NetOption func(ln *libp2pnet.LibP2pNet) error

// NewNet create a net instance
func NewNet(cfg *NetConfig, readySignalC chan struct{}) (protocol.Net, error) {
	var err error
	// get auth type
	authType := cfg.AuthType
	if len(authType) == 0 {
		return nil, fmt.Errorf("auth type is empty")
	}

	// check if custom trust root is empty, in certificate mode
	if strings.ToLower(authType) == protocol.PermissionedWithCert {
		if cfg.CustomTrustRootsConfigs == nil || len(cfg.CustomTrustRootsConfigs) == 0 ||
			len(cfg.CustomTrustRootsConfigs[0].TrustRoots) == 0 {
			return nil, fmt.Errorf("custom trust roots is empty")
		}
	}

	// load tls keys and cert path
	keyPath := cfg.TLSConfig.PrivKeyFile
	if !filepath.IsAbs(keyPath) {
		keyPath, err = filepath.Abs(keyPath)
		if err != nil {
			return nil, err
		}
	}
	rlogger.Infof("load net tls key file path: %s", keyPath)

	var certPath string
	var pubKeyMode bool
	switch strings.ToLower(authType) {
	case protocol.PermissionedWithKey, protocol.Public:
		pubKeyMode = true
	case protocol.PermissionedWithCert, protocol.Identity:
		pubKeyMode = false
		certPath = cfg.TLSConfig.CertFile
		if !filepath.IsAbs(certPath) {
			certPath, err = filepath.Abs(certPath)
			if err != nil {
				return nil, err
			}
		}
		rlogger.Infof("load net tls cert file path: %s", certPath)
	default:
		return nil, errors.New("wrong auth type")
	}

	// new net instance
	net, err := newNet(
		rlogger,
		WithReadySignalC(readySignalC),
		WithListenAddr(cfg.ListenAddr),
		WithCrypto(pubKeyMode, keyPath, certPath),
		WithPeerStreamPoolSize(cfg.PeerStreamPoolSize),
		WithMaxPeerCountAllowed(cfg.MaxPeerCountAllow),
		WithPeerEliminationStrategy(cfg.PeerEliminationStrategy),
		WithSeeds(cfg.Seeds...),
		WithBlackAddresses(cfg.BlackList.Addresses...),
		WithBlackNodeIds(cfg.BlackList.NodeIds...),
		WithInsecurity(cfg.IsNetInsecurity),
	)
	if err != nil {
		errMsg := fmt.Sprintf("new net failed, %s", err.Error())
		rlogger.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	// load trust roots
	for _, customTrustRoot := range cfg.CustomTrustRootsConfigs {
		var chainTrustRoots [][]byte
		for _, rootPath := range customTrustRoot.TrustRoots {
			rootBytes, err2 := ioutil.ReadFile(rootPath)
			if err2 != nil {
				return nil, err2
			}
			chainTrustRoots = append(chainTrustRoots, rootBytes)
		}
		net.SetChainCustomTrustRoots(customTrustRoot.ChainId, chainTrustRoots)
	}

	return net, nil
}

// WithReadySignalC set signal channel
func WithReadySignalC(signalC chan struct{}) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		ln.Prepare().SetReadySignalC(signalC)
		return nil
	}
}

// WithListenAddr set addr that the local net will listen on.
func WithListenAddr(addr string) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		ln.Prepare().SetListenAddr(addr)
		return nil
	}
}

// WithCrypto set private key file and tls cert file for the net to create connection.
func WithCrypto(pkMode bool, keyFile string, certFile string) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		var (
			err                 error
			keyBytes, certBytes []byte
		)
		keyBytes, err = ioutil.ReadFile(keyFile)
		if err != nil {
			return err
		}
		if !pkMode {
			certBytes, err = ioutil.ReadFile(certFile)
			if err != nil {
				return err
			}
		}
		ln.Prepare().SetPubKeyModeEnable(pkMode)
		ln.Prepare().SetKey(keyBytes)
		if !pkMode {
			ln.Prepare().SetCert(certBytes)
		}
		return nil
	}
}

// WithSeeds set addresses of discovery service node.
func WithSeeds(seeds ...string) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		if seeds == nil {
			return nil
		}
		for _, seed := range seeds {
			ln.Prepare().AddBootstrapsPeer(seed)
		}
		return nil
	}
}

// WithPeerStreamPoolSize set the max stream pool size for every node that connected to us.
func WithPeerStreamPoolSize(size int) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		ln.Prepare().SetPeerStreamPoolSize(size)
		return nil
	}
}

// WithMaxPeerCountAllowed set max count of nodes that connected to us.
func WithMaxPeerCountAllowed(max int) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		ln.Prepare().SetMaxPeerCountAllow(max)
		return nil
	}
}

// WithPeerEliminationStrategy set the strategy for eliminating node when the count of nodes
// that connected to us reach the max value.
func WithPeerEliminationStrategy(strategy int) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		ln.Prepare().SetPeerEliminationStrategy(strategy)
		return nil
	}
}

// WithBlackAddresses set addresses of the nodes for blacklist.
func WithBlackAddresses(blackAddresses ...string) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		if blackAddresses == nil {
			return nil
		}
		for _, ba := range blackAddresses {
			ln.Prepare().AddBlackAddress(ba)
		}
		return nil
	}
}

// WithBlackNodeIds set ids of the nodes for blacklist.
func WithBlackNodeIds(blackNodeIds ...string) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		if blackNodeIds == nil {
			return nil
		}
		for _, bn := range blackNodeIds {
			ln.Prepare().AddBlackPeerId(bn)
		}
		return nil
	}
}

// WithInsecurity set is insecurity
func WithInsecurity(isInsecurity bool) NetOption {
	return func(ln *libp2pnet.LibP2pNet) error {
		ln.Prepare().SetIsInsecurity(isInsecurity)
		return nil
	}
}

// newNet create a new net instance.
func newNet(netLogger protocol.Logger, opts ...NetOption) (protocol.Net, error) {
	localNet, err := libp2pnet.NewLibP2pNet(netLogger)
	if err != nil {
		return nil, err
	}
	if err = Apply(localNet, opts...); err != nil {
		return nil, err
	}
	return localNet, nil
}

// Apply options.
func Apply(ln *libp2pnet.LibP2pNet, opts ...NetOption) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(ln); err != nil {
			return err
		}
	}
	return nil
}
