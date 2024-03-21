/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"chainmaker.org/chainmaker/net-common/utils"
	"chainmaker.org/chainmaker/protocol/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// SetupDiscovery setup a discovery service.
func SetupDiscovery(host *LibP2pHost, readySignalC chan struct{}, enableDHTBootstrapProvider bool, bootstraps []string, log protocol.Logger) error {
	log.Info("[Discovery] discovery setting...")
	bootstrapAddrInfos, err := utils.ParseAddrInfo(bootstraps)
	if err != nil {
		return err
	}

	// set high level peer
	for _, bootstrapAddrInfo := range bootstrapAddrInfos {
		host.connManager.AddAsHighLevelPeer(bootstrapAddrInfo.ID)
	}

	var mode dht.ModeOpt
	// is enable bootstrap mode
	if enableDHTBootstrapProvider {
		log.Info("[Discovery] dht will be created with server-mode.")
		mode = dht.ModeServer
	} else {
		log.Info("[Discovery] dht will be created with client-mode.")
		mode = dht.ModeClient
	}

	options := []dht.Option{dht.Mode(mode)}
	//if len(bootstraps) > 0 {
	//	options = append(options, dht.BootstrapPeers(bootstraps...))
	//}
	ctx := host.Context()
	h := host.Host()
	// new kademlia DHT
	host.peerDHT, err = dht.New(
		ctx,
		h,
		options...)
	if err != nil {
		log.Infof("[Discovery] create dht failed,%s", err.Error())
		return err
	}
	// set as bootstrap
	if err = host.peerDHT.Bootstrap(ctx); err != nil {
		return err
	}
	// new ConnSupervisor
	host.connSupervisor = newConnSupervisor(host, bootstrapAddrInfos, log)
	// start supervising.
	host.connSupervisor.startSupervising(readySignalC)

	log.Info("[Discovery] discovery set up.")
	return nil
}
