/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package routing

import (
	"context"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

// PeerRouting provides a way to query the net address of the peer in network.
type PeerRouting interface {
	FindPeer(ctx context.Context, service string, targetPeerId peer.ID, timeout time.Duration) (peer.ID, error)
}

// ProtocolRouting provides a way to query the net addresses of the peers who support protocols given.
type ProtocolRouting interface {
	FindPeerSupportProtocolsAsync(context.Context, int, ...protocol.ID) <-chan ma.Multiaddr
}

// Routing .
type Routing interface {
	PeerRouting
	ProtocolRouting
}
