package engine

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"chainmaker.org/chainmaker/common/v2/msgbus"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/status"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/consensus-utils/v2/consistent_service"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"

	"github.com/gogo/protobuf/proto"
)

const (
	// MAXBFT_LOCAL_NODE_STATUS defines local node status of the consistent engine
	MAXBFT_LOCAL_NODE_STATUS = iota

	// MAXBFT_REMOTE_NODE_STATUS defines remote node status of the consistent engine
	MAXBFT_REMOTE_NODE_STATUS
)

// ID Identifies the node ID in the consistent service
func (e *Engine) ID() string {
	return e.nodeId
}

// TimePattern The timing set in the consistent service
func (e *Engine) TimePattern() interface{} {
	return e.broadcastTiming
}

// PreBroadcaster Preprocessing of a node's broadcast message, returning data to be broadcast
func (e *Engine) PreBroadcaster() consistent_service.Broadcast {
	return func(localInfo consistent_service.Node, peerInfo consistent_service.Node) (interface{}, error) {
		var (
			localStatus *status.NodeStatus
			peerStatus  *status.NodeStatus
			ok          bool
		)
		if localStatus, ok = localInfo.Statuses()[MAXBFT_LOCAL_NODE_STATUS].Data().(*status.NodeStatus); !ok {
			return nil, fmt.Errorf("invalid node status")
		}
		if peerStatus, ok = peerInfo.Statuses()[MAXBFT_REMOTE_NODE_STATUS].Data().(*status.NodeStatus); !ok {
			return nil, fmt.Errorf("invalid node status")
		}

		// 2. broadcasts the status of the current node
		msgs := make([]*net.NetMsg, 0, 1)
		msgs = append(msgs, &net.NetMsg{
			To:   peerStatus.NodeID,
			Type: net.NetMsg_CONSISTENT_MSG,
			Payload: utils.MustMarshal(
				&maxbft.NodeStatus{
					NodeId: e.nodeId,
					Height: localStatus.Height,
					View:   localStatus.View,
					Epoch:  localStatus.Epoch,
				}),
		})

		// 3. After the node status is updated for the first time,
		// the broadcast time of the local node is recorded
		if e.hasFirstSyncedStatus.Load() {
			e.lastBroadcastTime[peerInfo.ID()] = time.Now()
		}
		e.log.DebugDynamic(func() string {
			return fmt.Sprintf("broadcast status [%s:%d:%d:%d] in local node",
				localStatus.NodeID, localStatus.Epoch, localStatus.Height, localStatus.View)
		})
		return msgs, nil
	}
}

// Run The status broadcast service is started
func (e *Engine) Run() {
	e.statusRunning = true
}

// IsRunning Indicates whether the status broadcast service is running
func (e *Engine) IsRunning() bool {
	return e.statusRunning
}

// HandleStatus Update local state with information received from other nodes
func (e *Engine) HandleStatus() {
	//1. Calculate the lowest height of top F +1 node according to the status of other nodes
	localHeight := e.forest.GetGenericHeight()
	nodesStatus := make([]*status.NodeStatus, 0, len(e.remotes))
	higherNodes := make([]*RemoteNodeInfo, 0, len(e.remotes))
	for _, info := range e.remotes {
		if info.Height > localHeight {
			higherNodes = append(higherNodes, info)
		}
	}
	// if there are some validators higher then the local node in block height,
	// we pick one at random from them, and request the proposals we not have
	if len(higherNodes) > 0 {
		b := new(big.Int).SetInt64(int64(len(higherNodes)))
		i, err := rand.Int(rand.Reader, b)
		randIndex := uint64(0)
		if err != nil {
			e.log.Debugf("get rand number failed, use 0. error: %+v", err)
		} else {
			randIndex = i.Uint64()
		}
		e.requester.SendRequest(higherNodes[randIndex].NodeID, &maxbft.ProposalFetchMsg{
			Height:  higherNodes[randIndex].Height,
			View:    0,
			BlockId: []byte{},
		})
		return
	}
	for _, info := range e.remotes {
		nodesStatus = append(nodesStatus,
			status.NewNodeStatus(info.NodeID, info.Height, info.View, info.Epoch))
		e.log.DebugDynamic(func() string {
			return fmt.Sprintf("peer: %s, epoch:%d, height:%d, view: %d",
				info.NodeID, info.Epoch, info.Height, info.View)
		})
	}
	algoCollector := status.NewNodeStatusCollector(
		localHeight, utils.GetMiniNodes(len(e.nodes)), nodesStatus)
	commonState, err := algoCollector.LoadMinView()
	if err != nil {
		e.log.Warnf("load min view failed: %+v", err)
		return
	}

	//2. Writes the state result by computed to a channel
	e.log.DebugDynamic(func() string {
		return fmt.Sprintf("common state [%d:%d:%d] in remote peers from consistent service ",
			commonState.Epoch, commonState.Height, commonState.View)
	})
	e.handleRemoteStatus(commonState)
}

// ========================= node status =========================

// ------------------------- local node status ------------------------

// Type Returns the type of node status
func (e *Engine) Type() int8 {
	return MAXBFT_LOCAL_NODE_STATUS
}

// Data Return the status of the local node
func (e *Engine) Data() interface{} {
	s := &status.NodeStatus{
		Height: e.forest.GetGenericHeight(),
		View:   e.paceMaker.CurView(),
		NodeID: e.nodeId,
		Epoch:  e.epochId,
	}
	return s
}

// Update local node status with remote node information
func (e *Engine) Update(remoteStatus consistent_service.Status) {
	// Update the local status only when the view status is lower than the common node.
	var (
		localStatus *status.NodeStatus
		rStatus     *status.NodeStatus
		ok          bool
	)
	if localStatus, ok = e.Data().(*status.NodeStatus); !ok {
		e.log.Warnf("invalid node status")
		return
	}
	if rStatus, ok = remoteStatus.Data().(*status.NodeStatus); !ok {
		e.log.Warnf("invalid node status")
		return
	}
	if localStatus.Epoch != rStatus.Epoch {
		return
	}
	if localStatus.Height != rStatus.Height {
		return
	}
	if localStatus.View >= rStatus.View {
		return
	}

	if !e.hasFirstSyncedStatus.CAS(false, true) {
		// indicates the non-first update status, updates are only made when the view
		// state interval is greater than the threshold, avoiding the impact of
		// frequent updates using non-consensus logic on normal processes
		if rStatus.View-localStatus.View < uint64(e.updateViewInterval) {
			return
		}
	}
	// Update the view status of the local node
	updated := e.paceMaker.UpdateWithView(rStatus.View)
	if updated {
		e.startNewView()
		e.log.DebugDynamic(func() string {
			return fmt.Sprintf("status has been update by the msgs from consistent_service service, "+
				"local status: [%d:%d:%d]", localStatus.Epoch, localStatus.Height, localStatus.View)
		})
	}
}

// ------------------------- peer node status ------------------------

// RemotePeerStatus defines remote peer status, implements Status interface of the consistent engine
type RemotePeerStatus struct {
	*status.NodeStatus
	local *Engine
}

// NewRemotePeer initials and returns a RemotePeerStatus object
func NewRemotePeer(msg *maxbft.NodeStatus, local *Engine) *RemotePeerStatus {
	return &RemotePeerStatus{
		NodeStatus: &status.NodeStatus{
			Height: msg.Height,
			View:   msg.View,
			NodeID: msg.NodeId,
			Epoch:  msg.Epoch,
		},
		local: local,
	}
}

// Type returns the remote node status of the consistent engine
func (r *RemotePeerStatus) Type() int8 {
	return MAXBFT_REMOTE_NODE_STATUS
}

// Data returns the status data
func (r *RemotePeerStatus) Data() interface{} {
	return r.NodeStatus
}

// Update updates local status by the specified node status
func (r *RemotePeerStatus) Update(nodeStatus consistent_service.Status) {
	var (
		nowStatus *status.NodeStatus
		ok        bool
	)
	if nowStatus, ok = nodeStatus.Data().(*status.NodeStatus); !ok {
		return
	}
	if nowStatus.NodeID != r.NodeID {
		return
	}
	if nowStatus.View > r.View {
		r.View = nowStatus.View
	}
	if nowStatus.Height > r.Height {
		r.Height = nowStatus.Height
	}
	if nowStatus.Epoch > r.Epoch {
		r.Epoch = nowStatus.Epoch
	}
	localStatus, ok := r.local.Statuses()[MAXBFT_LOCAL_NODE_STATUS].Data().(*status.NodeStatus)
	if !ok {
		return
	}
	if nowStatus.Epoch > localStatus.Epoch {
		r.local.statusEventCh <- NewStartSyncService()
	}
}

// =========================== Node Info ==============================

// --------------------------- local node Info --------------------------

// Statuses implements Statuses of the Node interface in consistent engine for the local node
func (e *Engine) Statuses() map[int8]consistent_service.Status {
	m := make(map[int8]consistent_service.Status)
	m[MAXBFT_LOCAL_NODE_STATUS] = e
	return m
}

// UpdateStatus implements UpdateStatus of the Node interface in consistent engine for the local node
func (e *Engine) UpdateStatus(status consistent_service.Status) {
	e.Update(status)
}

// --------------------------- peer node info ---------------------------

// RemoteNodeInfo implements the Node interface in consistent engine,
// and to describe the status of the remote nodes
type RemoteNodeInfo struct {
	*RemotePeerStatus
}

// NewRemoteInfo initials and returns a RemoteNodeInfo object
func NewRemoteInfo(msg *maxbft.NodeStatus, local *Engine) *RemoteNodeInfo {
	return &RemoteNodeInfo{NewRemotePeer(msg, local)}
}

// ID returns the node Id of the remote node
func (r *RemoteNodeInfo) ID() string {
	return r.NodeID
}

// Statuses implements Statuses of the Node interface in consistent engine for a remote node
func (r *RemoteNodeInfo) Statuses() map[int8]consistent_service.Status {
	m := make(map[int8]consistent_service.Status, 1)
	m[MAXBFT_REMOTE_NODE_STATUS] = r.RemotePeerStatus
	return m
}

// UpdateStatus implements UpdateStatus of the Node interface in consistent engine for a remote node
func (r *RemoteNodeInfo) UpdateStatus(status consistent_service.Status) {
	r.RemotePeerStatus.Update(status)
}

// Decoder implements Decoder interface in consistent engine
type Decoder struct {
}

// MsgType returns the consistent message type
func (d *Decoder) MsgType() int8 {
	return int8(msgbus.RecvConsistentMsg)
}

// Decode deserialize messages for consistent engine
func (d *Decoder) Decode(arg interface{}) interface{} {
	netMsg, ok := arg.(*net.NetMsg)
	if !ok {
		return fmt.Sprintf("type of the arg should be *net.NetMsg. arg: %+v", arg)
	}

	status := &maxbft.NodeStatus{}
	switch netMsg.Type {
	case net.NetMsg_CONSISTENT_MSG:
		if err := proto.Unmarshal(netMsg.Payload, status); err != nil {
			return fmt.Sprintf("unmarshal failed. error: %+v", err)
		}
	}

	// Note: Only the status data transferred from the network is needed
	return NewRemoteInfo(status, nil)
}
