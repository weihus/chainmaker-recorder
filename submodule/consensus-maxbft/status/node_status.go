/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package status

// NodeStatus 节点状态
type NodeStatus struct {
	NodeID string // 节点ID
	Epoch  uint64
	Height uint64 // 当前节点所在的高度
	View   uint64 // 当前节点所在的视图ID
}

// NewNodeStatus 创建节点状态
func NewNodeStatus(nodeID string, height, view, epoch uint64) *NodeStatus {
	return &NodeStatus{
		NodeID: nodeID,
		Height: height,
		View:   view,
		Epoch:  epoch,
	}
}

// GetNodeID returns the node id of the node status
func (s *NodeStatus) GetNodeID() string {
	return s.NodeID
}

// GetHeight returns the current block height of the node status
func (s *NodeStatus) GetHeight() uint64 {
	return s.Height
}

// GetView returns the current view of the node status
func (s *NodeStatus) GetView() uint64 {
	return s.View
}

// SetHeight sets the node status height
func (s *NodeStatus) SetHeight(height uint64) {
	s.Height = height
}

// SetView sets the node status view
func (s *NodeStatus) SetView(view uint64) {
	s.View = view
}

// SetStatus sets the node status height and view
func (s *NodeStatus) SetStatus(height, view uint64) {
	s.Height = height
	s.View = view
}
