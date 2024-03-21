/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

// CachedForestNode defines a forest node
type CachedForestNode struct {
	data     *ProposalContainer
	parent   *CachedForestNode
	children []ForestNoder
}

// NewCachedForestNode initials and returns a new CachedForestNode object
func NewCachedForestNode(data *ProposalContainer) *CachedForestNode {
	return &CachedForestNode{
		data:     data,
		children: make([]ForestNoder, 0),
	}
}

// AddChild adds a child node to a forest node
func (n *CachedForestNode) AddChild(child ForestNoder) {
	if c, ok := child.(*CachedForestNode); ok && c != nil {
		n.addChild(c)
	}
}

func (n *CachedForestNode) addChild(child *CachedForestNode) {
	n.children = append(n.children, child)
	n.data.UpdateChildProposal(child.data) // 更新当前节点状态
}

// ClearChildren clears all children of a forest node
func (n *CachedForestNode) ClearChildren() {
	n.children = make([]ForestNoder, 0)
}

// SetParent sets parent node of a forest node
func (n *CachedForestNode) SetParent(parent ForestNoder) {
	if p, ok := parent.(*CachedForestNode); ok {
		n.setParent(p)
	}
}

func (n *CachedForestNode) setParent(parent *CachedForestNode) {
	n.parent = parent
}

// Parent returns the parent node of the forest node
func (n *CachedForestNode) Parent() ForestNoder {
	if n.parent == nil {
		return nil
	}
	return n.parent
}

// Data returns the data of a forest node
func (n *CachedForestNode) Data() *ProposalContainer {
	return n.data
}

// FromOtherNodes returns that the node is from other nodes' response or not
func (n *CachedForestNode) FromOtherNodes() bool {
	return n.data.fromOtherNodes
}

// Height returns the block height of a forest node
func (n *CachedForestNode) Height() uint64 {
	return n.data.Height()
}

// View returns the view of a forest node
func (n *CachedForestNode) View() uint64 {
	return n.data.View()
}

// Children returns all children of a forest node
func (n *CachedForestNode) Children() []ForestNoder {
	return n.children
}

// Key returns the string of the block hash of the forest node
func (n *CachedForestNode) Key() string {
	return n.data.Key()
}

// ParentKey returns the string of the pre block hash of the forest node
func (n *CachedForestNode) ParentKey() string {
	return n.data.ParentKey()
}
