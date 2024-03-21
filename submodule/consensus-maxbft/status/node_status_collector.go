/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package status

import (
	"fmt"
	"sort"
)

// NodeStatusCollector 节点状态收集器
type NodeStatusCollector struct {
	requireCommonNodeNum int
	localHeight          uint64
	Remotes              []*NodeStatus
}

// NewNodeStatusCollector 创建节点状态收集器
// local不允许为nil
func NewNodeStatusCollector(localHeight uint64, requireMinNode int, remotes []*NodeStatus) *NodeStatusCollector {
	return &NodeStatusCollector{
		requireCommonNodeNum: requireMinNode,
		localHeight:          localHeight,
		Remotes:              remotes,
	}
}

// LoadMinView 返回最小的视图ID，根据本地节点的高度
func (c *NodeStatusCollector) LoadMinView() (*NodeStatus, error) {
	return c.loadMinViewByHeight(c.localHeight)
}

func (c *NodeStatusCollector) loadMinViewByHeight(height uint64) (*NodeStatus, error) {
	// 首先获取节点中所有高度为height的节点集合
	nodes := make([]uint64, 0)
	for _, nodeStatus := range c.Remotes {
		if nodeStatus.Height == height {
			nodes = append(nodes, nodeStatus.View)
		}
	}
	if len(nodes) < c.requireCommonNodeNum {
		// 相同高度的数量无法满足需求，返回错误
		return nil, fmt.Errorf("the size is not enough for height[%d]", height)
	}
	// 满足的情况下，进行处理
	// 首先进行排序
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	// 排序后，选择最后的size个中的最小值即可
	return &NodeStatus{
		Epoch:  c.Remotes[len(nodes)-c.requireCommonNodeNum].Epoch,
		Height: c.localHeight,
		View:   nodes[len(nodes)-c.requireCommonNodeNum],
	}, nil
}
