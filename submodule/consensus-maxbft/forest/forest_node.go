/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

// ForestNoder defines a forest node
type ForestNoder interface {
	// AddChild 添加子节点
	AddChild(child ForestNoder)

	// ClearChildren 清理所有子节点
	ClearChildren()

	// SetParent 设置当前节点的父节点
	// 可以设置为nil
	SetParent(parent ForestNoder)

	// Parent 返回当前节点父节点
	Parent() ForestNoder

	// Data 返回节点中存储的数据
	Data() *ProposalContainer

	// Children 返回节点所有的子节点集合
	Children() []ForestNoder

	// Key 返回当前节点的Key
	Key() string

	// ParentKey 返回节点的父Key
	ParentKey() string

	Height() uint64

	View() uint64
}
