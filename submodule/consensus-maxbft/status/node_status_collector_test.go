/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package status

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewNodeStatus(t *testing.T) {
	statusC := NewNodeStatusCollector(1024, 3,
		[]*NodeStatus{
			NewNodeStatus("node1", 1024, 1456, 2),
			NewNodeStatus("node2", 1024, 1356, 2),
			NewNodeStatus("node3", 1024, 1534, 2),
		})

	minView, err := statusC.LoadMinView()
	require.Nil(t, err)
	require.Equal(t, uint64(1356), minView.View)
	require.EqualValues(t, 2, minView.Epoch)
}

func TestNewNodeStatusCollector(t *testing.T) {
	statusC := NewNodeStatusCollector(1024, 3, []*NodeStatus{
		NewNodeStatus("node1", 1024, 1456, 1),
		NewNodeStatus("node2", 1024, 1356, 1),
		NewNodeStatus("node3", 1024, 1534, 1),
		NewNodeStatus("node4", 1024, 1233, 1),
		NewNodeStatus("node5", 1024, 1507, 1),
		NewNodeStatus("node6", 1024, 1423, 1),
	})
	minView, err := statusC.LoadMinView()
	require.Nil(t, err)
	require.Equal(t, uint64(1456), minView.View)
	require.EqualValues(t, 1, minView.Epoch)
}
