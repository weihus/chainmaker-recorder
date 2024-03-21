/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgcache

import (
	"fmt"
	"sync"
	"time"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/compensator"
	"chainmaker.org/chainmaker/consensus-maxbft/v2/forest"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"
)

var (
	cacheCapacity   = 64
	pendingCapacity = 1024
	maxWaitTime     = 2 * time.Second

	addProposalTimeoutErrorFormat = "add proposal[%d:%d:%x] to handle channel timeout"
	viewLessErrorFormat           = "proposal[%d:%d:%x] is less than the final view[%d]"
	proposalNotExistErrorFormat   = "ancestor proposal[%d:%d:%x] by check-key[%x] is not exist"
)

var _ MsgTransmitter = (*CachedMsgTransmitter)(nil)

// PendingProposal packages the maxbft.ProposalData
type PendingProposal struct {
	// FromOtherNodes marks that the proposal is from other nodes' responses or not
	FromOtherNodes bool

	// Proposal is the raw proposal
	Proposal *maxbft.ProposalData

	// Sender indicates witch node the proposal is from
	Sender string

	// Qc is the qc of the proposal
	Qc *maxbft.QuorumCert
}

// CachedMsgTransmitter defines a message transmitter between forest and consensus engine
type CachedMsgTransmitter struct {
	sync.RWMutex // 读写锁，用户后续有查询需求

	// 基础属性检查
	baseCheck func(data *maxbft.ProposalData, qc *maxbft.QuorumCert) error

	cachedHashNodes   *ForestNodes                   // 缓存所有的节点，key:value => hash:value
	cachedHeightNodes map[uint64]*ForestNodes        // 缓存指定高度的节点信息，key:value => height:[hash:value]
	cachedForest      forest.Forester                // forest
	engineChannel     chan<- *PendingProposal        // 只写的channel
	reqCompensator    compensator.RequestCompensator // 请求服务
	logger            protocol.Logger

	pendingProposals   chan *PendingProposal // 提案处理缓存
	hasClosedPendingCh bool
}

// NewMsgCacheHandler initials and returns a new CachedMsgTransmitter object
func NewMsgCacheHandler(cachedForest forest.Forester,
	baseCheck func(data *maxbft.ProposalData, qc *maxbft.QuorumCert) error,
	engineChannel chan<- *PendingProposal,
	reqCompensator compensator.RequestCompensator,
	logger protocol.Logger,
) *CachedMsgTransmitter {
	handler := &CachedMsgTransmitter{
		baseCheck:         baseCheck,
		cachedForest:      cachedForest,
		cachedHashNodes:   NewForestNodes(),
		cachedHeightNodes: make(map[uint64]*ForestNodes, cacheCapacity),
		pendingProposals:  make(chan *PendingProposal, pendingCapacity),
		engineChannel:     engineChannel,
		reqCompensator:    reqCompensator,
		logger:            logger,
	}
	// 注册回调
	cachedForest.RegisterFinalEventFunc(handler.FinalEventFn())
	cachedForest.RegisterAddedEventFunc(handler.AddEventFn())
	return handler
}

// FinalEventFn 当产生final事件时的回调函数
func (h *CachedMsgTransmitter) FinalEventFn() forest.FinalEventFunc {
	finalEventFn := func(proposal *forest.ProposalContainer) {
		// 区块被提交时回调，proposal是最新提交的区块
		// 那么对应的比这个高度低的区块都需要被裁剪
		// 需要加锁
		h.Lock()
		defer h.Unlock()
		commitHeight := proposal.Height()
		h.logger.DebugDynamic(func() string {
			return fmt.Sprintf("trigger final event to cut node by commit height[%d]", commitHeight)
		})
		h.cutNodesByHeight(commitHeight)
	}
	return finalEventFn
}

// AddEventFn 当触发添加事件时的处理流程
// 核心处理是判断是否有串联需要处理的提案，如有则处理，用作一种补偿机制
func (h *CachedMsgTransmitter) AddEventFn() forest.AddedEventFunc {
	addEventFn := func(proposal *forest.ProposalContainer) {
		h.Lock()
		defer h.Unlock()
		// 检查对应的子区块是否有可以调用的，可以调用来处理
		addHeight, addProposalHash := proposal.Height(), proposal.Key()
		h.logger.DebugDynamic(func() string {
			return fmt.Sprintf("trigger add event to handle propose[%d:%x]", addHeight, addProposalHash)
		})
		if currNodes, exist := h.cachedHeightNodes[addHeight+1]; exist {
			ns := currNodes.GetNodesByParentKey(addProposalHash)
			if len(ns) > 0 {
				for _, n := range ns {
					// 处理该hash值
					h.handleProposalsByHash(n.Key())
				}
			}
		}
	}
	return addEventFn
}

// cutNodesByHeight 裁剪节点
func (h *CachedMsgTransmitter) cutNodesByHeight(commitHeight uint64) {
	for height, forestNodes := range h.cachedHeightNodes {
		if height <= commitHeight {
			// 需要处理，首先遍历节点，删除对应的数据
			for _, node := range forestNodes.nodes {
				h.cachedHashNodes.DelNode(node.Key())
			}
			// 再删除自己
			delete(h.cachedHeightNodes, height)
		}
	}
	// commitHeight + 1的节点进行父节点重置
	if forestNodes, exist := h.cachedHeightNodes[commitHeight+1]; exist {
		for _, node := range forestNodes.nodes {
			// 理论上不需要再对cacheHash进行设置
			node.SetParent(nil)
		}
	}
}

// FinalView returns the finalView of the transmitter
func (h *CachedMsgTransmitter) FinalView() uint64 {
	return h.cachedForest.FinalView()
}

// HandleProposal 处理proposal，会默认进行一次视图判断
func (h *CachedMsgTransmitter) HandleProposal(proposal *PendingProposal) error {
	p := proposal.Proposal
	if p == nil {
		return fmt.Errorf("HandleProposal failed: nil proposal")
	}
	height := p.Block.Header.BlockHeight
	// 首先进行一次视图判断
	if proposal.Proposal.View <= h.FinalView() {
		err := fmt.Errorf(viewLessErrorFormat, height, p.View, p.Block.Hash(), h.FinalView())
		h.logger.Warnf("check the proposal error : %v", err)
		return err
	}

	h.RLock()
	if h.hasClosedPendingCh {
		return nil
	}
	defer h.RUnlock()
	// 最长等待
	t := time.NewTimer(maxWaitTime)
	defer t.Stop()
	select {
	case h.pendingProposals <- proposal:
		return nil
	case <-t.C:
		return fmt.Errorf(addProposalTimeoutErrorFormat, height, p.View, p.Block.Hash())
	}
}

// Start 启动后台处理提案服务
func (h *CachedMsgTransmitter) Start() {
	// 启动pending处理器
	h.pendingStart()
}

// Stop stops the transmitter
func (h *CachedMsgTransmitter) Stop() error {
	h.Lock()
	if h.hasClosedPendingCh {
		return nil
	}
	h.hasClosedPendingCh = true
	h.Unlock()

	// 关闭channel，清空资源
	close(h.pendingProposals)
	return nil
}

// SetForest sets the forest of the msgCache
func (h *CachedMsgTransmitter) SetForest(fork forest.Forester) {
	h.cachedForest = fork
}

func (h *CachedMsgTransmitter) pendingStart() {
	go func() {
		for proposal := range h.pendingProposals {
			p := proposal.Proposal
			height := p.Block.Header.BlockHeight
			h.logger.DebugDynamic(func() string {
				return fmt.Sprintf("start handle proposal[%d:%d:%x]", height, p.View, p.Block.Hash())
			})
			if err := h.handleProposal(proposal); err != nil {
				h.logger.Warnf("handle proposal[%d:%d:%x] error : %v", height, p.View, p.Block.Hash(), err)
			}
		}
	}()
	h.logger.Info("pending started wait for handling proposals")
}

// handleProposal 处理channel中取出的proposal
func (h *CachedMsgTransmitter) handleProposal(proposal *PendingProposal) error {
	h.Lock()
	defer h.Unlock()
	p := proposal.Proposal
	// 再次进行视图判断
	if p.View <= h.FinalView() {
		return fmt.Errorf(viewLessErrorFormat, p.Block.Header.BlockHeight,
			p.View, p.Block.Hash(), h.FinalView())
	}
	// 将节点串联至整个缓存中
	currNode := forest.NewCachedForestNode(forest.NewProposalContainer(p, false, proposal.FromOtherNodes))
	if proposal.Qc != nil {
		currNode.Data().BlockContainer().SetQC(proposal.Qc)
	}
	exist := h.innerSeriesConnect(currNode)
	// 检查是否可以被直接处理
	missingProposal, err := h.canProcessNode(currNode)
	// 如果不缺少前序提案，且该提案是第一次收到，则处理这一系列提案，如果该提案不是第一次收到，则不再单独处理
	if err == nil && !exist {
		// 表示可以从processStartHash开始处理所有关联的
		h.handleProposalsByHash(missingProposal.BlockId)
		return nil
	}
	if missingProposal != nil && h.reqCompensator != nil {
		// 表示有区块需要获取，此处发送消息获取
		// 如果该提案是向其他节点拉取过来的，则继续向该节点拉取前序区块
		// 反之，如果该提案是主节点产生后直接广播的，则向该提案的提案者拉取前序区块
		var msgTo string
		if proposal.Sender != "" {
			msgTo = proposal.Sender
		} else {
			msgTo = p.Proposer
		}
		h.reqCompensator.SendRequest(msgTo, &maxbft.ProposalFetchMsg{
			BlockId: []byte(missingProposal.BlockId),
			Height:  missingProposal.BlockHeight,
			View:    missingProposal.View,
		})
		h.logger.Warnf("handle proposal[%d:%d:%x] failed: %v",
			p.Block.Header.BlockHeight, p.View, p.Block.Hash(), err)
	}
	return nil
}

// handleProposalsByHash 递归函数，会处理当前节点及其子节点的信息
func (h *CachedMsgTransmitter) handleProposalsByHash(hash string) {
	// 获取对应的节点
	startNode := h.cachedHashNodes.GetNode(hash)
	if startNode == nil {
		h.logger.Errorf("node is nil in cache which hash = %s", hash)
		return
	}
	// 处理该节点
	h.toEngine(startNode)
	// 删除对应的节点
	h.delNode(startNode)
	// 重置其父节点，防止异常
	startNode.SetParent(nil)
	children := startNode.Children()
	if len(children) > 0 {
		// 处理其所有的子节点
		for _, childNode := range children {
			h.handleProposalsByHash(childNode.Key())
		}
	}
	// 处理完成后，清空其子节点集合
	startNode.ClearChildren()
}

// toEngine 交由引擎调度处理
func (h *CachedMsgTransmitter) toEngine(currNode *forest.CachedForestNode) {
	h.engineChannel <- &PendingProposal{
		Proposal:       currNode.Data().ProposalData(),
		FromOtherNodes: currNode.FromOtherNodes(),
		Qc:             currNode.Data().BlockContainer().GetQC(),
	}
}

// delNode 从缓存中删除节点
func (h *CachedMsgTransmitter) delNode(currNode *forest.CachedForestNode) {
	// 需要删除两部分缓存
	h.cachedHashNodes.DelNode(currNode.Key())
	// 再删除对应高度中的信息
	if cacheNodes, exist := h.cachedHeightNodes[currNode.Data().Height()]; exist {
		cacheNodes.DelNode(currNode.Key())
	}
}

// innerSeriesConnect 在cache内部将可以连接的节点连接起来，返回该对象是否已存在
func (h *CachedMsgTransmitter) innerSeriesConnect(currNode *forest.CachedForestNode) bool {
	// 进行内部串联
	blkHash, parentBlkHash := currNode.Data().Key(), currNode.Data().ParentKey()
	currHeight := currNode.Data().Height()
	// 判断是否已经加入过该对象
	if h.cachedHashNodes.ExistNode(blkHash) {
		// 该对象已存在
		return true
	}
	// 实际加入到内存中
	h.cachedHashNodes.AddNode(currNode)
	// 加入到高度对应的内存中
	if nodes, ex := h.cachedHeightNodes[currHeight]; ex {
		nodes.AddNode(currNode)
	} else {
		nodes = NewForestNodes()
		nodes.AddNode(currNode)
		h.cachedHeightNodes[currHeight] = nodes
	}
	// 获取该节点对应的前置区块
	if parentBlk := h.cachedHashNodes.GetNode(parentBlkHash); parentBlk != nil {
		// 为该父节点添加一个子节点
		parentBlk.AddChild(currNode)
		// 更新当前节点对应的父节点
		currNode.SetParent(parentBlk)
	}
	// 获取该节点可能对应的子节点集合
	childHeight := currHeight + 1
	if possibleChildren, exist := h.cachedHeightNodes[childHeight]; exist {
		// 如果可能存在的话，从其中获取其对应的父区块hash为当前节点的集合
		realChildren := possibleChildren.GetNodesByParentKey(blkHash)
		if len(realChildren) > 0 {
			// 确定存在的话，设置下相互关联信息
			for _, realChild := range realChildren {
				realChild.SetParent(currNode)
				currNode.AddChild(realChild)
			}
		}
	}
	return false
}

// canProcessNode 是否可以处理对应的节点
// 返回值：error：nil表示可处理，从string开始；非nil表示有父节点数据不存在，需要广播获取，其hash为string
func (h *CachedMsgTransmitter) canProcessNode(currNode *forest.CachedForestNode) (*MissingProposal, error) {
	// 先获取该节点的父节点，直至最父级
	ancestorNode := ancestorNode(currNode)
	// 判断该节点的父节点是否存在
	if h.cachedForest.HasProposal(ancestorNode.ParentKey()) {
		// 表明该节点对应父节点存在，则返回其hash，后续会根据该hash来处理所有的串联节点
		return NewMissingProposal(ancestorNode.Data().Height(), ancestorNode.Data().View(), ancestorNode.Key()), nil
	}
	// 该节点对应的父节点不存在的话，就需要广播获取一下
	// 该节点中的提案中的QC描述的即缺少的提案
	var (
		height    = ancestorNode.Data().GetProposalQCHeight()
		view      = ancestorNode.Data().GetProposalQCView()
		parentKey = ancestorNode.ParentKey()
	)
	return NewMissingProposal(height, view, parentKey), fmt.Errorf(proposalNotExistErrorFormat,
		height, view, parentKey, currNode.Key())
}

func ancestorNode(currNode forest.ForestNoder) forest.ForestNoder {
	loopNode := currNode
	for {
		parent := loopNode.Parent()
		if parent == nil || parent.Data() == nil {
			break
		}
		loopNode = loopNode.Parent()
	}
	return loopNode
}

// MissingProposal 缺失的提案信息
type MissingProposal struct {
	BlockHeight uint64
	View        uint64
	BlockId     string
}

// NewMissingProposal initials and returns a new MissingProposal object
func NewMissingProposal(height, view uint64, hash string) *MissingProposal {
	return &MissingProposal{
		BlockHeight: height,
		View:        view,
		BlockId:     hash,
	}
}

// ForestNodes 节点集合，用于存储节点map
type ForestNodes struct {
	nodes map[string]*forest.CachedForestNode
}

// NewForestNodes returns a new ForestNodes object
func NewForestNodes() *ForestNodes {
	return &ForestNodes{
		nodes: make(map[string]*forest.CachedForestNode, cacheCapacity),
	}
}

// AddNode adds a node to ForestNodes
func (ns *ForestNodes) AddNode(node *forest.CachedForestNode) {
	ns.nodes[node.Key()] = node
}

// GetNode finds a node by the specified key and returns it
func (ns *ForestNodes) GetNode(key string) *forest.CachedForestNode {
	if n, exist := ns.nodes[key]; exist {
		return n
	}
	return nil
}

// ExistNode check a node with specified key was exist or not
func (ns ForestNodes) ExistNode(key string) bool {
	if _, exist := ns.nodes[key]; exist {
		return true
	}
	return false
}

// GetNodesByParentKey gets nodes by parent key
func (ns *ForestNodes) GetNodesByParentKey(parentKey string) []*forest.CachedForestNode {
	// 考虑到实际情况，不太可能同时存在太多分叉，因此直接使用数组来处理
	nodes := make([]*forest.CachedForestNode, 0)
	for _, node := range ns.nodes {
		var n = node
		if n.ParentKey() == parentKey {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// DelNode deletes a node with the specified key from nodes
func (ns *ForestNodes) DelNode(key string) {
	delete(ns.nodes, key)
}

// AllNodes returns all nodes
func (ns *ForestNodes) AllNodes() map[string]*forest.CachedForestNode {
	return ns.nodes
}
