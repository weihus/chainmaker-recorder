/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package forest

import (
	"fmt"
	"sync"
	"time"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/protocol/v2"
)

var (
	threeChain      = 3
	addedChanLength = 1024
	finalChanLength = 512

	parentProposalNotExistErrorFormat = "the parent proposal[%x:%x] is not exist"
	keyProposalExistedErrorFormat     = "the proposal by key[%x] is exist"
	viewProposalExistedErrorFormat    = "the proposal by view[%d] is exist"
	proposalNotExistErrorFormat       = "the proposal[%x] is not exist"
	proposalHeightWrongErrorFormat    = "current proposal[%x:%d] is wrong with parent[%x:%d]"
)

var _ Forester = (*CachedForest)(nil)

// FinalEventFunc 有提交事件发生时，回调的监听事件处理器
type FinalEventFunc func(proposal *ProposalContainer)

// AddedEventFunc 有添加提案事件发生时，回调的监听事件处理器
type AddedEventFunc func(proposal *ProposalContainer)

// CachedForest 基于cache实现的forest
type CachedForest struct {
	sync.RWMutex
	root          ForestNoder             // 根节点
	nodeCache     *InnerNodeCache         // 所有节点缓存
	commiter      protocol.BlockCommitter // 提交block接口
	lockedQC      *BlockContainer         // 锁定的QC
	finalQC       *BlockContainer         // 最新提交的QC
	genericQC     *maxbft.QuorumCert      // 下次生成时需要使用的QC
	finalEventFns []FinalEventFunc        // final-event函数集合
	addEventFns   []AddedEventFunc        // add-event函数集合
	logger        protocol.Logger

	genericHeight  uint64    // 当前最大区块高度
	lastUpdateTime time.Time // 最近一次更新genericQC的时间

	closeCh               chan struct{}
	addedProposalDataChan chan *ProposalContainer // 添加完成的提案回调通道
	finalProposalDataChan chan *ProposalContainer // final完成的提案回调通道
}

// NewForest initials and returns a new CachedForest object
func NewForest(root *CachedForestNode, commiter protocol.BlockCommitter, logger protocol.Logger) *CachedForest {
	forest := &CachedForest{
		root:                  root,
		nodeCache:             NewInnerNodeCache(),
		addedProposalDataChan: make(chan *ProposalContainer, addedChanLength),
		finalProposalDataChan: make(chan *ProposalContainer, finalChanLength),
		commiter:              commiter,
		finalEventFns:         make([]FinalEventFunc, 0),
		addEventFns:           make([]AddedEventFunc, 0),
		logger:                logger,
		closeCh:               make(chan struct{}),
	}
	forest.nodeCache.AddNode(root)
	// 初始化qc
	forest.initQCs()
	return forest
}

// Stop Close the channel to exit goroutine inside the component
func (f *CachedForest) Stop() {
	close(f.closeCh)
}

//Start the service in the background
func (f *CachedForest) Start() {
	// 启动pending
	f.pendingListening()
}

// RegisterFinalEventFunc 注册Final区块事件函数
func (f *CachedForest) RegisterFinalEventFunc(fn FinalEventFunc) {
	f.finalEventFns = append(f.finalEventFns, fn)
}

// RegisterAddedEventFunc 注册添加区块事件函数
func (f *CachedForest) RegisterAddedEventFunc(fn AddedEventFunc) {
	f.addEventFns = append(f.addEventFns, fn)
}

// GetGenericQC 返回GenericQC
func (f *CachedForest) GetGenericQC() *maxbft.QuorumCert {
	return f.genericQC
}

// GetGenericHeight 返回forest当前缓存的最高提案高度
func (f *CachedForest) GetGenericHeight() uint64 {
	return f.genericHeight
}

// GetFinalQC 返回finalQC
func (f *CachedForest) GetFinalQC() *BlockContainer {
	return f.finalQC
}

// GetLockedQC 返回LockedQC
func (f *CachedForest) GetLockedQC() *BlockContainer {
	return f.lockedQC
}

// FinalView 返回FinalView
func (f *CachedForest) FinalView() uint64 {
	return f.finalQC.View()
}

// HasProposal 判断指定Proposal是否存在
func (f *CachedForest) HasProposal(blockID string) bool {
	return f.nodeExist(blockID)
}

// GetProposalByView 根据视图获取对应的提案，不存在则返回nil
func (f *CachedForest) GetProposalByView(view uint64) *maxbft.ProposalData {
	if n := f.nodeCache.NodeByView(view); n != nil {
		return n.Data().proposal
	}
	return nil
}

func (f *CachedForest) nodeExist(key string) bool {
	if n := f.nodeCache.NodeByHash(key); n != nil {
		return true
	}
	return false
}

// Node 获取指定节点
func (f *CachedForest) Node(key string) ForestNoder {
	return f.node(key)
}

// node 获取指定节点
func (f *CachedForest) node(key string) ForestNoder {
	return f.nodeCache.NodeByHash(key)
}

// Root 返回根节点
func (f *CachedForest) Root() ForestNoder {
	return f.root
}

// UpdateStatesByQC 使用QC更新状态
func (f *CachedForest) UpdateStatesByQC(qc *maxbft.QuorumCert) error {
	// 1. 添加QC到它自身对应的区块中
	if err := f.addQC(qc); err != nil {
		f.logger.Errorf("add QC error : %v", err)
		return err
	}

	// 2. 尝试更新genericQC
	f.updateGenericQC(qc)
	return nil
}

// UpdateStatesByProposal 使用提案携带的QC更新forest状态
func (f *CachedForest) UpdateStatesByProposal(proposal *maxbft.ProposalData, validBlock bool) (
	[]*ProposalContainer, error) {
	// 创建临时节点
	proposalContainer := NewProposalContainer(proposal, false, true)
	currNode := NewCachedForestNode(proposalContainer)
	parent := f.nodeCache.NodeByHash(currNode.ParentKey())
	if parent == nil {
		return nil, fmt.Errorf(parentProposalNotExistErrorFormat, currNode.Key(), parent)
	}
	currNode.SetParent(parent)

	// 使用该区块，提交符合three-chain的区块
	committedNodes := f.commitNode(currNode)

	// 尝试更新genericQC
	f.updateGenericQC(proposal.JustifyQc)

	if validBlock && proposal.Block.Header.BlockHeight > f.genericHeight {
		f.genericHeight = proposal.Block.Header.BlockHeight
	}
	return committedNodes, nil
}

// AddProposal adds proposal to forest
func (f *CachedForest) AddProposal(proposal *maxbft.ProposalData, replay bool) error {
	// 1. 添加提案至forest中
	proposalContainer := NewProposalContainer(proposal, false, true)
	currNode := NewCachedForestNode(proposalContainer)
	if err := f.addNode(currNode); err != nil {
		f.logger.Errorf("add proposal to forest error : %v", err)
		return err
	}
	// 2. 添加完成即可以进行回调，为防止回调影响正常处理，可异步进行
	if !replay {
		f.handleAddedProposal(proposalContainer)
	}
	return nil
}

// commitNode 提交节点，过程中会判断是否需要进行提交
func (f *CachedForest) commitNode(currNode ForestNoder) []*ProposalContainer {
	// 确认的核心逻辑是确认最长链，那么首先判断当前链是不是最长的链
	orderedNodes := f.loadOrderedNodes(currNode)
	// 首先判断该确认节点是否存在
	if len(orderedNodes) == 0 {
		return nil
	}
	// 更新LockedQC
	f.updateLockedQC(orderedNodes)

	// 判断其状态是否可以提交，或者是可以从哪提交，去掉最开始的根节点
	canCommitNodes := loadCanCommitNodes(orderedNodes[1:])
	if len(canCommitNodes) == 0 {
		// 表示没有可以提交的节点
		return nil
	}
	// 需要提交，提交会进行裁剪
	return f.commit(canCommitNodes)
}

// GetLatest3Ancestors Gets the last three uncommitted ancestor blocks of the node
// The number of ancestors may be less than 3 or there are no uncommitted ancestor
// blocks for current proposal
func (f *CachedForest) GetLatest3Ancestors(proposal *maxbft.ProposalData) []ForestNoder {
	parentNode := f.Node(string(proposal.Block.Header.PreBlockHash))
	if parentNode == nil {
		// Normally, parent node should exist in the consensus cache,
		//  However, if multiple blocks in the channel are executed in sequence
		// and the ancestor block fails to pass the consensus due to verification
		// failure, the parent block cannot be found during the verification of
		// the descendant block. In this case, the parent block must be terminated in advance.
		return nil
	}
	currNode := NewCachedForestNode(NewProposalContainer(proposal, false, true))
	currNode.SetParent(parentNode)

	orderedNodes := f.loadOrderedNodes(currNode)
	if len(orderedNodes) == 0 {
		return nil
	}

	noCommittedAncestors := orderedNodes[1 : len(orderedNodes)-1]
	return noCommittedAncestors
}

// initQCs 初始化QCs
func (f *CachedForest) initQCs() {
	// 无论是final、locked还是generic都是使用root的数据
	f.finalQC = f.root.Data().block
	f.lockedQC = f.root.Data().block
	f.genericQC = f.root.Data().block.qc
	f.genericHeight = f.genericQC.Votes[0].Height
	f.lastUpdateTime = time.Now()
}

// pendingListening pending监听启动
func (f *CachedForest) pendingListening() {
	go f.addedProposalListenStart()
	go f.finalProposalListenStart()
	f.logger.Info("pending started wait for handling added-proposals and final-proposals")
}

// handleAddedProposal 处理加入完成的提案
func (f *CachedForest) handleAddedProposal(proposal *ProposalContainer) {
	// 放入到channel即可，若无法放入则放弃
	select {
	case f.addedProposalDataChan <- proposal:
	default:
		f.logger.Warnf("the added proposal channel is full")
	}
}

// handleFinalProposal 处理final完成的提案
func (f *CachedForest) handleFinalProposal(proposal *ProposalContainer) {
	// 放入到channel即可，若无法放入则放弃
	select {
	case f.finalProposalDataChan <- proposal:
	default:
		f.logger.Warnf("the final proposal channel is full")
	}
}

// addedProposalListenStart 启动协程处理addedProposal消息
func (f *CachedForest) addedProposalListenStart() {
	for {
		select {
		case data := <-f.addedProposalDataChan:
			proposalContainer := data
			f.logger.DebugDynamic(func() string {
				return fmt.Sprintf("tigger event to handle added proposal[%d:%d:%x]",
					proposalContainer.proposal.Block.Header.BlockHeight, proposalContainer.proposal.View,
					proposalContainer.proposal.Block.Hash())
			})
			for _, addEventFn := range f.addEventFns {
				addEventFn(proposalContainer)
			}
		case <-f.closeCh:
			return
		}
	}
}

// finalProposalListenStart 启动协程处理finalProposal消息
func (f *CachedForest) finalProposalListenStart() {
	for {
		select {
		case data := <-f.finalProposalDataChan:
			proposalContainer := data
			f.logger.DebugDynamic(func() string {
				return fmt.Sprintf("tigger event to handle final proposal[%d:%d:%x]",
					proposalContainer.proposal.Block.Header.BlockHeight, proposalContainer.proposal.View,
					proposalContainer.proposal.Block.Hash())
			})
			for _, finalEventFn := range f.finalEventFns {
				finalEventFn(proposalContainer)
			}
		case <-f.closeCh:
			return
		}
	}
}

// commit 提交对应的节点
func (f *CachedForest) commit(nodes []ForestNoder) []*ProposalContainer {
	// 第一个是root不需要提交
	// 其他全部提交
	commitProposals := make([]*ProposalContainer, 0)
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		err := f.writeToLedger(node)
		// 判断是否写入账本失败，若失败则停止写入，暂时缓存在内存中
		// 此时因为未进行裁剪，会导致forest中内容与账本不一致
		// 账本支持幂等操作，因此理论上应该没关系
		// 但还是强烈要求存储不能返回错误
		if err != nil {
			// 打印，然后返回
			f.logger.Errorf("insert block to chain failed, reason: %s", err)
			return commitProposals
		}
		commitProposals = append(commitProposals, node.Data())
	}
	// 裁剪整棵树
	f.cutTree(nodes[len(nodes)-1])
	// 更新finalQC
	f.updateFinalQC()
	// 处理final提案
	f.handleFinalProposal(f.root.Data())
	return commitProposals
}

// writeToLedger 将node写入到账本
func (f *CachedForest) writeToLedger(node ForestNoder) error {
	// 封装block，然后写入到账本中
	blockContainer := node.Data().block
	// 重新创建一个block（使用原先数据指针）
	block := &common.Block{
		Header:         blockContainer.block.Header,
		Dag:            blockContainer.block.Dag,
		Txs:            blockContainer.block.Txs,
		AdditionalData: blockContainer.block.AdditionalData,
	}
	// 将qc加入到block中
	utils.InsertQCToBlock(block, blockContainer.qc)
	// 提交该block
	return f.commiter.AddBlock(block)
}

// cutTree 对整棵树进行裁剪
func (f *CachedForest) cutTree(newRoot ForestNoder) {
	// 删除的对象是从旧的根节点开始，至新根节点间的所有数据
	f.delNodes(f.root, newRoot)
	// 删除根节点
	f.nodeCache.Delete(f.root.Key(), f.root.Data().proposal.View)
	// 设置新的根节点
	f.root = newRoot
}

// delNodes 删除树的节点（会判断其状态）
func (f *CachedForest) delNodes(root, checkNode ForestNoder) {
	// 删除root节点下临近的节点
	if root != nil {
		// 遍历root下的所有下一级节点
		children := root.Children()
		if len(children) > 0 {
			for _, childNode := range children {
				if childNode != checkNode {
					// 删除该节点下的所有子节点
					f.delNodes(childNode, checkNode)
					// 删除该节点
					f.nodeCache.Delete(childNode.Key(), childNode.Data().proposal.View)
					//TODO: delete txs if not root: blockId := root.Data().block.block.Header.BlockHash
				}
			}
		}
	}
}

// loadOrderedNodes 加载当前节点前面直到根节点的所有集合
// currNode <
func (f *CachedForest) loadOrderedNodes(currNode ForestNoder) []ForestNoder {
	// 获取对应的高度
	nodeHeight, rootHeight := currNode.Data().Height(), f.Root().Data().Height()
	// 创建数组
	arrayLength := int(nodeHeight - rootHeight + 1)
	orderedNodes := make([]ForestNoder, arrayLength)
	// 倒叙添加
	loopNode := currNode
	var arrayIdx = arrayLength - 1
	for arrayIdx >= 0 {
		orderedNodes[arrayIdx] = loopNode
		loopNode = loopNode.Parent()
		arrayIdx--
	}
	return orderedNodes
}

// addNode 将节点加入到整棵树中
func (f *CachedForest) addNode(currNode *CachedForestNode) error {
	key, parentKey, view := currNode.Key(), currNode.ParentKey(), currNode.Data().proposal.View
	// 判断该节点是否已经存在
	if f.nodeExist(key) {
		// 节点已经存在
		return fmt.Errorf(keyProposalExistedErrorFormat, key)
	}
	// 判断视图对应的节点是否存在
	if n := f.nodeCache.NodeByView(view); n != nil {
		// 节点已经存在
		return fmt.Errorf(viewProposalExistedErrorFormat, view)
	}
	parentNode := f.node(parentKey)
	// 判断该节点的父节点是否存在
	if parentNode == nil {
		return fmt.Errorf(parentProposalNotExistErrorFormat, key, parentKey)
	}
	// 存在的情况下再判断一下高度的情况
	if parentNode.Data().Height()+1 == currNode.Data().Height() {
		// 加入到集合中
		parentNode.AddChild(currNode)
		currNode.SetParent(parentNode)
		// 加入到map中
		f.nodeCache.AddNode(currNode)
		return nil
	}
	return fmt.Errorf(proposalHeightWrongErrorFormat,
		key, currNode.Data().Height(), parentNode.Data().Key(), parentNode.Data().Height())
}

// updateLockedQC 更新LockedQC
func (f *CachedForest) updateLockedQC(orderedNodes []ForestNoder) {
	// lockedQC要求必须是three-chain，也就是必须最起码有4个块
	if len(orderedNodes) > threeChain {
		// 新lockedQC为orderedNodes[length-3]
		newLockedQC := orderedNodes[len(orderedNodes)-threeChain]
		if newLockedQC.Data().block.View() > f.lockedQC.View() {
			f.logger.DebugDynamic(func() string {
				return fmt.Sprintf("update the lockedQC to proposal[%d:%d:%x]",
					newLockedQC.Data().block.Height(), newLockedQC.Data().block.View(),
					newLockedQC.Data().block.Hash())
			})
			f.lockedQC = newLockedQC.Data().block
		}
	}
}

// updateFinalQC 更新finalQC
func (f *CachedForest) updateFinalQC() {
	// 实际上finalQC就是整棵树的根节点
	f.finalQC = f.root.Data().block
	f.logger.Infof("update final qc [%d:%d:%x]", f.finalQC.Height(), f.finalQC.View(), f.finalQC.Key())
}

// addQC 将QC添加到它自己的区块中
func (f *CachedForest) addQC(qc *maxbft.QuorumCert) error {
	// 首先判断QC对应的Proposal是否存在
	key := string(utils.GetBlockIdFromQC(qc))
	var (
		targetNode ForestNoder
	)
	if targetNode = f.nodeCache.NodeByHash(key); targetNode == nil {
		return fmt.Errorf(proposalNotExistErrorFormat, key)
	}
	// 加入到对应的block中
	targetNode.Data().UpdateQC(qc)
	return nil
}

// updateGenericQC 更新GenericQC
func (f *CachedForest) updateGenericQC(qc *maxbft.QuorumCert) {
	// 更新genericQC规则
	// 当proposal.QC.View > genericQC.view时，设置genericQC=proposal.QC
	if utils.GetViewFromQC(qc) > utils.GetViewFromQC(f.genericQC) {
		now := time.Now()
		f.logger.DebugDynamic(func() string {
			return fmt.Sprintf("update the genericQC to [%d:%d:%x], escapetime: %s",
				utils.GetHeightFromQC(qc), utils.GetViewFromQC(qc),
				utils.GetBlockIdFromQC(qc), now.Sub(f.lastUpdateTime))
		})
		f.genericQC = qc
		f.lastUpdateTime = now
	}
	if height := utils.GetHeightFromQC(qc); height > f.genericHeight {
		f.genericHeight = height
	}
}

// 加载可以提交的节点集合
func loadCanCommitNodes(nodes []ForestNoder) []ForestNoder {
	// 提交的前提规则有两个
	// 1. 必须有一个3'chain
	// 2. 待提交的区块必须为新区块的直系祖先块

	// 首先必须是一个3'chain，则必须>=4个节点才可以
	if len(nodes) < threeChain+1 {
		return nil
	}

	num := len(nodes)
	commitIndex := num - threeChain - 1
	for i := num - 1; i > commitIndex; i-- {
		if nodes[i].ParentKey() != nodes[i-1].Key() {
			return nil
		}
	}
	return []ForestNoder{nodes[commitIndex]}
}

// InnerNodeCache defines an inner node cache
type InnerNodeCache struct {
	hash2Nodes sync.Map // map[string]ForestNoder 所有节点map[blockHash]Node
	view2Nodes sync.Map //map[uint64]ForestNoder  所有节点map[view]Node
}

// NewInnerNodeCache returns a new InnerNodeCache
func NewInnerNodeCache() *InnerNodeCache {
	return &InnerNodeCache{
		hash2Nodes: sync.Map{}, // make(map[string]ForestNoder, nodeLength),
		view2Nodes: sync.Map{}, // make(map[uint64]ForestNoder, nodeLength),
	}
}

// AddNode adds a node to the cache
func (c *InnerNodeCache) AddNode(node ForestNoder) {
	proposal := node.Data().proposal
	c.hash2Nodes.Store(node.Key(), node)
	c.view2Nodes.Store(proposal.View, node)
}

// Delete deletes a node from the cache
func (c *InnerNodeCache) Delete(key string, view uint64) {
	c.hash2Nodes.Delete(key)
	c.view2Nodes.Delete(view)
}

// NodeByHash returns the node with the specified block hash
func (c *InnerNodeCache) NodeByHash(hash string) ForestNoder {
	if n, exist := c.hash2Nodes.Load(hash); exist {
		return n.(ForestNoder)
	}
	return nil
}

// NodeByView returns the node with the specified view
func (c *InnerNodeCache) NodeByView(view uint64) ForestNoder {
	if n, exist := c.view2Nodes.Load(view); exist {
		return n.(ForestNoder)
	}
	return nil
}
