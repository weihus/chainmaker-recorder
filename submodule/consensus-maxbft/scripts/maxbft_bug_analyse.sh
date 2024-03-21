#!/usr/bin/env bash

grep_key_and_analyse(){
  node_idetify=`grep "currentNode" -a $LOG_PATH | head -n 1`
  echo "currentNode is ${node_idetify#*currentNode}"

  # sub commands
  start_node="Git Commit" # 节点启动标识
  init_step="BASE INIT STEP" # 初始化各模块
  start_step="START STEP" # 启动各模块
  enter_new_view="enter new view" # 进入新视图
  begin_handle_proposal="handle proposal" # 开始处理提案
  core_verify_block_failed="verify failed" # core模块验证区块失败
  consensus_verify_proposal_failed="ValidateProposal failed" # 共识模块输出提案验证失败
  consensus_handle_proposal_success="handle proposal success" # 共识模块输出提案验证成功
  core_validate_block="Validate block\[" # core 验证区块成功后，输出块中的交易数
  core_validate_block_time="verify success \[" # core统计验证区块的时间消耗
  consensus_fired_create_block_signal="fired block generation" # 共识模块进入新视图后触发信号，生成新区块
  core_proposer_success="proposer success " # core 生成区块成功
  not_allowed_generate_empty_block="empty block are not" # 不允许生成空块
  create_vote="start to GenerateVote." # 共识模块开始生成投票
  begin_handle_vote="handle vote info" # 开始处理投票
  add_pending_vote="addPendingVote start" # 将投票加入待处理队列
  add_vote="addVote" # 投票验证通过，添加到缓存中
  validate_vote_failed="ValidateVote failed" # 验证投票失败
  try_build_qc="tryBuildQc" # 使用投票尝试构建QC
  pacemaker_add_event="pacemaker add event" # 添加新的超时事件
  commit_block="commit block " # core模块提交区块上链
  enter_new_epoch="step in new epoch" # 进入新世代
  epoch_end="the current epoch has ended" # 生成区块时，检测到当前世代已结束
  sync_2_consensus="local node is switching to a consensus node" # 节点在世代切换后由同步节点变为共识节点
  consensus_2_sync="local node is switching to a sync node" # 节点在世代切换后由共识节点变为同步节点
  consensus_2_consensus="step in new epoch" # 节点在世代切换前后都为共识节点
  sync_module_has_stopped="receive block is done by sync module" # 同步模块已拉取数据到临界区，将关闭同步模块
  has_recv_vote_for_qc="vote for view"

  error="ERROR\|error"
  warn="WARN\|warn"

  # search command with grep in file
  command="$enter_new_view\|$begin_handle_proposal\|$core_verify_block_failed\
\|$consensus_verify_proposal_failed\|$consensus_handle_proposal_success\
\|$begin_handle_vote\|$add_pending_vote\|$consensus_fired_create_block_signal\
\|$create_vote\|$pacemaker_add_event\|$commit_block\|$error\|$warn\|$not_allowed_generate_empty_block\
\|$enter_new_epoch\|$validate_vote_failed\|$try_build_qc\|$start_node\|$init_step\|$start_step\
\|$add_vote\|$epoch_end\|$sync_2_consensus\|$consensus_2_sync\|$consensus_2_consensus\
\|$core_validate_block\|$core_validate_block_time\|$has_recv_vote_for_qc\|$sync_module_has_stopped\
\|$core_proposer_success"

  echo "$command"
  grep -a "$command" $LOG_PATH > filter.log
}

LOG_PATH=$1
grep_key_and_analyse