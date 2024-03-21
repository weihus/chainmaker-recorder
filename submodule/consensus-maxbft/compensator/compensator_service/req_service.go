/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package compensator_service

import (
	"fmt"

	"chainmaker.org/chainmaker/consensus-maxbft/v2/utils"

	"chainmaker.org/chainmaker/common/v2/msgbus"

	"chainmaker.org/chainmaker/pb-go/v2/consensus/maxbft"
	"chainmaker.org/chainmaker/pb-go/v2/net"
	"chainmaker.org/chainmaker/protocol/v2"
)

// ReqService implements s service to send requests to other consensus nodes to get missing proposals
type ReqService struct {
	nodeId string
	log    protocol.Logger
	msgBus msgbus.MessageBus
}

// NewReqService initials and returns a ReqService
func NewReqService(nodeId string, log protocol.Logger, msgBus msgbus.MessageBus) *ReqService {
	return &ReqService{nodeId: nodeId, log: log, msgBus: msgBus}
}

// SendRequest send a request to the specified node to get the block with the specified height, view and block hash
func (service *ReqService) SendRequest(nodeId string, missProposal *maxbft.ProposalFetchMsg) {
	service.log.DebugDynamic(func() string {
		return fmt.Sprintf("send request [%d:%d:%x] to get proposal from %s",
			missProposal.Height, missProposal.View, missProposal.BlockId, nodeId)
	})
	missProposal.Requester = []byte(service.nodeId) //todo. string ==> nodeId
	bz := utils.MustMarshal(&maxbft.ConsensusMsg{
		Type:    maxbft.MessageType_PROPOSAL_FETCH_MESSAGE,
		Payload: utils.MustMarshal(missProposal),
	})
	netMsg := &net.NetMsg{
		To:      nodeId,
		Type:    net.NetMsg_CONSENSUS_MSG,
		Payload: bz,
	}
	service.msgBus.Publish(msgbus.SendConsensusMsg, netMsg)
}
