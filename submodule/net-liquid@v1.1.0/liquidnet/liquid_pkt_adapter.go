/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package liquidnet

import (
	"chainmaker.org/chainmaker/net-common/common/pkt"
	"chainmaker.org/chainmaker/net-liquid/core/host"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/protocol"
	api "chainmaker.org/chainmaker/protocol/v2"

	"sync"
)

const (
	pktProtocol protocol.ID = "/_PKT/v0.0.1"
)

// pktAdapter is a adapter for pkt assembling/disassembling of net messages payload bytes.
type pktAdapter struct {
	sync.Once
	pktCache *pkt.Cache
	h        host.Host
	log      api.Logger

	closeC chan struct{}
}

// newPktAdapter used to create a new pktAdapter object.
func newPktAdapter(h host.Host, log api.Logger) *pktAdapter {
	return &pktAdapter{
		pktCache: pkt.NewPktCache(),
		h:        h,
		log:      log,
		closeC:   make(chan struct{}),
	}
}

// run start pktAdapter
func (pa *pktAdapter) run() {
	pa.Once.Do(func() {
		go pa.loop()
	})
}

// cancel close pktAdapter
func (pa *pktAdapter) cancel() {
	close(pa.closeC)
}

// sendMsg if pktAdapter is enabled, the message must be processed by pktAdapter before sending the message,
// that is, if the message is too large, the message will be split and processed.
func (pa *pktAdapter) sendMsg(targetPID peer.ID, protocolId protocol.ID, data []byte) error {
	select {
	case <-pa.closeC:
		// if adapter closed, call SendMsg method of host directly
		return pa.h.SendMsg(protocolId, targetPID, data)
	default:
		// continue
	}
	if !pa.h.IsPeerSupportProtocol(targetPID, pktProtocol) {
		// if remote peer not support pkt protocol, call SendMsg method of host directly
		return pa.h.SendMsg(protocolId, targetPID, data)
	}
	pktList, err := pkt.BytesDisassembler.DisassembleBytes(data, []byte(protocolId))
	if err != nil {
		return err
	}
	errC := make(chan error, len(pktList))
	var wg sync.WaitGroup
	wg.Add(len(pktList))
	for i := range pktList {
		p := pktList[i]
		go func(targetPID peer.ID, p *pkt.Pkt) {
			defer wg.Done()
			err = pa.h.SendMsg(pktProtocol, targetPID, p.Marshal())
			if err != nil {
				pa.log.Errorf("[PktAdapter] send pkt failed, %s (remote pid: %s)", err.Error(), targetPID)
				errC <- err
			}
		}(targetPID, p)
	}
	wg.Wait()
	select {
	case err = <-errC:
		return err
	default:

	}
	return nil
}

// msgPayloadFunc the processor used by pktAdapter to process the message.
func (pa *pktAdapter) msgPayloadFunc(senderPID peer.ID, msgPayload []byte) {
	p := &pkt.Pkt{}
	err := p.Unmarshal(msgPayload)
	if err != nil {
		pa.log.Errorf("[PktAdapter] pkt unmarshal failed, %s", err.Error())
		return
	}
	pa.pktCache.PutPkt(senderPID.ToString(), p)
}

// loop This is a continuously running background task, waiting for the remote peer to process the message
// after pktAdapter and perform subsequent processing.
func (pa *pktAdapter) loop() {
	for {
		select {
		case <-pa.closeC:
			return
		case fullPkt := <-pa.pktCache.FullPktC():
			// get the complete message sent by the remote peer
			payload, protocolBytes, err := pkt.BytesAssembler.AssembleBytes(fullPkt.PktList)
			if err != nil {
				pa.log.Errorf("[PktAdapter] assemble bytes failed, %s", err.Error())
				continue
			}
			// get the corresponding message processor according to the protocol ID
			p := protocol.ID(protocolBytes)
			h := pa.h.ProtocolMgr().GetHandler(p)
			if h == nil {
				pa.log.Errorf("[PktAdapter] msg payload handler not found (protocol: %s)", p)
				continue
			}
			// use message handlers to process messages
			go h(peer.ID(fullPkt.Sender), payload)
		}
	}
}
