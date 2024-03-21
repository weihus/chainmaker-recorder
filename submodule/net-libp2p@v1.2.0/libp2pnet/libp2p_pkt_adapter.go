/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package libp2pnet

import (
	"chainmaker.org/chainmaker/net-common/common/pkt"
	"chainmaker.org/chainmaker/net-common/utils"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/libp2p/go-libp2p-core/peer"

	"sync"
)

const (
	pktChainId = "_PKT_Chain"
	pktMsgFlag = "_PKT_0.0.1"
)

// pktAdapter is a adapter for pkt assembling/disassembling of net messages payload bytes.
type pktAdapter struct {
	sync.Once
	pktCache *pkt.Cache
	ln       *LibP2pNet

	closeC chan struct{}
}

func newPktAdapter(ln *LibP2pNet) *pktAdapter {
	return &pktAdapter{
		pktCache: pkt.NewPktCache(),
		ln:       ln,
		closeC:   make(chan struct{}),
	}
}

func (pa *pktAdapter) run() {
	pa.Once.Do(func() {
		go pa.loop()
	})
}

func (pa *pktAdapter) cancel() {
	close(pa.closeC)
}

func (pa *pktAdapter) sendMsg(chainId string, pid peer.ID, msgFlag string, data []byte) error {
	select {
	case <-pa.closeC:
		// if adapter closed, call SendMsg method of host directly
		return pa.ln.sendMsg(chainId, pid, msgFlag, data)
	default:
		// continue
	}
	protocolId := utils.CreateProtocolWithChainIdAndFlag(chainId, msgFlag)
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
			err = pa.ln.sendMsg(pktChainId, targetPID, pktMsgFlag, p.Marshal())
			if err != nil {
				pa.ln.log.Warnf("[PktAdapter] send pkt failed, %s", err.Error())
				errC <- err
			}
		}(pid, p)
	}
	wg.Wait()
	select {
	case err = <-errC:
		return err
	default:

	}
	return nil
}

func (pa *pktAdapter) loop() {
	for {
		select {
		case <-pa.closeC:
			return
		case fullPkt := <-pa.pktCache.FullPktC():
			payload, protocolBytes, err := pkt.BytesAssembler.AssembleBytes(fullPkt.PktList)
			if err != nil {
				pa.ln.log.Warnf("[PktAdapter] assemble bytes failed, %s", err.Error())
				continue
			}
			p := string(protocolBytes)
			chainId, msgFlag := utils.GetChainIdAndFlagWithProtocol(p)
			h := pa.ln.messageHandlerDistributor.handler(chainId, msgFlag)
			if h == nil {
				pa.ln.log.Warnf("[PktAdapter] msg payload handler not found (protocol: %s)", p)
				continue
			}
			go func(handler protocol.DirectMsgHandler, sender string, payload []byte) {
				e := handler(sender, payload)
				if e != nil {
					pa.ln.log.Warnf("[PktAdapter] call direct msg handler failed, %s "+
						"(sender: %s, chain-id: %s, msg-flag: %s)", e, fullPkt.Sender, chainId, msgFlag)
				}
			}(h, fullPkt.Sender, payload)
		}
	}
}

func (pa *pktAdapter) directMsgHandler(sender string, msgPayload []byte) error {
	p := &pkt.Pkt{}
	err := p.Unmarshal(msgPayload)
	if err != nil {
		pa.ln.log.Warnf("[PktAdapter] pkt unmarshal failed, %s", err.Error())
		return err
	}
	pa.pktCache.PutPkt(sender, p)
	return nil
}
