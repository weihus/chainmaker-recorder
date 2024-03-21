package libp2pnet

import (
	"context"

	"chainmaker.org/chainmaker/net-common/utils"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

type peerSendMsgHandler struct {
	pid      peer.ID
	dataChan chan []byte
	stream   network.Stream
	ctx      context.Context
	cancel   context.CancelFunc
	log      protocol.Logger
}

func NewPeerSendMsgHandler(hostContext context.Context, pid peer.ID,
	s network.Stream, log protocol.Logger) *peerSendMsgHandler {
	psh := &peerSendMsgHandler{
		pid:      pid,
		dataChan: make(chan []byte, 1024),
		stream:   s,
		log:      log,
	}
	psh.ctx, psh.cancel = context.WithCancel(hostContext)

	return psh
}

func (psh *peerSendMsgHandler) handleSendingMessages() {

	for {
		select {
		case dataBytes, ok := <-psh.dataChan:
			if !ok {
				return
			}

			lengthBytes := utils.IntToBytes(len(dataBytes))
			writeBytes := append(lengthBytes, dataBytes...)

			size, err := psh.stream.Write(writeBytes)
			if err != nil {
				psh.log.Warnf("[PeerSendMsgHandler] send the msg failed, err: [%s], peer: [%s]", err.Error(), psh.pid.Pretty())
				return
			}

			if size < len(writeBytes) {
				psh.log.Warnf("[PeerSendMsgHandler] send the msg incompletely, err: [%s], peer: [%s]", err.Error(), psh.pid.Pretty())
				return
			}

		case <-psh.ctx.Done():
			return
		}
	}
}

func (psh *peerSendMsgHandler) handlePeerEOF() {
	data := make([]byte, 8)
	for {
		select {
		case <-psh.ctx.Done():
			return
		default:
			_, err := psh.stream.Read(data)
			if err != nil {
				psh.stream.Reset()
				psh.cancel()
				psh.log.Warnf("[PeerSendMsgHandler] unexpected message from [%s]", psh.pid.Pretty())
				return
			}
		}
	}
}

func (psh *peerSendMsgHandler) close() {
	psh.stream.Reset()
	//close(psh.dataChan)
	psh.cancel()
	psh.log.Infof("[PeerSendMsgHandler] the peer [%s] send msg handler close. ", psh.pid.Pretty())
}

func (psh *peerSendMsgHandler) getDataChan() chan<- []byte {
	return psh.dataChan
}
