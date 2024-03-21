package holepunch

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/host"
	"chainmaker.org/chainmaker/net-liquid/core/network"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/util"
	"chainmaker.org/chainmaker/net-liquid/stun"
	api "chainmaker.org/chainmaker/protocol/v2"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	// sleep time between every punch
	timeSpace int = 30
	// try count to punch
	tryCount int = 3
)

var (
	errAnotherPunching = errors.New("another hole punching")
	errNotPublicAddr   = errors.New("is not public addr")
)

// HolePunch is one of way of nat traversal
// hole punch only success when udp and some nat type situation
// func HolePunchAvailable() can judge the situation if punch success
type HolePunch struct {
	// host
	host host.Host
	// log
	log api.Logger
	// time space to try hole punch
	timeSpace int
	// which addr is punching
	punchingMap map[peer.ID]struct{}
	// lock punchingMap
	punchingMx sync.Mutex
	// disconnect sign
	stopMap sync.Map
}

// NewHolePunch new a HolePunch
func NewHolePunch(log api.Logger) (*HolePunch, error) {
	return &HolePunch{
		log:         log,
		timeSpace:   timeSpace,
		punchingMap: make(map[peer.ID]struct{}),
		stopMap:     sync.Map{},
	}, nil
}

// SetTimeSpace default == const timeSpace
// TimeSpace is wait time between two dial
func (d *HolePunch) SetTimeSpace(time int) {
	d.timeSpace = time
}

// HolePunchAvailable provide a func to
// calculate a result if punch is available between two nat type
func (d *HolePunch) HolePunchAvailable(type1, type2 stun.NATDeviceType) bool {
	//only two type cannot punch
	switch {
	case type1 == stun.NATTypeSymmetric && type2 >= stun.NATTypePortRestrictedCone:
		return false
	case type2 == stun.NATTypeSymmetric && type1 >= stun.NATTypePortRestrictedCone:
		return false
	}
	return true
}

// DirectConnect try punch
// if hole punch success return a networkConn and err is nil
func (d *HolePunch) DirectConnect(otherPeerAddr ma.Multiaddr) (network.Conn, error) {
	if otherPeerAddr == nil {
		return nil, errors.New("otherPeerAddr is nil")
	}
	if !manet.IsPublicAddr(otherPeerAddr) {
		return nil, errNotPublicAddr
	}
	//remotePort := getPort(otherPeerAddr)
	//for _, addr := range d.host.LocalAddresses() {
	//	if getPort(addr) != remotePort {
	//		return nil, errors.New("no need punch")
	//	}
	//}
	log := d.log
	addr := otherPeerAddr
	_, pid := util.GetNetAddrAndPidFromNormalMultiAddr(addr)
	// return if a goroutine is punching
	d.punchingMx.Lock()
	defer d.punchingMx.Unlock()
	if _, ok := d.punchingMap[pid]; ok {
		return nil, errAnotherPunching
	}
	d.punchingMap[pid] = struct{}{}
	// stop sign
	stopChan := make(chan struct{})
	d.stopMap.Store(pid.ToString(), stopChan)
	defer d.stopMap.Delete(pid.ToString())

	listenAddr := d.host.Network().GetTempListenAddresses()
	log.Debug("[HolePunch] GetTempListenAddresses:", listenAddr)
	if len(listenAddr) > 0 {
		err := d.ListenForNat(listenAddr, d.host.Network().ListenAddresses())
		log.Info("ListenForNat res:", err)
	}

	for i := 0; i < tryCount; i++ {
		if d.host.ConnMgr().IsConnected(pid) {
			return nil, errors.New("already connected:" + pid.ToString())
		}
		select {
		case <-d.host.Context().Done():
			return nil, errors.New("safe exit")
		case <-time.After(time.Duration(d.timeSpace) * time.Second):
		case <-stopChan:
			return nil, errors.New("peer offline")
		}
		if d.host.ConnMgr().IsConnected(pid) {
			return nil, errors.New("already connected:" + pid.ToString())
		}
		log.Debug("DirectConnect to ", addr.String())
		//dial
		networkConn, errDial := d.host.Dial(addr)
		if errDial != nil || networkConn == nil {
			continue
		}
		return networkConn, nil
	}
	//fail
	log.Warn("holePunch failed ")
	return nil, fmt.Errorf("all retries for hole punch with peer %s failed", otherPeerAddr)
}

// ListenForNat when nat change port
func (d *HolePunch) ListenForNat(listenAddr []ma.Multiaddr, already []ma.Multiaddr) error {
	log := d.log

	alreadyListenAddr := already
	log.Debug("[HolePunch] alreadyListenAddr:", alreadyListenAddr)
	needListenAddr := make([]ma.Multiaddr, 0, 2)
	for _, addr := range listenAddr {
		needListen := true
		port := getPort(addr)
		for _, already := range alreadyListenAddr {
			portAlready := getPort(already)
			// same port ,already listen before
			if port == portAlready {
				needListen = false
				break
			}
		}
		if needListen {
			needListenAddr = append(needListenAddr, addr)
		}
	}
	log.Debug("[HolePunch] needListenAddr:", needListenAddr)
	if d.host == nil || d.host.Network() == nil {
		return errors.New("d.host == nil || Network() == nil")
	}
	return d.host.Network().DirectListen(d.host.Context(), needListenAddr...)
}

//	getPort get port from Multi addr
func getPort(addr ma.Multiaddr) string {
	if addr == nil {
		return ""
	}
	_, maPort := ma.SplitFunc(addr, func(c ma.Component) bool {
		return c.Protocol().Code == ma.P_TCP || c.Protocol().Code == ma.P_UDP
	})
	if len(maPort.Protocols()) > 0 {
		port, _ := maPort.ValueForProtocol(maPort.Protocols()[0].Code)
		return port
	}
	return ""
}

// SetHost set host for HolePunch
// need host.listen and dial
func (d *HolePunch) SetHost(host host.Host) error {
	//func dial
	d.host = host
	return nil
}

// HandlePeerDisconnect handle notify peer offline
// when get this msg,should stop holePunch to the peer immediately
func (d *HolePunch) HandlePeerDisconnect(_ peer.ID, msgPayload []byte) {
	peerId := string(msgPayload)
	d.log.Debug("peer offline:", peerId)
	// try to find chan
	stopChan, ok := d.stopMap.Load(peerId)
	if !ok {
		return
	}
	peerStopChan, _ := stopChan.(chan struct{})
	if peerStopChan == nil {
		return
	}
	// avoid others send msg notify self to stop
	// make sure only one select block here
	d.stopMap.Delete(peerId)
	d.log.Debug("notify peer offline:", peerId)
	select {
	case peerStopChan <- struct{}{}:
	case <-time.After(time.Duration(d.timeSpace) * time.Second):
	}
}
