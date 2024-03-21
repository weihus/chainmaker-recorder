package stun

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	ma "github.com/multiformats/go-multiaddr"

	api "chainmaker.org/chainmaker/protocol/v2"
	"github.com/pion/stun"
)

//a stun server implement with udp
type stunUdpServer struct {
	master              net.PacketConn
	slaver              net.PacketConn
	bTwoPublicAddress   bool
	masterOther         net.PacketConn
	slaverOther         net.PacketConn
	log                 api.Logger
	otherStunServerAddr ma.Multiaddr
	notifyAddr          string
	peerNameNat         sync.Map
	closeC              chan bool
	serverCount         int32
}

// SetNetwork
// stunUdpServer use udp dont need network
func (s *stunUdpServer) SetNetwork(_, _ network.Network) error {
	return nil
}

// Listen
// addr1 addr2 should have same ip and different port
func (s *stunUdpServer) Listen(addr1, addr2 ma.Multiaddr) error {
	ip1, port1 := mAddrToIpPort(addr1)
	ip2, port2 := mAddrToIpPort(addr2)
	add1 := fmt.Sprintf("%v:%v", ip1, port1)
	add2 := fmt.Sprintf("%v:%v", ip2, port2)
	master, err := s.listenUDPAndServe(add1)
	if err != nil {
		return err
	}
	slaver, err := s.listenUDPAndServe(add2)
	s.master = master
	s.slaver = slaver
	return err
}

// InitChangeIpLocal init other stun server
// local device should have two net card and two public address
// listen on other addr and same function as first addr
func (s *stunUdpServer) InitChangeIpLocal(p *ChangeParam) error {
	if p == nil {
		return errors.New("p==nil")
	}
	if p.OtherMasterListenAddr == nil || p.OtherSlaveListenAddr == nil {
		return errors.New("addr is nil")
	}
	if p.OtherAddr == nil {
		return errors.New("OtherAddr is nil")
	}
	//dont need network

	s.otherStunServerAddr = p.OtherAddr
	//set type
	s.bTwoPublicAddress = true

	ip1, port1 := mAddrToIpPort(p.OtherMasterListenAddr)
	ip2, port2 := mAddrToIpPort(p.OtherSlaveListenAddr)
	add1 := fmt.Sprintf("%v:%v", ip1, port1)
	add2 := fmt.Sprintf("%v:%v", ip2, port2)
	//listen
	masterOther, err := s.listenUDPAndServe(add1)
	if err != nil {
		return err
	}
	slaveOther, err := s.listenUDPAndServe(add2)
	s.masterOther = masterOther
	s.slaverOther = slaveOther
	return err
}

// InitChangeIpNotify init http server and return err
// use to handle notify
func (s *stunUdpServer) InitChangeIpNotify(p *ChangeNotifyParam) error {
	//init http server
	if p == nil {
		return errors.New("p==nil")
	}
	otherAddr := p.OtherAddr
	notifyAddr := p.NotifyAddr
	localNotifyAddr := p.LocalNotifyAddr
	log := s.log
	//set type
	s.bTwoPublicAddress = false
	s.otherStunServerAddr = otherAddr
	s.notifyAddr = notifyAddr + notifyRoutPattern
	//listen http for change ip send to client
	errChan := make(chan error)
	go func(errChan chan error) {
		//avoid block
		http.HandleFunc(notifyRoutPattern, s.handlerNotifyMsg)
		err := http.ListenAndServe(localNotifyAddr, nil)
		if err != nil {
			errChan <- err
			log.Error("notify open in(%v) err:%v", localNotifyAddr, err.Error())
		} else {
			log.Info("notify success open in:", localNotifyAddr)
		}
	}(errChan)
	//if err,let known
	select {
	case err := <-errChan:
		return err
	case <-time.After(1 * time.Second):
		return nil
	}
}

// HandleChangeIpReq implement to enable interface
func (s *stunUdpServer) HandleChangeIpReq(multiAddr ma.Multiaddr, stunMsg []byte) error {
	ip, port := mAddrToIpPort(multiAddr)
	rAddr := &net.UDPAddr{IP: net.ParseIP(ip), Port: port}
	return s.handleChangeIpReq(rAddr, stunMsg)
}

// CloseListen
// and notify read goroutine return
func (s *stunUdpServer) CloseListen() error {
	if s == nil {
		return nil
	}
	log := s.log
	log.Info("stunUdpServer CloseListen")
	//close first ip
	_ = s.master.Close()
	_ = s.slaver.Close()
	//close second ip
	if s.masterOther != nil {
		_ = s.masterOther.Close()
	}
	if s.slaverOther != nil {
		_ = s.slaverOther.Close()
	}
	//notify goroutine return
	for i := int32(0); i < s.serverCount; i++ {
		select {
		case s.closeC <- true:
		case <-time.After(1 * time.Second):
		}
	}
	log.Info("stunUdpServer Closed")
	return nil
}

// NewUdpStunServer New a Udp StunServer
func NewUdpStunServer(log api.Logger) (Server, error) {
	return &stunUdpServer{
		log:    log,
		closeC: make(chan bool),
	}, nil
}

// ListenUDPAndServe listens on lAddr and process incoming packets.
// goroutine to read udp packet and send to client
// if client command change port ,use second addr
// if client command change ip ,use notify or other
func (s *stunUdpServer) listenUDPAndServe(lAddr string) (net.PacketConn, error) {
	log := s.log
	//listen
	c, err := net.ListenPacket("udp", lAddr)
	if err != nil {
		return nil, err
	}
	log.Info("stunUdpServer listening ok ,addr:", lAddr)
	//server
	go s.Serve(c)
	return c, nil
}

// genMessage handle stun msg and gen res
// none of business with net
// only handle stun msg
// res is the response msg
// pAttType is the type of change type
func (s *stunUdpServer) genMessage(msgFromAddr net.Addr, b []byte, req, res *stun.Message, pAttType *int) error {
	log := s.log
	if !stun.IsMessage(b) {
		return errNotSTUNMessage
	}
	//stun write
	if _, err := req.Write(b); err != nil {
		return errors.New(err.Error() + "failed to read message")
	}
	//client ip and port
	var (
		udpClientIp   net.IP
		udpClientPort int
	)
	//get client ip and port
	switch a := msgFromAddr.(type) {
	case *net.UDPAddr:
		udpClientIp = a.IP
		udpClientPort = a.Port
	default:
		return errors.New("unknown addr:" + msgFromAddr.String())
	}

	//decode msg
	m := new(stun.Message)
	m.Raw = b
	if err := m.Decode(); err != nil {
		return errors.New("Unable to decode message:" + err.Error())
	}

	//prepare other addr
	ip, port := mAddrToIpPort(s.otherStunServerAddr)
	pOtherAddress := &stun.OtherAddress{
		IP:   net.ParseIP(ip),
		Port: port,
	}

	//handle Attributes
	userName := ""
	natType := ""
	for k, v := range m.Attributes {
		log.Debug("stun msg:", k, v.Type.String())
		switch v.Type {
		case stun.AttrChangeRequest:
			//get change value
			value := binary.BigEndian.Uint32(v.Value)
			*pAttType = int(value)
		case stun.AttrUsername:
			//get user name
			r := parseStunMsg(m)
			userName = r.username.String()
		case stun.AttrData:
			//get data
			r := parseStunMsg(m)
			natType = r.data.String()
		default:
			return errors.New("unknown v.Type:" + v.Type.String())
		}
	}

	//build response msg
	if err := res.Build(req,
		stun.BindingSuccess,
		software,
		&stun.XORMappedAddress{
			IP:   udpClientIp,
			Port: udpClientPort,
		},
		pOtherAddress,
		stun.Fingerprint,
	); err != nil {
		return err
	}

	//handle nat type set or get
	if len(userName) > 0 && len(natType) > 0 {
		log.Debug(".peerNameNat.Store:", userName, natType)
		s.peerNameNat.Store(userName, natType)
	} else if len(userName) > 0 {
		natType, _ := s.peerNameNat.Load(userName)
		log.Debug(".peerNameNat.Load:", userName, natType)
		if natType != nil {
			res.Add(stun.AttrUsername, []byte(userName))
			res.Add(stun.AttrData, []byte(natType.(string)))
		}
	}
	return nil
}

// serveConn serve a conn once
// read from conn and decode stun msg
// respond msg to client
// c: conn
// res: stun response
// req: stun request
func (s *stunUdpServer) serveConn(c net.PacketConn, res, req *stun.Message) error {
	if c == nil {
		return errors.New("PacketConn is nil")
	}
	log := s.log
	//read
	buf := make([]byte, stunMsgSize)
	n, fromAddr, err := c.ReadFrom(buf)
	if err != nil {
		return errors.New("ReadFrom err:" + err.Error())
	}
	log.Infof("serverip(%v) ReadFrom %v len(%v) ", c.LocalAddr(), fromAddr, n)
	//handle stun msg
	if _, err = req.Write(buf[:n]); err != nil {
		return errors.New("stun.Message Write err:" + err.Error())
	}
	changeType := 0
	if err = s.genMessage(fromAddr, buf[:n], req, res, &changeType); err != nil {
		if err == errNotSTUNMessage {
			return nil
		}
		return errors.New("genMessage:" + err.Error())
	}

	//write back
	var nWrite int
	switch changeType {
	case ChangeIpPort:
		//change ip
		err = s.handleChangeIpReq(fromAddr, res.Raw)
	case ChangeIp:

	case ChangePort:
		//change port
		nWrite, err = s.slaver.WriteTo(res.Raw, fromAddr)
		log.Infof("change port(%v) Write ,len(%v) res:%v", s.slaver.LocalAddr().String(), nWrite, err)
	default:
		//write back
		nWrite, err = c.WriteTo(res.Raw, fromAddr)
		log.Infof("default Write ,len(%v) res:%v", nWrite, err)
	}
	return err
}

// Serve read packets from connections
// responds BINDING requests
func (s *stunUdpServer) Serve(c net.PacketConn) {
	log := s.log
	//defer
	defer func() {
		if e := recover(); e != nil {
			stack := debug.Stack()
			_, _ = os.Stderr.Write(stack)
			log.Errorf("panic stack: %s", string(stack))
		}
	}()
	atomic.AddInt32(&s.serverCount, 1)
	// avoid new and free frequently
	var (
		res = new(stun.Message)
		req = new(stun.Message)
	)
	// return when <-s.closeC
	for {
		select {
		case <-s.closeC:
			return
		default:
		}
		if err := s.serveConn(c, res, req); err != nil {
			log.Warnf("stunUdpServer(%v) serve err: %v", c.LocalAddr(), err)
		}
		res.Reset()
		req.Reset()
	}
}

// handlerNotifyMsg handler NotifyMsg from another stun server
// use second port send msg to client
// write return success dont mead real success
func (s *stunUdpServer) handlerNotifyMsg(w http.ResponseWriter, r *http.Request) {
	log := s.log
	var data notifyParam
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		_ = r.Body.Close()
		log.Fatal(err)
	}
	//stun client addr
	log.Debug("handlerNotifyMsg DesAddr:", data.DesAddr)
	ipAndPort := strings.Split(data.DesAddr, ":")
	port, _ := strconv.Atoi(ipAndPort[1])
	rAddr := &net.UDPAddr{IP: net.ParseIP(ipAndPort[0]), Port: port}
	//write back To client
	n, errW := s.slaver.WriteTo(data.Raw, rAddr)

	var sendRes string
	if errW != nil {
		sendRes = fmt.Sprintf("(%v)send msg to %v err:%v", s.slaver.LocalAddr(), rAddr, errW)
	} else {
		sendRes = fmt.Sprintf("(%v)send msg to %v ok len(%v)", s.slaver.LocalAddr(), rAddr, n)
	}
	log.Info(sendRes)
	_, _ = w.Write([]byte(sendRes))
}

// handleChangeIpReq
// handle ChangeIp Req from stun client and change ip response
// if bTwoPublicAddress use other addr respond
// if !bTwoPublicAddress notify other stun respond
// stunClientAddr: stun client addr
// stunMsg: a response should write to client
func (s *stunUdpServer) handleChangeIpReq(stunClientAddr net.Addr, stunMsg []byte) error {
	log := s.log
	if s.bTwoPublicAddress {
		//use other addr
		log.Infof("use two addr(%v)send msg to stunClient:%v  ", s.masterOther.LocalAddr(), stunClientAddr)
		_, err := s.slaverOther.WriteTo(stunMsg, stunClientAddr)
		return err
	}

	//notify another stun server
	urlNotify := s.notifyAddr
	var pNotifyParam notifyParam
	pNotifyParam.Raw = stunMsg
	pNotifyParam.DesAddr = stunClientAddr.String()
	byteParam, _ := json.Marshal(pNotifyParam)
	log.Infof("send notify to url:%v req: %s ", urlNotify, string(byteParam))
	//http notify another
	clt := http.Client{}
	resp, errHttp := clt.Post(urlNotify, "application/json", bytes.NewBuffer(byteParam))
	if resp != nil {
		body, _ := ioutil.ReadAll(resp.Body)
		//must close otherwise leak
		defer func() {
			_ = resp.Body.Close()
		}()
		log.Infof("send notify to url:%v result: %s ", urlNotify, string(body))
	}
	return errHttp
}
