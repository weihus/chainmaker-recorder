package stun

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"chainmaker.org/chainmaker/net-liquid/core/network"
	api "chainmaker.org/chainmaker/protocol/v2"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/stun"
)

// NewDefaultStunServer .
func NewDefaultStunServer(log api.Logger) (Server, error) {
	return &defaultStunServer{
		log:    log,
		closeC: make(chan bool),
	}, nil
}

//stun server use network.Network
type defaultStunServer struct {
	// save STUN client NAT type
	peerNameNat sync.Map
	// log
	log api.Logger
	// first addr and port
	networkMaster network.Network
	// first addr and change port
	networkSlave network.Network
	// one device have two public address
	bTwoPublicAddress bool
	// second addr and port
	networkOtherMaster network.Network
	// second addr and change port
	networkOtherSlave network.Network
	// public addr for networkOtherMaster
	otherStunServerAddr ma.Multiaddr
	// localListenAddrs
	localListenAddrs []ma.Multiaddr
	// url for notify
	notifyAddr string
	// closeC
	closeC chan bool
}

//CloseListen
func (d *defaultStunServer) CloseListen() error {
	if d == nil {
		return nil
	}
	//close
	if d.networkMaster != nil {
		_ = d.networkMaster.Close()
	}
	if d.networkSlave != nil {
		_ = d.networkSlave.Close()
	}
	//close second addr
	if d.networkOtherMaster != nil {
		_ = d.networkOtherMaster.Close()
	}
	if d.networkOtherSlave != nil {
		_ = d.networkOtherSlave.Close()
	}

	if d.closeC != nil {
		select {
		case d.closeC <- true:
		case <-time.After(1 * time.Second):
		}
	}
	return nil
}

//set network,use listen and dial
//network should make from newNetwork()
func (d *defaultStunServer) SetNetwork(net1, net2 network.Network) error {
	//need two network
	d.networkMaster = net1
	d.networkSlave = net2
	//handler
	d.networkMaster.SetNewConnHandler(d.handlerNewListenConn)
	d.networkSlave.SetNewConnHandler(d.handlerNewListenConn)
	return nil
}

//handlerNewListenConn
//invoke when listen a new conn
//conn:a new conn established
func (d *defaultStunServer) handlerNewListenConn(conn networkConn) (bool, error) {
	log := d.log
	//first addr
	if conn == nil {
		return false, errors.New("conn is nil")
	}
	localAddr := conn.LocalNetAddr().String()
	remoteAddr := conn.RemoteAddr().String()
	remotePeer := conn.RemotePeerID()
	//accept
	log.Infof("stunServer(%v) accept a conn(%v)pId(%v),", localAddr, remoteAddr, remotePeer)

	go func() {
		//dont block
		if err := d.handlerAcceptConn(conn, d.networkMaster, d.networkSlave); err != nil {
			log.Errorf("stunServer(%v) handlerAcceptConn pId(%v),err:%v", localAddr, remotePeer, err.Error())
		}
	}()
	return true, nil
}

//handlerNewListenConn Other addr
//invoke when listen a new conn
//conn:a new conn established
func (d *defaultStunServer) handlerOtherNewListenConn(conn networkConn) (bool, error) {
	log := d.log
	//second addr
	if conn == nil {
		return false, errors.New("conn is nil")
	}
	localAddr := conn.LocalNetAddr().String()
	remoteAddr := conn.RemoteAddr().String()
	remotePeer := conn.RemotePeerID()
	//accept
	log.Infof("other stunServer(%v) accept a conn(%v)pId(%v),", localAddr, remoteAddr, remotePeer)

	go func() {
		//dont block
		if err := d.handlerAcceptConn(conn, d.networkOtherMaster, d.networkOtherSlave); err != nil {
			log.Errorf("stunServer(%v) handlerAcceptConn pId(%v),err:%v", localAddr, remotePeer, err.Error())
		}
	}()
	return true, nil
}

//handlerAcceptConn loop read and write msg back
//if change port ,user second network
//conn:a new conn established
//changePortNetwork: [1] is change port network
func (d *defaultStunServer) handlerAcceptConn(conn networkConn, masterNetwork, slaveNetwork network.Network) error {
	//handle new conn
	log := d.log
	defer func() {
		if e := recover(); e != nil {
			stack := debug.Stack()
			_, _ = os.Stderr.Write(stack)
			log.Errorf("panic stack: %s", string(stack))
		}
	}()

	localAddr := conn.LocalNetAddr().String()
	remotePeer := conn.RemotePeerID()
	for {
		select {
		case <-d.closeC:
			return nil
		default:
		}
		//AcceptReceiveStream
		readStream, errAccept := conn.AcceptReceiveStream()
		if errAccept != nil {
			log.Errorf("server(%v) AcceptReceiveStream (%v),err:%v", localAddr, remotePeer, errAccept.Error())
			return errAccept
		}
		log.Infof("server(%v) AcceptReceiveStream ok pId(%v),", localAddr, remotePeer)

		go func() {
			if err := d.serveConn(readStream, conn, masterNetwork, slaveNetwork); err != nil {
				log.Warn("serveConn err: ", err)
			}
		}()
	}
}

//handle readStream and CreateSendStream
//readStream: read buff from
//conn: networkConn use to CreateSendStream
//changePortNetwork: [1] is change port network
func (d *defaultStunServer) serveConn(s network.ReceiveStream, conn networkConn, master, slave network.Network) error {
	log := d.log
	defer func() {
		if e := recover(); e != nil {
			stack := debug.Stack()
			_, _ = os.Stderr.Write(stack)
			log.Errorf("panic stack: %s", string(stack))
		}
	}()
	//stream
	sendStream, errCreatSendS := conn.CreateSendStream()
	if errCreatSendS != nil {
		return errCreatSendS
		//return errors.New("CreateSendStream:" + errCreatSendS.Error())
	}
	log.Infof("stunServer(%v) CreateSendStream ok pId(%v),", conn.LocalNetAddr().String(), conn.RemotePeerID())
	clientAddr := conn.RemoteAddr()
	res := new(stun.Message)
	req := new(stun.Message)
	buff := make([]byte, stunMsgSize)
	var quit bool
	var err error

	//quit when read err
	for {
		if quit, err = d.readConn(s, sendStream, conn, master, slave, buff, res, req); err != nil {
			log.Warn("StunServer(%v) readConn client(%v) err: %v", conn.LocalAddr(), clientAddr, err)
		}
		//reset buff
		res.Reset()
		req.Reset()
		buff = buff[0:0]
		if quit {
			return nil
		}
	}
}

// readConn
// read from client and respond
// readStream: read buff from
// sendStream: send response
// conn: connect between server and client
// changePortNetwork: [1] is change port network
// res req :stun msg
func (d *defaultStunServer) readConn(
	readStream network.ReceiveStream,
	sendStream network.SendStream,
	conn networkConn,
	master, slave network.Network,
	buff []byte,
	res, req *stun.Message) (bool, error) {
	log := d.log
	clientAddr := conn.RemoteAddr()
	//read
	n, errRead := readStream.Read(buff)
	if errRead != nil {
		return true, errors.New("read err:" + errRead.Error())
	}
	if n == 0 {
		return true, errors.New("reset by peer")
	}
	log.Infof("Read ok ,len(%v)", n)

	changeType := 0
	//handle stun msg
	errMsg := d.genMessage(conn.RemoteAddr(), buff[:n], req, res, &changeType)
	if errMsg != nil {
		return false, errors.New("genMessage err:" + errMsg.Error())
	}

	var err error
	switch changeType {
	case ChangePort:
		//change port should use another network
		net := master
		if conn.Network() == slave && conn.Network() != master {
			net = master
		}
		//dial
		changeConn, changeErr := net.Dial(context.Background(), clientAddr)
		if changeErr != nil {
			return false, errors.New("change port dial(" + clientAddr.String() + ") err:" + changeErr.Error())
		}
		remoteAddr := changeConn.RemoteAddr().String()
		//stream
		changeStream, errC := changeConn.CreateSendStream()
		if errC != nil {
			return false, errors.New("change port CreateSendStream(" + remoteAddr + ") err:" + errC.Error())
		}
		//write
		nWrite, errW := changeStream.Write(res.Raw)
		if errW != nil {
			return false, errors.New("change port Write to(" + remoteAddr + ") err:" + errW.Error())
		}
		log.Infof("change port Write to(%v)ok ,len(%v)", remoteAddr, nWrite)
	case ChangeIpPort:
		//change ip
		err = d.HandleChangeIpReq(conn.RemoteAddr(), res.Raw)
	default:
		//write back
		nWrite, errW := sendStream.Write(res.Raw)
		log.Infof("default Write ,len(%v) res:%v", nWrite, errW)
	}
	return false, err
}

// Listen
// should set network before
// addr1 addr2 should have same ip and different port
func (d *defaultStunServer) Listen(listenAddr1, listenAddr2 ma.Multiaddr) error {
	//listen first addr
	d.localListenAddrs = append(d.localListenAddrs, listenAddr1, listenAddr2)
	//listen
	if err := d.networkMaster.Listen(context.Background(), listenAddr1); err != nil {
		return err
	}
	err := d.networkSlave.Listen(context.Background(), listenAddr2)
	return err
}

type notifyParam struct {
	SrcPort int    `json:"srcPort"`
	DesAddr string `json:"desAddr"`
	Raw     []byte `json:"raw"`
}

// InitChangeIpLocal
// init other stun server
// local device should have two net card and two public address
// listen on other addr and same function as first addr
func (d *defaultStunServer) InitChangeIpLocal(p *ChangeParam) error {
	//init two network
	if p == nil {
		return errors.New("p==nil")
	}
	if p.OtherMasterListenAddr == nil || p.OtherSlaveListenAddr == nil {
		return errors.New("addr is nil")
	}
	if p.OtherMasterNetwork == nil || p.OtherSlaveNetwork == nil {
		return errors.New("network is nil")
	}
	if p.OtherAddr == nil {
		return errors.New("OtherAddr is nil")
	}
	//avoid repeat addr
	otherListenAddr := []ma.Multiaddr{p.OtherMasterListenAddr, p.OtherSlaveListenAddr}
	for _, localListenAddr := range d.localListenAddrs {
		for _, otherListenAddrTmp := range otherListenAddr {
			if localListenAddr.String() == otherListenAddrTmp.String() {
				return errors.New("same addr")
			}
		}
	}

	//type
	d.bTwoPublicAddress = true
	d.otherStunServerAddr = p.OtherAddr
	d.networkOtherMaster = p.OtherMasterNetwork
	d.networkOtherSlave = p.OtherSlaveNetwork
	//listen other addr
	if err := d.networkOtherMaster.Listen(context.Background(), p.OtherMasterListenAddr); err != nil {
		return err
	}
	if err := d.networkOtherSlave.Listen(context.Background(), p.OtherSlaveListenAddr); err != nil {
		return err
	}
	d.networkOtherMaster.SetNewConnHandler(d.handlerOtherNewListenConn)
	d.networkOtherSlave.SetNewConnHandler(d.handlerOtherNewListenConn)
	return nil
}

// InitChangeIpNotify
// func (d *defaultStunServer)
// PrepareChangeIpNotify(otherAddr ma.MultiAddr, notifyAddr string, localNotifyAddr string) error {
// init http server and return err
// use to handle notify
func (d *defaultStunServer) InitChangeIpNotify(p *ChangeNotifyParam) error {
	//init http server
	if p == nil {
		return errors.New("p==nil")
	}
	otherAddr := p.OtherAddr
	notifyAddr := p.NotifyAddr
	localNotifyAddr := p.LocalNotifyAddr
	log := d.log
	//type
	d.bTwoPublicAddress = false
	d.otherStunServerAddr = otherAddr
	d.notifyAddr = notifyAddr + notifyRoutPattern
	//listen http for change ip send to client
	errChan := make(chan error)
	go func(errChan chan error) {
		//avoid block
		http.HandleFunc(notifyRoutPattern, d.handlerNotifyMsg)
		err := http.ListenAndServe(localNotifyAddr, nil)
		if err != nil {
			errChan <- err
			log.Errorf("notify open in(%v) err:%v", localNotifyAddr, err.Error())
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

// HandleChangeIpReq
// handle ChangeIp Req from stun client and change ip response
// if bTwoPublicAddress use other addr respond
// if !bTwoPublicAddress notify other stun respond
// stunClientAddr: stun client addr
// stunMsg: a response should write to client
func (d *defaultStunServer) HandleChangeIpReq(stunClientAddr ma.Multiaddr, stunMsg []byte) error {
	//change ip send back
	log := d.log
	log.Debug("ChangeIp send to client:", stunClientAddr.String())
	if stunClientAddr == nil {
		return errors.New("stunClientAddr==nil")
	}
	//a notify
	if !d.bTwoPublicAddress {
		//http type
		urlNotify := d.notifyAddr
		var pNotifyParam notifyParam
		pNotifyParam.Raw = stunMsg
		pNotifyParam.DesAddr = stunClientAddr.String()
		byteParam, _ := json.Marshal(pNotifyParam)
		log.Infof("send notify to url:%v req: %s ", urlNotify, string(byteParam))
		//notify other
		clt := http.Client{}
		resp, errHttp := clt.Post(urlNotify, "application/json", bytes.NewBuffer(byteParam))
		if errHttp != nil {
			log.Errorf("send to notify url:%v err:%v ", urlNotify, errHttp)
		}
		if resp != nil {
			body, _ := ioutil.ReadAll(resp.Body)
			//must close otherwise leak
			defer func() {
				_ = resp.Body.Close()
			}()
			log.Infof("send to notify url:%v result: %s ", urlNotify, string(body))
		} else {
			log.Warn("send to notify,resp is nil")
		}
		return errHttp
	}
	var err error
	//TwoPublicAddress type
	log.Infof("try use other(%v) send to client", d.networkOtherSlave.ListenAddresses())
	//dial
	conn, errDial := d.networkOtherSlave.Dial(context.Background(), stunClientAddr)
	if errDial != nil {
		log.Debug("other.Dial err:", errDial.Error())
		return errDial
	}
	//stream
	s, errCreat := conn.CreateSendStream()
	if errCreat != nil {
		log.Debug("other.CreateSendStream err:", errCreat.Error())
		return errCreat
	}
	//write
	_, err = s.Write(stunMsg)
	log.Debug("other.Write err:", err)
	if err == nil {
		return nil
	}
	return err

}

// handlerNotifyMsg
// handler http req from another stun server
// use second port send msg to client
func (d *defaultStunServer) handlerNotifyMsg(w http.ResponseWriter, r *http.Request) {
	log := d.log
	var data notifyParam
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		_ = r.Body.Close()
		log.Fatal(err)
	}
	log.Info("handlerNotifyMsg try send to:", data.DesAddr)
	//stun client addr
	stunClientAddr, _ := ma.NewMultiaddr(data.DesAddr)
	//dial
	conn, errDial := d.networkSlave.Dial(context.Background(), stunClientAddr)
	if errDial != nil {
		log.Warn("handlerNotifyMsg Dial err:", errDial)
	}
	if conn == nil {
		return
	}
	//stream
	stream, errStream := conn.CreateSendStream()
	if errStream != nil {
		log.Warn("handlerNotifyMsg CreateSendStream err:", errStream)
	}
	if stream == nil {
		return
	}
	//write
	_, errW := stream.Write(data.Raw)
	var sendRes string
	if errW != nil {
		sendRes = fmt.Sprintf("send msg to %v err:%v", stunClientAddr, errW)
	} else {
		sendRes = fmt.Sprintf("send msg to %v ok", stunClientAddr)
	}
	log.Info(sendRes)
	//readLen, remoteAddr, errRead := conn.ReadFrom(data)
	//fmt.Printf("localAdd(%v) recv from server(%v) len(%v) err:%v  ", lAddr, remoteAddr, readLen, errRead)
	_, _ = w.Write([]byte(sendRes))
}
