package stun

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/pion/stun"
)

const messageHeaderSize = 20

const (
	// ChangeIpPort 6
	ChangeIpPort = 6
	// ChangeIp 4
	ChangeIp = 4
	// ChangePort 2
	ChangePort = 2
)

// Data for stun
type Data []byte

// AddTo adds DATA to message.
func (d Data) AddTo(m *stun.Message) error {
	m.Add(stun.AttrData, d)
	return nil
}

// GetFrom decodes DATA from message.
func (d *Data) GetFrom(m *stun.Message) error {
	v, err := m.Get(stun.AttrData)
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// String
func (d *Data) String() string {
	if d == nil {
		return ""
	}
	return string(*d)
}

//stun msg with attr
type stunRes struct {
	// local public addr
	xorAddr *stun.XORMappedAddress
	// respOrigin *stun.ResponseOrigin
	mappedAddr *stun.MappedAddress
	// another stun server addr
	otherAddr *stun.OtherAddress
	// same function with magic
	software *stun.Software
	// username
	username *stun.Username
	// data
	data *Data
}

// genMessage
// handle stun msg and gen res
// none of business with net
// only about stun msg
// res is the response msg
// pAttType is the type of change type
func (d *defaultStunServer) genMessage(msg ma.Multiaddr, b []byte, req, res *stun.Message, pChangeType *int) error {
	log := d.log
	// msgFromAddr
	msgFromAddr := msg
	if !stun.IsMessage(b) {
		return errNotSTUNMessage
	}
	if _, err := req.Write(b); err != nil {
		return errors.New(err.Error() + "failed to read message")
	}
	//获得udp客户端的ip和port
	clientIp, clientPort := mAddrToIpPort(msgFromAddr)
	udpClientIp := net.ParseIP(clientIp)
	udpClientPort := clientPort
	// stun msg
	m := new(stun.Message)
	m.Raw = b
	if err := m.Decode(); err != nil {
		log.Error("Unable to decode message:", err)
	}

	log.Infof("recv from %v type.method(%v) lenA:%v ", msgFromAddr, m.Type.Method.String(), len(m.Attributes))
	ip, port := mAddrToIpPort(d.otherStunServerAddr)
	pOtherAddress := &stun.OtherAddress{
		IP:   net.ParseIP(ip),
		Port: port,
	}

	userName := ""
	natType := ""
	for k, v := range m.Attributes {
		log.Debug("got Attributes", k, v.Type.String())
		switch v.Type {
		case stun.AttrChangeRequest:
			//get change value
			value := binary.BigEndian.Uint32(v.Value)
			*pChangeType = int(value)
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
	err := res.Build(req,
		stun.BindingSuccess,
		software,
		&stun.XORMappedAddress{
			IP:   udpClientIp,
			Port: udpClientPort,
		},
		pOtherAddress,
		stun.Fingerprint,
	)

	//handle nat type set or get
	if len(userName) > 0 && len(natType) > 0 {
		log.Debug(".peerNameNat.Store:", userName, natType)
		d.peerNameNat.Store(userName, natType)
	} else if len(userName) > 0 {
		natType, _ := d.peerNameNat.Load(userName)
		log.Debug(".peerNameNat.Load:", userName, natType)
		if natType != nil {
			res.Add(stun.AttrUsername, []byte(userName))
			res.Add(stun.AttrData, []byte(natType.(string)))
		}
	}

	return err
}

// parseStunMsg
// parse StunMsg to stunRes
func parseStunMsg(msg *stun.Message) (ret stunRes) {
	//log := d.log
	ret.mappedAddr = &stun.MappedAddress{}
	ret.xorAddr = &stun.XORMappedAddress{}
	//ret.respOrigin = &stun.ResponseOrigin{}
	ret.otherAddr = &stun.OtherAddress{}
	ret.software = &stun.Software{}
	ret.username = &stun.Username{}
	ret.data = &Data{}
	if ret.xorAddr.GetFrom(msg) != nil {
		ret.xorAddr = nil
	}
	if ret.otherAddr.GetFrom(msg) != nil {
		ret.otherAddr = nil
	}
	//if ret.respOrigin.GetFrom(msg) != nil {
	//	ret.respOrigin = nil
	//}
	if ret.mappedAddr.GetFrom(msg) != nil {
		ret.mappedAddr = nil
	}
	if ret.software.GetFrom(msg) != nil {
		ret.software = nil
	}
	if ret.username.GetFrom(msg) != nil {
		ret.username = nil
	}
	if ret.data.GetFrom(msg) != nil {
		ret.data = nil
	}

	//for _, attr := range msg.Attributes {
	//	switch attr.Type {
	//	case
	//		stun.AttrXORMappedAddress,
	//		stun.AttrOtherAddress,
	//		//stun.AttrResponseOrigin,
	//		stun.AttrMappedAddress,
	//		stun.AttrSoftware:
	//		break
	//	default:
	//	}
	//}
	return ret
}

// mAddrToIpPort
// ma.Multiaddr to ip and port
func mAddrToIpPort(addr ma.Multiaddr) (string, int) {
	tmp := addr.String()
	r := strings.Split(tmp, "/")
	port := r[4]
	nPort, _ := strconv.Atoi(port)
	return r[2], nPort
}

// mAddrResetIpPort
// ma.Multiaddr reset ip and port
func mAddrResetIpPort(addr ma.Multiaddr, ip string, port int) ma.Multiaddr {
	tmp := addr.String()
	r := strings.Split(tmp, "/")
	var strRes string
	for n, v := range r {
		if n > 0 {
			strRes += "/"
		}
		switch n {
		case 2:
			strRes += ip
		case 4:
			strRes += fmt.Sprintf("%v", port)
		default:
			strRes += v
		}
	}
	res, _ := ma.NewMultiaddr(strRes)
	return res
}

// addrToMAddr
// gen ma.Multiaddr
func addrToMAddr(ipv, addr, network string) ma.Multiaddr {
	r := strings.Split(addr, ":")
	tmp := fmt.Sprintf("/%v/%v/udp/%v/%v", ipv, r[0], r[1], network)
	mAddr, _ := ma.NewMultiaddr(tmp)
	return mAddr
}
