package main

import (
	"errors"
	"io/ioutil"
	"time"

	api "chainmaker.org/chainmaker/protocol/v2"

	"chainmaker.org/chainmaker/net-liquid/stun"
	ma "github.com/multiformats/go-multiaddr"

	"chainmaker.org/chainmaker/net-liquid/logger"

	"gopkg.in/yaml.v2"
)

//a stun server implement with udp
//config save in stunserver.yaml
func main() {
	log := logger.NewLogPrinter("stunServer")
	if err := start(log); err != nil {
		log.Error(err)
		return
	}
}

//start listen and block
func start(log api.Logger) error {
	//config file
	yamlFile, err := ioutil.ReadFile("stunserver.yml")
	if err != nil {
		return errors.New("ReadFile err:" + err.Error())
	}
	pStunConfig := &stunConfig{}
	err = yaml.Unmarshal(yamlFile, pStunConfig)
	if err != nil {
		return errors.New("yaml.Unmarshal err:" + err.Error())
	}
	if !pStunConfig.StunServer.Enabled {
		return errors.New("!Enabled")
	}

	//config
	stunServerAddr1, _ := ma.NewMultiaddr(pStunConfig.StunServer.ListenAddr1)
	stunServerAddr2, _ := ma.NewMultiaddr(pStunConfig.StunServer.ListenAddr2)
	// return to client
	otherServerAddr, _ := ma.NewMultiaddr(pStunConfig.StunServer.OtherStunServerAddr)
	// slave addr
	stunServerAddr3, _ := ma.NewMultiaddr(pStunConfig.StunServer.ListenAddr3)
	stunServerAddr4, _ := ma.NewMultiaddr(pStunConfig.StunServer.ListenAddr4)
	localHttpAddr := pStunConfig.StunServer.LocalNotifyAddr
	otherHttpAddr := pStunConfig.StunServer.OtherNotifyAddr
	//new a server
	var stunServer stun.Server
	stunServer, _ = stun.NewUdpStunServer(log)

	//how to deal change req
	if pStunConfig.StunServer.TwoPublicAddress {
		changeParam := &stun.ChangeParam{
			OtherAddr:             otherServerAddr,
			OtherMasterListenAddr: stunServerAddr3,
			OtherSlaveListenAddr:  stunServerAddr4,
		}
		err = stunServer.InitChangeIpLocal(changeParam)
	} else {
		changeParam2 := &stun.ChangeNotifyParam{
			OtherAddr:       otherServerAddr,
			NotifyAddr:      otherHttpAddr,
			LocalNotifyAddr: localHttpAddr,
		}
		err = stunServer.InitChangeIpNotify(changeParam2)
	}
	if err != nil {
		return errors.New("InitChangeIp func err:" + err.Error())
	}
	//listen
	err = stunServer.Listen(stunServerAddr1, stunServerAddr2)
	if err != nil {
		return errors.New("stunServer Listen err:" + err.Error())
	}
	for {
		//todo
		time.Sleep(1 * time.Second)
	}
	//return stunServer.CloseListen()
}

//stun config
type stunConfig struct {
	StunClient  stunClient `yaml:"stunClient"`
	StunServer  stunServer `yaml:"stunServer"`
	EnablePunch bool       `yaml:"enablePunch"`
}

//client config
type stunClient struct {
	Enabled        bool   `yaml:"enabled"`
	ListenAddr     string `yaml:"listen_addr"`
	StunServerAddr string `yaml:"stun_server_addr"`
}

//server config
type stunServer struct {
	Enabled             bool   `yaml:"enabled"`
	OtherStunServerAddr string `yaml:"other_stun_server_addr"`
	ListenAddr1         string `yaml:"listen_addr1"`
	ListenAddr2         string `yaml:"listen_addr2"`
	TwoPublicAddress    bool   `yaml:"two_public_address"`
	ListenAddr3         string `yaml:"listen_addr3"`
	ListenAddr4         string `yaml:"listen_addr4"`
	LocalNotifyAddr     string `yaml:"local_notify_addr"`
	OtherNotifyAddr     string `yaml:"other_notify_addr"`
}
