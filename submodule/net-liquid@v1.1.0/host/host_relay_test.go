/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package host

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"chainmaker.org/chainmaker/common/v2/crypto/asym"
	cmTls "chainmaker.org/chainmaker/common/v2/crypto/tls"
	cmx509 "chainmaker.org/chainmaker/common/v2/crypto/x509"
	"chainmaker.org/chainmaker/common/v2/helper"
	"chainmaker.org/chainmaker/net-liquid/core/host"
	"chainmaker.org/chainmaker/net-liquid/core/peer"
	"chainmaker.org/chainmaker/net-liquid/core/protocol"
	"chainmaker.org/chainmaker/net-liquid/logger"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

//ip4/7.7.7.7/tcp/55555/p2p/QmRelay/p2p-circuit/p2p/QmAlice
var (
	addrRA = ma.StringCast("/ip4/127.0.0.1/tcp/8082/p2p/" + pidList[1].ToString() + "/p2p-circuit/p2p/" + pidList[0].ToString())

	addrR  = ma.StringCast("/ip4/127.0.0.1/tcp/8082/p2p/" + pidList[1].ToString())
	addrRB = ma.StringCast("/ip4/127.0.0.1/tcp/8082/p2p/" + pidList[1].ToString() + "/p2p-circuit/p2p/" + pidList[2].ToString())
)

func CreateHostRelay(idx int, seeds map[peer.ID]ma.Multiaddr) (host.Host, error) {
	certPool := cmx509.NewCertPool()
	for i := range certPEMs {
		certPool.AppendCertsFromPEM(certPEMs[i])
	}
	sk, err := asym.PrivateKeyFromPEM(keyPEMs[idx], nil)
	if err != nil {
		return nil, err
	}
	tlsCert, err := cmTls.X509KeyPair(certPEMs[idx], keyPEMs[idx])
	if err != nil {
		return nil, err
	}
	hostCfg := &HostConfig{
		TlsCfg: &cmTls.Config{
			Certificates:       []cmTls.Certificate{tlsCert},
			InsecureSkipVerify: true,
			ClientAuth:         cmTls.RequireAnyClientCert,
			VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*cmx509.Certificate) error {
				tlsCertBytes := rawCerts[0]
				cert, err := cmx509.ParseCertificate(tlsCertBytes)
				if err != nil {
					return err
				}
				_, err = cert.Verify(cmx509.VerifyOptions{Roots: certPool})
				if err != nil {
					return err
				}
				return nil
			},
		},
		LoadPidFunc: func(certificates []*cmx509.Certificate) (peer.ID, error) {
			pid, err := helper.GetLibp2pPeerIdFromCertDer(certificates[0].Raw)
			if err != nil {
				return "", err
			}
			return peer.ID(pid), err
		},
		SendStreamPoolInitSize:    10,
		SendStreamPoolCap:         50,
		PeerReceiveStreamMaxCount: 100,
		ListenAddresses:           []ma.Multiaddr{addrsTcp[idx]},
		DirectPeers:               seeds,
		MsgCompress:               false,
		Insecurity:                false,
		PrivateKey:                sk,
	}

	return hostCfg.NewHost(context.Background(), TcpNetwork, logger.NewLogPrinter("HOST"+strconv.Itoa(idx)))
}

func TestHostRelay(t *testing.T) {

	// create hostA
	host1, err := CreateHostRelay(0, map[peer.ID]ma.Multiaddr{pidList[1]: addrR,
		pidList[2]: addrRB})
	//host1, err := CreateHostRelay(0, map[peer.ID]ma.Multiaddr{pidList[1]: addrR})
	require.Nil(t, err)
	// create host2
	host2, err := CreateHostRelay(1, map[peer.ID]ma.Multiaddr{pidList[1]: addrR})
	require.Nil(t, err)
	// create host3
	host3, err := CreateHostRelay(2, map[peer.ID]ma.Multiaddr{pidList[1]: addrR,
		pidList[0]: addrRA})
	//host3, err := CreateHostRelay(2, map[peer.ID]ma.Multiaddr{pidList[1]: addrR})
	require.Nil(t, err)

	// register notifee
	connectC := make(chan struct{}, 3)
	disconnectC := make(chan struct{})
	protocolSupportC := make(chan struct{})
	protocolUnsupportedC := make(chan struct{})
	notifeeBundle := &host.NotifieeBundle{
		PeerConnectedFunc: func(id peer.ID) {
			fmt.Println("PeerConnectedFunc==", id)
			connectC <- struct{}{}
		},
		PeerDisconnectedFunc: func(id peer.ID) {
			disconnectC <- struct{}{}
		},
		PeerProtocolSupportedFunc: func(protocolID protocol.ID, pid peer.ID) {
			fmt.Println("protocolID==", protocolID)
			if protocolID == testProtocolID {
				protocolSupportC <- struct{}{}
			}
		},
		PeerProtocolUnsupportedFunc: func(protocolID protocol.ID, pid peer.ID) {
			protocolUnsupportedC <- struct{}{}
		},
	}
	host1.Notify(notifeeBundle)
	host2.Notify(notifeeBundle)
	host3.Notify(notifeeBundle)

	// start hosts
	err = host2.Start()
	require.Nil(t, err)
	err = host1.Start()
	require.Nil(t, err)
	err = host3.Start()
	require.Nil(t, err)

	// wait for connection established between host1, host2 and host3
	timer := time.NewTimer(10 * time.Second)
	for i := 0; i < 6; i++ {
		select {
		case <-timer.C:
			t.Fatal("connection establish timeout")
		case <-connectC:
		}
	}

	// register msg payload handler
	receiveC := make(chan struct{})
	err = host1.RegisterMsgPayloadHandler(testProtocolID, func(senderPID peer.ID, msgPayload []byte) {
		receiveC <- struct{}{}
	})
	require.Nil(t, err)

	err = host2.RegisterMsgPayloadHandler(testProtocolID, func(senderPID peer.ID, msgPayload []byte) {
		receiveC <- struct{}{}
	})
	require.Nil(t, err)

	err = host3.RegisterMsgPayloadHandler(testProtocolID, func(senderPID peer.ID, msgPayload []byte) {
		receiveC <- struct{}{}
	})
	require.Nil(t, err)

	timer = time.NewTimer(5 * time.Second)
	for i := 0; i < 6; i++ {
		select {
		case <-timer.C:
			t.Fatal("push protocol supported timeout")
		case <-protocolSupportC:

		}
	}

	// host1 send msg to host3
	err = host1.SendMsg(testProtocolID, pidList[2], []byte(msg))
	require.Nil(t, err)
	timer = time.NewTimer(5 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("host1 send msg to host2 timeout")
	case <-receiveC:

	}

	// host3 send msg to host1
	err = host3.SendMsg(testProtocolID, pidList[0], []byte(msg))
	require.Nil(t, err)
	timer = time.NewTimer(5 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("host2 send msg to host1 timeout")
	case <-receiveC:

	}

	bl := host1.IsPeerSupportProtocol(host3.ID(), testProtocolID)
	require.True(t, bl)

	// unregister msg payload handler
	err = host3.UnregisterMsgPayloadHandler(testProtocolID)
	require.Nil(t, err)
	timer = time.NewTimer(5 * time.Second)

	for i := 0; i < 2; i++ {
		select {
		case <-timer.C:
			t.Fatal("push protocol unsupported timeout")
		case <-protocolUnsupportedC:

		}
	}

	bl = host1.IsPeerSupportProtocol(host3.ID(), testProtocolID)
	require.True(t, !bl)

	// stop host2
	err = host3.Stop()
	require.Nil(t, err)

	timer = time.NewTimer(5 * time.Second)
	select {
	case <-timer.C:
		t.Fatal("peer disconnect notify timeout")
	case <-disconnectC:

	}

	// stop host1
	err = host1.Stop()
	require.Nil(t, err)

	// stop host2
	err = host2.Stop()
	require.Nil(t, err)
}
