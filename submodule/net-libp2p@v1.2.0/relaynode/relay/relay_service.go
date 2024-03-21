/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package relay

import (
	"fmt"

	"chainmaker.org/chainmaker/logger/v2"
	"chainmaker.org/chainmaker/protocol/v2"
	"github.com/spf13/viper"
)

// RelayService relay service, provide relay function
type RelayService struct {
	readySignalC chan struct{}

	// net instance
	localNet protocol.Net

	// logger 对象
	logger protocol.Logger
}

// LoadRelayCfg load relay config file
func LoadRelayCfg(configPath string) (*RelayConfig, error) {
	var conf = new(RelayConfig)

	// set config file path
	viper.SetConfigFile(configPath)

	// set config file type
	//viper.SetConfigType("yaml")

	// load config file
	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("load config err, %v", err)
	}

	err = viper.Unmarshal(conf)
	if err != nil {
		return nil, fmt.Errorf("load config and unmarshal err, %v", err)
	}
	return conf, nil
}

// global logger
var rlogger protocol.Logger

// NewRelayService create a relay service  instance
func NewRelayService(cfg *RelayConfig) (*RelayService, error) {

	// create logger instance
	rlogger = logger.GetLogger(logger.MODULE_NET)

	readySignalC := make(chan struct{})

	// create net instance
	localNet, err := NewNet(&cfg.NetConfig, readySignalC)
	if err != nil {
		return nil, err
	}

	// create relay service
	relayService := &RelayService{
		localNet:     localNet,
		logger:       rlogger,
		readySignalC: readySignalC,
	}

	return relayService, nil
}

// Start RelayService start
func (rs *RelayService) Start() error {
	// net start
	err := rs.localNet.Start()

	if err != nil {
		return err
	}
	close(rs.readySignalC)

	return nil
}

// Stop RelayService stop
func (rs *RelayService) Stop() error {
	return rs.localNet.Stop()
}
