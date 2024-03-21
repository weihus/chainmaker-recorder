package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"chainmaker.org/chainmaker/net-libp2p/relaynode/relay"
)

func main() {

	// get the configuration file from the command line argument
	var configPath string
	flag.StringVar(&configPath, "cfg", "", "-cfg configuration file path, specify the configuration file path")
	flag.Parse()

	if len(configPath) == 0 {
		log.Fatalln("please specify the configuration file path")
	}

	// load config file
	relayCfg, err := relay.LoadRelayCfg(configPath)
	if err != nil {
		log.Fatalln(err)
	}

	// create relay service
	relayService, err := relay.NewRelayService(relayCfg)
	if err != nil {
		log.Fatalln(err)
	}

	// start relay service
	err = relayService.Start()
	if err != nil {
		log.Fatalln(err)
	}

	// listen interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	<-interrupt

	// stop
	err = relayService.Stop()
	if err != nil {
		log.Fatalln(err)
	}

}
