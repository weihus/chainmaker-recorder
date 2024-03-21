package stun

import (
	"testing"

	"chainmaker.org/chainmaker/net-liquid/logger"
)

func TestUdpStunClient(t *testing.T) {
	log := logger.NewLogPrinter("stunTest")
	var client Client
	client, _ = NewUdpStunClient(log)
	client.GetMappingType()
}
