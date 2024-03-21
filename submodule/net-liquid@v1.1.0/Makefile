VERSION=v2.3.0_qc
gomod:
	go get chainmaker.org/chainmaker/common/v2@$(VERSION)
	go get chainmaker.org/chainmaker/protocol/v2@$(VERSION)
	go get chainmaker.org/chainmaker/logger/v2@$(VERSION)
	go get chainmaker.org/chainmaker/net-common@v1.2.0_qc
	go mod tidy
	cat go.mod|grep chainmaker

