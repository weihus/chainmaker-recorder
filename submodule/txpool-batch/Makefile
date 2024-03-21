VERSION=v2.3.0_qc

gomod:
	go get chainmaker.org/chainmaker/common/v2@$(VERSION)
	go get chainmaker.org/chainmaker/localconf/v2@$(VERSION)
	go get chainmaker.org/chainmaker/lws@v1.1.0_qc
	go get chainmaker.org/chainmaker/pb-go/v2@$(VERSION)
	go get chainmaker.org/chainmaker/protocol/v2@$(VERSION)
	go get chainmaker.org/chainmaker/utils/v2@$(VERSION)

	go mod tidy

ut:
	mkdir -p ../ut
	go test -v -coverprofile=../ut/txpool-batch.out ./...
	go tool cover -html=../ut/txpool-batch.out -o ../ut/txpool-batch.html
