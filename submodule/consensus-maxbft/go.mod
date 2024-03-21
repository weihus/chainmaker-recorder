module chainmaker.org/chainmaker/consensus-maxbft/v2

go 1.15

require (
	chainmaker.org/chainmaker/chainconf/v2 v2.3.1
	chainmaker.org/chainmaker/common/v2 v2.3.1
	chainmaker.org/chainmaker/consensus-utils/v2 v2.3.2
	chainmaker.org/chainmaker/localconf/v2 v2.3.1
	chainmaker.org/chainmaker/lws v1.1.0
	chainmaker.org/chainmaker/pb-go/v2 v2.3.2
	chainmaker.org/chainmaker/protocol/v2 v2.3.2
	chainmaker.org/chainmaker/utils/v2 v2.3.2
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/panjf2000/ants/v2 v2.4.8
	github.com/stretchr/testify v1.7.0
	github.com/tmthrgd/go-hex v0.0.0-20190904060850-447a3041c3bc
	go.uber.org/atomic v1.7.0
)

replace (
	github.com/libp2p/go-libp2p-core => chainmaker.org/chainmaker/libp2p-core v1.0.0
	github.com/linvon/cuckoo-filter => chainmaker.org/third_party/cuckoo-filter v1.0.0
	github.com/lucas-clemente/quic-go v0.26.0 => chainmaker.org/third_party/quic-go v1.0.0
	github.com/marten-seemann/qtls-go1-15 => chainmaker.org/third_party/qtls-go1-15 v1.0.0
	github.com/marten-seemann/qtls-go1-16 => chainmaker.org/third_party/qtls-go1-16 v1.0.0
	github.com/marten-seemann/qtls-go1-17 => chainmaker.org/third_party/qtls-go1-17 v1.0.0
	github.com/marten-seemann/qtls-go1-18 => chainmaker.org/third_party/qtls-go1-18 v1.0.0
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)
