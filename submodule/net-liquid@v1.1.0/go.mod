module chainmaker.org/chainmaker/net-liquid

go 1.15

require (
	chainmaker.org/chainmaker/common/v2 v2.3.1
	chainmaker.org/chainmaker/logger/v2 v2.3.0
	chainmaker.org/chainmaker/net-common v1.2.2
	chainmaker.org/chainmaker/protocol/v2 v2.3.2
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/gogo/protobuf v1.3.2
	github.com/libp2p/go-yamux/v2 v2.2.0
	github.com/lucas-clemente/quic-go v0.26.0
	github.com/multiformats/go-multiaddr v0.3.2
	github.com/multiformats/go-multiaddr-fmt v0.1.0
	github.com/pion/stun v0.3.5
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/tjfoc/gmsm v1.4.1
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a
	gopkg.in/yaml.v2 v2.4.0
)

replace (
	github.com/lucas-clemente/quic-go v0.26.0 => chainmaker.org/third_party/quic-go v1.1.0
	github.com/marten-seemann/qtls-go1-16 => chainmaker.org/third_party/qtls-go1-16 v1.1.0
	github.com/marten-seemann/qtls-go1-17 => chainmaker.org/third_party/qtls-go1-17 v1.1.0
	github.com/marten-seemann/qtls-go1-18 => chainmaker.org/third_party/qtls-go1-18 v1.1.0
	github.com/marten-seemann/qtls-go1-19 => chainmaker.org/third_party/qtls-go1-19 v1.0.0
)
