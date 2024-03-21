package exMultiaddr

import ma "github.com/multiformats/go-multiaddr"

// ExInfoMultiaddr implement Multiaddr
// and add exInfo
// to resolve Dial have only one para
type ExInfoMultiaddr struct {
	ma.Multiaddr
	Info string
}

// GetExInfo return the Info
// avoid change go-multiaddr or broke Dial
// of course could add other param
func (e *ExInfoMultiaddr) GetExInfo() string {
	return e.Info
}
