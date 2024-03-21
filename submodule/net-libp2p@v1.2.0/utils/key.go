/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	gocrypto "crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"errors"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/tjfoc/gmsm/sm2"
)

// ParsePublicKeyToPubKey parse a crypto.PublicKey to a libp2p-crypto.PubKey.
func ParsePublicKeyToPubKey(publicKey gocrypto.PublicKey) (crypto.PubKey, error) {
	switch p := publicKey.(type) {
	case *ecdsa.PublicKey:
		if p.Curve == sm2.P256Sm2() {
			pub := &sm2.PublicKey{
				Curve: p.Curve,
				X:     p.X,
				Y:     p.Y,
			}
			return crypto.NewSM2PublicKey(pub), nil
		}
		return crypto.NewECDSAPublicKey(p), nil
	case *sm2.PublicKey:
		return crypto.NewSM2PublicKey(p), nil
	case *rsa.PublicKey:
		return crypto.NewRsaPublicKey(*p), nil
	}
	return nil, errors.New("unsupported public key type")
}
