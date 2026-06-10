package identitymiddleware

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
)

type jwksDoc struct {
	Keys []jwksKey `json:"keys"`
}

type jwksKey struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func parseJWKS(data []byte) (map[string]*rsa.PublicKey, []*rsa.PublicKey, error) {
	var doc jwksDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, nil, err
	}
	keys := make(map[string]*rsa.PublicKey)
	all := make([]*rsa.PublicKey, 0, len(doc.Keys))
	for _, k := range doc.Keys {
		if k.Kty != "RSA" {
			continue
		}
		pub, err := toRSAPublicKey(k.N, k.E)
		if err != nil {
			continue
		}
		all = append(all, pub)
		if k.Kid != "" {
			keys[k.Kid] = pub
		}
	}
	if len(all) == 0 {
		return nil, nil, errors.New("jwks: no rsa keys found")
	}
	return keys, all, nil
}

func toRSAPublicKey(nStr, eStr string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nStr)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eStr)
	if err != nil {
		return nil, err
	}
	n := new(big.Int).SetBytes(nBytes)
	e := new(big.Int).SetBytes(eBytes)
	if e.Int64() == 0 {
		return nil, errors.New("jwks: invalid exponent")
	}
	return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
}
