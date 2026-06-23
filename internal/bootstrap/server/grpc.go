// Package server hosts bootstrap helpers shared by the cefasdb
// binary. Its purpose is to keep cmd/cefasdb/main.go small by
// holding pure, testable builders for server-side wiring (gRPC, TLS,
// interceptors).
package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/CefasDb/cefasdb/internal/auth"
	apiserver "github.com/CefasDb/cefasdb/internal/server"
)

// BuildGRPCOpts assembles ServerOptions for the gRPC server: auth
// interceptors (if a validator is configured) + TLS / mTLS credentials
// when cert paths are supplied.
func BuildGRPCOpts(v *auth.Validator, certPath, keyPath, caBundle string) ([]grpc.ServerOption, error) {
	var opts []grpc.ServerOption

	if certPath != "" || keyPath != "" {
		if certPath == "" || keyPath == "" {
			return nil, fmt.Errorf("both -tls-cert and -tls-key must be set together")
		}
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load tls cert: %w", err)
		}
		tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
		if caBundle != "" {
			caPEM, err := os.ReadFile(caBundle)
			if err != nil {
				return nil, fmt.Errorf("read mtls ca: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("mtls ca bundle has no PEM certs")
			}
			tlsCfg.ClientCAs = pool
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}

	// Workload prioritization (#489): tag every request with the
	// resolved service level. Runs in dev mode too so handlers can
	// observe the tag without auth enabled.
	slUnary, slStream := apiserver.ServiceLevelInterceptor()
	unaryChain := []grpc.UnaryServerInterceptor{slUnary}
	streamChain := []grpc.StreamServerInterceptor{slStream}

	if v != nil {
		// Reflection probe stays available without a token so
		// `grpcurl -plaintext localhost:9090 list` works in dev.
		skip := map[string]bool{
			"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      true,
			"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": true,
			"/cefas.v1.Cefas/ClusterStatus":                                  true,
		}
		authUnary, authStream := apiserver.AuthInterceptor(v, skip)
		if authUnary != nil {
			unaryChain = append(unaryChain, authUnary)
		}
		if authStream != nil {
			streamChain = append(streamChain, authStream)
		}
	}

	opts = append(opts, grpc.ChainUnaryInterceptor(unaryChain...))
	opts = append(opts, grpc.ChainStreamInterceptor(streamChain...))
	return opts, nil
}
