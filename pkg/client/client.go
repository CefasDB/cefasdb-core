package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// Client is a typed cefas gRPC client. Safe for concurrent use.
type Client struct {
	conn       *grpc.ClientConn
	stub       cefaspb.CefasClient
	bearer     string
	routeReads *routeAwareReads
}

// Option configures a Client at Dial time.
type Option func(*config)

type config struct {
	bearer             string
	tls                *tls.Config
	plaintext          bool
	dialOpts           []grpc.DialOption
	routeReadEndpoints map[string]string
}

// WithBearer adds an "Authorization: Bearer <token>" metadata header
// to every RPC.
func WithBearer(token string) Option {
	return func(c *config) { c.bearer = token }
}

// WithTLS enables transport encryption using the supplied tls.Config.
// Pass &tls.Config{} for the system roots, or build a custom config
// for mTLS.
func WithTLS(cfg *tls.Config) Option { return func(c *config) { c.tls = cfg } }

// WithPlaintext disables transport security. Required for local dev
// against a -grpc-reflection enabled server with no TLS cert.
func WithPlaintext() Option { return func(c *config) { c.plaintext = true } }

// WithMTLSFiles wires mTLS from filesystem paths: client cert + key
// the server verifies, plus the CA bundle that signed the server's
// certificate.
func WithMTLSFiles(certPath, keyPath, serverCAPath string) Option {
	return func(c *config) {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			c.dialOpts = append(c.dialOpts, grpc.WithDisableHealthCheck()) // no-op marker
			c.tls = &tls.Config{InsecureSkipVerify: false}
			fmt.Fprintf(os.Stderr, "cefas/client: load client cert: %v\n", err)
			return
		}
		pool := x509.NewCertPool()
		if pem, err := os.ReadFile(serverCAPath); err == nil {
			pool.AppendCertsFromPEM(pem)
		}
		c.tls = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
		}
	}
}

// WithDialOption appends a raw grpc.DialOption (escape hatch for
// keepalive, retry policies, etc.).
func WithDialOption(o grpc.DialOption) Option {
	return func(c *config) { c.dialOpts = append(c.dialOpts, o) }
}

// WithRouteAwareReads enables token-aware eventual reads. nodeEndpoints
// maps placement node IDs to their gRPC endpoints.
func WithRouteAwareReads(nodeEndpoints map[string]string) Option {
	return func(c *config) {
		if len(nodeEndpoints) == 0 {
			c.routeReadEndpoints = nil
			return
		}
		c.routeReadEndpoints = make(map[string]string, len(nodeEndpoints))
		for id, endpoint := range nodeEndpoints {
			if id == "" || endpoint == "" {
				continue
			}
			c.routeReadEndpoints[id] = endpoint
		}
	}
}

// Dial opens a connection to a cefas server.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}
	dialOpts := append([]grpc.DialOption{}, cfg.dialOpts...)
	switch {
	case cfg.tls != nil:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(cfg.tls)))
	case cfg.plaintext:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	default:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("cefas: dial %s: %w", addr, err)
	}
	c := &Client{conn: conn, stub: cefaspb.NewCefasClient(conn), bearer: cfg.bearer}
	if len(cfg.routeReadEndpoints) > 0 {
		routeReads, err := newRouteAwareReads(cfg.routeReadEndpoints, dialOpts)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		c.routeReads = routeReads
	}
	return c, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	var err error
	if c.routeReads != nil {
		err = c.routeReads.close()
	}
	if cerr := c.conn.Close(); err == nil {
		err = cerr
	}
	return err
}

// withAuth augments outgoing metadata with the bearer token (when set).
func (c *Client) withAuth(ctx context.Context) context.Context {
	if c.bearer == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.bearer)
}
