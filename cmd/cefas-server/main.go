// cefas-server is the cefas database binary. In single-node mode it
// opens Pebble, loads the catalog, and serves HTTP/JSON. With the
// -raft-bootstrap or -raft-join flags it additionally wires raft
// replication so writes flow through the consensus log.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	craft "github.com/osvaldoandrade/cefas/internal/raft"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/internal/tracing"
	"github.com/osvaldoandrade/cefas/pkg/api"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/config"
)

func main() {
	var (
		dataDir  = flag.String("data", "./cefas-data", "Pebble data directory")
		httpAddr = flag.String("http", ":8080", "HTTP listen address")
		fsync    = flag.Bool("fsync", false, "fsync on commit (durability over throughput)")

		// Raft mode flags. Empty raft-bind keeps the server in
		// single-node mode (Phase 1-3 behaviour).
		raftBind      = flag.String("raft-bind", "", "Raft TCP bind address (enables Raft mode)")
		raftID        = flag.String("raft-id", "", "Unique raft ServerID for this node")
		raftPath      = flag.String("raft-path", "", "Raft state path (snapshots/, etc.). Defaults to -data/raft")
		raftBootstrap = flag.Bool("raft-bootstrap", false, "Bootstrap a new cluster from -raft-peers (run on the first node only)")
		raftPeersFlag = flag.String("raft-peers", "", "Comma-separated id=raftAddr peer list, e.g. 'a=127.0.0.1:9001,b=127.0.0.1:9002,c=127.0.0.1:9003'")
		raftHTTPFlag  = flag.String("raft-http-peers", "", "Comma-separated id=httpURL peer list for 307 redirects, e.g. 'a=http://h1:8080,b=http://h2:8080'")

		// Identity/auth flags. Empty -identity-jwks-url keeps the
		// server open (single-node dev mode).
		identityJwks      = flag.String("identity-jwks-url", "", "Tikti JWKS endpoint (enables bearer-token auth)")
		identityIssuer    = flag.String("identity-issuer", "", "Expected token issuer")
		identityAudience  = flag.String("identity-audience", "", "Expected token audience")
		identityClockSkew = flag.Duration("identity-clock-skew", 30*time.Second, "Allowed clock skew on exp/iat checks")

		// Multi-Raft sharding.
		shardsN     = flag.Int("shards", 0, "Number of shards (multi-Raft). 0 → single-shard / single-node legacy bootstrap.")
		muxAddr     = flag.String("mux", "", "Mux TCP address shared by every shard's raft transport (multi-Raft mode).")

		// gRPC flags.
		grpcAddr       = flag.String("grpc", "", "gRPC listen address (e.g. ':9090'). Empty disables gRPC.")
		grpcReflection = flag.Bool("grpc-reflection", false, "Enable gRPC server reflection (handy for grpcurl)")
		tlsCert        = flag.String("tls-cert", "", "Path to TLS certificate (PEM). Enables TLS on the gRPC listener.")
		tlsKey         = flag.String("tls-key", "", "Path to TLS private key (PEM)")
		mtlsCA         = flag.String("mtls-ca", "", "Path to a client-CA bundle. When set, the gRPC listener requires mTLS.")

		// Observability + config.
		configPath  = flag.String("config", "", "Path to YAML config file. Flag/env values override the file.")
		metricsOff  = flag.Bool("metrics-disabled", false, "Disable the /metrics Prometheus endpoint.")
		tracingURL  = flag.String("tracing-endpoint", "", "OTLP/gRPC collector endpoint (e.g. 'jaeger:4317'). Empty disables tracing.")
		tracingIns  = flag.Bool("tracing-insecure", true, "Disable TLS to the OTLP collector.")
	)
	flag.Parse()

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if err := config.ApplyEnv(&cfg); err != nil {
		log.Fatalf("config env: %v", err)
	}
	// Promote any flag value the user actually set onto cfg so the
	// downstream code paths can read a single source of truth.
	overlayFlags(&cfg, *dataDir, *httpAddr, *fsync,
		*raftBind, *raftID, *raftPath, *raftBootstrap, *raftPeersFlag, *raftHTTPFlag,
		*identityJwks, *identityIssuer, *identityAudience, *identityClockSkew,
		*shardsN, *muxAddr,
		*grpcAddr, *grpcReflection, *tlsCert, *tlsKey, *mtlsCA,
		*metricsOff, *tracingURL, *tracingIns)

	// Initialise tracing first so subsequent setup gets spans on
	// failure. tracingShutdown is a no-op when no endpoint is set.
	tracingShutdown, err := tracing.Init(context.Background(), tracing.Config{
		Endpoint:   cfg.Tracing.Endpoint,
		Insecure:   cfg.Tracing.Insecure,
		SampleRate: cfg.Tracing.SampleRate,
	})
	if err != nil {
		log.Fatalf("tracing: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tracingShutdown(ctx)
	}()

	// Metrics: always-on unless explicitly disabled.
	var prom *metrics.Metrics
	if cfg.Metrics.Enabled {
		prom = metrics.New()
	}

	var (
		db     *storage.DB
		cat    *catalog.Catalog
		mgr    *cluster.Manager
		raftDB *craft.DB
	)

	if *shardsN > 0 {
		peers, err := parsePeers(*raftPeersFlag)
		if err != nil {
			log.Fatalf("-raft-peers: %v", err)
		}
		httpPeers, err := parsePeers(*raftHTTPFlag)
		if err != nil {
			log.Fatalf("-raft-http-peers: %v", err)
		}
		mgr, err = cluster.Open(context.Background(), cluster.Config{
			Root:          *dataDir,
			Shards:        *shardsN,
			SelfID:        *raftID,
			MuxAddr:       *muxAddr,
			Peers:         peers,
			PeerHTTPAddrs: httpPeers,
			Bootstrap:     *raftBootstrap,
			FsyncOnCommit: *fsync,
		})
		if err != nil {
			log.Fatalf("open cluster manager: %v", err)
		}
		defer mgr.Close()
		// Shard 0 is the metadata shard; the catalog lives there
		// and gets fanned out to other shards by the API layer.
		shard0, _ := mgr.Shard(0)
		db = shard0.Storage
		cat, err = catalog.New(db)
		if err != nil {
			log.Fatalf("load catalog (shard 0): %v", err)
		}
		log.Printf("multi-Raft enabled: shards=%d mux=%s peers=%v", *shardsN, *muxAddr, peers)
	} else {
		var err error
		db, err = storage.Open(storage.Options{Path: *dataDir, FsyncOnCommit: *fsync})
		if err != nil {
			log.Fatalf("open pebble: %v", err)
		}
		defer db.Close()
		cat, err = catalog.New(db)
		if err != nil {
			log.Fatalf("load catalog: %v", err)
		}
	}

	if mgr == nil && *raftBind != "" {
		if *raftID == "" {
			log.Fatal("-raft-id is required when -raft-bind is set")
		}
		path := *raftPath
		if path == "" {
			path = *dataDir + "/raft"
		}
		peers, err := parsePeers(*raftPeersFlag)
		if err != nil {
			log.Fatalf("-raft-peers: %v", err)
		}
		httpPeers, err := parsePeers(*raftHTTPFlag)
		if err != nil {
			log.Fatalf("-raft-http-peers: %v", err)
		}
		raftDB, err = craft.Open(context.Background(), craft.Config{
			Path:          path,
			SelfID:        *raftID,
			BindAddr:      *raftBind,
			Bootstrap:     *raftBootstrap,
			PeerAddrs:     peers,
			PeerHTTPAddrs: httpPeers,
		}, db.Raw())
		if err != nil {
			log.Fatalf("open raft: %v", err)
		}
		defer raftDB.Close()
		db.AttachReplicator(raftDB)
		log.Printf("raft attached: id=%s bind=%s bootstrap=%v peers=%v", *raftID, *raftBind, *raftBootstrap, peers)
	}

	var validator *auth.Validator
	if *identityJwks != "" {
		var err error
		validator, err = auth.NewValidator(auth.Config{
			JwksURL:   *identityJwks,
			Issuer:    *identityIssuer,
			Audience:  *identityAudience,
			ClockSkew: *identityClockSkew,
		})
		if err != nil {
			log.Fatalf("auth validator: %v", err)
		}
		log.Printf("identity auth enabled: jwks=%s issuer=%q audience=%q", *identityJwks, *identityIssuer, *identityAudience)
	}

	mux := http.NewServeMux()
	apiSrv := api.New(db, cat)
	if raftDB != nil {
		apiSrv.AttachCluster(raftDB)
	} else if mgr != nil {
		// In multi-shard mode the cluster-status surface uses shard
		// 0's raft handle as a representative; per-shard status is
		// available in the manager directly.
		if sh, ok := mgr.Shard(0); ok && sh.Raft != nil {
			apiSrv.AttachCluster(sh.Raft)
		}
	}
	if mgr != nil {
		apiSrv.AttachManager(mgr)
	}
	if validator != nil {
		apiSrv.AttachAuth(validator)
	}
	if prom != nil {
		apiSrv.AttachMetrics(prom)
		if mgr != nil {
			go metrics.RunShardCollector(context.Background(), prom, mgr, 5*time.Second)
		}
	}
	apiSrv.Routes(mux)

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		mode := "single-node"
		if raftDB != nil {
			mode = "raft"
		}
		log.Printf("cefas-server listening on %s (data=%s, mode=%s)", *httpAddr, *dataDir, mode)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	// gRPC listener (optional).
	var gsrv *grpc.Server
	if *grpcAddr != "" {
		opts, err := buildGRPCOpts(validator, *tlsCert, *tlsKey, *mtlsCA)
		if err != nil {
			log.Fatalf("grpc opts: %v", err)
		}
		gsrv = grpc.NewServer(opts...)
		var clu api.Cluster
		if raftDB != nil {
			clu = raftDB
		} else if mgr != nil {
			if sh, ok := mgr.Shard(0); ok && sh.Raft != nil {
				clu = sh.Raft
			}
		}
		gsrvImpl := api.NewGRPCServer(db, cat, clu)
		if mgr != nil {
			gsrvImpl.AttachManager(mgr)
		}
		cefaspb.RegisterCefasServer(gsrv, gsrvImpl)
		if *grpcReflection {
			reflection.Register(gsrv)
		}
		ln, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		go func() {
			log.Printf("gRPC listening on %s (tls=%v mtls=%v reflection=%v)", *grpcAddr, *tlsCert != "", *mtlsCA != "", *grpcReflection)
			if err := gsrv.Serve(ln); err != nil {
				log.Printf("grpc serve: %v", err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if gsrv != nil {
		gsrv.GracefulStop()
	}
}

// buildGRPCOpts assembles ServerOptions for the gRPC server: auth
// interceptors (if a validator is configured) + TLS / mTLS credentials
// when cert paths are supplied.
func buildGRPCOpts(v *auth.Validator, certPath, keyPath, caBundle string) ([]grpc.ServerOption, error) {
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

	if v != nil {
		// Reflection probe stays available without a token so
		// `grpcurl -plaintext localhost:9090 list` works in dev.
		skip := map[string]bool{
			"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      true,
			"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": true,
			"/cefas.v1.Cefas/ClusterStatus":                                  true,
		}
		unary, stream := api.AuthInterceptor(v, skip)
		if unary != nil {
			opts = append(opts, grpc.UnaryInterceptor(unary))
		}
		if stream != nil {
			opts = append(opts, grpc.StreamInterceptor(stream))
		}
	}
	return opts, nil
}

// parsePeers parses the "id1=addr1,id2=addr2" form used by both
// -raft-peers and -raft-http-peers.
func parsePeers(s string) (map[string]string, error) { return config.ParsePeers(s) }

// overlayFlags pushes flag values into the cfg struct. Only non-zero
// flag values overwrite — the YAML/env layer wins when the operator
// did not touch the flag. This is the cheap way to keep precedence
// "flag > env > yaml > default" without per-flag tracking of "user
// supplied this" bits.
func overlayFlags(
	cfg *config.Config,
	dataDir, httpAddr string, fsync bool,
	raftBind, raftID, raftPath string, raftBootstrap bool, raftPeers, raftHTTPPeers string,
	identityJwks, identityIssuer, identityAudience string, identityClockSkew time.Duration,
	shardsN int, muxAddr string,
	grpcAddr string, grpcRefl bool, tlsCert, tlsKey, mtlsCA string,
	metricsOff bool, tracingURL string, tracingInsecure bool,
) {
	if dataDir != "" && dataDir != "./cefas-data" {
		cfg.Data = dataDir
	} else if cfg.Data == "" {
		cfg.Data = dataDir
	}
	if httpAddr != "" && httpAddr != ":8080" {
		cfg.HTTP.Addr = httpAddr
	} else if cfg.HTTP.Addr == "" {
		cfg.HTTP.Addr = httpAddr
	}
	if fsync {
		cfg.Storage.FsyncOnCommit = true
	}
	if raftBind != "" {
		cfg.Raft.Bind = raftBind
	}
	if raftPath != "" {
		cfg.Raft.Path = raftPath
	}
	if raftID != "" {
		cfg.Cluster.SelfID = raftID
	}
	if raftBootstrap {
		cfg.Cluster.Bootstrap = true
	}
	if raftPeers != "" {
		peers, _ := parsePeers(raftPeers)
		cfg.Cluster.Peers = peers
	}
	if raftHTTPPeers != "" {
		hp, _ := parsePeers(raftHTTPPeers)
		cfg.Cluster.HTTPPeers = hp
	}
	if identityJwks != "" {
		cfg.Identity.JwksURL = identityJwks
	}
	if identityIssuer != "" {
		cfg.Identity.Issuer = identityIssuer
	}
	if identityAudience != "" {
		cfg.Identity.Audience = identityAudience
	}
	if identityClockSkew != 30*time.Second {
		cfg.Identity.ClockSkew = identityClockSkew
	}
	if shardsN > 0 {
		cfg.Cluster.Shards = shardsN
	}
	if muxAddr != "" {
		cfg.Cluster.MuxAddr = muxAddr
	}
	if grpcAddr != "" {
		cfg.GRPC.Addr = grpcAddr
	}
	if grpcRefl {
		cfg.GRPC.Reflection = true
	}
	if tlsCert != "" {
		cfg.GRPC.TLSCertPath = tlsCert
	}
	if tlsKey != "" {
		cfg.GRPC.TLSKeyPath = tlsKey
	}
	if mtlsCA != "" {
		cfg.GRPC.MTLSCAPath = mtlsCA
	}
	if metricsOff {
		cfg.Metrics.Enabled = false
	}
	if tracingURL != "" {
		cfg.Tracing.Endpoint = tracingURL
	}
	if !tracingInsecure {
		cfg.Tracing.Insecure = false
	}
	// Compatibility: keep parsing the old strings to avoid "imported
	// and not used" diagnostics for strings.
	_ = strings.TrimSpace("")
}
