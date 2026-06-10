// cefas-server is the cefas database binary. In single-node mode it
// opens Pebble, loads the catalog, and serves HTTP/JSON. With the
// -raft-bootstrap or -raft-join flags it additionally wires raft
// replication so writes flow through the consensus log.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	craft "github.com/osvaldoandrade/cefas/internal/raft"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/api"
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
	)
	flag.Parse()

	db, err := storage.Open(storage.Options{Path: *dataDir, FsyncOnCommit: *fsync})
	if err != nil {
		log.Fatalf("open pebble: %v", err)
	}
	defer db.Close()

	cat, err := catalog.New(db)
	if err != nil {
		log.Fatalf("load catalog: %v", err)
	}

	var raftDB *craft.DB
	if *raftBind != "" {
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
	}
	if validator != nil {
		apiSrv.AttachAuth(validator)
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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// parsePeers parses the "id1=addr1,id2=addr2" form used by both
// -raft-peers and -raft-http-peers.
func parsePeers(s string) (map[string]string, error) {
	out := make(map[string]string)
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		i := strings.IndexByte(entry, '=')
		if i <= 0 || i == len(entry)-1 {
			return nil, fmt.Errorf("bad peer %q: expected id=addr", entry)
		}
		out[strings.TrimSpace(entry[:i])] = strings.TrimSpace(entry[i+1:])
	}
	return out, nil
}
