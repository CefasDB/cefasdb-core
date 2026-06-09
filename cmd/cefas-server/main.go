// cefas-server is the Phase 1 single-node binary. Opens Pebble at the
// configured directory, loads the catalog, and serves HTTP/JSON on the
// configured port. Raft, multi-shard, and gRPC ship in later phases.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/api"
)

func main() {
	var (
		dataDir  = flag.String("data", "./cefas-data", "Pebble data directory")
		httpAddr = flag.String("http", ":8080", "HTTP listen address")
		fsync    = flag.Bool("fsync", false, "fsync on commit (durability over throughput)")
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

	mux := http.NewServeMux()
	api.New(db, cat).Routes(mux)

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("cefas-server listening on %s (data=%s)", *httpAddr, *dataDir)
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
