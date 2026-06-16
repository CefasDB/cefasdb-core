// cefas-server is the cefas database binary. In single-node mode it
// opens Pebble, loads the catalog, and serves HTTP/JSON. With the
// -raft-bootstrap or -raft-join flags it additionally wires raft
// replication so writes flow through the consensus log.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/osvaldoandrade/cefas/internal/api"
	"github.com/osvaldoandrade/cefas/internal/auth"
	bootstrapserver "github.com/osvaldoandrade/cefas/internal/bootstrap/server"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	craft "github.com/osvaldoandrade/cefas/internal/raft"
	"github.com/osvaldoandrade/cefas/internal/rebalancer"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/internal/tracing"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/config"
	// Side-effect import: every built-in plugin registers against
	// plugin.Default before the server exposes ListPlugins.
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/builtins"
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
		raftStorePath = flag.String("raft-store-path", "", "Pebble path for Raft log/stable metadata. Defaults to -data/raft-store in single-Raft mode.")
		raftBootstrap = flag.Bool("raft-bootstrap", false, "Bootstrap a new cluster from -raft-peers (run on the first node only)")
		raftPeersFlag = flag.String("raft-peers", "", "Comma-separated id=raftAddr peer list, e.g. 'a=127.0.0.1:9001,b=127.0.0.1:9002,c=127.0.0.1:9003'")
		raftHTTPFlag  = flag.String("raft-http-peers", "", "Comma-separated id=httpURL peer list for 307 redirects, e.g. 'a=http://h1:8080,b=http://h2:8080'")

		// Storage tuning.
		storageProfile            = flag.String("storage-profile", "", "Pebble profile: default, balanced, write-heavy")
		raftStorageProfile        = flag.String("raft-storage-profile", "", "Pebble profile for separated Raft metadata stores. Defaults to raft.")
		storageBlockCache         = flag.Int64("storage-block-cache-size", 0, "Pebble block cache size in bytes. 0 inherits selected profile.")
		storageMemTableSize       = flag.Uint64("storage-memtable-size", 0, "Pebble MemTableSize in bytes. 0 inherits selected profile.")
		storageMemTableStopWrites = flag.Int("storage-memtable-stop-writes", 0, "Pebble MemTableStopWritesThreshold. 0 inherits selected profile.")
		storageMaxCompactions     = flag.Int("storage-max-compactions", 0, "Pebble MaxConcurrentCompactions. 0 inherits selected profile.")
		storageL0Concurrency      = flag.Int("storage-l0-compaction-concurrency", 0, "Pebble Experimental.L0CompactionConcurrency. 0 inherits selected profile.")
		storageL0Threshold        = flag.Int("storage-l0-compaction-threshold", 0, "Pebble L0CompactionThreshold. 0 inherits selected profile.")
		storageL0FileThreshold    = flag.Int("storage-l0-compaction-file-threshold", 0, "Pebble L0CompactionFileThreshold. 0 inherits selected profile.")
		storageL0Stop             = flag.Int("storage-l0-stop-writes-threshold", 0, "Pebble L0StopWritesThreshold. 0 inherits selected profile.")
		storageBytesPerSync       = flag.Int("storage-bytes-per-sync", 0, "Pebble BytesPerSync. 0 inherits selected profile.")
		storageWALBytesPerSync    = flag.Int("storage-wal-bytes-per-sync", 0, "Pebble WALBytesPerSync. 0 inherits selected profile.")

		// Adaptive write backpressure.
		backpressureEnabled      = flag.Bool("storage-backpressure", false, "Enable write backpressure from Pebble pressure metrics.")
		backpressureReject       = flag.Bool("storage-backpressure-reject-critical", false, "Reject writes instead of only sleeping when pressure is critical.")
		backpressureWarnL0       = flag.Int64("storage-backpressure-warning-l0-files", 0, "Warning L0 file threshold. 0 uses default.")
		backpressureCriticalL0   = flag.Int64("storage-backpressure-critical-l0-files", 0, "Critical L0 file threshold. 0 uses default.")
		backpressureWarnDebt     = flag.Uint64("storage-backpressure-warning-debt", 0, "Warning compaction debt threshold in bytes. 0 uses default.")
		backpressureCriticalDebt = flag.Uint64("storage-backpressure-critical-debt", 0, "Critical compaction debt threshold in bytes. 0 uses default.")
		backpressureWarnReadAmp  = flag.Int("storage-backpressure-warning-read-amp", 0, "Warning Pebble read amplification threshold. 0 uses default.")
		backpressureCritReadAmp  = flag.Int("storage-backpressure-critical-read-amp", 0, "Critical Pebble read amplification threshold. 0 uses default.")
		backpressureWarnDelay    = flag.Duration("storage-backpressure-warning-delay", 0, "Delay applied to writes in warning state. 0 uses default.")
		backpressureCritDelay    = flag.Duration("storage-backpressure-critical-delay", 0, "Delay applied to writes in critical state. 0 uses default.")
		streamRetention          = flag.Duration("storage-stream-retention", 0, "DynamoDB Streams retention window. 0 inherits config/default 24h.")
		streamRetentionMaxBytes  = flag.Int64("storage-stream-retention-max-bytes", 0, "Maximum logical DynamoDB Streams retained bytes per table. 0 disables byte cap.")

		// Identity/auth flags. Empty -identity-jwks-url keeps the
		// server open (single-node dev mode).
		identityJwks      = flag.String("identity-jwks-url", "", "Tikti JWKS endpoint (enables bearer-token auth)")
		identityIssuer    = flag.String("identity-issuer", "", "Expected token issuer")
		identityAudience  = flag.String("identity-audience", "", "Expected token audience")
		identityClockSkew = flag.Duration("identity-clock-skew", 30*time.Second, "Allowed clock skew on exp/iat checks")

		// Multi-Raft sharding.
		shardsN = flag.Int("shards", 0, "Number of shards (multi-Raft). 0 → single-shard / single-node legacy bootstrap.")
		muxAddr = flag.String("mux", "", "Mux TCP address shared by every shard's raft transport (multi-Raft mode).")

		// gRPC flags.
		grpcAddr       = flag.String("grpc", "", "gRPC listen address (e.g. ':9090'). Empty disables gRPC.")
		grpcReflection = flag.Bool("grpc-reflection", false, "Enable gRPC server reflection (handy for grpcurl)")
		tlsCert        = flag.String("tls-cert", "", "Path to TLS certificate (PEM). Enables TLS on the gRPC listener.")
		tlsKey         = flag.String("tls-key", "", "Path to TLS private key (PEM)")
		mtlsCA         = flag.String("mtls-ca", "", "Path to a client-CA bundle. When set, the gRPC listener requires mTLS.")

		// Observability + config.
		configPath = flag.String("config", "", "Path to YAML config file. Flag/env values override the file.")
		metricsOff = flag.Bool("metrics-disabled", false, "Disable the /metrics Prometheus endpoint.")
		tracingURL = flag.String("tracing-endpoint", "", "OTLP/gRPC collector endpoint (e.g. 'jaeger:4317'). Empty disables tracing.")
		tracingIns = flag.Bool("tracing-insecure", true, "Disable TLS to the OTLP collector.")

		// Autonomous rebalancer. Disabled by default; dry-run/manual
		// modes are intended for rollout before automatic apply.
		rebalancerEnabled       = flag.Bool("rebalancer-enabled", false, "Enable the hotspot-driven placement rebalancer.")
		rebalancerMode          = flag.String("rebalancer-mode", "", "Rebalancer mode: dry-run, manual, or auto.")
		rebalancerInterval      = flag.Duration("rebalancer-interval", 0, "Interval between rebalancer evaluations. 0 inherits config/default.")
		rebalancerMinInterval   = flag.Duration("rebalancer-min-interval", 0, "Minimum time between rebalance operations. 0 inherits config/default.")
		rebalancerMaxConcurrent = flag.Int("rebalancer-max-concurrent", 0, "Maximum concurrent automatic rebalance operations. 0 inherits config/default.")
		rebalancerMaxHotspots   = flag.Int("rebalancer-max-hotspots", 0, "Maximum hot ranges to inspect per rebalancer tick. 0 inherits config/default.")
		rebalancerMinVoters     = flag.Int("rebalancer-min-voters", 0, "Minimum voters for generated placement plans. 0 keeps the current voter count.")
		rebalancerApplyTimeout  = flag.Duration("rebalancer-apply-timeout", 0, "Per-plan apply timeout for auto mode. 0 inherits config/default.")
		rebalancerManualDir     = flag.String("rebalancer-manual-plan-dir", "", "Directory where manual mode writes rebalance plans.")

		// Scheduled backups. Disabled by default; operators must
		// enable explicitly through flags, env, or YAML.
		backupSchedulerEnabled             = flag.Bool("backup-scheduler-enabled", false, "Enable scheduled admin-named backups.")
		backupSchedulerDisabled            = flag.Bool("backup-scheduler-disabled", false, "Disable scheduled backups even when config/env enabled them.")
		backupSchedulerDryRun              = flag.Bool("backup-scheduler-dry-run", false, "Validate scheduled backup inputs without creating backups.")
		backupSchedulerInterval            = flag.Duration("backup-scheduler-interval", 0, "Interval between scheduled backups. 0 inherits config/default.")
		backupSchedulerNameTemplate        = flag.String("backup-scheduler-name-template", "", "Backup name template. Supports {{timestamp}}, {{unix}}, {{unix_nano}}, {{date}}, {{time}}.")
		backupSchedulerTables              = flag.String("backup-scheduler-tables", "", "Comma-separated tables to back up. Empty captures every table.")
		backupSchedulerRetentionKeepLatest = flag.Int("backup-scheduler-retention-keep-latest", -1, "Retain this many newest backups after each scheduled run. Negative inherits config/disabled.")
		backupSchedulerRetentionMaxAge     = flag.Duration("backup-scheduler-retention-max-age", 0, "Delete backups older than this age after each scheduled run. 0 inherits config/disabled.")
		backupSchedulerRetentionDryRun     = flag.Bool("backup-scheduler-retention-dry-run", false, "Evaluate scheduled retention without deleting backups.")
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
		*raftBind, *raftID, *raftPath, *raftStorePath, *raftBootstrap, *raftPeersFlag, *raftHTTPFlag,
		*storageProfile, *raftStorageProfile,
		*storageBlockCache, *storageMemTableSize, *storageMemTableStopWrites,
		*storageMaxCompactions, *storageL0Concurrency, *storageL0Threshold,
		*storageL0FileThreshold, *storageL0Stop, *storageBytesPerSync, *storageWALBytesPerSync,
		*backpressureEnabled, *backpressureReject, *backpressureWarnL0, *backpressureCriticalL0,
		*backpressureWarnDebt, *backpressureCriticalDebt, *backpressureWarnReadAmp,
		*backpressureCritReadAmp, *backpressureWarnDelay, *backpressureCritDelay,
		*streamRetention, *streamRetentionMaxBytes,
		*identityJwks, *identityIssuer, *identityAudience, *identityClockSkew,
		*shardsN, *muxAddr,
		*grpcAddr, *grpcReflection, *tlsCert, *tlsKey, *mtlsCA,
		*metricsOff, *tracingURL, *tracingIns,
		*rebalancerEnabled, *rebalancerMode, *rebalancerInterval, *rebalancerMinInterval,
		*rebalancerMaxConcurrent, *rebalancerMaxHotspots, *rebalancerMinVoters,
		*rebalancerApplyTimeout, *rebalancerManualDir,
		*backupSchedulerEnabled, *backupSchedulerDisabled, *backupSchedulerDryRun,
		*backupSchedulerInterval, *backupSchedulerNameTemplate, *backupSchedulerTables,
		*backupSchedulerRetentionKeepLatest, *backupSchedulerRetentionMaxAge, *backupSchedulerRetentionDryRun)

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
		prom = metrics.NewWithRangeHotspots(bootstrapserver.RangeHotspotConfig(cfg))
	}

	var (
		db     *storage.DB
		cat    *catalog.Catalog
		mgr    *cluster.Manager
		raftDB *craft.DB
	)

	if cfg.Cluster.Shards > 0 {
		mgr, err = cluster.Open(context.Background(), cluster.Config{
			Root:            cfg.Data,
			Shards:          cfg.Cluster.Shards,
			SelfID:          cfg.Cluster.SelfID,
			MuxAddr:         cfg.Cluster.MuxAddr,
			Peers:           cfg.Cluster.Peers,
			PeerHTTPAddrs:   cfg.Cluster.HTTPPeers,
			Bootstrap:       cfg.Cluster.Bootstrap,
			FsyncOnCommit:   cfg.Storage.FsyncOnCommit,
			StorageProfile:  cfg.Storage.Profile,
			StorageTuning:   bootstrapserver.StorageTuning(cfg),
			Backpressure:    bootstrapserver.BackpressureOptions(cfg),
			StreamRetention: bootstrapserver.StreamRetentionOptions(cfg),
			RaftProfile:     cfg.Storage.RaftProfile,
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
		log.Printf("multi-Raft enabled: shards=%d mux=%s peers=%v", cfg.Cluster.Shards, cfg.Cluster.MuxAddr, cfg.Cluster.Peers)
	} else {
		var err error
		db, err = storage.Open(bootstrapserver.StorageOptions(cfg, cfg.Data))
		if err != nil {
			log.Fatalf("open pebble: %v", err)
		}
		defer db.Close()
		cat, err = catalog.New(db)
		if err != nil {
			log.Fatalf("load catalog: %v", err)
		}
	}

	var raftStore *storage.DB
	if mgr == nil && cfg.Raft.Bind != "" {
		if cfg.Cluster.SelfID == "" {
			log.Fatal("-raft-id is required when -raft-bind is set")
		}
		path := cfg.Raft.Path
		if path == "" {
			path = cfg.Data + "/raft"
		}
		storePath := cfg.Raft.StorePath
		if storePath == "" {
			storePath = cfg.Data + "/raft-store"
		}
		raftProfile := cfg.Storage.RaftProfile
		if raftProfile == "" {
			raftProfile = storage.ProfileRaft
		}
		raftStore, err = storage.Open(storage.Options{
			Path:          storePath,
			FsyncOnCommit: cfg.Storage.FsyncOnCommit,
			Profile:       raftProfile,
		})
		if err != nil {
			log.Fatalf("open raft store: %v", err)
		}
		defer raftStore.Close()
		raftDB, err = craft.Open(context.Background(), craft.Config{
			Path:          path,
			SelfID:        cfg.Cluster.SelfID,
			BindAddr:      cfg.Raft.Bind,
			Bootstrap:     cfg.Cluster.Bootstrap,
			PeerAddrs:     cfg.Cluster.Peers,
			PeerHTTPAddrs: cfg.Cluster.HTTPPeers,
		}, db.Raw(), raftStore.Raw())
		if err != nil {
			log.Fatalf("open raft: %v", err)
		}
		defer raftDB.Close()
		db.AttachReplicator(raftDB)
		log.Printf("raft attached: id=%s bind=%s bootstrap=%v peers=%v raftStore=%s", cfg.Cluster.SelfID, cfg.Raft.Bind, cfg.Cluster.Bootstrap, cfg.Cluster.Peers, storePath)
	}

	var validator *auth.Validator
	if cfg.Identity.JwksURL != "" {
		var err error
		validator, err = auth.NewValidator(auth.Config{
			JwksURL:   cfg.Identity.JwksURL,
			Issuer:    cfg.Identity.Issuer,
			Audience:  cfg.Identity.Audience,
			ClockSkew: cfg.Identity.ClockSkew,
		})
		if err != nil {
			log.Fatalf("auth validator: %v", err)
		}
		log.Printf("identity auth enabled: jwks=%s issuer=%q audience=%q", cfg.Identity.JwksURL, cfg.Identity.Issuer, cfg.Identity.Audience)
	}

	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	defer runtimeCancel()
	backupScheduler := storage.NewScheduledBackupRunner(db, bootstrapserver.ScheduledBackupConfig(cfg, prom, log.Printf))

	mux := http.NewServeMux()
	apiSrv := api.New(db, cat)
	if raftDB != nil {
		apiSrv.AttachCluster(raftDB)
		// Wire the CDC publisher + stream adapter so /v1/Stream
		// and /v1/admin/snapshots have a source.
		pub := craft.NewPublisher(2048)
		raftDB.AttachPublisher(pub)
		apiSrv.AttachChangeStream(bootstrapserver.NewStreamAdapter(raftDB))
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
	apiSrv.AttachBackupScheduler(backupScheduler)
	if prom != nil {
		apiSrv.AttachMetrics(prom)
		if mgr != nil {
			go metrics.RunShardCollector(runtimeCtx, prom, mgr, 5*time.Second)
		} else if db != nil {
			go metrics.RunStorageCollector(runtimeCtx, prom, "0", db, raftDB, 5*time.Second)
			if raftStore != nil {
				go metrics.RunStorageCollector(runtimeCtx, prom, "raft", raftStore, nil, 5*time.Second)
			}
		}
	}
	if cfg.BackupScheduler.Enabled {
		go backupScheduler.Run(runtimeCtx)
		log.Printf("scheduled backups enabled: interval=%s dryRun=%v template=%q tables=%v", cfg.BackupScheduler.Interval, cfg.BackupScheduler.DryRun, cfg.BackupScheduler.NameTemplate, cfg.BackupScheduler.Tables)
	}
	if cfg.Rebalancer.Enabled {
		if mgr == nil {
			log.Printf("rebalancer disabled: multi-Raft cluster manager is not configured")
		} else if prom == nil {
			log.Printf("rebalancer disabled: metrics must be enabled for hotspot input")
		} else {
			ctrl := rebalancer.NewController(bootstrapserver.RebalancerConfig(cfg), mgr, prom, nil)
			ctrl.SetLogger(log.Printf)
			go ctrl.Run(runtimeCtx)
			log.Printf("rebalancer enabled: mode=%s interval=%s maxConcurrent=%d", cfg.Rebalancer.Mode, cfg.Rebalancer.Interval, cfg.Rebalancer.MaxConcurrentOperations)
		}
	}
	apiSrv.Routes(mux)

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		mode := "single-node"
		if raftDB != nil {
			mode = "raft"
		}
		log.Printf("cefas-server listening on %s (data=%s, mode=%s)", cfg.HTTP.Addr, cfg.Data, mode)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	// gRPC listener (optional).
	var gsrv *grpc.Server
	if cfg.GRPC.Addr != "" {
		opts, err := bootstrapserver.BuildGRPCOpts(validator, cfg.GRPC.TLSCertPath, cfg.GRPC.TLSKeyPath, cfg.GRPC.MTLSCAPath)
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
		if prom != nil {
			gsrvImpl.AttachMetrics(prom)
		}
		gsrvImpl.AttachBackupScheduler(backupScheduler)
		if raftDB != nil {
			gsrvImpl.AttachChangeStream(bootstrapserver.NewStreamAdapter(raftDB))
		}
		cefaspb.RegisterCefasServer(gsrv, gsrvImpl)
		api.RegisterAtomic(gsrv, gsrvImpl)
		if cfg.GRPC.Reflection {
			reflection.Register(gsrv)
		}
		ln, err := net.Listen("tcp", cfg.GRPC.Addr)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		go func() {
			log.Printf("gRPC listening on %s (tls=%v mtls=%v reflection=%v)", cfg.GRPC.Addr, cfg.GRPC.TLSCertPath != "", cfg.GRPC.MTLSCAPath != "", cfg.GRPC.Reflection)
			if err := gsrv.Serve(ln); err != nil {
				log.Printf("grpc serve: %v", err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
	runtimeCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if gsrv != nil {
		gsrv.GracefulStop()
	}
}

// parsePeers parses the "id1=addr1,id2=addr2" form used by both
// -raft-peers and -raft-http-peers.
func parsePeers(s string) (map[string]string, error) { return config.ParsePeers(s) }

func storageOptionsFromConfig(cfg config.Config, path string) storage.Options {
	return storage.Options{
		Path:            path,
		FsyncOnCommit:   cfg.Storage.FsyncOnCommit,
		Profile:         cfg.Storage.Profile,
		Tuning:          storageTuningFromConfig(cfg),
		Backpressure:    backpressureFromConfig(cfg),
		StreamRetention: streamRetentionFromConfig(cfg),
	}
}

func storageTuningFromConfig(cfg config.Config) storage.PebbleTuning {
	return storage.PebbleTuning{
		BlockCacheSizeBytes:       cfg.Storage.BlockCacheSizeBytes,
		MemTableSizeBytes:         cfg.Storage.MemTableSizeBytes,
		MemTableStopWrites:        cfg.Storage.MemTableStopWritesThreshold,
		MaxConcurrentCompactions:  cfg.Storage.MaxConcurrentCompactions,
		L0CompactionConcurrency:   cfg.Storage.L0CompactionConcurrency,
		L0CompactionThreshold:     cfg.Storage.L0CompactionThreshold,
		L0CompactionFileThreshold: cfg.Storage.L0CompactionFileThreshold,
		L0StopWritesThreshold:     cfg.Storage.L0StopWritesThreshold,
		BytesPerSync:              cfg.Storage.BytesPerSync,
		WALBytesPerSync:           cfg.Storage.WALBytesPerSync,
	}
}

func rangeHotspotConfigFromConfig(cfg config.Config) metrics.RangeHotspotConfig {
	return metrics.RangeHotspotConfig{
		Buckets:                      cfg.Metrics.HotspotBuckets,
		Window:                       cfg.Metrics.HotspotWindow,
		CoolingWindow:                cfg.Metrics.HotspotCoolingWindow,
		MaxSummaries:                 cfg.Metrics.HotspotMaxSummaries,
		ReadThreshold:                cfg.Metrics.HotspotReadThreshold,
		WriteThreshold:               cfg.Metrics.HotspotWriteThreshold,
		BytesThreshold:               cfg.Metrics.HotspotBytesThreshold,
		LatencyThresholdSeconds:      cfg.Metrics.HotspotLatencyThreshold.Seconds(),
		CompactionDebtThresholdBytes: cfg.Metrics.HotspotCompactionDebtThreshold,
		ThrottleStateThreshold:       cfg.Metrics.HotspotThrottleStateThreshold,
	}
}

func rebalancerConfigFromConfig(cfg config.Config) rebalancer.Config {
	return rebalancer.Config{
		Mode:                    rebalancer.Mode(cfg.Rebalancer.Mode),
		Interval:                cfg.Rebalancer.Interval,
		MinInterval:             cfg.Rebalancer.MinInterval,
		MaxConcurrentOperations: cfg.Rebalancer.MaxConcurrentOperations,
		MaxHotspots:             cfg.Rebalancer.MaxHotspots,
		MinVoters:               cfg.Rebalancer.MinVoters,
		ApplyTimeoutMS:          int(cfg.Rebalancer.ApplyTimeout / time.Millisecond),
		ManualPlanDir:           cfg.Rebalancer.ManualPlanDir,
	}
}

func scheduledBackupConfigFromConfig(cfg config.Config, prom *metrics.Metrics, logger func(string, ...any)) storage.ScheduledBackupConfig {
	return storage.ScheduledBackupConfig{
		Enabled:      cfg.BackupScheduler.Enabled,
		DryRun:       cfg.BackupScheduler.DryRun,
		Interval:     cfg.BackupScheduler.Interval,
		NameTemplate: cfg.BackupScheduler.NameTemplate,
		Tables:       append([]string(nil), cfg.BackupScheduler.Tables...),
		Retention: storage.BackupRetentionOptions{
			KeepLatest:    cfg.BackupScheduler.Retention.KeepLatest,
			KeepLatestSet: cfg.BackupScheduler.Retention.KeepLatestSet,
			MaxAge:        cfg.BackupScheduler.Retention.MaxAge,
			MaxAgeSet:     cfg.BackupScheduler.Retention.MaxAgeSet,
			DryRun:        cfg.BackupScheduler.Retention.DryRun,
		},
		Logger:  logger,
		Metrics: prom,
	}
}

func backpressureFromConfig(cfg config.Config) storage.BackpressureOptions {
	return storage.BackpressureOptions{
		Enabled:                     cfg.Storage.BackpressureEnabled,
		RejectOnCritical:            cfg.Storage.BackpressureRejectCritical,
		WarningL0Files:              cfg.Storage.BackpressureWarningL0Files,
		CriticalL0Files:             cfg.Storage.BackpressureCriticalL0Files,
		WarningCompactionDebtBytes:  cfg.Storage.BackpressureWarningDebt,
		CriticalCompactionDebtBytes: cfg.Storage.BackpressureCriticalDebt,
		WarningReadAmp:              cfg.Storage.BackpressureWarningReadAmp,
		CriticalReadAmp:             cfg.Storage.BackpressureCriticalReadAmp,
		WarningDelay:                cfg.Storage.BackpressureWarningDelay,
		CriticalDelay:               cfg.Storage.BackpressureCriticalDelay,
	}
}

func streamRetentionFromConfig(cfg config.Config) storage.StreamRetentionOptions {
	return storage.StreamRetentionOptions{
		Retention: cfg.Storage.StreamRetention,
		MaxBytes:  cfg.Storage.StreamRetentionMaxBytes,
	}
}

// parsePeers parses the "id1=addr1,id2=addr2" form used by both
// -raft-peers and -raft-http-peers.
func parsePeers(s string) (map[string]string, error) { return config.ParsePeers(s) }

func storageOptionsFromConfig(cfg config.Config, path string) storage.Options {
	return storage.Options{
		Path:            path,
		FsyncOnCommit:   cfg.Storage.FsyncOnCommit,
		Profile:         cfg.Storage.Profile,
		Tuning:          storageTuningFromConfig(cfg),
		Backpressure:    backpressureFromConfig(cfg),
		StreamRetention: streamRetentionFromConfig(cfg),
	}
}

func storageTuningFromConfig(cfg config.Config) storage.PebbleTuning {
	return storage.PebbleTuning{
		BlockCacheSizeBytes:       cfg.Storage.BlockCacheSizeBytes,
		MemTableSizeBytes:         cfg.Storage.MemTableSizeBytes,
		MemTableStopWrites:        cfg.Storage.MemTableStopWritesThreshold,
		MaxConcurrentCompactions:  cfg.Storage.MaxConcurrentCompactions,
		L0CompactionConcurrency:   cfg.Storage.L0CompactionConcurrency,
		L0CompactionThreshold:     cfg.Storage.L0CompactionThreshold,
		L0CompactionFileThreshold: cfg.Storage.L0CompactionFileThreshold,
		L0StopWritesThreshold:     cfg.Storage.L0StopWritesThreshold,
		BytesPerSync:              cfg.Storage.BytesPerSync,
		WALBytesPerSync:           cfg.Storage.WALBytesPerSync,
	}
}

func rangeHotspotConfigFromConfig(cfg config.Config) metrics.RangeHotspotConfig {
	return metrics.RangeHotspotConfig{
		Buckets:                      cfg.Metrics.HotspotBuckets,
		Window:                       cfg.Metrics.HotspotWindow,
		CoolingWindow:                cfg.Metrics.HotspotCoolingWindow,
		MaxSummaries:                 cfg.Metrics.HotspotMaxSummaries,
		ReadThreshold:                cfg.Metrics.HotspotReadThreshold,
		WriteThreshold:               cfg.Metrics.HotspotWriteThreshold,
		BytesThreshold:               cfg.Metrics.HotspotBytesThreshold,
		LatencyThresholdSeconds:      cfg.Metrics.HotspotLatencyThreshold.Seconds(),
		CompactionDebtThresholdBytes: cfg.Metrics.HotspotCompactionDebtThreshold,
		ThrottleStateThreshold:       cfg.Metrics.HotspotThrottleStateThreshold,
	}
}

func rebalancerConfigFromConfig(cfg config.Config) rebalancer.Config {
	return rebalancer.Config{
		Mode:                    rebalancer.Mode(cfg.Rebalancer.Mode),
		Interval:                cfg.Rebalancer.Interval,
		MinInterval:             cfg.Rebalancer.MinInterval,
		MaxConcurrentOperations: cfg.Rebalancer.MaxConcurrentOperations,
		MaxHotspots:             cfg.Rebalancer.MaxHotspots,
		MinVoters:               cfg.Rebalancer.MinVoters,
		ApplyTimeoutMS:          int(cfg.Rebalancer.ApplyTimeout / time.Millisecond),
		ManualPlanDir:           cfg.Rebalancer.ManualPlanDir,
	}
}

func scheduledBackupConfigFromConfig(cfg config.Config, prom *metrics.Metrics, logger func(string, ...any)) storage.ScheduledBackupConfig {
	return storage.ScheduledBackupConfig{
		Enabled:      cfg.BackupScheduler.Enabled,
		DryRun:       cfg.BackupScheduler.DryRun,
		Interval:     cfg.BackupScheduler.Interval,
		NameTemplate: cfg.BackupScheduler.NameTemplate,
		Tables:       append([]string(nil), cfg.BackupScheduler.Tables...),
		Retention: storage.BackupRetentionOptions{
			KeepLatest:    cfg.BackupScheduler.Retention.KeepLatest,
			KeepLatestSet: cfg.BackupScheduler.Retention.KeepLatestSet,
			MaxAge:        cfg.BackupScheduler.Retention.MaxAge,
			MaxAgeSet:     cfg.BackupScheduler.Retention.MaxAgeSet,
			DryRun:        cfg.BackupScheduler.Retention.DryRun,
		},
		Logger:  logger,
		Metrics: prom,
	}
}

func backpressureFromConfig(cfg config.Config) storage.BackpressureOptions {
	return storage.BackpressureOptions{
		Enabled:                     cfg.Storage.BackpressureEnabled,
		RejectOnCritical:            cfg.Storage.BackpressureRejectCritical,
		WarningL0Files:              cfg.Storage.BackpressureWarningL0Files,
		CriticalL0Files:             cfg.Storage.BackpressureCriticalL0Files,
		WarningCompactionDebtBytes:  cfg.Storage.BackpressureWarningDebt,
		CriticalCompactionDebtBytes: cfg.Storage.BackpressureCriticalDebt,
		WarningReadAmp:              cfg.Storage.BackpressureWarningReadAmp,
		CriticalReadAmp:             cfg.Storage.BackpressureCriticalReadAmp,
		WarningDelay:                cfg.Storage.BackpressureWarningDelay,
		CriticalDelay:               cfg.Storage.BackpressureCriticalDelay,
	}
}

func streamRetentionFromConfig(cfg config.Config) storage.StreamRetentionOptions {
	return storage.StreamRetentionOptions{
		Retention: cfg.Storage.StreamRetention,
		MaxBytes:  cfg.Storage.StreamRetentionMaxBytes,
	}
}

// overlayFlags pushes flag values into the cfg struct. Only non-zero
// flag values overwrite — the YAML/env layer wins when the operator
// did not touch the flag. This is the cheap way to keep precedence
// "flag > env > yaml > default" without per-flag tracking of "user
// supplied this" bits.
func overlayFlags(
	cfg *config.Config,
	dataDir, httpAddr string, fsync bool,
	raftBind, raftID, raftPath, raftStorePath string, raftBootstrap bool, raftPeers, raftHTTPPeers string,
	storageProfile, raftStorageProfile string,
	storageBlockCache int64, storageMemTableSize uint64, storageMemTableStopWrites int,
	storageMaxCompactions, storageL0Concurrency, storageL0Threshold int,
	storageL0FileThreshold, storageL0Stop, storageBytesPerSync, storageWALBytesPerSync int,
	backpressureEnabled, backpressureReject bool,
	backpressureWarnL0, backpressureCriticalL0 int64,
	backpressureWarnDebt, backpressureCriticalDebt uint64,
	backpressureWarnReadAmp, backpressureCriticalReadAmp int,
	backpressureWarnDelay, backpressureCriticalDelay time.Duration,
	streamRetention time.Duration, streamRetentionMaxBytes int64,
	identityJwks, identityIssuer, identityAudience string, identityClockSkew time.Duration,
	shardsN int, muxAddr string,
	grpcAddr string, grpcRefl bool, tlsCert, tlsKey, mtlsCA string,
	metricsOff bool, tracingURL string, tracingInsecure bool,
	rebalancerEnabled bool, rebalancerMode string, rebalancerInterval, rebalancerMinInterval time.Duration,
	rebalancerMaxConcurrent, rebalancerMaxHotspots, rebalancerMinVoters int,
	rebalancerApplyTimeout time.Duration, rebalancerManualDir string,
	backupSchedulerEnabled, backupSchedulerDisabled, backupSchedulerDryRun bool,
	backupSchedulerInterval time.Duration, backupSchedulerNameTemplate, backupSchedulerTables string,
	backupSchedulerRetentionKeepLatest int, backupSchedulerRetentionMaxAge time.Duration, backupSchedulerRetentionDryRun bool,
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
	if raftStorePath != "" {
		cfg.Raft.StorePath = raftStorePath
	}
	if raftID != "" {
		cfg.Cluster.SelfID = raftID
	}
	if raftBootstrap {
		cfg.Cluster.Bootstrap = true
	}
	if raftPeers != "" {
		peers, _ := bootstrapserver.ParsePeers(raftPeers)
		cfg.Cluster.Peers = peers
	}
	if raftHTTPPeers != "" {
		hp, _ := bootstrapserver.ParsePeers(raftHTTPPeers)
		cfg.Cluster.HTTPPeers = hp
	}
	if storageProfile != "" {
		cfg.Storage.Profile = storageProfile
	}
	if raftStorageProfile != "" {
		cfg.Storage.RaftProfile = raftStorageProfile
	}
	if storageBlockCache > 0 {
		cfg.Storage.BlockCacheSizeBytes = storageBlockCache
	}
	if storageMemTableSize > 0 {
		cfg.Storage.MemTableSizeBytes = storageMemTableSize
	}
	if storageMemTableStopWrites > 0 {
		cfg.Storage.MemTableStopWritesThreshold = storageMemTableStopWrites
	}
	if storageMaxCompactions > 0 {
		cfg.Storage.MaxConcurrentCompactions = storageMaxCompactions
	}
	if storageL0Concurrency > 0 {
		cfg.Storage.L0CompactionConcurrency = storageL0Concurrency
	}
	if storageL0Threshold > 0 {
		cfg.Storage.L0CompactionThreshold = storageL0Threshold
	}
	if storageL0FileThreshold > 0 {
		cfg.Storage.L0CompactionFileThreshold = storageL0FileThreshold
	}
	if storageL0Stop > 0 {
		cfg.Storage.L0StopWritesThreshold = storageL0Stop
	}
	if storageBytesPerSync > 0 {
		cfg.Storage.BytesPerSync = storageBytesPerSync
	}
	if storageWALBytesPerSync > 0 {
		cfg.Storage.WALBytesPerSync = storageWALBytesPerSync
	}
	if backpressureEnabled {
		cfg.Storage.BackpressureEnabled = true
	}
	if backpressureReject {
		cfg.Storage.BackpressureRejectCritical = true
	}
	if backpressureWarnL0 > 0 {
		cfg.Storage.BackpressureWarningL0Files = backpressureWarnL0
	}
	if backpressureCriticalL0 > 0 {
		cfg.Storage.BackpressureCriticalL0Files = backpressureCriticalL0
	}
	if backpressureWarnDebt > 0 {
		cfg.Storage.BackpressureWarningDebt = backpressureWarnDebt
	}
	if backpressureCriticalDebt > 0 {
		cfg.Storage.BackpressureCriticalDebt = backpressureCriticalDebt
	}
	if backpressureWarnReadAmp > 0 {
		cfg.Storage.BackpressureWarningReadAmp = backpressureWarnReadAmp
	}
	if backpressureCriticalReadAmp > 0 {
		cfg.Storage.BackpressureCriticalReadAmp = backpressureCriticalReadAmp
	}
	if backpressureWarnDelay > 0 {
		cfg.Storage.BackpressureWarningDelay = backpressureWarnDelay
	}
	if backpressureCriticalDelay > 0 {
		cfg.Storage.BackpressureCriticalDelay = backpressureCriticalDelay
	}
	if streamRetention > 0 {
		cfg.Storage.StreamRetention = streamRetention
	}
	if streamRetentionMaxBytes > 0 {
		cfg.Storage.StreamRetentionMaxBytes = streamRetentionMaxBytes
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
	if rebalancerEnabled {
		cfg.Rebalancer.Enabled = true
	}
	if rebalancerMode != "" {
		cfg.Rebalancer.Mode = rebalancerMode
	}
	if rebalancerInterval > 0 {
		cfg.Rebalancer.Interval = rebalancerInterval
	}
	if rebalancerMinInterval > 0 {
		cfg.Rebalancer.MinInterval = rebalancerMinInterval
	}
	if rebalancerMaxConcurrent > 0 {
		cfg.Rebalancer.MaxConcurrentOperations = rebalancerMaxConcurrent
	}
	if rebalancerMaxHotspots > 0 {
		cfg.Rebalancer.MaxHotspots = rebalancerMaxHotspots
	}
	if rebalancerMinVoters > 0 {
		cfg.Rebalancer.MinVoters = rebalancerMinVoters
	}
	if rebalancerApplyTimeout > 0 {
		cfg.Rebalancer.ApplyTimeout = rebalancerApplyTimeout
	}
	if rebalancerManualDir != "" {
		cfg.Rebalancer.ManualPlanDir = rebalancerManualDir
	}
	if backupSchedulerEnabled {
		cfg.BackupScheduler.Enabled = true
	}
	if backupSchedulerDisabled {
		cfg.BackupScheduler.Enabled = false
	}
	if backupSchedulerDryRun {
		cfg.BackupScheduler.DryRun = true
	}
	if backupSchedulerInterval > 0 {
		cfg.BackupScheduler.Interval = backupSchedulerInterval
	}
	if backupSchedulerNameTemplate != "" {
		cfg.BackupScheduler.NameTemplate = backupSchedulerNameTemplate
	}
	if backupSchedulerTables != "" {
		cfg.BackupScheduler.Tables = splitCSVFlag(backupSchedulerTables)
	}
	if backupSchedulerRetentionKeepLatest >= 0 {
		cfg.BackupScheduler.Retention.KeepLatest = backupSchedulerRetentionKeepLatest
		cfg.BackupScheduler.Retention.KeepLatestSet = true
	}
	if backupSchedulerRetentionMaxAge > 0 {
		cfg.BackupScheduler.Retention.MaxAge = backupSchedulerRetentionMaxAge
		cfg.BackupScheduler.Retention.MaxAgeSet = true
	}
	if backupSchedulerRetentionDryRun {
		cfg.BackupScheduler.Retention.DryRun = true
	}
}

func splitCSVFlag(in string) []string {
	if strings.TrimSpace(in) == "" {
		return nil
	}
	parts := strings.Split(in, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
