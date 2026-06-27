// cefasdb is the cefas database binary. In single-node mode it
// opens Pebble, loads the catalog, and serves HTTP/JSON. With the
// -raft-bootstrap or -raft-join flags it additionally wires raft
// replication so writes flow through the consensus log.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/CefasDb/cefasdb/internal/auth"
	bootstrapserver "github.com/CefasDb/cefasdb/internal/bootstrap/server"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/cluster"
	"github.com/CefasDb/cefasdb/internal/config"
	"github.com/CefasDb/cefasdb/internal/identitylease"
	"github.com/CefasDb/cefasdb/internal/metrics"
	"github.com/CefasDb/cefasdb/internal/rebalance"
	craft "github.com/CefasDb/cefasdb/internal/replication"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/tracing"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"

	// Side-effect import: every built-in plugin registers against
	// plugin.Default before the server exposes ListPlugins.
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/registry"
)

func main() {
	var (
		dataDir  = flag.String("data", "./cefas-data", "Pebble data directory")
		httpAddr = flag.String("http", ":8080", "HTTP listen address")
		fsync    = flag.Bool("fsync", false, "fsync on commit (durability over throughput)")

		// Raft mode flags. Empty raft-bind keeps the server in
		// single-node mode (Phase 1-3 behaviour).
		raftBind                   = flag.String("raft-bind", "", "Raft TCP bind address (enables Raft mode)")
		raftID                     = flag.String("raft-id", "", "Unique raft ServerID for this node")
		raftPath                   = flag.String("raft-path", "", "Raft state path (snapshots/, etc.). Defaults to -data/raft")
		raftStorePath              = flag.String("raft-store-path", "", "Pebble path for Raft log/stable metadata. Defaults to -data/raft-store in single-Raft mode.")
		raftBootstrap              = flag.Bool("raft-bootstrap", false, "Bootstrap a new cluster from -raft-peers (run on the first node only)")
		raftPeersFlag              = flag.String("raft-peers", "", "Comma-separated id=raftAddr peer list, e.g. 'a=127.0.0.1:9001,b=127.0.0.1:9002,c=127.0.0.1:9003'")
		raftHTTPFlag               = flag.String("raft-http-peers", "", "Comma-separated id=httpURL peer list for 307 redirects, e.g. 'a=http://h1:8080,b=http://h2:8080'")
		raftGRPCFlag               = flag.String("raft-grpc-peers", "", "Comma-separated id=grpcAddr peer list for cross-shard fan-in via Replica.ScanShard, e.g. 'a=h1:9090,b=h2:9090'")
		raftHeartbeat              = flag.Duration("raft-heartbeat-timeout", 0, "Raft heartbeat timeout. 0 inherits config/default.")
		raftElection               = flag.Duration("raft-election-timeout", 0, "Raft election timeout. 0 inherits config/default.")
		raftLease                  = flag.Duration("raft-leader-lease-timeout", 0, "Raft leader lease timeout. Must be <= heartbeat timeout. 0 inherits config/default.")
		raftCommit                 = flag.Duration("raft-commit-timeout", 0, "Raft commit timeout. 0 inherits config/default.")
		raftApply                  = flag.Duration("raft-apply-timeout", 0, "Raft apply timeout per replicated batch. 0 inherits config/default.")
		raftSnapshots              = flag.Uint64("raft-snapshot-entries", 0, "Raft log entries between snapshots. 0 inherits config/default.")
		raftLogComp                = flag.String("raft-log-compression", "", "Raft log payload compression: snappy or none. Empty inherits config/default.")
		raftLogCompMinBytes        = flag.Int("raft-log-compression-min-bytes", 0, "Minimum raft log payload bytes before compression. 0 inherits config/default.")
		raftLogCompMinSavingsRatio = flag.Float64("raft-log-compression-min-savings-ratio", -1, "Minimum compression savings ratio required to keep compressed payloads. Negative inherits config/default.")
		raftLogCompSkipCooldown    = flag.Duration("raft-log-compression-skip-cooldown", -1, "Cooldown after an unhelpful compression attempt. Negative inherits config/default; 0 disables cooldown.")
		raftIdentityLeaseBackend   = flag.String("raft-identity-lease-backend", "", "Raft identity lease backend: file, kubernetes, auto, or off. Empty inherits config/default.")
		raftIdentityLeasePath      = flag.String("raft-identity-lease-path", "", "Directory for file-backed raft identity leases. Empty defaults to -data/raft-identity.")
		raftIdentityLeaseName      = flag.String("raft-identity-lease-name", "", "Lease resource name for external backends. Empty derives from -raft-id.")
		raftIdentityLeaseNamespace = flag.String("raft-identity-lease-namespace", "", "Kubernetes namespace for raft identity Lease objects. Empty uses in-cluster namespace.")
		raftIdentityLeaseAPIURL    = flag.String("raft-identity-kubernetes-api-url", "", "Kubernetes API URL for raft identity leases. Empty uses in-cluster service discovery.")
		raftIdentityLeaseTTL       = flag.Duration("raft-identity-lease-ttl", 0, "Raft identity lease TTL. 0 inherits config/default.")
		raftIdentityLeaseRenew     = flag.Duration("raft-identity-lease-renew-interval", 0, "Raft identity lease renew interval. 0 inherits config/default.")

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
		storageLanes              = flag.String("storage-lanes", "", "Storage read/write lanes: auto, on, or off. Empty inherits config/default.")
		storageLaneReadWorkers    = flag.Int("storage-lane-read-workers", 0, "Read lane worker count. 0 inherits default.")
		storageLaneWriteWorkers   = flag.Int("storage-lane-write-workers", 0, "Write lane worker count. 0 inherits default.")
		storageLaneReadQueue      = flag.Int("storage-lane-read-queue", 0, "Read lane queue capacity. 0 inherits default.")
		storageLaneWriteQueue     = flag.Int("storage-lane-write-queue", 0, "Write lane queue capacity. 0 inherits default.")

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
		streamRetentionInterval  = flag.Duration("storage-stream-retention-interval", 0, "Background stream retention scan interval. Positive enables; negative disables. 0 inherits config/default disabled.")
		streamRetentionMaxBytes  = flag.Int64("storage-stream-retention-max-bytes", 0, "Maximum logical DynamoDB Streams retained bytes per table. 0 disables byte cap.")
		storageChangeLogMode     = flag.String("storage-changelog-mode", "", "Physical changelog mode: always, streams-only, or off. Empty inherits config/default.")
		storageAdaptiveMode      = flag.Bool("storage-adaptive-mode", false, "Enable workload-mode adaptive tuner (read/write ratio observer adjusts commitLoop merge cap and retention interval). Off by default.")

		// Identity/auth flags. Empty -identity-jwks-url keeps the
		// server open (single-node dev mode).
		identityJwks      = flag.String("identity-jwks-url", "", "Tikti JWKS endpoint (enables bearer-token auth)")
		identityIssuer    = flag.String("identity-issuer", "", "Expected token issuer")
		identityAudience  = flag.String("identity-audience", "", "Expected token audience")
		identityClockSkew = flag.Duration("identity-clock-skew", 30*time.Second, "Allowed clock skew on exp/iat checks")

		// Multi-Raft sharding.
		shardsN           = flag.Int("shards", 0, "Number of shards (multi-Raft). 0 → single-shard / single-node legacy bootstrap.")
		replicationFactor = flag.Int("replication-factor", 0, "Number of voters per data shard during fresh multi-Raft placement bootstrap. 0 uses every peer.")
		muxAddr           = flag.String("mux", "", "Mux TCP address shared by every shard's raft transport (multi-Raft mode).")

		// gRPC flags.
		grpcAddr       = flag.String("grpc", "", "gRPC listen address (e.g. ':9090'). Empty disables gRPC.")
		grpcReflection = flag.Bool("grpc-reflection", false, "Enable gRPC server reflection (handy for grpcurl)")
		tlsCert        = flag.String("tls-cert", "", "Path to TLS certificate (PEM). Enables TLS on the gRPC listener.")
		tlsKey         = flag.String("tls-key", "", "Path to TLS private key (PEM)")
		mtlsCA         = flag.String("mtls-ca", "", "Path to a client-CA bundle. When set, the gRPC listener requires mTLS.")

		// Observability + config.
		configPath                 = flag.String("config", "", "Path to YAML config file. Flag/env values override the file.")
		metricsOff                 = flag.Bool("metrics-disabled", false, "Disable the /metrics Prometheus endpoint.")
		tracingURL                 = flag.String("tracing-endpoint", "", "OTLP/gRPC collector endpoint (e.g. 'jaeger:4317'). Empty disables tracing.")
		tracingIns                 = flag.Bool("tracing-insecure", true, "Disable TLS to the OTLP collector.")
		shutdownGracePeriod        = flag.Duration("shutdown-grace-period", 0, "Grace period for HTTP/gRPC/Raft/storage shutdown. 0 inherits config/default.")
		shutdownDrainDelay         = flag.Duration("shutdown-drain-delay", -1, "Delay after readiness is dropped before listener shutdown. Negative inherits config/default; 0 disables delay.")
		shutdownLeadershipTransfer = flag.Duration("shutdown-leadership-transfer-timeout", 0, "Per-shard leadership transfer timeout during shutdown. 0 inherits config/default.")

		// pprof debug listener. Default empty = disabled. Recommended
		// bind '127.0.0.1:6060' for benchmark runs; binding to a
		// non-loopback interface logs a warning because pprof leaks
		// process internals.
		pprofAddr      = flag.String("pprof-addr", "", "Bind address for net/http/pprof (e.g. '127.0.0.1:6060'). Empty disables it.")
		pprofMutexRate = flag.Int("pprof-mutex-rate", 0, "runtime.SetMutexProfileFraction value (1 = sample every event). 0 disables mutex profiling.")
		pprofBlockRate = flag.Int("pprof-block-rate", 0, "runtime.SetBlockProfileRate value in nanoseconds (1 = sample every event). 0 disables block profiling.")

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

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logf := func(format string, args ...any) {
		logger.Info(fmt.Sprintf(format, args...))
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}
	if err := config.ApplyEnv(&cfg); err != nil {
		logger.Error("config env", "err", err)
		os.Exit(1)
	}
	// Promote any flag value the user actually set onto cfg so the
	// downstream code paths can read a single source of truth.
	bootstrapserver.OverlayFlags(&cfg, *dataDir, *httpAddr, *fsync,
		*raftBind, *raftID, *raftPath, *raftStorePath, *raftBootstrap, *raftPeersFlag, *raftHTTPFlag, *raftGRPCFlag,
		*raftHeartbeat, *raftElection, *raftLease, *raftCommit, *raftApply,
		*raftSnapshots, *raftLogComp, *raftLogCompMinBytes, *raftLogCompMinSavingsRatio, *raftLogCompSkipCooldown,
		*storageProfile, *raftStorageProfile,
		*storageBlockCache, *storageMemTableSize, *storageMemTableStopWrites,
		*storageMaxCompactions, *storageL0Concurrency, *storageL0Threshold,
		*storageL0FileThreshold, *storageL0Stop, *storageBytesPerSync, *storageWALBytesPerSync,
		*storageLanes, *storageLaneReadWorkers, *storageLaneWriteWorkers,
		*storageLaneReadQueue, *storageLaneWriteQueue,
		*backpressureEnabled, *backpressureReject, *backpressureWarnL0, *backpressureCriticalL0,
		*backpressureWarnDebt, *backpressureCriticalDebt, *backpressureWarnReadAmp,
		*backpressureCritReadAmp, *backpressureWarnDelay, *backpressureCritDelay,
		*streamRetention, *streamRetentionInterval, *streamRetentionMaxBytes, *storageChangeLogMode,
		*identityJwks, *identityIssuer, *identityAudience, *identityClockSkew,
		*shardsN, *replicationFactor, *muxAddr,
		*grpcAddr, *grpcReflection, *tlsCert, *tlsKey, *mtlsCA,
		*metricsOff, *tracingURL, *tracingIns,
		*rebalancerEnabled, *rebalancerMode, *rebalancerInterval, *rebalancerMinInterval,
		*rebalancerMaxConcurrent, *rebalancerMaxHotspots, *rebalancerMinVoters,
		*rebalancerApplyTimeout, *rebalancerManualDir,
		*backupSchedulerEnabled, *backupSchedulerDisabled, *backupSchedulerDryRun,
		*backupSchedulerInterval, *backupSchedulerNameTemplate, *backupSchedulerTables,
		*backupSchedulerRetentionKeepLatest, *backupSchedulerRetentionMaxAge, *backupSchedulerRetentionDryRun)
	bootstrapserver.OverlayRaftIdentityLeaseFlags(&cfg,
		*raftIdentityLeaseBackend, *raftIdentityLeasePath, *raftIdentityLeaseName,
		*raftIdentityLeaseNamespace, *raftIdentityLeaseAPIURL,
		*raftIdentityLeaseTTL, *raftIdentityLeaseRenew)
	bootstrapserver.OverlayLifecycleFlags(&cfg, *shutdownGracePeriod, *shutdownDrainDelay, *shutdownLeadershipTransfer)

	// Initialise tracing first so subsequent setup gets spans on
	// failure. tracingShutdown is a no-op when no endpoint is set.
	tracingShutdown, err := tracing.Init(context.Background(), tracing.Config{
		Endpoint:   cfg.Tracing.Endpoint,
		Insecure:   cfg.Tracing.Insecure,
		SampleRate: cfg.Tracing.SampleRate,
	})
	if err != nil {
		logger.Error("tracing", "err", err)
		os.Exit(1)
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

	pprofSrv, err := server.StartPprof(server.PprofOptions{
		Addr:      *pprofAddr,
		MutexRate: *pprofMutexRate,
		BlockRate: *pprofBlockRate,
	}, logf)
	if err != nil {
		logger.Error("pprof", "err", err)
		os.Exit(1)
	}

	leaseCtx, leaseCancel := context.WithCancel(context.Background())
	defer leaseCancel()
	leaseLost := make(chan error, 1)
	var leaseHealthy atomic.Bool
	var raftIdentityGuard *identitylease.Guard
	if raftIdentityLeaseRequired(cfg) {
		raftIdentityGuard, err = identitylease.Acquire(context.Background(), identitylease.Options{
			NodeID:                cfg.Cluster.SelfID,
			DataDir:               cfg.Data,
			Backend:               cfg.RaftIdentity.LeaseBackend,
			LeasePath:             cfg.RaftIdentity.LeasePath,
			LeaseName:             cfg.RaftIdentity.LeaseName,
			LeaseTTL:              cfg.RaftIdentity.LeaseTTL,
			RenewInterval:         cfg.RaftIdentity.LeaseRenewInterval,
			KubernetesNamespace:   cfg.RaftIdentity.LeaseNamespace,
			KubernetesAPIURL:      cfg.RaftIdentity.KubernetesAPIURL,
			KubernetesBearerToken: "",
			KubernetesCAFile:      "",
			HTTPClient:            nil,
		})
		if err != nil {
			logger.Error("raft identity lease", "err", err, "raftID", cfg.Cluster.SelfID, "backend", cfg.RaftIdentity.LeaseBackend)
			os.Exit(1)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := raftIdentityGuard.Close(ctx); err != nil {
				logger.Error("release raft identity lease", "err", err, "raftID", cfg.Cluster.SelfID)
			}
		}()
		leaseHealthy.Store(true)
		rec := raftIdentityGuard.Record()
		logger.Info("raft identity lease acquired", "raftID", rec.NodeID, "holder", rec.HolderID, "epoch", rec.Epoch, "backend", rec.Backend, "resource", rec.Resource, "expires", rec.ExpiresAt)
		go raftIdentityGuard.RenewLoop(leaseCtx, func(err error) {
			leaseHealthy.Store(false)
			logger.Error("raft identity lease lost", "err", err, "raftID", cfg.Cluster.SelfID)
			select {
			case leaseLost <- err:
			default:
			}
		})
	} else if cfg.Cluster.SelfID != "" && cfg.RaftIdentity.LeaseBackend == identitylease.BackendOff {
		logger.Info("raft identity lease disabled", "raftID", cfg.Cluster.SelfID)
	}

	var (
		db     *pebble.DB
		cat    *catalog.Catalog
		mgr    *cluster.Manager
		raftDB *craft.DB
	)

	if cfg.Cluster.Shards > 0 {
		mgr, err = cluster.Open(context.Background(), cluster.Config{
			Root:                          cfg.Data,
			Shards:                        cfg.Cluster.Shards,
			ReplicationFactor:             cfg.Cluster.ReplicationFactor,
			SelfID:                        cfg.Cluster.SelfID,
			MuxAddr:                       cfg.Cluster.MuxAddr,
			Peers:                         cfg.Cluster.Peers,
			PeerHTTPAddrs:                 cfg.Cluster.HTTPPeers,
			PeerGRPCAddrs:                 cfg.Cluster.GRPCPeers,
			Bootstrap:                     cfg.Cluster.Bootstrap,
			FsyncOnCommit:                 cfg.Storage.FsyncOnCommit,
			StorageProfile:                cfg.Storage.Profile,
			StorageTuning:                 bootstrapserver.StorageTuning(cfg),
			Backpressure:                  bootstrapserver.BackpressureOptions(cfg),
			StorageLanes:                  bootstrapserver.StorageLaneOptions(cfg),
			StreamRetention:               bootstrapserver.StreamRetentionOptions(cfg),
			ChangeLogMode:                 cfg.Storage.ChangeLogMode,
			AdaptiveStorageMode:           *storageAdaptiveMode,
			RaftProfile:                   cfg.Storage.RaftProfile,
			HeartbeatMS:                   int(cfg.Raft.HeartbeatTimeout / time.Millisecond),
			ElectionMS:                    int(cfg.Raft.ElectionTimeout / time.Millisecond),
			LeaderLeaseMS:                 int(cfg.Raft.LeaderLeaseTimeout / time.Millisecond),
			CommitMS:                      int(cfg.Raft.CommitTimeout / time.Millisecond),
			ApplyTimeout:                  cfg.Raft.ApplyTimeout,
			SnapshotEntries:               cfg.Raft.SnapshotEntries,
			LogCompression:                cfg.Raft.LogCompression,
			LogCompressionMinBytes:        cfg.Raft.LogCompressionMinBytes,
			LogCompressionMinSavingsRatio: cfg.Raft.LogCompressionMinSavingsRatio,
			LogCompressionSkipCooldown:    cfg.Raft.LogCompressionSkipCooldown,
		})
		if err != nil {
			logger.Error("open cluster manager", "err", err)
			os.Exit(1)
		}
		defer mgr.Close()
		// Shard 0 is the metadata shard; the catalog lives there
		// and gets fanned out to other shards by the API layer.
		shard0, _ := mgr.Shard(0)
		db = shard0.Storage
		cat, err = catalog.New(db)
		if err != nil {
			logger.Error("load catalog (shard 0)", "err", err)
			os.Exit(1)
		}
		logger.Info("multi-Raft enabled", "shards", cfg.Cluster.Shards, "mux", cfg.Cluster.MuxAddr, "peers", cfg.Cluster.Peers)
	} else {
		var err error
		db, err = pebble.Open(bootstrapserver.StorageOptions(cfg, cfg.Data))
		if err != nil {
			logger.Error("open pebble", "err", err)
			os.Exit(1)
		}
		defer db.Close()
		cat, err = catalog.New(db)
		if err != nil {
			logger.Error("load catalog", "err", err)
			os.Exit(1)
		}
	}

	// Per-table CDC retention (#521): catalog supplies the override
	// for any stream-enabled table that declares
	// StreamSpecification.RetentionSeconds. Zero means "inherit
	// cluster default".
	db.AttachStreamRetentionResolver(func(table string) int64 {
		if cat == nil {
			return 0
		}
		td, err := cat.Describe(table)
		if err != nil || td.StreamSpecification == nil {
			return 0
		}
		return td.StreamSpecification.RetentionSeconds
	})

	var raftStore *pebble.DB
	if mgr == nil && cfg.Raft.Bind != "" {
		if cfg.Cluster.SelfID == "" {
			logger.Error("invalid flags", "reason", "-raft-id is required when -raft-bind is set")
			os.Exit(1)
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
			raftProfile = pebble.ProfileRaft
		}
		raftStore, err = pebble.Open(pebble.Options{
			Path:          storePath,
			FsyncOnCommit: cfg.Storage.FsyncOnCommit,
			Profile:       raftProfile,
		})
		if err != nil {
			logger.Error("open raft store", "err", err)
			os.Exit(1)
		}
		defer raftStore.Close()
		raftDB, err = craft.Open(context.Background(), craft.Config{
			Path:                          path,
			SelfID:                        cfg.Cluster.SelfID,
			BindAddr:                      cfg.Raft.Bind,
			Bootstrap:                     cfg.Cluster.Bootstrap,
			PeerAddrs:                     cfg.Cluster.Peers,
			PeerHTTPAddrs:                 cfg.Cluster.HTTPPeers,
			HeartbeatMS:                   int(cfg.Raft.HeartbeatTimeout / time.Millisecond),
			ElectionMS:                    int(cfg.Raft.ElectionTimeout / time.Millisecond),
			LeaderLeaseMS:                 int(cfg.Raft.LeaderLeaseTimeout / time.Millisecond),
			CommitMS:                      int(cfg.Raft.CommitTimeout / time.Millisecond),
			ApplyTimeout:                  cfg.Raft.ApplyTimeout,
			SnapshotEntries:               cfg.Raft.SnapshotEntries,
			LogCompression:                cfg.Raft.LogCompression,
			LogCompressionMinBytes:        cfg.Raft.LogCompressionMinBytes,
			LogCompressionMinSavingsRatio: cfg.Raft.LogCompressionMinSavingsRatio,
			LogCompressionSkipCooldown:    cfg.Raft.LogCompressionSkipCooldown,
			CommittedApplier:              db,
		}, db.Raw(), raftStore.Raw())
		if err != nil {
			logger.Error("open raft", "err", err)
			os.Exit(1)
		}
		defer raftDB.Close()
		db.AttachReplicator(raftDB)
		logger.Info("raft attached", "id", cfg.Cluster.SelfID, "bind", cfg.Raft.Bind, "bootstrap", cfg.Cluster.Bootstrap, "peers", cfg.Cluster.Peers, "raftStore", storePath)
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
			logger.Error("auth validator", "err", err)
			os.Exit(1)
		}
		logger.Info("identity auth enabled", "jwks", cfg.Identity.JwksURL, "issuer", cfg.Identity.Issuer, "audience", cfg.Identity.Audience)
	}

	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	defer runtimeCancel()
	backupScheduler := pebble.NewScheduledBackupRunner(db, bootstrapserver.ScheduledBackupConfig(cfg, prom, logf))

	mux := http.NewServeMux()
	apiSrv := server.New(db, cat)
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
	if raftIdentityGuard != nil {
		apiSrv.AddReadinessCheck("raft_identity_lease", func(context.Context) error {
			if !leaseHealthy.Load() {
				return fmt.Errorf("raft identity lease not healthy")
			}
			rec := raftIdentityGuard.Record()
			if rec.NodeID == "" || rec.HolderID == "" || rec.Epoch == 0 {
				return fmt.Errorf("raft identity lease incomplete")
			}
			if !rec.ExpiresAt.IsZero() && time.Now().UTC().After(rec.ExpiresAt) {
				return fmt.Errorf("raft identity lease expired at %s", rec.ExpiresAt.Format(time.RFC3339Nano))
			}
			return nil
		})
	}
	if cfg.BackupScheduler.Enabled {
		go backupScheduler.Run(runtimeCtx)
		logger.Info("scheduled backups enabled", "interval", cfg.BackupScheduler.Interval, "dryRun", cfg.BackupScheduler.DryRun, "template", cfg.BackupScheduler.NameTemplate, "tables", cfg.BackupScheduler.Tables)
	}
	if cfg.Rebalancer.Enabled {
		if mgr == nil {
			logger.Info("rebalancer disabled", "reason", "multi-Raft cluster manager is not configured")
		} else if prom == nil {
			logger.Info("rebalancer disabled", "reason", "metrics must be enabled for hotspot input")
		} else {
			ctrl := rebalance.NewController(bootstrapserver.RebalancerConfig(cfg), mgr, prom, nil)
			ctrl.SetLogger(logf)
			go ctrl.Run(runtimeCtx)
			logger.Info("rebalancer enabled", "mode", cfg.Rebalancer.Mode, "interval", cfg.Rebalancer.Interval, "maxConcurrent", cfg.Rebalancer.MaxConcurrentOperations)
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
		logger.Info("cefasdb listening", "addr", cfg.HTTP.Addr, "data", cfg.Data, "mode", mode)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http serve", "err", err)
			os.Exit(1)
		}
	}()

	// gRPC listener (optional).
	var gsrv *grpc.Server
	var gsrvImpl *server.GRPCServer
	if cfg.GRPC.Addr != "" {
		// Workload prioritization (#499): wire the per-SL quota
		// controller against the catalog. Hot reload via
		// OnServiceLevelChanged so an ALTER SERVICE LEVEL takes
		// effect on the next request.
		slQuota := server.NewSLQuotaController(cat, nil)
		cat.OnServiceLevelChanged(slQuota.Invalidate)
		opts, err := bootstrapserver.BuildGRPCOpts(validator, cfg.GRPC.TLSCertPath, cfg.GRPC.TLSKeyPath, cfg.GRPC.MTLSCAPath, slQuota)
		if err != nil {
			logger.Error("grpc opts", "err", err)
			os.Exit(1)
		}
		gsrv = grpc.NewServer(opts...)
		var clu server.Cluster
		if raftDB != nil {
			clu = raftDB
		} else if mgr != nil {
			if sh, ok := mgr.Shard(0); ok && sh.Raft != nil {
				clu = sh.Raft
			}
		}
		gsrvImpl = server.NewGRPCServer(db, cat, clu)
		gsrvImpl.AttachLifecycle(apiSrv.Lifecycle())
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
		cefaspb.RegisterReplicaServer(gsrv, gsrvImpl)
		server.RegisterAtomic(gsrv, gsrvImpl)
		if cfg.GRPC.Reflection {
			reflection.Register(gsrv)
		}
		ln, err := net.Listen("tcp", cfg.GRPC.Addr)
		if err != nil {
			logger.Error("grpc listen", "err", err)
			os.Exit(1)
		}
		go func() {
			logger.Info("gRPC listening", "addr", cfg.GRPC.Addr, "tls", cfg.GRPC.TLSCertPath != "", "mtls", cfg.GRPC.MTLSCAPath != "", "reflection", cfg.GRPC.Reflection)
			if err := gsrv.Serve(ln); err != nil {
				logger.Error("grpc serve", "err", err)
			}
		}()
	}
	apiSrv.MarkStarted()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	shutdownReason := "signal"
	select {
	case sig := <-stop:
		shutdownReason = sig.String()
	case err := <-leaseLost:
		shutdownReason = "raft identity lease lost: " + err.Error()
	}
	logger.Info("shutting down", "reason", shutdownReason)
	apiSrv.StartDraining(shutdownReason)
	rejectWrites(db, mgr, shutdownReason)
	if cfg.Lifecycle.DrainDelay > 0 {
		time.Sleep(cfg.Lifecycle.DrainDelay)
	}
	if err := transferLeadershipOnShutdown(context.Background(), raftDB, mgr, cfg.Cluster.SelfID, cfg.Cluster.Peers, cfg.Lifecycle.LeadershipTransferTimeout); err != nil {
		logger.Error("shutdown leadership transfer", "err", err)
	}
	leaseCancel()
	runtimeCancel()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Lifecycle.ShutdownGracePeriod)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = server.ShutdownPprof(ctx, pprofSrv)
	if gsrvImpl != nil {
		gsrvImpl.StopMVScheduler()
	}
	if gsrv != nil {
		done := make(chan struct{})
		go func() {
			gsrv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			gsrv.Stop()
		}
	}
}

func raftIdentityLeaseRequired(cfg config.Config) bool {
	if cfg.Cluster.SelfID == "" || cfg.RaftIdentity.LeaseBackend == identitylease.BackendOff {
		return false
	}
	return cfg.Cluster.Shards > 0 || cfg.Raft.Bind != ""
}

func rejectWrites(db *pebble.DB, mgr *cluster.Manager, reason string) {
	if mgr == nil {
		if db != nil {
			db.RejectWrites(reason)
		}
		return
	}
	for _, sh := range mgr.Shards() {
		if sh != nil && sh.Storage != nil {
			sh.Storage.RejectWrites(reason)
		}
	}
}

func transferLeadershipOnShutdown(ctx context.Context, raftDB *craft.DB, mgr *cluster.Manager, selfID string, peers map[string]string, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	if mgr == nil {
		if raftDB == nil || !raftDB.IsLeader() {
			return nil
		}
		targetID, targetAddr := firstPeerTarget(selfID, peers, nil)
		if targetID == "" {
			return nil
		}
		return raftDB.TransferLeadership(targetID, targetAddr, timeout)
	}
	var errs []error
	for _, sh := range mgr.Shards() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if sh == nil || sh.Raft == nil || !sh.Raft.IsLeader() {
			continue
		}
		targetID, targetAddr := firstPeerTarget(selfID, peers, sh.Voters)
		if targetID == "" {
			continue
		}
		if err := sh.Raft.TransferLeadership(targetID, targetAddr, timeout); err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", sh.ID, err))
		}
	}
	return errors.Join(errs...)
}

func firstPeerTarget(selfID string, peers map[string]string, voters []string) (string, string) {
	if len(voters) > 0 {
		for _, id := range voters {
			if id == "" || id == selfID {
				continue
			}
			if addr := peers[id]; addr != "" {
				return id, addr
			}
		}
		return "", ""
	}
	for id, addr := range peers {
		if id != "" && id != selfID && addr != "" {
			return id, addr
		}
	}
	return "", ""
}
