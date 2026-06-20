package server

import (
	"reflect"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/config"
)

func TestSplitCSVFlag(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "empty", in: "", want: nil},
		{name: "whitespace only", in: "   ", want: nil},
		{name: "single value", in: "alpha", want: []string{"alpha"}},
		{name: "comma separated", in: "a,b,c", want: []string{"a", "b", "c"}},
		{name: "trim spaces", in: " a , b , c ", want: []string{"a", "b", "c"}},
		{name: "drop blank entries", in: "a,, ,b", want: []string{"a", "b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SplitCSVFlag(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("SplitCSVFlag(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

// baseCfg returns a Config with non-zero baseline values so we can
// distinguish "overlay wrote nothing" from "overlay zeroed the field".
// The shape mirrors what config.Defaults() and the YAML loader put on
// the struct before overlayFlags runs in production.
func baseCfg() config.Config {
	var c config.Config
	c.Data = "/srv/baseline"
	c.HTTP.Addr = ":7000"
	c.Identity.ClockSkew = 30 * time.Second
	c.Tracing.Insecure = true
	c.BackupScheduler.Enabled = false
	return c
}

// callOverlay invokes OverlayFlags with all "zero" arguments and lets
// the caller mutate the few they actually want to exercise. The helper
// keeps the per-test setup readable while preserving the exact full
// parameter signature.
type overlayArgs struct {
	dataDir, httpAddr string
	fsync             bool

	raftBind, raftID, raftPath, raftStorePath string
	raftBootstrap                             bool
	raftPeers, raftHTTPPeers                  string
	raftHeartbeatTimeout                      time.Duration
	raftElectionTimeout                       time.Duration
	raftLeaderLeaseTimeout                    time.Duration
	raftCommitTimeout                         time.Duration
	raftApplyTimeout                          time.Duration
	raftSnapshotEntries                       uint64
	raftLogCompression                        string
	raftLogCompressionMinBytes                int
	raftLogCompressionMinSavingsRatio         float64
	raftLogCompressionSkipCooldown            time.Duration

	storageProfile, raftStorageProfile string
	storageBlockCache                  int64
	storageMemTableSize                uint64
	storageMemTableStopWrites          int
	storageMaxCompactions              int
	storageL0Concurrency               int
	storageL0Threshold                 int
	storageL0FileThreshold             int
	storageL0Stop                      int
	storageBytesPerSync                int
	storageWALBytesPerSync             int
	storageLanes                       string
	storageLaneReadWorkers             int
	storageLaneWriteWorkers            int
	storageLaneReadQueue               int
	storageLaneWriteQueue              int

	backpressureEnabled, backpressureReject       bool
	backpressureWarnL0, backpressureCriticalL0    int64
	backpressureWarnDebt, backpressureCriticalDbt uint64
	backpressureWarnReadAmp, backpressureCritRA   int
	backpressureWarnDelay, backpressureCritDelay  time.Duration

	streamRetention         time.Duration
	streamRetentionMaxBytes int64
	storageChangeLogMode    string

	identityJwks, identityIssuer, identityAudience string
	identityClockSkew                              time.Duration

	shardsN           int
	replicationFactor int
	muxAddr           string

	grpcAddr             string
	grpcRefl             bool
	tlsCert, tlsKey, mCA string

	metricsOff      bool
	tracingURL      string
	tracingInsecure bool

	rebalancerEnabled                              bool
	rebalancerMode                                 string
	rebalancerInterval, rebalancerMinInterval      time.Duration
	rebalancerMaxConcurrent, rebalancerMaxHotspots int
	rebalancerMinVoters                            int
	rebalancerApplyTimeout                         time.Duration
	rebalancerManualDir                            string

	backupSchedulerEnabled, backupSchedulerDisabled, backupSchedulerDryRun bool
	backupSchedulerInterval                                                time.Duration
	backupSchedulerNameTemplate, backupSchedulerTables                     string
	backupSchedulerRetentionKeepLatest                                     int
	backupSchedulerRetentionMaxAge                                         time.Duration
	backupSchedulerRetentionDryRun                                         bool
}

// zeroArgs returns a fully-zero argument bundle except for the small
// number of fields whose "do nothing" sentinel is non-zero (clock skew
// defaults to 30s; backup retention KeepLatest sentinel is -1; tracing
// insecure default is true so the overlay path is a no-op).
func zeroArgs() overlayArgs {
	return overlayArgs{
		identityClockSkew:                  30 * time.Second,
		tracingInsecure:                    true,
		raftLogCompressionMinSavingsRatio:  -1,
		raftLogCompressionSkipCooldown:     -1,
		backupSchedulerRetentionKeepLatest: -1,
	}
}

func runOverlay(cfg *config.Config, a overlayArgs) {
	OverlayFlags(cfg,
		a.dataDir, a.httpAddr, a.fsync,
		a.raftBind, a.raftID, a.raftPath, a.raftStorePath, a.raftBootstrap, a.raftPeers, a.raftHTTPPeers,
		a.raftHeartbeatTimeout, a.raftElectionTimeout, a.raftLeaderLeaseTimeout, a.raftCommitTimeout, a.raftApplyTimeout,
		a.raftSnapshotEntries, a.raftLogCompression,
		a.raftLogCompressionMinBytes, a.raftLogCompressionMinSavingsRatio, a.raftLogCompressionSkipCooldown,
		a.storageProfile, a.raftStorageProfile,
		a.storageBlockCache, a.storageMemTableSize, a.storageMemTableStopWrites,
		a.storageMaxCompactions, a.storageL0Concurrency, a.storageL0Threshold,
		a.storageL0FileThreshold, a.storageL0Stop, a.storageBytesPerSync, a.storageWALBytesPerSync,
		a.storageLanes, a.storageLaneReadWorkers, a.storageLaneWriteWorkers,
		a.storageLaneReadQueue, a.storageLaneWriteQueue,
		a.backpressureEnabled, a.backpressureReject,
		a.backpressureWarnL0, a.backpressureCriticalL0,
		a.backpressureWarnDebt, a.backpressureCriticalDbt,
		a.backpressureWarnReadAmp, a.backpressureCritRA,
		a.backpressureWarnDelay, a.backpressureCritDelay,
		a.streamRetention, a.streamRetentionMaxBytes, a.storageChangeLogMode,
		a.identityJwks, a.identityIssuer, a.identityAudience, a.identityClockSkew,
		a.shardsN, a.replicationFactor, a.muxAddr,
		a.grpcAddr, a.grpcRefl, a.tlsCert, a.tlsKey, a.mCA,
		a.metricsOff, a.tracingURL, a.tracingInsecure,
		a.rebalancerEnabled, a.rebalancerMode, a.rebalancerInterval, a.rebalancerMinInterval,
		a.rebalancerMaxConcurrent, a.rebalancerMaxHotspots, a.rebalancerMinVoters,
		a.rebalancerApplyTimeout, a.rebalancerManualDir,
		a.backupSchedulerEnabled, a.backupSchedulerDisabled, a.backupSchedulerDryRun,
		a.backupSchedulerInterval, a.backupSchedulerNameTemplate, a.backupSchedulerTables,
		a.backupSchedulerRetentionKeepLatest, a.backupSchedulerRetentionMaxAge, a.backupSchedulerRetentionDryRun,
	)
}

func TestOverlayFlags_NoOverridesPreservesBaseline(t *testing.T) {
	cfg := baseCfg()
	runOverlay(&cfg, zeroArgs())

	if cfg.Data != "/srv/baseline" {
		t.Errorf("Data overwritten: %q", cfg.Data)
	}
	if cfg.HTTP.Addr != ":7000" {
		t.Errorf("HTTP.Addr overwritten: %q", cfg.HTTP.Addr)
	}
	if cfg.Identity.ClockSkew != 30*time.Second {
		t.Errorf("ClockSkew overwritten: %v", cfg.Identity.ClockSkew)
	}
	if !cfg.Tracing.Insecure {
		t.Errorf("Tracing.Insecure unexpectedly cleared")
	}
}

func TestOverlayFlags_StorageGroup(t *testing.T) {
	cfg := baseCfg()
	args := zeroArgs()
	args.storageProfile = "write-heavy"
	args.storageBlockCache = 1 << 30
	args.storageMemTableSize = 64 << 20
	args.storageL0Threshold = 8
	args.storageLanes = "off"
	args.storageLaneReadWorkers = 4
	args.storageLaneWriteWorkers = 3
	args.storageLaneReadQueue = 128
	args.storageLaneWriteQueue = 64
	args.fsync = true
	runOverlay(&cfg, args)

	if cfg.Storage.Profile != "write-heavy" {
		t.Errorf("Storage.Profile = %q", cfg.Storage.Profile)
	}
	if cfg.Storage.BlockCacheSizeBytes != 1<<30 {
		t.Errorf("BlockCacheSizeBytes = %d", cfg.Storage.BlockCacheSizeBytes)
	}
	if cfg.Storage.MemTableSizeBytes != 64<<20 {
		t.Errorf("MemTableSizeBytes = %d", cfg.Storage.MemTableSizeBytes)
	}
	if cfg.Storage.L0CompactionThreshold != 8 {
		t.Errorf("L0CompactionThreshold = %d", cfg.Storage.L0CompactionThreshold)
	}
	if !cfg.Storage.FsyncOnCommit {
		t.Errorf("FsyncOnCommit not set")
	}
	if cfg.Storage.Lanes != "off" {
		t.Errorf("Lanes = %q", cfg.Storage.Lanes)
	}
	if cfg.Storage.LaneReadWorkers != 4 || cfg.Storage.LaneWriteWorkers != 3 {
		t.Errorf("lane workers = read %d write %d", cfg.Storage.LaneReadWorkers, cfg.Storage.LaneWriteWorkers)
	}
	if cfg.Storage.LaneReadQueue != 128 || cfg.Storage.LaneWriteQueue != 64 {
		t.Errorf("lane queues = read %d write %d", cfg.Storage.LaneReadQueue, cfg.Storage.LaneWriteQueue)
	}
}

func TestOverlayFlags_RaftGroup(t *testing.T) {
	cfg := baseCfg()
	args := zeroArgs()
	args.raftBind = "127.0.0.1:9001"
	args.raftID = "node-a"
	args.raftPath = "/var/cefas/raft"
	args.raftStorePath = "/var/cefas/raft-store"
	args.raftBootstrap = true
	args.raftHeartbeatTimeout = 2 * time.Second
	args.raftElectionTimeout = 10 * time.Second
	args.raftLeaderLeaseTimeout = 1500 * time.Millisecond
	args.raftCommitTimeout = 100 * time.Millisecond
	args.raftApplyTimeout = 30 * time.Second
	args.raftSnapshotEntries = 65536
	args.raftLogCompression = "none"
	args.raftLogCompressionMinBytes = 2048
	args.raftLogCompressionMinSavingsRatio = 0.15
	args.raftLogCompressionSkipCooldown = 2 * time.Second
	runOverlay(&cfg, args)

	if cfg.Raft.Bind != "127.0.0.1:9001" {
		t.Errorf("Raft.Bind = %q", cfg.Raft.Bind)
	}
	if cfg.Raft.Path != "/var/cefas/raft" {
		t.Errorf("Raft.Path = %q", cfg.Raft.Path)
	}
	if cfg.Raft.StorePath != "/var/cefas/raft-store" {
		t.Errorf("Raft.StorePath = %q", cfg.Raft.StorePath)
	}
	if cfg.Cluster.SelfID != "node-a" {
		t.Errorf("Cluster.SelfID = %q", cfg.Cluster.SelfID)
	}
	if !cfg.Cluster.Bootstrap {
		t.Errorf("Cluster.Bootstrap not set")
	}
	if cfg.Raft.HeartbeatTimeout != 2*time.Second {
		t.Errorf("Raft.HeartbeatTimeout = %v", cfg.Raft.HeartbeatTimeout)
	}
	if cfg.Raft.ElectionTimeout != 10*time.Second {
		t.Errorf("Raft.ElectionTimeout = %v", cfg.Raft.ElectionTimeout)
	}
	if cfg.Raft.LeaderLeaseTimeout != 1500*time.Millisecond {
		t.Errorf("Raft.LeaderLeaseTimeout = %v", cfg.Raft.LeaderLeaseTimeout)
	}
	if cfg.Raft.CommitTimeout != 100*time.Millisecond {
		t.Errorf("Raft.CommitTimeout = %v", cfg.Raft.CommitTimeout)
	}
	if cfg.Raft.ApplyTimeout != 30*time.Second {
		t.Errorf("Raft.ApplyTimeout = %v", cfg.Raft.ApplyTimeout)
	}
	if cfg.Raft.SnapshotEntries != 65536 {
		t.Errorf("Raft.SnapshotEntries = %d", cfg.Raft.SnapshotEntries)
	}
	if cfg.Raft.LogCompression != "none" {
		t.Errorf("Raft.LogCompression = %q", cfg.Raft.LogCompression)
	}
	if cfg.Raft.LogCompressionMinBytes != 2048 {
		t.Errorf("Raft.LogCompressionMinBytes = %d", cfg.Raft.LogCompressionMinBytes)
	}
	if cfg.Raft.LogCompressionMinSavingsRatio != 0.15 {
		t.Errorf("Raft.LogCompressionMinSavingsRatio = %v", cfg.Raft.LogCompressionMinSavingsRatio)
	}
	if cfg.Raft.LogCompressionSkipCooldown != 2*time.Second {
		t.Errorf("Raft.LogCompressionSkipCooldown = %v", cfg.Raft.LogCompressionSkipCooldown)
	}
}

func TestOverlayFlags_PeerSetGroup(t *testing.T) {
	cfg := baseCfg()
	args := zeroArgs()
	args.raftPeers = "a=127.0.0.1:9001,b=127.0.0.1:9002"
	args.raftHTTPPeers = "a=http://h1:8080,b=http://h2:8080"
	runOverlay(&cfg, args)

	wantPeers := map[string]string{"a": "127.0.0.1:9001", "b": "127.0.0.1:9002"}
	if !reflect.DeepEqual(cfg.Cluster.Peers, wantPeers) {
		t.Errorf("Cluster.Peers = %#v, want %#v", cfg.Cluster.Peers, wantPeers)
	}
	wantHTTP := map[string]string{"a": "http://h1:8080", "b": "http://h2:8080"}
	if !reflect.DeepEqual(cfg.Cluster.HTTPPeers, wantHTTP) {
		t.Errorf("Cluster.HTTPPeers = %#v, want %#v", cfg.Cluster.HTTPPeers, wantHTTP)
	}
}

func TestOverlayFlags_ClusterGroup(t *testing.T) {
	cfg := baseCfg()
	args := zeroArgs()
	args.shardsN = 24
	args.replicationFactor = 3
	args.muxAddr = "127.0.0.1:7000"
	runOverlay(&cfg, args)

	if cfg.Cluster.Shards != 24 {
		t.Errorf("Cluster.Shards = %d", cfg.Cluster.Shards)
	}
	if cfg.Cluster.ReplicationFactor != 3 {
		t.Errorf("Cluster.ReplicationFactor = %d", cfg.Cluster.ReplicationFactor)
	}
	if cfg.Cluster.MuxAddr != "127.0.0.1:7000" {
		t.Errorf("Cluster.MuxAddr = %q", cfg.Cluster.MuxAddr)
	}
}

func TestOverlayFlags_MetricsAndTracing(t *testing.T) {
	cfg := baseCfg()
	cfg.Metrics.Enabled = true
	args := zeroArgs()
	args.metricsOff = true
	args.tracingURL = "https://collector.example:4317"
	args.tracingInsecure = false
	runOverlay(&cfg, args)

	if cfg.Metrics.Enabled {
		t.Errorf("Metrics.Enabled not disabled")
	}
	if cfg.Tracing.Endpoint != "https://collector.example:4317" {
		t.Errorf("Tracing.Endpoint = %q", cfg.Tracing.Endpoint)
	}
	if cfg.Tracing.Insecure {
		t.Errorf("Tracing.Insecure not cleared when flag false")
	}
}

func TestOverlayFlags_RetentionAndBackup(t *testing.T) {
	cfg := baseCfg()
	args := zeroArgs()
	args.streamRetention = 12 * time.Hour
	args.streamRetentionMaxBytes = 256 << 20
	args.storageChangeLogMode = "streams-only"
	args.backupSchedulerEnabled = true
	args.backupSchedulerInterval = 2 * time.Hour
	args.backupSchedulerNameTemplate = "nightly-{{timestamp}}"
	args.backupSchedulerTables = "orders, payments , audit"
	args.backupSchedulerRetentionKeepLatest = 5
	args.backupSchedulerRetentionMaxAge = 7 * 24 * time.Hour
	args.backupSchedulerRetentionDryRun = true
	runOverlay(&cfg, args)

	if cfg.Storage.StreamRetention != 12*time.Hour {
		t.Errorf("StreamRetention = %v", cfg.Storage.StreamRetention)
	}
	if cfg.Storage.StreamRetentionMaxBytes != 256<<20 {
		t.Errorf("StreamRetentionMaxBytes = %d", cfg.Storage.StreamRetentionMaxBytes)
	}
	if cfg.Storage.ChangeLogMode != "streams-only" {
		t.Errorf("ChangeLogMode = %q", cfg.Storage.ChangeLogMode)
	}
	if !cfg.BackupScheduler.Enabled {
		t.Errorf("BackupScheduler.Enabled not set")
	}
	if cfg.BackupScheduler.Interval != 2*time.Hour {
		t.Errorf("BackupScheduler.Interval = %v", cfg.BackupScheduler.Interval)
	}
	if cfg.BackupScheduler.NameTemplate != "nightly-{{timestamp}}" {
		t.Errorf("BackupScheduler.NameTemplate = %q", cfg.BackupScheduler.NameTemplate)
	}
	wantTables := []string{"orders", "payments", "audit"}
	if !reflect.DeepEqual(cfg.BackupScheduler.Tables, wantTables) {
		t.Errorf("BackupScheduler.Tables = %#v, want %#v", cfg.BackupScheduler.Tables, wantTables)
	}
	if cfg.BackupScheduler.Retention.KeepLatest != 5 || !cfg.BackupScheduler.Retention.KeepLatestSet {
		t.Errorf("KeepLatest = %d set=%v", cfg.BackupScheduler.Retention.KeepLatest, cfg.BackupScheduler.Retention.KeepLatestSet)
	}
	if cfg.BackupScheduler.Retention.MaxAge != 7*24*time.Hour || !cfg.BackupScheduler.Retention.MaxAgeSet {
		t.Errorf("MaxAge = %v set=%v", cfg.BackupScheduler.Retention.MaxAge, cfg.BackupScheduler.Retention.MaxAgeSet)
	}
	if !cfg.BackupScheduler.Retention.DryRun {
		t.Errorf("Retention.DryRun not set")
	}
}

func TestOverlayFlags_BackupSchedulerDisabledFlag(t *testing.T) {
	cfg := baseCfg()
	cfg.BackupScheduler.Enabled = true
	args := zeroArgs()
	args.backupSchedulerDisabled = true
	runOverlay(&cfg, args)
	if cfg.BackupScheduler.Enabled {
		t.Fatalf("disabled flag should clear BackupScheduler.Enabled")
	}
}
