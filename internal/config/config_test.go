package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/config"
)

func TestDefaultsPopulated(t *testing.T) {
	d := config.Defaults()
	if d.HTTP.Addr == "" {
		t.Errorf("HTTP.Addr default missing")
	}
	if d.Identity.ClockSkew != 30*time.Second {
		t.Errorf("clock skew default = %v", d.Identity.ClockSkew)
	}
	if d.Lifecycle.ShutdownGracePeriod != 25*time.Second || d.Lifecycle.DrainDelay != 2*time.Second || d.Lifecycle.LeadershipTransferTimeout != 5*time.Second {
		t.Errorf("lifecycle defaults not populated: %+v", d.Lifecycle)
	}
	if !d.Metrics.Enabled {
		t.Errorf("metrics should default on")
	}
	if d.Metrics.HotspotBuckets != 64 || d.Metrics.HotspotCoolingWindow != time.Minute {
		t.Errorf("hotspot defaults not populated: %+v", d.Metrics)
	}
	if d.Rebalancer.Mode != "dry-run" || d.Rebalancer.Interval != 30*time.Second || d.Rebalancer.MaxConcurrentOperations != 1 {
		t.Errorf("rebalancer defaults not populated: %+v", d.Rebalancer)
	}
	if d.BackupScheduler.Enabled || d.BackupScheduler.Interval != time.Hour || d.BackupScheduler.NameTemplate == "" {
		t.Errorf("backup scheduler defaults not populated: %+v", d.BackupScheduler)
	}
	if d.Raft.HeartbeatTimeout != 2*time.Second || d.Raft.ElectionTimeout != 10*time.Second || d.Raft.LeaderLeaseTimeout != 2*time.Second {
		t.Errorf("raft timeout defaults not populated: %+v", d.Raft)
	}
	if d.Raft.CommitTimeout != 100*time.Millisecond || d.Raft.ApplyTimeout != 30*time.Second {
		t.Errorf("raft commit/apply defaults not populated: %+v", d.Raft)
	}
	if d.Raft.SnapshotEntries != 65536 {
		t.Errorf("raft snapshot entries default = %d", d.Raft.SnapshotEntries)
	}
	if d.Raft.LogCompression != "snappy" {
		t.Errorf("raft log compression default = %q", d.Raft.LogCompression)
	}
	if d.Raft.LogCompressionMinBytes != 1024 || d.Raft.LogCompressionMinSavingsRatio != 0.05 || d.Raft.LogCompressionSkipCooldown != time.Second {
		t.Errorf("raft log compression guardrail defaults not populated: %+v", d.Raft)
	}
	if d.RaftIdentity.LeaseBackend != "file" || d.RaftIdentity.LeaseTTL != 30*time.Second || d.RaftIdentity.LeaseRenewInterval != 10*time.Second {
		t.Errorf("raft identity lease defaults not populated: %+v", d.RaftIdentity)
	}
	if d.Storage.Lanes != "auto" {
		t.Errorf("storage lanes default = %q", d.Storage.Lanes)
	}
	if d.Storage.StreamRetentionInterval >= 0 {
		t.Errorf("storage stream retention loop should default disabled, got %v", d.Storage.StreamRetentionInterval)
	}
}

func TestLoadFileMissingReturnsDefaults(t *testing.T) {
	cfg, err := config.LoadFile(filepath.Join(t.TempDir(), "no-such-file"))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if cfg.HTTP.Addr == "" {
		t.Errorf("defaults lost")
	}
}

func TestLoadFileYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cefas.yaml")
	yamlSrc := `
data: /var/lib/cefas-test
http:
  addr: ":18080"
lifecycle:
  shutdownGracePeriod: 20s
  drainDelay: 1500ms
  leadershipTransferTimeout: 4s
cluster:
  shards: 3
  replicationFactor: 2
  bootstrap: true
  peers:
    n1: 10.0.0.1:9100
    n2: 10.0.0.2:9100
storage:
  changeLogMode: streams-only
  streamRetentionInterval: 5m
  lanes: off
  laneReadWorkers: 4
  laneWriteWorkers: 3
  laneReadQueue: 128
  laneWriteQueue: 64
raft:
  heartbeatTimeout: 3s
  electionTimeout: 12s
  leaderLeaseTimeout: 1500ms
  commitTimeout: 125ms
  applyTimeout: 45s
  snapshotEntries: 131072
  logCompression: none
  logCompressionMinBytes: 2048
  logCompressionMinSavingsRatio: 0.2
  logCompressionSkipCooldown: 2s
raftIdentity:
  leaseBackend: kubernetes
  leaseName: cefasdb-geo-cefas-0
  leaseNamespace: cefasdb
  leaseTtl: 45s
  leaseRenewInterval: 15s
identity:
  jwksUrl: https://tikti.example.com/jwks.json
  clockSkew: 45s
metrics:
  hotspotBuckets: 16
  hotspotWriteThreshold: 42
  hotspotLatencyThreshold: 75ms
rebalancer:
  enabled: true
  mode: manual
  interval: 10s
  manualPlanDir: /tmp/cefas-rebalance
backupScheduler:
  enabled: true
  dryRun: true
  interval: 15m
  nameTemplate: "nightly-{{date}}"
  tables: [Users, Orders]
  retention:
    keepLatest: 7
    keepLatestSet: true
    maxAge: 720h
    maxAgeSet: true
    dryRun: true
`
	if err := os.WriteFile(path, []byte(yamlSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Addr != ":18080" || cfg.Cluster.Shards != 3 || cfg.Cluster.ReplicationFactor != 2 || !cfg.Cluster.Bootstrap {
		t.Fatalf("YAML did not override: %+v", cfg)
	}
	if cfg.Cluster.Peers["n2"] != "10.0.0.2:9100" {
		t.Fatalf("peer map lost: %+v", cfg.Cluster.Peers)
	}
	if cfg.Identity.ClockSkew != 45*time.Second {
		t.Fatalf("clock skew = %v", cfg.Identity.ClockSkew)
	}
	if cfg.Lifecycle.ShutdownGracePeriod != 20*time.Second || cfg.Lifecycle.DrainDelay != 1500*time.Millisecond || cfg.Lifecycle.LeadershipTransferTimeout != 4*time.Second {
		t.Fatalf("lifecycle config not loaded: %+v", cfg.Lifecycle)
	}
	if cfg.Storage.ChangeLogMode != "streams-only" {
		t.Fatalf("storage changelog mode config not loaded: %+v", cfg.Storage)
	}
	if cfg.Storage.StreamRetentionInterval != 5*time.Minute {
		t.Fatalf("storage stream retention interval config not loaded: %+v", cfg.Storage)
	}
	if cfg.Storage.Lanes != "off" || cfg.Storage.LaneReadWorkers != 4 || cfg.Storage.LaneWriteWorkers != 3 || cfg.Storage.LaneReadQueue != 128 || cfg.Storage.LaneWriteQueue != 64 {
		t.Fatalf("storage lanes config not loaded: %+v", cfg.Storage)
	}
	if cfg.Raft.HeartbeatTimeout != 3*time.Second || cfg.Raft.ElectionTimeout != 12*time.Second || cfg.Raft.LeaderLeaseTimeout != 1500*time.Millisecond {
		t.Fatalf("raft timeout config not loaded: %+v", cfg.Raft)
	}
	if cfg.Raft.CommitTimeout != 125*time.Millisecond || cfg.Raft.ApplyTimeout != 45*time.Second {
		t.Fatalf("raft commit/apply config not loaded: %+v", cfg.Raft)
	}
	if cfg.Raft.SnapshotEntries != 131072 {
		t.Fatalf("raft snapshot entries config not loaded: %+v", cfg.Raft)
	}
	if cfg.Raft.LogCompression != "none" {
		t.Fatalf("raft log compression config not loaded: %+v", cfg.Raft)
	}
	if cfg.Raft.LogCompressionMinBytes != 2048 || cfg.Raft.LogCompressionMinSavingsRatio != 0.2 || cfg.Raft.LogCompressionSkipCooldown != 2*time.Second {
		t.Fatalf("raft log compression guardrail config not loaded: %+v", cfg.Raft)
	}
	if cfg.RaftIdentity.LeaseBackend != "kubernetes" || cfg.RaftIdentity.LeaseName != "cefasdb-geo-cefas-0" || cfg.RaftIdentity.LeaseNamespace != "cefasdb" {
		t.Fatalf("raft identity lease config not loaded: %+v", cfg.RaftIdentity)
	}
	if cfg.RaftIdentity.LeaseTTL != 45*time.Second || cfg.RaftIdentity.LeaseRenewInterval != 15*time.Second {
		t.Fatalf("raft identity lease durations not loaded: %+v", cfg.RaftIdentity)
	}
	if cfg.Metrics.HotspotBuckets != 16 || cfg.Metrics.HotspotWriteThreshold != 42 || cfg.Metrics.HotspotLatencyThreshold != 75*time.Millisecond {
		t.Fatalf("hotspot metrics config not loaded: %+v", cfg.Metrics)
	}
	if !cfg.Rebalancer.Enabled || cfg.Rebalancer.Mode != "manual" || cfg.Rebalancer.Interval != 10*time.Second || cfg.Rebalancer.ManualPlanDir != "/tmp/cefas-rebalance" {
		t.Fatalf("rebalancer config not loaded: %+v", cfg.Rebalancer)
	}
	if !cfg.BackupScheduler.Enabled || !cfg.BackupScheduler.DryRun || cfg.BackupScheduler.Interval != 15*time.Minute || cfg.BackupScheduler.NameTemplate != "nightly-{{date}}" {
		t.Fatalf("backup scheduler config not loaded: %+v", cfg.BackupScheduler)
	}
	if len(cfg.BackupScheduler.Tables) != 2 || cfg.BackupScheduler.Tables[0] != "Users" || cfg.BackupScheduler.Tables[1] != "Orders" {
		t.Fatalf("backup scheduler tables not loaded: %+v", cfg.BackupScheduler.Tables)
	}
	if cfg.BackupScheduler.Retention.KeepLatest != 7 || !cfg.BackupScheduler.Retention.KeepLatestSet || cfg.BackupScheduler.Retention.MaxAge != 720*time.Hour || !cfg.BackupScheduler.Retention.MaxAgeSet || !cfg.BackupScheduler.Retention.DryRun {
		t.Fatalf("backup scheduler retention not loaded: %+v", cfg.BackupScheduler.Retention)
	}
}

func TestApplyEnv(t *testing.T) {
	t.Setenv("CEFAS_HTTP_ADDR", ":19090")
	t.Setenv("CEFAS_LIFECYCLE_SHUTDOWN_GRACE_PERIOD", "21s")
	t.Setenv("CEFAS_LIFECYCLE_DRAIN_DELAY", "750ms")
	t.Setenv("CEFAS_LIFECYCLE_LEADERSHIP_TRANSFER_TIMEOUT", "6s")
	t.Setenv("CEFAS_CLUSTER_SHARDS", "4")
	t.Setenv("CEFAS_CLUSTER_REPLICATION_FACTOR", "3")
	t.Setenv("CEFAS_RAFT_HEARTBEAT_TIMEOUT", "2500ms")
	t.Setenv("CEFAS_RAFT_ELECTION_TIMEOUT", "11s")
	t.Setenv("CEFAS_RAFT_LEADER_LEASE_TIMEOUT", "1500ms")
	t.Setenv("CEFAS_RAFT_COMMIT_TIMEOUT", "120ms")
	t.Setenv("CEFAS_RAFT_APPLY_TIMEOUT", "40s")
	t.Setenv("CEFAS_RAFT_SNAPSHOT_ENTRIES", "262144")
	t.Setenv("CEFAS_RAFT_LOG_COMPRESSION", "none")
	t.Setenv("CEFAS_RAFT_LOG_COMPRESSION_MIN_BYTES", "4096")
	t.Setenv("CEFAS_RAFT_LOG_COMPRESSION_MIN_SAVINGS_RATIO", "0.25")
	t.Setenv("CEFAS_RAFT_LOG_COMPRESSION_SKIP_COOLDOWN", "3s")
	t.Setenv("CEFAS_RAFT_IDENTITY_LEASE_BACKEND", "kubernetes")
	t.Setenv("CEFAS_RAFT_IDENTITY_LEASE_NAME", "lease-n1")
	t.Setenv("CEFAS_RAFT_IDENTITY_LEASE_NAMESPACE", "cefasdb")
	t.Setenv("CEFAS_RAFT_IDENTITY_LEASE_TTL", "40s")
	t.Setenv("CEFAS_RAFT_IDENTITY_LEASE_RENEW_INTERVAL", "10s")
	t.Setenv("CEFAS_METRICS_ENABLED", "false")
	t.Setenv("CEFAS_METRICS_HOTSPOT_BUCKETS", "32")
	t.Setenv("CEFAS_METRICS_HOTSPOT_WRITE_THRESHOLD", "99")
	t.Setenv("CEFAS_METRICS_HOTSPOT_LATENCY_THRESHOLD", "25ms")
	t.Setenv("CEFAS_REBALANCER_ENABLED", "true")
	t.Setenv("CEFAS_REBALANCER_MODE", "auto")
	t.Setenv("CEFAS_REBALANCER_MAX_CONCURRENT_OPERATIONS", "2")
	t.Setenv("CEFAS_REBALANCER_MIN_INTERVAL", "30s")
	t.Setenv("CEFAS_IDENTITY_CLOCK_SKEW", "1m")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_ENABLED", "true")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_DRY_RUN", "true")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_INTERVAL", "5m")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_NAME_TEMPLATE", "hourly-{{unix}}")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_TABLES", "Users, Orders")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_RETENTION_KEEP_LATEST", "3")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_RETENTION_MAX_AGE", "168h")
	t.Setenv("CEFAS_BACKUP_SCHEDULER_RETENTION_DRY_RUN", "true")
	t.Setenv("CEFAS_STORAGE_CHANGELOG_MODE", "off")
	t.Setenv("CEFAS_STORAGE_STREAM_RETENTION_INTERVAL", "10m")
	t.Setenv("CEFAS_STORAGE_LANES", "on")
	t.Setenv("CEFAS_STORAGE_LANE_READ_WORKERS", "5")
	t.Setenv("CEFAS_STORAGE_LANE_WRITE_WORKERS", "4")
	t.Setenv("CEFAS_STORAGE_LANE_READ_QUEUE", "256")
	t.Setenv("CEFAS_STORAGE_LANE_WRITE_QUEUE", "128")

	cfg := config.Defaults()
	if err := config.ApplyEnv(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Addr != ":19090" {
		t.Errorf("http addr override: %q", cfg.HTTP.Addr)
	}
	if cfg.Lifecycle.ShutdownGracePeriod != 21*time.Second || cfg.Lifecycle.DrainDelay != 750*time.Millisecond || cfg.Lifecycle.LeadershipTransferTimeout != 6*time.Second {
		t.Errorf("lifecycle env not applied: %+v", cfg.Lifecycle)
	}
	if cfg.Cluster.Shards != 4 {
		t.Errorf("shards override: %d", cfg.Cluster.Shards)
	}
	if cfg.Cluster.ReplicationFactor != 3 {
		t.Errorf("replication factor override: %d", cfg.Cluster.ReplicationFactor)
	}
	if cfg.Raft.HeartbeatTimeout != 2500*time.Millisecond || cfg.Raft.ElectionTimeout != 11*time.Second || cfg.Raft.LeaderLeaseTimeout != 1500*time.Millisecond {
		t.Errorf("raft timeout env not applied: %+v", cfg.Raft)
	}
	if cfg.Raft.CommitTimeout != 120*time.Millisecond || cfg.Raft.ApplyTimeout != 40*time.Second {
		t.Errorf("raft commit/apply env not applied: %+v", cfg.Raft)
	}
	if cfg.Raft.SnapshotEntries != 262144 {
		t.Errorf("raft snapshot entries env not applied: %+v", cfg.Raft)
	}
	if cfg.Raft.LogCompression != "none" {
		t.Errorf("raft log compression env not applied: %+v", cfg.Raft)
	}
	if cfg.Raft.LogCompressionMinBytes != 4096 || cfg.Raft.LogCompressionMinSavingsRatio != 0.25 || cfg.Raft.LogCompressionSkipCooldown != 3*time.Second {
		t.Errorf("raft log compression guardrail env not applied: %+v", cfg.Raft)
	}
	if cfg.RaftIdentity.LeaseBackend != "kubernetes" || cfg.RaftIdentity.LeaseName != "lease-n1" || cfg.RaftIdentity.LeaseNamespace != "cefasdb" {
		t.Errorf("raft identity lease env not applied: %+v", cfg.RaftIdentity)
	}
	if cfg.RaftIdentity.LeaseTTL != 40*time.Second || cfg.RaftIdentity.LeaseRenewInterval != 10*time.Second {
		t.Errorf("raft identity lease duration env not applied: %+v", cfg.RaftIdentity)
	}
	if cfg.Storage.ChangeLogMode != "off" {
		t.Errorf("storage changelog mode env not applied: %+v", cfg.Storage)
	}
	if cfg.Storage.StreamRetentionInterval != 10*time.Minute {
		t.Errorf("storage stream retention interval env not applied: %+v", cfg.Storage)
	}
	if cfg.Storage.Lanes != "on" || cfg.Storage.LaneReadWorkers != 5 || cfg.Storage.LaneWriteWorkers != 4 || cfg.Storage.LaneReadQueue != 256 || cfg.Storage.LaneWriteQueue != 128 {
		t.Errorf("storage lanes env not applied: %+v", cfg.Storage)
	}
	if cfg.Metrics.Enabled {
		t.Errorf("metrics disable not applied")
	}
	if cfg.Metrics.HotspotBuckets != 32 || cfg.Metrics.HotspotWriteThreshold != 99 || cfg.Metrics.HotspotLatencyThreshold != 25*time.Millisecond {
		t.Errorf("hotspot env not applied: %+v", cfg.Metrics)
	}
	if !cfg.Rebalancer.Enabled || cfg.Rebalancer.Mode != "auto" || cfg.Rebalancer.MaxConcurrentOperations != 2 || cfg.Rebalancer.MinInterval != 30*time.Second {
		t.Errorf("rebalancer env not applied: %+v", cfg.Rebalancer)
	}
	if cfg.Identity.ClockSkew != time.Minute {
		t.Errorf("clock skew: %v", cfg.Identity.ClockSkew)
	}
	if !cfg.BackupScheduler.Enabled || !cfg.BackupScheduler.DryRun || cfg.BackupScheduler.Interval != 5*time.Minute || cfg.BackupScheduler.NameTemplate != "hourly-{{unix}}" {
		t.Errorf("backup scheduler env not applied: %+v", cfg.BackupScheduler)
	}
	if len(cfg.BackupScheduler.Tables) != 2 || cfg.BackupScheduler.Tables[0] != "Users" || cfg.BackupScheduler.Tables[1] != "Orders" {
		t.Errorf("backup scheduler tables env not applied: %+v", cfg.BackupScheduler.Tables)
	}
	if cfg.BackupScheduler.Retention.KeepLatest != 3 || !cfg.BackupScheduler.Retention.KeepLatestSet || cfg.BackupScheduler.Retention.MaxAge != 168*time.Hour || !cfg.BackupScheduler.Retention.MaxAgeSet || !cfg.BackupScheduler.Retention.DryRun {
		t.Errorf("backup scheduler retention env not applied: %+v", cfg.BackupScheduler.Retention)
	}
}

func TestParsePeers(t *testing.T) {
	good := []struct {
		in   string
		want map[string]string
	}{
		{"", map[string]string{}},
		{"n1=127.0.0.1:9100", map[string]string{"n1": "127.0.0.1:9100"}},
		{"n1=a:1,n2=b:2", map[string]string{"n1": "a:1", "n2": "b:2"}},
		{"  n1 = a:1 ,  n2 = b:2 ", map[string]string{"n1": "a:1", "n2": "b:2"}},
	}
	for _, g := range good {
		got, err := config.ParsePeers(g.in)
		if err != nil {
			t.Fatalf("%q: %v", g.in, err)
		}
		if len(got) != len(g.want) {
			t.Fatalf("%q: got %v, want %v", g.in, got, g.want)
		}
	}
	if _, err := config.ParsePeers("nope"); err == nil {
		t.Errorf("missing = should error")
	}
}
