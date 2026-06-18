package server_test

import (
	"testing"
	"time"

	bootstrapserver "github.com/CefasDb/cefasdb/internal/bootstrap/server"
	"github.com/CefasDb/cefasdb/internal/config"
	"github.com/CefasDb/cefasdb/internal/rebalance"
)

// fixtureConfig returns a config.Config with non-zero values across
// every stanza the server bootstrap helpers read. Tests can mutate the
// returned value before asserting projections.
func fixtureConfig() config.Config {
	var c config.Config
	c.Storage.FsyncOnCommit = true
	c.Storage.Profile = "write-heavy"
	c.Storage.BlockCacheSizeBytes = 1 << 30
	c.Storage.MemTableSizeBytes = 128 << 20
	c.Storage.MemTableStopWritesThreshold = 4
	c.Storage.MaxConcurrentCompactions = 6
	c.Storage.L0CompactionConcurrency = 3
	c.Storage.L0CompactionThreshold = 8
	c.Storage.L0CompactionFileThreshold = 64
	c.Storage.L0StopWritesThreshold = 32
	c.Storage.BytesPerSync = 1 << 20
	c.Storage.WALBytesPerSync = 512 << 10

	c.Storage.BackpressureEnabled = true
	c.Storage.BackpressureRejectCritical = true
	c.Storage.BackpressureWarningL0Files = 12
	c.Storage.BackpressureCriticalL0Files = 24
	c.Storage.BackpressureWarningDebt = 1 << 28
	c.Storage.BackpressureCriticalDebt = 1 << 30
	c.Storage.BackpressureWarningReadAmp = 16
	c.Storage.BackpressureCriticalReadAmp = 32
	c.Storage.BackpressureWarningDelay = 5 * time.Millisecond
	c.Storage.BackpressureCriticalDelay = 50 * time.Millisecond

	c.Storage.StreamRetention = 12 * time.Hour
	c.Storage.StreamRetentionMaxBytes = 1 << 30
	c.Storage.ChangeLogMode = "streams-only"

	c.Metrics.HotspotBuckets = 32
	c.Metrics.HotspotWindow = 90 * time.Second
	c.Metrics.HotspotCoolingWindow = 5 * time.Minute
	c.Metrics.HotspotMaxSummaries = 4
	c.Metrics.HotspotReadThreshold = 9_000
	c.Metrics.HotspotWriteThreshold = 4_500
	c.Metrics.HotspotBytesThreshold = 32 << 20
	c.Metrics.HotspotLatencyThreshold = 250 * time.Millisecond
	c.Metrics.HotspotCompactionDebtThreshold = 2 << 30
	c.Metrics.HotspotThrottleStateThreshold = 2

	c.Rebalancer.Mode = "auto"
	c.Rebalancer.Interval = 45 * time.Second
	c.Rebalancer.MinInterval = 10 * time.Minute
	c.Rebalancer.MaxConcurrentOperations = 3
	c.Rebalancer.MaxHotspots = 12
	c.Rebalancer.MinVoters = 3
	c.Rebalancer.ApplyTimeout = 2_500 * time.Millisecond
	c.Rebalancer.ManualPlanDir = "/var/lib/cefas/plans"

	c.BackupScheduler.Enabled = true
	c.BackupScheduler.DryRun = false
	c.BackupScheduler.Interval = 6 * time.Hour
	c.BackupScheduler.NameTemplate = "snap-{{timestamp}}"
	c.BackupScheduler.Tables = []string{"orders", "users"}
	c.BackupScheduler.Retention.KeepLatest = 5
	c.BackupScheduler.Retention.KeepLatestSet = true
	c.BackupScheduler.Retention.MaxAge = 7 * 24 * time.Hour
	c.BackupScheduler.Retention.MaxAgeSet = true
	c.BackupScheduler.Retention.DryRun = true

	return c
}

func TestStorageOptions(t *testing.T) {
	cfg := fixtureConfig()
	opts := bootstrapserver.StorageOptions(cfg, "/data/cefas")

	if opts.Path != "/data/cefas" {
		t.Errorf("Path = %q, want /data/cefas", opts.Path)
	}
	if !opts.FsyncOnCommit {
		t.Error("FsyncOnCommit should be true")
	}
	if opts.Profile != "write-heavy" {
		t.Errorf("Profile = %q, want write-heavy", opts.Profile)
	}
	if opts.Tuning.BlockCacheSizeBytes != 1<<30 {
		t.Errorf("Tuning.BlockCacheSizeBytes = %d", opts.Tuning.BlockCacheSizeBytes)
	}
	if !opts.Backpressure.Enabled {
		t.Error("Backpressure.Enabled should mirror config")
	}
	if opts.StreamRetention.Retention != 12*time.Hour {
		t.Errorf("StreamRetention.Retention = %v", opts.StreamRetention.Retention)
	}
	if opts.ChangeLogMode != "streams-only" {
		t.Errorf("ChangeLogMode = %q", opts.ChangeLogMode)
	}
}

func TestStorageTuning(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*config.Config)
		want func(t *testing.T, cfg config.Config)
	}{
		{
			name: "all knobs propagate",
			mut:  func(c *config.Config) {},
			want: func(t *testing.T, cfg config.Config) {
				got := bootstrapserver.StorageTuning(cfg)
				if got.MemTableSizeBytes != cfg.Storage.MemTableSizeBytes {
					t.Errorf("MemTableSizeBytes = %d, want %d", got.MemTableSizeBytes, cfg.Storage.MemTableSizeBytes)
				}
				if got.MemTableStopWrites != cfg.Storage.MemTableStopWritesThreshold {
					t.Errorf("MemTableStopWrites = %d, want %d", got.MemTableStopWrites, cfg.Storage.MemTableStopWritesThreshold)
				}
				if got.MaxConcurrentCompactions != cfg.Storage.MaxConcurrentCompactions {
					t.Errorf("MaxConcurrentCompactions = %d", got.MaxConcurrentCompactions)
				}
				if got.L0CompactionConcurrency != cfg.Storage.L0CompactionConcurrency {
					t.Errorf("L0CompactionConcurrency = %d", got.L0CompactionConcurrency)
				}
				if got.L0CompactionThreshold != cfg.Storage.L0CompactionThreshold {
					t.Errorf("L0CompactionThreshold = %d", got.L0CompactionThreshold)
				}
				if got.L0CompactionFileThreshold != cfg.Storage.L0CompactionFileThreshold {
					t.Errorf("L0CompactionFileThreshold = %d", got.L0CompactionFileThreshold)
				}
				if got.L0StopWritesThreshold != cfg.Storage.L0StopWritesThreshold {
					t.Errorf("L0StopWritesThreshold = %d", got.L0StopWritesThreshold)
				}
				if got.BytesPerSync != cfg.Storage.BytesPerSync {
					t.Errorf("BytesPerSync = %d", got.BytesPerSync)
				}
				if got.WALBytesPerSync != cfg.Storage.WALBytesPerSync {
					t.Errorf("WALBytesPerSync = %d", got.WALBytesPerSync)
				}
			},
		},
		{
			name: "zero config yields zero tuning",
			mut: func(c *config.Config) {
				*c = config.Config{}
			},
			want: func(t *testing.T, cfg config.Config) {
				got := bootstrapserver.StorageTuning(cfg)
				if got.BlockCacheSizeBytes != 0 || got.WALBytesPerSync != 0 {
					t.Errorf("expected zero tuning, got %+v", got)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := fixtureConfig()
			tc.mut(&cfg)
			tc.want(t, cfg)
		})
	}
}

func TestRangeHotspotConfig(t *testing.T) {
	cfg := fixtureConfig()
	got := bootstrapserver.RangeHotspotConfig(cfg)

	if got.Buckets != 32 {
		t.Errorf("Buckets = %d, want 32", got.Buckets)
	}
	if got.Window != 90*time.Second {
		t.Errorf("Window = %v", got.Window)
	}
	if got.CoolingWindow != 5*time.Minute {
		t.Errorf("CoolingWindow = %v", got.CoolingWindow)
	}
	if got.ReadThreshold != 9_000 || got.WriteThreshold != 4_500 {
		t.Errorf("Read/Write thresholds wrong: %+v", got)
	}
	if got.BytesThreshold != 32<<20 {
		t.Errorf("BytesThreshold = %d", got.BytesThreshold)
	}
	if got.LatencyThresholdSeconds != 0.25 {
		t.Errorf("LatencyThresholdSeconds = %v, want 0.25", got.LatencyThresholdSeconds)
	}
	if got.CompactionDebtThresholdBytes != 2<<30 {
		t.Errorf("CompactionDebtThresholdBytes = %d", got.CompactionDebtThresholdBytes)
	}
	if got.ThrottleStateThreshold != 2 {
		t.Errorf("ThrottleStateThreshold = %d", got.ThrottleStateThreshold)
	}
	if got.MaxSummaries != 4 {
		t.Errorf("MaxSummaries = %d", got.MaxSummaries)
	}
}

func TestRebalancerConfig(t *testing.T) {
	cfg := fixtureConfig()
	got := bootstrapserver.RebalancerConfig(cfg)

	if got.Mode != rebalance.ModeAuto {
		t.Errorf("Mode = %q, want auto", got.Mode)
	}
	if got.Interval != 45*time.Second {
		t.Errorf("Interval = %v", got.Interval)
	}
	if got.MinInterval != 10*time.Minute {
		t.Errorf("MinInterval = %v", got.MinInterval)
	}
	if got.MaxConcurrentOperations != 3 {
		t.Errorf("MaxConcurrentOperations = %d", got.MaxConcurrentOperations)
	}
	if got.MaxHotspots != 12 {
		t.Errorf("MaxHotspots = %d", got.MaxHotspots)
	}
	if got.MinVoters != 3 {
		t.Errorf("MinVoters = %d", got.MinVoters)
	}
	if got.ApplyTimeoutMS != 2500 {
		t.Errorf("ApplyTimeoutMS = %d, want 2500", got.ApplyTimeoutMS)
	}
	if got.ManualPlanDir != "/var/lib/cefas/plans" {
		t.Errorf("ManualPlanDir = %q", got.ManualPlanDir)
	}
}

func TestScheduledBackupConfig(t *testing.T) {
	cfg := fixtureConfig()
	called := 0
	logger := func(string, ...any) { called++ }

	got := bootstrapserver.ScheduledBackupConfig(cfg, nil, logger)

	if !got.Enabled || got.DryRun {
		t.Errorf("Enabled/DryRun wrong: %+v", got)
	}
	if got.Interval != 6*time.Hour {
		t.Errorf("Interval = %v", got.Interval)
	}
	if got.NameTemplate != "snap-{{timestamp}}" {
		t.Errorf("NameTemplate = %q", got.NameTemplate)
	}
	if len(got.Tables) != 2 || got.Tables[0] != "orders" || got.Tables[1] != "users" {
		t.Errorf("Tables = %v", got.Tables)
	}
	// Mutating the returned slice must not mutate the source config —
	// the helper is expected to copy the slice.
	got.Tables[0] = "mutated"
	if cfg.BackupScheduler.Tables[0] != "orders" {
		t.Errorf("source Tables mutated through returned copy")
	}
	if got.Retention.KeepLatest != 5 || !got.Retention.KeepLatestSet {
		t.Errorf("Retention.KeepLatest projection wrong: %+v", got.Retention)
	}
	if got.Retention.MaxAge != 7*24*time.Hour || !got.Retention.MaxAgeSet {
		t.Errorf("Retention.MaxAge projection wrong: %+v", got.Retention)
	}
	if !got.Retention.DryRun {
		t.Error("Retention.DryRun should be true")
	}
	if got.Logger == nil {
		t.Fatal("Logger missing")
	}
	got.Logger("hi")
	if called != 1 {
		t.Errorf("Logger not threaded through, calls = %d", called)
	}
}

func TestBackpressureOptions(t *testing.T) {
	cfg := fixtureConfig()
	got := bootstrapserver.BackpressureOptions(cfg)

	if !got.Enabled || !got.RejectOnCritical {
		t.Errorf("Enabled/RejectOnCritical wrong: %+v", got)
	}
	if got.WarningL0Files != 12 || got.CriticalL0Files != 24 {
		t.Errorf("L0 thresholds wrong: %+v", got)
	}
	if got.WarningCompactionDebtBytes != 1<<28 || got.CriticalCompactionDebtBytes != 1<<30 {
		t.Errorf("compaction debt thresholds wrong: %+v", got)
	}
	if got.WarningReadAmp != 16 || got.CriticalReadAmp != 32 {
		t.Errorf("read-amp thresholds wrong: %+v", got)
	}
	if got.WarningDelay != 5*time.Millisecond || got.CriticalDelay != 50*time.Millisecond {
		t.Errorf("delays wrong: %+v", got)
	}
}

func TestStreamRetentionOptions(t *testing.T) {
	cfg := fixtureConfig()
	got := bootstrapserver.StreamRetentionOptions(cfg)

	if got.Retention != 12*time.Hour {
		t.Errorf("Retention = %v", got.Retention)
	}
	if got.MaxBytes != 1<<30 {
		t.Errorf("MaxBytes = %d", got.MaxBytes)
	}
}

func TestParsePeers(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    map[string]string
		wantErr bool
	}{
		{
			name: "two peers",
			in:   "a=127.0.0.1:9001,b=127.0.0.1:9002",
			want: map[string]string{"a": "127.0.0.1:9001", "b": "127.0.0.1:9002"},
		},
		{
			name: "empty input yields empty map or nil",
			in:   "",
			want: nil,
		},
		{
			name:    "missing equals errors",
			in:      "no-equals",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := bootstrapserver.ParsePeers(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tc.want) != len(got) {
				t.Fatalf("len mismatch: got=%v want=%v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("got[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}
