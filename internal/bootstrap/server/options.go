// Package server hosts the cefas-server bootstrap helpers that lift
// configuration values into the concrete option structs consumed by
// storage, metrics, and rebalancer subsystems. The package exists so
// cmd/cefas-server/main.go can stay close to a thin wiring shell while
// the config-to-options translation stays unit-testable in isolation.
package server

import (
	"time"

	"github.com/osvaldoandrade/cefas/internal/metrics"
	"github.com/osvaldoandrade/cefas/internal/rebalance"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/config"
)

// StorageOptions assembles the storage.Options struct that storage.Open
// expects, threading the Pebble tuning, backpressure, and stream
// retention sub-configs alongside the on-disk path.
func StorageOptions(cfg config.Config, path string) storage.Options {
	return storage.Options{
		Path:            path,
		FsyncOnCommit:   cfg.Storage.FsyncOnCommit,
		Profile:         cfg.Storage.Profile,
		Tuning:          StorageTuning(cfg),
		Backpressure:    BackpressureOptions(cfg),
		StreamRetention: StreamRetentionOptions(cfg),
	}
}

// StorageTuning projects the Pebble tuning knobs that ship with the
// Storage stanza of the config into the storage.PebbleTuning shape.
func StorageTuning(cfg config.Config) storage.PebbleTuning {
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

// RangeHotspotConfig translates the Metrics hotspot stanza into the
// metrics.RangeHotspotConfig the Prometheus registry consumes when the
// hot-key detector is enabled.
func RangeHotspotConfig(cfg config.Config) metrics.RangeHotspotConfig {
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

// RebalancerConfig copies the Rebalancer stanza into the controller's
// internal Config form, including the duration-to-millis conversion the
// rebalancer expects for ApplyTimeout.
func RebalancerConfig(cfg config.Config) rebalance.Config {
	return rebalance.Config{
		Mode:                    rebalance.Mode(cfg.Rebalancer.Mode),
		Interval:                cfg.Rebalancer.Interval,
		MinInterval:             cfg.Rebalancer.MinInterval,
		MaxConcurrentOperations: cfg.Rebalancer.MaxConcurrentOperations,
		MaxHotspots:             cfg.Rebalancer.MaxHotspots,
		MinVoters:               cfg.Rebalancer.MinVoters,
		ApplyTimeoutMS:          int(cfg.Rebalancer.ApplyTimeout / time.Millisecond),
		ManualPlanDir:           cfg.Rebalancer.ManualPlanDir,
	}
}

// ScheduledBackupConfig builds the storage.ScheduledBackupConfig used by
// NewScheduledBackupRunner, injecting the Prometheus metrics handle and
// the host logger so the runner can report without reaching back into
// main.
func ScheduledBackupConfig(cfg config.Config, prom *metrics.Metrics, logger func(string, ...any)) storage.ScheduledBackupConfig {
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

// BackpressureOptions extracts the adaptive write-backpressure thresholds
// from the Storage stanza.
func BackpressureOptions(cfg config.Config) storage.BackpressureOptions {
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

// StreamRetentionOptions extracts the DynamoDB Streams retention knobs
// that bound the in-memory change feed.
func StreamRetentionOptions(cfg config.Config) storage.StreamRetentionOptions {
	return storage.StreamRetentionOptions{
		Retention: cfg.Storage.StreamRetention,
		MaxBytes:  cfg.Storage.StreamRetentionMaxBytes,
	}
}

// ParsePeers parses the "id1=addr1,id2=addr2" form used by both
// -raft-peers and -raft-http-peers. It is a thin wrapper over
// config.ParsePeers retained so the bootstrap package owns the surface
// area cmd/cefas-server depends on.
func ParsePeers(s string) (map[string]string, error) { return config.ParsePeers(s) }
