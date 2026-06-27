package pebble

import (
	"strings"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

const (
	ProfileDefault    = "default"
	ProfileBalanced   = "balanced"
	ProfileWriteHeavy = "write-heavy"
	ProfileRaft       = "raft"

	LaneModeAuto = "auto"
	LaneModeOn   = "on"
	LaneModeOff  = "off"

	ChangeLogModeAlways      = "always"
	ChangeLogModeStreamsOnly = "streams-only"
	ChangeLogModeOff         = "off"
)

// PebbleTuning contains explicit Pebble knobs used by benchmark and
// production profiles. Zero values leave Pebble's option default in place.
type PebbleTuning struct {
	BlockCacheSizeBytes       int64
	MemTableSizeBytes         uint64
	MemTableStopWrites        int
	MaxConcurrentCompactions  int
	L0CompactionConcurrency   int
	L0CompactionThreshold     int
	L0CompactionFileThreshold int
	L0StopWritesThreshold     int
	BytesPerSync              int
	WALBytesPerSync           int
}

// LaneOptions configures the read/write worker lanes that sit above Pebble.
// They are intended for multi-shard data stores where caller goroutines should
// not all enter Pebble's read and write paths directly. Mode "auto" is resolved
// by the bootstrap layer: data shards enable it, raft metadata stores do not.
type LaneOptions struct {
	Mode         string
	ReadWorkers  int
	WriteWorkers int
	ReadQueue    int
	WriteQueue   int
}

// BackpressureOptions throttles caller-facing writes before the LSM reaches
// the point where read p99 collapses. It is disabled by default.
type BackpressureOptions struct {
	Enabled                     bool
	WarningL0Files              int64
	CriticalL0Files             int64
	WarningCompactionDebtBytes  uint64
	CriticalCompactionDebtBytes uint64
	WarningReadAmp              int
	CriticalReadAmp             int
	WarningDelay                time.Duration
	CriticalDelay               time.Duration
	RejectOnCritical            bool
}

const (
	DefaultStreamRetention             = 24 * time.Hour
	DefaultStreamRetentionLoopInterval = 30 * time.Second
	DefaultStreamRetentionCleanupBatch = 65536
)

// StreamRetentionOptions controls DynamoDB Streams / CDC retention.
// Retention defaults to 24h for DynamoDB parity. Expiration is physical and
// lazy: each changelog entry receives a time-ordered expiration pointer on
// append, and the background cleaner deletes only expired pointers in bounded
// batches.
//
// Interval controls the cleaner cadence. Positive enables the cleaner,
// negative disables it, and zero inherits the default. MaxBytes is retained for
// config compatibility; the physical cleaner is time-window based.
type StreamRetentionOptions struct {
	Retention time.Duration
	MaxBytes  int64
	Interval  time.Duration
	BatchSize int
}

func normalizeStreamRetentionOptions(o StreamRetentionOptions) StreamRetentionOptions {
	if o.Retention <= 0 {
		o.Retention = DefaultStreamRetention
	}
	if o.MaxBytes < 0 {
		o.MaxBytes = 0
	}
	if o.Interval == 0 {
		o.Interval = DefaultStreamRetentionLoopInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultStreamRetentionCleanupBatch
	}
	return o
}

func normalizeProfile(profile string) string {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "", ProfileDefault:
		return ProfileDefault
	case ProfileBalanced:
		return ProfileBalanced
	case ProfileWriteHeavy, "writeheavy", "write_heavy":
		return ProfileWriteHeavy
	case ProfileRaft:
		return ProfileRaft
	default:
		return ProfileDefault
	}
}

// normalizeChangeLogMode resolves the wire string into the canonical
// mode. Empty input maps to "streams-only" — only tables with stream enabled
// pay the per-mutation changelog write. Operators who need retention-bound
// PITR over every table opt in explicitly with "always".
func normalizeChangeLogMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", ChangeLogModeStreamsOnly, "streams", "stream":
		return ChangeLogModeStreamsOnly
	case ChangeLogModeAlways:
		return ChangeLogModeAlways
	case ChangeLogModeOff, "disabled", "none":
		return ChangeLogModeOff
	default:
		return ChangeLogModeStreamsOnly
	}
}

func NormalizeLaneMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", LaneModeAuto:
		return LaneModeAuto
	case LaneModeOn, "enabled", "true", "1":
		return LaneModeOn
	case LaneModeOff, "disabled", "false", "0":
		return LaneModeOff
	default:
		return LaneModeAuto
	}
}

func DataLaneOptions(opts LaneOptions) LaneOptions {
	if NormalizeLaneMode(opts.Mode) == LaneModeAuto {
		opts.Mode = LaneModeOn
	}
	return opts
}

func MetadataLaneOptions(opts LaneOptions) LaneOptions {
	if NormalizeLaneMode(opts.Mode) == LaneModeAuto {
		opts.Mode = LaneModeOff
	}
	return opts
}

func profileTuning(profile string) PebbleTuning {
	switch normalizeProfile(profile) {
	case ProfileBalanced:
		return PebbleTuning{
			BlockCacheSizeBytes:       512 << 20,
			MemTableSizeBytes:         64 << 20,
			MemTableStopWrites:        4,
			MaxConcurrentCompactions:  4,
			L0CompactionConcurrency:   6,
			L0CompactionThreshold:     4,
			L0CompactionFileThreshold: 128,
			L0StopWritesThreshold:     128,
			BytesPerSync:              1 << 20,
			WALBytesPerSync:           1 << 20,
		}
	case ProfileWriteHeavy:
		return PebbleTuning{
			BlockCacheSizeBytes:       1 << 30,
			MemTableSizeBytes:         128 << 20,
			MemTableStopWrites:        6,
			MaxConcurrentCompactions:  6,
			L0CompactionConcurrency:   6,
			L0CompactionThreshold:     4,
			L0CompactionFileThreshold: 96,
			L0StopWritesThreshold:     192,
			BytesPerSync:              1 << 20,
			WALBytesPerSync:           1 << 20,
		}
	case ProfileRaft:
		return PebbleTuning{
			BlockCacheSizeBytes:       64 << 20,
			MemTableSizeBytes:         32 << 20,
			MemTableStopWrites:        4,
			MaxConcurrentCompactions:  4,
			L0CompactionConcurrency:   4,
			L0CompactionThreshold:     4,
			L0CompactionFileThreshold: 128,
			L0StopWritesThreshold:     128,
			BytesPerSync:              512 << 10,
			WALBytesPerSync:           512 << 10,
		}
	default:
		return PebbleTuning{
			BlockCacheSizeBytes: 256 << 20,
		}
	}
}

func mergeTuning(base, override PebbleTuning) PebbleTuning {
	if override.BlockCacheSizeBytes > 0 {
		base.BlockCacheSizeBytes = override.BlockCacheSizeBytes
	}
	if override.MemTableSizeBytes > 0 {
		base.MemTableSizeBytes = override.MemTableSizeBytes
	}
	if override.MemTableStopWrites > 0 {
		base.MemTableStopWrites = override.MemTableStopWrites
	}
	if override.MaxConcurrentCompactions > 0 {
		base.MaxConcurrentCompactions = override.MaxConcurrentCompactions
	}
	if override.L0CompactionConcurrency > 0 {
		base.L0CompactionConcurrency = override.L0CompactionConcurrency
	}
	if override.L0CompactionThreshold > 0 {
		base.L0CompactionThreshold = override.L0CompactionThreshold
	}
	if override.L0CompactionFileThreshold > 0 {
		base.L0CompactionFileThreshold = override.L0CompactionFileThreshold
	}
	if override.L0StopWritesThreshold > 0 {
		base.L0StopWritesThreshold = override.L0StopWritesThreshold
	}
	if override.BytesPerSync > 0 {
		base.BytesPerSync = override.BytesPerSync
	}
	if override.WALBytesPerSync > 0 {
		base.WALBytesPerSync = override.WALBytesPerSync
	}
	return base
}

func newPebbleOptions(opts Options) *pebbledb.Options {
	tuning := mergeTuning(profileTuning(opts.Profile), opts.Tuning)
	pOpts := &pebbledb.Options{}
	if tuning.BlockCacheSizeBytes > 0 {
		pOpts.Cache = pebbledb.NewCache(tuning.BlockCacheSizeBytes)
	}
	if tuning.MemTableSizeBytes > 0 {
		pOpts.MemTableSize = tuning.MemTableSizeBytes
	}
	if tuning.MemTableStopWrites > 0 {
		pOpts.MemTableStopWritesThreshold = tuning.MemTableStopWrites
	}
	if tuning.MaxConcurrentCompactions > 0 {
		n := tuning.MaxConcurrentCompactions
		pOpts.MaxConcurrentCompactions = func() int { return n }
	}
	if tuning.L0CompactionConcurrency > 0 {
		pOpts.Experimental.L0CompactionConcurrency = tuning.L0CompactionConcurrency
	}
	if tuning.L0CompactionThreshold > 0 {
		pOpts.L0CompactionThreshold = tuning.L0CompactionThreshold
	}
	if tuning.L0CompactionFileThreshold > 0 {
		pOpts.L0CompactionFileThreshold = tuning.L0CompactionFileThreshold
	}
	if tuning.L0StopWritesThreshold > 0 {
		pOpts.L0StopWritesThreshold = tuning.L0StopWritesThreshold
	}
	if tuning.BytesPerSync > 0 {
		pOpts.BytesPerSync = tuning.BytesPerSync
	}
	if tuning.WALBytesPerSync > 0 {
		pOpts.WALBytesPerSync = tuning.WALBytesPerSync
	}
	for i := range pOpts.Levels {
		pOpts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
		pOpts.Levels[i].FilterType = pebbledb.TableFilter
	}
	return pOpts
}

func normalizeBackpressureOptions(o BackpressureOptions) BackpressureOptions {
	if !o.Enabled {
		return o
	}
	if o.WarningL0Files <= 0 {
		o.WarningL0Files = 64
	}
	if o.CriticalL0Files <= 0 {
		o.CriticalL0Files = 128
	}
	if o.WarningCompactionDebtBytes == 0 {
		o.WarningCompactionDebtBytes = 256 << 20
	}
	if o.CriticalCompactionDebtBytes == 0 {
		o.CriticalCompactionDebtBytes = 768 << 20
	}
	if o.WarningReadAmp <= 0 {
		o.WarningReadAmp = 24
	}
	if o.CriticalReadAmp <= 0 {
		o.CriticalReadAmp = 48
	}
	if o.WarningDelay <= 0 {
		o.WarningDelay = 2 * time.Millisecond
	}
	if o.CriticalDelay <= 0 {
		o.CriticalDelay = 25 * time.Millisecond
	}
	return o
}
