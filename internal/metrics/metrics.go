// Package metrics is the cefas Prometheus surface. It exposes a
// per-process Registry with counters and histograms for every public
// operation, plus engine-level gauges (raft commit lag, Pebble
// health and level stats). The HTTP server registers /metrics against this
// registry so a Prometheus scraper sees a stable schema across
// versions.
//
// Conventions:
//   - All metric names are prefixed "cefas_".
//   - Histograms use the seconds unit ("_seconds") with buckets sized
//     for the ops range we actually see (10us..10s).
//   - Labels are kept small — every label combination is a new time
//     series, so we only label by operation name, by table when the
//     operation is per-table, and by shard.
package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/CefasDb/cefasdb/internal/core/model"
	craft "github.com/CefasDb/cefasdb/internal/replication"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

// Metrics groups every cefas metric instance so the storage / API
// layers receive a single struct to call into instead of importing
// the Prometheus package directly.
type Metrics struct {
	Registry *prometheus.Registry

	OpDuration *prometheus.HistogramVec // op{put,get,query,...}, table
	OpCount    *prometheus.CounterVec   // op, table, outcome{ok,err,notfound,notleader}

	BatchSize    *prometheus.HistogramVec // op{put,batch_write}: ops per batch
	IndexFanout  *prometheus.HistogramVec // shard fan-out width (multi-shard writes)
	SpatialCells prometheus.Histogram     // cover prefix count per spatial query

	RaftCommitLagSeconds *prometheus.GaugeVec // shard
	RaftIsLeader         *prometheus.GaugeVec // shard
	RaftLogRawBytes      *prometheus.GaugeVec // shard
	RaftLogEncodedBytes  *prometheus.GaugeVec // shard
	RaftLogPayloads      *prometheus.GaugeVec // shard, result{compressed,raw,skipped}
	RaftLogSavingsRatio  *prometheus.GaugeVec // shard

	PebbleReadAmp               *prometheus.GaugeVec // shard
	PebbleCompactionDebtBytes   *prometheus.GaugeVec // shard
	PebbleCompactionsInProgress *prometheus.GaugeVec // shard
	PebbleCompactionCount       *prometheus.GaugeVec // shard
	PebbleFlushCount            *prometheus.GaugeVec // shard
	PebbleMemTableBytes         *prometheus.GaugeVec // shard
	PebbleWALBytes              *prometheus.GaugeVec // shard
	PebbleTableBytes            *prometheus.GaugeVec // shard
	PebbleL0Files               *prometheus.GaugeVec // shard
	PebbleLevelFiles            *prometheus.GaugeVec // shard, level
	PebbleLevelBytes            *prometheus.GaugeVec // shard, level
	PebbleLevelSublevels        *prometheus.GaugeVec // shard, level
	PebbleLevelScore            *prometheus.GaugeVec // shard, level

	BackpressureState  *prometheus.GaugeVec // shard
	BackpressureReason *prometheus.GaugeVec // shard, reason

	AuthRejectedTotal *prometheus.CounterVec // reason{missing_token,invalid_token,bad_scope}

	ScheduledBackupDuration *prometheus.HistogramVec // backup, outcome
	ScheduledBackupRows     *prometheus.GaugeVec     // backup
	ScheduledBackupBytes    *prometheus.GaugeVec     // backup
	ScheduledBackupRuns     *prometheus.CounterVec   // backup, outcome
	ScheduledBackupFailures *prometheus.CounterVec   // backup, reason

	RangeOpsTotal        *prometheus.CounterVec   // shard, bucket, op{read,write}
	RangeBytesTotal      *prometheus.CounterVec   // shard, bucket, op{read,write}
	RangeLatencySeconds  *prometheus.HistogramVec // shard, bucket, op{read,write}
	RangeCompactionDebt  *prometheus.GaugeVec     // shard, bucket
	RangeThrottleState   *prometheus.GaugeVec     // shard, bucket
	RangeHotspotRegistry *RangeHotspotTracker
	rangeHandleMu        sync.RWMutex
	rangeHandles         map[rangeMetricKey]rangeMetricHandles

	StreamRecordsAppended         *prometheus.GaugeVec   // shard, table
	StreamRecordsTrimmed          *prometheus.GaugeVec   // shard, table
	StreamRetainedBytes           *prometheus.GaugeVec   // shard, table
	StreamOldestSequence          *prometheus.GaugeVec   // shard, table
	StreamNewestSequence          *prometheus.GaugeVec   // shard, table
	StreamGetRecordsCalls         *prometheus.CounterVec // table, outcome
	StreamGetRecordsEmptyPolls    *prometheus.CounterVec // table
	StreamIteratorCreationFailure *prometheus.CounterVec // table, reason
	StreamTrimmedErrors           *prometheus.CounterVec // table, op
	StreamExpiredIteratorErrors   *prometheus.CounterVec // table, op
}

type rangeMetricKey struct {
	shard  string
	bucket string
	op     string
}

type rangeMetricHandles struct {
	ops     prometheus.Counter
	bytes   prometheus.Counter
	latency prometheus.Observer
}

// New builds a Metrics with every collector pre-registered against a
// fresh registry. Single-process pattern: one Metrics per cefasdb.
func New() *Metrics {
	return NewWithRangeHotspots(DefaultRangeHotspotConfig())
}

// NewWithRangeHotspots builds a Metrics instance with explicit
// hotspot bucketing and threshold configuration.
func NewWithRangeHotspots(hotspots RangeHotspotConfig) *Metrics {
	reg := prometheus.NewRegistry()
	// Standard process + go collectors so users get the usual
	// runtime + GC metrics for free.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		Registry: reg,
		OpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cefas_op_duration_seconds",
			Help:    "Latency of a single public operation.",
			Buckets: prometheus.ExponentialBuckets(0.00001, 4, 12), // 10us..~40s
		}, []string{"op", "table"}),
		OpCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_op_total",
			Help: "Total operations served, by outcome.",
		}, []string{"op", "table", "outcome"}),
		BatchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cefas_batch_ops",
			Help:    "Number of sub-operations packed into a single batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512},
		}, []string{"op"}),
		IndexFanout: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cefas_shard_fanout",
			Help:    "Number of shards touched by a single request.",
			Buckets: []float64{1, 2, 3, 4, 6, 8, 12, 16},
		}, []string{"op"}),
		SpatialCells: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cefas_spatial_cover_cells",
			Help:    "Cover prefix count returned for a spatial query.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 8), // 1..16k
		}),
		RaftCommitLagSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_raft_commit_lag_seconds",
			Help: "Seconds since the last raft commit was applied on this node.",
		}, []string{"shard"}),
		RaftIsLeader: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_raft_is_leader",
			Help: "1 if this node is the current leader of the shard, else 0.",
		}, []string{"shard"}),
		RaftLogRawBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_raft_log_raw_bytes",
			Help: "Cumulative unencoded raft log payload bytes submitted by this node.",
		}, []string{"shard"}),
		RaftLogEncodedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_raft_log_encoded_bytes",
			Help: "Cumulative raft log payload bytes after compression guardrails.",
		}, []string{"shard"}),
		RaftLogPayloads: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_raft_log_payloads",
			Help: "Cumulative raft log payload decisions by compression result.",
		}, []string{"shard", "result"}),
		RaftLogSavingsRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_raft_log_compression_savings_ratio",
			Help: "Cumulative raft log byte savings ratio from compression.",
		}, []string{"shard"}),
		PebbleReadAmp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_read_amp",
			Help: "Current Pebble read amplification estimate for the shard.",
		}, []string{"shard"}),
		PebbleCompactionDebtBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_compaction_debt_bytes",
			Help: "Estimated bytes that need compaction for the shard to reach steady state.",
		}, []string{"shard"}),
		PebbleCompactionsInProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_compactions_in_progress",
			Help: "Number of Pebble compactions currently in progress for the shard.",
		}, []string{"shard"}),
		PebbleCompactionCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_compaction_count",
			Help: "Cumulative Pebble compaction count since the shard was opened.",
		}, []string{"shard"}),
		PebbleFlushCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_flush_count",
			Help: "Cumulative Pebble flush count since the shard was opened.",
		}, []string{"shard"}),
		PebbleMemTableBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_memtable_bytes",
			Help: "Pebble memtable bytes allocated for the shard.",
		}, []string{"shard"}),
		PebbleWALBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_wal_bytes",
			Help: "Pebble live WAL bytes for the shard.",
		}, []string{"shard"}),
		PebbleTableBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_table_bytes",
			Help: "Pebble backing table bytes for the shard.",
		}, []string{"shard"}),
		PebbleL0Files: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_l0_files",
			Help: "Number of L0 SSTables in the shard's Pebble store.",
		}, []string{"shard"}),
		PebbleLevelFiles: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_level_files",
			Help: "Pebble SSTable count per level.",
		}, []string{"shard", "level"}),
		PebbleLevelBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_level_bytes",
			Help: "Pebble SSTable bytes per level.",
		}, []string{"shard", "level"}),
		PebbleLevelSublevels: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_level_sublevels",
			Help: "Pebble sublevel count per level.",
		}, []string{"shard", "level"}),
		PebbleLevelScore: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_level_score",
			Help: "Pebble compaction score per level.",
		}, []string{"shard", "level"}),
		BackpressureState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_backpressure_state",
			Help: "Write backpressure state for the shard: 0 normal, 1 warning, 2 critical.",
		}, []string{"shard"}),
		BackpressureReason: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_backpressure_active",
			Help: "1 when write backpressure is active for the shard and reason.",
		}, []string{"shard", "reason"}),
		AuthRejectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_auth_rejected_total",
			Help: "Authentication / authorisation failures by reason.",
		}, []string{"reason"}),
		ScheduledBackupDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cefas_scheduled_backup_duration_seconds",
			Help:    "Duration of scheduled backup runs.",
			Buckets: prometheus.ExponentialBuckets(0.01, 4, 10),
		}, []string{"backup", "outcome"}),
		ScheduledBackupRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_scheduled_backup_rows",
			Help: "Rows captured by the last scheduled backup run for this backup name.",
		}, []string{"backup"}),
		ScheduledBackupBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_scheduled_backup_bytes",
			Help: "Checkpoint bytes written by the last scheduled backup run for this backup name.",
		}, []string{"backup"}),
		ScheduledBackupRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_scheduled_backup_runs_total",
			Help: "Scheduled backup runs by backup name and outcome.",
		}, []string{"backup", "outcome"}),
		ScheduledBackupFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_scheduled_backup_failures_total",
			Help: "Scheduled backup failures by backup name and reason.",
		}, []string{"backup", "reason"}),
		RangeOpsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_range_ops_total",
			Help: "Total primary-key operations by shard and bounded token bucket.",
		}, []string{"shard", "bucket", "op"}),
		RangeBytesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_range_bytes_total",
			Help: "Approximate payload bytes by shard and bounded token bucket.",
		}, []string{"shard", "bucket", "op"}),
		RangeLatencySeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cefas_range_latency_seconds",
			Help:    "Primary-key operation latency by shard and bounded token bucket.",
			Buckets: prometheus.ExponentialBuckets(0.00001, 4, 12),
		}, []string{"shard", "bucket", "op"}),
		RangeCompactionDebt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_range_compaction_debt_bytes",
			Help: "Shard compaction debt projected onto bounded token buckets for hotspot summaries.",
		}, []string{"shard", "bucket"}),
		RangeThrottleState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_range_throttle_state",
			Help: "Shard write throttle state projected onto bounded token buckets: 0 normal, 1 warning, 2 critical.",
		}, []string{"shard", "bucket"}),
		RangeHotspotRegistry: NewRangeHotspotTracker(hotspots),
		rangeHandles:         make(map[rangeMetricKey]rangeMetricHandles),
		StreamRecordsAppended: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_stream_records_appended",
			Help: "Cumulative DynamoDB Streams records appended, reconstructed from persisted stream retention state.",
		}, []string{"shard", "table"}),
		StreamRecordsTrimmed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_stream_records_trimmed",
			Help: "Cumulative DynamoDB Streams records logically trimmed by retention.",
		}, []string{"shard", "table"}),
		StreamRetainedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_stream_retained_bytes",
			Help: "Approximate bytes retained for DynamoDB Streams after logical trimming.",
		}, []string{"shard", "table"}),
		StreamOldestSequence: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_stream_oldest_sequence",
			Help: "Oldest readable DynamoDB Streams sequence number for the table stream.",
		}, []string{"shard", "table"}),
		StreamNewestSequence: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_stream_newest_sequence",
			Help: "Newest appended DynamoDB Streams sequence number for the table stream.",
		}, []string{"shard", "table"}),
		StreamGetRecordsCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_stream_get_records_total",
			Help: "DynamoDB Streams GetRecords calls by outcome.",
		}, []string{"table", "outcome"}),
		StreamGetRecordsEmptyPolls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_stream_get_records_empty_polls_total",
			Help: "DynamoDB Streams GetRecords calls that returned no records while the shard stayed readable.",
		}, []string{"table"}),
		StreamIteratorCreationFailure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_stream_iterator_creation_failures_total",
			Help: "DynamoDB Streams GetShardIterator failures by reason.",
		}, []string{"table", "reason"}),
		StreamTrimmedErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_stream_trimmed_errors_total",
			Help: "DynamoDB Streams trimmed-data errors by operation.",
		}, []string{"table", "op"}),
		StreamExpiredIteratorErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_stream_expired_iterator_errors_total",
			Help: "DynamoDB Streams expired iterator errors by operation.",
		}, []string{"table", "op"}),
	}
	reg.MustRegister(
		m.OpDuration,
		m.OpCount,
		m.BatchSize,
		m.IndexFanout,
		m.SpatialCells,
		m.RaftCommitLagSeconds,
		m.RaftIsLeader,
		m.RaftLogRawBytes,
		m.RaftLogEncodedBytes,
		m.RaftLogPayloads,
		m.RaftLogSavingsRatio,
		m.PebbleReadAmp,
		m.PebbleCompactionDebtBytes,
		m.PebbleCompactionsInProgress,
		m.PebbleCompactionCount,
		m.PebbleFlushCount,
		m.PebbleMemTableBytes,
		m.PebbleWALBytes,
		m.PebbleTableBytes,
		m.PebbleL0Files,
		m.PebbleLevelFiles,
		m.PebbleLevelBytes,
		m.PebbleLevelSublevels,
		m.PebbleLevelScore,
		m.BackpressureState,
		m.BackpressureReason,
		m.AuthRejectedTotal,
		m.ScheduledBackupDuration,
		m.ScheduledBackupRows,
		m.ScheduledBackupBytes,
		m.ScheduledBackupRuns,
		m.ScheduledBackupFailures,
		m.RangeOpsTotal,
		m.RangeBytesTotal,
		m.RangeLatencySeconds,
		m.RangeCompactionDebt,
		m.RangeThrottleState,
		m.StreamRecordsAppended,
		m.StreamRecordsTrimmed,
		m.StreamRetainedBytes,
		m.StreamOldestSequence,
		m.StreamNewestSequence,
		m.StreamGetRecordsCalls,
		m.StreamGetRecordsEmptyPolls,
		m.StreamIteratorCreationFailure,
		m.StreamTrimmedErrors,
		m.StreamExpiredIteratorErrors,
	)
	return m
}

// Handler returns the HTTP handler that exposes the Prometheus
// registry. Mount this on /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// Observe records the duration and outcome of an op. Pass through
// from API handlers via defer.
func (m *Metrics) Observe(op, table, outcome string, durationSeconds float64) {
	if m == nil {
		return
	}
	m.OpDuration.WithLabelValues(op, table).Observe(durationSeconds)
	m.OpCount.WithLabelValues(op, table, outcome).Inc()
}

// AuthRejected increments the rejection counter for `reason`. No-op
// when m is nil.
func (m *Metrics) AuthRejected(reason string) {
	if m == nil {
		return
	}
	m.AuthRejectedTotal.WithLabelValues(reason).Inc()
}

func (m *Metrics) ObserveRaftLogCompression(shard string, stats craft.LogCompressionStats) {
	if m == nil {
		return
	}
	if shard == "" {
		shard = "0"
	}
	m.RaftLogRawBytes.WithLabelValues(shard).Set(float64(stats.RawBytes))
	m.RaftLogEncodedBytes.WithLabelValues(shard).Set(float64(stats.EncodedBytes))
	m.RaftLogPayloads.WithLabelValues(shard, "compressed").Set(float64(stats.CompressedPayloads))
	m.RaftLogPayloads.WithLabelValues(shard, "raw").Set(float64(stats.RawPayloads))
	m.RaftLogPayloads.WithLabelValues(shard, "skipped").Set(float64(stats.SkippedPayloads))
	savings := 0.0
	if stats.RawBytes > 0 && stats.EncodedBytes < stats.RawBytes {
		savings = 1 - float64(stats.EncodedBytes)/float64(stats.RawBytes)
	}
	m.RaftLogSavingsRatio.WithLabelValues(shard).Set(savings)
}

func (m *Metrics) ObserveStreamRetention(shard string, stats pebble.StreamRetentionStats) {
	if m == nil {
		return
	}
	if shard == "" {
		shard = "0"
	}
	table := stats.Table
	if table == "" {
		table = "unknown"
	}
	m.StreamRecordsAppended.WithLabelValues(shard, table).Set(float64(stats.RecordsAppended))
	m.StreamRecordsTrimmed.WithLabelValues(shard, table).Set(float64(stats.RecordsTrimmed))
	m.StreamRetainedBytes.WithLabelValues(shard, table).Set(float64(stats.RetainedBytes))
	m.StreamOldestSequence.WithLabelValues(shard, table).Set(float64(stats.OldestSequence))
	m.StreamNewestSequence.WithLabelValues(shard, table).Set(float64(stats.NewestSequence))
}

func (m *Metrics) ObserveStreamGetRecords(table, outcome string, empty bool) {
	if m == nil {
		return
	}
	if table == "" {
		table = "unknown"
	}
	if outcome == "" {
		outcome = "ok"
	}
	m.StreamGetRecordsCalls.WithLabelValues(table, outcome).Inc()
	if empty {
		m.StreamGetRecordsEmptyPolls.WithLabelValues(table).Inc()
	}
}

func (m *Metrics) ObserveStreamIteratorFailure(table, reason string) {
	if m == nil {
		return
	}
	if table == "" {
		table = "unknown"
	}
	if reason == "" {
		reason = "err"
	}
	m.StreamIteratorCreationFailure.WithLabelValues(table, reason).Inc()
}

func (m *Metrics) ObserveStreamTrimmedError(table, op string) {
	if m == nil {
		return
	}
	if table == "" {
		table = "unknown"
	}
	m.StreamTrimmedErrors.WithLabelValues(table, op).Inc()
}

func (m *Metrics) ObserveStreamExpiredIterator(table, op string) {
	if m == nil {
		return
	}
	if table == "" {
		table = "unknown"
	}
	m.StreamExpiredIteratorErrors.WithLabelValues(table, op).Inc()
}

func (m *Metrics) ObserveScheduledBackup(backupName, outcome, reason string, duration time.Duration, rows, bytes int64) {
	if m == nil {
		return
	}
	if reason == "" {
		reason = "none"
	}
	m.ScheduledBackupDuration.WithLabelValues(backupName, outcome).Observe(duration.Seconds())
	m.ScheduledBackupRows.WithLabelValues(backupName).Set(float64(rows))
	m.ScheduledBackupBytes.WithLabelValues(backupName).Set(float64(bytes))
	m.ScheduledBackupRuns.WithLabelValues(backupName, outcome).Inc()
	if outcome == "failed" {
		m.ScheduledBackupFailures.WithLabelValues(backupName, reason).Inc()
	}
}

// ObserveRangeOperation records bounded per-token-bucket read/write
// signals. `op` is intentionally limited by callers to read/write to
// keep Prometheus label cardinality stable.
//
// shardID is a model.ShardID value object (phase 5b); the underlying
// label form is shardID.String(), so "0" / "1" / "unrouted" emit
// byte-for-byte the same series the legacy string parameter
// produced.
func (m *Metrics) ObserveRangeOperation(shardID model.ShardID, token uint64, op string, bytes uint64, latency time.Duration) {
	if m == nil || m.RangeHotspotRegistry == nil {
		return
	}
	if op != "write" {
		op = "read"
	}
	bucket := m.RangeHotspotRegistry.BucketForToken(token)
	bucketLabel := bucketLabel(bucket)
	label := shardID.String()
	handles := m.rangeMetricHandles(label, bucketLabel, op)
	handles.ops.Inc()
	if bytes > 0 {
		handles.bytes.Add(float64(bytes))
	}
	if latency > 0 {
		handles.latency.Observe(latency.Seconds())
	}
	m.RangeHotspotRegistry.recordOperationBucket(label, bucket, op, bytes, latency)
}

func (m *Metrics) rangeMetricHandles(label, bucket, op string) rangeMetricHandles {
	key := rangeMetricKey{shard: label, bucket: bucket, op: op}
	m.rangeHandleMu.RLock()
	handles, ok := m.rangeHandles[key]
	m.rangeHandleMu.RUnlock()
	if ok {
		return handles
	}

	m.rangeHandleMu.Lock()
	defer m.rangeHandleMu.Unlock()
	if handles, ok = m.rangeHandles[key]; ok {
		return handles
	}
	handles = rangeMetricHandles{
		ops:     m.RangeOpsTotal.WithLabelValues(label, bucket, op),
		bytes:   m.RangeBytesTotal.WithLabelValues(label, bucket, op),
		latency: m.RangeLatencySeconds.WithLabelValues(label, bucket, op),
	}
	m.rangeHandles[key] = handles
	return handles
}

// ObserveRangePressure records shard-level storage pressure on every
// bounded bucket for that shard. Pebble exposes these signals per
// engine, not per token; projecting them keeps status summaries
// aligned with the bucketed traffic signal without unbounded labels.
func (m *Metrics) ObserveRangePressure(shardID string, compactionDebtBytes uint64, throttleState int) {
	if m == nil || m.RangeHotspotRegistry == nil {
		return
	}
	cfg := m.RangeHotspotRegistry.Config()
	for bucket := 0; bucket < cfg.Buckets; bucket++ {
		label := bucketLabel(bucket)
		m.RangeCompactionDebt.WithLabelValues(shardID, label).Set(float64(compactionDebtBytes))
		m.RangeThrottleState.WithLabelValues(shardID, label).Set(float64(throttleState))
	}
	m.RangeHotspotRegistry.RecordShardPressure(shardID, compactionDebtBytes, throttleState)
}

func (m *Metrics) RangeHotspotSummaries(max int) []RangeHotspotSummary {
	if m == nil || m.RangeHotspotRegistry == nil {
		return nil
	}
	return m.RangeHotspotRegistry.Snapshot(max)
}
