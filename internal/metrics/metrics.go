// Package metrics is the cefas Prometheus surface. It exposes a
// per-process Registry with counters and histograms for every public
// operation, plus a few engine-level gauges (raft commit lag, Pebble
// L0 file count). The HTTP server registers /metrics against this
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	PebbleL0Files        *prometheus.GaugeVec // shard

	AuthRejectedTotal *prometheus.CounterVec // reason{missing_token,invalid_token,bad_scope}
}

// New builds a Metrics with every collector pre-registered against a
// fresh registry. Single-process pattern: one Metrics per cefas-server.
func New() *Metrics {
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
		PebbleL0Files: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cefas_pebble_l0_files",
			Help: "Number of L0 SSTables in the shard's Pebble store.",
		}, []string{"shard"}),
		AuthRejectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cefas_auth_rejected_total",
			Help: "Authentication / authorisation failures by reason.",
		}, []string{"reason"}),
	}
	reg.MustRegister(
		m.OpDuration,
		m.OpCount,
		m.BatchSize,
		m.IndexFanout,
		m.SpatialCells,
		m.RaftCommitLagSeconds,
		m.RaftIsLeader,
		m.PebbleL0Files,
		m.AuthRejectedTotal,
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
