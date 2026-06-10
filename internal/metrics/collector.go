package metrics

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/storage"
)

type LeaderGate interface {
	IsLeader() bool
}

// RunShardCollector samples raft leadership + Pebble engine metrics
// for every shard managed by `mgr` and emits them as Prometheus
// gauges. Runs until ctx is cancelled.
//
// Polling beats wiring callbacks into pebble/hashicorp-raft because
// it stays decoupled from those libraries' internal hooks. Five
// seconds is a fine cadence — the gauges feed dashboards, not
// alerting on transient blips.
func RunShardCollector(ctx context.Context, m *Metrics, mgr *cluster.Manager, interval time.Duration) {
	if m == nil || mgr == nil {
		return
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, sh := range mgr.Shards() {
				label := fmt.Sprintf("%d", sh.ID)
				if sh.Raft != nil {
					leader := 0.0
					if sh.Raft.IsLeader() {
						leader = 1.0
					}
					m.RaftIsLeader.WithLabelValues(label).Set(leader)
				}
				collectPebble(m, label, sh.Storage)
				collectBackpressure(m, label, sh.Storage)
				if sh.RaftStorage != nil {
					collectPebble(m, label+":raft", sh.RaftStorage)
					collectBackpressure(m, label+":raft", sh.RaftStorage)
				}
			}
		}
	}
}

// RunStorageCollector samples one storage engine. It is used by the
// single-shard server path where there is no cluster.Manager.
func RunStorageCollector(ctx context.Context, m *Metrics, label string, st *storage.DB, leader LeaderGate, interval time.Duration) {
	if m == nil || st == nil {
		return
	}
	if label == "" {
		label = "0"
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if leader != nil {
				v := 0.0
				if leader.IsLeader() {
					v = 1.0
				}
				m.RaftIsLeader.WithLabelValues(label).Set(v)
			}
			collectPebble(m, label, st)
			collectBackpressure(m, label, st)
		}
	}
}

func collectPebble(m *Metrics, label string, st *storage.DB) {
	if m == nil || st == nil {
		return
	}
	pm := st.Metrics()
	m.PebbleReadAmp.WithLabelValues(label).Set(float64(pm.ReadAmp()))
	m.PebbleCompactionDebtBytes.WithLabelValues(label).Set(float64(pm.Compact.EstimatedDebt))
	m.PebbleCompactionsInProgress.WithLabelValues(label).Set(float64(pm.Compact.NumInProgress))
	m.PebbleCompactionCount.WithLabelValues(label).Set(float64(pm.Compact.Count))
	m.PebbleFlushCount.WithLabelValues(label).Set(float64(pm.Flush.Count))
	m.PebbleMemTableBytes.WithLabelValues(label).Set(float64(pm.MemTable.Size))
	m.PebbleWALBytes.WithLabelValues(label).Set(float64(pm.WAL.Size))
	m.PebbleTableBytes.WithLabelValues(label).Set(float64(pm.Table.BackingTableSize))

	for level, lm := range pm.Levels {
		levelLabel := strconv.Itoa(level)
		m.PebbleLevelFiles.WithLabelValues(label, levelLabel).Set(float64(lm.NumFiles))
		m.PebbleLevelBytes.WithLabelValues(label, levelLabel).Set(float64(lm.Size))
		m.PebbleLevelSublevels.WithLabelValues(label, levelLabel).Set(float64(lm.Sublevels))
		if !math.IsNaN(lm.Score) {
			m.PebbleLevelScore.WithLabelValues(label, levelLabel).Set(lm.Score)
		}
		if level == 0 {
			m.PebbleL0Files.WithLabelValues(label).Set(float64(lm.NumFiles))
		}
	}
}

func collectBackpressure(m *Metrics, label string, st *storage.DB) {
	if m == nil || st == nil {
		return
	}
	snap := st.WritePressure()
	m.BackpressureState.WithLabelValues(label).Set(float64(snap.State))
	for _, reason := range storage.BackpressureReasons() {
		active := 0.0
		if snap.Enabled && snap.State != storage.PressureNormal && snap.Reason == reason {
			active = 1.0
		}
		m.BackpressureReason.WithLabelValues(label, reason).Set(active)
	}
}
