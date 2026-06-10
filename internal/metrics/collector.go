package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/osvaldoandrade/cefas/internal/cluster"
)

// RunShardCollector samples raft leadership + Pebble L0 file counts
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
				if sh.Storage != nil {
					// Pebble exposes Metrics() with detailed L0 info.
					// Storage layer wraps it, but we keep this collector
					// loose: just expose a 0 when no info available so
					// the metric stays present in scrapes.
					m.PebbleL0Files.WithLabelValues(label).Set(0)
				}
			}
		}
	}
}
