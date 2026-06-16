package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

func TestRangeHotspotBucketForTokenIsBoundedAndDeterministic(t *testing.T) {
	tracker := NewRangeHotspotTracker(RangeHotspotConfig{Buckets: 4})
	cases := []struct {
		token uint64
		want  int
	}{
		{token: 0, want: 0},
		{token: 1 << 62, want: 1},
		{token: 1 << 63, want: 2},
		{token: ^uint64(0), want: 3},
	}
	for _, tc := range cases {
		if got := tracker.BucketForToken(tc.token); got != tc.want {
			t.Fatalf("BucketForToken(%d) = %d, want %d", tc.token, got, tc.want)
		}
	}

	start, end := bucketBounds(1, 4)
	if start != 1<<62 || end != 1<<63 {
		t.Fatalf("bucket bounds = [%d,%d), want [%d,%d)", start, end, uint64(1<<62), uint64(1<<63))
	}
}

func TestRangeHotspotSummaryMarksHotAndCooling(t *testing.T) {
	tracker := NewRangeHotspotTracker(RangeHotspotConfig{
		Buckets:                 4,
		Window:                  time.Minute,
		CoolingWindow:           time.Minute,
		MaxSummaries:            4,
		WriteThreshold:          2,
		ReadThreshold:           100,
		BytesThreshold:          1 << 20,
		LatencyThresholdSeconds: 1,
	})

	tracker.RecordOperation(model.MustShardID(0), 1<<63, "write", 1, time.Millisecond)
	tracker.RecordOperation(model.MustShardID(0), 1<<63, "write", 1, time.Millisecond)
	snap := tracker.Snapshot(1)
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snap))
	}
	if snap[0].ShardID != "0" || snap[0].Bucket != 2 || snap[0].Status != "hot" {
		t.Fatalf("summary = %+v, want shard 0 bucket 2 hot", snap[0])
	}
	if !containsReason(snap[0].Reasons, "write_threshold") {
		t.Fatalf("reasons = %v, want write_threshold", snap[0].Reasons)
	}

	key := rangeBucketKey{shard: "0", bucket: 2}
	tracker.mu.Lock()
	stats := tracker.buckets[key]
	stats.writes = 0
	stats.reasons = nil
	tracker.mu.Unlock()

	snap = tracker.Snapshot(1)
	if len(snap) != 1 || snap[0].Status != "cooling" || !containsReason(snap[0].Reasons, "cooling_window") {
		t.Fatalf("cooling summary = %+v", snap)
	}
}

func TestMetricsExposeRangeSeries(t *testing.T) {
	m := NewWithRangeHotspots(RangeHotspotConfig{
		Buckets:                 4,
		WriteThreshold:          1,
		ReadThreshold:           100,
		BytesThreshold:          1 << 20,
		LatencyThresholdSeconds: 1,
	})
	m.ObserveRangeOperation(model.MustShardID(2), 1<<63, "write", 42, 2*time.Millisecond)
	m.ObserveRangePressure("2", 512, 1)

	body := scrapeMetrics(t, m)
	for _, want := range []string{
		`cefas_range_ops_total{bucket="2",op="write",shard="2"} 1`,
		`cefas_range_bytes_total{bucket="2",op="write",shard="2"} 42`,
		`cefas_range_latency_seconds_bucket{bucket="2",op="write",shard="2"`,
		`cefas_range_compaction_debt_bytes{bucket="2",shard="2"} 512`,
		`cefas_range_throttle_state{bucket="2",shard="2"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q\n--- got ---\n%s", want, body)
		}
	}
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
