package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/testutil/wait"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestObserveEngineEventsIsMonotonicAcrossStoreReset(t *testing.T) {
	m := New()
	m.ObserveEngineEvents("0", pebble.EngineEventStats{WriteStalls: 2, WriteStallSeconds: 1.5, WriteStallActive: true, DiskSlowEvents: 1})
	m.ObserveEngineEvents("0", pebble.EngineEventStats{WriteStalls: 3, WriteStallSeconds: 2.5, DiskSlowEvents: 1})
	// A replaced store restarts its totals; exporter counters keep growing.
	m.ObserveEngineEvents("0", pebble.EngineEventStats{WriteStalls: 1, WriteStallSeconds: 0.5})
	body := scrapeMetrics(t, m)
	for _, want := range []string{
		`cefas_pebble_write_stalls_total{shard="0"} 4`,
		`cefas_pebble_write_stall_seconds_total{shard="0"} 3`,
		`cefas_pebble_write_stall_active{shard="0"} 0`,
		`cefas_pebble_disk_slow_total{shard="0"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q\n--- got ---\n%s", want, body)
		}
	}
}

func TestStorageCollectorExportsStallAndStreamFenceSeries(t *testing.T) {
	m := New()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stop := startStorageCollectorForTest(m, "solo", db, nil, time.Millisecond)
	defer stop()
	td := types.TableDescriptor{
		Name:                "logs",
		KeySchema:           types.KeySchema{PK: "id"},
		TextIndexes:         []types.TextIndexDescriptor{{Name: "message", Field: "message"}},
		StreamSpecification: &types.StreamSpecification{StreamEnabled: true, StreamViewType: types.StreamViewTypeNewImage},
	}
	var body string
	wait.Eventually(t, func() bool {
		// Writes after the collector attached the observer land in the histogram.
		_ = db.PutItemWith(td, types.Item{"id": {T: types.AttrS, S: "1"}, "message": {T: types.AttrS, S: "hello"}}, pebble.PutOptions{})
		body = scrapeMetrics(t, m)
		return strings.Contains(body, `cefas_pebble_write_stalls_total{shard="solo"} 0`) &&
			strings.Contains(body, `cefas_pebble_write_stall_active{shard="solo"} 0`) &&
			strings.Contains(body, `cefas_stream_fence_inflight{shard="solo"} 0`) &&
			strings.Contains(body, `cefas_stream_fence_lag_sequences{shard="solo"} 0`) &&
			strings.Contains(body, `cefas_stream_fence_hold_seconds_count{shard="solo"}`)
	}, 2*time.Second, 10*time.Millisecond, "collector missing stall/fence series\n--- got ---\n%s", body)
}

// TestStorageCollectorAttachesHoldHistogramBeforeFirstTick: native-stream
// writes are observed from collector start, not from its first sample.
func TestStorageCollectorAttachesHoldHistogramBeforeFirstTick(t *testing.T) {
	m := New()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stop := startStorageCollectorForTest(m, "solo", db, nil, time.Hour)
	defer stop()
	td := types.TableDescriptor{
		Name:                "logs",
		KeySchema:           types.KeySchema{PK: "id"},
		TextIndexes:         []types.TextIndexDescriptor{{Name: "message", Field: "message"}},
		StreamSpecification: &types.StreamSpecification{StreamEnabled: true, StreamViewType: types.StreamViewTypeNewImage},
	}
	var body string
	wait.Eventually(t, func() bool {
		if err := db.PutItemWith(td, types.Item{"id": {T: types.AttrS, S: "1"}, "message": {T: types.AttrS, S: "hello"}}, pebble.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		body = scrapeMetrics(t, m)
		return strings.Contains(body, `cefas_stream_fence_hold_seconds_count{shard="solo"}`)
	}, 2*time.Second, 5*time.Millisecond, "hold histogram not attached before the first tick\n--- got ---\n%s", body)
}

// TestRaftLogStorageCollectorExportsNoStreamFenceSeries: a Raft log store
// exports stall counters but not the always-zero native-stream series.
func TestRaftLogStorageCollectorExportsNoStreamFenceSeries(t *testing.T) {
	m := New()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunRaftLogStorageCollector(ctx, m, "raft", db, time.Millisecond)
	}()
	defer func() { cancel(); <-done }()
	var body string
	wait.Eventually(t, func() bool {
		body = scrapeMetrics(t, m)
		return strings.Contains(body, `cefas_pebble_write_stalls_total{shard="raft"} 0`)
	}, 2*time.Second, 5*time.Millisecond, "raft log collector missing stall series\n--- got ---\n%s", body)
	if strings.Contains(body, `cefas_stream_fence_`) {
		t.Fatalf("raft log store exported stream fence series\n--- got ---\n%s", body)
	}
}
