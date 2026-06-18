package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	craft "github.com/CefasDb/cefasdb/internal/replication"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/testutil/wait"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type staticLeader bool

func (s staticLeader) IsLeader() bool { return bool(s) }

type staticLeaderWithAsyncStats struct {
	leader bool
	stats  craft.AsyncReplicationStats
}

func (s staticLeaderWithAsyncStats) IsLeader() bool { return s.leader }
func (s staticLeaderWithAsyncStats) AsyncReplicationStats() craft.AsyncReplicationStats {
	return s.stats
}

func TestRunStorageCollectorExposesPebbleAndLeaderMetrics(t *testing.T) {
	m := New()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunStorageCollector(ctx, m, "solo", db, staticLeader(true), time.Millisecond)

	var body string
	wait.Eventually(t, func() bool {
		body = scrapeMetrics(t, m)
		return strings.Contains(body, `cefas_raft_is_leader{shard="solo"} 1`) &&
			strings.Contains(body, `cefas_pebble_read_amp{shard="solo"}`) &&
			strings.Contains(body, `cefas_pebble_level_files{level="0",shard="solo"}`)
	}, time.Second, 10*time.Millisecond, "metrics body missing expected storage collector series\n--- got ---\n%s", body)
}

func TestRunStorageCollectorExposesAsyncReplicationMetrics(t *testing.T) {
	m := New()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	leader := staticLeaderWithAsyncStats{
		leader: true,
		stats: craft.AsyncReplicationStats{
			Submitted:     12,
			Dropped:       2,
			ApplyErrors:   1,
			QueueDepth:    3,
			QueueCapacity: 1024,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunStorageCollector(ctx, m, "solo", db, leader, time.Millisecond)

	var body string
	wait.Eventually(t, func() bool {
		body = scrapeMetrics(t, m)
		return strings.Contains(body, `cefas_raft_async_replication_submitted{shard="solo"} 12`) &&
			strings.Contains(body, `cefas_raft_async_replication_dropped{shard="solo"} 2`) &&
			strings.Contains(body, `cefas_raft_async_replication_apply_errors{shard="solo"} 1`) &&
			strings.Contains(body, `cefas_raft_async_replication_queue_depth{shard="solo"} 3`) &&
			strings.Contains(body, `cefas_raft_async_replication_queue_capacity{shard="solo"} 1024`)
	}, time.Second, 10*time.Millisecond, "metrics body missing expected async replication series\n--- got ---\n%s", body)
}

func TestRunStorageCollectorExposesStreamRetentionMetrics(t *testing.T) {
	m := New()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	td := types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "id"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
	}
	if err := db.PutItemWith(td, types.Item{
		"id": {T: types.AttrS, S: "event-1"},
	}, pebble.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunStorageCollector(ctx, m, "solo", db, nil, time.Millisecond)

	var body string
	wait.Eventually(t, func() bool {
		body = scrapeMetrics(t, m)
		return strings.Contains(body, `cefas_stream_records_appended{shard="solo",table="Events"} 1`) &&
			strings.Contains(body, `cefas_stream_newest_sequence{shard="solo",table="Events"} 1`)
	}, time.Second, 10*time.Millisecond, "metrics body missing expected stream retention series\n--- got ---\n%s", body)
}

func scrapeMetrics(t *testing.T, m *Metrics) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
