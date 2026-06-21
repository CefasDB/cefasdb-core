package pebble_test

import (
	"testing"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

func openLaneTestDB(t testing.TB) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(pebble.Options{
		Path: t.TempDir(),
		Lanes: pebble.LaneOptions{
			Mode:         pebble.LaneModeOn,
			ReadWorkers:  1,
			WriteWorkers: 1,
			ReadQueue:    8,
			WriteQueue:   8,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestReadWriteLanesServePointReadsAndWrites(t *testing.T) {
	db := openLaneTestDB(t)

	// Set/Delete keep the write lane (small independent ops the lane
	// was designed to throttle). CommitBatch bypasses it after #428 —
	// see TestCommitBatchBypassesWriteLane.
	// Get/Has bypass the read lane after #434 — see
	// TestGetHasBypassesReadLane. Reads still flow through the lane
	// for Iter/Scan/Query paths; Get/Has prove correctness, not lane
	// accounting.
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("value = %q, want v1", got)
	}
	ok, err := db.Has([]byte("k2"))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !ok {
		t.Fatal("Has(k2) = false")
	}

	stats := laneStatsByName(db.LaneStats())
	if stats["write"].Ops == 0 {
		t.Fatalf("write lane did not record operations: %+v", stats["write"])
	}
}

// TestGetHasBypassesReadLane pins the #434 contract: Get / Has are
// point reads that pay the lane round-trip more than they pay Pebble
// itself, so they skip the lane entirely. Long-running readers (Iter,
// Scan, Query) still use it for tail-latency isolation.
func TestGetHasBypassesReadLane(t *testing.T) {
	db := openLaneTestDB(t)
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before := laneStatsByName(db.LaneStats())["read"].Ops

	if _, err := db.Get([]byte("k")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := db.Has([]byte("k")); err != nil {
		t.Fatalf("Has: %v", err)
	}

	after := laneStatsByName(db.LaneStats())["read"].Ops
	if after != before {
		t.Fatalf("Get/Has must not increment read lane Ops: before=%d after=%d", before, after)
	}
}

// TestCommitBatchBypassesWriteLane pins the #428 contract: CommitBatch
// is intentionally routed straight to commitCh so the group-commit
// coalescer is not throttled by lane.write.workers.
func TestCommitBatchBypassesWriteLane(t *testing.T) {
	db := openLaneTestDB(t)
	before := laneStatsByName(db.LaneStats())["write"].Ops

	b := db.Raw().NewBatch()
	if err := b.Set([]byte("cb"), []byte("v"), nil); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if err := db.CommitBatch(b); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}

	after := laneStatsByName(db.LaneStats())["write"].Ops
	if after != before {
		t.Fatalf("CommitBatch must not increment write lane Ops: before=%d after=%d", before, after)
	}
}

func TestApplyCommittedBatchUsesWriteLane(t *testing.T) {
	db := openLaneTestDB(t)
	b := db.Raw().NewBatch()
	if err := b.Set([]byte("cefas/table/events/item/a"), []byte("raw"), nil); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	repr := append([]byte(nil), b.Repr()...)
	if err := b.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}

	if err := db.ApplyCommittedBatch(repr); err != nil {
		t.Fatalf("ApplyCommittedBatch: %v", err)
	}
	got, err := db.Get([]byte("cefas/table/events/item/a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "raw" {
		t.Fatalf("value = %q", got)
	}
	stats := laneStatsByName(db.LaneStats())
	if stats["write"].Ops == 0 {
		t.Fatalf("write lane did not record committed apply: %+v", stats["write"])
	}
}

func laneStatsByName(in []pebble.LaneSnapshot) map[string]pebble.LaneSnapshot {
	out := make(map[string]pebble.LaneSnapshot, len(in))
	for _, snap := range in {
		out[snap.Lane] = snap
	}
	return out
}
