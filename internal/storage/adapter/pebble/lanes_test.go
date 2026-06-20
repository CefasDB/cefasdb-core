package pebble_test

import (
	"testing"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
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
	ks := types.KeySchema{PK: "id"}
	td := types.TableDescriptor{Name: "events", KeySchema: ks}

	if err := db.BatchWriteItem(td, []pebble.BatchOp{
		{Op: pebble.BatchOpPut, Item: types.Item{"id": sAttr("a"), "v": sAttr("one")}},
		{Op: pebble.BatchOpPut, Item: types.Item{"id": sAttr("b"), "v": sAttr("two")}},
	}); err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}

	got, err := db.BatchGetItem("events", ks, []types.Item{
		{"id": sAttr("a")},
		{"id": sAttr("b")},
	})
	if err != nil {
		t.Fatalf("BatchGetItem: %v", err)
	}
	if len(got) != 2 || got[0]["v"].S != "one" || got[1]["v"].S != "two" {
		t.Fatalf("BatchGetItem = %#v", got)
	}

	stats := laneStatsByName(db.LaneStats())
	if stats["read"].Ops == 0 {
		t.Fatalf("read lane did not record operations: %+v", stats["read"])
	}
	if stats["write"].Ops == 0 {
		t.Fatalf("write lane did not record operations: %+v", stats["write"])
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
