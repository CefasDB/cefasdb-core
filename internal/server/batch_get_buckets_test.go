package server

import (
	"sync/atomic"
	"testing"
	"time"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestBatchGetGroupByShardPreservesOrderAndGroups(t *testing.T) {
	dbA := newTestPebbleDB(t)
	dbB := newTestPebbleDB(t)
	ks := types.KeySchema{PK: "id"}
	td := types.TableDescriptor{Name: "T", KeySchema: ks}

	// Seed: dbA owns "a*"; dbB owns "b*". Mix the requested order so the
	// helper must splice results back into the right slot.
	put := func(db *pebble.DB, id string) {
		t.Helper()
		if err := db.PutItemWith(td, types.Item{"id": sAttr(id), "v": sAttr(id + "-val")}, pebble.PutOptions{}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	put(dbA, "a1")
	put(dbA, "a2")
	put(dbB, "b1")

	keys := []types.Item{
		{"id": sAttr("b1")},
		{"id": sAttr("a2")},
		{"id": sAttr("missing")},
		{"id": sAttr("a1")},
	}

	var groupsHit atomic.Int32
	routeFor := func(pkBytes []byte) (*pebble.DB, error) {
		if len(pkBytes) > 0 && pkBytes[0] == 'a' {
			return dbA, nil
		}
		if len(pkBytes) > 0 && pkBytes[0] == 'b' {
			return dbB, nil
		}
		// "missing" → arbitrarily route to A; key won't exist there.
		return dbA, nil
	}
	observeRead := func(_ []byte, _ uint64, _ time.Time) {
		groupsHit.Add(1)
	}

	out, err := batchGetGroupByShard("T", ks, keys, routeFor, observeRead)
	if err != nil {
		t.Fatalf("batchGetGroupByShard: %v", err)
	}
	if len(out) != len(keys) {
		t.Fatalf("len(out) = %d, want %d", len(out), len(keys))
	}
	if out[0]["v"].S != "b1-val" {
		t.Fatalf("out[0] = %+v, want b1-val", out[0])
	}
	if out[1]["v"].S != "a2-val" {
		t.Fatalf("out[1] = %+v, want a2-val", out[1])
	}
	if out[2] != nil {
		t.Fatalf("out[2] = %+v, want nil (missing)", out[2])
	}
	if out[3]["v"].S != "a1-val" {
		t.Fatalf("out[3] = %+v, want a1-val", out[3])
	}
	if got := groupsHit.Load(); got != int32(len(keys)) {
		t.Fatalf("observeRead invocations = %d, want %d (once per key)", got, len(keys))
	}
}

func TestBatchGetGroupByShardEmptyKeys(t *testing.T) {
	ks := types.KeySchema{PK: "id"}
	called := atomic.Int32{}
	out, err := batchGetGroupByShard("T", ks, nil,
		func(_ []byte) (*pebble.DB, error) {
			called.Add(1)
			return nil, nil
		},
		func(_ []byte, _ uint64, _ time.Time) { called.Add(1) },
	)
	if err != nil {
		t.Fatalf("batchGetGroupByShard(empty): %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("len(out) = %d, want 0", len(out))
	}
	if got := called.Load(); got != 0 {
		t.Fatalf("routeFor/observeRead called %d times on empty input", got)
	}
}

func TestBatchGetGroupByShardIssuesOneRPCPerShard(t *testing.T) {
	dbA := newTestPebbleDB(t)
	dbB := newTestPebbleDB(t)
	ks := types.KeySchema{PK: "id"}
	td := types.TableDescriptor{Name: "T", KeySchema: ks}
	for _, id := range []string{"a1", "a2", "a3"} {
		if err := dbA.PutItemWith(td, types.Item{"id": sAttr(id)}, pebble.PutOptions{}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	for _, id := range []string{"b1", "b2"} {
		if err := dbB.PutItemWith(td, types.Item{"id": sAttr(id)}, pebble.PutOptions{}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	keys := []types.Item{
		{"id": sAttr("a1")}, {"id": sAttr("b1")}, {"id": sAttr("a2")},
		{"id": sAttr("b2")}, {"id": sAttr("a3")},
	}
	routeFor := func(pkBytes []byte) (*pebble.DB, error) {
		if pkBytes[0] == 'a' {
			return dbA, nil
		}
		return dbB, nil
	}
	// Per-shard timestamps prove there were exactly 2 RPCs (one per
	// shard), not 5 (one per key).
	starts := map[*pebble.DB]time.Time{}
	observeRead := func(pkBytes []byte, _ uint64, started time.Time) {
		db := dbA
		if pkBytes[0] == 'b' {
			db = dbB
		}
		if prev, ok := starts[db]; ok && !prev.Equal(started) {
			t.Fatalf("shard %p saw two distinct started timestamps — expected one RPC per shard", db)
		}
		starts[db] = started
	}
	if _, err := batchGetGroupByShard("T", ks, keys, routeFor, observeRead); err != nil {
		t.Fatalf("batchGetGroupByShard: %v", err)
	}
	if len(starts) != 2 {
		t.Fatalf("started count = %d, want 2 (one per shard)", len(starts))
	}
}

func sAttr(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func newTestPebbleDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(pebble.Options{
		Path: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
