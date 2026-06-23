package pebble_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/CefasDb/cefasdb/internal/auth"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestCtxAwareMethodsUseServiceLevelShares(t *testing.T) {
	// Only read-side Ctx entries route through a per-SL lane queue.
	// Write-side entries (PutItemWithCtx, BatchWriteItemCtx,
	// DeleteItemWithCtx) intentionally bypass the lane to avoid the
	// FSM re-entry deadlock — ApplyCommittedBatch on the leader still
	// takes the write lane after raft commit. SL accounting on writes
	// would have to be threaded through CommitBatch's payload, not the
	// caller's goroutine.
	db := openLaneTestDB(t)
	var shares atomic.Int64
	shares.Store(7)
	db.AttachServiceLevelSharesResolver(func(name string) (int, error) {
		if name == "interactive" {
			return int(shares.Load()), nil
		}
		return 1, nil
	})

	ctx := auth.WithServiceLevel(context.Background(), "interactive")
	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	item := types.Item{"id": {T: types.AttrS, S: "a"}, "v": {T: types.AttrS, S: "one"}}

	if err := db.PutItemWithCtx(ctx, td, item, pebble.PutOptions{}); err != nil {
		t.Fatalf("PutItemWithCtx: %v", err)
	}
	if _, err := db.GetItemCtx(ctx, td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: "a"}}); err != nil {
		t.Fatalf("GetItemCtx: %v", err)
	}
	if err := db.ScanTableWithCtx(ctx, td.Name, func(types.Item) bool { return true }); err != nil {
		t.Fatalf("ScanTableWithCtx: %v", err)
	}

	stats := laneStatsByName(db.LaneStats())
	assertSLLane(t, stats["read"], "interactive", 7)
	assertNoSLLane(t, stats["write"], "interactive")

	shares.Store(11)
	if _, err := db.GetItemCtx(ctx, td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: "a"}}); err != nil {
		t.Fatalf("second GetItemCtx: %v", err)
	}
	assertSLLane(t, laneStatsByName(db.LaneStats())["read"], "interactive", 7)

	db.InvalidateServiceLevelShares("interactive")
	if _, err := db.GetItemCtx(ctx, td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: "a"}}); err != nil {
		t.Fatalf("third GetItemCtx: %v", err)
	}
	assertSLLane(t, laneStatsByName(db.LaneStats())["read"], "interactive", 11)
}

func assertNoSLLane(t *testing.T, snap pebble.LaneSnapshot, name string) {
	t.Helper()
	for _, sl := range snap.ServiceLevels {
		if sl.Name == name {
			t.Fatalf("%s lane unexpectedly registered SL %q: %+v", snap.Lane, name, snap.ServiceLevels)
		}
	}
}

func assertSLLane(t *testing.T, snap pebble.LaneSnapshot, name string, shares int) {
	t.Helper()
	for _, sl := range snap.ServiceLevels {
		if sl.Name == name {
			if sl.Shares != shares {
				t.Fatalf("%s lane shares = %d, want %d", snap.Lane, sl.Shares, shares)
			}
			if sl.Served == 0 {
				t.Fatalf("%s lane served = 0 for %s", snap.Lane, name)
			}
			return
		}
	}
	t.Fatalf("%s lane missing service level %q: %+v", snap.Lane, name, snap.ServiceLevels)
}
