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
	assertSLLane(t, stats["write"], "interactive", 7)
	assertSLLane(t, stats["read"], "interactive", 7)

	shares.Store(11)
	if err := db.PutItemWithCtx(ctx, td, types.Item{"id": {T: types.AttrS, S: "b"}}, pebble.PutOptions{}); err != nil {
		t.Fatalf("second PutItemWithCtx: %v", err)
	}
	assertSLLane(t, laneStatsByName(db.LaneStats())["write"], "interactive", 7)

	db.InvalidateServiceLevelShares("interactive")
	if err := db.PutItemWithCtx(ctx, td, types.Item{"id": {T: types.AttrS, S: "c"}}, pebble.PutOptions{}); err != nil {
		t.Fatalf("third PutItemWithCtx: %v", err)
	}
	assertSLLane(t, laneStatsByName(db.LaneStats())["write"], "interactive", 11)
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
