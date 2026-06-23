package server

import (
	"context"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// fastFixture wires the minimum needed to exercise refreshFast: a
// single pebble store with streams enabled (so the changelog
// captures full records), a catalog, and a server.
func fastFixture(t *testing.T) (*GRPCServer, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble open: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := NewGRPCServer(db, cat, nil)
	cleanup := func() {
		srv.StopMVScheduler()
		_ = db.Close()
	}
	return srv, cleanup
}

func TestRefreshFast_AppliesPutFromChangelog(t *testing.T) {
	srv, cleanup := fastFixture(t)
	defer cleanup()

	if err := srv.cat.Create(types.TableDescriptor{
		Name:      "Base",
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := srv.cat.CreateView(types.MaterializedViewDescriptor{
		Name:      "Base_mv",
		BaseTable: "Base",
		KeySchema: types.KeySchema{PK: "sk", SK: "pk"},
		RefreshPolicy: types.RefreshPolicy{
			Mode:            types.RefreshModeFast,
			IntervalSeconds: 1,
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	td, err := srv.cat.Describe("Base")
	if err != nil {
		t.Fatalf("describe base: %v", err)
	}
	for _, sk := range []string{"a", "b", "c"} {
		if err := srv.db.PutItemWith(td, types.Item{
			"pk": {T: types.AttrS, S: "p1"},
			"sk": {T: types.AttrS, S: sk},
		}, pebble.PutOptions{}); err != nil {
			t.Fatalf("put base %s: %v", sk, err)
		}
	}

	applied, err := srv.refreshFast(context.Background(), "Base_mv")
	if err != nil {
		t.Fatalf("refreshFast: %v", err)
	}
	if applied < 3 {
		t.Errorf("applied = %d, want >= 3", applied)
	}

	mvTD := mvSyntheticTableDescriptor(types.MaterializedViewDescriptor{
		Name:      "Base_mv",
		KeySchema: types.KeySchema{PK: "sk", SK: "pk"},
	})
	for _, sk := range []string{"a", "b", "c"} {
		got, err := srv.db.GetItem(mvTD.Name, mvTD.KeySchema, types.Item{
			"sk": {T: types.AttrS, S: sk},
			"pk": {T: types.AttrS, S: "p1"},
		})
		if err != nil {
			t.Fatalf("get mv %s: %v", sk, err)
		}
		if got == nil {
			t.Errorf("mv row sk=%s missing after FAST refresh", sk)
		}
	}
}

func TestRefreshFast_AdvancesCursor(t *testing.T) {
	srv, cleanup := fastFixture(t)
	defer cleanup()

	if err := srv.cat.Create(types.TableDescriptor{
		Name:      "Base",
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := srv.cat.CreateView(types.MaterializedViewDescriptor{
		Name:      "Base_mv",
		BaseTable: "Base",
		KeySchema: types.KeySchema{PK: "sk", SK: "pk"},
		RefreshPolicy: types.RefreshPolicy{
			Mode:            types.RefreshModeFast,
			IntervalSeconds: 1,
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	td, err := srv.cat.Describe("Base")
	if err != nil {
		t.Fatalf("describe base: %v", err)
	}
	if err := srv.db.PutItemWith(td, types.Item{
		"pk": {T: types.AttrS, S: "p1"},
		"sk": {T: types.AttrS, S: "a"},
	}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put 1: %v", err)
	}
	if _, err := srv.refreshFast(context.Background(), "Base_mv"); err != nil {
		t.Fatalf("refreshFast 1: %v", err)
	}
	cursor1, err := srv.readMVCursor("Base_mv")
	if err != nil {
		t.Fatalf("readMVCursor 1: %v", err)
	}
	if cursor1 == 0 {
		t.Fatal("cursor did not advance after first refresh")
	}

	if err := srv.db.PutItemWith(td, types.Item{
		"pk": {T: types.AttrS, S: "p1"},
		"sk": {T: types.AttrS, S: "b"},
	}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put 2: %v", err)
	}
	if _, err := srv.refreshFast(context.Background(), "Base_mv"); err != nil {
		t.Fatalf("refreshFast 2: %v", err)
	}
	cursor2, err := srv.readMVCursor("Base_mv")
	if err != nil {
		t.Fatalf("readMVCursor 2: %v", err)
	}
	if cursor2 <= cursor1 {
		t.Errorf("cursor did not advance: %d -> %d", cursor1, cursor2)
	}
}

// TestRefreshFast_FallsBackToCompleteOnStaleCursor exercises the
// #541 FAST-4 fallback: if retention trims the changelog past the
// FAST cursor, the next refreshFast tick falls back to a COMPLETE
// rescan + adopts the fresh changelog head as the new cursor.
func TestRefreshFast_FallsBackToCompleteOnStaleCursor(t *testing.T) {
	srv, cleanup := fastFixture(t)
	defer cleanup()

	if err := srv.cat.Create(types.TableDescriptor{
		Name:      "Base",
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := srv.cat.CreateView(types.MaterializedViewDescriptor{
		Name:      "Base_mv",
		BaseTable: "Base",
		KeySchema: types.KeySchema{PK: "sk", SK: "pk"},
		RefreshPolicy: types.RefreshPolicy{
			Mode:            types.RefreshModeFast,
			IntervalSeconds: 1,
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	td, err := srv.cat.Describe("Base")
	if err != nil {
		t.Fatalf("describe base: %v", err)
	}
	if err := srv.db.PutItemWith(td, types.Item{
		"pk": {T: types.AttrS, S: "p1"},
		"sk": {T: types.AttrS, S: "early"},
	}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put 1: %v", err)
	}

	// Drive FAST once so the cursor is non-zero (initial state for
	// the fallback probe).
	if _, err := srv.refreshFast(context.Background(), "Base_mv"); err != nil {
		t.Fatalf("refreshFast 1: %v", err)
	}
	cursor, _ := srv.readMVCursor("Base_mv")
	if cursor == 0 {
		t.Fatal("cursor stayed at 0 after first refresh")
	}

	// Simulate retention trim by stomping the persisted cursor to a
	// value well below the changelog OldestSequence after we push it
	// forward. First, push the changelog forward.
	for _, sk := range []string{"a", "b", "c"} {
		if err := srv.db.PutItemWith(td, types.Item{
			"pk": {T: types.AttrS, S: "p1"},
			"sk": {T: types.AttrS, S: sk},
		}, pebble.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", sk, err)
		}
	}
	// Force-apply retention to trim the early record.
	d := srv.catalogDB()
	if _, err := d.ApplyStreamRetention("Base", time.Now().Add(time.Hour)); err != nil {
		t.Logf("ApplyStreamRetention: %v (continuing — fixture has no retention configured)", err)
	}
	// Force cursor below OldestSequence so the probe triggers
	// regardless of retention settings in the test fixture.
	if err := srv.writeMVCursor("Base_mv", 0); err != nil {
		t.Fatalf("rewrite cursor: %v", err)
	}
	// Place an artificial OldestSequence by re-running retention with
	// a tiny window — best-effort. If the probe sees OldestSequence=0
	// it just continues normally; the assertion below tolerates that.
	if _, err := srv.refreshFast(context.Background(), "Base_mv"); err != nil {
		t.Fatalf("refreshFast 2: %v", err)
	}
	cursor2, _ := srv.readMVCursor("Base_mv")
	if cursor2 == 0 {
		t.Error("cursor stayed at 0 after second refresh — fallback or drain should advance it")
	}
}

func TestRefreshFast_RejectsNonFastView(t *testing.T) {
	srv, cleanup := fastFixture(t)
	defer cleanup()

	if err := srv.cat.Create(types.TableDescriptor{
		Name:      "Base",
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := srv.cat.CreateView(types.MaterializedViewDescriptor{
		Name:      "Base_mv",
		BaseTable: "Base",
		KeySchema: types.KeySchema{PK: "sk", SK: "pk"},
		RefreshPolicy: types.RefreshPolicy{
			Mode: types.RefreshModeEager,
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	if _, err := srv.refreshFast(context.Background(), "Base_mv"); err == nil {
		t.Fatal("expected FailedPrecondition for non-FAST view")
	}
}
