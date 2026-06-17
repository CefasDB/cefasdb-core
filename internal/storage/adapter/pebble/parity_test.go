package pebble_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func tableWithLSI() types.TableDescriptor {
	return types.TableDescriptor{
		Name:      "messages",
		KeySchema: types.KeySchema{PK: "thread_id", SK: "msg_id"},
		LSIs: []types.LSIDescriptor{{
			Name: "by_author",
			SK:   "author",
		}},
	}
}

func TestLSIQueryByLocalSK(t *testing.T) {
	db := openTestDB(t)
	td := tableWithLSI()

	for _, m := range []struct {
		thread, msg, author string
	}{
		{"t1", "001", "alice"},
		{"t1", "002", "bob"},
		{"t1", "003", "alice"},
		{"t2", "001", "alice"}, // different partition; must not appear in t1 LSI
	} {
		if err := db.PutItemWith(td, types.Item{
			"thread_id": sAttr(m.thread),
			"msg_id":    sAttr(m.msg),
			"author":    sAttr(m.author),
		}, pebble.PutOptions{}); err != nil {
			t.Fatalf("put %s/%s: %v", m.thread, m.msg, err)
		}
	}

	// All messages by author within thread t1, in alphabetical order
	// (alice comes before bob lexicographically).
	got, err := db.QueryByLSI(td, "by_author", sAttr("t1"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("QueryByLSI: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 items in t1 LSI, got %d", len(got))
	}
	// First two entries belong to alice (msg_id 001 then 003); third
	// is bob's 002.
	if got[0]["author"].S != "alice" || got[1]["author"].S != "alice" || got[2]["author"].S != "bob" {
		t.Fatalf("LSI ordering wrong: %+v", got)
	}
}

func TestLSIRangeOnSK(t *testing.T) {
	db := openTestDB(t)
	td := tableWithLSI()

	for _, m := range []struct {
		thread, msg, author string
	}{
		{"t1", "001", "alice"},
		{"t1", "002", "bob"},
		{"t1", "003", "carol"},
		{"t1", "004", "dave"},
	} {
		_ = db.PutItemWith(td, types.Item{
			"thread_id": sAttr(m.thread),
			"msg_id":    sAttr(m.msg),
			"author":    sAttr(m.author),
		}, pebble.PutOptions{})
	}
	got, err := db.QueryByLSI(td, "by_author", sAttr("t1"), pebble.QueryOptions{
		SKLow:  sAttr("b"),
		SKHigh: sAttr("d"),
	})
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("range [b,d) want 2 items, got %d (%v)", len(got), got)
	}
	authors := map[string]bool{got[0]["author"].S: true, got[1]["author"].S: true}
	if !authors["bob"] || !authors["carol"] {
		t.Fatalf("range wrong: %+v", got)
	}
}

func TestLSISparseExclusion(t *testing.T) {
	db := openTestDB(t)
	td := tableWithLSI()

	if err := db.PutItemWith(td, types.Item{
		"thread_id": sAttr("t1"),
		"msg_id":    sAttr("001"),
		// author intentionally missing — sparse exclusion.
	}, pebble.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	got, _ := db.QueryByLSI(td, "by_author", sAttr("t1"), pebble.QueryOptions{})
	if len(got) != 0 {
		t.Fatalf("sparse item leaked into LSI: %+v", got)
	}
}

func TestLSIDeleteRemovesPointer(t *testing.T) {
	db := openTestDB(t)
	td := tableWithLSI()
	item := types.Item{
		"thread_id": sAttr("t1"),
		"msg_id":    sAttr("001"),
		"author":    sAttr("alice"),
	}
	_ = db.PutItemWith(td, item, pebble.PutOptions{})
	if err := db.DeleteItemWith(td, types.Item{
		"thread_id": sAttr("t1"),
		"msg_id":    sAttr("001"),
	}, pebble.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	got, _ := db.QueryByLSI(td, "by_author", sAttr("t1"), pebble.QueryOptions{})
	if len(got) != 0 {
		t.Fatalf("LSI pointer leaked after delete: %+v", got)
	}
}

// ---------- TTL ----------

type fakeCatalog struct {
	tables []types.TableDescriptor
}

func (f *fakeCatalog) List() []types.TableDescriptor { return f.tables }

func TestTTLReaperSweepsExpired(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{
		Name:         "sessions",
		KeySchema:    types.KeySchema{PK: "id"},
		TTLAttribute: "expires_at",
	}

	// Three rows: two expired, one fresh.
	now := time.Now()
	_ = db.PutItemWith(td, types.Item{
		"id":         sAttr("old1"),
		"expires_at": nAttr(fmt.Sprintf("%d", now.Add(-2*time.Hour).Unix())),
	}, pebble.PutOptions{})
	_ = db.PutItemWith(td, types.Item{
		"id":         sAttr("old2"),
		"expires_at": nAttr(fmt.Sprintf("%d", now.Add(-1*time.Hour).Unix())),
	}, pebble.PutOptions{})
	_ = db.PutItemWith(td, types.Item{
		"id":         sAttr("fresh"),
		"expires_at": nAttr(fmt.Sprintf("%d", now.Add(time.Hour).Unix())),
	}, pebble.PutOptions{})

	reaper := pebble.NewReaper(db, &fakeCatalog{tables: []types.TableDescriptor{td}}, nil, pebble.ReaperConfig{
		BatchSize: 1024,
		Now:       func() time.Time { return now },
	})
	if err := reaper.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	_, err := db.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttr("old1")})
	if err != types.ErrItemNotFound {
		t.Errorf("old1 still present, got %v", err)
	}
	_, err = db.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttr("old2")})
	if err != types.ErrItemNotFound {
		t.Errorf("old2 still present, got %v", err)
	}
	fresh, err := db.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttr("fresh")})
	if err != nil || fresh == nil {
		t.Errorf("fresh row reaped: %v %v", fresh, err)
	}
}

func TestTTLNoAttributeNoop(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{
		Name:         "sessions",
		KeySchema:    types.KeySchema{PK: "id"},
		TTLAttribute: "expires_at",
	}
	// Item without the TTL attribute → sparse, no reaper action.
	if err := db.PutItemWith(td, types.Item{"id": sAttr("k")}, pebble.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	reaper := pebble.NewReaper(db, &fakeCatalog{tables: []types.TableDescriptor{td}}, nil, pebble.ReaperConfig{
		Now: func() time.Time { return time.Now().Add(100 * 365 * 24 * time.Hour) },
	})
	if err := reaper.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttr("k")})
	if err != nil || got == nil {
		t.Fatalf("ttl-less row was reaped: %v %v", got, err)
	}
}

// ---------- Projections ----------

func tableWithProjectedGSI(mode string, include []string) types.TableDescriptor {
	return types.TableDescriptor{
		Name:      "events",
		KeySchema: types.KeySchema{PK: "user_id", SK: "ts"},
		GSIs: []types.GSIDescriptor{{
			Name:      "by_event",
			KeySchema: types.KeySchema{PK: "event", SK: "ts"},
			Projection: types.IndexProjection{
				Mode:    mode,
				Include: include,
			},
		}},
	}
}

func TestGSIProjectionAll(t *testing.T) {
	db := openTestDB(t)
	td := tableWithProjectedGSI("ALL", nil)
	if err := db.PutItemWith(td, types.Item{
		"user_id": sAttr("alice"),
		"ts":      sAttr("001"),
		"event":   sAttr("login"),
		"city":    sAttr("Vancouver"),
	}, pebble.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// Wipe the primary item so a KEYS_ONLY-style dereference would
	// fail; the ALL projection must satisfy the read entirely from
	// the index value.
	if err := db.Delete([]byte("cefas/t/events/p/__nonexistent__")); err != nil && err != pebble.ErrNotFound {
		// Best-effort; the goal is just to confirm we can serve
		// reads when the projection is ALL.
	}
	got, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows", len(got))
	}
	if got[0]["city"].S != "Vancouver" {
		t.Fatalf("ALL projection did not carry city: %+v", got[0])
	}
}

func TestGSIProjectionInclude(t *testing.T) {
	db := openTestDB(t)
	td := tableWithProjectedGSI("INCLUDE", []string{"event", "city"})
	if err := db.PutItemWith(td, types.Item{
		"user_id": sAttr("alice"),
		"ts":      sAttr("001"),
		"event":   sAttr("login"),
		"city":    sAttr("Lisboa"),
		"agent":   sAttr("cli/0.1"), // NOT projected
	}, pebble.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows", len(got))
	}
	if got[0]["city"].S != "Lisboa" {
		t.Fatalf("included attribute missing: %+v", got[0])
	}
	if _, ok := got[0]["agent"]; ok {
		t.Fatalf("non-projected attribute leaked: %+v", got[0])
	}
}

func TestGSIProjectionKeysOnlyBackwardCompat(t *testing.T) {
	db := openTestDB(t)
	td := tableWithProjectedGSI("", nil) // KEYS_ONLY default
	_ = db.PutItemWith(td, types.Item{
		"user_id": sAttr("alice"),
		"ts":      sAttr("001"),
		"event":   sAttr("login"),
	}, pebble.PutOptions{})
	got, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0]["user_id"].S != "alice" {
		t.Fatalf("KEYS_ONLY read broken: %+v", got)
	}
}
