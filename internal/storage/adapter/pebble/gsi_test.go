package pebble_test

import (
	"errors"
	"testing"

	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func tableWithGSI() types.TableDescriptor {
	return types.TableDescriptor{
		Name:      "events",
		KeySchema: types.KeySchema{PK: "user_id", SK: "ts"},
		GSIs: []types.GSIDescriptor{{
			Name:      "by_event",
			KeySchema: types.KeySchema{PK: "event", SK: "ts"},
		}},
	}
}

func TestGSIPutAndQuery(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGSI()

	items := []types.Item{
		{"user_id": sAttr("alice"), "ts": sAttr("001"), "event": sAttr("login")},
		{"user_id": sAttr("bob"), "ts": sAttr("002"), "event": sAttr("login")},
		{"user_id": sAttr("alice"), "ts": sAttr("003"), "event": sAttr("purchase")},
	}
	for _, it := range items {
		if err := db.PutItemWith(td, it, pebble.PutOptions{}); err != nil {
			t.Fatalf("PutItemWith: %v", err)
		}
	}

	got, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("QueryByGSI: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("login query returned %d items, want 2", len(got))
	}
	if got[0]["ts"].S != "001" || got[1]["ts"].S != "002" {
		t.Fatalf("login GSI order wrong: %+v", got)
	}

	got, err = db.QueryByGSI(td, "by_event", sAttr("purchase"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("QueryByGSI purchase: %v", err)
	}
	if len(got) != 1 || got[0]["user_id"].S != "alice" {
		t.Fatalf("purchase query returned %+v", got)
	}
}

func TestGSIUpdateMovesPointer(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGSI()

	item := types.Item{"user_id": sAttr("alice"), "ts": sAttr("001"), "event": sAttr("login")}
	if err := db.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Update GSI PK from "login" to "logout".
	item["event"] = sAttr("logout")
	if err := db.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
		t.Fatalf("put updated: %v", err)
	}

	loginHits, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("query login: %v", err)
	}
	if len(loginHits) != 0 {
		t.Fatalf("login should be empty after rename, got %+v", loginHits)
	}
	logoutHits, err := db.QueryByGSI(td, "by_event", sAttr("logout"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("query logout: %v", err)
	}
	if len(logoutHits) != 1 {
		t.Fatalf("logout should have 1 item, got %d", len(logoutHits))
	}
}

func TestGSIDeleteRemovesPointer(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGSI()

	item := types.Item{"user_id": sAttr("alice"), "ts": sAttr("001"), "event": sAttr("login")}
	if err := db.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.DeleteItemWith(td, types.Item{
		"user_id": sAttr("alice"),
		"ts":      sAttr("001"),
	}, pebble.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GSI should be empty after primary delete, got %+v", got)
	}
}

func TestGSISparseIndex(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGSI()

	// Item without the "event" attribute — should not appear in any
	// GSI partition.
	noEvent := types.Item{"user_id": sAttr("alice"), "ts": sAttr("001"), "other": sAttr("x")}
	if err := db.PutItemWith(td, noEvent, pebble.PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty GSI, got %+v", got)
	}
}

func TestGSIQuerySKRange(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGSI()

	for _, ts := range []string{"001", "002", "003", "004"} {
		_ = db.PutItemWith(td, types.Item{
			"user_id": sAttr("alice"),
			"ts":      sAttr(ts),
			"event":   sAttr("login"),
		}, pebble.PutOptions{})
	}
	got, err := db.QueryByGSI(td, "by_event", sAttr("login"), pebble.QueryOptions{
		SKLow:  sAttr("002"),
		SKHigh: sAttr("004"),
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 2 || got[0]["ts"].S != "002" || got[1]["ts"].S != "003" {
		t.Fatalf("range returned %+v", got)
	}
}

func TestConditionalPutWritesOnce(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{
		Name:      "singles",
		KeySchema: types.KeySchema{PK: "id"},
	}
	item := types.Item{"id": sAttr("k1"), "data": sAttr("first")}
	err := db.PutItemWith(td, item, pebble.PutOptions{Condition: "attribute_not_exists(id)"})
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	err = db.PutItemWith(td, item, pebble.PutOptions{Condition: "attribute_not_exists(id)"})
	if !errors.Is(err, storage.ErrConditionFailed) {
		t.Fatalf("second put expected ErrConditionFailed, got %v", err)
	}
}

func TestConditionalDelete(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{
		Name:      "singles",
		KeySchema: types.KeySchema{PK: "id"},
	}
	item := types.Item{"id": sAttr("k1"), "version": nAttr("1")}
	_ = db.PutItemWith(td, item, pebble.PutOptions{})
	err := db.DeleteItemWith(td, item, pebble.DeleteOptions{
		Condition: "version = :v",
		Binds:     map[string]types.AttributeValue{"v": nAttr("2")},
	})
	if !errors.Is(err, storage.ErrConditionFailed) {
		t.Fatalf("expected ErrConditionFailed on stale version, got %v", err)
	}
	err = db.DeleteItemWith(td, item, pebble.DeleteOptions{
		Condition: "version = :v",
		Binds:     map[string]types.AttributeValue{"v": nAttr("1")},
	})
	if err != nil {
		t.Fatalf("expected success on matching version, got %v", err)
	}
}

func TestBatchWriteItemAtomic(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGSI()

	ops := []pebble.BatchOp{
		{Op: pebble.BatchOpPut, Item: types.Item{"user_id": sAttr("a"), "ts": sAttr("1"), "event": sAttr("e1")}},
		{Op: pebble.BatchOpPut, Item: types.Item{"user_id": sAttr("b"), "ts": sAttr("2"), "event": sAttr("e1")}},
		{Op: pebble.BatchOpPut, Item: types.Item{"user_id": sAttr("c"), "ts": sAttr("3"), "event": sAttr("e2")}},
	}
	if err := db.BatchWriteItem(td, ops); err != nil {
		t.Fatalf("batch write: %v", err)
	}
	hits, err := db.QueryByGSI(td, "by_event", sAttr("e1"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("e1 should have 2 items, got %d", len(hits))
	}
}

func TestBatchGetItem(t *testing.T) {
	db := openTestDB(t)
	ks := types.KeySchema{PK: "id"}
	_ = db.PutItem("t", ks, types.Item{"id": sAttr("a"), "v": sAttr("A")})
	_ = db.PutItem("t", ks, types.Item{"id": sAttr("b"), "v": sAttr("B")})

	items, err := db.BatchGetItem("t", ks, []types.Item{
		{"id": sAttr("a")},
		{"id": sAttr("missing")},
		{"id": sAttr("b")},
	})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d items, want 3", len(items))
	}
	if items[0]["v"].S != "A" || items[2]["v"].S != "B" {
		t.Fatalf("ordering wrong: %+v", items)
	}
	if items[1] != nil {
		t.Fatalf("expected nil for missing key, got %+v", items[1])
	}
}
