package pebble

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func textTable() types.TableDescriptor {
	return types.TableDescriptor{Name: "logs", KeySchema: types.KeySchema{PK: "tenantId", SK: "orderKey"}, TTLAttribute: "expiresAt", TextIndexes: []types.TextIndexDescriptor{{Name: "message", Field: "message"}}}
}

func textEvent(tenant, id, message string, expiry int64) types.Item {
	return types.Item{"tenantId": {T: types.AttrS, S: tenant}, "orderKey": {T: types.AttrS, S: id}, "timestamp": {T: types.AttrS, S: "2026-10-19T12:00:01Z"}, "message": {T: types.AttrS, S: message}, "level": {T: types.AttrS, S: "error"}, "service": {T: types.AttrS, S: "api"}, "acceptedAt": {T: types.AttrS, S: "2026-10-19T12:00:00Z"}, "expiresAt": {T: types.AttrN, N: fmt.Sprint(expiry)}, "fields": {T: types.AttrM, M: map[string]types.AttributeValue{"attempt": {T: types.AttrN, N: "2"}}}}
}

func textDB(t *testing.T) (*DB, types.TableDescriptor) {
	t.Helper()
	db, err := Open(Options{Path: t.TempDir(), FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	td := textTable()
	raw, _ := json.Marshal(td)
	if err := db.Set(storage.KeyCatalog(td.Name), raw); err != nil {
		t.Fatal(err)
	}
	return db, td
}

func TestNativeTextIsolationExpiryPaginationAndAggregate(t *testing.T) {
	db, td := textDB(t)
	now := time.Now().Unix()
	for _, item := range []types.Item{textEvent("a", "1", "Connection FAILED", now+3600), textEvent("a", "2", "connection failed again", now+3600), textEvent("a", "3", "connection failed expired", now), textEvent("b", "4", "connection failed foreign", now+3600), textEvent("a", "5", "different", now+3600)} {
		if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	q := types.TextQuery{IndexName: "message", PKValue: types.AttributeValue{T: types.AttrS, S: "a"}, Search: "failed connection", Phrase: "CONNECTION failed", Fields: map[string]types.AttributeValue{"fields.attempt": {T: types.AttrN, N: "2.0"}}, Descending: true, Limit: 1}
	page, err := db.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 1 || page.Items[0]["orderKey"].S != "2" || page.NextCursor == "" {
		t.Fatalf("first page: rows=%d cursor=%t err=%v", len(page.Items), page.NextCursor != "", err)
	}
	q.Cursor = page.NextCursor
	page, err = db.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 1 || page.Items[0]["orderKey"].S != "1" || page.NextCursor != "" {
		t.Fatalf("second page: rows=%d cursor=%t err=%v", len(page.Items), page.NextCursor != "", err)
	}
	q.PKValue.S = "b"
	if _, err := db.TextQuery(context.Background(), td, q); err == nil {
		t.Fatal("cross-partition cursor accepted")
	}
	q.PKValue.S = "a"
	q.Cursor = ""
	q.Limit = 0
	agg, err := db.TextAggregate(context.Background(), td, types.TextAggregate{TextQuery: q, IntervalSeconds: 60, GroupBy: "service"})
	if err != nil || len(agg.Buckets) != 1 || agg.Buckets[0].Count != 2 || agg.Buckets[0].Errors != 2 || agg.Buckets[0].Groups["api"] != 2 {
		t.Fatalf("aggregate: %+v err=%v", agg.Buckets, err)
	}
	q.MinValues = map[string]types.AttributeValue{"acceptedAt": {T: types.AttrS, S: "2026-10-19T12:01:00Z"}}
	page, err = db.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("shortened retention: rows=%d err=%v", len(page.Items), err)
	}
	// Expiration is visible before asynchronous deletion on other native reads.
	if _, err := db.GetItem(td.Name, td.KeySchema, textEvent("a", "3", "", now)); err == nil {
		t.Fatal("point read served expired row")
	}
	rows, err := db.QueryByPK(td.Name, td.KeySchema, q.PKValue, 0)
	if err != nil || len(rows) != 3 {
		t.Fatalf("primary query TTL: rows=%d err=%v", len(rows), err)
	}
	rows, err = db.ScanTable(td.Name, 0)
	if err != nil || len(rows) != 4 {
		t.Fatalf("primary scan TTL: rows=%d err=%v", len(rows), err)
	}
}

func TestNativeTextPostingsPersistAndFollowAllMutations(t *testing.T) {
	path := t.TempDir()
	td := textTable()
	db, err := Open(Options{Path: path, FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour).Unix()
	item := textEvent("a", "1", "Original needle", future)
	if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(Options{Path: path, FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := types.TextQuery{IndexName: "message", PKValue: item["tenantId"], Search: "needle"}
	page, err := db.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("restart needs no rebuild: rows=%d err=%v", len(page.Items), err)
	}
	item["message"] = types.AttributeValue{T: types.AttrS, S: "changed"}
	if err := db.BatchWriteItem(td, []BatchOp{{Op: BatchOpPut, Item: item}}); err != nil {
		t.Fatal(err)
	}
	page, err = db.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("old posting: rows=%d err=%v", len(page.Items), err)
	}
	_, err = db.AtomicUpdate(td, types.Item{"tenantId": item["tenantId"], "orderKey": item["orderKey"]}, AtomicOptions{Actions: []AtomicAction{{Kind: AtomicActionSet, Attribute: "message", Value: types.AttributeValue{T: types.AttrS, S: "atomic needle"}}}})
	if err != nil {
		t.Fatal(err)
	}
	page, err = db.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("atomic posting: rows=%d err=%v", len(page.Items), err)
	}
	if err := db.DeleteItemWith(td, item, DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	page, err = db.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("deleted posting: rows=%d err=%v", len(page.Items), err)
	}
}
