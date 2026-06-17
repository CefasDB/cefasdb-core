package item_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	itemhttp "github.com/CefasDb/cefasdb/internal/server/http/item"
	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/ddbjson"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// localWriteTargets is the single-shard test stand-in for
// routedWriteTargets. The item package uses it through the
// itemhttp.WriteTargets interface so the tests exercise the real Put /
// Delete code paths against a real pebble.DB.
type localWriteTargets struct{ db *pebble.DB }

func (t localWriteTargets) PutItemWith(td types.TableDescriptor, item types.Item, opts pebble.PutOptions) error {
	return t.db.PutItemWith(td, item, opts)
}

func (t localWriteTargets) DeleteItemWith(td types.TableDescriptor, key types.Item, opts pebble.DeleteOptions) error {
	return t.db.DeleteItemWith(td, key, opts)
}

func (t localWriteTargets) Release() {}

func newHandlers(t *testing.T) (*itemhttp.Handlers, *pebble.DB, *catalog.Catalog, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	deps := itemhttp.Deps{
		Cat:        cat,
		StorageFor: func(_ []byte) *pebble.DB { return db },
		WriteTargetsForPK: func(_ []byte) (itemhttp.WriteTargets, error) {
			return localWriteTargets{db: db}, nil
		},
		BatchWriteByShard: func(td types.TableDescriptor, ops []pebble.BatchOp) error {
			return db.BatchWriteItem(td, ops)
		},
		BatchGetByShard: func(table string, ks types.KeySchema, keys []types.Item) ([]types.Item, error) {
			return db.BatchGetItem(table, ks, keys)
		},
		EnsureStrongRead: func(_ http.ResponseWriter, _ *http.Request) bool { return true },
		WriteWriteErr: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		},
		ObserveWrite: func(_ []byte, _ types.Item, _ time.Time) {},
		ObserveRead:  func(_ []byte, _ types.Item, _ time.Time) {},
	}
	cleanup := func() { _ = db.Close() }
	return itemhttp.New(deps), db, cat, cleanup
}

func mustCreateTable(t *testing.T, cat *catalog.Catalog, name, pk string) {
	t.Helper()
	if err := cat.Create(types.TableDescriptor{
		Name:      name,
		KeySchema: types.KeySchema{PK: pk},
	}); err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}
}

func postJSON(t *testing.T, h func(http.ResponseWriter, *http.Request), path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func TestHandlePutItemGetItemRoundTrip(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()
	mustCreateTable(t, cat, "events", "id")

	rec := postJSON(t, h.HandlePutItem, "/v1/PutItem", `{
		"table": "events",
		"item": {"id": {"S": "evt-1"}, "name": {"S": "alpha"}}
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("PutItem status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	rec = postJSON(t, h.HandleGetItem, "/v1/GetItem", `{
		"table": "events",
		"key": {"id": {"S": "evt-1"}}
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetItem status = %d, want 200", rec.Code)
	}
	var got struct {
		Found bool                         `json:"found"`
		Item  map[string]ddbjson.Attribute `json:"item"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Found {
		t.Fatalf("found = false, want true")
	}
	if got.Item["name"].S == nil || *got.Item["name"].S != "alpha" {
		t.Fatalf("item['name'] = %+v, want alpha", got.Item["name"])
	}
}

func TestHandleDeleteItemThenGetReturnsNotFound(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()
	mustCreateTable(t, cat, "events", "id")

	if rec := postJSON(t, h.HandlePutItem, "/v1/PutItem", `{
		"table": "events",
		"item": {"id": {"S": "evt-x"}}
	}`); rec.Code != http.StatusOK {
		t.Fatalf("PutItem = %d", rec.Code)
	}
	if rec := postJSON(t, h.HandleDeleteItem, "/v1/DeleteItem", `{
		"table": "events",
		"key": {"id": {"S": "evt-x"}}
	}`); rec.Code != http.StatusOK {
		t.Fatalf("DeleteItem = %d", rec.Code)
	}
	rec := postJSON(t, h.HandleGetItem, "/v1/GetItem", `{
		"table": "events",
		"key": {"id": {"S": "evt-x"}}
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("GetItem after delete = %d, want 200", rec.Code)
	}
	var got struct {
		Found bool `json:"found"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Found {
		t.Fatalf("found = true after delete, want false")
	}
}

func TestHandleBatchWriteItemMixedPutDelete(t *testing.T) {
	t.Parallel()
	h, db, cat, cleanup := newHandlers(t)
	defer cleanup()
	mustCreateTable(t, cat, "events", "id")

	// Seed an item that the batch will delete.
	if err := db.PutItemWith(types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}},
		types.Item{"id": types.AttributeValue{T: types.AttrS, S: "old"}}, pebble.PutOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rec := postJSON(t, h.HandleBatchWriteItem, "/v1/BatchWriteItem", `{
		"table": "events",
		"ops": [
			{"op": "put",    "item": {"id": {"S": "new-1"}, "v": {"N": "1"}}},
			{"op": "put",    "item": {"id": {"S": "new-2"}, "v": {"N": "2"}}},
			{"op": "delete", "key":  {"id": {"S": "old"}}}
		]
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("BatchWrite status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// Confirm via a real GetItem that the deletes + puts landed.
	for _, want := range []struct {
		id    string
		found bool
	}{
		{"new-1", true},
		{"new-2", true},
		{"old", false},
	} {
		rec := postJSON(t, h.HandleGetItem, "/v1/GetItem", `{
			"table": "events",
			"key": {"id": {"S": "`+want.id+`"}}
		}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("GetItem %s = %d", want.id, rec.Code)
		}
		var got struct {
			Found bool `json:"found"`
		}
		_ = json.NewDecoder(rec.Body).Decode(&got)
		if got.Found != want.found {
			t.Fatalf("GetItem %s found = %v, want %v", want.id, got.Found, want.found)
		}
	}
}

func TestHandleBatchGetItemMultipleKeys(t *testing.T) {
	t.Parallel()
	h, db, cat, cleanup := newHandlers(t)
	defer cleanup()
	mustCreateTable(t, cat, "events", "id")

	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	for _, v := range []string{"a", "b", "c"} {
		if err := db.PutItemWith(td, types.Item{
			"id":   types.AttributeValue{T: types.AttrS, S: v},
			"slot": types.AttributeValue{T: types.AttrS, S: v + "-slot"},
		}, pebble.PutOptions{}); err != nil {
			t.Fatalf("seed %s: %v", v, err)
		}
	}

	rec := postJSON(t, h.HandleBatchGetItem, "/v1/BatchGetItem", `{
		"table": "events",
		"keys": [
			{"id": {"S": "a"}},
			{"id": {"S": "missing"}},
			{"id": {"S": "c"}}
		]
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("BatchGet status = %d", rec.Code)
	}
	var got struct {
		Items []map[string]ddbjson.Attribute `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Items) != 3 {
		t.Fatalf("len(items) = %d, want 3", len(got.Items))
	}
	// Items are non-nil for the keys that exist and nil for misses.
	var present []string
	for _, it := range got.Items {
		if it == nil {
			continue
		}
		if it["id"].S == nil {
			continue
		}
		present = append(present, *it["id"].S)
	}
	sort.Strings(present)
	if len(present) != 2 || present[0] != "a" || present[1] != "c" {
		t.Fatalf("present ids = %v, want [a c]", present)
	}
}

func TestHandlePutItemMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()
	req := httptest.NewRequest(http.MethodGet, "/v1/PutItem", nil)
	rec := httptest.NewRecorder()
	h.HandlePutItem(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHandlePutItemBadJSON(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()
	rec := postJSON(t, h.HandlePutItem, "/v1/PutItem", `{not-json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandleGetItemMissingKey(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()
	mustCreateTable(t, cat, "events", "id")

	// Body has the table but no `key` object — pkBytesFromItem hits the
	// missing-key branch and the handler returns 400.
	rec := postJSON(t, h.HandleGetItem, "/v1/GetItem", `{"table": "events", "key": {}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleDeleteItemTableNotFound(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()
	rec := postJSON(t, h.HandleDeleteItem, "/v1/DeleteItem", `{
		"table": "ghost",
		"key": {"id": {"S": "x"}}
	}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestHandleBatchWriteItemUnknownOp(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()
	mustCreateTable(t, cat, "events", "id")

	rec := postJSON(t, h.HandleBatchWriteItem, "/v1/BatchWriteItem", `{
		"table": "events",
		"ops": [{"op": "update", "item": {"id": {"S": "x"}}}]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleBatchGetItemBadKey(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()
	mustCreateTable(t, cat, "events", "id")

	// "S" is a string attribute, but we send a number; ddbjson.DecodeItem
	// rejects the mismatch.
	rec := postJSON(t, h.HandleBatchGetItem, "/v1/BatchGetItem", `{
		"table": "events",
		"keys": [{"id": {"S": "ok"}}, {"id": {"S": 42}}]
	}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}
