package table_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	tablehttp "github.com/osvaldoandrade/cefas/internal/server/http/table"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func newHandlers(t *testing.T) (*tablehttp.Handlers, *pebble.DB, *catalog.Catalog, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	return tablehttp.New(cat, nil), db, cat, func() { _ = db.Close() }
}

func TestHandleTablesCreateRoundTrip(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()

	body := bytes.NewBufferString(`{
		"name": "events",
		"keySchema": {"pk": "id"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/tables", body)
	rec := httptest.NewRecorder()
	h.HandleTables(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var got types.TableDescriptor
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != "events" || got.KeySchema.PK != "id" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if _, err := cat.Describe("events"); err != nil {
		t.Fatalf("descriptor not persisted: %v", err)
	}
}

func TestHandleTablesCreateConflict(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	post := func() *httptest.ResponseRecorder {
		body := bytes.NewBufferString(`{"name":"dupe","keySchema":{"pk":"id"}}`)
		req := httptest.NewRequest(http.MethodPost, "/v1/tables", body)
		rec := httptest.NewRecorder()
		h.HandleTables(rec, req)
		return rec
	}
	if rec := post(); rec.Code != http.StatusCreated {
		t.Fatalf("first POST = %d, want 201", rec.Code)
	}
	if rec := post(); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate POST = %d, want 409; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleTablesList(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()

	if err := cat.Create(types.TableDescriptor{Name: "a", KeySchema: types.KeySchema{PK: "id"}}); err != nil {
		t.Fatal(err)
	}
	if err := cat.Create(types.TableDescriptor{Name: "b", KeySchema: types.KeySchema{PK: "id"}}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tables", nil)
	rec := httptest.NewRecorder()
	h.HandleTables(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []types.TableDescriptor
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	names := make([]string, len(got))
	for i, td := range got {
		names[i] = td.Name
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("got names %v, want [a b]", names)
	}
}

func TestHandleTableDescribeNotFound(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/tables/ghost", nil)
	rec := httptest.NewRecorder()
	h.HandleTable(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleTableDescribeFound(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()

	if err := cat.Create(types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/tables/events", nil)
	rec := httptest.NewRecorder()
	h.HandleTable(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got types.TableDescriptor
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != "events" {
		t.Fatalf("got %s, want events", got.Name)
	}
}

func TestHandleTablesMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPut, "/v1/tables", nil)
	rec := httptest.NewRecorder()
	h.HandleTables(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHandleTableEmptyName(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/tables/", nil)
	rec := httptest.NewRecorder()
	h.HandleTable(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestHandleTablesCreateFansOut(t *testing.T) {
	t.Parallel()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}

	var fanned types.TableDescriptor
	h := tablehttp.New(cat, func(td types.TableDescriptor) { fanned = td })

	body := bytes.NewBufferString(`{"name":"events","keySchema":{"pk":"id"}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/tables", body)
	rec := httptest.NewRecorder()
	h.HandleTables(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	if fanned.Name != "events" {
		t.Fatalf("fanOut never called; got %+v", fanned)
	}
}
