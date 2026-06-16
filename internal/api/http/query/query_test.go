package query_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	queryhttp "github.com/osvaldoandrade/cefas/internal/api/http/query"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// newHandlers spins up a tempdir-backed pebble.DB + catalog and wraps
// them in a *Handlers configured for single-node behaviour (no shard
// fan-out, no leader gating, no metrics).
func newHandlers(t *testing.T) (*queryhttp.Handlers, *pebble.DB, *catalog.Catalog, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("catalog: %v", err)
	}
	storageFor := func(pkBytes []byte) *pebble.DB { return db }
	allShards := func() []*pebble.DB { return []*pebble.DB{db} }
	spatialAllShards := func(td types.TableDescriptor, idxName string, q pebble.SpatialQuery) ([]types.Item, error) {
		return db.SpatialQueryItems(td, idxName, q)
	}
	ensureStrong := func(http.ResponseWriter, *http.Request) bool { return true }
	h := queryhttp.New(cat, db, storageFor, allShards, spatialAllShards, ensureStrong, nil)
	return h, db, cat, func() { _ = db.Close() }
}

func sAttr(s string) types.AttributeValue { return types.AttributeValue{T: types.AttrS, S: s} }
func nAttr(n string) types.AttributeValue { return types.AttributeValue{T: types.AttrN, N: n} }

func mustCreateTable(t *testing.T, cat *catalog.Catalog, td types.TableDescriptor) {
	t.Helper()
	if err := cat.Create(td); err != nil {
		t.Fatalf("create table %s: %v", td.Name, err)
	}
}

func TestHandleQueryByPKHappyPath(t *testing.T) {
	t.Parallel()
	h, db, cat, cleanup := newHandlers(t)
	defer cleanup()

	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	mustCreateTable(t, cat, td)
	if err := db.PutItem("events", td.KeySchema, types.Item{
		"id":      sAttr("alice"),
		"payload": sAttr("hello"),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	body := bytes.NewBufferString(`{"table":"events","pkValue":{"S":"alice"}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/Query", body)
	rec := httptest.NewRecorder()
	h.HandleQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Items []map[string]map[string]any `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("got %d items, want 1: %+v", len(got.Items), got.Items)
	}
	if s, _ := got.Items[0]["id"]["S"].(string); s != "alice" {
		t.Fatalf("unexpected id: %+v", got.Items[0]["id"])
	}
}

func TestHandleQueryUnknownTable(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	body := bytes.NewBufferString(`{"table":"ghost","pkValue":{"S":"x"}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/Query", body)
	rec := httptest.NewRecorder()
	h.HandleQuery(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleSpatialQueryBBoxHappyPath(t *testing.T) {
	t.Parallel()
	h, db, cat, cleanup := newHandlers(t)
	defer cleanup()

	td := types.TableDescriptor{
		Name:      "places",
		KeySchema: types.KeySchema{PK: "id"},
		SpatialIndexes: []types.SpatialIndexDescriptor{{
			Name:       "by_location",
			Kind:       pebble.SpatialKindGeohash,
			Attributes: []string{"lat", "lon"},
			Precision:  6,
		}},
	}
	mustCreateTable(t, cat, td)

	put := func(id string, lat, lon float64) {
		t.Helper()
		if err := db.PutItemWith(td, types.Item{
			"id":  sAttr(id),
			"lat": nAttr(fmt.Sprintf("%.6f", lat)),
			"lon": nAttr(fmt.Sprintf("%.6f", lon)),
		}, pebble.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	put("nyc", 40.7128, -74.0060)
	put("brooklyn", 40.6782, -73.9442)
	put("la", 34.0522, -118.2437)

	body := bytes.NewBufferString(`{
		"table": "places",
		"indexName": "by_location",
		"bbox": {"minLat": 40.6, "minLon": -74.1, "maxLat": 40.8, "maxLon": -73.9}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/SpatialQuery", body)
	rec := httptest.NewRecorder()
	h.HandleSpatialQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		Items []map[string]map[string]any `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	ids := map[string]bool{}
	for _, it := range got.Items {
		if s, _ := it["id"]["S"].(string); s != "" {
			ids[s] = true
		}
	}
	if !ids["nyc"] || !ids["brooklyn"] {
		t.Fatalf("expected nyc+brooklyn in bbox, got %v", ids)
	}
	if ids["la"] {
		t.Fatalf("west coast leaked into bbox: %v", ids)
	}
}

func TestHandleSpatialQueryMissingShape(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()

	mustCreateTable(t, cat, types.TableDescriptor{
		Name:      "places",
		KeySchema: types.KeySchema{PK: "id"},
		SpatialIndexes: []types.SpatialIndexDescriptor{{
			Name:       "by_location",
			Kind:       pebble.SpatialKindGeohash,
			Attributes: []string{"lat", "lon"},
			Precision:  6,
		}},
	})

	body := bytes.NewBufferString(`{"table":"places","indexName":"by_location"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/SpatialQuery", body)
	rec := httptest.NewRecorder()
	h.HandleSpatialQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleSqlSelectHappyPath(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	// CREATE + INSERT via the same SQL handler so the test is
	// end-to-end on the resource being refactored.
	for _, q := range []string{
		`CREATE TABLE t (PRIMARY KEY (id))`,
		`INSERT INTO t (id, v) VALUES ('a', '1')`,
	} {
		doSQL(t, h, q, http.StatusOK)
	}

	rec := doSQL(t, h, `SELECT * FROM t WHERE id = 'a'`, http.StatusOK)
	var got struct {
		AffectedRows int                         `json:"affectedRows"`
		Rows         []map[string]map[string]any `json:"rows"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Rows) != 1 {
		t.Fatalf("got %d rows, want 1: %+v", len(got.Rows), got.Rows)
	}
	if s, _ := got.Rows[0]["id"]["S"].(string); s != "a" {
		t.Fatalf("unexpected id row: %+v", got.Rows[0])
	}
}

func TestHandleSqlParseError(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	rec := doSQL(t, h, `NOT VALID SQL`, http.StatusBadRequest)
	if rec.Body.Len() == 0 {
		t.Fatalf("expected error body")
	}
}

func TestHandlePartiQLHappyPath(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	doSQL(t, h, `CREATE TABLE t (PRIMARY KEY (id))`, http.StatusOK)

	body := bytes.NewBufferString(`{
		"Statement": "INSERT INTO t (id, v) VALUES (?, ?)",
		"Parameters": [{"S": "k1"}, {"S": "v1"}]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/PartiQL", body)
	rec := httptest.NewRecorder()
	h.HandlePartiQL(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("insert status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	rec = doSQL(t, h, `SELECT * FROM t WHERE id = 'k1'`, http.StatusOK)
	var got struct {
		Rows []map[string]map[string]any `json:"rows"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Rows) != 1 {
		t.Fatalf("got %d rows, want 1: %+v", len(got.Rows), got.Rows)
	}
}

func TestHandlePartiQLBadBinding(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	// Mismatched arity: 1 marker, zero parameters.
	body := bytes.NewBufferString(`{"Statement":"SELECT * FROM t WHERE id = ?"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/PartiQL", body)
	rec := httptest.NewRecorder()
	h.HandlePartiQL(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleQueryMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/Query", nil)
	rec := httptest.NewRecorder()
	h.HandleQuery(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

// doSQL is the helper used by the SQL-shaped tests: POST a JSON body
// against HandleSql, assert status, return the recorder for body
// inspection.
func doSQL(t *testing.T, h *queryhttp.Handlers, query string, wantStatus int) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/Sql", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.HandleSql(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; query=%q body=%s", rec.Code, wantStatus, query, rec.Body.String())
	}
	return rec
}
