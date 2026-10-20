package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/auth"
	identitymw "github.com/codecompany/identity-middleware/pkg/identitymiddleware"

	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

func TestTextStorageBootstrapPreservesNativeSchema(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	New(db, cat).Routes(mux)
	r := httptest.NewRecorder()
	mux.ServeHTTP(r, httptest.NewRequest(http.MethodPost, "/v1/tables", strings.NewReader(`{"name":"logs-events","keySchema":{"pk":"tenantId","sk":"orderKey"},"ttlAttribute":"expiresAt","textIndexes":[{"name":"message","field":"message"}]}`)))
	if r.Code != http.StatusCreated {
		t.Fatalf("create: status %d", r.Code)
	}
	var descriptor map[string]any
	if err := json.Unmarshal(r.Body.Bytes(), &descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor["ttlAttribute"] != "expiresAt" {
		t.Errorf("TTL schema was ignored")
	}
	indexes, _ := descriptor["textIndexes"].([]any)
	if len(indexes) != 1 {
		t.Errorf("native text schema was ignored")
	}
}

func TestNativeTextHTTPRejectsForeignScopeAndInvalidShape(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	New(db, cat).Routes(mux)
	body := `{"table":"logs","indexName":"message","pkValue":{"S":"tenant-a"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/TextQuery", strings.NewReader(body))
	req = req.WithContext(identitymw.WithClaims(req.Context(), &auth.Claims{Scopes: []string{"cefas:query:other"}}))
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != http.StatusForbidden {
		t.Fatalf("foreign scope accepted: %d", response.Code)
	}
	for _, body := range []string{`{"table":"logs","unknown":true}`, `{} {}`, `{"table":"logs","pkValue":{"NULL":false}}`} {
		response = httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/TextQuery", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid request returned %d", response.Code)
		}
	}
}
