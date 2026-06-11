package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/api"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestHTTPClusterStatusIncludesRangeHotspots(t *testing.T) {
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.Create(types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}); err != nil {
		t.Fatal(err)
	}
	prom := metrics.NewWithRangeHotspots(metrics.RangeHotspotConfig{
		Buckets:                 8,
		Window:                  time.Minute,
		CoolingWindow:           time.Minute,
		WriteThreshold:          1,
		ReadThreshold:           100,
		BytesThreshold:          1 << 20,
		LatencyThresholdSeconds: 1,
	})
	srv := api.New(db, cat)
	srv.AttachMetrics(prom)
	mux := http.NewServeMux()
	srv.Routes(mux)

	put := httptest.NewRequest(http.MethodPost, "/v1/PutItem", strings.NewReader(`{"table":"events","item":{"id":{"S":"hot-key"},"v":{"S":"payload"}}}`))
	putRec := httptest.NewRecorder()
	mux.ServeHTTP(putRec, put)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d body=%s", putRec.Code, putRec.Body.String())
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	statusRec := httptest.NewRecorder()
	mux.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", statusRec.Code, statusRec.Body.String())
	}
	var body struct {
		HotRanges []metrics.RangeHotspotSummary `json:"hotRanges"`
	}
	if err := json.NewDecoder(statusRec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.HotRanges) != 1 {
		t.Fatalf("hotRanges len = %d, want 1: %+v", len(body.HotRanges), body.HotRanges)
	}
	if body.HotRanges[0].ShardID != "0" || body.HotRanges[0].Writes != 1 || body.HotRanges[0].Status != "hot" {
		t.Fatalf("unexpected hot range: %+v", body.HotRanges[0])
	}
}
