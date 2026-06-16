package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/internal/api"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestHTTPClusterStatusIncludesRangeHotspots(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
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

func TestHTTPClusterStatusIncludesBackupScheduler(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := api.New(db, cat)
	srv.AttachBackupScheduler(pebble.NewScheduledBackupRunner(db, pebble.ScheduledBackupConfig{
		Enabled:      true,
		DryRun:       true,
		Interval:     time.Minute,
		NameTemplate: "http-{{unix}}",
		Tables:       []string{"Users"},
	}))
	mux := http.NewServeMux()
	srv.Routes(mux)

	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		BackupScheduler *pebble.ScheduledBackupStatus `json:"backupScheduler"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.BackupScheduler == nil || !body.BackupScheduler.Enabled || body.BackupScheduler.NameTemplate != "http-{{unix}}" || len(body.BackupScheduler.Tables) != 1 {
		t.Fatalf("backup scheduler status = %+v", body.BackupScheduler)
	}
}
