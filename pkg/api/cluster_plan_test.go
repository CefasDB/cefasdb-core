package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/pkg/api"
)

func TestHTTPPlacementPlanSplit(t *testing.T) {
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:   t.TempDir(),
		Shards: 1,
		SelfID: "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	shard0, ok := mgr.Shard(0)
	if !ok {
		t.Fatal("missing shard 0")
	}
	cat, err := catalog.New(shard0.Storage)
	if err != nil {
		t.Fatal(err)
	}
	srv := api.New(shard0.Storage, cat)
	srv.AttachManager(mgr)
	mux := http.NewServeMux()
	srv.Routes(mux)

	body := strings.NewReader(`{"operation":"split","shardId":0}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/plan", body)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var plan cluster.PlacementPlan
	if err := json.NewDecoder(rec.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if plan.Operation != cluster.PlacementOperationSplit || len(plan.After.Shards) != 2 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}
