package api_test

import (
	"bytes"
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
	mux, cleanup := placementTestMux(t)
	defer cleanup()

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

func TestHTTPPlacementApplyNoopMove(t *testing.T) {
	mux, cleanup := placementTestMux(t)
	defer cleanup()

	planBody := strings.NewReader(`{"operation":"move","shardId":0,"targetVoters":["n1"],"minVoters":1}`)
	planReq := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/plan", planBody)
	planRec := httptest.NewRecorder()
	mux.ServeHTTP(planRec, planReq)
	if planRec.Code != http.StatusOK {
		t.Fatalf("plan status = %d body=%s", planRec.Code, planRec.Body.String())
	}
	var plan cluster.PlacementPlan
	if err := json.NewDecoder(planRec.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	rawApply, err := json.Marshal(cluster.PlacementApplyRequest{Plan: plan, ExpectedEpoch: plan.BeforeEpoch})
	if err != nil {
		t.Fatal(err)
	}
	applyReq := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/apply", bytes.NewReader(rawApply))
	applyRec := httptest.NewRecorder()
	mux.ServeHTTP(applyRec, applyReq)
	if applyRec.Code != http.StatusOK {
		t.Fatalf("apply status = %d body=%s", applyRec.Code, applyRec.Body.String())
	}
	var result cluster.PlacementApplyResult
	if err := json.NewDecoder(applyRec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch {
		t.Fatalf("after epoch = %d, want %d", result.AfterEpoch, plan.AfterEpoch)
	}
}

func placementTestMux(t *testing.T) (*http.ServeMux, func()) {
	t.Helper()
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:   t.TempDir(),
		Shards: 1,
		SelfID: "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
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
	return mux, func() { _ = mgr.Close() }
}
