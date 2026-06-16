package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/placement"
	"github.com/osvaldoandrade/cefas/internal/api"
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
	var plan placement.PlacementPlan
	if err := json.NewDecoder(rec.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if plan.Operation != placement.PlacementOperationSplit || len(plan.After.Shards) != 2 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}

func TestHTTPPlacementPlanRangeMove(t *testing.T) {
	mux, cleanup := placementTestMux(t)
	defer cleanup()

	body := strings.NewReader(`{"operation":"range_move","shardId":0,"rangeStart":0,"rangeEnd":9223372036854775808}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/plan", body)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var plan placement.PlacementPlan
	if err := json.NewDecoder(rec.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if plan.Operation != placement.PlacementOperationRangeMove || len(plan.After.Shards) != 2 || plan.After.Shards[0].State != placement.ShardStateMoving {
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
	var plan placement.PlacementPlan
	if err := json.NewDecoder(planRec.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	rawApply, err := json.Marshal(placement.PlacementApplyRequest{Plan: plan, ExpectedEpoch: plan.BeforeEpoch})
	if err != nil {
		t.Fatal(err)
	}
	applyReq := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/apply", bytes.NewReader(rawApply))
	applyRec := httptest.NewRecorder()
	mux.ServeHTTP(applyRec, applyReq)
	if applyRec.Code != http.StatusOK {
		t.Fatalf("apply status = %d body=%s", applyRec.Code, applyRec.Body.String())
	}
	var result placement.PlacementApplyResult
	if err := json.NewDecoder(applyRec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch {
		t.Fatalf("after epoch = %d, want %d", result.AfterEpoch, plan.AfterEpoch)
	}
}

func TestHTTPPlacementAuditCleanReport(t *testing.T) {
	mux, cleanup := placementTestMux(t)
	defer cleanup()

	body := strings.NewReader(`{"maxPrimaryKeysPerShard":8,"includeRepairPlan":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/audit", body)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var report placement.PlacementAuditReport
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if report.ConsistencyVerdict != "pass" || len(report.Issues) != 0 {
		t.Fatalf("unexpected audit report: %+v", report)
	}
	if report.RepairPlan == nil || report.RepairPlan.ApplySupported {
		t.Fatalf("repair plan = %+v, want review-only plan", report.RepairPlan)
	}
}

func TestHTTPFinalizeSplit(t *testing.T) {
	mux, cleanup, plan := transitionPlacementTestMux(t)
	defer cleanup()

	raw, err := json.Marshal(cluster.SplitFinalizeRequest{
		ParentShardID:  0,
		ChildShardID:   1,
		ExpectedEpoch:  plan.AfterEpoch,
		WritesQuiesced: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/split/finalize", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var result cluster.SplitFinalizeResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch+1 || len(result.Placement.Shards) != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHTTPRollbackSplit(t *testing.T) {
	mux, cleanup, plan := transitionPlacementTestMux(t)
	defer cleanup()

	raw, err := json.Marshal(cluster.SplitRollbackRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/split/rollback", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var result cluster.SplitRollbackResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch+1 || result.Phase != string(cluster.SplitFinalizePhaseRolledBack) {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHTTPFinalizeRangeMove(t *testing.T) {
	mux, cleanup, plan := transitionRangeMovePlacementTestMux(t)
	defer cleanup()

	raw, err := json.Marshal(cluster.RangeMoveFinalizeRequest{
		SourceShardID: 0,
		TargetShardID: 1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/range-move/finalize", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var result cluster.RangeMoveFinalizeResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch+1 || len(result.Placement.Shards) != 2 || result.Placement.Shards[1].State != placement.ShardStateActive {
		t.Fatalf("unexpected result: %+v", result)
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

func transitionPlacementTestMux(t *testing.T) (*http.ServeMux, func(), placement.PlacementPlan) {
	t.Helper()
	env := transitionPlacementTestEnv(t)
	return env.Mux, env.Cleanup, env.Plan
}

func transitionRangeMovePlacementTestMux(t *testing.T) (*http.ServeMux, func(), placement.PlacementPlan) {
	t.Helper()
	env := transitionRangeMovePlacementTestEnv(t)
	return env.Mux, env.Cleanup, env.Plan
}

type transitionPlacementEnv struct {
	Mux     *http.ServeMux
	Manager *cluster.Manager
	Catalog *catalog.Catalog
	Plan    placement.PlacementPlan
	Cleanup func()
}

func transitionRangeMovePlacementTestEnv(t *testing.T) transitionPlacementEnv {
	t.Helper()
	root := t.TempDir()
	cat := placement.DefaultPlacement(1, "n1", nil, nil, placement.NodeCapacity{}, placement.PlacementStrategyTokenRange)
	start := uint64(0)
	end := uint64(1) << 63
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation:  placement.PlacementOperationRangeMove,
		ShardID:    0,
		RangeStart: &start,
		RangeEnd:   &end,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := placement.SavePlacementFile(filepath.Join(root, "placement.json"), plan.After); err != nil {
		t.Fatal(err)
	}
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:   root,
		Shards: 2,
		SelfID: "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
	shard0, ok := mgr.Shard(0)
	if !ok {
		t.Fatal("missing shard 0")
	}
	catStore, err := catalog.New(shard0.Storage)
	if err != nil {
		t.Fatal(err)
	}
	srv := api.New(shard0.Storage, catStore)
	srv.AttachManager(mgr)
	mux := http.NewServeMux()
	srv.Routes(mux)
	return transitionPlacementEnv{
		Mux:     mux,
		Manager: mgr,
		Catalog: catStore,
		Plan:    plan,
		Cleanup: func() { _ = mgr.Close() },
	}
}

func transitionPlacementTestEnv(t *testing.T) transitionPlacementEnv {
	t.Helper()
	root := t.TempDir()
	cat := placement.DefaultPlacement(1, "n1", nil, nil, placement.NodeCapacity{}, placement.PlacementStrategyTokenRange)
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := placement.SavePlacementFile(filepath.Join(root, "placement.json"), plan.After); err != nil {
		t.Fatal(err)
	}
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:   root,
		Shards: 2,
		SelfID: "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
	shard0, ok := mgr.Shard(0)
	if !ok {
		t.Fatal("missing shard 0")
	}
	catStore, err := catalog.New(shard0.Storage)
	if err != nil {
		t.Fatal(err)
	}
	srv := api.New(shard0.Storage, catStore)
	srv.AttachManager(mgr)
	mux := http.NewServeMux()
	srv.Routes(mux)
	return transitionPlacementEnv{
		Mux:     mux,
		Manager: mgr,
		Catalog: catStore,
		Plan:    plan,
		Cleanup: func() { _ = mgr.Close() },
	}
}
