package cluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clusterhttp "github.com/osvaldoandrade/cefas/internal/api/http/cluster"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/placement"
)

// stubCluster is the minimal Cluster surface the package needs. It
// records calls so happy-path tests can assert the handler routed to
// the cluster surface; failing variants return errCanned to drive the
// error branches.
type stubCluster struct {
	leader        bool
	selfID        string
	bindAddr      string
	leaderHTTP    string
	addVoterErr   error
	removeSrvErr  error
	addVoterCalls int
	removeCalls   int
}

func (s *stubCluster) IsLeader() bool         { return s.leader }
func (s *stubCluster) LeaderHTTPAddr() string { return s.leaderHTTP }
func (s *stubCluster) SelfID() string         { return s.selfID }
func (s *stubCluster) BindAddr() string       { return s.bindAddr }
func (s *stubCluster) AddVoter(id, addr string, timeout time.Duration) error {
	s.addVoterCalls++
	return s.addVoterErr
}
func (s *stubCluster) RemoveServer(id string, timeout time.Duration) error {
	s.removeCalls++
	return s.removeSrvErr
}

// rawWriteErr is the trivial WriteErrFunc — it writes the error string
// with status 500. The package keeps the leader-aware redirect logic
// in the host server; tests don't need it.
func rawWriteErr(w http.ResponseWriter, r *http.Request, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func TestStatusSingleNode(t *testing.T) {
	t.Parallel()
	h := clusterhttp.New(nil, nil, rawWriteErr, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()
	h.HandleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["mode"] != "single-node" {
		t.Fatalf("mode = %v, want single-node", got["mode"])
	}
	if got["isLeader"] != false {
		t.Fatalf("isLeader = %v, want false", got["isLeader"])
	}
}

func TestStatusRaftLeader(t *testing.T) {
	t.Parallel()
	cls := &stubCluster{leader: true, selfID: "n1", bindAddr: "127.0.0.1:9000", leaderHTTP: "http://127.0.0.1:8080"}
	h := clusterhttp.New(cls, nil, rawWriteErr, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()
	h.HandleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["mode"] != "raft" {
		t.Fatalf("mode = %v, want raft", got["mode"])
	}
	if got["isLeader"] != true {
		t.Fatalf("isLeader = %v, want true", got["isLeader"])
	}
	if got["selfId"] != "n1" {
		t.Fatalf("selfId = %v", got["selfId"])
	}
	if got["leaderHttp"] != "http://127.0.0.1:8080" {
		t.Fatalf("leaderHttp = %v", got["leaderHttp"])
	}
}

func TestStatusRaftFollower(t *testing.T) {
	t.Parallel()
	cls := &stubCluster{leader: false, selfID: "n2"}
	h := clusterhttp.New(cls, nil, rawWriteErr, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil)
	rec := httptest.NewRecorder()
	h.HandleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["isLeader"] != false {
		t.Fatalf("isLeader = %v, want false", got["isLeader"])
	}
}

func TestStatusExtras(t *testing.T) {
	t.Parallel()
	extras := func() map[string]any {
		return map[string]any{
			"hotRanges":       []map[string]any{{"table": "events"}},
			"backupScheduler": map[string]any{"state": "idle"},
		}
	}
	h := clusterhttp.New(nil, nil, rawWriteErr, extras)

	rec := httptest.NewRecorder()
	h.HandleStatus(rec, httptest.NewRequest(http.MethodGet, "/v1/cluster/status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["hotRanges"] == nil {
		t.Fatalf("hotRanges missing in %+v", got)
	}
	if got["backupScheduler"] == nil {
		t.Fatalf("backupScheduler missing in %+v", got)
	}
}

func TestAddVoterHappyPath(t *testing.T) {
	t.Parallel()
	cls := &stubCluster{leader: true}
	h := clusterhttp.New(cls, nil, rawWriteErr, nil)

	body := strings.NewReader(`{"id":"n2","addr":"127.0.0.1:9001","timeoutMs":250}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/AddVoter", body)
	rec := httptest.NewRecorder()
	h.HandleAddVoter(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if cls.addVoterCalls != 1 {
		t.Fatalf("AddVoter calls = %d, want 1", cls.addVoterCalls)
	}
}

func TestAddVoterNoClusterReturns400(t *testing.T) {
	t.Parallel()
	h := clusterhttp.New(nil, nil, rawWriteErr, nil)

	body := strings.NewReader(`{"id":"n2","addr":"127.0.0.1:9001"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/AddVoter", body)
	rec := httptest.NewRecorder()
	h.HandleAddVoter(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestAddVoterBadJSONReturns400(t *testing.T) {
	t.Parallel()
	cls := &stubCluster{leader: true}
	h := clusterhttp.New(cls, nil, rawWriteErr, nil)

	body := strings.NewReader(`{not-json`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/AddVoter", body)
	rec := httptest.NewRecorder()
	h.HandleAddVoter(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestAddVoterMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h := clusterhttp.New(&stubCluster{}, nil, rawWriteErr, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/AddVoter", nil)
	rec := httptest.NewRecorder()
	h.HandleAddVoter(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestAddVoterClusterErrorDelegatesToWriteErr(t *testing.T) {
	t.Parallel()
	cls := &stubCluster{leader: true, addVoterErr: errors.New("boom")}
	h := clusterhttp.New(cls, nil, rawWriteErr, nil)

	body := strings.NewReader(`{"id":"n2","addr":"127.0.0.1:9001"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/AddVoter", body)
	rec := httptest.NewRecorder()
	h.HandleAddVoter(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestRemoveServerHappyPath(t *testing.T) {
	t.Parallel()
	cls := &stubCluster{leader: true}
	h := clusterhttp.New(cls, nil, rawWriteErr, nil)

	body := strings.NewReader(`{"id":"n2","timeoutMs":250}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/RemoveServer", body)
	rec := httptest.NewRecorder()
	h.HandleRemoveServer(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if cls.removeCalls != 1 {
		t.Fatalf("RemoveServer calls = %d, want 1", cls.removeCalls)
	}
}

func TestRemoveServerNoClusterReturns400(t *testing.T) {
	t.Parallel()
	h := clusterhttp.New(nil, nil, rawWriteErr, nil)

	body := strings.NewReader(`{"id":"n2"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/RemoveServer", body)
	rec := httptest.NewRecorder()
	h.HandleRemoveServer(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestPlacementHandlersNoManagerReturn400(t *testing.T) {
	t.Parallel()
	h := clusterhttp.New(nil, nil, rawWriteErr, nil)

	for name, fn := range map[string]http.HandlerFunc{
		"plan":       h.HandlePlacementPlan,
		"apply":      h.HandlePlacementApply,
		"audit":      h.HandlePlacementAudit,
		"split-fin":  h.HandleSplitFinalize,
		"split-rb":   h.HandleSplitRollback,
		"range-move": h.HandleRangeMoveFinalize,
	} {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		fn(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", name, rec.Code)
		}
	}
}

func TestPlacementPlanSplit(t *testing.T) {
	t.Parallel()
	h, cleanup := managerHandlers(t)
	defer cleanup()

	body := strings.NewReader(`{"operation":"split","shardId":0}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/plan", body)
	rec := httptest.NewRecorder()
	h.HandlePlacementPlan(rec, req)

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

func TestPlacementApplyHappyPath(t *testing.T) {
	t.Parallel()
	h, cleanup := managerHandlers(t)
	defer cleanup()

	planBody := strings.NewReader(`{"operation":"move","shardId":0,"targetVoters":["n1"],"minVoters":1}`)
	planReq := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/plan", planBody)
	planRec := httptest.NewRecorder()
	h.HandlePlacementPlan(planRec, planReq)
	if planRec.Code != http.StatusOK {
		t.Fatalf("plan status = %d body=%s", planRec.Code, planRec.Body.String())
	}
	var plan placement.PlacementPlan
	if err := json.NewDecoder(planRec.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(placement.PlacementApplyRequest{Plan: plan, ExpectedEpoch: plan.BeforeEpoch})
	if err != nil {
		t.Fatal(err)
	}
	applyReq := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/apply", strings.NewReader(string(raw)))
	applyRec := httptest.NewRecorder()
	h.HandlePlacementApply(applyRec, applyReq)
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

func TestPlacementAuditHappyPath(t *testing.T) {
	t.Parallel()
	h, cleanup := managerHandlers(t)
	defer cleanup()

	body := strings.NewReader(`{"maxPrimaryKeysPerShard":8,"includeRepairPlan":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/placement/audit", body)
	rec := httptest.NewRecorder()
	h.HandlePlacementAudit(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var report placement.PlacementAuditReport
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if report.ConsistencyVerdict != "pass" {
		t.Fatalf("verdict = %q, want pass", report.ConsistencyVerdict)
	}
}

func TestPlacementAuditQueryStringParseError(t *testing.T) {
	t.Parallel()
	h, cleanup := managerHandlers(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/cluster/placement/audit?maxIssues=oops", nil)
	rec := httptest.NewRecorder()
	h.HandlePlacementAudit(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", rec.Code, rec.Body.String())
	}
}

func managerHandlers(t *testing.T) (*clusterhttp.Handlers, func()) {
	t.Helper()
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:   t.TempDir(),
		Shards: 1,
		SelfID: "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return clusterhttp.New(nil, mgr, rawWriteErr, nil), func() { _ = mgr.Close() }
}
