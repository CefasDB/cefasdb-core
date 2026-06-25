package manager

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/pkg/client"
)

type fakeCefas struct {
	status      client.ClusterStatus
	audit       placement.PlacementAuditReport
	statusErr   error
	auditErr    error
	planResult  client.PlacementPlan
	applyResult client.PlacementApplyResult
	planCalls   int
	applyCalls  int
}

func (f *fakeCefas) Status(context.Context) (client.ClusterStatus, error) {
	return f.status, f.statusErr
}

func (f *fakeCefas) AuditPlacement(context.Context, placement.PlacementAuditRequest) (placement.PlacementAuditReport, error) {
	return f.audit, f.auditErr
}

func (f *fakeCefas) PlanPlacement(context.Context, client.PlacementPlanRequest) (client.PlacementPlan, error) {
	f.planCalls++
	return f.planResult, nil
}

func (f *fakeCefas) ApplyPlacement(context.Context, client.PlacementApplyRequest) (client.PlacementApplyResult, error) {
	f.applyCalls++
	return f.applyResult, nil
}

type fakeKube struct {
	snap KubernetesSnapshot
	err  error
}

func (f fakeKube) Snapshot(context.Context, KubeSnapshotOptions) (KubernetesSnapshot, error) {
	return f.snap, f.err
}

func TestDoctorClassifiesKubernetesAndPlacementFailures(t *testing.T) {
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	fc := &fakeCefas{
		status: healthyStatus(),
		audit: placement.PlacementAuditReport{
			ConsistencyVerdict: "fail",
			Issues: []placement.PlacementAuditIssue{{
				Kind:     placement.PlacementAuditKindGap,
				Severity: placement.PlacementAuditSeverityError,
				Detail:   "gap",
			}},
			RepairPlan: &placement.PlacementRepairPlan{Actions: []placement.PlacementRepairAction{{Action: "review_placement_gap", Detail: "review gap"}}},
		},
	}
	report, err := Doctor(context.Background(), DoctorOptions{
		Cefas: fc,
		Kubernetes: fakeKube{snap: KubernetesSnapshot{
			Namespace: "default",
			Pods: []Pod{{
				Metadata: ObjectMeta{Name: "cefas-0"},
				Status: PodStatus{
					Phase:      "Running",
					Conditions: []PodCondition{{Type: "Ready", Status: "False", Reason: "ContainersNotReady"}},
					ContainerStatuses: []ContainerStatus{{
						Name:  "cefas",
						State: ContainerState{Waiting: &ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "crashed"}},
					}},
				},
			}},
			Nodes:     []Node{{Metadata: ObjectMeta{Name: "m1"}, Status: NodeStatus{Conditions: []NodeCondition{{Type: "Ready", Status: "True"}}}}},
			Endpoints: []Endpoint{{Metadata: ObjectMeta{Name: "cefas"}, Subsets: []EndpointSubset{{Addresses: []EndpointAddress{{IP: "10.0.0.1"}}}}}},
			PVCs:      []PersistentVolumeClaim{{Metadata: ObjectMeta{Name: "data-cefas-0"}, Status: PVCStatus{Phase: "Bound"}}},
		}},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Classification != HealthUnsafe {
		t.Fatalf("classification = %s, want %s", report.Classification, HealthUnsafe)
	}
	if !hasSignal(report, "kubernetes", "CrashLoopBackOff") {
		t.Fatalf("report did not include crashloop signal: %+v", report.Signals)
	}
	if !hasSignal(report, "cefas", placement.PlacementAuditKindGap) {
		t.Fatalf("report did not include placement gap signal: %+v", report.Signals)
	}
}

func TestDryRunPlansDrainWithoutMutation(t *testing.T) {
	fc := &fakeCefas{status: drainingStatus(), audit: passAudit()}
	report, err := Doctor(context.Background(), DoctorOptions{Cefas: fc})
	if err != nil {
		t.Fatal(err)
	}
	result := DryRunRepair(RepairOptions{Report: report})
	if len(result.Plan.Actions) != 1 {
		t.Fatalf("actions = %+v, want one drain action", result.Plan.Actions)
	}
	if got := result.Plan.Actions[0].Type; got != "placement_drain" {
		t.Fatalf("action type = %s, want placement_drain", got)
	}
	if fc.planCalls != 0 || fc.applyCalls != 0 {
		t.Fatalf("dry-run mutated: planCalls=%d applyCalls=%d", fc.planCalls, fc.applyCalls)
	}
}

func TestExecuteRequiresLeaderElection(t *testing.T) {
	fc := &fakeCefas{status: drainingStatus(), audit: passAudit()}
	report, err := Doctor(context.Background(), DoctorOptions{Cefas: fc})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExecuteRepair(context.Background(), RepairOptions{Cefas: fc, Report: report})
	if err == nil || !strings.Contains(err.Error(), "manager_leader_election") {
		t.Fatalf("error = %v, want leader election precondition failure", err)
	}
	if fc.planCalls != 0 || fc.applyCalls != 0 {
		t.Fatalf("execute mutated before preconditions passed: planCalls=%d applyCalls=%d", fc.planCalls, fc.applyCalls)
	}
}

func TestExecuteAppliesSupportedDrain(t *testing.T) {
	fc := &fakeCefas{
		status: drainingStatus(),
		audit:  passAudit(),
		planResult: client.PlacementPlan{
			Operation:      "drain",
			BeforeEpoch:    7,
			AfterEpoch:     8,
			ApplySupported: true,
		},
		applyResult: client.PlacementApplyResult{Operation: "drain", BeforeEpoch: 7, AfterEpoch: 8},
	}
	report, err := Doctor(context.Background(), DoctorOptions{Cefas: fc})
	if err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteRepair(context.Background(), RepairOptions{
		Cefas:  fc,
		Report: report,
		Leader: LeaderLease{Acquired: true, Holder: "manager-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fc.planCalls != 1 || fc.applyCalls != 1 {
		t.Fatalf("calls = plan:%d apply:%d, want 1/1", fc.planCalls, fc.applyCalls)
	}
	if len(result.Applied) != 1 || result.Applied[0].Status != "applied" {
		t.Fatalf("applied = %+v", result.Applied)
	}
}

func TestKubeLeaderElectorAcquiresAndRespectsHolder(t *testing.T) {
	now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	var stored Lease
	exists := false
	mux := http.NewServeMux()
	mux.HandleFunc("/apis/coordination.k8s.io/v1/namespaces/default/leases/cefas-manager", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if !exists {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(stored)
		case http.MethodPut:
			if err := json.NewDecoder(r.Body).Decode(&stored); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			exists = true
			_ = json.NewEncoder(w).Encode(stored)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/apis/coordination.k8s.io/v1/namespaces/default/leases", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&stored); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		exists = true
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(stored)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	kube, err := NewHTTPKubeClient(server.URL, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	kube.now = func() time.Time { return now }
	first := &KubeLeaderElector{Client: kube, Opts: LeaderElectionOptions{Namespace: "default", Name: "cefas-manager", HolderID: "manager-1", TTL: 30 * time.Second}}
	lease, err := first.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Acquired || lease.Holder != "manager-1" {
		t.Fatalf("first lease = %+v", lease)
	}
	second := &KubeLeaderElector{Client: kube, Opts: LeaderElectionOptions{Namespace: "default", Name: "cefas-manager", HolderID: "manager-2", TTL: 30 * time.Second}}
	lease, err = second.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lease.Acquired || lease.Holder != "manager-1" {
		t.Fatalf("second lease before expiry = %+v", lease)
	}
	kube.now = func() time.Time { return now.Add(31 * time.Second) }
	lease, err = second.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Acquired || lease.Holder != "manager-2" {
		t.Fatalf("second lease after expiry = %+v", lease)
	}
}

func TestDoctorHandlesUnreachableCluster(t *testing.T) {
	report, err := Doctor(context.Background(), DoctorOptions{Cefas: &fakeCefas{statusErr: errors.New("down")}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Classification != HealthUnsafe {
		t.Fatalf("classification = %s, want unsafe", report.Classification)
	}
}

func healthyStatus() client.ClusterStatus {
	return client.ClusterStatus{
		Mode:       "raft",
		ShardCount: 1,
		Shards:     []client.ShardPlacement{{ID: 0, State: "active", Voters: []string{"n1", "n2", "n3"}}},
		Nodes: []client.NodeDescriptor{
			{ID: "n1", State: "active"},
			{ID: "n2", State: "active"},
			{ID: "n3", State: "active"},
		},
	}
}

func drainingStatus() client.ClusterStatus {
	st := healthyStatus()
	st.Nodes[0].State = "draining"
	st.Shards[0].Voters = []string{"n1", "n2", "n3"}
	return st
}

func passAudit() placement.PlacementAuditReport {
	return placement.PlacementAuditReport{ConsistencyVerdict: "pass", RepairPlan: &placement.PlacementRepairPlan{}}
}

func hasSignal(report DoctorReport, component, statusPart string) bool {
	for _, sig := range report.Signals {
		if sig.Component == component && (strings.Contains(sig.Status, statusPart) || strings.Contains(sig.Name, statusPart) || strings.Contains(sig.Detail, statusPart)) {
			return true
		}
	}
	return false
}
