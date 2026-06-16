package rebalance

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/metrics"
)

type fakePlanner struct {
	cat        cluster.PlacementCatalog
	planCalls  int
	applyCalls int
}

func (f *fakePlanner) RefreshPlacement() error { return nil }
func (f *fakePlanner) Placement() cluster.PlacementCatalog {
	return f.cat.Clone()
}
func (f *fakePlanner) PlanPlacement(req cluster.PlacementPlanRequest) (cluster.PlacementPlan, error) {
	f.planCalls++
	return cluster.BuildPlacementPlan(f.cat, req)
}
func (f *fakePlanner) ApplyPlacement(_ context.Context, req cluster.PlacementApplyRequest) (cluster.PlacementApplyResult, error) {
	f.applyCalls++
	f.cat = req.Plan.After.Clone()
	return cluster.PlacementApplyResult{
		Operation:   req.Plan.Operation,
		BeforeEpoch: req.Plan.BeforeEpoch,
		AfterEpoch:  req.Plan.AfterEpoch,
		Placement:   req.Plan.After.Clone(),
	}, nil
}

type fakeHotspots []metrics.RangeHotspotSummary

func (f fakeHotspots) RangeHotspotSummaries(max int) []metrics.RangeHotspotSummary {
	out := append([]metrics.RangeHotspotSummary(nil), f...)
	if max > 0 && len(out) > max {
		return out[:max]
	}
	return out
}

func TestTickProposesDeterministicSplitWithReason(t *testing.T) {
	planner := &fakePlanner{cat: testCatalog()}
	ctrl := NewController(Config{Mode: ModeDryRun, MaxHotspots: 4, MinInterval: time.Second}, planner, fakeHotspots{
		hotspot("0", 1, 10, 200, 4096),
	}, nil)
	ctrl.now = fixedNow

	decisions, err := ctrl.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 1 {
		t.Fatalf("decisions len = %d, want 1: %+v", len(decisions), decisions)
	}
	d := decisions[0]
	if d.Status != "planned" || d.Plan.Operation != cluster.PlacementOperationSplit {
		t.Fatalf("decision = %+v, want planned split", d)
	}
	if !strings.Contains(d.Reason, "hot range shard=0 bucket=1") || !strings.Contains(d.Reason, "split owning shard") {
		t.Fatalf("reason = %q", d.Reason)
	}
	if planner.applyCalls != 0 {
		t.Fatalf("dry-run applied %d times", planner.applyCalls)
	}
}

func TestBuildCandidatesDropsConflictingHotspotsPerShard(t *testing.T) {
	candidates := BuildCandidates(testCatalog(), []metrics.RangeHotspotSummary{
		hotspot("0", 1, 10, 50, 1024),
		hotspot("0", 2, 10, 500, 2048),
	}, Config{MaxHotspots: 8})

	if len(candidates) != 1 {
		t.Fatalf("candidates len = %d, want 1: %+v", len(candidates), candidates)
	}
	if candidates[0].HotRange.Bucket != 2 {
		t.Fatalf("selected bucket = %d, want highest-priority bucket 2", candidates[0].HotRange.Bucket)
	}
}

func TestTickManualWritesPlanWithoutApplying(t *testing.T) {
	planner := &fakePlanner{cat: testCatalog()}
	dir := t.TempDir()
	ctrl := NewController(Config{
		Mode:          ModeManual,
		ManualPlanDir: dir,
		MinInterval:   time.Second,
	}, planner, fakeHotspots{hotspot("0", 1, 10, 200, 4096)}, nil)
	ctrl.now = fixedNow

	decisions, err := ctrl.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 1 || decisions[0].Status != "written" || decisions[0].WrittenPath == "" {
		t.Fatalf("decisions = %+v, want written plan", decisions)
	}
	if planner.applyCalls != 0 {
		t.Fatalf("manual mode applied %d times", planner.applyCalls)
	}
	raw, err := os.ReadFile(decisions[0].WrittenPath)
	if err != nil {
		t.Fatal(err)
	}
	var written Decision
	if err := json.Unmarshal(raw, &written); err != nil {
		t.Fatal(err)
	}
	if written.Plan.Operation != cluster.PlacementOperationSplit || written.Status != "planned" {
		t.Fatalf("written decision = %+v", written)
	}
}

func TestTickAutoRespectsBudgetExhaustion(t *testing.T) {
	planner := &fakePlanner{cat: testCatalog()}
	ctrl := NewController(Config{
		Mode:                    ModeAuto,
		MaxConcurrentOperations: 1,
		MinInterval:             time.Second,
	}, planner, fakeHotspots{hotspot("0", 1, 10, 200, 4096)}, nil)
	ctrl.inFlight = 1

	decisions, err := ctrl.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 1 || decisions[0].Status != "budget_exhausted" {
		t.Fatalf("decisions = %+v, want budget_exhausted", decisions)
	}
	if planner.planCalls != 0 || planner.applyCalls != 0 {
		t.Fatalf("budget exhausted should not plan/apply: plan=%d apply=%d", planner.planCalls, planner.applyCalls)
	}
}

func TestBuildCandidatesIncludesDrainAndRangeMove(t *testing.T) {
	cat := testCatalog()
	n1 := cat.Nodes["n1"]
	n1.State = cluster.NodeStateDraining
	cat.Nodes["n1"] = n1
	drain := BuildCandidates(cat, nil, Config{})
	if len(drain) != 1 || drain[0].Operation != cluster.PlacementOperationDrain {
		t.Fatalf("drain candidates = %+v", drain)
	}

	multiRange := testCatalog()
	multiRange.Shards[0].Ranges = []cluster.TokenRange{
		{Start: 0, End: 1 << 63},
		{Start: 1 << 63, End: 0},
	}
	move := BuildCandidates(multiRange, []metrics.RangeHotspotSummary{
		hotspot("0", 0, 0, 100, 1024),
	}, Config{})
	if len(move) != 1 || move[0].Operation != cluster.PlacementOperationRangeMove {
		t.Fatalf("range move candidates = %+v", move)
	}
}

func TestTickAutoAppliesOnePlan(t *testing.T) {
	planner := &fakePlanner{cat: testCatalog()}
	ctrl := NewController(Config{
		Mode:        ModeAuto,
		MinInterval: time.Second,
	}, planner, fakeHotspots{hotspot("0", 1, 10, 200, 4096)}, nil)
	ctrl.now = fixedNow

	decisions, err := ctrl.Tick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 1 || decisions[0].Status != "applied" {
		t.Fatalf("decisions = %+v, want applied", decisions)
	}
	if planner.applyCalls != 1 {
		t.Fatalf("applyCalls = %d, want 1", planner.applyCalls)
	}
}

func testCatalog() cluster.PlacementCatalog {
	return cluster.PlacementCatalog{
		Version:  1,
		Epoch:    1,
		Strategy: cluster.PlacementStrategyTokenRange,
		Shards: []cluster.ShardPlacement{{
			ID:     0,
			State:  cluster.ShardStateActive,
			Epoch:  1,
			Ranges: []cluster.TokenRange{{Start: 0, End: 0}},
			Voters: []string{"n1", "n2", "n3"},
		}},
		Nodes: map[string]cluster.NodeDescriptor{
			"n1": {ID: "n1", RaftAddr: "127.0.0.1:9101", State: cluster.NodeStateActive, Capacity: cluster.NodeCapacity{Weight: 1, Zone: "az-a"}},
			"n2": {ID: "n2", RaftAddr: "127.0.0.1:9102", State: cluster.NodeStateActive, Capacity: cluster.NodeCapacity{Weight: 1, Zone: "az-b"}},
			"n3": {ID: "n3", RaftAddr: "127.0.0.1:9103", State: cluster.NodeStateActive, Capacity: cluster.NodeCapacity{Weight: 1, Zone: "az-c"}},
			"n4": {ID: "n4", RaftAddr: "127.0.0.1:9104", State: cluster.NodeStateActive, Capacity: cluster.NodeCapacity{Weight: 2, Zone: "az-d"}},
		},
	}
}

func hotspot(shard string, bucket int, reads, writes, bytes uint64) metrics.RangeHotspotSummary {
	bucketCount := 4
	start := uint64(bucket) * (1 << 62)
	end := start + (1 << 62)
	if bucket == bucketCount-1 {
		end = 0
	}
	return metrics.RangeHotspotSummary{
		ShardID:     shard,
		Bucket:      bucket,
		BucketCount: bucketCount,
		TokenStart:  start,
		TokenEnd:    end,
		Reads:       reads,
		Writes:      writes,
		Bytes:       bytes,
		Status:      "hot",
		Reasons:     []string{"write_threshold"},
	}
}

func fixedNow() time.Time {
	return time.Date(2026, 6, 11, 5, 45, 0, 0, time.UTC)
}
