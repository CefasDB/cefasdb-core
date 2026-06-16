package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/placement"
	"github.com/osvaldoandrade/cefas/internal/routing"
	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/internal/testutil/wait"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func plannerCatalog(shards int) placement.PlacementCatalog {
	cat := placement.DefaultPlacement(
		shards,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
			"n3": "127.0.0.1:9103",
		},
		map[string]string{
			"n1": "http://127.0.0.1:8081",
			"n2": "http://127.0.0.1:8082",
			"n3": "http://127.0.0.1:8083",
		},
		placement.NodeCapacity{},
		placement.PlacementStrategyTokenRange,
	)
	cat.Nodes["n4"] = placement.NodeDescriptor{ID: "n4", RaftAddr: "127.0.0.1:9104", HTTPAddr: "http://127.0.0.1:8084", State: placement.NodeStateActive}
	return cat
}

func TestPlanSplitCreatesSafeTransition(t *testing.T) {
	cat := plannerCatalog(1)
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.RequiresDataCopy || plan.RequiresRestart || !plan.ApplySupported {
		t.Fatalf("unexpected split flags: %+v", plan)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].Action != "create_shard" {
		t.Fatalf("unexpected split steps: %+v", plan.Steps)
	}
	if len(plan.After.Shards) != 2 {
		t.Fatalf("after shard count = %d, want 2", len(plan.After.Shards))
	}
	if plan.After.Shards[0].State != placement.ShardStateSplitting {
		t.Fatalf("parent state = %s", plan.After.Shards[0].State)
	}
	if plan.After.Shards[1].State != placement.ShardStateCreating {
		t.Fatalf("child state = %s", plan.After.Shards[1].State)
	}
	if len(plan.After.Shards[0].Ranges) != 1 || plan.After.Shards[0].Ranges[0] != cat.Shards[0].Ranges[0] {
		t.Fatalf("parent range changed before activation: %+v", plan.After.Shards[0].Ranges)
	}
	if err := placement.ValidatePlacement(plan.After); err != nil {
		t.Fatalf("planned placement invalid: %v", err)
	}
}

func TestPlanMoveReplacesVoter(t *testing.T) {
	cat := plannerCatalog(2)
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation:  placement.PlacementOperationMove,
		ShardID:    0,
		SourceNode: "n1",
		TargetNode: "n4",
		MinVoters:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := plan.After.Shards[0].Voters
	if contains(got, "n1") || !contains(got, "n4") || len(got) != 3 {
		t.Fatalf("voters = %v, want n1 replaced by n4", got)
	}
	if plan.After.Shards[0].State != placement.ShardStateActive {
		t.Fatalf("state = %s", plan.After.Shards[0].State)
	}
	if plan.RequiresDataCopy || plan.RequiresRestart || !plan.ApplySupported || len(plan.Steps) != 3 {
		t.Fatalf("unexpected move plan: %+v", plan)
	}
}

func TestPlanRangeMoveCreatesSafeTransition(t *testing.T) {
	cat := plannerCatalog(1)
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
	if !plan.RequiresDataCopy || plan.RequiresRestart || !plan.ApplySupported {
		t.Fatalf("unexpected range move flags: %+v", plan)
	}
	if len(plan.After.Shards) != 2 {
		t.Fatalf("after shard count = %d, want 2", len(plan.After.Shards))
	}
	if plan.After.Shards[0].State != placement.ShardStateMoving {
		t.Fatalf("source state = %s", plan.After.Shards[0].State)
	}
	if plan.After.Shards[1].State != placement.ShardStateCreating {
		t.Fatalf("target state = %s", plan.After.Shards[1].State)
	}
	if len(plan.After.Shards[1].Ranges) != 1 || plan.After.Shards[1].Ranges[0] != (placement.TokenRange{Start: start, End: end}) {
		t.Fatalf("target range = %+v", plan.After.Shards[1].Ranges)
	}
	if len(plan.Steps) != 6 || plan.Steps[0].Action != "create_shard" || plan.Steps[1].Action != "target_membership" || plan.Steps[2].Action != "copy_range" || plan.Steps[4].Action != "publish_cutover" {
		t.Fatalf("unexpected range move steps: %+v", plan.Steps)
	}
	if err := placement.ValidatePlacement(plan.After); err != nil {
		t.Fatalf("planned placement invalid: %v", err)
	}
	router, err := routing.NewRouterFromCatalog(plan.After)
	if err != nil {
		t.Fatal(err)
	}
	key := keyInRange(t, plan.After.Shards[1].Ranges[0])
	got, err := router.ShardForPK([]byte(key))
	if err != nil {
		t.Fatalf("ShardForPK returned error: %v", err)
	}
	if got != 0 {
		t.Fatalf("transition route = %d, want source shard 0", got)
	}
}

func TestPlanSplitPolicyAvoidsDrainingAndSpreadsZones(t *testing.T) {
	cat := plannerCatalog(1)
	n1 := cat.Nodes["n1"]
	n1.State = placement.NodeStateDraining
	n1.Capacity = placement.NodeCapacity{Weight: 10, Zone: "az-draining"}
	cat.Nodes["n1"] = n1
	n2 := cat.Nodes["n2"]
	n2.Capacity = placement.NodeCapacity{Weight: 1, Zone: "az-a"}
	cat.Nodes["n2"] = n2
	n3 := cat.Nodes["n3"]
	n3.Capacity = placement.NodeCapacity{Weight: 1, Zone: "az-b"}
	cat.Nodes["n3"] = n3
	n4 := cat.Nodes["n4"]
	n4.Capacity = placement.NodeCapacity{Weight: 1, Zone: "az-c"}
	cat.Nodes["n4"] = n4

	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	childVoters := plan.After.Shards[1].Voters
	if contains(childVoters, "n1") || len(childVoters) != 3 {
		t.Fatalf("child voters = %v, want three active voters without n1", childVoters)
	}
	for _, want := range []string{"n2", "n3", "n4"} {
		if !contains(childVoters, want) {
			t.Fatalf("child voters = %v, missing %s", childVoters, want)
		}
	}
	if !containsWarning(plan.Warnings, "placement policy applied zone anti-affinity") {
		t.Fatalf("missing zone anti-affinity warning: %v", plan.Warnings)
	}
	if !containsWarning(plan.Warnings, "placement policy ignored inactive nodes: n1=draining") {
		t.Fatalf("missing inactive-node warning: %v", plan.Warnings)
	}
}

func TestPlanRangeMovePolicyPrefersCapacitySkew(t *testing.T) {
	cat := plannerCatalog(1)
	cat.Shards[0].Voters = []string{"n1"}
	n1 := cat.Nodes["n1"]
	n1.Capacity = placement.NodeCapacity{Weight: 1, Zone: "az-a"}
	cat.Nodes["n1"] = n1
	n2 := cat.Nodes["n2"]
	n2.Capacity = placement.NodeCapacity{
		Weight:      5,
		CPU:         32,
		MemoryBytes: 64 << 30,
		DiskBytes:   1024 << 30,
		Zone:        "az-b",
		Tags:        []string{"ssd"},
	}
	cat.Nodes["n2"] = n2
	n3 := cat.Nodes["n3"]
	n3.Capacity = placement.NodeCapacity{Weight: 1, Zone: "az-c"}
	cat.Nodes["n3"] = n3

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
	got := plan.After.Shards[1].Voters
	if len(got) != 1 || got[0] != "n2" {
		t.Fatalf("target voters = %v, want n2", got)
	}
	if !containsWarning(plan.Warnings, "n2(score=") {
		t.Fatalf("missing selected-node explanation: %v", plan.Warnings)
	}
}

func TestPlanSplitPolicyMissingCapacityIsDeterministic(t *testing.T) {
	cat := plannerCatalog(1)
	cat.Shards[0].Voters = []string{"n1", "n2"}
	for id, node := range cat.Nodes {
		node.Capacity = placement.NodeCapacity{}
		cat.Nodes[id] = node
	}

	plan1, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan2, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	got1 := strings.Join(plan1.After.Shards[1].Voters, ",")
	got2 := strings.Join(plan2.After.Shards[1].Voters, ",")
	if got1 != got2 {
		t.Fatalf("target voters are not deterministic: first=%s second=%s", got1, got2)
	}
	if len(plan1.After.Shards[1].Voters) != 2 {
		t.Fatalf("target voters = %v, want two voters", plan1.After.Shards[1].Voters)
	}
	if !containsWarning(plan1.Warnings, "memoryGiB=0") {
		t.Fatalf("missing missing-capacity explanation: %v", plan1.Warnings)
	}
}

func TestPlanSplitPolicyRejectsInsufficientActiveNodes(t *testing.T) {
	cat := plannerCatalog(1)
	cat.Shards[0].Voters = []string{"n1", "n2"}
	n2 := cat.Nodes["n2"]
	n2.State = placement.NodeStateDraining
	cat.Nodes["n2"] = n2
	n3 := cat.Nodes["n3"]
	n3.State = placement.NodeStateDecommissioned
	cat.Nodes["n3"] = n3
	n4 := cat.Nodes["n4"]
	n4.State = placement.NodeStateDecommissioned
	cat.Nodes["n4"] = n4

	_, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if !errors.Is(err, placement.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
	if !strings.Contains(err.Error(), "placement policy found 1 active nodes; need 2 voters") {
		t.Fatalf("error = %v, want active-node count", err)
	}
}

func TestPlanDrainUsesPlacementPolicyWithoutTargets(t *testing.T) {
	cat := plannerCatalog(1)
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationDrain,
		NodeID:    "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := plan.After.Shards[0].Voters
	if contains(got, "n1") || !contains(got, "n4") || len(got) != 3 {
		t.Fatalf("voters = %v, want n1 replaced by n4", got)
	}
	if len(plan.Steps) != 3 || plan.Steps[0].Action != "add_voter" || plan.Steps[1].Action != "wait_catchup" || plan.Steps[2].Action != "remove_voter" {
		t.Fatalf("unexpected drain steps: %+v", plan.Steps)
	}
	if !containsWarning(plan.Warnings, "no target nodes supplied; placement policy selected replacement voters") {
		t.Fatalf("missing auto-target warning: %v", plan.Warnings)
	}
	if !containsWarning(plan.Warnings, "shard 0: placement policy selected target voters") {
		t.Fatalf("missing shard policy warning: %v", plan.Warnings)
	}
}

func TestPlanDrainRemovesNodeFromEveryShard(t *testing.T) {
	cat := plannerCatalog(3)
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation:   placement.PlacementOperationDrain,
		NodeID:      "n1",
		TargetNodes: []string{"n4"},
		MinVoters:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.After.Nodes["n1"].State != placement.NodeStateDraining {
		t.Fatalf("node state = %s", plan.After.Nodes["n1"].State)
	}
	for _, sh := range plan.After.Shards {
		if contains(sh.Voters, "n1") {
			t.Fatalf("shard %d still has n1 voter: %v", sh.ID, sh.Voters)
		}
		if !contains(sh.Voters, "n4") {
			t.Fatalf("shard %d missing replacement n4: %v", sh.ID, sh.Voters)
		}
		if sh.State != placement.ShardStateActive {
			t.Fatalf("shard %d state = %s", sh.ID, sh.State)
		}
	}
	if !plan.ApplySupported {
		t.Fatalf("drain should be apply-supported")
	}
}

func TestPlanDrainClearsLeaderHintBlocker(t *testing.T) {
	cat := plannerCatalog(1)
	cat.Shards[0].LeaderHint = "n1"
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation:   placement.PlacementOperationDrain,
		NodeID:      "n1",
		TargetNodes: []string{"n4"},
		MinVoters:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.After.Shards[0].LeaderHint != "" {
		t.Fatalf("leader hint = %q, want cleared", plan.After.Shards[0].LeaderHint)
	}
}

func TestPlanDecommissionRejectsActiveReferences(t *testing.T) {
	cat := plannerCatalog(1)
	n1 := cat.Nodes["n1"]
	n1.State = placement.NodeStateDraining
	cat.Nodes["n1"] = n1

	_, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationDecommission,
		NodeID:    "n1",
	})
	if !errors.Is(err, placement.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
	if !strings.Contains(err.Error(), "still has active placement references") || !strings.Contains(err.Error(), "shard 0 voter") {
		t.Fatalf("error = %v, want active reference blocker", err)
	}
}

func TestPlanDecommissionAfterDrainMarksNode(t *testing.T) {
	cat := plannerCatalog(2)
	drain, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation:   placement.PlacementOperationDrain,
		NodeID:      "n1",
		TargetNodes: []string{"n4"},
		MinVoters:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := placement.BuildPlacementPlan(drain.After, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationDecommission,
		NodeID:    "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.After.Nodes["n1"].State != placement.NodeStateDecommissioned {
		t.Fatalf("node state = %s", plan.After.Nodes["n1"].State)
	}
	if len(plan.Steps) != 4 || plan.Steps[0].Action != "verify_no_active_references" || plan.Steps[3].Action != "decommission_node" {
		t.Fatalf("unexpected decommission steps: %+v", plan.Steps)
	}
	for i, sh := range drain.After.Shards {
		got := plan.After.Shards[i]
		if !sameStrings(got.Voters, sh.Voters) || !sameStrings(got.NonVoters, sh.NonVoters) || got.LeaderHint != sh.LeaderHint || got.State != sh.State {
			t.Fatalf("shard %d changed during decommission: before=%+v after=%+v", sh.ID, sh, got)
		}
	}
}

func TestApplyPlacementDecommissionsDrainedNode(t *testing.T) {
	root := t.TempDir()
	cat := plannerCatalog(1)
	n1 := cat.Nodes["n1"]
	n1.State = placement.NodeStateDraining
	cat.Nodes["n1"] = n1
	cat.Shards[0].Voters = []string{"n2", "n3", "n4"}
	if err := placement.SavePlacementFile(filepath.Join(root, "placement.json"), cat); err != nil {
		t.Fatal(err)
	}
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	plan, err := mgr.PlanPlacement(placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationDecommission,
		NodeID:    "n1",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := mgr.ApplyPlacement(context.Background(), placement.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Placement.Nodes["n1"].State != placement.NodeStateDecommissioned || mgr.Placement().Nodes["n1"].State != placement.NodeStateDecommissioned {
		t.Fatalf("node not decommissioned: result=%s manager=%s", result.Placement.Nodes["n1"].State, mgr.Placement().Nodes["n1"].State)
	}
	if len(result.Steps) != 4 || result.Steps[1].Action != "cleanup_unowned_data" || result.Steps[1].Status != "ok" {
		t.Fatalf("unexpected apply steps: %+v", result.Steps)
	}
}

func TestDecommissionedNodeExcludedFromFuturePlacement(t *testing.T) {
	cat := plannerCatalog(1)
	n1 := cat.Nodes["n1"]
	n1.State = placement.NodeStateDecommissioned
	n1.Capacity = placement.NodeCapacity{Weight: 100, Zone: "az-a"}
	cat.Nodes["n1"] = n1
	cat.Shards[0].Voters = []string{"n2", "n3", "n4"}

	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
		MinVoters: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if contains(plan.After.Shards[1].Voters, "n1") {
		t.Fatalf("decommissioned node selected as voter: %v", plan.After.Shards[1].Voters)
	}
	if !containsWarning(plan.Warnings, "n1=decommissioned") {
		t.Fatalf("missing decommissioned exclusion warning: %v", plan.Warnings)
	}
}

func TestPlanSplitRejectsLegacyModuloPlacement(t *testing.T) {
	cat := placement.DefaultPlacement(2, "n1", map[string]string{"n1": "127.0.0.1:9101"}, nil, placement.NodeCapacity{}, placement.PlacementStrategyLegacyModulo)
	_, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if !errors.Is(err, placement.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
}

func TestApplyPlacementPublishesNoopMove(t *testing.T) {
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      t.TempDir(),
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	plan, err := mgr.PlanPlacement(placement.PlacementPlanRequest{
		Operation:    placement.PlacementOperationMove,
		ShardID:      0,
		TargetVoters: []string{"n1"},
		MinVoters:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 0 {
		t.Fatalf("expected no membership steps, got %+v", plan.Steps)
	}
	result, err := mgr.ApplyPlacement(context.Background(), placement.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch || mgr.RoutingEpoch() != plan.AfterEpoch {
		t.Fatalf("epoch not advanced: result=%d manager=%d plan=%d", result.AfterEpoch, mgr.RoutingEpoch(), plan.AfterEpoch)
	}
}

func TestApplyPlacementPreparesSplitOnline(t *testing.T) {
	root := t.TempDir()
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	plan, err := mgr.PlanPlacement(placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := mgr.ApplyPlacement(context.Background(), placement.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch || mgr.RoutingEpoch() != plan.AfterEpoch {
		t.Fatalf("epoch not advanced: result=%d manager=%d plan=%d", result.AfterEpoch, mgr.RoutingEpoch(), plan.AfterEpoch)
	}
	if len(mgr.Shards()) != 2 {
		t.Fatalf("open shards = %d, want 2", len(mgr.Shards()))
	}
	child, ok := mgr.Shard(1)
	if !ok || child.State != placement.ShardStateCreating {
		t.Fatalf("child shard not creating: %#v", child)
	}
	if _, err := os.Stat(filepath.Join(root, "shards", "1", "state")); err != nil {
		t.Fatalf("child state dir not created: %v", err)
	}
	key := keyInRange(t, plan.After.Shards[1].Ranges[0])
	got, err := mgr.Router().ShardForPK([]byte(key))
	if err != nil {
		t.Fatalf("ShardForPK returned error: %v", err)
	}
	if got != 0 {
		t.Fatalf("transition route = %d, want parent shard 0", got)
	}

	retry, err := mgr.ApplyPlacement(context.Background(), placement.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(retry.Steps) != 1 || retry.Steps[0].Status != "already_applied" {
		t.Fatalf("unexpected retry result: %+v", retry)
	}
}

func TestApplyPlacementPreparesRangeMoveOnline(t *testing.T) {
	root := t.TempDir()
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	start := uint64(0)
	end := uint64(1) << 63
	plan, err := mgr.PlanPlacement(placement.PlacementPlanRequest{
		Operation:  placement.PlacementOperationRangeMove,
		ShardID:    0,
		RangeStart: &start,
		RangeEnd:   &end,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := mgr.ApplyPlacement(context.Background(), placement.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch || mgr.RoutingEpoch() != plan.AfterEpoch {
		t.Fatalf("epoch not advanced: result=%d manager=%d plan=%d", result.AfterEpoch, mgr.RoutingEpoch(), plan.AfterEpoch)
	}
	if len(mgr.Shards()) != 2 {
		t.Fatalf("open shards = %d, want 2", len(mgr.Shards()))
	}
	target, ok := mgr.Shard(1)
	if !ok || target.State != placement.ShardStateCreating {
		t.Fatalf("target shard not creating: %#v", target)
	}
	if len(result.Steps) != 6 || result.Steps[0].Status != "ok" || result.Steps[1].Status != "ok" || result.Steps[2].Status != "pending_finalize" {
		t.Fatalf("unexpected apply steps: %+v", result.Steps)
	}
	if _, err := os.Stat(filepath.Join(root, "shards", "1", "state")); err != nil {
		t.Fatalf("target state dir not created: %v", err)
	}
	key := keyInRange(t, plan.After.Shards[1].Ranges[0])
	got, err := mgr.Router().ShardForPK([]byte(key))
	if err != nil {
		t.Fatalf("ShardForPK returned error: %v", err)
	}
	if got != 0 {
		t.Fatalf("transition route = %d, want source shard 0", got)
	}
}

func TestApplyPlacementPreparesSplitWithRaft(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	addr := pickPort(t)
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:          t.TempDir(),
		Shards:        1,
		SelfID:        "n1",
		MuxAddr:       addr,
		Peers:         map[string]string{"n1": addr},
		Bootstrap:     true,
		HeartbeatMS:   50,
		ElectionMS:    150,
		LeaderLeaseMS: 50,
		CommitMS:      10,
		LogOutput:     io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	waitShardLeader(t, mgr, 0)

	plan, err := mgr.PlanPlacement(placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.ApplyPlacement(context.Background(), placement.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	}); err != nil {
		t.Fatal(err)
	}
	child, ok := mgr.Shard(1)
	if !ok || child.Raft == nil || child.RaftStorage == nil || child.State != placement.ShardStateCreating {
		t.Fatalf("child raft shard not open: %#v", child)
	}
	waitShardLeader(t, mgr, 1)
}

func waitShardLeader(t *testing.T, mgr *cluster.Manager, shardID uint32) {
	t.Helper()
	wait.Eventually(t, func() bool {
		sh, ok := mgr.Shard(shardID)
		return ok && sh != nil && sh.Raft != nil && sh.Raft.IsLeader()
	}, 5*time.Second, 25*time.Millisecond, "shard %d did not become leader", shardID)
}

func TestApplyPlacementRejectsBeforeMismatch(t *testing.T) {
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      t.TempDir(),
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	plan, err := mgr.PlanPlacement(placement.PlacementPlanRequest{
		Operation:    placement.PlacementOperationMove,
		ShardID:      0,
		TargetVoters: []string{"n1"},
		MinVoters:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan.Before.UpdatedAtUnix++
	_, err = mgr.ApplyPlacement(context.Background(), placement.PlacementApplyRequest{Plan: plan})
	if !errors.Is(err, placement.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
}

func TestFinalizeSplitCopiesRangeAndActivatesChild(t *testing.T) {
	mgr, plan := openTransitionSplitManager(t)
	defer mgr.Close()

	parent, _ := mgr.Shard(0)
	child, _ := mgr.Shard(1)
	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	parentCatalog, err := catalog.New(parent.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := parentCatalog.Create(td); err != nil {
		t.Fatal(err)
	}

	childRange := plan.After.Shards[1].Ranges[0]
	key := keyInRange(t, childRange)
	item := types.Item{
		"id": {T: types.AttrS, S: key},
		"v":  {T: types.AttrS, S: "moved"},
	}
	if err := parent.Storage.PutItem(td.Name, td.KeySchema, item); err != nil {
		t.Fatal(err)
	}

	result, err := mgr.FinalizeSplit(context.Background(), cluster.SplitFinalizeRequest{
		ParentShardID:  0,
		ChildShardID:   1,
		ExpectedEpoch:  plan.AfterEpoch,
		WritesQuiesced: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CopiedKeys != 1 || result.DeletedKeys != 1 || result.CopiedCatalogKeys != 1 {
		t.Fatalf("unexpected copy/delete counts: %+v", result)
	}
	if result.Placement.Shards[0].State != placement.ShardStateActive || result.Placement.Shards[1].State != placement.ShardStateActive {
		t.Fatalf("unexpected shard states: %+v", result.Placement.Shards)
	}
	routed, err := mgr.Router().ShardForPK([]byte(key))
	if err != nil {
		t.Fatalf("ShardForPK returned error: %v", err)
	}
	if routed != 1 {
		t.Fatalf("key routes to shard %d, want child shard 1", routed)
	}
	got, err := child.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: key}})
	if err != nil {
		t.Fatalf("child get: %v", err)
	}
	if got["v"].S != "moved" {
		t.Fatalf("child value = %q", got["v"].S)
	}
	if _, err := parent.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: key}}); !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("parent get error = %v, want ErrItemNotFound", err)
	}
}

func TestFinalizeSplitAllowsLiveWriteBarrier(t *testing.T) {
	mgr, plan := openTransitionSplitManager(t)
	defer mgr.Close()

	if _, err := mgr.FinalizeSplit(context.Background(), cluster.SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	}); err != nil {
		t.Fatalf("finalize without external quiesce: %v", err)
	}
}

func TestFinalizeSplitMigratesIndexesAndTTL(t *testing.T) {
	mgr, plan := openTransitionSplitManager(t)
	defer mgr.Close()

	parent, _ := mgr.Shard(0)
	child, _ := mgr.Shard(1)
	td := types.TableDescriptor{
		Name:      "events",
		KeySchema: types.KeySchema{PK: "user_id", SK: "ts"},
		GSIs: []types.GSIDescriptor{{
			Name:      "by_event",
			KeySchema: types.KeySchema{PK: "event", SK: "ts"},
		}},
		LSIs: []types.LSIDescriptor{{
			Name: "by_author",
			SK:   "author",
		}},
		SpatialIndexes: []types.SpatialIndexDescriptor{{
			Name:       "by_location",
			Kind:       pebble.SpatialKindGeohash,
			Attributes: []string{"lat", "lon"},
			Precision:  5,
		}},
		TTLAttribute: "expires_at",
	}
	parentCatalog, err := catalog.New(parent.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := parentCatalog.Create(td); err != nil {
		t.Fatal(err)
	}

	keys := keysInRange(t, plan.After.Shards[1].Ranges[0], 3)
	now := time.Now()
	moved := types.Item{
		"user_id":    splitSAttr(keys[0]),
		"ts":         splitSAttr("001"),
		"event":      splitSAttr("signup"),
		"author":     splitSAttr("old-author"),
		"lat":        splitNAttr("40.7128"),
		"lon":        splitNAttr("-74.0060"),
		"expires_at": splitNAttr(fmt.Sprintf("%d", now.Add(time.Hour).Unix())),
	}
	if err := parent.Storage.PutItemWith(td, moved, pebble.PutOptions{}); err != nil {
		t.Fatalf("put seed: %v", err)
	}
	moved["event"] = splitSAttr("login")
	moved["author"] = splitSAttr("alice")
	if err := parent.Storage.PutItemWith(td, moved, pebble.PutOptions{}); err != nil {
		t.Fatalf("update indexed row: %v", err)
	}

	deleted := types.Item{
		"user_id": splitSAttr(keys[1]),
		"ts":      splitSAttr("002"),
		"event":   splitSAttr("login"),
		"author":  splitSAttr("deleted"),
		"lat":     splitNAttr("40.7300"),
		"lon":     splitNAttr("-73.9900"),
	}
	if err := parent.Storage.PutItemWith(td, deleted, pebble.PutOptions{}); err != nil {
		t.Fatalf("put deleted seed: %v", err)
	}
	if err := parent.Storage.DeleteItemWith(td, deleted, pebble.DeleteOptions{}); err != nil {
		t.Fatalf("delete seed: %v", err)
	}

	expired := types.Item{
		"user_id":    splitSAttr(keys[2]),
		"ts":         splitSAttr("003"),
		"event":      splitSAttr("expired"),
		"author":     splitSAttr("ttl"),
		"lat":        splitNAttr("40.7000"),
		"lon":        splitNAttr("-74.0100"),
		"expires_at": splitNAttr(fmt.Sprintf("%d", now.Add(-time.Hour).Unix())),
	}
	if err := parent.Storage.PutItemWith(td, expired, pebble.PutOptions{}); err != nil {
		t.Fatalf("put expired row: %v", err)
	}

	result, err := mgr.FinalizeSplit(context.Background(), cluster.SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("finalize indexed split: %v", err)
	}
	if result.CopiedKeys != 2 || result.DeletedKeys != 2 || result.CopiedCatalogKeys != 1 {
		t.Fatalf("unexpected copy/delete counts: %+v", result)
	}

	gsiHits, err := child.Storage.QueryByGSI(td, "by_event", splitSAttr("login"), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("child GSI query: %v", err)
	}
	if len(gsiHits) != 1 || gsiHits[0]["user_id"].S != keys[0] || gsiHits[0]["author"].S != "alice" {
		t.Fatalf("child GSI hits = %+v", gsiHits)
	}
	lsiHits, err := child.Storage.QueryByLSI(td, "by_author", splitSAttr(keys[0]), pebble.QueryOptions{})
	if err != nil {
		t.Fatalf("child LSI query: %v", err)
	}
	if len(lsiHits) != 1 || lsiHits[0]["author"].S != "alice" {
		t.Fatalf("child LSI hits = %+v", lsiHits)
	}
	box := spatial.BBox{MinLat: 40.6, MinLon: -74.1, MaxLat: 40.8, MaxLon: -73.9}
	spatialHits, err := child.Storage.SpatialQueryItems(td, "by_location", pebble.SpatialQuery{BBox: &box})
	if err != nil {
		t.Fatalf("child spatial query: %v", err)
	}
	if len(spatialHits) != 2 {
		t.Fatalf("child spatial hits = %+v, want moved and expired rows", spatialHits)
	}

	if got := countTableDataKeys(t, parent.Storage, td.Name); got != 0 {
		t.Fatalf("parent table data keys = %d, want 0", got)
	}

	reaper := pebble.NewReaper(child.Storage, splitCatalog{tables: []types.TableDescriptor{td}}, nil, pebble.ReaperConfig{
		BatchSize: 1024,
		Now:       func() time.Time { return now },
	})
	if err := reaper.Tick(context.Background()); err != nil {
		t.Fatalf("child TTL reaper: %v", err)
	}
	_, err = child.Storage.GetItem(td.Name, td.KeySchema, types.Item{
		"user_id": splitSAttr(keys[2]),
		"ts":      splitSAttr("003"),
	})
	if !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("expired child row error = %v, want ErrItemNotFound", err)
	}
}

func TestFinalizeRangeMoveCopiesRangeAndActivatesTarget(t *testing.T) {
	mgr, plan := openTransitionRangeMoveManager(t)
	defer mgr.Close()

	source, _ := mgr.Shard(0)
	target, _ := mgr.Shard(1)
	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	sourceCatalog, err := catalog.New(source.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceCatalog.Create(td); err != nil {
		t.Fatal(err)
	}

	moveRange := plan.After.Shards[1].Ranges[0]
	stayRange := placement.TokenRange{Start: moveRange.End, End: 0}
	movedKey := keyInRange(t, moveRange)
	stayKey := keyInRange(t, stayRange)
	if err := source.Storage.PutItem(td.Name, td.KeySchema, types.Item{
		"id": {T: types.AttrS, S: movedKey},
		"v":  {T: types.AttrS, S: "moved"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.Storage.PutItem(td.Name, td.KeySchema, types.Item{
		"id": {T: types.AttrS, S: stayKey},
		"v":  {T: types.AttrS, S: "stay"},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := mgr.FinalizeRangeMove(context.Background(), cluster.RangeMoveFinalizeRequest{
		SourceShardID: 0,
		TargetShardID: 1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CopiedKeys != 1 || result.DeletedKeys != 1 || result.CopiedCatalogKeys != 1 {
		t.Fatalf("unexpected copy/delete counts: %+v", result)
	}
	if result.Placement.Shards[0].State != placement.ShardStateActive || result.Placement.Shards[1].State != placement.ShardStateActive {
		t.Fatalf("unexpected shard states: %+v", result.Placement.Shards)
	}
	if len(result.SourceRangesAfter) != 1 || result.SourceRangesAfter[0] != stayRange {
		t.Fatalf("source ranges after = %+v, want %+v", result.SourceRangesAfter, stayRange)
	}
	movedRouted, err := mgr.Router().ShardForPK([]byte(movedKey))
	if err != nil {
		t.Fatalf("ShardForPK(moved) returned error: %v", err)
	}
	if movedRouted != 1 {
		t.Fatalf("moved key routes to shard %d, want target shard 1", movedRouted)
	}
	stayRouted, err := mgr.Router().ShardForPK([]byte(stayKey))
	if err != nil {
		t.Fatalf("ShardForPK(stay) returned error: %v", err)
	}
	if stayRouted != 0 {
		t.Fatalf("stay key routes to shard %d, want source shard 0", stayRouted)
	}
	got, err := target.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: movedKey}})
	if err != nil {
		t.Fatalf("target get: %v", err)
	}
	if got["v"].S != "moved" {
		t.Fatalf("target value = %q", got["v"].S)
	}
	if _, err := source.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: movedKey}}); !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("source moved get error = %v, want ErrItemNotFound", err)
	}
	got, err = source.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: stayKey}})
	if err != nil {
		t.Fatalf("source stay get: %v", err)
	}
	if got["v"].S != "stay" {
		t.Fatalf("source stay value = %q", got["v"].S)
	}
}

func openTransitionSplitManager(t *testing.T) (*cluster.Manager, placement.PlacementPlan) {
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
		Root:      root,
		Shards:    2,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgr, plan
}

func openTransitionRangeMoveManager(t *testing.T) (*cluster.Manager, placement.PlacementPlan) {
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
		Root:      root,
		Shards:    2,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgr, plan
}

func keyInRange(t *testing.T, rng placement.TokenRange) string {
	t.Helper()
	keys := keysInRange(t, rng, 1)
	return keys[0]
}

func keysInRange(t *testing.T, rng placement.TokenRange, n int) []string {
	t.Helper()
	router := routing.NewRouter(1)
	keys := make([]string, 0, n)
	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("split-key-%d", i)
		if rng.Contains(router.TokenForPK([]byte(key))) {
			keys = append(keys, key)
			if len(keys) == n {
				return keys
			}
		}
	}
	t.Fatalf("could not find %d keys in range", n)
	return nil
}

func splitSAttr(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func splitNAttr(n string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrN, N: n}
}

type splitCatalog struct {
	tables []types.TableDescriptor
}

func (c splitCatalog) List() []types.TableDescriptor { return c.tables }

func countTableDataKeys(t *testing.T, db *pebble.DB, table string) int {
	t.Helper()
	lower, upper := storage.PrefixTable(table)
	iter, err := db.Iter(lower, upper)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	var count int
	for valid := iter.First(); valid; valid = iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	return count
}

func contains(in []string, v string) bool {
	for _, existing := range in {
		if existing == v {
			return true
		}
	}
	return false
}

func containsWarning(warnings []string, substr string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, substr) {
			return true
		}
	}
	return false
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
