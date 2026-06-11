package cluster_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/cluster"
)

func plannerCatalog(shards int) cluster.PlacementCatalog {
	cat := cluster.DefaultPlacement(
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
		cluster.NodeCapacity{},
		cluster.PlacementStrategyTokenRange,
	)
	cat.Nodes["n4"] = cluster.NodeDescriptor{ID: "n4", RaftAddr: "127.0.0.1:9104", HTTPAddr: "http://127.0.0.1:8084", State: cluster.NodeStateActive}
	return cat
}

func TestPlanSplitCreatesSafeTransition(t *testing.T) {
	cat := plannerCatalog(1)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.RequiresDataCopy || !plan.RequiresRestart || plan.ApplySupported {
		t.Fatalf("unexpected split flags: %+v", plan)
	}
	if len(plan.After.Shards) != 2 {
		t.Fatalf("after shard count = %d, want 2", len(plan.After.Shards))
	}
	if plan.After.Shards[0].State != cluster.ShardStateSplitting {
		t.Fatalf("parent state = %s", plan.After.Shards[0].State)
	}
	if plan.After.Shards[1].State != cluster.ShardStateCreating {
		t.Fatalf("child state = %s", plan.After.Shards[1].State)
	}
	if len(plan.After.Shards[0].Ranges) != 1 || plan.After.Shards[0].Ranges[0] != cat.Shards[0].Ranges[0] {
		t.Fatalf("parent range changed before activation: %+v", plan.After.Shards[0].Ranges)
	}
	if err := cluster.ValidatePlacement(plan.After); err != nil {
		t.Fatalf("planned placement invalid: %v", err)
	}
}

func TestPlanMoveReplacesVoter(t *testing.T) {
	cat := plannerCatalog(2)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation:  cluster.PlacementOperationMove,
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
	if plan.After.Shards[0].State != cluster.ShardStateActive {
		t.Fatalf("state = %s", plan.After.Shards[0].State)
	}
	if plan.RequiresDataCopy || plan.RequiresRestart || !plan.ApplySupported || len(plan.Steps) != 3 {
		t.Fatalf("unexpected move plan: %+v", plan)
	}
}

func TestPlanDrainRemovesNodeFromEveryShard(t *testing.T) {
	cat := plannerCatalog(3)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation:   cluster.PlacementOperationDrain,
		NodeID:      "n1",
		TargetNodes: []string{"n4"},
		MinVoters:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.After.Nodes["n1"].State != cluster.NodeStateDraining {
		t.Fatalf("node state = %s", plan.After.Nodes["n1"].State)
	}
	for _, sh := range plan.After.Shards {
		if contains(sh.Voters, "n1") {
			t.Fatalf("shard %d still has n1 voter: %v", sh.ID, sh.Voters)
		}
		if !contains(sh.Voters, "n4") {
			t.Fatalf("shard %d missing replacement n4: %v", sh.ID, sh.Voters)
		}
		if sh.State != cluster.ShardStateActive {
			t.Fatalf("shard %d state = %s", sh.ID, sh.State)
		}
	}
	if !plan.ApplySupported {
		t.Fatalf("drain should be apply-supported")
	}
}

func TestPlanSplitRejectsLegacyModuloPlacement(t *testing.T) {
	cat := cluster.DefaultPlacement(2, "n1", map[string]string{"n1": "127.0.0.1:9101"}, nil, cluster.NodeCapacity{}, cluster.PlacementStrategyLegacyModulo)
	_, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if !errors.Is(err, cluster.ErrInvalidPlacementPlan) {
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

	plan, err := mgr.PlanPlacement(cluster.PlacementPlanRequest{
		Operation:    cluster.PlacementOperationMove,
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
	result, err := mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{
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

func TestApplyPlacementRejectsSplitPlan(t *testing.T) {
	cat := plannerCatalog(1)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
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
	_, err = mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{Plan: plan})
	if !errors.Is(err, cluster.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
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
	plan, err := mgr.PlanPlacement(cluster.PlacementPlanRequest{
		Operation:    cluster.PlacementOperationMove,
		ShardID:      0,
		TargetVoters: []string{"n1"},
		MinVoters:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan.Before.UpdatedAtUnix++
	_, err = mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{Plan: plan})
	if !errors.Is(err, cluster.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
}

func contains(in []string, v string) bool {
	for _, existing := range in {
		if existing == v {
			return true
		}
	}
	return false
}
