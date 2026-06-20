package cluster

import (
	"context"
	"testing"

	"github.com/CefasDb/cefasdb/internal/placement"
)

func TestPlanLeaderRebalanceSkipsShardZeroByDefault(t *testing.T) {
	mgr := managerForLeaderRebalancePlan()
	steps := mgr.planLeaderRebalance(LeaderRebalanceRequest{}, []ShardLeadership{
		{ShardID: 0, ActualLeader: "n1", DesiredLeader: "n2", LeaderMismatch: true},
		{ShardID: 1, ActualLeader: "n1", DesiredLeader: "n2", LeaderMismatch: true},
	})

	if steps[0].Status != "skipped" || steps[0].Reason != "shard_zero_skipped" {
		t.Fatalf("shard 0 step = %+v, want shard_zero_skipped", steps[0])
	}
	if steps[1].Status != "planned" || steps[1].TargetLeader != "n2" {
		t.Fatalf("shard 1 step = %+v, want planned transfer to n2", steps[1])
	}
}

func TestPlanLeaderRebalanceRejectsInvalidTargets(t *testing.T) {
	mgr := managerForLeaderRebalancePlan()
	mgr.cat.Shards[1].LeaderHint = "n3"
	mgr.cat.Shards[1].Voters = []string{"n1", "n2"}
	mgr.cat.Shards[2].LeaderHint = "n3"
	mgr.cat.Shards[2].Voters = []string{"n2", "n3"}
	mgr.cat.Nodes["n3"] = placement.NodeDescriptor{ID: "n3", RaftAddr: "n3:7000", State: placement.NodeStateDraining}

	steps := mgr.planLeaderRebalance(LeaderRebalanceRequest{IncludeShardZero: true}, []ShardLeadership{
		{ShardID: 1, ActualLeader: "n1", DesiredLeader: "n3", LeaderMismatch: true},
		{ShardID: 2, ActualLeader: "n2", DesiredLeader: "n3", LeaderMismatch: true},
	})

	if steps[1].Reason != "target_not_voter" {
		t.Fatalf("shard 1 step = %+v, want target_not_voter", steps[1])
	}
	if steps[2].Reason != "target_unavailable" {
		t.Fatalf("shard 2 step = %+v, want target_unavailable", steps[2])
	}
}

func TestRebalanceLeadersDryRunSummarizesPlan(t *testing.T) {
	mgr := managerForLeaderRebalancePlan()
	result, err := mgr.RebalanceLeaders(context.Background(), LeaderRebalanceRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}

	if !result.DryRun || result.MaxConcurrent != 1 || result.Timeout <= 0 {
		t.Fatalf("normalized request fields missing: %+v", result)
	}
	if result.Planned != 0 || result.Transferred != 0 || result.Failed != 0 {
		t.Fatalf("unexpected active work in dry run without known leaders: %+v", result)
	}
	if result.Skipped != len(result.Steps) {
		t.Fatalf("skipped = %d, steps = %d", result.Skipped, len(result.Steps))
	}
}

func managerForLeaderRebalancePlan() *Manager {
	return &Manager{
		cfg: Config{
			SelfID: "n1",
			Peers:  map[string]string{"n1": "n1:7000", "n2": "n2:7000"},
		},
		cat: placement.PlacementCatalog{
			Version:  1,
			Epoch:    1,
			Strategy: placement.PlacementStrategyTokenRange,
			Nodes: map[string]placement.NodeDescriptor{
				"n1": {ID: "n1", RaftAddr: "n1:7000", State: placement.NodeStateActive},
				"n2": {ID: "n2", RaftAddr: "n2:7000", State: placement.NodeStateActive},
			},
			Shards: []placement.ShardPlacement{
				{
					ID:         0,
					State:      placement.ShardStateActive,
					Epoch:      1,
					Ranges:     []placement.TokenRange{{Start: 0, End: 10}},
					Voters:     []string{"n1", "n2"},
					LeaderHint: "n2",
				},
				{
					ID:         1,
					State:      placement.ShardStateActive,
					Epoch:      1,
					Ranges:     []placement.TokenRange{{Start: 10, End: 20}},
					Voters:     []string{"n1", "n2"},
					LeaderHint: "n2",
				},
				{
					ID:         2,
					State:      placement.ShardStateActive,
					Epoch:      1,
					Ranges:     []placement.TokenRange{{Start: 20, End: 0}},
					Voters:     []string{"n1", "n2"},
					LeaderHint: "n1",
				},
			},
		},
	}
}
