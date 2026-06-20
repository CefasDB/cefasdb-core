package cluster

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/client"
)

func TestClusterDrainProgressShowsBlockers(t *testing.T) {
	st := client.ClusterStatus{
		Nodes: []client.NodeDescriptor{
			{ID: "n3", State: "decommissioned"},
			{ID: "n1", State: "draining"},
			{ID: "n2", State: "draining"},
		},
		Shards: []client.ShardPlacement{
			{ID: 0, State: "active", Ranges: []client.TokenRange{{Start: 0, End: 10}}, Voters: []string{"n1"}, LeaderHint: "n1"},
			{ID: 1, State: "active", NonVoters: []string{"n2"}},
			{ID: 2, State: "decommissioned", Voters: []string{"n2"}},
		},
	}

	progress := clusterDrainProgress(st)
	if len(progress) != 3 {
		t.Fatalf("progress len = %d, want 3: %+v", len(progress), progress)
	}
	if progress[0].NodeID != "n1" || progress[0].Status != "blocked" || progress[0].ActiveReferences != 2 {
		t.Fatalf("n1 progress = %+v, want blocked with two blockers", progress[0])
	}
	if progress[1].NodeID != "n2" || progress[1].Status != "blocked" || progress[1].ActiveReferences != 1 {
		t.Fatalf("n2 progress = %+v, want blocked with one active blocker", progress[1])
	}
	if progress[2].NodeID != "n3" || progress[2].Status != "decommissioned" || progress[2].ActiveReferences != 0 {
		t.Fatalf("n3 progress = %+v, want decommissioned", progress[2])
	}
}

func TestClusterDrainProgressReadyForDecommission(t *testing.T) {
	st := client.ClusterStatus{
		Nodes: []client.NodeDescriptor{{ID: "n1", State: "draining"}},
		Shards: []client.ShardPlacement{
			{ID: 0, State: "active", Voters: []string{"n2"}},
		},
	}

	progress := clusterDrainProgress(st)
	if len(progress) != 1 || progress[0].Status != "ready_for_decommission" || progress[0].ActiveReferences != 0 {
		t.Fatalf("progress = %+v, want ready_for_decommission", progress)
	}
}

func TestMergeShardLeadershipStatusesAcrossNodes(t *testing.T) {
	nodes := []rebalanceLeadersNodeResult{
		{
			NodeID: "n1",
			Result: client.RebalanceLeadersResult{
				Before: []client.ShardLeadershipStatus{
					{ShardID: 0, ActualLeader: "n1", DesiredLeader: "n1"},
					{ShardID: 1, ActualLeader: "n1", DesiredLeader: "n1"},
					{ShardID: 2, DesiredLeader: "n2"},
				},
			},
		},
		{
			NodeID: "n2",
			Result: client.RebalanceLeadersResult{
				Before: []client.ShardLeadershipStatus{
					{ShardID: 1, DesiredLeader: "n1"},
					{ShardID: 2, ActualLeader: "n2", DesiredLeader: "n2"},
				},
			},
		},
	}

	merged := mergeShardLeadershipStatuses(nodes, func(r client.RebalanceLeadersResult) []client.ShardLeadershipStatus {
		return r.Before
	})
	if len(merged) != 3 {
		t.Fatalf("merged len = %d, want 3: %+v", len(merged), merged)
	}
	if merged[1].ShardID != 1 || merged[1].ActualLeader != "n1" || merged[1].DesiredLeader != "n1" {
		t.Fatalf("shard 1 = %+v, want n1/n1", merged[1])
	}
	if merged[2].ShardID != 2 || merged[2].ActualLeader != "n2" || merged[2].DesiredLeader != "n2" {
		t.Fatalf("shard 2 = %+v, want n2/n2", merged[2])
	}
	counts := leaderCountsFromStatuses(merged)
	if len(counts) != 2 || counts[0].NodeID != "n1" || counts[0].Count != 1 || counts[1].NodeID != "n2" || counts[1].Count != 1 {
		t.Fatalf("counts = %+v, want n1=1 n2=1", counts)
	}
}

func TestAggregateLeaderRebalanceStepsDeduplicatesShards(t *testing.T) {
	nodes := []rebalanceLeadersNodeResult{
		{
			NodeID: "n1",
			Result: client.RebalanceLeadersResult{Steps: []client.LeaderRebalanceStep{
				{ShardID: 1, Status: "planned"},
				{ShardID: 2, Status: "skipped"},
				{ShardID: 4, Status: "skipped"},
			}},
		},
		{
			NodeID: "n2",
			Result: client.RebalanceLeadersResult{Steps: []client.LeaderRebalanceStep{
				{ShardID: 1, Status: "skipped"},
				{ShardID: 2, Status: "transferred"},
				{ShardID: 3, Status: "failed"},
			}},
		},
	}

	planned, transferred, skipped, failed := aggregateLeaderRebalanceSteps(nodes)
	if planned != 1 || transferred != 1 || skipped != 1 || failed != 1 {
		t.Fatalf("planned/transferred/skipped/failed = %d/%d/%d/%d, want 1/1/1/1", planned, transferred, skipped, failed)
	}
}
