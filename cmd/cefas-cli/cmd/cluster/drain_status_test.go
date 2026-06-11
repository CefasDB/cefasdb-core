package cluster

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/client"
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
