package main

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/client"
)

func TestRouterFromStatusPrefersActualLeader(t *testing.T) {
	rt, shards, err := routerFromStatus(client.ClusterStatus{
		ShardCount:        1,
		PlacementStrategy: "token-range-v1",
		Shards: []client.ShardPlacement{{
			ID:           0,
			Ranges:       []client.TokenRange{{Start: 0, End: 0}},
			Voters:       []string{"n1", "n2", "n3"},
			LeaderHint:   "n1",
			ActualLeader: "n2",
		}},
	})
	if err != nil {
		t.Fatalf("routerFromStatus: %v", err)
	}
	if len(shards) != 1 {
		t.Fatalf("shards len = %d, want 1", len(shards))
	}
	if shards[0].Leader != "n2" {
		t.Fatalf("leader = %q, want actual leader n2", shards[0].Leader)
	}
	if shards[0].LeaderHint != "n1" {
		t.Fatalf("leader hint = %q, want n1", shards[0].LeaderHint)
	}
	target, err := rt.routeForID(42, 100)
	if err != nil {
		t.Fatalf("routeForID: %v", err)
	}
	if target.Leader != "n2" {
		t.Fatalf("target leader = %q, want actual leader n2", target.Leader)
	}
}

func TestRouterFromStatusFallsBackToLeaderHint(t *testing.T) {
	_, shards, err := routerFromStatus(client.ClusterStatus{
		ShardCount:        1,
		PlacementStrategy: "token-range-v1",
		Shards: []client.ShardPlacement{{
			ID:         0,
			Ranges:     []client.TokenRange{{Start: 0, End: 0}},
			Voters:     []string{"n1", "n2", "n3"},
			LeaderHint: "n1",
		}},
	})
	if err != nil {
		t.Fatalf("routerFromStatus: %v", err)
	}
	if shards[0].Leader != "n1" {
		t.Fatalf("leader = %q, want leader hint n1", shards[0].Leader)
	}
}
