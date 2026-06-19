package cluster

import (
	"testing"

	"github.com/CefasDb/cefasdb/internal/placement"
)

func TestPlanLeaderRebalanceSkipsShardZeroByDefault(t *testing.T) {
	got := planLeaderRebalanceShard(placement.ShardPlacement{
		ID:         0,
		Voters:     []string{"n1", "n2", "n3"},
		LeaderHint: "n1",
	}, "n2", false)
	if got.Status != LeaderRebalanceStatusSkipped {
		t.Fatalf("status = %q, want skipped", got.Status)
	}
	if got.Detail != "shard 0 skipped" {
		t.Fatalf("detail = %q", got.Detail)
	}
}

func TestPlanLeaderRebalancePlansMismatchToVoterHint(t *testing.T) {
	got := planLeaderRebalanceShard(placement.ShardPlacement{
		ID:         7,
		Voters:     []string{"n1", "n2", "n3"},
		LeaderHint: "n1",
	}, "n2", false)
	if got.Status != LeaderRebalanceStatusPlanned {
		t.Fatalf("status = %q, want planned", got.Status)
	}
	if got.CurrentLeader != "n2" || got.DesiredLeader != "n1" {
		t.Fatalf("leaders = current %q desired %q", got.CurrentLeader, got.DesiredLeader)
	}
}

func TestPlanLeaderRebalanceSkipsNonVoterHint(t *testing.T) {
	got := planLeaderRebalanceShard(placement.ShardPlacement{
		ID:         7,
		Voters:     []string{"n2", "n3"},
		LeaderHint: "n1",
	}, "n2", true)
	if got.Status != LeaderRebalanceStatusSkipped {
		t.Fatalf("status = %q, want skipped", got.Status)
	}
}
