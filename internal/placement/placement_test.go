package placement

import "testing"

func TestDefaultPlacementDistributesLeaderHintsAcrossVoters(t *testing.T) {
	cat := DefaultPlacement(
		5,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
			"n3": "127.0.0.1:9103",
		},
		nil,
		NodeCapacity{},
		PlacementStrategyTokenRange,
	)

	want := []string{"n1", "n2", "n3", "n1", "n2"}
	for i, sh := range cat.Shards {
		if sh.LeaderHint != want[i] {
			t.Fatalf("shard %d leader hint = %q, want %q", sh.ID, sh.LeaderHint, want[i])
		}
		if !containsString(sh.Voters, sh.LeaderHint) {
			t.Fatalf("shard %d leader hint %q is not a voter in %v", sh.ID, sh.LeaderHint, sh.Voters)
		}
	}
}

func TestTransitionPlansAssignLeaderHintToCreatedShard(t *testing.T) {
	cat := DefaultPlacement(
		1,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
		},
		nil,
		NodeCapacity{},
		PlacementStrategyTokenRange,
	)

	split, err := BuildPlacementPlan(cat, PlacementPlanRequest{Operation: PlacementOperationSplit, ShardID: 0})
	if err != nil {
		t.Fatal(err)
	}
	child := split.After.Shards[1]
	if child.LeaderHint != leaderHintForShard(child.Voters, child.ID) {
		t.Fatalf("split child leader hint = %q, want %q", child.LeaderHint, leaderHintForShard(child.Voters, child.ID))
	}

	start := uint64(0)
	end := uint64(10)
	move, err := BuildPlacementPlan(cat, PlacementPlanRequest{
		Operation:  PlacementOperationRangeMove,
		ShardID:    0,
		RangeStart: &start,
		RangeEnd:   &end,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := move.After.Shards[1]
	if target.LeaderHint != leaderHintForShard(target.Voters, target.ID) {
		t.Fatalf("range-move target leader hint = %q, want %q", target.LeaderHint, leaderHintForShard(target.Voters, target.ID))
	}
}

func TestBackfillLeaderHintsMigratesOlderPlacement(t *testing.T) {
	cat := DefaultPlacement(
		3,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
		},
		nil,
		NodeCapacity{},
		PlacementStrategyTokenRange,
	)
	for i := range cat.Shards {
		cat.Shards[i].LeaderHint = ""
	}

	got := BackfillLeaderHints(cat)
	want := []string{"n1", "n2", "n1"}
	for i, sh := range got.Shards {
		if sh.LeaderHint != want[i] {
			t.Fatalf("shard %d leader hint = %q, want %q", sh.ID, sh.LeaderHint, want[i])
		}
	}
}
