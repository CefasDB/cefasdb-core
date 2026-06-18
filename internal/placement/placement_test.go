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

func TestDefaultPlacementWithReplicationFactorKeepsMetadataShardGlobal(t *testing.T) {
	cat := DefaultPlacementWithReplicationFactor(
		8,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
			"n3": "127.0.0.1:9103",
			"n4": "127.0.0.1:9104",
			"n5": "127.0.0.1:9105",
			"n6": "127.0.0.1:9106",
			"n7": "127.0.0.1:9107",
			"n8": "127.0.0.1:9108",
		},
		nil,
		NodeCapacity{},
		PlacementStrategyTokenRange,
		3,
	)

	wantLeaders := []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}
	voterCounts := map[string]int{}
	for i, sh := range cat.Shards {
		wantVoters := 3
		if i == 0 {
			wantVoters = 8
		}
		if len(sh.Voters) != wantVoters {
			t.Fatalf("shard %d voters = %v, want %d voters", sh.ID, sh.Voters, wantVoters)
		}
		if sh.LeaderHint != wantLeaders[i] {
			t.Fatalf("shard %d leader hint = %q, want %q", sh.ID, sh.LeaderHint, wantLeaders[i])
		}
		if !containsString(sh.Voters, sh.LeaderHint) {
			t.Fatalf("shard %d leader hint %q is not a voter in %v", sh.ID, sh.LeaderHint, sh.Voters)
		}
		for _, voter := range sh.Voters {
			voterCounts[voter]++
		}
	}
	wantCounts := map[string]int{
		"n1": 3,
		"n2": 3,
		"n3": 3,
		"n4": 4,
		"n5": 4,
		"n6": 4,
		"n7": 4,
		"n8": 4,
	}
	for node, want := range wantCounts {
		if voterCounts[node] != want {
			t.Fatalf("node %s voter count = %d, want %d", node, voterCounts[node], want)
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
