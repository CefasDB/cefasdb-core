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

func TestDefaultPlacementWithReplicationFactorRotatesDataVoters(t *testing.T) {
	cat := DefaultPlacementWithReplicationFactor(
		5,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
			"n3": "127.0.0.1:9103",
			"n4": "127.0.0.1:9104",
		},
		nil,
		NodeCapacity{},
		PlacementStrategyTokenRange,
		2,
	)

	if got := cat.Shards[0].Voters; !sameStringsPlacement(got, []string{"n1", "n2", "n3", "n4"}) {
		t.Fatalf("metadata shard voters = %v, want every peer", got)
	}
	if len(cat.Shards[0].Ranges) != 0 {
		t.Fatalf("metadata shard ranges = %+v, want none", cat.Shards[0].Ranges)
	}
	if len(cat.Shards[1].Ranges) != 1 || cat.Shards[1].Ranges[0].Start != 0 {
		t.Fatalf("first data shard range = %+v, want start at 0", cat.Shards[1].Ranges)
	}
	if len(cat.Shards[len(cat.Shards)-1].Ranges) != 1 || cat.Shards[len(cat.Shards)-1].Ranges[0].End != 0 {
		t.Fatalf("last data shard range = %+v, want wrap to 0", cat.Shards[len(cat.Shards)-1].Ranges)
	}
	want := [][]string{
		{"n1", "n2", "n3", "n4"},
		{"n2", "n4"},
		{"n1", "n3"},
		{"n2", "n4"},
		{"n1", "n3"},
	}
	for i, sh := range cat.Shards {
		if !sameStringsPlacement(sh.Voters, want[i]) {
			t.Fatalf("shard %d voters = %v, want %v", i, sh.Voters, want[i])
		}
		if !containsString(sh.Voters, sh.LeaderHint) {
			t.Fatalf("shard %d leader hint %q is not a voter in %v", sh.ID, sh.LeaderHint, sh.Voters)
		}
	}
}

func TestDefaultPlacementWithReplicationFactorSpacesDataVoters(t *testing.T) {
	cat := DefaultPlacementWithReplicationFactor(
		6,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
			"n3": "127.0.0.1:9103",
			"n4": "127.0.0.1:9104",
			"n5": "127.0.0.1:9105",
			"n6": "127.0.0.1:9106",
			"n7": "127.0.0.1:9107",
		},
		nil,
		NodeCapacity{},
		PlacementStrategyTokenRange,
		3,
	)

	want := [][]string{
		{"n1", "n2", "n3", "n4", "n5", "n6", "n7"},
		{"n1", "n2", "n5"},
		{"n2", "n3", "n6"},
		{"n3", "n4", "n7"},
		{"n1", "n4", "n5"},
		{"n2", "n5", "n6"},
	}
	for i, sh := range cat.Shards {
		if !sameStringsPlacement(sh.Voters, want[i]) {
			t.Fatalf("shard %d voters = %v, want %v", i, sh.Voters, want[i])
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

func sameStringsPlacement(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
