package cluster

import "fmt"

// planSplit derives the placement-plan that splits one
// PlacementStrategyTokenRange shard in two. The parent shard keeps
// serving the full range until SplitFinalize copies data and
// activates the child — the plan only describes the catalog
// transition.
func planSplit(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	if cat.Strategy != PlacementStrategyTokenRange {
		return PlacementPlan{}, invalidPlan("split requires %s placement, got %s", PlacementStrategyTokenRange, cat.Strategy)
	}
	shardIdx, shard, err := findShard(cat, req.ShardID)
	if err != nil {
		return PlacementPlan{}, err
	}
	if !shard.State.routable() {
		return PlacementPlan{}, invalidPlan("shard %d is not routable: %s", shard.ID, shard.State)
	}
	if len(shard.Ranges) != 1 {
		return PlacementPlan{}, invalidPlan("split currently requires exactly one range on shard %d", shard.ID)
	}

	newShardID, err := splitNewShardID(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	rng := shard.Ranges[0]
	split := midpointToken(rng)
	if req.SplitToken != nil {
		split = *req.SplitToken
	}
	if !tokenStrictlyInside(rng, split) {
		return PlacementPlan{}, invalidPlan("split token %d is outside shard %d range [%d,%d)", split, shard.ID, rng.Start, rng.End)
	}
	_, childRange := splitRange(rng, split)

	voters, policyWarnings, err := splitChildVoters(cat, shard, req)
	if err != nil {
		return PlacementPlan{}, err
	}

	after := nextCatalog(cat)
	after.Shards[shardIdx].State = ShardStateSplitting
	after.Shards[shardIdx].Epoch = after.Epoch
	after.Shards = append(after.Shards, ShardPlacement{
		ID:     newShardID,
		Ranges: []TokenRange{childRange},
		State:  ShardStateCreating,
		Epoch:  after.Epoch,
		Voters: voters,
	})
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementPlan{}, err
	}

	warnings := []string{
		"split apply opens the child shard online and publishes a transition placement; the parent keeps serving the full range until split finalize copies data and activates the child",
		"split finalize still requires writesQuiesced=true until live split catch-up is implemented",
	}
	warnings = append(warnings, policyWarnings...)

	return PlacementPlan{
		Operation:        PlacementOperationSplit,
		BeforeEpoch:      cat.Epoch,
		AfterEpoch:       after.Epoch,
		Before:           cat.Clone(),
		After:            after,
		RequiresDataCopy: true,
		ApplySupported:   true,
		Warnings:         warnings,
		Steps: []PlacementPlanStep{
			{Action: "create_shard", ShardID: u32ptr(newShardID), Detail: fmt.Sprintf("open shard %d online with range [%d,%d)", newShardID, childRange.Start, childRange.End)},
		},
	}, nil
}

// splitNewShardID enforces the contiguous-ID invariant: a new shard
// must be inserted at position len(cat.Shards). The caller can
// override only with the same value (defensive against stale UI
// inputs).
func splitNewShardID(cat PlacementCatalog, req PlacementPlanRequest) (uint32, error) {
	newShardID := uint32(len(cat.Shards))
	if req.NewShardID != nil {
		newShardID = *req.NewShardID
	}
	if int(newShardID) != len(cat.Shards) {
		return 0, invalidPlan("new shard id must be %d to keep placement IDs contiguous", len(cat.Shards))
	}
	return newShardID, nil
}

// splitChildVoters resolves the voter set for the child shard:
// either the explicit TargetVoters list (validated against the
// catalog) or one chosen by the placement policy. policyWarnings is
// non-empty only when the policy selected the voters and produced
// advisory notes.
func splitChildVoters(cat PlacementCatalog, parent ShardPlacement, req PlacementPlanRequest) ([]string, []string, error) {
	if len(req.TargetVoters) > 0 {
		if err := validateNodeSet(cat, req.TargetVoters, minVoters(req.MinVoters)); err != nil {
			return nil, nil, err
		}
		return sortedUnique(req.TargetVoters), nil, nil
	}
	voterCount := targetVoterCount(req.MinVoters, parent.Voters)
	return selectPlacementVoters(cat, voterCount, nil)
}
