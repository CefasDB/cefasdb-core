package cluster

import "fmt"

// planSplit derives the placement-plan that splits one
// PlacementStrategyTokenRange shard in two. The parent shard keeps
// serving the full range until SplitFinalize copies data and
// activates the child — the plan only describes the catalog
// transition.
func planSplit(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	shardIdx, shard, err := validateSplitTarget(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	newShardID, err := splitNewShardID(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	childRange, err := splitChildRange(shard, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	voters, policyWarnings, err := splitChildVoters(cat, shard, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	after, err := applySplitTransition(cat, shardIdx, newShardID, childRange, voters)
	if err != nil {
		return PlacementPlan{}, err
	}
	return buildSplitPlan(cat, after, newShardID, childRange, policyWarnings), nil
}

// validateSplitTarget enforces every precondition the split path
// requires on the source shard: the catalog must use the token-range
// strategy, the shard must exist, be routable, and own exactly one
// range (multi-range splits are deliberately out of scope today).
func validateSplitTarget(cat PlacementCatalog, req PlacementPlanRequest) (int, ShardPlacement, error) {
	if cat.Strategy != PlacementStrategyTokenRange {
		return 0, ShardPlacement{}, invalidPlan("split requires %s placement, got %s", PlacementStrategyTokenRange, cat.Strategy)
	}
	shardIdx, shard, err := findShard(cat, req.ShardID)
	if err != nil {
		return 0, ShardPlacement{}, err
	}
	if !shard.State.routable() {
		return 0, ShardPlacement{}, invalidPlan("shard %d is not routable: %s", shard.ID, shard.State)
	}
	if len(shard.Ranges) != 1 {
		return 0, ShardPlacement{}, invalidPlan("split currently requires exactly one range on shard %d", shard.ID)
	}
	return shardIdx, shard, nil
}

// splitChildRange picks the split token (caller-supplied or
// midpoint) and returns the half that becomes the child shard's
// range. The parent keeps the lower half implicitly through the
// transition placement.
func splitChildRange(shard ShardPlacement, req PlacementPlanRequest) (TokenRange, error) {
	rng := shard.Ranges[0]
	split := midpointToken(rng)
	if req.SplitToken != nil {
		split = *req.SplitToken
	}
	if !tokenStrictlyInside(rng, split) {
		return TokenRange{}, invalidPlan("split token %d is outside shard %d range [%d,%d)", split, shard.ID, rng.Start, rng.End)
	}
	_, childRange := splitRange(rng, split)
	return childRange, nil
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

// applySplitTransition mutates the catalog to reflect the in-flight
// split: parent → Splitting, child → Creating with the new range
// and voter set. Returns the validated next-epoch catalog.
func applySplitTransition(cat PlacementCatalog, shardIdx int, newShardID uint32, childRange TokenRange, voters []string) (PlacementCatalog, error) {
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
		return PlacementCatalog{}, err
	}
	return after, nil
}

// buildSplitPlan assembles the user-facing PlacementPlan from the
// validated before/after catalogs. Kept separate from
// applySplitTransition so the LOC budget of planSplit stays inside
// the §9 cap.
func buildSplitPlan(cat, after PlacementCatalog, newShardID uint32, childRange TokenRange, policyWarnings []string) PlacementPlan {
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
	}
}
