package placement

import (
	"fmt"
	"strings"
)

// planRangeMove derives the placement-plan that hands a token range
// off from one shard to a new target shard. Source routing stays
// active until RangeMoveFinalize verifies the data and publishes
// cutover.
func planRangeMove(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	sourceIdx, source, moveRange, err := validateRangeMoveInputs(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	targetShardID, err := rangeMoveTargetID(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	voters, policyWarnings, err := rangeMoveVoters(cat, source, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	if len(voters) == 0 {
		return PlacementPlan{}, InvalidPlan("range move target shard %d needs at least one voter", targetShardID)
	}
	after, err := applyRangeMoveTransition(cat, sourceIdx, targetShardID, moveRange, voters)
	if err != nil {
		return PlacementPlan{}, err
	}
	return buildRangeMovePlan(cat, after, source.ID, targetShardID, moveRange, voters, policyWarnings), nil
}

// validateRangeMoveInputs gathers every precondition for the
// range-move path: token-range strategy, both range bounds supplied,
// source shard exists and is Active, and the requested range is in
// fact owned by the source.
func validateRangeMoveInputs(cat PlacementCatalog, req PlacementPlanRequest) (int, ShardPlacement, TokenRange, error) {
	if cat.Strategy != PlacementStrategyTokenRange {
		return 0, ShardPlacement{}, TokenRange{}, InvalidPlan("range move requires %s placement, got %s", PlacementStrategyTokenRange, cat.Strategy)
	}
	if req.RangeStart == nil || req.RangeEnd == nil {
		return 0, ShardPlacement{}, TokenRange{}, InvalidPlan("range move requires rangeStart and rangeEnd")
	}
	sourceIdx, source, err := FindShard(cat, req.ShardID)
	if err != nil {
		return 0, ShardPlacement{}, TokenRange{}, err
	}
	if source.State != ShardStateActive {
		return 0, ShardPlacement{}, TokenRange{}, InvalidPlan("source shard %d must be %s, got %s", source.ID, ShardStateActive, source.State)
	}
	moveRange := TokenRange{Start: *req.RangeStart, End: *req.RangeEnd}
	if err := validateRangeOwnedBySource(source, moveRange); err != nil {
		return 0, ShardPlacement{}, TokenRange{}, err
	}
	return sourceIdx, source, moveRange, nil
}

// applyRangeMoveTransition mutates the catalog to reflect the
// in-flight move: source → Moving, target → Creating with the
// pending range and voter set. Returns the validated next-epoch
// catalog.
func applyRangeMoveTransition(cat PlacementCatalog, sourceIdx int, targetShardID uint32, moveRange TokenRange, voters []string) (PlacementCatalog, error) {
	after := NextCatalog(cat)
	after.Shards[sourceIdx].State = ShardStateMoving
	after.Shards[sourceIdx].Epoch = after.Epoch
	after.Shards = append(after.Shards, ShardPlacement{
		ID:     targetShardID,
		Ranges: []TokenRange{moveRange},
		State:  ShardStateCreating,
		Epoch:  after.Epoch,
		Voters: voters,
	})
	after.Normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementCatalog{}, err
	}
	return after, nil
}

// buildRangeMovePlan assembles the user-facing PlacementPlan from
// the validated before/after catalogs. Kept separate so planRangeMove
// stays inside the §9 LOC cap.
func buildRangeMovePlan(cat, after PlacementCatalog, sourceID, targetID uint32, moveRange TokenRange, voters, policyWarnings []string) PlacementPlan {
	warnings := []string{
		"range-move apply opens the target shard online and publishes a transition placement; source routing stays active until range-move finalize verifies data and publishes cutover",
	}
	warnings = append(warnings, policyWarnings...)
	return PlacementPlan{
		Operation:        PlacementOperationRangeMove,
		BeforeEpoch:      cat.Epoch,
		AfterEpoch:       after.Epoch,
		Before:           cat.Clone(),
		After:            after,
		RequiresDataCopy: true,
		ApplySupported:   true,
		Warnings:         warnings,
		Steps:            rangeMoveSteps(sourceID, targetID, moveRange, voters),
	}
}

func validateRangeOwnedBySource(source ShardPlacement, moveRange TokenRange) error {
	sourceRangesAfter, err := SubtractTokenRanges(source.Ranges, moveRange)
	if err != nil {
		return fmt.Errorf("%w: range move source shard %d: %v", ErrInvalidPlacementPlan, source.ID, err)
	}
	if len(sourceRangesAfter) == len(source.Ranges) && sameTokenRanges(sourceRangesAfter, source.Ranges) {
		return InvalidPlan("range [%d,%d) is not owned by source shard %d", moveRange.Start, moveRange.End, source.ID)
	}
	return nil
}

func rangeMoveTargetID(cat PlacementCatalog, req PlacementPlanRequest) (uint32, error) {
	targetShardID := uint32(len(cat.Shards))
	if req.TargetShardID != nil {
		targetShardID = *req.TargetShardID
	}
	if int(targetShardID) != len(cat.Shards) {
		return 0, InvalidPlan("range move target shard id must be %d to keep placement IDs contiguous", len(cat.Shards))
	}
	return targetShardID, nil
}

func rangeMoveVoters(cat PlacementCatalog, source ShardPlacement, req PlacementPlanRequest) ([]string, []string, error) {
	if len(req.TargetVoters) > 0 {
		if err := validateNodeSet(cat, req.TargetVoters, minVoters(req.MinVoters)); err != nil {
			return nil, nil, err
		}
		return sortedUnique(req.TargetVoters), nil, nil
	}
	voterCount := targetVoterCount(req.MinVoters, source.Voters)
	return selectPlacementVoters(cat, voterCount, nil)
}

func rangeMoveSteps(sourceID, targetID uint32, moveRange TokenRange, voters []string) []PlacementPlanStep {
	return []PlacementPlanStep{
		{Action: "create_shard", ShardID: u32ptr(targetID), Detail: fmt.Sprintf("open target shard %d online with pending range [%d,%d)", targetID, moveRange.Start, moveRange.End)},
		{Action: "target_membership", ShardID: u32ptr(targetID), Detail: fmt.Sprintf("target shard voters: %s", strings.Join(voters, ","))},
		{Action: "copy_range", ShardID: u32ptr(sourceID), Detail: fmt.Sprintf("copy range [%d,%d) from shard %d to shard %d", moveRange.Start, moveRange.End, sourceID, targetID)},
		{Action: "wait_catchup", ShardID: u32ptr(targetID), Detail: "dual-write catch-up fence for writes accepted during range movement"},
		{Action: "publish_cutover", ShardID: u32ptr(targetID), Detail: fmt.Sprintf("publish routing cutover for range [%d,%d)", moveRange.Start, moveRange.End)},
		{Action: "cleanup_source", ShardID: u32ptr(sourceID), Detail: fmt.Sprintf("delete moved range [%d,%d) from source shard %d", moveRange.Start, moveRange.End, sourceID)},
	}
}
