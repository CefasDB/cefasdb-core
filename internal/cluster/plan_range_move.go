package cluster

import (
	"fmt"
	"strings"
)

// planRangeMove derives the placement-plan that hands a token range
// off from one shard to a new target shard. Source routing stays
// active until RangeMoveFinalize verifies the data and publishes
// cutover.
func planRangeMove(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	if cat.Strategy != PlacementStrategyTokenRange {
		return PlacementPlan{}, invalidPlan("range move requires %s placement, got %s", PlacementStrategyTokenRange, cat.Strategy)
	}
	if req.RangeStart == nil || req.RangeEnd == nil {
		return PlacementPlan{}, invalidPlan("range move requires rangeStart and rangeEnd")
	}
	sourceIdx, source, err := findShard(cat, req.ShardID)
	if err != nil {
		return PlacementPlan{}, err
	}
	if source.State != ShardStateActive {
		return PlacementPlan{}, invalidPlan("source shard %d must be %s, got %s", source.ID, ShardStateActive, source.State)
	}
	moveRange := TokenRange{Start: *req.RangeStart, End: *req.RangeEnd}
	if err := validateRangeOwnedBySource(source, moveRange); err != nil {
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
		return PlacementPlan{}, invalidPlan("range move target shard %d needs at least one voter", targetShardID)
	}

	after := nextCatalog(cat)
	after.Shards[sourceIdx].State = ShardStateMoving
	after.Shards[sourceIdx].Epoch = after.Epoch
	after.Shards = append(after.Shards, ShardPlacement{
		ID:     targetShardID,
		Ranges: []TokenRange{moveRange},
		State:  ShardStateCreating,
		Epoch:  after.Epoch,
		Voters: voters,
	})
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementPlan{}, err
	}

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
		Steps:            rangeMoveSteps(source.ID, targetShardID, moveRange, voters),
	}, nil
}

func validateRangeOwnedBySource(source ShardPlacement, moveRange TokenRange) error {
	sourceRangesAfter, err := subtractTokenRanges(source.Ranges, moveRange)
	if err != nil {
		return fmt.Errorf("%w: range move source shard %d: %v", ErrInvalidPlacementPlan, source.ID, err)
	}
	if len(sourceRangesAfter) == len(source.Ranges) && sameTokenRanges(sourceRangesAfter, source.Ranges) {
		return invalidPlan("range [%d,%d) is not owned by source shard %d", moveRange.Start, moveRange.End, source.ID)
	}
	return nil
}

func rangeMoveTargetID(cat PlacementCatalog, req PlacementPlanRequest) (uint32, error) {
	targetShardID := uint32(len(cat.Shards))
	if req.TargetShardID != nil {
		targetShardID = *req.TargetShardID
	}
	if int(targetShardID) != len(cat.Shards) {
		return 0, invalidPlan("range move target shard id must be %d to keep placement IDs contiguous", len(cat.Shards))
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
