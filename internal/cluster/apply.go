// Package cluster: ApplyPlacement and its validators.
//
// Split off from planner.go to keep that file under the playbook
// §1 ≤ 500 LOC cap. ApplyPlacement is the runtime path that executes
// a previously-built placement.PlacementPlan, including the four per-operation
// validators (split, range-move, decommission, plus the generic
// gate) and the small same*-comparison helpers they rely on.
package cluster

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/internal/core/model"
)

func (m *Manager) ApplyPlacement(ctx context.Context, req placement.PlacementApplyRequest) (placement.PlacementApplyResult, error) {
	if err := m.RefreshPlacement(); err != nil {
		return placement.PlacementApplyResult{}, err
	}
	current := m.Placement()
	if result, ok, err := m.applyAlreadyPreparedTransition(ctx, current, req); ok || err != nil {
		return result, err
	}
	if err := validateApplyRequest(current, req); err != nil {
		return placement.PlacementApplyResult{}, err
	}
	timeout := applyTimeout(req.TimeoutMS)
	applied := make([]placement.PlacementApplyStep, 0, len(req.Plan.Steps))
	if req.Plan.Operation == placement.PlacementOperationSplit || req.Plan.Operation == placement.PlacementOperationRangeMove {
		if err := m.openMissingShardsForPlacement(ctx, req.Plan.After); err != nil {
			return placement.PlacementApplyResult{}, err
		}
		for _, step := range req.Plan.Steps {
			status := "ok"
			if req.Plan.Operation == placement.PlacementOperationRangeMove {
				status = "pending_finalize"
				if step.Action == "create_shard" || step.Action == "target_membership" {
					status = "ok"
				}
			}
			applied = append(applied, placement.PlacementApplyStep{
				Action:  step.Action,
				ShardID: step.ShardID,
				NodeID:  step.NodeID,
				Status:  status,
				Detail:  step.Detail,
			})
		}
	} else if req.Plan.Operation == placement.PlacementOperationDecommission {
		for _, step := range req.Plan.Steps {
			if err := ctx.Err(); err != nil {
				return placement.PlacementApplyResult{}, err
			}
			applied = append(applied, placement.PlacementApplyStep{
				Action:  step.Action,
				ShardID: step.ShardID,
				NodeID:  step.NodeID,
				Status:  "ok",
				Detail:  step.Detail,
			})
		}
	} else {
		for _, step := range req.Plan.Steps {
			if err := ctx.Err(); err != nil {
				return placement.PlacementApplyResult{}, err
			}
			if err := m.executePlacementStep(step, timeout); err != nil {
				return placement.PlacementApplyResult{}, err
			}
			applied = append(applied, placement.PlacementApplyStep{
				Action:  step.Action,
				ShardID: step.ShardID,
				NodeID:  step.NodeID,
				Status:  "ok",
				Detail:  step.Detail,
			})
		}
	}
	if err := m.persistPlacementSnapshotStrict(m.placementPath, req.Plan.After); err != nil {
		return placement.PlacementApplyResult{}, err
	}
	if err := m.applyPlacement(req.Plan.After, false); err != nil {
		return placement.PlacementApplyResult{}, err
	}
	return placement.PlacementApplyResult{
		Operation:   req.Plan.Operation,
		BeforeEpoch: current.Epoch,
		AfterEpoch:  req.Plan.After.Epoch,
		Steps:       applied,
		Placement:   req.Plan.After.Clone(),
	}, nil
}

func validateApplyRequest(current placement.PlacementCatalog, req placement.PlacementApplyRequest) error {
	plan := req.Plan
	if plan.Operation != placement.PlacementOperationMove && plan.Operation != placement.PlacementOperationDrain && plan.Operation != placement.PlacementOperationSplit && plan.Operation != placement.PlacementOperationRangeMove && plan.Operation != placement.PlacementOperationDecommission {
		return placement.InvalidPlan("apply supports split, move, range_move, drain and decommission plans only, got %q", plan.Operation)
	}
	if plan.RequiresRestart {
		return placement.InvalidPlan("plan requires restart and cannot be applied online")
	}
	if plan.RequiresDataCopy && plan.Operation != placement.PlacementOperationSplit && plan.Operation != placement.PlacementOperationRangeMove {
		return placement.InvalidPlan("plan requires data copy and cannot be applied online")
	}
	if !plan.ApplySupported {
		return placement.InvalidPlan("plan is not marked apply-supported")
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != current.Epoch {
		return &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: current.Epoch}
	}
	if plan.BeforeEpoch != current.Epoch || plan.Before.Epoch != current.Epoch {
		return &StaleRouteError{ClientEpoch: plan.BeforeEpoch, CurrentEpoch: current.Epoch}
	}
	if !samePlacement(current, plan.Before) {
		return placement.InvalidPlan("plan before catalog does not match current placement")
	}
	if plan.After.Epoch <= current.Epoch {
		return placement.InvalidPlan("after epoch %d must be greater than current epoch %d", plan.After.Epoch, current.Epoch)
	}
	if plan.Operation == placement.PlacementOperationSplit {
		return validateSplitApplyRequest(current, plan)
	}
	if plan.Operation == placement.PlacementOperationRangeMove {
		return validateRangeMoveApplyRequest(current, plan)
	}
	if plan.Operation == placement.PlacementOperationDecommission {
		return validateDecommissionApplyRequest(current, plan)
	}
	if len(plan.After.Shards) != len(current.Shards) {
		return placement.InvalidPlan("apply cannot change shard count: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	if err := placement.ValidatePlacement(plan.After); err != nil {
		return err
	}
	return nil
}

func validateDecommissionApplyRequest(current placement.PlacementCatalog, plan placement.PlacementPlan) error {
	if plan.RequiresDataCopy {
		return placement.InvalidPlan("decommission apply must not require data copy")
	}
	if len(plan.After.Shards) != len(current.Shards) {
		return placement.InvalidPlan("decommission apply cannot change shard count: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	if err := placement.ValidatePlacement(plan.After); err != nil {
		return err
	}

	var target string
	for id, beforeNode := range current.Nodes {
		afterNode, ok := plan.After.Nodes[id]
		if !ok {
			return placement.InvalidPlan("decommission apply cannot remove node %q from placement", id)
		}
		if beforeNode.State == afterNode.State {
			continue
		}
		if target != "" {
			return placement.InvalidPlan("decommission apply can change one node state only")
		}
		if beforeNode.State != placement.NodeStateDraining || afterNode.State != placement.NodeStateDecommissioned {
			return placement.InvalidPlan("decommission apply requires node %q to transition %s -> %s, got %s -> %s", id, placement.NodeStateDraining, placement.NodeStateDecommissioned, beforeNode.State, afterNode.State)
		}
		target = id
	}
	for id := range plan.After.Nodes {
		if _, ok := current.Nodes[id]; !ok {
			return placement.InvalidPlan("decommission apply cannot add node %q", id)
		}
	}
	if target == "" {
		return placement.InvalidPlan("decommission apply found no node state transition")
	}
	targetNodeID, err := model.NewNodeID(target)
	if err != nil {
		return placement.InvalidPlan("decommission apply target node %q is invalid: %v", target, err)
	}
	if blockers := placement.PlacementNodeActiveReferences(current, targetNodeID); len(blockers) > 0 {
		return placement.InvalidPlan("node %q still has active placement references: %s", target, strings.Join(blockers, "; "))
	}
	for i, before := range current.Shards {
		after := plan.After.Shards[i]
		if before.ID != after.ID || before.State != after.State || before.Epoch != after.Epoch || before.LeaderHint != after.LeaderHint || !sameTokenRanges(before.Ranges, after.Ranges) || !sameStringSet(before.Voters, after.Voters) || !sameStringSet(before.NonVoters, after.NonVoters) {
			return placement.InvalidPlan("decommission apply cannot change shard %d placement", before.ID)
		}
	}
	return nil
}

func validateRangeMoveApplyRequest(current placement.PlacementCatalog, plan placement.PlacementPlan) error {
	if !plan.RequiresDataCopy {
		return placement.InvalidPlan("range_move apply expects a transition plan that still requires data copy")
	}
	if len(plan.After.Shards) != len(current.Shards)+1 {
		return placement.InvalidPlan("range_move apply expects exactly one target shard: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	sourceIdx := -1
	var sourceBefore, sourceAfter placement.ShardPlacement
	for i, before := range current.Shards {
		after := plan.After.Shards[i]
		if before.ID != after.ID {
			return placement.InvalidPlan("range_move apply requires existing shard order to remain stable")
		}
		if after.State == placement.ShardStateMoving {
			if sourceIdx >= 0 {
				return placement.InvalidPlan("range_move apply found more than one moving source")
			}
			sourceIdx = i
			sourceBefore = before
			sourceAfter = after
		}
	}
	if sourceIdx < 0 {
		return placement.InvalidPlan("range_move apply requires one source shard in %s state", placement.ShardStateMoving)
	}
	target := plan.After.Shards[len(plan.After.Shards)-1]
	if target.ID != uint32(len(current.Shards)) {
		return placement.InvalidPlan("range_move target shard id must be %d, got %d", len(current.Shards), target.ID)
	}
	if sourceAfter.ID != sourceBefore.ID || sourceAfter.State != placement.ShardStateMoving {
		return placement.InvalidPlan("range_move source shard %d must transition to %s", sourceBefore.ID, placement.ShardStateMoving)
	}
	if !sameTokenRanges(sourceAfter.Ranges, sourceBefore.Ranges) {
		return placement.InvalidPlan("range_move source ranges must not shrink before finalization")
	}
	if target.State != placement.ShardStateCreating {
		return placement.InvalidPlan("range_move target shard %d must be %s, got %s", target.ID, placement.ShardStateCreating, target.State)
	}
	if len(target.Ranges) != 1 {
		return placement.InvalidPlan("range_move target shard %d requires exactly one pending range", target.ID)
	}
	if _, err := placement.SubtractTokenRanges(sourceBefore.Ranges, target.Ranges[0]); err != nil {
		return fmt.Errorf("%w: range_move target range [%d,%d) is not owned by source shard %d", placement.ErrInvalidPlacementPlan, target.Ranges[0].Start, target.Ranges[0].End, sourceBefore.ID)
	}
	if err := placement.ValidatePlacement(plan.After); err != nil {
		return err
	}
	return nil
}

func validateSplitApplyRequest(current placement.PlacementCatalog, plan placement.PlacementPlan) error {
	if !plan.RequiresDataCopy {
		return placement.InvalidPlan("split apply expects a transition plan that still requires data copy")
	}
	if len(plan.After.Shards) != len(current.Shards)+1 {
		return placement.InvalidPlan("split apply expects exactly one new shard: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	parentIdx := -1
	var parentBefore, parentAfter placement.ShardPlacement
	for i, before := range current.Shards {
		after := plan.After.Shards[i]
		if before.ID != after.ID {
			return placement.InvalidPlan("split apply requires existing shard order to remain stable")
		}
		if after.State == placement.ShardStateSplitting {
			if parentIdx >= 0 {
				return placement.InvalidPlan("split apply found more than one splitting parent")
			}
			parentIdx = i
			parentBefore = before
			parentAfter = after
		}
	}
	if parentIdx < 0 {
		return placement.InvalidPlan("split apply requires one parent shard in %s state", placement.ShardStateSplitting)
	}
	child := plan.After.Shards[len(plan.After.Shards)-1]
	if child.ID != uint32(len(current.Shards)) {
		return placement.InvalidPlan("split child shard id must be %d, got %d", len(current.Shards), child.ID)
	}
	if parentAfter.ID != parentBefore.ID || parentAfter.State != placement.ShardStateSplitting {
		return placement.InvalidPlan("split parent shard %d must transition to %s", parentBefore.ID, placement.ShardStateSplitting)
	}
	if child.State != placement.ShardStateCreating {
		return placement.InvalidPlan("split child shard %d must be %s, got %s", child.ID, placement.ShardStateCreating, child.State)
	}
	if len(parentBefore.Ranges) != 1 || len(parentAfter.Ranges) != 1 || len(child.Ranges) != 1 {
		return placement.InvalidPlan("split apply requires one parent range and one child range")
	}
	if parentBefore.Ranges[0] != parentAfter.Ranges[0] {
		return placement.InvalidPlan("split parent range must not shrink before finalization")
	}
	if child.Ranges[0].End != parentBefore.Ranges[0].End || !placement.TokenStrictlyInside(parentBefore.Ranges[0], child.Ranges[0].Start) {
		return placement.InvalidPlan("split child range [%d,%d) must be a suffix of parent range [%d,%d)", child.Ranges[0].Start, child.Ranges[0].End, parentBefore.Ranges[0].Start, parentBefore.Ranges[0].End)
	}
	if err := placement.ValidatePlacement(plan.After); err != nil {
		return err
	}
	return nil
}

func (m *Manager) applyAlreadyPreparedTransition(ctx context.Context, current placement.PlacementCatalog, req placement.PlacementApplyRequest) (placement.PlacementApplyResult, bool, error) {
	if (req.Plan.Operation != placement.PlacementOperationSplit && req.Plan.Operation != placement.PlacementOperationRangeMove) || !samePlacement(current, req.Plan.After) {
		return placement.PlacementApplyResult{}, false, nil
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != current.Epoch && req.ExpectedEpoch != req.Plan.BeforeEpoch {
		return placement.PlacementApplyResult{}, true, &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: current.Epoch}
	}
	if err := m.openMissingShardsForPlacement(ctx, current); err != nil {
		return placement.PlacementApplyResult{}, true, err
	}
	steps := make([]placement.PlacementApplyStep, 0, len(req.Plan.Steps))
	for _, step := range req.Plan.Steps {
		status := "already_applied"
		if req.Plan.Operation == placement.PlacementOperationRangeMove {
			status = "pending_finalize"
			if step.Action == "create_shard" || step.Action == "target_membership" {
				status = "already_applied"
			}
		}
		steps = append(steps, placement.PlacementApplyStep{
			Action:  step.Action,
			ShardID: step.ShardID,
			NodeID:  step.NodeID,
			Status:  status,
			Detail:  step.Detail,
		})
	}
	return placement.PlacementApplyResult{
		Operation:   req.Plan.Operation,
		BeforeEpoch: current.Epoch,
		AfterEpoch:  current.Epoch,
		Steps:       steps,
		Placement:   current.Clone(),
	}, true, nil
}

func samePlacement(a, b placement.PlacementCatalog) bool {
	encA, errA := placement.EncodePlacement(a)
	encB, errB := placement.EncodePlacement(b)
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(encA, encB)
}

func sameTokenRanges(a, b []placement.TokenRange) bool {
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

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func applyTimeout(timeoutMS int) time.Duration {
	if timeoutMS <= 0 {
		return 5 * time.Second
	}
	return time.Duration(timeoutMS) * time.Millisecond
}

func (m *Manager) executePlacementStep(step placement.PlacementPlanStep, timeout time.Duration) error {
	switch step.Action {
	case "add_voter", "remove_voter", "wait_catchup":
	default:
		return placement.InvalidPlan("unsupported apply step %q", step.Action)
	}
	if step.ShardID == nil {
		return placement.InvalidPlan("step %q requires shardId", step.Action)
	}
	shard, ok := m.Shard(*step.ShardID)
	if !ok || shard == nil || shard.Raft == nil {
		return fmt.Errorf("cluster: shard %d has no raft group", *step.ShardID)
	}
	switch step.Action {
	case "add_voter":
		if step.NodeID == "" || step.Addr == "" {
			return placement.InvalidPlan("add_voter requires nodeId and addr")
		}
		return shard.Raft.AddVoter(step.NodeID, step.Addr, timeout)
	case "remove_voter":
		if step.NodeID == "" {
			return placement.InvalidPlan("remove_voter requires nodeId")
		}
		return shard.Raft.RemoveServer(step.NodeID, timeout)
	case "wait_catchup":
		return shard.Raft.Barrier(timeout)
	default:
		return nil
	}
}
