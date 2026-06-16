package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
)

type PlacementOperation string

const (
	PlacementOperationSplit        PlacementOperation = "split"
	PlacementOperationMove         PlacementOperation = "move"
	PlacementOperationRangeMove    PlacementOperation = "range_move"
	PlacementOperationDrain        PlacementOperation = "drain"
	PlacementOperationDecommission PlacementOperation = "decommission"
)

type PlacementPlanRequest struct {
	Operation     PlacementOperation `json:"operation"`
	ShardID       uint32             `json:"shardId,omitempty"`
	SplitToken    *uint64            `json:"splitToken,omitempty"`
	NewShardID    *uint32            `json:"newShardId,omitempty"`
	TargetShardID *uint32            `json:"targetShardId,omitempty"`
	RangeStart    *uint64            `json:"rangeStart,omitempty"`
	RangeEnd      *uint64            `json:"rangeEnd,omitempty"`
	SourceNode    string             `json:"sourceNode,omitempty"`
	TargetNode    string             `json:"targetNode,omitempty"`
	TargetNodes   []string           `json:"targetNodes,omitempty"`
	TargetVoters  []string           `json:"targetVoters,omitempty"`
	NodeID        string             `json:"nodeId,omitempty"`
	MinVoters     int                `json:"minVoters,omitempty"`
}

type PlacementPlanStep struct {
	Action  string  `json:"action"`
	ShardID *uint32 `json:"shardId,omitempty"`
	NodeID  string  `json:"nodeId,omitempty"`
	Addr    string  `json:"addr,omitempty"`
	Detail  string  `json:"detail,omitempty"`
}

type PlacementPlan struct {
	Operation        PlacementOperation  `json:"operation"`
	BeforeEpoch      uint64              `json:"beforeEpoch"`
	AfterEpoch       uint64              `json:"afterEpoch"`
	Before           PlacementCatalog    `json:"before"`
	After            PlacementCatalog    `json:"after"`
	Steps            []PlacementPlanStep `json:"steps,omitempty"`
	Warnings         []string            `json:"warnings,omitempty"`
	RequiresDataCopy bool                `json:"requiresDataCopy"`
	RequiresRestart  bool                `json:"requiresRestart"`
	ApplySupported   bool                `json:"applySupported"`
}

type PlacementApplyRequest struct {
	Plan          PlacementPlan `json:"plan"`
	ExpectedEpoch uint64        `json:"expectedEpoch,omitempty"`
	TimeoutMS     int           `json:"timeoutMs,omitempty"`
}

type PlacementApplyStep struct {
	Action  string  `json:"action"`
	ShardID *uint32 `json:"shardId,omitempty"`
	NodeID  string  `json:"nodeId,omitempty"`
	Status  string  `json:"status"`
	Detail  string  `json:"detail,omitempty"`
}

type PlacementApplyResult struct {
	Operation   PlacementOperation   `json:"operation"`
	BeforeEpoch uint64               `json:"beforeEpoch"`
	AfterEpoch  uint64               `json:"afterEpoch"`
	Steps       []PlacementApplyStep `json:"steps,omitempty"`
	Placement   PlacementCatalog     `json:"placement"`
}

var ErrInvalidPlacementPlan = errors.New("cluster: invalid placement plan")

func (m *Manager) PlanPlacement(req PlacementPlanRequest) (PlacementPlan, error) {
	if err := m.RefreshPlacement(); err != nil {
		return PlacementPlan{}, err
	}
	return BuildPlacementPlan(m.Placement(), req)
}

func BuildPlacementPlan(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	cat.normalize()
	if err := ValidatePlacement(cat); err != nil {
		return PlacementPlan{}, err
	}
	strategy, ok := defaultPlanStrategies[req.Operation]
	if !ok {
		return PlacementPlan{}, invalidPlan("unknown placement operation %q", req.Operation)
	}
	return strategy.Plan(cat, req)
}


func (m *Manager) ApplyPlacement(ctx context.Context, req PlacementApplyRequest) (PlacementApplyResult, error) {
	if err := m.RefreshPlacement(); err != nil {
		return PlacementApplyResult{}, err
	}
	current := m.Placement()
	if result, ok, err := m.applyAlreadyPreparedTransition(ctx, current, req); ok || err != nil {
		return result, err
	}
	if err := validateApplyRequest(current, req); err != nil {
		return PlacementApplyResult{}, err
	}
	timeout := applyTimeout(req.TimeoutMS)
	applied := make([]PlacementApplyStep, 0, len(req.Plan.Steps))
	if req.Plan.Operation == PlacementOperationSplit || req.Plan.Operation == PlacementOperationRangeMove {
		if err := m.openMissingShardsForPlacement(ctx, req.Plan.After); err != nil {
			return PlacementApplyResult{}, err
		}
		for _, step := range req.Plan.Steps {
			status := "ok"
			if req.Plan.Operation == PlacementOperationRangeMove {
				status = "pending_finalize"
				if step.Action == "create_shard" || step.Action == "target_membership" {
					status = "ok"
				}
			}
			applied = append(applied, PlacementApplyStep{
				Action:  step.Action,
				ShardID: step.ShardID,
				NodeID:  step.NodeID,
				Status:  status,
				Detail:  step.Detail,
			})
		}
	} else if req.Plan.Operation == PlacementOperationDecommission {
		for _, step := range req.Plan.Steps {
			if err := ctx.Err(); err != nil {
				return PlacementApplyResult{}, err
			}
			applied = append(applied, PlacementApplyStep{
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
				return PlacementApplyResult{}, err
			}
			if err := m.executePlacementStep(step, timeout); err != nil {
				return PlacementApplyResult{}, err
			}
			applied = append(applied, PlacementApplyStep{
				Action:  step.Action,
				ShardID: step.ShardID,
				NodeID:  step.NodeID,
				Status:  "ok",
				Detail:  step.Detail,
			})
		}
	}
	if err := m.persistPlacementSnapshotStrict(m.placementPath, req.Plan.After); err != nil {
		return PlacementApplyResult{}, err
	}
	if err := m.applyPlacement(req.Plan.After, false); err != nil {
		return PlacementApplyResult{}, err
	}
	return PlacementApplyResult{
		Operation:   req.Plan.Operation,
		BeforeEpoch: current.Epoch,
		AfterEpoch:  req.Plan.After.Epoch,
		Steps:       applied,
		Placement:   req.Plan.After.Clone(),
	}, nil
}

func validateApplyRequest(current PlacementCatalog, req PlacementApplyRequest) error {
	plan := req.Plan
	if plan.Operation != PlacementOperationMove && plan.Operation != PlacementOperationDrain && plan.Operation != PlacementOperationSplit && plan.Operation != PlacementOperationRangeMove && plan.Operation != PlacementOperationDecommission {
		return invalidPlan("apply supports split, move, range_move, drain and decommission plans only, got %q", plan.Operation)
	}
	if plan.RequiresRestart {
		return invalidPlan("plan requires restart and cannot be applied online")
	}
	if plan.RequiresDataCopy && plan.Operation != PlacementOperationSplit && plan.Operation != PlacementOperationRangeMove {
		return invalidPlan("plan requires data copy and cannot be applied online")
	}
	if !plan.ApplySupported {
		return invalidPlan("plan is not marked apply-supported")
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != current.Epoch {
		return &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: current.Epoch}
	}
	if plan.BeforeEpoch != current.Epoch || plan.Before.Epoch != current.Epoch {
		return &StaleRouteError{ClientEpoch: plan.BeforeEpoch, CurrentEpoch: current.Epoch}
	}
	if !samePlacement(current, plan.Before) {
		return invalidPlan("plan before catalog does not match current placement")
	}
	if plan.After.Epoch <= current.Epoch {
		return invalidPlan("after epoch %d must be greater than current epoch %d", plan.After.Epoch, current.Epoch)
	}
	if plan.Operation == PlacementOperationSplit {
		return validateSplitApplyRequest(current, plan)
	}
	if plan.Operation == PlacementOperationRangeMove {
		return validateRangeMoveApplyRequest(current, plan)
	}
	if plan.Operation == PlacementOperationDecommission {
		return validateDecommissionApplyRequest(current, plan)
	}
	if len(plan.After.Shards) != len(current.Shards) {
		return invalidPlan("apply cannot change shard count: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	if err := ValidatePlacement(plan.After); err != nil {
		return err
	}
	return nil
}

func validateDecommissionApplyRequest(current PlacementCatalog, plan PlacementPlan) error {
	if plan.RequiresDataCopy {
		return invalidPlan("decommission apply must not require data copy")
	}
	if len(plan.After.Shards) != len(current.Shards) {
		return invalidPlan("decommission apply cannot change shard count: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	if err := ValidatePlacement(plan.After); err != nil {
		return err
	}

	var target string
	for id, beforeNode := range current.Nodes {
		afterNode, ok := plan.After.Nodes[id]
		if !ok {
			return invalidPlan("decommission apply cannot remove node %q from placement", id)
		}
		if beforeNode.State == afterNode.State {
			continue
		}
		if target != "" {
			return invalidPlan("decommission apply can change one node state only")
		}
		if beforeNode.State != NodeStateDraining || afterNode.State != NodeStateDecommissioned {
			return invalidPlan("decommission apply requires node %q to transition %s -> %s, got %s -> %s", id, NodeStateDraining, NodeStateDecommissioned, beforeNode.State, afterNode.State)
		}
		target = id
	}
	for id := range plan.After.Nodes {
		if _, ok := current.Nodes[id]; !ok {
			return invalidPlan("decommission apply cannot add node %q", id)
		}
	}
	if target == "" {
		return invalidPlan("decommission apply found no node state transition")
	}
	if blockers := placementNodeActiveReferences(current, target); len(blockers) > 0 {
		return invalidPlan("node %q still has active placement references: %s", target, strings.Join(blockers, "; "))
	}
	for i, before := range current.Shards {
		after := plan.After.Shards[i]
		if before.ID != after.ID || before.State != after.State || before.Epoch != after.Epoch || before.LeaderHint != after.LeaderHint || !sameTokenRanges(before.Ranges, after.Ranges) || !sameStringSet(before.Voters, after.Voters) || !sameStringSet(before.NonVoters, after.NonVoters) {
			return invalidPlan("decommission apply cannot change shard %d placement", before.ID)
		}
	}
	return nil
}

func validateRangeMoveApplyRequest(current PlacementCatalog, plan PlacementPlan) error {
	if !plan.RequiresDataCopy {
		return invalidPlan("range_move apply expects a transition plan that still requires data copy")
	}
	if len(plan.After.Shards) != len(current.Shards)+1 {
		return invalidPlan("range_move apply expects exactly one target shard: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	sourceIdx := -1
	var sourceBefore, sourceAfter ShardPlacement
	for i, before := range current.Shards {
		after := plan.After.Shards[i]
		if before.ID != after.ID {
			return invalidPlan("range_move apply requires existing shard order to remain stable")
		}
		if after.State == ShardStateMoving {
			if sourceIdx >= 0 {
				return invalidPlan("range_move apply found more than one moving source")
			}
			sourceIdx = i
			sourceBefore = before
			sourceAfter = after
		}
	}
	if sourceIdx < 0 {
		return invalidPlan("range_move apply requires one source shard in %s state", ShardStateMoving)
	}
	target := plan.After.Shards[len(plan.After.Shards)-1]
	if target.ID != uint32(len(current.Shards)) {
		return invalidPlan("range_move target shard id must be %d, got %d", len(current.Shards), target.ID)
	}
	if sourceAfter.ID != sourceBefore.ID || sourceAfter.State != ShardStateMoving {
		return invalidPlan("range_move source shard %d must transition to %s", sourceBefore.ID, ShardStateMoving)
	}
	if !sameTokenRanges(sourceAfter.Ranges, sourceBefore.Ranges) {
		return invalidPlan("range_move source ranges must not shrink before finalization")
	}
	if target.State != ShardStateCreating {
		return invalidPlan("range_move target shard %d must be %s, got %s", target.ID, ShardStateCreating, target.State)
	}
	if len(target.Ranges) != 1 {
		return invalidPlan("range_move target shard %d requires exactly one pending range", target.ID)
	}
	if _, err := subtractTokenRanges(sourceBefore.Ranges, target.Ranges[0]); err != nil {
		return fmt.Errorf("%w: range_move target range [%d,%d) is not owned by source shard %d", ErrInvalidPlacementPlan, target.Ranges[0].Start, target.Ranges[0].End, sourceBefore.ID)
	}
	if err := ValidatePlacement(plan.After); err != nil {
		return err
	}
	return nil
}

func validateSplitApplyRequest(current PlacementCatalog, plan PlacementPlan) error {
	if !plan.RequiresDataCopy {
		return invalidPlan("split apply expects a transition plan that still requires data copy")
	}
	if len(plan.After.Shards) != len(current.Shards)+1 {
		return invalidPlan("split apply expects exactly one new shard: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	parentIdx := -1
	var parentBefore, parentAfter ShardPlacement
	for i, before := range current.Shards {
		after := plan.After.Shards[i]
		if before.ID != after.ID {
			return invalidPlan("split apply requires existing shard order to remain stable")
		}
		if after.State == ShardStateSplitting {
			if parentIdx >= 0 {
				return invalidPlan("split apply found more than one splitting parent")
			}
			parentIdx = i
			parentBefore = before
			parentAfter = after
		}
	}
	if parentIdx < 0 {
		return invalidPlan("split apply requires one parent shard in %s state", ShardStateSplitting)
	}
	child := plan.After.Shards[len(plan.After.Shards)-1]
	if child.ID != uint32(len(current.Shards)) {
		return invalidPlan("split child shard id must be %d, got %d", len(current.Shards), child.ID)
	}
	if parentAfter.ID != parentBefore.ID || parentAfter.State != ShardStateSplitting {
		return invalidPlan("split parent shard %d must transition to %s", parentBefore.ID, ShardStateSplitting)
	}
	if child.State != ShardStateCreating {
		return invalidPlan("split child shard %d must be %s, got %s", child.ID, ShardStateCreating, child.State)
	}
	if len(parentBefore.Ranges) != 1 || len(parentAfter.Ranges) != 1 || len(child.Ranges) != 1 {
		return invalidPlan("split apply requires one parent range and one child range")
	}
	if parentBefore.Ranges[0] != parentAfter.Ranges[0] {
		return invalidPlan("split parent range must not shrink before finalization")
	}
	if child.Ranges[0].End != parentBefore.Ranges[0].End || !tokenStrictlyInside(parentBefore.Ranges[0], child.Ranges[0].Start) {
		return invalidPlan("split child range [%d,%d) must be a suffix of parent range [%d,%d)", child.Ranges[0].Start, child.Ranges[0].End, parentBefore.Ranges[0].Start, parentBefore.Ranges[0].End)
	}
	if err := ValidatePlacement(plan.After); err != nil {
		return err
	}
	return nil
}

func (m *Manager) applyAlreadyPreparedTransition(ctx context.Context, current PlacementCatalog, req PlacementApplyRequest) (PlacementApplyResult, bool, error) {
	if (req.Plan.Operation != PlacementOperationSplit && req.Plan.Operation != PlacementOperationRangeMove) || !samePlacement(current, req.Plan.After) {
		return PlacementApplyResult{}, false, nil
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != current.Epoch && req.ExpectedEpoch != req.Plan.BeforeEpoch {
		return PlacementApplyResult{}, true, &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: current.Epoch}
	}
	if err := m.openMissingShardsForPlacement(ctx, current); err != nil {
		return PlacementApplyResult{}, true, err
	}
	steps := make([]PlacementApplyStep, 0, len(req.Plan.Steps))
	for _, step := range req.Plan.Steps {
		status := "already_applied"
		if req.Plan.Operation == PlacementOperationRangeMove {
			status = "pending_finalize"
			if step.Action == "create_shard" || step.Action == "target_membership" {
				status = "already_applied"
			}
		}
		steps = append(steps, PlacementApplyStep{
			Action:  step.Action,
			ShardID: step.ShardID,
			NodeID:  step.NodeID,
			Status:  status,
			Detail:  step.Detail,
		})
	}
	return PlacementApplyResult{
		Operation:   req.Plan.Operation,
		BeforeEpoch: current.Epoch,
		AfterEpoch:  current.Epoch,
		Steps:       steps,
		Placement:   current.Clone(),
	}, true, nil
}

func samePlacement(a, b PlacementCatalog) bool {
	encA, errA := encodePlacement(a)
	encB, errB := encodePlacement(b)
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(encA, encB)
}

func sameTokenRanges(a, b []TokenRange) bool {
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

func (m *Manager) executePlacementStep(step PlacementPlanStep, timeout time.Duration) error {
	switch step.Action {
	case "add_voter", "remove_voter", "wait_catchup":
	default:
		return invalidPlan("unsupported apply step %q", step.Action)
	}
	if step.ShardID == nil {
		return invalidPlan("step %q requires shardId", step.Action)
	}
	shard, ok := m.Shard(*step.ShardID)
	if !ok || shard == nil || shard.Raft == nil {
		return fmt.Errorf("cluster: shard %d has no raft group", *step.ShardID)
	}
	switch step.Action {
	case "add_voter":
		if step.NodeID == "" || step.Addr == "" {
			return invalidPlan("add_voter requires nodeId and addr")
		}
		return shard.Raft.AddVoter(step.NodeID, step.Addr, timeout)
	case "remove_voter":
		if step.NodeID == "" {
			return invalidPlan("remove_voter requires nodeId")
		}
		return shard.Raft.RemoveServer(step.NodeID, timeout)
	case "wait_catchup":
		return shard.Raft.Barrier(timeout)
	default:
		return nil
	}
}

func nextCatalog(cat PlacementCatalog) PlacementCatalog {
	after := cat.Clone()
	after.Epoch = cat.Epoch + 1
	after.UpdatedAtUnix = time.Now().Unix()
	return after
}

func findShard(cat PlacementCatalog, shardID uint32) (int, ShardPlacement, error) {
	for i, sh := range cat.Shards {
		if sh.ID == shardID {
			return i, sh, nil
		}
	}
	return 0, ShardPlacement{}, invalidPlan("shard %d does not exist", shardID)
}

func validateNodeSet(cat PlacementCatalog, ids []string, min int) error {
	if len(ids) < min {
		return invalidPlan("need at least %d nodes, got %d", min, len(ids))
	}
	seen := map[string]struct{}{}
	for _, id := range ids {
		if id == "" {
			return invalidPlan("node id cannot be empty")
		}
		if _, dup := seen[id]; dup {
			return invalidPlan("duplicate node %q", id)
		}
		seen[id] = struct{}{}
		node, ok := cat.Nodes[id]
		if !ok {
			return invalidPlan("node %q does not exist in placement", id)
		}
		if node.State != "" && node.State != NodeStateActive {
			return invalidPlan("node %q is not active: %s", id, node.State)
		}
	}
	return nil
}

func placementNodeActiveReferences(cat PlacementCatalog, nodeID string) []string {
	var blockers []string
	for _, shard := range cat.Shards {
		if shard.State == ShardStateDecommissioned {
			continue
		}
		if containsString(shard.Voters, nodeID) {
			blockers = append(blockers, fmt.Sprintf("shard %d voter state=%s ranges=%d", shard.ID, shard.State, len(shard.Ranges)))
		}
		if containsString(shard.NonVoters, nodeID) {
			blockers = append(blockers, fmt.Sprintf("shard %d non-voter state=%s", shard.ID, shard.State))
		}
		if shard.LeaderHint == nodeID {
			blockers = append(blockers, fmt.Sprintf("shard %d leader hint state=%s", shard.ID, shard.State))
		}
	}
	sort.Strings(blockers)
	return blockers
}

func minVoters(v int) int {
	if v <= 0 {
		return 1
	}
	return v
}

func invalidPlan(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPlacementPlan, fmt.Sprintf(format, args...))
}

func midpointToken(r TokenRange) uint64 {
	start := new(big.Int).SetUint64(r.Start)
	end := new(big.Int).SetUint64(r.End)
	if r.Start == r.End || r.Start > r.End {
		end.Add(end, bigTokenSpace)
	}
	mid := new(big.Int).Add(start, end)
	mid.Div(mid, big.NewInt(2))
	if mid.Cmp(bigTokenSpace) >= 0 {
		mid.Sub(mid, bigTokenSpace)
	}
	return mid.Uint64()
}

func tokenStrictlyInside(r TokenRange, token uint64) bool {
	if token == r.Start || token == r.End {
		return false
	}
	return r.Contains(token)
}

func splitRange(r TokenRange, token uint64) (TokenRange, TokenRange) {
	return TokenRange{Start: r.Start, End: token}, TokenRange{Start: token, End: r.End}
}

func subtractTokenRanges(ranges []TokenRange, remove TokenRange) ([]TokenRange, error) {
	if len(ranges) == 0 {
		return nil, fmt.Errorf("source has no ranges")
	}
	owners := make([]tokenSegment, 0, len(ranges)*2)
	for _, rng := range ranges {
		owners = append(owners, tokenRangeSegments(rng)...)
	}
	sortTokenSegments(owners)
	removeSegs := tokenRangeSegments(remove)
	sortTokenSegments(removeSegs)
	for _, seg := range removeSegs {
		if !segmentCoveredBySegments(seg, owners) {
			return nil, fmt.Errorf("range [%d,%d) is not fully owned", remove.Start, remove.End)
		}
	}

	remaining := cloneTokenSegments(owners)
	for _, rm := range removeSegs {
		next := make([]tokenSegment, 0, len(remaining)+1)
		for _, seg := range remaining {
			next = append(next, subtractTokenSegment(seg, rm)...)
		}
		remaining = next
	}
	sortTokenSegments(remaining)
	remaining = mergeAdjacentTokenSegments(remaining)
	return tokenSegmentsToRanges(remaining), nil
}

func subtractTokenSegment(seg, remove tokenSegment) []tokenSegment {
	if remove.end.Cmp(seg.start) <= 0 || remove.start.Cmp(seg.end) >= 0 {
		return []tokenSegment{cloneTokenSegment(seg)}
	}
	var out []tokenSegment
	if remove.start.Cmp(seg.start) > 0 {
		out = append(out, tokenSegment{
			start: new(big.Int).Set(seg.start),
			end:   minBig(remove.start, seg.end),
		})
	}
	if remove.end.Cmp(seg.end) < 0 {
		out = append(out, tokenSegment{
			start: maxBig(remove.end, seg.start),
			end:   new(big.Int).Set(seg.end),
		})
	}
	return out
}

func segmentCoveredBySegments(seg tokenSegment, owners []tokenSegment) bool {
	coveredUntil := new(big.Int).Set(seg.start)
	for _, owner := range owners {
		if owner.end.Cmp(coveredUntil) <= 0 {
			continue
		}
		if owner.start.Cmp(coveredUntil) > 0 {
			return false
		}
		if owner.end.Cmp(seg.end) >= 0 {
			return true
		}
		coveredUntil.Set(owner.end)
	}
	return false
}

func tokenSegmentsToRanges(segs []tokenSegment) []TokenRange {
	out := make([]TokenRange, 0, len(segs))
	for _, seg := range segs {
		if seg.start.Cmp(seg.end) == 0 {
			continue
		}
		out = append(out, TokenRange{Start: bigTokenToUint64(seg.start), End: bigTokenToUint64(seg.end)})
	}
	return out
}

func bigTokenToUint64(v *big.Int) uint64 {
	if v.Cmp(bigTokenSpace) == 0 {
		return 0
	}
	return v.Uint64()
}

func sortTokenSegments(segs []tokenSegment) {
	sort.Slice(segs, func(i, j int) bool { return segs[i].start.Cmp(segs[j].start) < 0 })
}

func mergeAdjacentTokenSegments(segs []tokenSegment) []tokenSegment {
	if len(segs) <= 1 {
		return segs
	}
	out := make([]tokenSegment, 0, len(segs))
	current := cloneTokenSegment(segs[0])
	for _, seg := range segs[1:] {
		if current.end.Cmp(seg.start) == 0 {
			current.end = new(big.Int).Set(seg.end)
			continue
		}
		out = append(out, current)
		current = cloneTokenSegment(seg)
	}
	out = append(out, current)
	return out
}

func cloneTokenSegments(in []tokenSegment) []tokenSegment {
	out := make([]tokenSegment, 0, len(in))
	for _, seg := range in {
		out = append(out, cloneTokenSegment(seg))
	}
	return out
}

func cloneTokenSegment(seg tokenSegment) tokenSegment {
	return tokenSegment{start: new(big.Int).Set(seg.start), end: new(big.Int).Set(seg.end)}
}

func minBig(a, b *big.Int) *big.Int {
	if a.Cmp(b) <= 0 {
		return new(big.Int).Set(a)
	}
	return new(big.Int).Set(b)
}

func maxBig(a, b *big.Int) *big.Int {
	if a.Cmp(b) >= 0 {
		return new(big.Int).Set(a)
	}
	return new(big.Int).Set(b)
}

func replaceVoter(voters []string, source, target string) []string {
	out := make([]string, 0, len(voters)+1)
	replaced := false
	for _, voter := range voters {
		switch voter {
		case source:
			if !containsString(out, target) {
				out = append(out, target)
			}
			replaced = true
		default:
			if !containsString(out, voter) {
				out = append(out, voter)
			}
		}
	}
	if !replaced && !containsString(out, target) {
		out = append(out, target)
	}
	sort.Strings(out)
	return out
}

func membershipDiffSteps(cat PlacementCatalog, shardID uint32, current, target []string) []PlacementPlanStep {
	var steps []PlacementPlanStep
	for _, nodeID := range target {
		if containsString(current, nodeID) {
			continue
		}
		node := cat.Nodes[nodeID]
		steps = append(steps, PlacementPlanStep{Action: "add_voter", ShardID: u32ptr(shardID), NodeID: nodeID, Addr: node.RaftAddr})
	}
	for _, nodeID := range target {
		if !containsString(current, nodeID) {
			steps = append(steps, PlacementPlanStep{Action: "wait_catchup", ShardID: u32ptr(shardID), NodeID: nodeID})
		}
	}
	for _, nodeID := range current {
		if containsString(target, nodeID) {
			continue
		}
		steps = append(steps, PlacementPlanStep{Action: "remove_voter", ShardID: u32ptr(shardID), NodeID: nodeID})
	}
	return steps
}

func sortedUnique(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" || containsString(out, v) {
			continue
		}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func containsString(in []string, v string) bool {
	for _, existing := range in {
		if existing == v {
			return true
		}
	}
	return false
}

func removeAny(in, remove []string) []string {
	out := in[:0]
	for _, v := range in {
		if !containsString(remove, v) {
			out = append(out, v)
		}
	}
	return out
}

func u32ptr(v uint32) *uint32 { return &v }
