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
	PlacementOperationSplit PlacementOperation = "split"
	PlacementOperationMove  PlacementOperation = "move"
	PlacementOperationDrain PlacementOperation = "drain"
)

type PlacementPlanRequest struct {
	Operation    PlacementOperation `json:"operation"`
	ShardID      uint32             `json:"shardId,omitempty"`
	SplitToken   *uint64            `json:"splitToken,omitempty"`
	NewShardID   *uint32            `json:"newShardId,omitempty"`
	SourceNode   string             `json:"sourceNode,omitempty"`
	TargetNode   string             `json:"targetNode,omitempty"`
	TargetNodes  []string           `json:"targetNodes,omitempty"`
	TargetVoters []string           `json:"targetVoters,omitempty"`
	NodeID       string             `json:"nodeId,omitempty"`
	MinVoters    int                `json:"minVoters,omitempty"`
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
	switch req.Operation {
	case PlacementOperationSplit:
		return planSplit(cat, req)
	case PlacementOperationMove:
		return planMove(cat, req)
	case PlacementOperationDrain:
		return planDrain(cat, req)
	default:
		return PlacementPlan{}, invalidPlan("unknown placement operation %q", req.Operation)
	}
}

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

	newShardID := uint32(len(cat.Shards))
	if req.NewShardID != nil {
		newShardID = *req.NewShardID
	}
	if int(newShardID) != len(cat.Shards) {
		return PlacementPlan{}, invalidPlan("new shard id must be %d to keep placement IDs contiguous", len(cat.Shards))
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

	voters := append([]string(nil), shard.Voters...)
	if len(req.TargetVoters) > 0 {
		if err := validateNodeSet(cat, req.TargetVoters, 1); err != nil {
			return PlacementPlan{}, err
		}
		voters = sortedUnique(req.TargetVoters)
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

	return PlacementPlan{
		Operation:        PlacementOperationSplit,
		BeforeEpoch:      cat.Epoch,
		AfterEpoch:       after.Epoch,
		Before:           cat.Clone(),
		After:            after,
		RequiresDataCopy: true,
		RequiresRestart:  true,
		ApplySupported:   false,
		Warnings: []string{
			"split is a transition plan only; the parent keeps serving the full range until data copy and child activation are implemented",
			"opening the new shard currently requires restarting nodes with the expanded placement catalog",
		},
		Steps: []PlacementPlanStep{
			{Action: "create_shard", ShardID: u32ptr(newShardID), Detail: fmt.Sprintf("prepare shard %d with range [%d,%d)", newShardID, childRange.Start, childRange.End)},
			{Action: "copy_range", ShardID: u32ptr(shard.ID), Detail: fmt.Sprintf("copy token range [%d,%d) from shard %d to shard %d", childRange.Start, childRange.End, shard.ID, newShardID)},
			{Action: "activate_child", ShardID: u32ptr(newShardID), Detail: "mark child active after copy verification"},
			{Action: "shrink_parent", ShardID: u32ptr(shard.ID), Detail: fmt.Sprintf("remove child range [%d,%d) from shard %d", childRange.Start, childRange.End, shard.ID)},
		},
	}, nil
}

func planMove(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	shardIdx, shard, err := findShard(cat, req.ShardID)
	if err != nil {
		return PlacementPlan{}, err
	}
	if len(shard.Voters) == 0 {
		return PlacementPlan{}, invalidPlan("shard %d has no voters to move", shard.ID)
	}

	minVoters := minVoters(req.MinVoters)
	var voters []string
	var steps []PlacementPlanStep
	if len(req.TargetVoters) > 0 {
		voters = sortedUnique(req.TargetVoters)
		if err := validateNodeSet(cat, voters, minVoters); err != nil {
			return PlacementPlan{}, err
		}
		steps = membershipDiffSteps(cat, shard.ID, shard.Voters, voters)
	} else {
		if req.SourceNode == "" || req.TargetNode == "" {
			return PlacementPlan{}, invalidPlan("move requires sourceNode and targetNode when targetVoters is empty")
		}
		if req.SourceNode == req.TargetNode {
			return PlacementPlan{}, invalidPlan("sourceNode and targetNode must differ")
		}
		if err := validateNodeSet(cat, []string{req.TargetNode}, 1); err != nil {
			return PlacementPlan{}, err
		}
		if !containsString(shard.Voters, req.SourceNode) {
			return PlacementPlan{}, invalidPlan("source node %q is not a voter for shard %d", req.SourceNode, shard.ID)
		}
		voters = replaceVoter(shard.Voters, req.SourceNode, req.TargetNode)
		if len(voters) < minVoters {
			return PlacementPlan{}, invalidPlan("move would leave shard %d with %d voters; minVoters=%d", shard.ID, len(voters), minVoters)
		}
		steps = append(steps,
			PlacementPlanStep{Action: "add_voter", ShardID: u32ptr(shard.ID), NodeID: req.TargetNode, Addr: cat.Nodes[req.TargetNode].RaftAddr},
			PlacementPlanStep{Action: "wait_catchup", ShardID: u32ptr(shard.ID), NodeID: req.TargetNode},
			PlacementPlanStep{Action: "remove_voter", ShardID: u32ptr(shard.ID), NodeID: req.SourceNode},
		)
	}

	after := nextCatalog(cat)
	after.Shards[shardIdx].State = ShardStateActive
	after.Shards[shardIdx].Epoch = after.Epoch
	after.Shards[shardIdx].Voters = voters
	after.Shards[shardIdx].NonVoters = removeAny(after.Shards[shardIdx].NonVoters, voters)
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementPlan{}, err
	}

	return PlacementPlan{
		Operation:        PlacementOperationMove,
		BeforeEpoch:      cat.Epoch,
		AfterEpoch:       after.Epoch,
		Before:           cat.Clone(),
		After:            after,
		Steps:            steps,
		RequiresDataCopy: false,
		RequiresRestart:  false,
		ApplySupported:   true,
		Warnings: []string{
			"move applies Raft membership steps first and publishes the new placement epoch only after those steps succeed",
		},
	}, nil
}

func planDrain(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	if req.NodeID == "" {
		return PlacementPlan{}, invalidPlan("drain requires nodeId")
	}
	if _, ok := cat.Nodes[req.NodeID]; !ok {
		return PlacementPlan{}, invalidPlan("node %q does not exist in placement", req.NodeID)
	}
	targets := sortedUnique(req.TargetNodes)
	if len(targets) == 0 && req.TargetNode != "" {
		targets = []string{req.TargetNode}
	}
	if err := validateNodeSet(cat, targets, 0); err != nil {
		return PlacementPlan{}, err
	}

	minVoters := minVoters(req.MinVoters)
	after := nextCatalog(cat)
	node := after.Nodes[req.NodeID]
	node.State = NodeStateDraining
	after.Nodes[req.NodeID] = node

	var steps []PlacementPlanStep
	affected := 0
	for i := range after.Shards {
		sh := &after.Shards[i]
		if !containsString(sh.Voters, req.NodeID) && !containsString(sh.NonVoters, req.NodeID) {
			continue
		}
		affected++
		for _, target := range targets {
			if !containsString(sh.Voters, target) {
				sh.Voters = append(sh.Voters, target)
				steps = append(steps, PlacementPlanStep{Action: "add_voter", ShardID: u32ptr(sh.ID), NodeID: target, Addr: after.Nodes[target].RaftAddr})
			}
		}
		sh.Voters = removeString(sh.Voters, req.NodeID)
		sh.NonVoters = removeString(sh.NonVoters, req.NodeID)
		if len(sh.Voters) < minVoters {
			return PlacementPlan{}, invalidPlan("drain would leave shard %d with %d voters; minVoters=%d", sh.ID, len(sh.Voters), minVoters)
		}
		sh.State = ShardStateActive
		sh.Epoch = after.Epoch
		steps = append(steps,
			PlacementPlanStep{Action: "wait_catchup", ShardID: u32ptr(sh.ID), Detail: strings.Join(targets, ",")},
			PlacementPlanStep{Action: "remove_voter", ShardID: u32ptr(sh.ID), NodeID: req.NodeID},
		)
	}
	if affected == 0 {
		return PlacementPlan{}, invalidPlan("node %q is not present in any shard membership", req.NodeID)
	}

	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementPlan{}, err
	}
	warnings := []string{
		"drain is a plan only; apply Raft membership changes shard by shard before stopping the node",
	}
	if len(targets) == 0 {
		warnings = append(warnings, "no target nodes supplied; this plan only removes the draining node and may reduce fault tolerance")
	}

	return PlacementPlan{
		Operation:        PlacementOperationDrain,
		BeforeEpoch:      cat.Epoch,
		AfterEpoch:       after.Epoch,
		Before:           cat.Clone(),
		After:            after,
		Steps:            steps,
		Warnings:         warnings,
		RequiresDataCopy: false,
		RequiresRestart:  false,
		ApplySupported:   true,
	}, nil
}

func (m *Manager) ApplyPlacement(ctx context.Context, req PlacementApplyRequest) (PlacementApplyResult, error) {
	if err := m.RefreshPlacement(); err != nil {
		return PlacementApplyResult{}, err
	}
	current := m.Placement()
	if err := validateApplyRequest(current, req); err != nil {
		return PlacementApplyResult{}, err
	}
	timeout := applyTimeout(req.TimeoutMS)
	applied := make([]PlacementApplyStep, 0, len(req.Plan.Steps))
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
	if plan.Operation != PlacementOperationMove && plan.Operation != PlacementOperationDrain {
		return invalidPlan("apply supports move and drain plans only, got %q", plan.Operation)
	}
	if plan.RequiresDataCopy || plan.RequiresRestart {
		return invalidPlan("plan requires data copy or restart and cannot be applied online")
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
	if len(plan.After.Shards) != len(current.Shards) {
		return invalidPlan("apply cannot change shard count: before=%d after=%d", len(current.Shards), len(plan.After.Shards))
	}
	if err := ValidatePlacement(plan.After); err != nil {
		return err
	}
	return nil
}

func samePlacement(a, b PlacementCatalog) bool {
	encA, errA := encodePlacement(a)
	encB, errB := encodePlacement(b)
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(encA, encB)
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
