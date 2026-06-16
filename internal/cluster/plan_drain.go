package cluster

import (
	"fmt"
	"sort"
)

// planDrain derives the placement-plan that removes a node from
// every shard membership it participates in. Drain is plan-only;
// callers must apply the resulting Raft membership steps shard by
// shard before stopping the node.
func planDrain(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	if req.NodeID == "" {
		return PlacementPlan{}, invalidPlan("drain requires nodeId")
	}
	if _, ok := cat.Nodes[req.NodeID]; !ok {
		return PlacementPlan{}, invalidPlan("node %q does not exist in placement", req.NodeID)
	}
	targets, err := drainTargets(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}

	after := nextCatalog(cat)
	node := after.Nodes[req.NodeID]
	node.State = NodeStateDraining
	after.Nodes[req.NodeID] = node

	steps, policyWarnings, affected, err := drainShards(after, req, targets)
	if err != nil {
		return PlacementPlan{}, err
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
		warnings = append(warnings, "no target nodes supplied; placement policy selected replacement voters for affected shards")
	}
	warnings = append(warnings, policyWarnings...)

	return PlacementPlan{
		Operation:      PlacementOperationDrain,
		BeforeEpoch:    cat.Epoch,
		AfterEpoch:     after.Epoch,
		Before:         cat.Clone(),
		After:          after,
		Steps:          steps,
		Warnings:       warnings,
		ApplySupported: true,
	}, nil
}

// drainTargets collects the caller's preferred replacement nodes
// (TargetNodes wins over the legacy single TargetNode field) and
// rejects the draining node from the set.
func drainTargets(cat PlacementCatalog, req PlacementPlanRequest) ([]string, error) {
	targets := sortedUnique(req.TargetNodes)
	if len(targets) == 0 && req.TargetNode != "" {
		targets = []string{req.TargetNode}
	}
	if err := validateNodeSet(cat, targets, 0); err != nil {
		return nil, err
	}
	if containsString(targets, req.NodeID) {
		return nil, invalidPlan("drain target nodes cannot include draining node %q", req.NodeID)
	}
	return targets, nil
}

// drainShards walks every shard that references the draining node
// and rewrites membership in place on `after`. Returns the
// per-shard Raft steps, any policy-emitted warnings, and the count
// of affected shards.
func drainShards(after PlacementCatalog, req PlacementPlanRequest, targets []string) ([]PlacementPlanStep, []string, int, error) {
	var steps []PlacementPlanStep
	var policyWarnings []string
	affected := 0
	for i := range after.Shards {
		sh := &after.Shards[i]
		if !drainShardReferences(*sh, req.NodeID) {
			continue
		}
		affected++
		currentVoters := append([]string(nil), sh.Voters...)
		remainingVoters := removeString(append([]string(nil), sh.Voters...), req.NodeID)
		voters, shardWarnings, err := drainPickVoters(after, *sh, remainingVoters, targets, req)
		if err != nil {
			return nil, nil, 0, err
		}
		policyWarnings = append(policyWarnings, shardWarnings...)
		steps = append(steps, membershipDiffSteps(after, sh.ID, currentVoters, voters)...)
		sh.Voters = voters
		sh.NonVoters = removeString(sh.NonVoters, req.NodeID)
		if sh.LeaderHint == req.NodeID {
			sh.LeaderHint = ""
		}
		sh.State = ShardStateActive
		sh.Epoch = after.Epoch
	}
	return steps, policyWarnings, affected, nil
}

func drainShardReferences(sh ShardPlacement, nodeID string) bool {
	return containsString(sh.Voters, nodeID) || containsString(sh.NonVoters, nodeID) || sh.LeaderHint == nodeID
}

// drainPickVoters picks the new voter set for a single shard during
// drain. With explicit targets it appends them deduplicated;
// otherwise it asks the placement policy and prefixes each warning
// with "shard N:" so the operator knows which shard a policy note
// refers to.
func drainPickVoters(cat PlacementCatalog, sh ShardPlacement, remainingVoters, targets []string, req PlacementPlanRequest) ([]string, []string, error) {
	if len(targets) > 0 {
		voters := append([]string(nil), remainingVoters...)
		for _, target := range targets {
			if !containsString(voters, target) {
				voters = append(voters, target)
			}
		}
		sort.Strings(voters)
		if len(voters) < minVoters(req.MinVoters) {
			return nil, nil, invalidPlan("drain would leave shard %d with %d voters; minVoters=%d", sh.ID, len(voters), minVoters(req.MinVoters))
		}
		return voters, nil, nil
	}
	voterCount := targetVoterCount(req.MinVoters, sh.Voters)
	voters, shardWarnings, err := selectPlacementVoters(cat, voterCount, remainingVoters)
	if err != nil {
		return nil, nil, fmt.Errorf("drain shard %d: %w", sh.ID, err)
	}
	prefixed := make([]string, 0, len(shardWarnings))
	for _, w := range shardWarnings {
		prefixed = append(prefixed, fmt.Sprintf("shard %d: %s", sh.ID, w))
	}
	return voters, prefixed, nil
}
