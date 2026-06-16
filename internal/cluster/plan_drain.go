package cluster

import (
	"fmt"
	"sort"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

// planDrain derives the placement-plan that removes a node from
// every shard membership it participates in. Drain is plan-only;
// callers must apply the resulting Raft membership steps shard by
// shard before stopping the node.
func planDrain(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	nodeID, err := validateDrainInputs(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	targets, err := drainTargets(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	after := markNodeDraining(cat, nodeID)
	steps, policyWarnings, err := executeDrainShards(after, req, nodeID, targets)
	if err != nil {
		return PlacementPlan{}, err
	}
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementPlan{}, err
	}
	return buildDrainPlan(cat, after, steps, policyWarnings, len(targets) == 0), nil
}

// validateDrainInputs enforces the two preconditions every drain
// request must satisfy before any catalog mutation happens, and
// returns the validated NodeID value object so downstream helpers
// cannot accidentally consume the raw request string.
func validateDrainInputs(cat PlacementCatalog, req PlacementPlanRequest) (model.NodeID, error) {
	nodeID, err := model.NewNodeID(req.NodeID)
	if err != nil {
		return model.NodeID{}, invalidPlan("drain requires nodeId")
	}
	if _, ok := cat.Nodes[nodeID.String()]; !ok {
		return model.NodeID{}, invalidPlan("node %q does not exist in placement", nodeID.String())
	}
	return nodeID, nil
}

// markNodeDraining produces the next-epoch catalog with the node's
// state flipped to NodeStateDraining. Subsequent helpers mutate the
// returned catalog's shard membership in place.
func markNodeDraining(cat PlacementCatalog, nodeID model.NodeID) PlacementCatalog {
	after := nextCatalog(cat)
	key := nodeID.String()
	node := after.Nodes[key]
	node.State = NodeStateDraining
	after.Nodes[key] = node
	return after
}

// executeDrainShards walks the affected shards on `after` and
// rejects the drain if the node is unreferenced by every shard.
func executeDrainShards(after PlacementCatalog, req PlacementPlanRequest, nodeID model.NodeID, targets []string) ([]PlacementPlanStep, []string, error) {
	steps, policyWarnings, affected, err := drainShards(after, req, targets)
	if err != nil {
		return nil, nil, err
	}
	if affected == 0 {
		return nil, nil, invalidPlan("node %q is not present in any shard membership", nodeID.String())
	}
	return steps, policyWarnings, nil
}

// buildDrainPlan assembles the user-facing PlacementPlan from the
// validated before/after catalogs. usedPolicyVoters toggles the
// "no target nodes supplied" advisory.
func buildDrainPlan(cat, after PlacementCatalog, steps []PlacementPlanStep, policyWarnings []string, usedPolicyVoters bool) PlacementPlan {
	warnings := []string{
		"drain is a plan only; apply Raft membership changes shard by shard before stopping the node",
	}
	if usedPolicyVoters {
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
	}
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
