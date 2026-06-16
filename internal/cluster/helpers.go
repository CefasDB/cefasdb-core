// Package cluster: membership / voter helpers used by both plan
// strategies and apply validators. Split off from planner.go to keep
// the planner focused on the dispatcher and Manager-side glue.
package cluster

import "sort"

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
