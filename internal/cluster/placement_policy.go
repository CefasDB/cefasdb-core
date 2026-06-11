package cluster

import (
	"fmt"
	"sort"
	"strings"
)

type placementNodeLoad struct {
	shardCount int
	rangeCount int
}

type placementCandidate struct {
	id       string
	node     NodeDescriptor
	load     placementNodeLoad
	score    int64
	zone     string
	tags     []string
	memoryGB int64
	diskGB   int64
}

type placementIgnoredNode struct {
	id    string
	state NodeState
}

func selectPlacementVoters(cat PlacementCatalog, count int, required []string) ([]string, []string, error) {
	if count < 1 {
		count = 1
	}
	required = sortedUnique(required)
	if len(required) > count {
		count = len(required)
	}
	if err := validateNodeSet(cat, required, 0); err != nil {
		return nil, nil, err
	}

	candidates, ignored := placementCandidates(cat)
	if len(candidates) < count {
		return nil, nil, invalidPlan("placement policy found %d active nodes; need %d voters", len(candidates), count)
	}

	candidateByID := make(map[string]placementCandidate, len(candidates))
	for _, candidate := range candidates {
		candidateByID[candidate.id] = candidate
	}

	selected := make([]placementCandidate, 0, count)
	selectedIDs := make(map[string]struct{}, count)
	usedZones := make(map[string]struct{}, count)
	addCandidate := func(candidate placementCandidate) {
		selected = append(selected, candidate)
		selectedIDs[candidate.id] = struct{}{}
		usedZones[placementZoneKey(candidate)] = struct{}{}
	}

	for _, id := range required {
		candidate, ok := candidateByID[id]
		if !ok {
			return nil, nil, invalidPlan("placement policy requires node %q, but it is not active", id)
		}
		addCandidate(candidate)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return betterPlacementCandidate(candidates[i], candidates[j])
	})

	zoneSpread := placementHasEnoughZones(candidates, count)
	if zoneSpread {
		for _, candidate := range candidates {
			if len(selected) == count {
				break
			}
			if candidate.zone == "" {
				continue
			}
			if _, ok := selectedIDs[candidate.id]; ok {
				continue
			}
			if _, ok := usedZones[placementZoneKey(candidate)]; ok {
				continue
			}
			addCandidate(candidate)
		}
	}

	for _, candidate := range candidates {
		if len(selected) == count {
			break
		}
		if _, ok := selectedIDs[candidate.id]; ok {
			continue
		}
		if _, ok := usedZones[placementZoneKey(candidate)]; ok {
			continue
		}
		addCandidate(candidate)
	}

	for _, candidate := range candidates {
		if len(selected) == count {
			break
		}
		if _, ok := selectedIDs[candidate.id]; ok {
			continue
		}
		addCandidate(candidate)
	}

	voters := make([]string, 0, len(selected))
	for _, candidate := range selected {
		voters = append(voters, candidate.id)
	}
	sort.Strings(voters)
	return voters, placementPolicyWarnings(voters, selected, ignored, required, count, zoneSpread), nil
}

func targetVoterCount(min int, current []string) int {
	count := len(sortedUnique(current))
	if count < 1 {
		count = 1
	}
	if min > count {
		count = min
	}
	return count
}

func placementCandidates(cat PlacementCatalog) ([]placementCandidate, []placementIgnoredNode) {
	loads := placementLoads(cat)
	nodeIDs := make([]string, 0, len(cat.Nodes))
	for id := range cat.Nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)

	candidates := make([]placementCandidate, 0, len(nodeIDs))
	ignored := make([]placementIgnoredNode, 0)
	for _, id := range nodeIDs {
		node := cat.Nodes[id]
		if node.ID == "" {
			node.ID = id
		}
		if node.State == "" {
			node.State = NodeStateActive
		}
		if node.State != NodeStateActive {
			ignored = append(ignored, placementIgnoredNode{id: id, state: node.State})
			continue
		}
		if node.Capacity.Weight <= 0 {
			node.Capacity.Weight = 1
		}
		tags := append([]string(nil), node.Capacity.Tags...)
		sort.Strings(tags)
		node.Capacity.Tags = tags
		load := loads[id]
		candidate := placementCandidate{
			id:       id,
			node:     node,
			load:     load,
			zone:     node.Capacity.Zone,
			tags:     tags,
			memoryGB: placementGiB(node.Capacity.MemoryBytes),
			diskGB:   placementGiB(node.Capacity.DiskBytes),
		}
		candidate.score = placementCandidateScore(node.Capacity, load)
		candidates = append(candidates, candidate)
	}
	return candidates, ignored
}

func placementLoads(cat PlacementCatalog) map[string]placementNodeLoad {
	loads := make(map[string]placementNodeLoad)
	for _, shard := range cat.Shards {
		if shard.State == ShardStateDecommissioned {
			continue
		}
		rangeCount := len(shard.Ranges)
		if rangeCount == 0 {
			rangeCount = 1
		}
		members := append([]string(nil), shard.Voters...)
		members = append(members, shard.NonVoters...)
		for _, nodeID := range sortedUnique(members) {
			load := loads[nodeID]
			load.shardCount++
			load.rangeCount += rangeCount
			loads[nodeID] = load
		}
	}
	return loads
}

func placementCandidateScore(capacity NodeCapacity, load placementNodeLoad) int64 {
	weight := placementPositiveInt(capacity.Weight)
	cpu := placementNonNegativeInt(capacity.CPU)
	score := weight * 1_000_000_000_000
	score += cpu * 1_000_000_000
	score += placementGiB(capacity.MemoryBytes) * 1_000_000
	score += placementGiB(capacity.DiskBytes) * 1_000
	score += int64(len(capacity.Tags)) * 10
	score -= int64(load.shardCount) * 10_000_000
	score -= int64(load.rangeCount) * 1_000_000
	return score
}

func betterPlacementCandidate(a, b placementCandidate) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	if a.load.shardCount != b.load.shardCount {
		return a.load.shardCount < b.load.shardCount
	}
	if a.load.rangeCount != b.load.rangeCount {
		return a.load.rangeCount < b.load.rangeCount
	}
	return a.id < b.id
}

func placementHasEnoughZones(candidates []placementCandidate, count int) bool {
	zones := make(map[string]struct{})
	for _, candidate := range candidates {
		if candidate.zone == "" {
			continue
		}
		zones[candidate.zone] = struct{}{}
	}
	return len(zones) >= count
}

func placementZoneKey(candidate placementCandidate) string {
	if candidate.zone != "" {
		return "zone:" + candidate.zone
	}
	return "node:" + candidate.id
}

func placementPolicyWarnings(voters []string, selected []placementCandidate, ignored []placementIgnoredNode, required []string, count int, zoneSpread bool) []string {
	warnings := []string{
		fmt.Sprintf("placement policy selected target voters %s with minVoters=%d using active-node scoring inputs: weight,cpu,memory,disk,tags,shard_count,range_count", strings.Join(voters, ","), count),
	}
	if len(required) > 0 {
		warnings = append(warnings, fmt.Sprintf("placement policy preserved existing voters required by the operation: %s", strings.Join(required, ",")))
	}
	if zoneSpread {
		warnings = append(warnings, fmt.Sprintf("placement policy applied zone anti-affinity across zones: %s", strings.Join(placementSelectedZones(selected), ",")))
	}
	if len(ignored) > 0 {
		warnings = append(warnings, fmt.Sprintf("placement policy ignored inactive nodes: %s", strings.Join(placementIgnoredDetails(ignored), ",")))
	}
	warnings = append(warnings, fmt.Sprintf("placement policy selected voter details: %s", strings.Join(placementSelectedDetails(selected), "; ")))
	return warnings
}

func placementSelectedZones(selected []placementCandidate) []string {
	seen := make(map[string]struct{})
	for _, candidate := range selected {
		if candidate.zone == "" {
			continue
		}
		seen[candidate.zone] = struct{}{}
	}
	zones := make([]string, 0, len(seen))
	for zone := range seen {
		zones = append(zones, zone)
	}
	sort.Strings(zones)
	return zones
}

func placementIgnoredDetails(ignored []placementIgnoredNode) []string {
	sort.Slice(ignored, func(i, j int) bool { return ignored[i].id < ignored[j].id })
	details := make([]string, 0, len(ignored))
	for _, node := range ignored {
		details = append(details, fmt.Sprintf("%s=%s", node.id, node.state))
	}
	return details
}

func placementSelectedDetails(selected []placementCandidate) []string {
	selected = append([]placementCandidate(nil), selected...)
	sort.Slice(selected, func(i, j int) bool { return selected[i].id < selected[j].id })
	details := make([]string, 0, len(selected))
	for _, candidate := range selected {
		zone := candidate.zone
		if zone == "" {
			zone = "unknown"
		}
		details = append(details, fmt.Sprintf(
			"%s(score=%d,zone=%s,weight=%d,cpu=%d,memoryGiB=%d,diskGiB=%d,tags=%d,shards=%d,ranges=%d)",
			candidate.id,
			candidate.score,
			zone,
			candidate.node.Capacity.Weight,
			candidate.node.Capacity.CPU,
			candidate.memoryGB,
			candidate.diskGB,
			len(candidate.tags),
			candidate.load.shardCount,
			candidate.load.rangeCount,
		))
	}
	return details
}

func placementPositiveInt(v int) int64 {
	if v <= 0 {
		return 1
	}
	if v > 1_000_000 {
		return 1_000_000
	}
	return int64(v)
}

func placementNonNegativeInt(v int) int64 {
	if v <= 0 {
		return 0
	}
	if v > 1_000_000 {
		return 1_000_000
	}
	return int64(v)
}

func placementGiB(bytes uint64) int64 {
	return int64(bytes >> 30)
}
