package placement

// planMove derives the placement-plan that replaces a voter on a
// shard. ApplyPlacement executes the Raft membership steps before
// publishing the new placement epoch.
func planMove(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	shardIdx, shard, err := FindShard(cat, req.ShardID)
	if err != nil {
		return PlacementPlan{}, err
	}
	if len(shard.Voters) == 0 {
		return PlacementPlan{}, InvalidPlan("shard %d has no voters to move", shard.ID)
	}

	voters, steps, err := moveVoterPlan(cat, shard, req)
	if err != nil {
		return PlacementPlan{}, err
	}

	after := NextCatalog(cat)
	after.Shards[shardIdx].State = ShardStateActive
	after.Shards[shardIdx].Epoch = after.Epoch
	after.Shards[shardIdx].Voters = voters
	after.Shards[shardIdx].NonVoters = removeAny(after.Shards[shardIdx].NonVoters, voters)
	after.Shards[shardIdx].LeaderHint = normalizeLeaderHint(after.Shards[shardIdx])
	after.Normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementPlan{}, err
	}

	return PlacementPlan{
		Operation:      PlacementOperationMove,
		BeforeEpoch:    cat.Epoch,
		AfterEpoch:     after.Epoch,
		Before:         cat.Clone(),
		After:          after,
		Steps:          steps,
		ApplySupported: true,
		Warnings: []string{
			"move applies Raft membership steps first and publishes the new placement epoch only after those steps succeed",
		},
	}, nil
}

// moveVoterPlan picks the new voter set and the Raft membership
// steps. Two paths: an explicit TargetVoters list (validated) or a
// SourceNode → TargetNode swap (one voter replaced).
func moveVoterPlan(cat PlacementCatalog, shard ShardPlacement, req PlacementPlanRequest) ([]string, []PlacementPlanStep, error) {
	minV := minVoters(req.MinVoters)
	if len(req.TargetVoters) > 0 {
		voters := sortedUnique(req.TargetVoters)
		if err := validateNodeSet(cat, voters, minV); err != nil {
			return nil, nil, err
		}
		return voters, membershipDiffSteps(cat, shard.ID, shard.Voters, voters), nil
	}
	if req.SourceNode == "" || req.TargetNode == "" {
		return nil, nil, InvalidPlan("move requires sourceNode and targetNode when targetVoters is empty")
	}
	if req.SourceNode == req.TargetNode {
		return nil, nil, InvalidPlan("sourceNode and targetNode must differ")
	}
	if err := validateNodeSet(cat, []string{req.TargetNode}, 1); err != nil {
		return nil, nil, err
	}
	if !containsString(shard.Voters, req.SourceNode) {
		return nil, nil, InvalidPlan("source node %q is not a voter for shard %d", req.SourceNode, shard.ID)
	}
	voters := replaceVoter(shard.Voters, req.SourceNode, req.TargetNode)
	if len(voters) < minV {
		return nil, nil, InvalidPlan("move would leave shard %d with %d voters; minVoters=%d", shard.ID, len(voters), minV)
	}
	steps := []PlacementPlanStep{
		{Action: "add_voter", ShardID: u32ptr(shard.ID), NodeID: req.TargetNode, Addr: cat.Nodes[req.TargetNode].RaftAddr},
		{Action: "wait_catchup", ShardID: u32ptr(shard.ID), NodeID: req.TargetNode},
		{Action: "remove_voter", ShardID: u32ptr(shard.ID), NodeID: req.SourceNode},
	}
	return voters, steps, nil
}
