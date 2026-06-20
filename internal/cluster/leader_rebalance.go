package cluster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/internal/placement"
	craft "github.com/CefasDb/cefasdb/internal/replication"
)

const defaultLeaderRebalanceTimeout = 5 * time.Second

type LeaderRebalanceRequest struct {
	DryRun           bool
	IncludeShardZero bool
	MaxConcurrent    int
	Timeout          time.Duration
}

type LeaderRebalanceResult struct {
	DryRun           bool
	IncludeShardZero bool
	MaxConcurrent    int
	Timeout          time.Duration
	Before           []ShardLeadership
	After            []ShardLeadership
	BeforeCounts     []LeaderCount
	AfterCounts      []LeaderCount
	Steps            []LeaderRebalanceStep
	Planned          int
	Transferred      int
	Skipped          int
	Failed           int
}

type LeaderCount struct {
	NodeID string
	Count  int
}

type LeaderRebalanceStep struct {
	ShardID       uint32
	CurrentLeader string
	DesiredLeader string
	TargetLeader  string
	Status        string
	Reason        string
	Detail        string
}

type leaderRebalanceTarget struct {
	nodeID string
	addr   string
}

func (m *Manager) RebalanceLeaders(ctx context.Context, req LeaderRebalanceRequest) (LeaderRebalanceResult, error) {
	req = normalizeLeaderRebalanceRequest(req)
	before := m.sortedShardLeadership()
	steps := m.planLeaderRebalance(req, before)

	result := LeaderRebalanceResult{
		DryRun:           req.DryRun,
		IncludeShardZero: req.IncludeShardZero,
		MaxConcurrent:    req.MaxConcurrent,
		Timeout:          req.Timeout,
		Before:           before,
		BeforeCounts:     leaderCounts(before),
		Steps:            steps,
	}
	if req.DryRun {
		result.After = before
		result.AfterCounts = result.BeforeCounts
		result.summarize()
		return result, nil
	}

	m.executeLeaderRebalance(ctx, req, result.Steps)
	result.After = m.sortedShardLeadership()
	result.AfterCounts = leaderCounts(result.After)
	result.summarize()
	return result, nil
}

func normalizeLeaderRebalanceRequest(req LeaderRebalanceRequest) LeaderRebalanceRequest {
	if req.MaxConcurrent <= 0 {
		req.MaxConcurrent = 1
	}
	if req.Timeout <= 0 {
		req.Timeout = defaultLeaderRebalanceTimeout
	}
	return req
}

func (m *Manager) sortedShardLeadership() []ShardLeadership {
	leadership := m.ShardLeadership()
	out := make([]ShardLeadership, 0, len(leadership))
	for _, st := range leadership {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

func (m *Manager) planLeaderRebalance(req LeaderRebalanceRequest, before []ShardLeadership) []LeaderRebalanceStep {
	cat := m.Placement()
	leadership := make(map[uint32]ShardLeadership, len(before))
	for _, st := range before {
		leadership[st.ShardID] = st
	}
	steps := make([]LeaderRebalanceStep, 0, len(cat.Shards))
	for _, shard := range cat.Shards {
		st := leadership[shard.ID]
		desired := st.DesiredLeader
		if desired == "" {
			desired = shard.LeaderHint
		}
		step := LeaderRebalanceStep{
			ShardID:       shard.ID,
			CurrentLeader: st.ActualLeader,
			DesiredLeader: desired,
			TargetLeader:  desired,
		}
		if shard.ID == 0 && !req.IncludeShardZero {
			steps = append(steps, skipLeaderRebalance(step, "shard_zero_skipped", "metadata shard is skipped by default"))
			continue
		}
		if desired == "" {
			steps = append(steps, skipLeaderRebalance(step, "desired_leader_missing", "shard has no leader hint"))
			continue
		}
		if !containsString(shard.Voters, desired) {
			steps = append(steps, skipLeaderRebalance(step, "target_not_voter", fmt.Sprintf("%s is not a placement voter", desired)))
			continue
		}
		target, ok := m.leaderRebalanceTarget(cat, shard, desired)
		if !ok {
			steps = append(steps, skipLeaderRebalance(step, "target_unavailable", fmt.Sprintf("%s is not an active voter with a raft address", desired)))
			continue
		}
		step.TargetLeader = target.nodeID
		if st.ActualLeader == "" {
			steps = append(steps, skipLeaderRebalance(step, "leader_unknown", "current raft leader is not known"))
			continue
		}
		if st.ActualLeader == desired {
			steps = append(steps, skipLeaderRebalance(step, "already_balanced", "actual leader already matches desired leader"))
			continue
		}
		step.Status = "planned"
		step.Detail = fmt.Sprintf("transfer shard %d leadership from %s to %s", shard.ID, st.ActualLeader, desired)
		steps = append(steps, step)
	}
	return steps
}

func (m *Manager) leaderRebalanceTarget(cat placement.PlacementCatalog, shard placement.ShardPlacement, nodeID string) (leaderRebalanceTarget, bool) {
	if !containsString(shard.Voters, nodeID) {
		return leaderRebalanceTarget{}, false
	}
	if cat.Nodes != nil {
		node, ok := cat.Nodes[nodeID]
		if ok {
			if node.State != "" && node.State != placement.NodeStateActive {
				return leaderRebalanceTarget{}, false
			}
			if node.RaftAddr != "" {
				return leaderRebalanceTarget{nodeID: nodeID, addr: node.RaftAddr}, true
			}
		}
	}
	if addr := m.cfg.Peers[nodeID]; addr != "" {
		return leaderRebalanceTarget{nodeID: nodeID, addr: addr}, true
	}
	return leaderRebalanceTarget{}, false
}

func skipLeaderRebalance(step LeaderRebalanceStep, reason, detail string) LeaderRebalanceStep {
	step.Status = "skipped"
	step.Reason = reason
	step.Detail = detail
	return step
}

func failLeaderRebalance(step LeaderRebalanceStep, reason string, err error) LeaderRebalanceStep {
	step.Status = "failed"
	step.Reason = reason
	if err != nil {
		step.Detail = err.Error()
	}
	return step
}

func (m *Manager) executeLeaderRebalance(ctx context.Context, req LeaderRebalanceRequest, steps []LeaderRebalanceStep) {
	if req.MaxConcurrent <= 1 {
		for i := range steps {
			if steps[i].Status != "planned" {
				continue
			}
			steps[i] = m.executeLeaderRebalanceStep(ctx, req, steps[i])
		}
		return
	}

	sem := make(chan struct{}, req.MaxConcurrent)
	var wg sync.WaitGroup
	for i := range steps {
		if steps[i].Status != "planned" {
			continue
		}
		i := i
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			steps[i] = m.executeLeaderRebalanceStep(ctx, req, steps[i])
		}()
	}
	wg.Wait()
}

func (m *Manager) executeLeaderRebalanceStep(ctx context.Context, req LeaderRebalanceRequest, step LeaderRebalanceStep) LeaderRebalanceStep {
	if err := ctx.Err(); err != nil {
		return failLeaderRebalance(step, "context_done", err)
	}
	sh, ok := m.Shard(step.ShardID)
	if !ok || sh == nil || sh.Raft == nil {
		return skipLeaderRebalance(step, "no_local_raft", "this node has no local raft group for the shard")
	}
	current, _ := sh.Raft.LeaderInfo()
	step.CurrentLeader = current
	if current == "" {
		return skipLeaderRebalance(step, "leader_unknown", "current raft leader is not known")
	}
	if current == step.TargetLeader {
		return skipLeaderRebalance(step, "already_balanced", "actual leader already matches desired leader")
	}
	if !sh.Raft.IsLeader() {
		return skipLeaderRebalance(step, "not_local_leader", fmt.Sprintf("current node does not lead shard %d", step.ShardID))
	}
	addr, voter, err := sh.Raft.VoterAddress(step.TargetLeader)
	if err != nil {
		return failLeaderRebalance(step, "configuration_unavailable", err)
	}
	if !voter {
		return skipLeaderRebalance(step, "target_not_current_voter", fmt.Sprintf("%s is not a current raft voter", step.TargetLeader))
	}
	if addr == "" {
		return skipLeaderRebalance(step, "target_addr_missing", fmt.Sprintf("%s has no current raft address", step.TargetLeader))
	}
	if err := transferLeadershipWithBackoff(ctx, sh, step.TargetLeader, addr, req.Timeout); err != nil {
		return failLeaderRebalance(step, "transfer_failed", err)
	}
	if err := waitShardLeader(ctx, sh, step.TargetLeader, req.Timeout); err != nil {
		return failLeaderRebalance(step, "settle_failed", err)
	}
	step.Status = "transferred"
	step.Reason = ""
	step.Detail = fmt.Sprintf("transferred shard %d leadership to %s", step.ShardID, step.TargetLeader)
	return step
}

func transferLeadershipWithBackoff(ctx context.Context, sh *Shard, targetID, targetAddr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("leadership transfer to %s timed out after %s", targetID, timeout)
		}
		err := sh.Raft.TransferLeadership(targetID, targetAddr, remaining)
		if err == nil {
			return nil
		}
		if !craft.IsLeadershipTransferInProgress(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(minDuration(100*time.Millisecond, remaining)):
		}
	}
}

func waitShardLeader(ctx context.Context, sh *Shard, targetID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		leaderID, _ := sh.Raft.LeaderInfo()
		if leaderID == targetID {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("shard %d leader = %q, want %q after %s", sh.ID, leaderID, targetID, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(minDuration(50*time.Millisecond, remaining)):
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func leaderCounts(in []ShardLeadership) []LeaderCount {
	counts := map[string]int{}
	for _, st := range in {
		if st.ShardID == 0 || st.ActualLeader == "" {
			continue
		}
		counts[st.ActualLeader]++
	}
	out := make([]LeaderCount, 0, len(counts))
	for nodeID, count := range counts {
		out = append(out, LeaderCount{NodeID: nodeID, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (r *LeaderRebalanceResult) summarize() {
	for _, step := range r.Steps {
		switch step.Status {
		case "planned":
			r.Planned++
		case "transferred":
			r.Transferred++
		case "skipped":
			r.Skipped++
		case "failed":
			r.Failed++
		}
	}
}
