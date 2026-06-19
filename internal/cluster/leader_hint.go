package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/internal/placement"
)

const (
	leaderHintReconcileWindow   = 30 * time.Second
	leaderHintReconcileInterval = 250 * time.Millisecond
	leaderHintTransferTimeout   = 5 * time.Second
)

const (
	LeaderRebalanceStatusPlanned         = "planned"
	LeaderRebalanceStatusTransferred     = "transferred"
	LeaderRebalanceStatusAlreadyBalanced = "already_balanced"
	LeaderRebalanceStatusSkipped         = "skipped"
	LeaderRebalanceStatusFailed          = "failed"
)

// LeaderRebalanceOptions controls one explicit leader rebalance pass.
type LeaderRebalanceOptions struct {
	DryRun           bool
	IncludeShardZero bool
	MaxConcurrent    int
	TransferTimeout  time.Duration
}

// LeaderRebalanceShardResult is the per-shard outcome from a leader
// rebalance pass.
type LeaderRebalanceShardResult struct {
	ShardID       uint32
	CurrentLeader string
	DesiredLeader string
	Status        string
	Detail        string
}

// LeaderRebalanceResult summarizes one explicit leader rebalance pass.
type LeaderRebalanceResult struct {
	DryRun           bool
	IncludeShardZero bool
	MaxConcurrent    int
	Planned          int
	Transferred      int
	Skipped          int
	Failed           int
	Shards           []LeaderRebalanceShardResult
}

type leaderRebalanceOp struct {
	resultIdx  int
	meta       placement.ShardPlacement
	shard      *Shard
	targetAddr string
}

func (m *Manager) startLeaderHintReconciliation() {
	if !m.hasTransferableLeaderHints() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), leaderHintReconcileWindow)
	go func() {
		defer cancel()
		ticker := time.NewTicker(leaderHintReconcileInterval)
		defer ticker.Stop()
		for {
			_ = m.ApplyLeaderHints(ctx)
			if m.leaderHintsSatisfied() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// ApplyLeaderHints asks any locally-led shard to transfer leadership to
// the placement catalog's hinted voter. Followers skip the shard; the
// same reconciliation runs on every node during startup, so whichever
// process currently leads a shard performs the handoff.
func (m *Manager) ApplyLeaderHints(ctx context.Context) error {
	res, err := m.RebalanceLeaders(ctx, LeaderRebalanceOptions{
		IncludeShardZero: true,
		MaxConcurrent:    1,
		TransferTimeout:  leaderHintTransferTimeout,
	})
	if err != nil {
		return err
	}
	var errs []error
	for _, sh := range res.Shards {
		if sh.Status == LeaderRebalanceStatusFailed {
			errs = append(errs, fmt.Errorf("shard %d: %s", sh.ShardID, sh.Detail))
		}
	}
	return errors.Join(errs...)
}

// RebalanceLeaders compares each shard's actual Raft leader with the
// placement leader hint and transfers locally-led mismatches to the
// hinted voter. Shards led by another process are reported but not
// transferred; callers that need a whole-cluster pass should invoke
// this operation on every node.
func (m *Manager) RebalanceLeaders(ctx context.Context, opts LeaderRebalanceOptions) (LeaderRebalanceResult, error) {
	if m == nil {
		return LeaderRebalanceResult{}, errors.New("cluster: manager is nil")
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 1
	}
	if opts.TransferTimeout <= 0 {
		opts.TransferTimeout = leaderHintTransferTimeout
	}

	cat := m.Placement()
	out := LeaderRebalanceResult{
		DryRun:           opts.DryRun,
		IncludeShardZero: opts.IncludeShardZero,
		MaxConcurrent:    opts.MaxConcurrent,
		Shards:           make([]LeaderRebalanceShardResult, 0, len(cat.Shards)),
	}
	var ops []leaderRebalanceOp
	for _, meta := range cat.Shards {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		res, op := m.planLeaderRebalanceShard(cat, meta, opts)
		out.Shards = append(out.Shards, res)
		if op != nil {
			op.resultIdx = len(out.Shards) - 1
			ops = append(ops, *op)
		}
	}
	if opts.DryRun || len(ops) == 0 {
		out.recount()
		return out, nil
	}

	sem := make(chan struct{}, opts.MaxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, op := range ops {
		op := op
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				mu.Lock()
				out.Shards[op.resultIdx] = failedLeaderRebalanceResult(op.meta, currentLeaderForShard(op.shard), ctx.Err().Error())
				mu.Unlock()
				return
			}
			res := m.applyLeaderRebalanceOp(ctx, op, opts.TransferTimeout)
			mu.Lock()
			out.Shards[op.resultIdx] = res
			mu.Unlock()
		}()
	}
	wg.Wait()
	out.recount()
	return out, nil
}

func (m *Manager) planLeaderRebalanceShard(cat placement.PlacementCatalog, meta placement.ShardPlacement, opts LeaderRebalanceOptions) (LeaderRebalanceShardResult, *leaderRebalanceOp) {
	sh, ok := m.Shard(meta.ID)
	current := ""
	if ok && sh != nil && sh.Raft != nil {
		current, _ = sh.Raft.LeaderInfo()
	}
	res := planLeaderRebalanceShard(meta, current, opts.IncludeShardZero)
	if res.Status != LeaderRebalanceStatusPlanned || opts.DryRun {
		return res, nil
	}
	if !ok || sh == nil || sh.Raft == nil {
		res.Status = LeaderRebalanceStatusSkipped
		res.Detail = "local shard replica is not open"
		return res, nil
	}
	if !sh.Raft.IsLeader() {
		res.Status = LeaderRebalanceStatusSkipped
		res.Detail = "current leader is not the local endpoint"
		return res, nil
	}
	addr := leaderHintRaftAddr(cat, meta.LeaderHint)
	if addr == "" {
		res.Status = LeaderRebalanceStatusFailed
		res.Detail = fmt.Sprintf("desired leader %q has no raft address", meta.LeaderHint)
		return res, nil
	}
	return res, &leaderRebalanceOp{meta: meta, shard: sh, targetAddr: addr}
}

func (m *Manager) applyLeaderRebalanceOp(ctx context.Context, op leaderRebalanceOp, timeout time.Duration) LeaderRebalanceShardResult {
	res := LeaderRebalanceShardResult{
		ShardID:       op.meta.ID,
		DesiredLeader: op.meta.LeaderHint,
		Status:        LeaderRebalanceStatusTransferred,
	}
	if op.shard != nil && op.shard.Raft != nil {
		res.CurrentLeader, _ = op.shard.Raft.LeaderInfo()
	}
	if err := ctx.Err(); err != nil {
		return failedLeaderRebalanceResult(op.meta, res.CurrentLeader, err.Error())
	}
	if err := op.shard.Raft.TransferLeadership(op.meta.LeaderHint, op.targetAddr, timeout); err != nil {
		return failedLeaderRebalanceResult(op.meta, res.CurrentLeader, fmt.Sprintf("transfer leadership to %q: %v", op.meta.LeaderHint, err))
	}
	if err := waitForShardLeader(ctx, op.shard, op.meta.LeaderHint, timeout); err != nil {
		return failedLeaderRebalanceResult(op.meta, res.CurrentLeader, err.Error())
	}
	res.Detail = "leadership transferred to desired voter"
	return res
}

func planLeaderRebalanceShard(meta placement.ShardPlacement, currentLeader string, includeShardZero bool) LeaderRebalanceShardResult {
	res := LeaderRebalanceShardResult{
		ShardID:       meta.ID,
		CurrentLeader: currentLeader,
		DesiredLeader: meta.LeaderHint,
	}
	if meta.ID == 0 && !includeShardZero {
		res.Status = LeaderRebalanceStatusSkipped
		res.Detail = "shard 0 skipped"
		return res
	}
	if !transferableLeaderHint(meta) {
		res.Status = LeaderRebalanceStatusSkipped
		res.Detail = "leader hint is empty, duplicated, or not a voter"
		return res
	}
	if currentLeader == "" {
		res.Status = LeaderRebalanceStatusSkipped
		res.Detail = "current leader is unknown"
		return res
	}
	if currentLeader == meta.LeaderHint {
		res.Status = LeaderRebalanceStatusAlreadyBalanced
		res.Detail = "current leader already matches desired leader"
		return res
	}
	res.Status = LeaderRebalanceStatusPlanned
	res.Detail = "leadership should transfer to desired voter"
	return res
}

func waitForShardLeader(ctx context.Context, sh *Shard, want string, timeout time.Duration) error {
	if sh == nil || sh.Raft == nil {
		return errors.New("local shard raft is not available")
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, _ := sh.Raft.LeaderInfo()
		if got == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("leadership did not settle on %q within %s", want, timeout)
		case <-ticker.C:
		}
	}
}

func failedLeaderRebalanceResult(meta placement.ShardPlacement, currentLeader, detail string) LeaderRebalanceShardResult {
	return LeaderRebalanceShardResult{
		ShardID:       meta.ID,
		CurrentLeader: currentLeader,
		DesiredLeader: meta.LeaderHint,
		Status:        LeaderRebalanceStatusFailed,
		Detail:        detail,
	}
}

func currentLeaderForShard(sh *Shard) string {
	if sh == nil || sh.Raft == nil {
		return ""
	}
	leader, _ := sh.Raft.LeaderInfo()
	return leader
}

func (r *LeaderRebalanceResult) recount() {
	r.Planned = 0
	r.Transferred = 0
	r.Skipped = 0
	r.Failed = 0
	for _, sh := range r.Shards {
		switch sh.Status {
		case LeaderRebalanceStatusPlanned:
			r.Planned++
		case LeaderRebalanceStatusTransferred:
			r.Planned++
			r.Transferred++
		case LeaderRebalanceStatusFailed:
			r.Planned++
			r.Failed++
		case LeaderRebalanceStatusSkipped, LeaderRebalanceStatusAlreadyBalanced:
			r.Skipped++
		default:
			r.Skipped++
		}
	}
}

func (m *Manager) hasTransferableLeaderHints() bool {
	for _, meta := range m.Placement().Shards {
		if !transferableLeaderHint(meta) {
			continue
		}
		sh, ok := m.Shard(meta.ID)
		if ok && sh != nil && sh.Raft != nil {
			return true
		}
	}
	return false
}

func (m *Manager) leaderHintsSatisfied() bool {
	for _, meta := range m.Placement().Shards {
		if !transferableLeaderHint(meta) {
			continue
		}
		sh, ok := m.Shard(meta.ID)
		if !ok || sh == nil || sh.Raft == nil {
			continue
		}
		leaderID, _ := sh.Raft.LeaderInfo()
		if leaderID != meta.LeaderHint {
			return false
		}
	}
	return true
}

func transferableLeaderHint(sh placement.ShardPlacement) bool {
	if sh.LeaderHint == "" || !containsString(sh.Voters, sh.LeaderHint) {
		return false
	}
	return len(uniqueStrings(sh.Voters)) > 1
}

func leaderHintRaftAddr(cat placement.PlacementCatalog, nodeID string) string {
	if cat.Nodes == nil {
		return ""
	}
	node := cat.Nodes[nodeID]
	return node.RaftAddr
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func containsString(in []string, want string) bool {
	for _, v := range in {
		if v == want {
			return true
		}
	}
	return false
}
