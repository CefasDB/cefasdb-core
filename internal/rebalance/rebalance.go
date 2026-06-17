package rebalance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/internal/metrics"
	"github.com/CefasDb/cefasdb/internal/placement"
)

type Mode string

const (
	ModeDryRun Mode = "dry-run"
	ModeManual Mode = "manual"
	ModeAuto   Mode = "auto"
)

type Config struct {
	Mode                    Mode
	Interval                time.Duration
	MinInterval             time.Duration
	MaxConcurrentOperations int
	MaxHotspots             int
	MinVoters               int
	ApplyTimeoutMS          int
	ManualPlanDir           string
}

type Planner interface {
	RefreshPlacement() error
	Placement() placement.PlacementCatalog
	PlanPlacement(placement.PlacementPlanRequest) (placement.PlacementPlan, error)
	ApplyPlacement(context.Context, placement.PlacementApplyRequest) (placement.PlacementApplyResult, error)
}

type HotspotSource interface {
	RangeHotspotSummaries(max int) []metrics.RangeHotspotSummary
}

type PlanSink interface {
	WritePlan(context.Context, Decision) (string, error)
}

type Controller struct {
	cfg      Config
	planner  Planner
	hotspots HotspotSource
	sink     PlanSink
	now      func() time.Time
	logf     func(string, ...any)

	mu         sync.Mutex
	inFlight   int
	lastGlobal time.Time
	lastShard  map[uint32]time.Time
}

type Candidate struct {
	Operation     placement.PlacementOperation   `json:"operation"`
	Request       placement.PlacementPlanRequest `json:"request"`
	SourceShardID uint32                         `json:"sourceShardId,omitempty"`
	NodeID        string                         `json:"nodeId,omitempty"`
	Reason        string                         `json:"reason"`
	HotRange      metrics.RangeHotspotSummary    `json:"hotRange,omitempty"`
	Priority      uint64                         `json:"priority"`
}

type Decision struct {
	Mode        Mode                           `json:"mode"`
	Status      string                         `json:"status"`
	Reason      string                         `json:"reason,omitempty"`
	Candidate   Candidate                      `json:"candidate,omitempty"`
	Plan        placement.PlacementPlan        `json:"plan,omitempty"`
	ApplyResult placement.PlacementApplyResult `json:"applyResult,omitempty"`
	WrittenPath string                         `json:"writtenPath,omitempty"`
	Error       string                         `json:"error,omitempty"`
	CreatedAt   time.Time                      `json:"createdAt"`
}

func NormalizeConfig(cfg Config) Config {
	if cfg.Mode == "" {
		cfg.Mode = ModeDryRun
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 5 * time.Minute
	}
	if cfg.MaxConcurrentOperations <= 0 {
		cfg.MaxConcurrentOperations = 1
	}
	if cfg.MaxHotspots <= 0 {
		cfg.MaxHotspots = 8
	}
	if cfg.ApplyTimeoutMS <= 0 {
		cfg.ApplyTimeoutMS = 5000
	}
	return cfg
}

func NewController(cfg Config, planner Planner, hotspots HotspotSource, sink PlanSink) *Controller {
	cfg = NormalizeConfig(cfg)
	if sink == nil && cfg.Mode == ModeManual && cfg.ManualPlanDir != "" {
		sink = FileSink{Dir: cfg.ManualPlanDir}
	}
	return &Controller{
		cfg:       cfg,
		planner:   planner,
		hotspots:  hotspots,
		sink:      sink,
		now:       time.Now,
		lastShard: make(map[uint32]time.Time),
	}
}

func (c *Controller) SetLogger(logf func(string, ...any)) {
	c.logf = logf
}

func (c *Controller) Run(ctx context.Context) {
	if c == nil || c.planner == nil {
		return
	}
	interval := NormalizeConfig(c.cfg).Interval
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := c.Tick(ctx); err != nil && c.logf != nil {
			c.logf("rebalancer tick failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Controller) Tick(ctx context.Context) ([]Decision, error) {
	if c == nil || c.planner == nil {
		return nil, fmt.Errorf("rebalancer: planner is required")
	}
	cfg := NormalizeConfig(c.cfg)
	if err := c.planner.RefreshPlacement(); err != nil {
		return nil, err
	}
	cat := c.planner.Placement()
	var hot []metrics.RangeHotspotSummary
	if c.hotspots != nil {
		hot = c.hotspots.RangeHotspotSummaries(cfg.MaxHotspots)
	}
	candidates := BuildCandidates(cat, hot, cfg)
	if len(candidates) == 0 {
		return nil, nil
	}

	now := c.nowTime()
	decisions := make([]Decision, 0, len(candidates))
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return decisions, ctx.Err()
		}
		if !c.tryBudget() {
			decisions = append(decisions, Decision{
				Mode:      cfg.Mode,
				Status:    "budget_exhausted",
				Reason:    fmt.Sprintf("max concurrent operations reached: %d", cfg.MaxConcurrentOperations),
				Candidate: candidate,
				CreatedAt: now,
			})
			break
		}
		if limited, reason := c.rateLimited(candidate, now, cfg.MinInterval); limited {
			c.releaseBudget()
			decisions = append(decisions, Decision{
				Mode:      cfg.Mode,
				Status:    "rate_limited",
				Reason:    reason,
				Candidate: candidate,
				CreatedAt: now,
			})
			continue
		}

		plan, err := c.planner.PlanPlacement(candidate.Request)
		if err != nil {
			c.releaseBudget()
			decisions = append(decisions, Decision{
				Mode:      cfg.Mode,
				Status:    "plan_failed",
				Reason:    candidate.Reason,
				Candidate: candidate,
				Error:     err.Error(),
				CreatedAt: now,
			})
			continue
		}
		decision := Decision{
			Mode:      cfg.Mode,
			Status:    "planned",
			Reason:    candidate.Reason,
			Candidate: candidate,
			Plan:      plan,
			CreatedAt: now,
		}
		switch cfg.Mode {
		case ModeDryRun:
			c.releaseBudget()
		case ModeManual:
			c.releaseBudget()
			if c.sink == nil {
				decision.Status = "write_failed"
				decision.Error = "manual mode requires a plan sink or manualPlanDir"
				break
			}
			path, err := c.sink.WritePlan(ctx, decision)
			if err != nil {
				decision.Status = "write_failed"
				decision.Error = err.Error()
				break
			}
			decision.Status = "written"
			decision.WrittenPath = path
			c.markExecuted(candidate, now)
		case ModeAuto:
			result, err := c.planner.ApplyPlacement(ctx, placement.PlacementApplyRequest{
				Plan:          plan,
				ExpectedEpoch: plan.BeforeEpoch,
				TimeoutMS:     cfg.ApplyTimeoutMS,
			})
			c.releaseBudget()
			if err != nil {
				decision.Status = "apply_failed"
				decision.Error = err.Error()
				break
			}
			decision.Status = "applied"
			decision.ApplyResult = result
			c.markExecuted(candidate, now)
		default:
			c.releaseBudget()
			decision.Status = "skipped"
			decision.Error = fmt.Sprintf("unknown mode %q", cfg.Mode)
		}
		decisions = append(decisions, decision)
		if cfg.Mode == ModeAuto && decision.Status == "applied" {
			break
		}
	}
	for _, d := range decisions {
		if c.logf != nil {
			c.logf("rebalancer decision status=%s mode=%s operation=%s reason=%s error=%s", d.Status, d.Mode, d.Candidate.Operation, d.Reason, d.Error)
		}
	}
	return decisions, nil
}

func BuildCandidates(cat placement.PlacementCatalog, hot []metrics.RangeHotspotSummary, cfg Config) []Candidate {
	cfg = NormalizeConfig(cfg)
	var candidates []Candidate
	candidates = append(candidates, drainCandidates(cat, cfg)...)
	candidates = append(candidates, hotspotCandidates(cat, hot, cfg)...)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		if candidates[i].Operation != candidates[j].Operation {
			return string(candidates[i].Operation) < string(candidates[j].Operation)
		}
		if candidates[i].SourceShardID != candidates[j].SourceShardID {
			return candidates[i].SourceShardID < candidates[j].SourceShardID
		}
		return candidates[i].NodeID < candidates[j].NodeID
	})
	return candidates
}

type FileSink struct {
	Dir string
}

func (s FileSink) WritePlan(_ context.Context, d Decision) (string, error) {
	if s.Dir == "" {
		return "", fmt.Errorf("manual plan directory is required")
	}
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s-%s-shard-%d-epoch-%d.json",
		d.CreatedAt.UTC().Format("20060102T150405Z"),
		d.Candidate.Operation,
		d.Candidate.SourceShardID,
		d.Plan.AfterEpoch,
	)
	path := filepath.Join(s.Dir, name)
	raw, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func drainCandidates(cat placement.PlacementCatalog, cfg Config) []Candidate {
	nodeIDs := make([]string, 0, len(cat.Nodes))
	for id := range cat.Nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)
	out := make([]Candidate, 0)
	for _, nodeID := range nodeIDs {
		node := cat.Nodes[nodeID]
		if node.State != placement.NodeStateDraining {
			continue
		}
		if !nodeHasActivePlacement(cat, nodeID) {
			continue
		}
		out = append(out, Candidate{
			Operation: placement.PlacementOperationDrain,
			Request: placement.PlacementPlanRequest{
				Operation: placement.PlacementOperationDrain,
				NodeID:    nodeID,
				MinVoters: cfg.MinVoters,
			},
			NodeID:   nodeID,
			Reason:   fmt.Sprintf("node %s is draining and still owns active placement references", nodeID),
			Priority: ^uint64(0),
		})
	}
	return out
}

func hotspotCandidates(cat placement.PlacementCatalog, hot []metrics.RangeHotspotSummary, cfg Config) []Candidate {
	shards := make(map[uint32]placement.ShardPlacement, len(cat.Shards))
	for _, sh := range cat.Shards {
		shards[sh.ID] = sh
	}
	sort.Slice(hot, func(i, j int) bool {
		pi := hotspotPriority(hot[i])
		pj := hotspotPriority(hot[j])
		if pi != pj {
			return pi > pj
		}
		if hot[i].ShardID != hot[j].ShardID {
			return hot[i].ShardID < hot[j].ShardID
		}
		return hot[i].Bucket < hot[j].Bucket
	})

	seenShard := make(map[uint32]struct{})
	out := make([]Candidate, 0, len(hot))
	for _, hs := range hot {
		if hs.Status != "hot" {
			continue
		}
		shardID64, err := strconv.ParseUint(hs.ShardID, 10, 32)
		if err != nil {
			continue
		}
		shardID := uint32(shardID64)
		if _, exists := seenShard[shardID]; exists {
			continue
		}
		shard, ok := shards[shardID]
		if !ok || shard.State != placement.ShardStateActive {
			continue
		}
		candidate, ok := hotspotCandidateForShard(shard, hs, cfg)
		if !ok {
			continue
		}
		out = append(out, candidate)
		seenShard[shardID] = struct{}{}
	}
	return out
}

func hotspotCandidateForShard(shard placement.ShardPlacement, hs metrics.RangeHotspotSummary, cfg Config) (Candidate, bool) {
	reason := fmt.Sprintf("hot range shard=%s bucket=%d reads=%d writes=%d bytes=%d reasons=%s",
		hs.ShardID, hs.Bucket, hs.Reads, hs.Writes, hs.Bytes, strings.Join(hs.Reasons, ","))
	priority := hotspotPriority(hs)
	if len(shard.Ranges) == 1 {
		return Candidate{
			Operation:     placement.PlacementOperationSplit,
			SourceShardID: shard.ID,
			Request: placement.PlacementPlanRequest{
				Operation: placement.PlacementOperationSplit,
				ShardID:   shard.ID,
				MinVoters: cfg.MinVoters,
			},
			Reason:   reason + "; split owning shard to reduce hotspot concentration",
			HotRange: hs,
			Priority: priority,
		}, true
	}
	bucketRange := placement.TokenRange{Start: hs.TokenStart, End: hs.TokenEnd}
	for _, rng := range shard.Ranges {
		if !rangeContained(rng, bucketRange) {
			continue
		}
		start, end := bucketRange.Start, bucketRange.End
		return Candidate{
			Operation:     placement.PlacementOperationRangeMove,
			SourceShardID: shard.ID,
			Request: placement.PlacementPlanRequest{
				Operation:  placement.PlacementOperationRangeMove,
				ShardID:    shard.ID,
				RangeStart: &start,
				RangeEnd:   &end,
				MinVoters:  cfg.MinVoters,
			},
			Reason:   reason + "; move hot token bucket into a new shard",
			HotRange: hs,
			Priority: priority,
		}, true
	}
	return Candidate{}, false
}

func hotspotPriority(hs metrics.RangeHotspotSummary) uint64 {
	ops := hs.Reads + hs.Writes*2
	bytesScore := hs.Bytes / 1024
	latencyScore := uint64(hs.MaxLatencySeconds * 1_000_000)
	debtScore := hs.CompactionDebtBytes / (1 << 20)
	return ops*1_000_000_000 + bytesScore*1_000 + latencyScore + debtScore
}

func nodeHasActivePlacement(cat placement.PlacementCatalog, nodeID string) bool {
	for _, shard := range cat.Shards {
		if shard.State == placement.ShardStateDecommissioned {
			continue
		}
		if containsString(shard.Voters, nodeID) || containsString(shard.NonVoters, nodeID) || shard.LeaderHint == nodeID {
			return true
		}
	}
	return false
}

func rangeContained(owner, child placement.TokenRange) bool {
	if child.Start == child.End {
		return owner.Start == owner.End
	}
	if !owner.Contains(child.Start) {
		return false
	}
	end := child.End - 1
	if child.End == 0 {
		end = ^uint64(0)
	}
	return owner.Contains(end)
}

func containsString(in []string, want string) bool {
	for _, v := range in {
		if v == want {
			return true
		}
	}
	return false
}

func (c *Controller) tryBudget() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	max := NormalizeConfig(c.cfg).MaxConcurrentOperations
	if c.inFlight >= max {
		return false
	}
	c.inFlight++
	return true
}

func (c *Controller) releaseBudget() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inFlight > 0 {
		c.inFlight--
	}
}

func (c *Controller) rateLimited(candidate Candidate, now time.Time, min time.Duration) (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.lastGlobal.IsZero() && now.Sub(c.lastGlobal) < min {
		return true, fmt.Sprintf("minimum interval between rebalance operations has not elapsed: %s remaining", min-now.Sub(c.lastGlobal))
	}
	if last := c.lastShard[candidate.SourceShardID]; !last.IsZero() && now.Sub(last) < min {
		return true, fmt.Sprintf("minimum interval for shard %d has not elapsed: %s remaining", candidate.SourceShardID, min-now.Sub(last))
	}
	return false, ""
}

func (c *Controller) markExecuted(candidate Candidate, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastGlobal = now
	c.lastShard[candidate.SourceShardID] = now
}

func (c *Controller) nowTime() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}
