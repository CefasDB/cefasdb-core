package cluster

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
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

func placementNodeActiveReferences(cat PlacementCatalog, nodeID model.NodeID) []string {
	key := nodeID.String()
	var blockers []string
	for _, shard := range cat.Shards {
		if shard.State == ShardStateDecommissioned {
			continue
		}
		if containsString(shard.Voters, key) {
			blockers = append(blockers, fmt.Sprintf("shard %d voter state=%s ranges=%d", shard.ID, shard.State, len(shard.Ranges)))
		}
		if containsString(shard.NonVoters, key) {
			blockers = append(blockers, fmt.Sprintf("shard %d non-voter state=%s", shard.ID, shard.State))
		}
		if shard.LeaderHint == key {
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
