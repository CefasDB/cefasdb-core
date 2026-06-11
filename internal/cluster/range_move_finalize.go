package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/osvaldoandrade/cefas/internal/storage"
)

const rangeMoveFinalizeStatePrefix = storage.Namespace + "cluster/range-move/finalize/"

type RangeMoveFinalizePhase string

const (
	RangeMoveFinalizePhaseCopying      RangeMoveFinalizePhase = "copying"
	RangeMoveFinalizePhaseCopied       RangeMoveFinalizePhase = "copied"
	RangeMoveFinalizePhaseVerified     RangeMoveFinalizePhase = "verified"
	RangeMoveFinalizePhasePublishing   RangeMoveFinalizePhase = "publishing"
	RangeMoveFinalizePhasePublished    RangeMoveFinalizePhase = "published"
	RangeMoveFinalizePhaseCleanup      RangeMoveFinalizePhase = "cleanup"
	RangeMoveFinalizePhaseDone         RangeMoveFinalizePhase = "done"
	RangeMoveFinalizePhaseVerifyFailed RangeMoveFinalizePhase = "verify_failed"
)

type RangeMoveFinalizeRequest struct {
	SourceShardID uint32 `json:"sourceShardId"`
	TargetShardID uint32 `json:"targetShardId"`
	ExpectedEpoch uint64 `json:"expectedEpoch,omitempty"`
	TimeoutMS     int    `json:"timeoutMs,omitempty"`
}

type RangeMoveFinalizeState struct {
	SourceShardID      uint32                 `json:"sourceShardId"`
	TargetShardID      uint32                 `json:"targetShardId"`
	BeforeEpoch        uint64                 `json:"beforeEpoch"`
	AfterEpoch         uint64                 `json:"afterEpoch,omitempty"`
	SourceRangesBefore []TokenRange           `json:"sourceRangesBefore,omitempty"`
	SourceRangesAfter  []TokenRange           `json:"sourceRangesAfter,omitempty"`
	MovedRange         TokenRange             `json:"movedRange"`
	Phase              RangeMoveFinalizePhase `json:"phase"`
	CopiedKeys         int64                  `json:"copiedKeys,omitempty"`
	CopiedCatalogKeys  int64                  `json:"copiedCatalogKeys,omitempty"`
	DeletedKeys        int64                  `json:"deletedKeys,omitempty"`
	Verification       SplitVerification      `json:"verification,omitempty"`
	UpdatedAtUnix      int64                  `json:"updatedAtUnix"`
	LastError          string                 `json:"lastError,omitempty"`
}

type RangeMoveFinalizeResult struct {
	SourceShardID      uint32            `json:"sourceShardId"`
	TargetShardID      uint32            `json:"targetShardId"`
	BeforeEpoch        uint64            `json:"beforeEpoch"`
	AfterEpoch         uint64            `json:"afterEpoch"`
	SourceRangesBefore []TokenRange      `json:"sourceRangesBefore,omitempty"`
	SourceRangesAfter  []TokenRange      `json:"sourceRangesAfter,omitempty"`
	MovedRange         TokenRange        `json:"movedRange"`
	CopiedKeys         int64             `json:"copiedKeys"`
	CopiedCatalogKeys  int64             `json:"copiedCatalogKeys"`
	DeletedKeys        int64             `json:"deletedKeys"`
	Phase              string            `json:"phase,omitempty"`
	Verification       SplitVerification `json:"verification,omitempty"`
	Placement          PlacementCatalog  `json:"placement"`
}

var rangeMoveFinalizeTestHook func(phase RangeMoveFinalizePhase, state RangeMoveFinalizeState) error

func (m *Manager) FinalizeRangeMove(ctx context.Context, req RangeMoveFinalizeRequest) (RangeMoveFinalizeResult, error) {
	m.splitMu.Lock()
	defer m.splitMu.Unlock()

	if err := m.RefreshPlacement(); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	current := m.Placement()
	if result, ok, err := m.resumePublishedRangeMove(ctx, current, req); ok || err != nil {
		return result, err
	}
	sourceIdx, targetIdx, source, target, sourceRangesAfter, err := validateFinalizeRangeMove(current, req)
	if err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	sourceShard, ok := m.Shard(req.SourceShardID)
	if !ok || sourceShard == nil || sourceShard.Storage == nil {
		return RangeMoveFinalizeResult{}, invalidPlan("source shard %d is not open locally", req.SourceShardID)
	}
	targetShard, ok := m.Shard(req.TargetShardID)
	if !ok || targetShard == nil || targetShard.Storage == nil {
		return RangeMoveFinalizeResult{}, invalidPlan("target shard %d is not open locally", req.TargetShardID)
	}
	if err := m.requireRangeMoveFinalizeLeaders(sourceShard, targetShard); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	tables, err := loadSplitCopyTables(sourceShard.Storage)
	if err != nil {
		return RangeMoveFinalizeResult{}, err
	}

	state := RangeMoveFinalizeState{
		SourceShardID:      req.SourceShardID,
		TargetShardID:      req.TargetShardID,
		BeforeEpoch:        current.Epoch,
		SourceRangesBefore: append([]TokenRange(nil), source.Ranges...),
		SourceRangesAfter:  append([]TokenRange(nil), sourceRangesAfter...),
		MovedRange:         target.Ranges[0],
		Phase:              RangeMoveFinalizePhaseCopying,
	}
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	if err := runRangeMoveFinalizeHook(RangeMoveFinalizePhaseCopying, state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}

	catalogKeys, err := copyCatalogKeys(ctx, sourceShard.Storage, targetShard.Storage)
	if err != nil {
		return RangeMoveFinalizeResult{}, fmt.Errorf("copy catalog keys: %w", err)
	}
	copied, err := copyPrimaryTokenRangeWithIndexes(ctx, tables, sourceShard.Storage, targetShard.Storage, target.Ranges[0])
	if err != nil {
		return RangeMoveFinalizeResult{}, fmt.Errorf("copy token range: %w", err)
	}
	state.CopiedCatalogKeys = catalogKeys
	state.CopiedKeys = copied
	state.Phase = RangeMoveFinalizePhaseCopied
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	if err := runRangeMoveFinalizeHook(RangeMoveFinalizePhaseCopied, state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}

	verification, err := verifySplitRange(ctx, tables, sourceShard.Storage, targetShard.Storage, target.Ranges[0])
	state.Verification = verification
	if err != nil {
		state.Phase = RangeMoveFinalizePhaseVerifyFailed
		state.LastError = err.Error()
		_ = m.saveRangeMoveFinalizeState(state)
		return RangeMoveFinalizeResult{}, err
	}
	state.Phase = RangeMoveFinalizePhaseVerified
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	if err := runRangeMoveFinalizeHook(RangeMoveFinalizePhaseVerified, state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}

	after := nextCatalog(current)
	after.Shards[sourceIdx].Ranges = append([]TokenRange(nil), sourceRangesAfter...)
	if len(sourceRangesAfter) == 0 {
		after.Shards[sourceIdx].State = ShardStateDecommissioned
	} else {
		after.Shards[sourceIdx].State = ShardStateActive
	}
	after.Shards[sourceIdx].Epoch = after.Epoch
	after.Shards[targetIdx].State = ShardStateActive
	after.Shards[targetIdx].Epoch = after.Epoch
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	state.AfterEpoch = after.Epoch
	state.Phase = RangeMoveFinalizePhasePublishing
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	if err := runRangeMoveFinalizeHook(RangeMoveFinalizePhasePublishing, state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	if err := m.persistPlacementSnapshotStrict(m.placementPath, after); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	if err := m.applyPlacement(after, false); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	state.Phase = RangeMoveFinalizePhasePublished
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	if err := runRangeMoveFinalizeHook(RangeMoveFinalizePhasePublished, state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}

	state.Phase = RangeMoveFinalizePhaseCleanup
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	deleted, err := deletePrimaryTokenRangeWithIndexes(ctx, tables, sourceShard.Storage, target.Ranges[0])
	if err != nil {
		return RangeMoveFinalizeResult{}, fmt.Errorf("delete source token range after final placement epoch %d: %w", after.Epoch, err)
	}
	state.DeletedKeys = deleted
	state.Phase = RangeMoveFinalizePhaseDone
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, err
	}

	return RangeMoveFinalizeResult{
		SourceShardID:      req.SourceShardID,
		TargetShardID:      req.TargetShardID,
		BeforeEpoch:        current.Epoch,
		AfterEpoch:         after.Epoch,
		SourceRangesBefore: append([]TokenRange(nil), source.Ranges...),
		SourceRangesAfter:  append([]TokenRange(nil), sourceRangesAfter...),
		MovedRange:         target.Ranges[0],
		CopiedKeys:         copied,
		CopiedCatalogKeys:  catalogKeys,
		DeletedKeys:        deleted,
		Phase:              string(state.Phase),
		Verification:       verification,
		Placement:          after.Clone(),
	}, nil
}

func (m *Manager) resumePublishedRangeMove(ctx context.Context, current PlacementCatalog, req RangeMoveFinalizeRequest) (RangeMoveFinalizeResult, bool, error) {
	state, ok, err := m.loadRangeMoveFinalizeState(req.SourceShardID, req.TargetShardID)
	if err != nil || !ok {
		return RangeMoveFinalizeResult{}, false, err
	}
	if !rangeMovePhasePublishedOrDone(state.Phase) {
		return RangeMoveFinalizeResult{}, false, nil
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != current.Epoch && req.ExpectedEpoch != state.BeforeEpoch {
		return RangeMoveFinalizeResult{}, true, &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: current.Epoch}
	}
	sourceShard, ok := m.Shard(req.SourceShardID)
	if !ok || sourceShard == nil || sourceShard.Storage == nil {
		return RangeMoveFinalizeResult{}, true, invalidPlan("source shard %d is not open locally", req.SourceShardID)
	}
	targetShard, ok := m.Shard(req.TargetShardID)
	if !ok || targetShard == nil || targetShard.Storage == nil {
		return RangeMoveFinalizeResult{}, true, invalidPlan("target shard %d is not open locally", req.TargetShardID)
	}
	if err := m.requireRangeMoveFinalizeLeaders(sourceShard, targetShard); err != nil {
		return RangeMoveFinalizeResult{}, true, err
	}
	if err := validatePublishedRangeMovePlacement(current, state); err != nil {
		return RangeMoveFinalizeResult{}, true, err
	}
	if state.Phase == RangeMoveFinalizePhaseDone {
		return rangeMoveFinalizeResultFromState(state, current), true, nil
	}
	tables, err := loadSplitCopyTables(sourceShard.Storage)
	if err != nil {
		return RangeMoveFinalizeResult{}, true, err
	}
	state.Phase = RangeMoveFinalizePhaseCleanup
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, true, err
	}
	deleted, err := deletePrimaryTokenRangeWithIndexes(ctx, tables, sourceShard.Storage, state.MovedRange)
	if err != nil {
		return RangeMoveFinalizeResult{}, true, fmt.Errorf("delete source token range after final placement epoch %d: %w", state.AfterEpoch, err)
	}
	state.DeletedKeys += deleted
	state.Phase = RangeMoveFinalizePhaseDone
	if err := m.saveRangeMoveFinalizeState(state); err != nil {
		return RangeMoveFinalizeResult{}, true, err
	}
	return rangeMoveFinalizeResultFromState(state, current), true, nil
}

func validateFinalizeRangeMove(cat PlacementCatalog, req RangeMoveFinalizeRequest) (int, int, ShardPlacement, ShardPlacement, []TokenRange, error) {
	if cat.Strategy != PlacementStrategyTokenRange {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, invalidPlan("finalize range_move requires %s placement, got %s", PlacementStrategyTokenRange, cat.Strategy)
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != cat.Epoch {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: cat.Epoch}
	}
	sourceIdx, source, err := findShard(cat, req.SourceShardID)
	if err != nil {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, err
	}
	targetIdx, target, err := findShard(cat, req.TargetShardID)
	if err != nil {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, err
	}
	if source.ID == target.ID {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, invalidPlan("range_move source and target shards must differ")
	}
	if source.State != ShardStateMoving {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, invalidPlan("source shard %d must be %s, got %s", source.ID, ShardStateMoving, source.State)
	}
	if target.State != ShardStateCreating {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, invalidPlan("target shard %d must be %s, got %s", target.ID, ShardStateCreating, target.State)
	}
	if len(target.Ranges) != 1 {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, invalidPlan("finalize range_move requires exactly one target range")
	}
	sourceRangesAfter, err := subtractTokenRanges(source.Ranges, target.Ranges[0])
	if err != nil {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, nil, fmt.Errorf("%w: range_move target range [%d,%d) is not owned by source shard %d", ErrInvalidPlacementPlan, target.Ranges[0].Start, target.Ranges[0].End, source.ID)
	}
	return sourceIdx, targetIdx, source, target, sourceRangesAfter, nil
}

func validatePublishedRangeMovePlacement(cat PlacementCatalog, state RangeMoveFinalizeState) error {
	if cat.Strategy != PlacementStrategyTokenRange {
		return invalidPlan("resume range_move cleanup requires %s placement, got %s", PlacementStrategyTokenRange, cat.Strategy)
	}
	_, source, err := findShard(cat, state.SourceShardID)
	if err != nil {
		return err
	}
	_, target, err := findShard(cat, state.TargetShardID)
	if err != nil {
		return err
	}
	if source.State != ShardStateActive && source.State != ShardStateDecommissioned {
		return invalidPlan("resume range_move cleanup requires active or decommissioned source, got %s", source.State)
	}
	if target.State != ShardStateActive {
		return invalidPlan("resume range_move cleanup requires active target, got %s", target.State)
	}
	if !sameTokenRanges(source.Ranges, state.SourceRangesAfter) {
		return invalidPlan("resume range_move cleanup source ranges mismatch")
	}
	if len(target.Ranges) != 1 || target.Ranges[0] != state.MovedRange {
		return invalidPlan("resume range_move cleanup target range mismatch")
	}
	return nil
}

func rangeMoveFinalizeResultFromState(state RangeMoveFinalizeState, placement PlacementCatalog) RangeMoveFinalizeResult {
	return RangeMoveFinalizeResult{
		SourceShardID:      state.SourceShardID,
		TargetShardID:      state.TargetShardID,
		BeforeEpoch:        state.BeforeEpoch,
		AfterEpoch:         state.AfterEpoch,
		SourceRangesBefore: append([]TokenRange(nil), state.SourceRangesBefore...),
		SourceRangesAfter:  append([]TokenRange(nil), state.SourceRangesAfter...),
		MovedRange:         state.MovedRange,
		CopiedKeys:         state.CopiedKeys,
		CopiedCatalogKeys:  state.CopiedCatalogKeys,
		DeletedKeys:        state.DeletedKeys,
		Phase:              string(state.Phase),
		Verification:       state.Verification,
		Placement:          placement.Clone(),
	}
}

func rangeMovePhasePublishedOrDone(phase RangeMoveFinalizePhase) bool {
	switch phase {
	case RangeMoveFinalizePhasePublished, RangeMoveFinalizePhaseCleanup, RangeMoveFinalizePhaseDone:
		return true
	default:
		return false
	}
}

func (m *Manager) requireRangeMoveFinalizeLeaders(source, target *Shard) error {
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil {
		return fmt.Errorf("cluster: shard 0 unavailable")
	}
	seen := map[uint32]struct{}{}
	for _, sh := range []*Shard{source, target, shard0} {
		if sh == nil {
			continue
		}
		if _, ok := seen[sh.ID]; ok {
			continue
		}
		seen[sh.ID] = struct{}{}
		if sh.Raft != nil && !sh.Raft.IsLeader() {
			return fmt.Errorf("cluster: shard %d must be led by this node to finalize range move", sh.ID)
		}
	}
	return nil
}

func (m *Manager) RangeMoveFinalizeState(sourceShardID, targetShardID uint32) (RangeMoveFinalizeState, bool, error) {
	return m.loadRangeMoveFinalizeState(sourceShardID, targetShardID)
}

func (m *Manager) loadRangeMoveFinalizeState(sourceShardID, targetShardID uint32) (RangeMoveFinalizeState, bool, error) {
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return RangeMoveFinalizeState{}, false, fmt.Errorf("cluster: shard 0 unavailable")
	}
	raw, err := shard0.Storage.Get(rangeMoveFinalizeStateKey(sourceShardID, targetShardID))
	if err == storage.ErrNotFound {
		return RangeMoveFinalizeState{}, false, nil
	}
	if err != nil {
		return RangeMoveFinalizeState{}, false, err
	}
	var state RangeMoveFinalizeState
	if err := json.Unmarshal(raw, &state); err != nil {
		return RangeMoveFinalizeState{}, false, fmt.Errorf("decode range_move finalize state: %w", err)
	}
	return state, true, nil
}

func (m *Manager) saveRangeMoveFinalizeState(state RangeMoveFinalizeState) error {
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return fmt.Errorf("cluster: shard 0 unavailable")
	}
	state.UpdatedAtUnix = time.Now().Unix()
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode range_move finalize state: %w", err)
	}
	return shard0.Storage.Set(rangeMoveFinalizeStateKey(state.SourceShardID, state.TargetShardID), raw)
}

func rangeMoveFinalizeStateKey(sourceShardID, targetShardID uint32) []byte {
	return []byte(fmt.Sprintf("%s%d/%d", rangeMoveFinalizeStatePrefix, sourceShardID, targetShardID))
}

func runRangeMoveFinalizeHook(phase RangeMoveFinalizePhase, state RangeMoveFinalizeState) error {
	if rangeMoveFinalizeTestHook == nil {
		return nil
	}
	return rangeMoveFinalizeTestHook(phase, state)
}
