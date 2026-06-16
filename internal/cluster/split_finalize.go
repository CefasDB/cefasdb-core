package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/internal/placement"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

const splitCopyBatchSize = 1024

const splitFinalizeStatePrefix = storage.Namespace + "cluster/split/finalize/"

type SplitFinalizePhase string

const (
	SplitFinalizePhaseCopying      SplitFinalizePhase = "copying"
	SplitFinalizePhaseCopied       SplitFinalizePhase = "copied"
	SplitFinalizePhaseVerified     SplitFinalizePhase = "verified"
	SplitFinalizePhasePublishing   SplitFinalizePhase = "publishing"
	SplitFinalizePhasePublished    SplitFinalizePhase = "published"
	SplitFinalizePhaseCleanup      SplitFinalizePhase = "cleanup"
	SplitFinalizePhaseDone         SplitFinalizePhase = "done"
	SplitFinalizePhaseVerifyFailed SplitFinalizePhase = "verify_failed"
	SplitFinalizePhaseRolledBack   SplitFinalizePhase = "rolled_back"
)

type SplitFinalizeRequest struct {
	ParentShardID  uint32 `json:"parentShardId"`
	ChildShardID   uint32 `json:"childShardId"`
	ExpectedEpoch  uint64 `json:"expectedEpoch,omitempty"`
	TimeoutMS      int    `json:"timeoutMs,omitempty"`
	WritesQuiesced bool   `json:"writesQuiesced"`
}

type SplitRollbackRequest struct {
	ParentShardID uint32 `json:"parentShardId"`
	ChildShardID  uint32 `json:"childShardId"`
	ExpectedEpoch uint64 `json:"expectedEpoch,omitempty"`
	TimeoutMS     int    `json:"timeoutMs,omitempty"`
}

type SplitVerification struct {
	ParentCount    int64  `json:"parentCount"`
	ChildCount     int64  `json:"childCount"`
	ParentChecksum uint64 `json:"parentChecksum"`
	ChildChecksum  uint64 `json:"childChecksum"`
	Verified       bool   `json:"verified"`
}

type SplitFinalizeState struct {
	ParentShardID     uint32             `json:"parentShardId"`
	ChildShardID      uint32             `json:"childShardId"`
	BeforeEpoch       uint64             `json:"beforeEpoch"`
	AfterEpoch        uint64             `json:"afterEpoch,omitempty"`
	ParentRangeBefore placement.TokenRange         `json:"parentRangeBefore"`
	ParentRangeAfter  placement.TokenRange         `json:"parentRangeAfter"`
	ChildRange        placement.TokenRange         `json:"childRange"`
	Phase             SplitFinalizePhase `json:"phase"`
	CopiedKeys        int64              `json:"copiedKeys,omitempty"`
	CopiedCatalogKeys int64              `json:"copiedCatalogKeys,omitempty"`
	DeletedKeys       int64              `json:"deletedKeys,omitempty"`
	Verification      SplitVerification  `json:"verification,omitempty"`
	UpdatedAtUnix     int64              `json:"updatedAtUnix"`
	LastError         string             `json:"lastError,omitempty"`
}

type SplitFinalizeResult struct {
	ParentShardID     uint32            `json:"parentShardId"`
	ChildShardID      uint32            `json:"childShardId"`
	BeforeEpoch       uint64            `json:"beforeEpoch"`
	AfterEpoch        uint64            `json:"afterEpoch"`
	ParentRangeBefore placement.TokenRange        `json:"parentRangeBefore"`
	ParentRangeAfter  placement.TokenRange        `json:"parentRangeAfter"`
	ChildRange        placement.TokenRange        `json:"childRange"`
	CopiedKeys        int64             `json:"copiedKeys"`
	CopiedCatalogKeys int64             `json:"copiedCatalogKeys"`
	DeletedKeys       int64             `json:"deletedKeys"`
	Phase             string            `json:"phase,omitempty"`
	Verification      SplitVerification `json:"verification,omitempty"`
	Placement         placement.PlacementCatalog  `json:"placement"`
}

type SplitRollbackResult struct {
	ParentShardID      uint32           `json:"parentShardId"`
	ChildShardID       uint32           `json:"childShardId"`
	BeforeEpoch        uint64           `json:"beforeEpoch"`
	AfterEpoch         uint64           `json:"afterEpoch"`
	ChildRange         placement.TokenRange       `json:"childRange"`
	DeletedChildKeys   int64            `json:"deletedChildKeys"`
	DeletedCatalogKeys int64            `json:"deletedCatalogKeys"`
	Phase              string           `json:"phase"`
	Placement          placement.PlacementCatalog `json:"placement"`
}

var splitFinalizeTestHook func(phase SplitFinalizePhase, state SplitFinalizeState) error

func (m *Manager) FinalizeSplit(ctx context.Context, req SplitFinalizeRequest) (SplitFinalizeResult, error) {
	m.splitMu.Lock()
	defer m.splitMu.Unlock()

	if err := m.RefreshPlacement(); err != nil {
		return SplitFinalizeResult{}, err
	}
	current := m.Placement()
	if result, ok, err := m.resumePublishedSplit(ctx, current, req); ok || err != nil {
		return result, err
	}
	parentIdx, childIdx, parent, child, parentRangeAfter, err := validateFinalizeSplit(current, req)
	if err != nil {
		return SplitFinalizeResult{}, err
	}
	parentShard, ok := m.Shard(req.ParentShardID)
	if !ok || parentShard == nil || parentShard.Storage == nil {
		return SplitFinalizeResult{}, placement.InvalidPlan("parent shard %d is not open locally", req.ParentShardID)
	}
	childShard, ok := m.Shard(req.ChildShardID)
	if !ok || childShard == nil || childShard.Storage == nil {
		return SplitFinalizeResult{}, placement.InvalidPlan("child shard %d is not open locally", req.ChildShardID)
	}
	if err := m.requireSplitFinalizeLeaders(parentShard, childShard); err != nil {
		return SplitFinalizeResult{}, err
	}
	tables, err := loadSplitCopyTables(parentShard.Storage)
	if err != nil {
		return SplitFinalizeResult{}, err
	}

	state := SplitFinalizeState{
		ParentShardID:     req.ParentShardID,
		ChildShardID:      req.ChildShardID,
		BeforeEpoch:       current.Epoch,
		ParentRangeBefore: parent.Ranges[0],
		ParentRangeAfter:  parentRangeAfter,
		ChildRange:        child.Ranges[0],
		Phase:             SplitFinalizePhaseCopying,
	}
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := runSplitFinalizeHook(SplitFinalizePhaseCopying, state); err != nil {
		return SplitFinalizeResult{}, err
	}

	catalogKeys, err := copyCatalogKeys(ctx, parentShard.Storage, childShard.Storage)
	if err != nil {
		return SplitFinalizeResult{}, fmt.Errorf("copy catalog keys: %w", err)
	}
	copied, err := copyPrimaryTokenRangeWithIndexes(ctx, tables, parentShard.Storage, childShard.Storage, child.Ranges[0])
	if err != nil {
		return SplitFinalizeResult{}, fmt.Errorf("copy token range: %w", err)
	}
	state.CopiedCatalogKeys = catalogKeys
	state.CopiedKeys = copied
	state.Phase = SplitFinalizePhaseCopied
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := runSplitFinalizeHook(SplitFinalizePhaseCopied, state); err != nil {
		return SplitFinalizeResult{}, err
	}

	verification, err := verifySplitRange(ctx, tables, parentShard.Storage, childShard.Storage, child.Ranges[0])
	state.Verification = verification
	if err != nil {
		state.Phase = SplitFinalizePhaseVerifyFailed
		state.LastError = err.Error()
		_ = m.saveSplitFinalizeState(state)
		return SplitFinalizeResult{}, err
	}
	state.Phase = SplitFinalizePhaseVerified
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := runSplitFinalizeHook(SplitFinalizePhaseVerified, state); err != nil {
		return SplitFinalizeResult{}, err
	}

	after := placement.NextCatalog(current)
	after.Shards[parentIdx].Ranges = []placement.TokenRange{parentRangeAfter}
	after.Shards[parentIdx].State = placement.ShardStateActive
	after.Shards[parentIdx].Epoch = after.Epoch
	after.Shards[childIdx].State = placement.ShardStateActive
	after.Shards[childIdx].Epoch = after.Epoch
	after.Normalize()
	if err := placement.ValidatePlacement(after); err != nil {
		return SplitFinalizeResult{}, err
	}
	state.AfterEpoch = after.Epoch
	state.Phase = SplitFinalizePhasePublishing
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := runSplitFinalizeHook(SplitFinalizePhasePublishing, state); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := m.persistPlacementSnapshotStrict(m.placementPath, after); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := m.applyPlacement(after, false); err != nil {
		return SplitFinalizeResult{}, err
	}
	state.Phase = SplitFinalizePhasePublished
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := runSplitFinalizeHook(SplitFinalizePhasePublished, state); err != nil {
		return SplitFinalizeResult{}, err
	}

	state.Phase = SplitFinalizePhaseCleanup
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, err
	}
	deleted, err := deletePrimaryTokenRangeWithIndexes(ctx, tables, parentShard.Storage, child.Ranges[0])
	if err != nil {
		return SplitFinalizeResult{}, fmt.Errorf("delete parent token range after final placement epoch %d: %w", after.Epoch, err)
	}
	state.DeletedKeys = deleted
	state.Phase = SplitFinalizePhaseDone
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, err
	}

	return SplitFinalizeResult{
		ParentShardID:     req.ParentShardID,
		ChildShardID:      req.ChildShardID,
		BeforeEpoch:       current.Epoch,
		AfterEpoch:        after.Epoch,
		ParentRangeBefore: parent.Ranges[0],
		ParentRangeAfter:  parentRangeAfter,
		ChildRange:        child.Ranges[0],
		CopiedKeys:        copied,
		CopiedCatalogKeys: catalogKeys,
		DeletedKeys:       deleted,
		Phase:             string(state.Phase),
		Verification:      verification,
		Placement:         after.Clone(),
	}, nil
}

func (m *Manager) RollbackSplit(ctx context.Context, req SplitRollbackRequest) (SplitRollbackResult, error) {
	m.splitMu.Lock()
	defer m.splitMu.Unlock()

	if err := m.RefreshPlacement(); err != nil {
		return SplitRollbackResult{}, err
	}
	current := m.Placement()
	state, ok, err := m.loadSplitFinalizeState(req.ParentShardID, req.ChildShardID)
	if err != nil {
		return SplitRollbackResult{}, err
	}
	if ok && splitPhasePublishedOrDone(state.Phase) {
		return SplitRollbackResult{}, placement.InvalidPlan("cannot roll back split after final routing epoch was published; phase=%s", state.Phase)
	}
	parentIdx, childIdx, parent, child, _, err := validateFinalizeSplit(current, SplitFinalizeRequest{
		ParentShardID: req.ParentShardID,
		ChildShardID:  req.ChildShardID,
		ExpectedEpoch: req.ExpectedEpoch,
	})
	if err != nil {
		return SplitRollbackResult{}, err
	}
	parentShard, ok := m.Shard(req.ParentShardID)
	if !ok || parentShard == nil || parentShard.Storage == nil {
		return SplitRollbackResult{}, placement.InvalidPlan("parent shard %d is not open locally", req.ParentShardID)
	}
	childShard, ok := m.Shard(req.ChildShardID)
	if !ok || childShard == nil || childShard.Storage == nil {
		return SplitRollbackResult{}, placement.InvalidPlan("child shard %d is not open locally", req.ChildShardID)
	}
	if err := m.requireSplitFinalizeLeaders(parentShard, childShard); err != nil {
		return SplitRollbackResult{}, err
	}

	deletedData, err := deleteAllKeys(ctx, childShard.Storage, storage.PrefixTables)
	if err != nil {
		return SplitRollbackResult{}, fmt.Errorf("delete child data: %w", err)
	}
	deletedCatalog, err := deleteAllKeys(ctx, childShard.Storage, storage.PrefixCatalog)
	if err != nil {
		return SplitRollbackResult{}, fmt.Errorf("delete child catalog: %w", err)
	}

	after := placement.NextCatalog(current)
	after.Shards[parentIdx].State = placement.ShardStateActive
	after.Shards[parentIdx].Ranges = append([]placement.TokenRange(nil), parent.Ranges...)
	after.Shards[parentIdx].Epoch = after.Epoch
	after.Shards[childIdx].State = placement.ShardStateDecommissioned
	after.Shards[childIdx].Ranges = nil
	after.Shards[childIdx].Epoch = after.Epoch
	after.Normalize()
	if err := placement.ValidatePlacement(after); err != nil {
		return SplitRollbackResult{}, err
	}
	if err := m.persistPlacementSnapshotStrict(m.placementPath, after); err != nil {
		return SplitRollbackResult{}, err
	}
	if err := m.applyPlacement(after, false); err != nil {
		return SplitRollbackResult{}, err
	}
	if !ok {
		state = SplitFinalizeState{
			ParentShardID:     req.ParentShardID,
			ChildShardID:      req.ChildShardID,
			BeforeEpoch:       current.Epoch,
			ParentRangeBefore: parent.Ranges[0],
			ParentRangeAfter:  parent.Ranges[0],
			ChildRange:        child.Ranges[0],
		}
	}
	state.AfterEpoch = after.Epoch
	state.Phase = SplitFinalizePhaseRolledBack
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitRollbackResult{}, err
	}
	return SplitRollbackResult{
		ParentShardID:      req.ParentShardID,
		ChildShardID:       req.ChildShardID,
		BeforeEpoch:        current.Epoch,
		AfterEpoch:         after.Epoch,
		ChildRange:         child.Ranges[0],
		DeletedChildKeys:   deletedData,
		DeletedCatalogKeys: deletedCatalog,
		Phase:              string(state.Phase),
		Placement:          after.Clone(),
	}, nil
}

func (m *Manager) requireSplitFinalizeLeaders(parent, child *Shard) error {
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil {
		return fmt.Errorf("cluster: shard 0 unavailable")
	}
	seen := map[uint32]struct{}{}
	for _, sh := range []*Shard{parent, child, shard0} {
		if sh == nil {
			continue
		}
		if _, ok := seen[sh.ID]; ok {
			continue
		}
		seen[sh.ID] = struct{}{}
		if sh.Raft != nil && !sh.Raft.IsLeader() {
			return fmt.Errorf("cluster: shard %d must be led by this node to finalize split", sh.ID)
		}
	}
	return nil
}

func (m *Manager) resumePublishedSplit(ctx context.Context, current placement.PlacementCatalog, req SplitFinalizeRequest) (SplitFinalizeResult, bool, error) {
	state, ok, err := m.loadSplitFinalizeState(req.ParentShardID, req.ChildShardID)
	if err != nil || !ok {
		return SplitFinalizeResult{}, false, err
	}
	if !splitPhasePublishedOrDone(state.Phase) {
		return SplitFinalizeResult{}, false, nil
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != current.Epoch && req.ExpectedEpoch != state.BeforeEpoch {
		return SplitFinalizeResult{}, true, &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: current.Epoch}
	}
	parentShard, ok := m.Shard(req.ParentShardID)
	if !ok || parentShard == nil || parentShard.Storage == nil {
		return SplitFinalizeResult{}, true, placement.InvalidPlan("parent shard %d is not open locally", req.ParentShardID)
	}
	childShard, ok := m.Shard(req.ChildShardID)
	if !ok || childShard == nil || childShard.Storage == nil {
		return SplitFinalizeResult{}, true, placement.InvalidPlan("child shard %d is not open locally", req.ChildShardID)
	}
	if err := m.requireSplitFinalizeLeaders(parentShard, childShard); err != nil {
		return SplitFinalizeResult{}, true, err
	}
	if err := validatePublishedSplitPlacement(current, state); err != nil {
		return SplitFinalizeResult{}, true, err
	}
	if state.Phase == SplitFinalizePhaseDone {
		return splitFinalizeResultFromState(state, current), true, nil
	}
	tables, err := loadSplitCopyTables(parentShard.Storage)
	if err != nil {
		return SplitFinalizeResult{}, true, err
	}
	state.Phase = SplitFinalizePhaseCleanup
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, true, err
	}
	deleted, err := deletePrimaryTokenRangeWithIndexes(ctx, tables, parentShard.Storage, state.ChildRange)
	if err != nil {
		return SplitFinalizeResult{}, true, fmt.Errorf("delete parent token range after final placement epoch %d: %w", state.AfterEpoch, err)
	}
	state.DeletedKeys += deleted
	state.Phase = SplitFinalizePhaseDone
	if err := m.saveSplitFinalizeState(state); err != nil {
		return SplitFinalizeResult{}, true, err
	}
	return splitFinalizeResultFromState(state, current), true, nil
}

func splitFinalizeResultFromState(state SplitFinalizeState, placement placement.PlacementCatalog) SplitFinalizeResult {
	return SplitFinalizeResult{
		ParentShardID:     state.ParentShardID,
		ChildShardID:      state.ChildShardID,
		BeforeEpoch:       state.BeforeEpoch,
		AfterEpoch:        state.AfterEpoch,
		ParentRangeBefore: state.ParentRangeBefore,
		ParentRangeAfter:  state.ParentRangeAfter,
		ChildRange:        state.ChildRange,
		CopiedKeys:        state.CopiedKeys,
		CopiedCatalogKeys: state.CopiedCatalogKeys,
		DeletedKeys:       state.DeletedKeys,
		Phase:             string(state.Phase),
		Verification:      state.Verification,
		Placement:         placement.Clone(),
	}
}

func validatePublishedSplitPlacement(cat placement.PlacementCatalog, state SplitFinalizeState) error {
	if cat.Strategy != placement.PlacementStrategyTokenRange {
		return placement.InvalidPlan("resume split cleanup requires %s placement, got %s", placement.PlacementStrategyTokenRange, cat.Strategy)
	}
	_, parent, err := placement.FindShard(cat, state.ParentShardID)
	if err != nil {
		return err
	}
	_, child, err := placement.FindShard(cat, state.ChildShardID)
	if err != nil {
		return err
	}
	if parent.State != placement.ShardStateActive || child.State != placement.ShardStateActive {
		return placement.InvalidPlan("resume split cleanup requires active parent and child, got parent=%s child=%s", parent.State, child.State)
	}
	if len(parent.Ranges) != 1 || parent.Ranges[0] != state.ParentRangeAfter {
		return placement.InvalidPlan("resume split cleanup parent range mismatch")
	}
	if len(child.Ranges) != 1 || child.Ranges[0] != state.ChildRange {
		return placement.InvalidPlan("resume split cleanup child range mismatch")
	}
	return nil
}

func splitPhasePublishedOrDone(phase SplitFinalizePhase) bool {
	switch phase {
	case SplitFinalizePhasePublished, SplitFinalizePhaseCleanup, SplitFinalizePhaseDone:
		return true
	default:
		return false
	}
}

func validateFinalizeSplit(cat placement.PlacementCatalog, req SplitFinalizeRequest) (int, int, placement.ShardPlacement, placement.ShardPlacement, placement.TokenRange, error) {
	if cat.Strategy != placement.PlacementStrategyTokenRange {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, placement.InvalidPlan("finalize split requires %s placement, got %s", placement.PlacementStrategyTokenRange, cat.Strategy)
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != cat.Epoch {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: cat.Epoch}
	}
	parentIdx, parent, err := placement.FindShard(cat, req.ParentShardID)
	if err != nil {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, err
	}
	childIdx, child, err := placement.FindShard(cat, req.ChildShardID)
	if err != nil {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, err
	}
	if parent.State != placement.ShardStateSplitting {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, placement.InvalidPlan("parent shard %d must be %s, got %s", parent.ID, placement.ShardStateSplitting, parent.State)
	}
	if child.State != placement.ShardStateCreating {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, placement.InvalidPlan("child shard %d must be %s, got %s", child.ID, placement.ShardStateCreating, child.State)
	}
	if len(parent.Ranges) != 1 || len(child.Ranges) != 1 {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, placement.InvalidPlan("finalize split requires exactly one parent range and one child range")
	}
	parentRange := parent.Ranges[0]
	childRange := child.Ranges[0]
	if childRange.End != parentRange.End {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, placement.InvalidPlan("child range [%d,%d) must end at parent range end %d", childRange.Start, childRange.End, parentRange.End)
	}
	if !placement.TokenStrictlyInside(parentRange, childRange.Start) {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, placement.InvalidPlan("child range start %d is outside parent range [%d,%d)", childRange.Start, parentRange.Start, parentRange.End)
	}
	parentRangeAfter, expectedChild := placement.SplitRange(parentRange, childRange.Start)
	if expectedChild != childRange {
		return 0, 0, placement.ShardPlacement{}, placement.ShardPlacement{}, placement.TokenRange{}, placement.InvalidPlan("child range [%d,%d) does not match parent suffix [%d,%d)", childRange.Start, childRange.End, expectedChild.Start, expectedChild.End)
	}
	return parentIdx, childIdx, parent, child, parentRangeAfter, nil
}

func loadSplitCopyTables(db *storage.DB) ([]types.TableDescriptor, error) {
	cat, err := catalog.New(db)
	if err != nil {
		return nil, fmt.Errorf("load catalog before split finalize: %w", err)
	}
	return cat.List(), nil
}

func (m *Manager) SplitFinalizeState(parentShardID, childShardID uint32) (SplitFinalizeState, bool, error) {
	return m.loadSplitFinalizeState(parentShardID, childShardID)
}

func (m *Manager) loadSplitFinalizeState(parentShardID, childShardID uint32) (SplitFinalizeState, bool, error) {
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return SplitFinalizeState{}, false, fmt.Errorf("cluster: shard 0 unavailable")
	}
	raw, err := shard0.Storage.Get(splitFinalizeStateKey(parentShardID, childShardID))
	if err == storage.ErrNotFound {
		return SplitFinalizeState{}, false, nil
	}
	if err != nil {
		return SplitFinalizeState{}, false, err
	}
	var state SplitFinalizeState
	if err := json.Unmarshal(raw, &state); err != nil {
		return SplitFinalizeState{}, false, fmt.Errorf("decode split finalize state: %w", err)
	}
	return state, true, nil
}

func (m *Manager) saveSplitFinalizeState(state SplitFinalizeState) error {
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return fmt.Errorf("cluster: shard 0 unavailable")
	}
	state.UpdatedAtUnix = time.Now().Unix()
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode split finalize state: %w", err)
	}
	return shard0.Storage.Set(splitFinalizeStateKey(state.ParentShardID, state.ChildShardID), raw)
}

func splitFinalizeStateKey(parentShardID, childShardID uint32) []byte {
	return []byte(fmt.Sprintf("%s%d/%d", splitFinalizeStatePrefix, parentShardID, childShardID))
}

func runSplitFinalizeHook(phase SplitFinalizePhase, state SplitFinalizeState) error {
	if splitFinalizeTestHook == nil {
		return nil
	}
	return splitFinalizeTestHook(phase, state)
}

func copyCatalogKeys(ctx context.Context, src, dst *storage.DB) (int64, error) {
	lower, upper := storage.PrefixCatalog()
	return copyKeys(ctx, src, dst, lower, upper, nil)
}

func copyPrimaryTokenRange(ctx context.Context, src, dst *storage.DB, rng placement.TokenRange) (int64, error) {
	lower, upper := storage.PrefixTables()
	return copyKeys(ctx, src, dst, lower, upper, func(key []byte) bool {
		token, ok := storage.PrimaryTokenFromKey(key)
		return ok && rng.Contains(token)
	})
}

func copyPrimaryTokenRangeWithIndexes(ctx context.Context, tables []types.TableDescriptor, src, dst *storage.DB, rng placement.TokenRange) (int64, error) {
	if src == nil || dst == nil {
		return 0, fmt.Errorf("storage is not open")
	}
	var copied int64
	for _, td := range tables {
		lower, upper := storage.PrefixPrimaryAll(td.Name)
		iter, err := src.Iter(lower, upper)
		if err != nil {
			return copied, err
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				return copied, err
			}
			token, ok := storage.PrimaryTokenFromKey(iter.Key())
			if !ok || !rng.Contains(token) {
				continue
			}
			raw := append([]byte(nil), iter.Value()...)
			item, err := storage.DecodeItem(raw)
			if err != nil {
				_ = iter.Close()
				return copied, fmt.Errorf("decode %s primary row: %w", td.Name, err)
			}
			if err := dst.PutItemWith(td, item, storage.PutOptions{}); err != nil {
				_ = iter.Close()
				return copied, fmt.Errorf("put %s primary row on child: %w", td.Name, err)
			}
			copied++
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return copied, err
		}
		if err := iter.Close(); err != nil {
			return copied, err
		}
	}
	return copied, nil
}

func verifySplitRange(ctx context.Context, tables []types.TableDescriptor, parent, child *storage.DB, rng placement.TokenRange) (SplitVerification, error) {
	parentDigest, err := digestPrimaryTokenRange(ctx, tables, parent, rng)
	if err != nil {
		return SplitVerification{}, fmt.Errorf("digest parent range: %w", err)
	}
	childDigest, err := digestPrimaryTokenRange(ctx, tables, child, rng)
	if err != nil {
		return SplitVerification{}, fmt.Errorf("digest child range: %w", err)
	}
	out := SplitVerification{
		ParentCount:    parentDigest.count,
		ChildCount:     childDigest.count,
		ParentChecksum: parentDigest.checksum,
		ChildChecksum:  childDigest.checksum,
		Verified:       parentDigest == childDigest,
	}
	if !out.Verified {
		return out, placement.InvalidPlan("split verification failed: parent count/checksum %d/%d child %d/%d", out.ParentCount, out.ParentChecksum, out.ChildCount, out.ChildChecksum)
	}
	return out, nil
}

type splitDigest struct {
	count    int64
	checksum uint64
}

func digestPrimaryTokenRange(ctx context.Context, tables []types.TableDescriptor, db *storage.DB, rng placement.TokenRange) (splitDigest, error) {
	var out splitDigest
	for _, td := range tables {
		lower, upper := storage.PrefixPrimaryAll(td.Name)
		iter, err := db.Iter(lower, upper)
		if err != nil {
			return out, err
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				return out, err
			}
			token, ok := storage.PrimaryTokenFromKey(iter.Key())
			if !ok || !rng.Contains(token) {
				continue
			}
			out.count++
			out.checksum ^= checksumKV(iter.Key(), iter.Value())
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return out, err
		}
		if err := iter.Close(); err != nil {
			return out, err
		}
	}
	return out, nil
}

func checksumKV(key, value []byte) uint64 {
	h := xxhash.New()
	_, _ = h.Write(key)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(value)
	return h.Sum64()
}

func deletePrimaryTokenRange(ctx context.Context, db *storage.DB, rng placement.TokenRange) (int64, error) {
	lower, upper := storage.PrefixTables()
	return deleteKeys(ctx, db, lower, upper, func(key []byte) bool {
		token, ok := storage.PrimaryTokenFromKey(key)
		return ok && rng.Contains(token)
	})
}

func deletePrimaryTokenRangeWithIndexes(ctx context.Context, tables []types.TableDescriptor, db *storage.DB, rng placement.TokenRange) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("storage is not open")
	}
	var deleted int64
	for _, td := range tables {
		items, err := primaryItemsInRange(ctx, db, td, rng)
		if err != nil {
			return deleted, err
		}
		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return deleted, err
			}
			if err := db.DeleteItemWith(td, item, storage.DeleteOptions{}); err != nil {
				return deleted, fmt.Errorf("delete %s primary row from parent: %w", td.Name, err)
			}
			deleted++
		}
	}
	return deleted, nil
}

func primaryItemsInRange(ctx context.Context, db *storage.DB, td types.TableDescriptor, rng placement.TokenRange) ([]types.Item, error) {
	lower, upper := storage.PrefixPrimaryAll(td.Name)
	iter, err := db.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var items []types.Item
	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		token, ok := storage.PrimaryTokenFromKey(iter.Key())
		if !ok || !rng.Contains(token) {
			continue
		}
		raw := append([]byte(nil), iter.Value()...)
		item, err := storage.DecodeItem(raw)
		if err != nil {
			return nil, fmt.Errorf("decode %s primary row: %w", td.Name, err)
		}
		items = append(items, item)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return items, nil
}

func deleteAllKeys(ctx context.Context, db *storage.DB, bounds func() ([]byte, []byte)) (int64, error) {
	lower, upper := bounds()
	return deleteKeys(ctx, db, lower, upper, nil)
}

func copyKeys(ctx context.Context, src, dst *storage.DB, lower, upper []byte, include func([]byte) bool) (int64, error) {
	if src == nil || dst == nil {
		return 0, fmt.Errorf("storage is not open")
	}
	iter, err := src.Iter(lower, upper)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	batch := dst.Batch()
	defer batch.Close()
	pending := 0
	var copied int64
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := dst.CommitBatch(batch); err != nil {
			return err
		}
		_ = batch.Close()
		batch = dst.Batch()
		pending = 0
		return nil
	}

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return copied, err
		}
		if include != nil && !include(iter.Key()) {
			continue
		}
		key := append([]byte(nil), iter.Key()...)
		value := append([]byte(nil), iter.Value()...)
		if err := batch.Set(key, value, nil); err != nil {
			return copied, err
		}
		pending++
		copied++
		if pending >= splitCopyBatchSize {
			if err := flush(); err != nil {
				return copied, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return copied, err
	}
	if err := flush(); err != nil {
		return copied, err
	}
	return copied, nil
}

func deleteKeys(ctx context.Context, db *storage.DB, lower, upper []byte, include func([]byte) bool) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("storage is not open")
	}
	iter, err := db.Iter(lower, upper)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	batch := db.Batch()
	defer batch.Close()
	pending := 0
	var deleted int64
	flush := func() error {
		if pending == 0 {
			return nil
		}
		if err := db.CommitBatch(batch); err != nil {
			return err
		}
		_ = batch.Close()
		batch = db.Batch()
		pending = 0
		return nil
	}

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if include != nil && !include(iter.Key()) {
			continue
		}
		key := append([]byte(nil), iter.Key()...)
		if err := batch.Delete(key, nil); err != nil {
			return deleted, err
		}
		pending++
		deleted++
		if pending >= splitCopyBatchSize {
			if err := flush(); err != nil {
				return deleted, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return deleted, err
	}
	if err := flush(); err != nil {
		return deleted, err
	}
	return deleted, nil
}
