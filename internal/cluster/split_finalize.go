package cluster

import (
	"context"
	"fmt"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
)

const splitCopyBatchSize = 1024

type SplitFinalizeRequest struct {
	ParentShardID  uint32 `json:"parentShardId"`
	ChildShardID   uint32 `json:"childShardId"`
	ExpectedEpoch  uint64 `json:"expectedEpoch,omitempty"`
	TimeoutMS      int    `json:"timeoutMs,omitempty"`
	WritesQuiesced bool   `json:"writesQuiesced"`
}

type SplitFinalizeResult struct {
	ParentShardID     uint32           `json:"parentShardId"`
	ChildShardID      uint32           `json:"childShardId"`
	BeforeEpoch       uint64           `json:"beforeEpoch"`
	AfterEpoch        uint64           `json:"afterEpoch"`
	ParentRangeBefore TokenRange       `json:"parentRangeBefore"`
	ParentRangeAfter  TokenRange       `json:"parentRangeAfter"`
	ChildRange        TokenRange       `json:"childRange"`
	CopiedKeys        int64            `json:"copiedKeys"`
	CopiedCatalogKeys int64            `json:"copiedCatalogKeys"`
	DeletedKeys       int64            `json:"deletedKeys"`
	Placement         PlacementCatalog `json:"placement"`
}

func (m *Manager) FinalizeSplit(ctx context.Context, req SplitFinalizeRequest) (SplitFinalizeResult, error) {
	m.splitMu.Lock()
	defer m.splitMu.Unlock()

	if err := m.RefreshPlacement(); err != nil {
		return SplitFinalizeResult{}, err
	}
	current := m.Placement()
	parentIdx, childIdx, parent, child, parentRangeAfter, err := validateFinalizeSplit(current, req)
	if err != nil {
		return SplitFinalizeResult{}, err
	}
	parentShard, ok := m.Shard(req.ParentShardID)
	if !ok || parentShard == nil || parentShard.Storage == nil {
		return SplitFinalizeResult{}, invalidPlan("parent shard %d is not open locally", req.ParentShardID)
	}
	childShard, ok := m.Shard(req.ChildShardID)
	if !ok || childShard == nil || childShard.Storage == nil {
		return SplitFinalizeResult{}, invalidPlan("child shard %d is not open locally", req.ChildShardID)
	}
	if err := m.requireSplitFinalizeLeaders(parentShard, childShard); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := ensureSplitCopySupported(parentShard.Storage); err != nil {
		return SplitFinalizeResult{}, err
	}

	catalogKeys, err := copyCatalogKeys(ctx, parentShard.Storage, childShard.Storage)
	if err != nil {
		return SplitFinalizeResult{}, fmt.Errorf("copy catalog keys: %w", err)
	}
	copied, err := copyPrimaryTokenRange(ctx, parentShard.Storage, childShard.Storage, child.Ranges[0])
	if err != nil {
		return SplitFinalizeResult{}, fmt.Errorf("copy token range: %w", err)
	}

	after := nextCatalog(current)
	after.Shards[parentIdx].Ranges = []TokenRange{parentRangeAfter}
	after.Shards[parentIdx].State = ShardStateActive
	after.Shards[parentIdx].Epoch = after.Epoch
	after.Shards[childIdx].State = ShardStateActive
	after.Shards[childIdx].Epoch = after.Epoch
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := m.persistPlacementSnapshotStrict(m.placementPath, after); err != nil {
		return SplitFinalizeResult{}, err
	}
	if err := m.applyPlacement(after, false); err != nil {
		return SplitFinalizeResult{}, err
	}
	deleted, err := deletePrimaryTokenRange(ctx, parentShard.Storage, child.Ranges[0])
	if err != nil {
		return SplitFinalizeResult{}, fmt.Errorf("delete parent token range after final placement epoch %d: %w", after.Epoch, err)
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
		Placement:         after.Clone(),
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

func validateFinalizeSplit(cat PlacementCatalog, req SplitFinalizeRequest) (int, int, ShardPlacement, ShardPlacement, TokenRange, error) {
	if cat.Strategy != PlacementStrategyTokenRange {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, invalidPlan("finalize split requires %s placement, got %s", PlacementStrategyTokenRange, cat.Strategy)
	}
	if req.ExpectedEpoch != 0 && req.ExpectedEpoch != cat.Epoch {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, &StaleRouteError{ClientEpoch: req.ExpectedEpoch, CurrentEpoch: cat.Epoch}
	}
	parentIdx, parent, err := findShard(cat, req.ParentShardID)
	if err != nil {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, err
	}
	childIdx, child, err := findShard(cat, req.ChildShardID)
	if err != nil {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, err
	}
	if parent.State != ShardStateSplitting {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, invalidPlan("parent shard %d must be %s, got %s", parent.ID, ShardStateSplitting, parent.State)
	}
	if child.State != ShardStateCreating {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, invalidPlan("child shard %d must be %s, got %s", child.ID, ShardStateCreating, child.State)
	}
	if len(parent.Ranges) != 1 || len(child.Ranges) != 1 {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, invalidPlan("finalize split requires exactly one parent range and one child range")
	}
	parentRange := parent.Ranges[0]
	childRange := child.Ranges[0]
	if childRange.End != parentRange.End {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, invalidPlan("child range [%d,%d) must end at parent range end %d", childRange.Start, childRange.End, parentRange.End)
	}
	if !tokenStrictlyInside(parentRange, childRange.Start) {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, invalidPlan("child range start %d is outside parent range [%d,%d)", childRange.Start, parentRange.Start, parentRange.End)
	}
	parentRangeAfter, expectedChild := splitRange(parentRange, childRange.Start)
	if expectedChild != childRange {
		return 0, 0, ShardPlacement{}, ShardPlacement{}, TokenRange{}, invalidPlan("child range [%d,%d) does not match parent suffix [%d,%d)", childRange.Start, childRange.End, expectedChild.Start, expectedChild.End)
	}
	return parentIdx, childIdx, parent, child, parentRangeAfter, nil
}

func ensureSplitCopySupported(db *storage.DB) error {
	cat, err := catalog.New(db)
	if err != nil {
		return fmt.Errorf("load catalog before split finalize: %w", err)
	}
	for _, td := range cat.List() {
		if len(td.GSIs) > 0 || len(td.LSIs) > 0 || len(td.SpatialIndexes) > 0 || td.TTLAttribute != "" {
			return invalidPlan("split finalize currently supports primary rows only; table %q has secondary indexes, spatial indexes, or TTL", td.Name)
		}
	}
	return nil
}

func copyCatalogKeys(ctx context.Context, src, dst *storage.DB) (int64, error) {
	lower, upper := storage.PrefixCatalog()
	return copyKeys(ctx, src, dst, lower, upper, nil)
}

func copyPrimaryTokenRange(ctx context.Context, src, dst *storage.DB, rng TokenRange) (int64, error) {
	lower, upper := storage.PrefixTables()
	return copyKeys(ctx, src, dst, lower, upper, func(key []byte) bool {
		token, ok := storage.PrimaryTokenFromKey(key)
		return ok && rng.Contains(token)
	})
}

func deletePrimaryTokenRange(ctx context.Context, db *storage.DB, rng TokenRange) (int64, error) {
	lower, upper := storage.PrefixTables()
	return deleteKeys(ctx, db, lower, upper, func(key []byte) bool {
		token, ok := storage.PrimaryTokenFromKey(key)
		return ok && rng.Contains(token)
	})
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
