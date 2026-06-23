package server

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// applyGlobalIndexPut runs every GlobalIndex attached to td against
// the just-written base item. Phase 2 of epic #509 — mirror of the
// MV eager hook (#491 + #535/#537 cross-shard cascade).
//
// Limitation (locked in ADR 0005 §3 for Phase 2): when the indexed
// column is not part of the base table's key schema, an update that
// changes its value cannot evict the old index entry without a
// prior read. v1 leaves stale pointers; the Phase 4 rebuild (#513)
// reconciles. Same constraint MV carries today.
func (s *GRPCServer) applyGlobalIndexPut(ctx context.Context, td types.TableDescriptor, item types.Item) error {
	if len(td.GlobalIndexes) == 0 || s.cat == nil {
		return nil
	}
	for _, idxName := range td.GlobalIndexes {
		gi, err := s.cat.DescribeGlobalIndex(idxName)
		if err != nil {
			return status.Errorf(codes.Internal, "gi lookup %s: %v", idxName, err)
		}
		if gi.Paused {
			continue
		}
		giItem := deriveGIItem(gi, td.KeySchema, item)
		if giItem == nil {
			continue
		}
		started := time.Now()
		if err := s.writeGIRow(ctx, gi, td.KeySchema.PK, giItem); err != nil {
			return status.Errorf(codes.Internal, "gi %s write: %v", gi.Name, err)
		}
		s.giObserveDuration(gi.Name, "put", started)
	}
	return nil
}

// applyGlobalIndexDelete cascades a base delete to every attached
// global index. Derives the index key from baseKey via deriveGIItem;
// when IndexedColumn is not in the base key the call no-ops (the
// rebuild path #513 reconciles).
func (s *GRPCServer) applyGlobalIndexDelete(ctx context.Context, td types.TableDescriptor, baseKey types.Item) error {
	if len(td.GlobalIndexes) == 0 || s.cat == nil {
		return nil
	}
	for _, idxName := range td.GlobalIndexes {
		gi, err := s.cat.DescribeGlobalIndex(idxName)
		if err != nil {
			return status.Errorf(codes.Internal, "gi lookup %s: %v", idxName, err)
		}
		if gi.Paused {
			continue
		}
		giItem := deriveGIItem(gi, td.KeySchema, baseKey)
		if giItem == nil {
			continue
		}
		giKey := giKeyOnly(giItem, gi.IndexedColumn, td.KeySchema.PK)
		started := time.Now()
		if err := s.deleteGIRow(ctx, gi, td.KeySchema.PK, giKey); err != nil {
			return status.Errorf(codes.Internal, "gi %s delete: %v", gi.Name, err)
		}
		s.giObserveDuration(gi.Name, "delete", started)
	}
	return nil
}

// applyGlobalIndexBatch fans out a BatchWriteItem's worth of puts +
// deletes to every attached GI. Coalesces per (gi, shard) bucket
// and dispatches in parallel through the cross-shard plumbing
// shared with MV (#535 / #537).
func (s *GRPCServer) applyGlobalIndexBatch(ctx context.Context, td types.TableDescriptor, ops []pebble.BatchOp) error {
	if len(td.GlobalIndexes) == 0 || s.cat == nil {
		return nil
	}
	for _, idxName := range td.GlobalIndexes {
		gi, err := s.cat.DescribeGlobalIndex(idxName)
		if err != nil {
			return status.Errorf(codes.Internal, "gi lookup %s: %v", idxName, err)
		}
		if gi.Paused {
			continue
		}
		if err := s.applyGlobalIndexBatchOneIndex(ctx, td, gi, ops); err != nil {
			return err
		}
	}
	return nil
}

func (s *GRPCServer) applyGlobalIndexBatchOneIndex(ctx context.Context, td types.TableDescriptor, gi types.GlobalIndexDescriptor, ops []pebble.BatchOp) error {
	giTD := giSyntheticTableDescriptor(gi, td.KeySchema.PK)
	giOps := make([]pebble.BatchOp, 0, len(ops))
	for _, op := range ops {
		switch op.Op {
		case pebble.BatchOpPut:
			giItem := deriveGIItem(gi, td.KeySchema, op.Item)
			if giItem == nil {
				continue
			}
			giOps = append(giOps, pebble.BatchOp{Op: pebble.BatchOpPut, Item: giItem})
		case pebble.BatchOpDelete:
			giItem := deriveGIItem(gi, td.KeySchema, op.Key)
			if giItem == nil {
				continue
			}
			giOps = append(giOps, pebble.BatchOp{
				Op:  pebble.BatchOpDelete,
				Key: giKeyOnly(giItem, gi.IndexedColumn, td.KeySchema.PK),
			})
		}
	}
	if len(giOps) == 0 {
		return nil
	}

	if s.manager == nil {
		started := time.Now()
		if err := s.db.BatchWriteItem(giTD, giOps); err != nil {
			return status.Errorf(codes.Internal, "gi %s: %v", gi.Name, err)
		}
		s.giObserveDuration(gi.Name, "batch", started)
		return nil
	}

	router := s.manager.Router()
	buckets := make(map[uint32][]pebble.BatchOp, 16)
	for _, op := range giOps {
		probe := op.Item
		if op.Op == pebble.BatchOpDelete {
			probe = op.Key
		}
		pkBytes, err := pkBytesFromItem(probe, giTD.KeySchema)
		if err != nil {
			return status.Errorf(codes.Internal, "gi %s pk: %v", gi.Name, err)
		}
		shardID, err := router.ShardForPK(pkBytes)
		if err != nil {
			return status.Errorf(codes.Internal, "gi %s shard: %v", gi.Name, err)
		}
		buckets[shardID] = append(buckets[shardID], op)
	}

	started := time.Now()
	if err := s.dispatchGIBuckets(ctx, gi, giTD, buckets); err != nil {
		return status.Errorf(codes.Internal, "gi %s: %v", gi.Name, err)
	}
	s.giObserveDuration(gi.Name, "batch", started)
	return nil
}

func (s *GRPCServer) dispatchGIBuckets(ctx context.Context, gi types.GlobalIndexDescriptor, giTD types.TableDescriptor, buckets map[uint32][]pebble.BatchOp) error {
	switch len(buckets) {
	case 0:
		return nil
	case 1:
		for shardID, ops := range buckets {
			return s.dispatchGIBucket(ctx, gi, giTD, shardID, ops)
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(buckets))
	for shardID, ops := range buckets {
		shardID, ops := shardID, ops
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.dispatchGIBucket(ctx, gi, giTD, shardID, ops); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// dispatchGIBucket commits one bucket of GI pointer ops to its
// owning shard. Same RF=1 contract as the MV cascade: local-leader
// writes go straight to pebble (BatchWriteItemLocal), remote-leader
// buckets are forwarded via Replica.BatchWriteGI.
func (s *GRPCServer) dispatchGIBucket(ctx context.Context, gi types.GlobalIndexDescriptor, giTD types.TableDescriptor, shardID uint32, ops []pebble.BatchOp) error {
	peerID, addr, isSelf, err := s.manager.LeaderEndpoint(shardID)
	if err != nil {
		return err
	}
	if isSelf {
		sh, ok := s.manager.Shard(shardID)
		if !ok || sh == nil || sh.Storage == nil {
			return status.Errorf(codes.Internal, "gi %s shard %d not local", gi.Name, shardID)
		}
		return sh.Storage.BatchWriteItemLocal(giTD, ops)
	}
	req := giBatchOpsToPB(gi.Name, ops)
	return s.manager.BatchWriteGIToPeer(ctx, peerID, addr, req)
}

func giBatchOpsToPB(index string, ops []pebble.BatchOp) *cefaspb.BatchWriteGIRequest {
	out := &cefaspb.BatchWriteGIRequest{
		Index: index,
		Ops:   make([]*cefaspb.BatchWriteOp, 0, len(ops)),
	}
	for _, op := range ops {
		switch op.Op {
		case pebble.BatchOpPut:
			out.Ops = append(out.Ops, &cefaspb.BatchWriteOp{
				Kind: cefaspb.BatchWriteOp_KIND_PUT,
				Item: itemToPB(op.Item),
			})
		case pebble.BatchOpDelete:
			out.Ops = append(out.Ops, &cefaspb.BatchWriteOp{
				Kind: cefaspb.BatchWriteOp_KIND_DELETE,
				Key:  itemToPB(op.Key),
			})
		}
	}
	return out
}

// writeGIRow dispatches a single put to the index's owning shard,
// reusing the per-bucket pipeline.
func (s *GRPCServer) writeGIRow(ctx context.Context, gi types.GlobalIndexDescriptor, basePKCol string, giItem types.Item) error {
	giTD := giSyntheticTableDescriptor(gi, basePKCol)
	if s.manager == nil {
		return s.db.PutItemWith(giTD, giItem, pebble.PutOptions{})
	}
	pkBytes, err := pkBytesFromItem(giItem, giTD.KeySchema)
	if err != nil {
		return err
	}
	shardID, err := s.manager.Router().ShardForPK(pkBytes)
	if err != nil {
		return err
	}
	return s.dispatchGIBucket(ctx, gi, giTD, shardID, []pebble.BatchOp{{Op: pebble.BatchOpPut, Item: giItem}})
}

func (s *GRPCServer) deleteGIRow(ctx context.Context, gi types.GlobalIndexDescriptor, basePKCol string, giKey types.Item) error {
	giTD := giSyntheticTableDescriptor(gi, basePKCol)
	if s.manager == nil {
		return s.db.DeleteItemWith(giTD, giKey, pebble.DeleteOptions{})
	}
	pkBytes, err := pkBytesFromItem(giKey, giTD.KeySchema)
	if err != nil {
		return err
	}
	shardID, err := s.manager.Router().ShardForPK(pkBytes)
	if err != nil {
		return err
	}
	return s.dispatchGIBucket(ctx, gi, giTD, shardID, []pebble.BatchOp{{Op: pebble.BatchOpDelete, Key: giKey}})
}

// deriveGIItem builds the pointer-row item from a base mutation.
// Includes the indexed column value, the base PK (so reads can
// hydrate the full base on demand), and any projected columns.
// Returns nil when the base item lacks the indexed column or the
// base PK — the cascade then no-ops for that op.
func deriveGIItem(gi types.GlobalIndexDescriptor, baseKS types.KeySchema, base types.Item) types.Item {
	if base == nil {
		return nil
	}
	idxVal, ok := base[gi.IndexedColumn]
	if !ok {
		return nil
	}
	baseKey, hasBaseKey := base[baseKS.PK]
	if !hasBaseKey {
		return nil
	}
	out := types.Item{
		gi.IndexedColumn: idxVal,
		baseKS.PK:        baseKey,
	}
	for _, a := range gi.ProjectedColumns {
		if a == gi.IndexedColumn || a == baseKS.PK {
			continue
		}
		if v, ok := base[a]; ok {
			out[a] = v
		}
	}
	return out
}

func giKeyOnly(item types.Item, indexedCol, baseKeyCol string) types.Item {
	out := make(types.Item, 2)
	if v, ok := item[indexedCol]; ok {
		out[indexedCol] = v
	}
	if v, ok := item[baseKeyCol]; ok {
		out[baseKeyCol] = v
	}
	return out
}

func giSyntheticTableDescriptor(gi types.GlobalIndexDescriptor, basePKCol string) types.TableDescriptor {
	sk := basePKCol
	if sk == "" {
		sk = "_base_pk"
	}
	return types.TableDescriptor{
		Name:      gi.Name,
		KeySchema: types.KeySchema{PK: gi.IndexedColumn, SK: sk},
	}
}

func (s *GRPCServer) giObserveDuration(idx, op string, started time.Time) {
	if s.metrics == nil {
		return
	}
	s.metrics.Observe("gi_"+op, idx, "ok", time.Since(started).Seconds())
}
