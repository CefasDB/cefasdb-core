package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type mvBaseMutation struct {
	OldItem   types.Item
	NewItem   types.Item
	DeleteKey types.Item
}

// applyMVEagerPut runs every EAGER materialized view attached to td
// against the just-written base item. SCHEDULED / ON_DEMAND views
// are filtered out cheaply: the loop reads the catalog's view list
// (already cached), checks the policy mode, and skips. They are
// brought up to date by the refresh engine (#493) and scheduler
// (#502).
//
// Each EAGER MV write goes through the normal write path
// (writeTargetsForPK + PutItemWith) so the routing layer, raft
// replication, and the existing per-shard backpressure all kick in.
//
// Failures are propagated to the caller — the contract is
// read-your-write for EAGER, so a partial update would silently
// break the view. The caller's gRPC status surfaces the offending
// view name in the message for operability.
func (s *GRPCServer) applyMVEagerPut(ctx context.Context, td types.TableDescriptor, item types.Item) error {
	return s.applyMVEagerMutation(ctx, td, mvBaseMutation{NewItem: item})
}

func (s *GRPCServer) applyMVEagerMutation(ctx context.Context, td types.TableDescriptor, mut mvBaseMutation) error {
	if len(td.MaterializedViews) == 0 || s.cat == nil {
		return nil
	}
	for _, viewName := range td.MaterializedViews {
		mv, err := s.cat.DescribeView(viewName)
		if err != nil {
			return status.Errorf(codes.Internal, "mv lookup %s: %v", viewName, err)
		}
		if mv.Status == types.MVStatusPaused {
			s.mvObserveDuration(mv.Name, "skip_paused", time.Now())
			continue
		}
		if mv.RefreshPolicy.Mode != types.RefreshModeEager {
			s.mvObserveDuration(mv.Name, "skip_non_eager", time.Now())
			continue
		}
		started := time.Now()
		if mvHasAggregations(mv) {
			if err := s.applyMVAggregateMutations(ctx, mv, []mvBaseMutation{mut}); err != nil {
				return status.Errorf(codes.Internal, "mv %s aggregate: %v", mv.Name, err)
			}
			s.mvObserveDuration(mv.Name, "aggregate", started)
			continue
		}
		if mut.NewItem != nil {
			mvItem := deriveMVItem(mv, mut.NewItem)
			if mvItem == nil {
				// Base row missing the MV PK / SK — cannot place the
				// derived row deterministically. Drop with a metric so
				// operators can flag schema drift.
				s.mvObserveDuration(mv.Name, "skip_missing_key", time.Now())
				continue
			}
			if err := s.writeMVRow(ctx, mv, mvItem); err != nil {
				return status.Errorf(codes.Internal, "mv %s write: %v", mv.Name, err)
			}
			s.mvObserveDuration(mv.Name, "put", started)
			continue
		}
		base := mut.OldItem
		if base == nil {
			base = mut.DeleteKey
		}
		mvItem := deriveMVItem(mv, base)
		if mvItem == nil {
			continue
		}
		mvKey := itemKeyOnly(mvItem, mv.KeySchema)
		if err := s.deleteMVRow(ctx, mv, mvKey); err != nil {
			return status.Errorf(codes.Internal, "mv %s delete: %v", mv.Name, err)
		}
		s.mvObserveDuration(mv.Name, "delete", started)
	}
	return nil
}

// applyMVEagerBatch propagates a BatchWriteItem's worth of puts +
// deletes to every attached EAGER materialized view. Each MV's ops
// are bucketed by the MV's owning shard and submitted in a single
// batchWriteBuckets call — one raft round-trip per (MV, shard)
// bucket, not per op.
//
// The earlier shape called applyMVEagerPut / applyMVEagerDelete per
// op, which produced K = batch-size raft round-trips per worker and
// collapsed the 8-node cluster under realistic load (issue #531).
// EAGER consistency is preserved: the call blocks until every
// bucket commits.
func (s *GRPCServer) applyMVEagerBatch(ctx context.Context, td types.TableDescriptor, ops []pebble.BatchOp, muts []mvBaseMutation) error {
	if len(td.MaterializedViews) == 0 || s.cat == nil {
		return nil
	}
	for _, viewName := range td.MaterializedViews {
		mv, err := s.cat.DescribeView(viewName)
		if err != nil {
			return status.Errorf(codes.Internal, "mv lookup %s: %v", viewName, err)
		}
		if mv.Status == types.MVStatusPaused {
			s.mvObserveDuration(mv.Name, "skip_paused", time.Now())
			continue
		}
		if mv.RefreshPolicy.Mode != types.RefreshModeEager {
			s.mvObserveDuration(mv.Name, "skip_non_eager", time.Now())
			continue
		}
		if mvHasAggregations(mv) {
			started := time.Now()
			if err := s.applyMVAggregateMutations(ctx, mv, muts); err != nil {
				return status.Errorf(codes.Internal, "mv %s aggregate: %v", mv.Name, err)
			}
			s.mvObserveDuration(mv.Name, "aggregate_batch", started)
			continue
		}
		if err := s.applyMVEagerBatchOneView(ctx, mv, ops); err != nil {
			return err
		}
	}
	return nil
}

// applyMVEagerBatchOneView fans out a base-table batch into a single MV.
//
// Layout:
//
//  1. Derive every MV row up front; ops missing the MV PK / SK are
//     dropped with a metric (schema drift).
//  2. Group MV ops by the shard that currently owns the MV PK. A
//     single MV write may land on a different shard than the base
//     table — when MV PK ≠ base PK the routing diverges.
//  3. For each (MV, shard) bucket: dispatch in parallel. Local-leader
//     buckets write directly to the shard's pebble.DB; remote-leader
//     buckets are forwarded to the peer via cluster.BatchWriteItemToPeer.
//     EAGER read-your-write is preserved: the call blocks until every
//     bucket has committed (locally or remotely).
func (s *GRPCServer) applyMVEagerBatchOneView(ctx context.Context, mv types.MaterializedViewDescriptor, ops []pebble.BatchOp) error {
	mvTD := mvSyntheticTableDescriptor(mv)
	mvOps := make([]pebble.BatchOp, 0, len(ops))
	for _, op := range ops {
		switch op.Op {
		case pebble.BatchOpPut:
			mvItem := deriveMVItem(mv, op.Item)
			if mvItem == nil {
				s.mvObserveDuration(mv.Name, "skip_missing_key", time.Now())
				continue
			}
			mvOps = append(mvOps, pebble.BatchOp{Op: pebble.BatchOpPut, Item: mvItem})
		case pebble.BatchOpDelete:
			mvItem := deriveMVItem(mv, op.Key)
			if mvItem == nil {
				continue
			}
			mvOps = append(mvOps, pebble.BatchOp{Op: pebble.BatchOpDelete, Key: itemKeyOnly(mvItem, mv.KeySchema)})
		}
	}
	if len(mvOps) == 0 {
		return nil
	}

	if s.manager == nil {
		// Single-node fallback: no manager, only one pebble.DB. Write
		// every MV op against s.db with the synthetic descriptor — same
		// behaviour the pre-cross-shard cascade gave for unit fixtures.
		started := time.Now()
		if err := s.db.BatchWriteItem(mvTD, mvOps); err != nil {
			return status.Errorf(codes.Internal, "mv %s: %v", mv.Name, err)
		}
		s.mvObserveDuration(mv.Name, "batch", started)
		return nil
	}

	router := s.manager.Router()
	buckets := make(map[uint32][]pebble.BatchOp, 16)
	for _, op := range mvOps {
		probe := op.Item
		if op.Op == pebble.BatchOpDelete {
			probe = op.Key
		}
		pkBytes, err := pkBytesFromItem(probe, mv.KeySchema)
		if err != nil {
			return status.Errorf(codes.Internal, "mv %s pk: %v", mv.Name, err)
		}
		shardID, err := router.ShardForPK(pkBytes)
		if err != nil {
			return status.Errorf(codes.Internal, "mv %s shard: %v", mv.Name, err)
		}
		buckets[shardID] = append(buckets[shardID], op)
	}

	started := time.Now()
	if err := s.dispatchMVBuckets(ctx, mvTD, buckets); err != nil {
		return status.Errorf(codes.Internal, "mv %s: %v", mv.Name, err)
	}
	s.mvObserveDuration(mv.Name, "batch", started)
	return nil
}

// dispatchMVBuckets runs every (shardID → ops) bucket in parallel,
// returning the first error after all goroutines drain. Each bucket
// routes through dispatchMVBucket which decides local vs remote.
func (s *GRPCServer) dispatchMVBuckets(ctx context.Context, mvTD types.TableDescriptor, buckets map[uint32][]pebble.BatchOp) error {
	switch len(buckets) {
	case 0:
		return nil
	case 1:
		for shardID, ops := range buckets {
			return s.dispatchMVBucket(ctx, mvTD, shardID, ops)
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(buckets))
	for shardID, ops := range buckets {
		shardID, ops := shardID, ops
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.dispatchMVBucket(ctx, mvTD, shardID, ops); err != nil {
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

// dispatchMVBucket commits one bucket of MV ops to its owning shard.
// MVs are RF=1: only the owning leader carries authoritative state.
// If the local node is the current leader the bucket is written
// directly to local pebble via BatchWriteItemLocal (no raft).
// Otherwise the bucket is forwarded to the leader peer through
// Replica.BatchWriteMV, which also writes locally without raft.
//
// MV synthetic descriptors carry no attached MVs of their own, so
// no recursive cascade fires on either side.
func (s *GRPCServer) dispatchMVBucket(ctx context.Context, mvTD types.TableDescriptor, shardID uint32, ops []pebble.BatchOp) error {
	peerID, addr, isSelf, err := s.manager.LeaderEndpoint(shardID)
	if err != nil {
		return err
	}
	if isSelf {
		sh, ok := s.manager.Shard(shardID)
		if !ok || sh == nil || sh.Storage == nil {
			return status.Errorf(codes.Internal, "mv %s shard %d not local", mvTD.Name, shardID)
		}
		return sh.Storage.BatchWriteItemLocal(mvTD, ops)
	}
	req := mvBatchOpsToMVPB(mvTD.Name, ops)
	return s.manager.BatchWriteMVToPeer(ctx, peerID, addr, req)
}

// mvBatchOpsToMVPB converts MV-side BatchOps to the protobuf
// BatchWriteMV request format used for peer dispatch.
func mvBatchOpsToMVPB(view string, ops []pebble.BatchOp) *cefaspb.BatchWriteMVRequest {
	out := &cefaspb.BatchWriteMVRequest{
		View: view,
		Ops:  make([]*cefaspb.BatchWriteOp, 0, len(ops)),
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

// applyMVEagerDelete cascades a base delete to every attached EAGER
// MV. Computes the MV key from the same base item the delete request
// carried (the catalog's KeySchema for the base table guarantees the
// fields the MV will need are present in the request).
func (s *GRPCServer) applyMVEagerDelete(ctx context.Context, td types.TableDescriptor, baseKey types.Item) error {
	return s.applyMVEagerMutation(ctx, td, mvBaseMutation{DeleteKey: baseKey})
}

func mvHasAggregations(mv types.MaterializedViewDescriptor) bool {
	return len(mv.Aggregations) > 0
}

func (s *GRPCServer) tableHasAggregatingEagerMV(td types.TableDescriptor) (bool, error) {
	if len(td.MaterializedViews) == 0 || s.cat == nil {
		return false, nil
	}
	for _, viewName := range td.MaterializedViews {
		mv, err := s.cat.DescribeView(viewName)
		if err != nil {
			return false, status.Errorf(codes.Internal, "mv lookup %s: %v", viewName, err)
		}
		if mv.RefreshPolicy.Mode == types.RefreshModeEager && mv.Status != types.MVStatusPaused && mvHasAggregations(mv) {
			return true, nil
		}
	}
	return false, nil
}

func captureMVEagerMutation(db *pebble.DB, td types.TableDescriptor, newItem, deleteKey types.Item) (mvBaseMutation, error) {
	mut := mvBaseMutation{NewItem: newItem, DeleteKey: deleteKey}
	if db == nil {
		return mut, nil
	}
	probe := newItem
	if probe == nil {
		probe = deleteKey
	}
	if probe == nil {
		return mut, nil
	}
	oldItem, err := db.GetItem(td.Name, td.KeySchema, itemKeyOnly(probe, td.KeySchema))
	if errors.Is(err, types.ErrItemNotFound) {
		return mut, nil
	}
	if err != nil {
		return mut, err
	}
	mut.OldItem = oldItem
	return mut, nil
}

func captureMVEagerBatchMutations(dbForOp func(int) *pebble.DB, td types.TableDescriptor, ops []pebble.BatchOp) ([]mvBaseMutation, error) {
	muts := make([]mvBaseMutation, 0, len(ops))
	for i, op := range ops {
		db := dbForOp(i)
		switch op.Op {
		case pebble.BatchOpPut:
			mut, err := captureMVEagerMutation(db, td, op.Item, nil)
			if err != nil {
				return nil, fmt.Errorf("op %d prior: %w", i, err)
			}
			muts = append(muts, mut)
		case pebble.BatchOpDelete:
			mut, err := captureMVEagerMutation(db, td, nil, op.Key)
			if err != nil {
				return nil, fmt.Errorf("op %d prior: %w", i, err)
			}
			muts = append(muts, mut)
		}
	}
	return muts, nil
}

type mvAggregateDelta struct {
	key    types.Item
	values map[string]float64
}

func (s *GRPCServer) applyMVAggregateMutations(ctx context.Context, mv types.MaterializedViewDescriptor, muts []mvBaseMutation) error {
	if len(muts) == 0 {
		return nil
	}
	deltas := make(map[string]*mvAggregateDelta)
	for _, mut := range muts {
		if mut.OldItem != nil {
			if err := addMVAggregateContribution(mv, deltas, mut.OldItem, -1); err != nil {
				return err
			}
		}
		if mut.NewItem != nil {
			if err := addMVAggregateContribution(mv, deltas, mut.NewItem, 1); err != nil {
				return err
			}
		}
	}
	for _, delta := range deltas {
		actions := make([]pebble.AtomicAction, 0, len(delta.values))
		for attr, value := range delta.values {
			if value == 0 {
				continue
			}
			actions = append(actions, pebble.AtomicAction{
				Kind:      pebble.AtomicActionAddReturn,
				Attribute: attr,
				Value:     types.AttributeValue{T: types.AttrN, N: formatMVNumber(value)},
			})
		}
		if len(actions) == 0 {
			continue
		}
		if err := s.dispatchMVAtomicUpdate(ctx, mv, delta.key, actions); err != nil {
			return err
		}
	}
	return nil
}

func addMVAggregateContribution(mv types.MaterializedViewDescriptor, deltas map[string]*mvAggregateDelta, base types.Item, sign float64) error {
	key := deriveMVGroupKey(mv, base)
	if key == nil {
		return nil
	}
	keyID, err := mvAggregateKeyID(mv, key)
	if err != nil {
		return err
	}
	delta := deltas[keyID]
	if delta == nil {
		delta = &mvAggregateDelta{key: key, values: make(map[string]float64, len(mv.Aggregations))}
		deltas[keyID] = delta
	}
	for _, agg := range mv.Aggregations {
		switch agg.Function {
		case types.MVAggregationCount:
			delta.values[agg.TargetAttribute] += sign
		case types.MVAggregationSum:
			av, ok := base[agg.SourceAttribute]
			if !ok || av.T == types.AttrNull {
				continue
			}
			if av.T != types.AttrN {
				return fmt.Errorf("SUM(%s) source is %v, want N", agg.SourceAttribute, av.T)
			}
			n, err := strconv.ParseFloat(av.N, 64)
			if err != nil {
				return fmt.Errorf("SUM(%s) parse %q: %w", agg.SourceAttribute, av.N, err)
			}
			delta.values[agg.TargetAttribute] += sign * n
		}
	}
	return nil
}

func deriveMVGroupKey(mv types.MaterializedViewDescriptor, base types.Item) types.Item {
	if base == nil {
		return nil
	}
	pkVal, ok := base[mv.KeySchema.PK]
	if !ok {
		return nil
	}
	key := types.Item{mv.KeySchema.PK: pkVal}
	if mv.KeySchema.SK != "" {
		skVal, ok := base[mv.KeySchema.SK]
		if !ok {
			return nil
		}
		key[mv.KeySchema.SK] = skVal
	}
	return key
}

func mvAggregateKeyID(mv types.MaterializedViewDescriptor, key types.Item) (string, error) {
	pk, err := storage.AttrCanonicalBytes(key[mv.KeySchema.PK])
	if err != nil {
		return "", err
	}
	var sk []byte
	if mv.KeySchema.SK != "" {
		sk, err = storage.AttrCanonicalBytes(key[mv.KeySchema.SK])
		if err != nil {
			return "", err
		}
	}
	return string(storage.KeyPrimary(mv.Name, pk, sk)), nil
}

func formatMVNumber(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func (s *GRPCServer) dispatchMVAtomicUpdate(ctx context.Context, mv types.MaterializedViewDescriptor, key types.Item, actions []pebble.AtomicAction) error {
	td := mvSyntheticTableDescriptor(mv)
	if s.manager == nil {
		_, err := s.db.AtomicUpdate(td, key, pebble.AtomicOptions{Actions: actions})
		return err
	}
	pkBytes, err := pkBytesFromItem(key, td.KeySchema)
	if err != nil {
		return err
	}
	shardID, err := s.manager.Router().ShardForPK(pkBytes)
	if err != nil {
		return err
	}
	peerID, addr, isSelf, err := s.manager.LeaderEndpoint(shardID)
	if err != nil {
		return err
	}
	if isSelf {
		sh, ok := s.manager.Shard(shardID)
		if !ok || sh == nil || sh.Storage == nil {
			return status.Errorf(codes.Internal, "mv %s shard %d not local", mv.Name, shardID)
		}
		_, err := sh.Storage.AtomicUpdate(td, key, pebble.AtomicOptions{Actions: actions})
		return err
	}
	return s.manager.AtomicUpdateMVToPeer(ctx, peerID, addr, &cefaspb.AtomicUpdateMVRequest{
		View:    mv.Name,
		Key:     itemToPB(key),
		Actions: mvAtomicActionsToPB(actions),
	})
}

func mvAtomicActionsToPB(actions []pebble.AtomicAction) []*cefaspb.AtomicAction {
	out := make([]*cefaspb.AtomicAction, 0, len(actions))
	for _, action := range actions {
		out = append(out, &cefaspb.AtomicAction{
			Kind:      mvAtomicKindToPB(action.Kind),
			Attribute: action.Attribute,
			Value:     attrToPB(action.Value),
		})
	}
	return out
}

func mvAtomicKindToPB(kind pebble.AtomicActionKind) cefaspb.AtomicActionKind {
	switch kind {
	case pebble.AtomicActionSet:
		return cefaspb.AtomicActionKind_ATOMIC_SET
	case pebble.AtomicActionIncrReturn:
		return cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN
	case pebble.AtomicActionAddReturn:
		return cefaspb.AtomicActionKind_ATOMIC_ADD_RETURN
	case pebble.AtomicActionApply:
		return cefaspb.AtomicActionKind_ATOMIC_APPLY
	default:
		return cefaspb.AtomicActionKind_ATOMIC_ACTION_UNSPECIFIED
	}
}

// deriveMVItem projects the base item into the view's row. The view
// row always carries the MV's PK + SK; ProjectedAttributes adds the
// remaining columns. Empty ProjectedAttributes means "copy every
// attribute the base item has". Returns nil if the base item lacks
// the MV's PK or SK.
func deriveMVItem(mv types.MaterializedViewDescriptor, base types.Item) types.Item {
	if base == nil {
		return nil
	}
	pkVal, ok := base[mv.KeySchema.PK]
	if !ok {
		return nil
	}
	out := types.Item{mv.KeySchema.PK: pkVal}
	if mv.KeySchema.SK != "" {
		skVal, ok := base[mv.KeySchema.SK]
		if !ok {
			return nil
		}
		out[mv.KeySchema.SK] = skVal
	}
	if len(mv.ProjectedAttributes) == 0 {
		for k, v := range base {
			if _, already := out[k]; already {
				continue
			}
			out[k] = v
		}
		return out
	}
	for _, a := range mv.ProjectedAttributes {
		if a == mv.KeySchema.PK || a == mv.KeySchema.SK {
			continue
		}
		if v, ok := base[a]; ok {
			out[a] = v
		}
	}
	return out
}

func mvSyntheticTableDescriptor(mv types.MaterializedViewDescriptor) types.TableDescriptor {
	td := types.TableDescriptor{
		Name:      mv.Name,
		KeySchema: mv.KeySchema,
	}
	if len(mv.Aggregations) > 0 {
		td.AttributeDefinitions = make([]types.AttributeDefinition, 0, len(mv.Aggregations))
		for _, agg := range mv.Aggregations {
			td.AttributeDefinitions = append(td.AttributeDefinitions, types.AttributeDefinition{
				Name: agg.TargetAttribute,
				Type: types.AttributeTypeCounter,
			})
		}
	}
	return td
}

func (s *GRPCServer) writeMVRow(ctx context.Context, mv types.MaterializedViewDescriptor, mvItem types.Item) error {
	td := mvSyntheticTableDescriptor(mv)
	if s.manager == nil {
		return s.db.PutItemWith(td, mvItem, pebble.PutOptions{})
	}
	pkBytes, err := pkBytesFromItem(mvItem, td.KeySchema)
	if err != nil {
		return err
	}
	shardID, err := s.manager.Router().ShardForPK(pkBytes)
	if err != nil {
		return err
	}
	return s.dispatchMVBucket(ctx, td, shardID, []pebble.BatchOp{{Op: pebble.BatchOpPut, Item: mvItem}})
}

func (s *GRPCServer) deleteMVRow(ctx context.Context, mv types.MaterializedViewDescriptor, mvKey types.Item) error {
	td := mvSyntheticTableDescriptor(mv)
	if s.manager == nil {
		return s.db.DeleteItemWith(td, mvKey, pebble.DeleteOptions{})
	}
	pkBytes, err := pkBytesFromItem(mvKey, td.KeySchema)
	if err != nil {
		return err
	}
	shardID, err := s.manager.Router().ShardForPK(pkBytes)
	if err != nil {
		return err
	}
	return s.dispatchMVBucket(ctx, td, shardID, []pebble.BatchOp{{Op: pebble.BatchOpDelete, Key: mvKey}})
}

// mvObserveDuration records a per-view write latency sample if
// metrics are wired. Used by phases that need finer-grained
// observability than the simple counter applied inline.
func (s *GRPCServer) mvObserveDuration(view, op string, started time.Time) {
	if s.metrics == nil {
		return
	}
	s.metrics.Observe("mv_"+op, view, "ok", time.Since(started).Seconds())
}

// maybeSetMVStalenessHeader emits the x-cefas-mv-staleness-seconds
// header on the gRPC response when the requested name resolves to a
// materialized view. Callers receive a numeric value; "-1" means
// "view has not refreshed yet" (LastRefreshAtUnix == 0).
func (s *GRPCServer) maybeSetMVStalenessHeader(ctx mvHeaderCtx, name string) {
	if s.cat == nil {
		return
	}
	mv, err := s.cat.DescribeView(name)
	if err != nil {
		return
	}
	var staleness int64
	if mv.LastRefreshAtUnix == 0 {
		staleness = -1
	} else {
		staleness = time.Now().Unix() - mv.LastRefreshAtUnix
		if staleness < 0 {
			staleness = 0
		}
	}
	ctx.setHeader("x-cefas-mv-staleness-seconds", formatInt(staleness))
}

// mvHeaderCtx abstracts the SetHeader call so unit tests can probe
// the value without standing up a full gRPC server.
type mvHeaderCtx interface {
	setHeader(key, value string)
}

// grpcStreamHeaderCtx adapts a gRPC server stream to the mvHeaderCtx
// interface. SetHeader fires before the first Send; subsequent
// SendHeader calls are no-ops.
type grpcStreamHeaderCtx struct {
	stream interface {
		SetHeader(metadata.MD) error
	}
}

func (g grpcStreamHeaderCtx) setHeader(key, value string) {
	if g.stream == nil {
		return
	}
	_ = g.stream.SetHeader(metadata.Pairs(key, value))
}

func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
