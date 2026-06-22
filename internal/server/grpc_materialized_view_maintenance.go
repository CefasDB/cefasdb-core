package server

import (
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

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
func (s *GRPCServer) applyMVEagerPut(td types.TableDescriptor, item types.Item) error {
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
		mvItem := deriveMVItem(mv, item)
		if mvItem == nil {
			// Base row missing the MV PK / SK — cannot place the
			// derived row deterministically. Drop with a metric so
			// operators can flag schema drift.
			s.mvObserveDuration(mv.Name, "skip_missing_key", time.Now())
			continue
		}
		started := time.Now()
		if err := s.writeMVRow(mv, mvItem); err != nil {
			return status.Errorf(codes.Internal, "mv %s write: %v", mv.Name, err)
		}
		s.mvObserveDuration(mv.Name, "put", started)
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
func (s *GRPCServer) applyMVEagerBatch(td types.TableDescriptor, ops []pebble.BatchOp) error {
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
		if err := s.applyMVEagerBatchOneView(mv, ops); err != nil {
			return err
		}
	}
	return nil
}

// applyMVEagerBatchOneView fans out a base-table batch into a single
// MV. Steps:
//
//  1. Derive every MV row up front; ops that lack the MV PK / SK are
//     dropped with a metric (schema drift).
//  2. Dedupe routing per MV PK so writeTargetsForPK runs once per
//     unique MV PK, not per op.
//  3. Bucket the resulting MV ops by primary shard + mirror shards.
//  4. Submit each bucket with batchWriteBuckets — one raft entry per
//     (MV, shard) bucket.
func (s *GRPCServer) applyMVEagerBatchOneView(mv types.MaterializedViewDescriptor, ops []pebble.BatchOp) error {
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

	// Routing dedup for the MV ops.
	type pending struct {
		op       pebble.BatchOp
		routeIdx int
	}
	pendings := make([]pending, 0, len(mvOps))
	keyToRoute := make(map[string]int, len(mvOps))
	routes := make([]routedWriteTargets, 0, len(mvOps))
	releaseRoutes := func() {
		for i := len(routes) - 1; i >= 0; i-- {
			routes[i].Release()
		}
	}
	for _, op := range mvOps {
		probe := op.Item
		if op.Op == pebble.BatchOpDelete {
			probe = op.Key
		}
		pkBytes, err := pkBytesFromItem(probe, mv.KeySchema)
		if err != nil {
			releaseRoutes()
			return status.Errorf(codes.Internal, "mv %s pk: %v", mv.Name, err)
		}
		key := string(pkBytes)
		idx, ok := keyToRoute[key]
		if !ok {
			targets, err := s.writeTargetsForPK(pkBytes)
			if err != nil {
				releaseRoutes()
				return status.Errorf(codes.Internal, "mv %s route: %v", mv.Name, err)
			}
			idx = len(routes)
			routes = append(routes, targets)
			keyToRoute[key] = idx
		}
		pendings = append(pendings, pending{op: op, routeIdx: idx})
	}

	primaryBuckets := make(map[*pebble.DB][]pebble.BatchOp, len(routes))
	mirrorBuckets := make(map[*pebble.DB][]pebble.BatchOp, len(routes))
	for _, r := range routes {
		if _, ok := primaryBuckets[r.primary]; !ok {
			primaryBuckets[r.primary] = make([]pebble.BatchOp, 0, len(pendings))
		}
		for _, m := range r.mirrors {
			if _, ok := mirrorBuckets[m]; !ok {
				mirrorBuckets[m] = make([]pebble.BatchOp, 0, len(pendings))
			}
		}
	}
	for _, p := range pendings {
		r := routes[p.routeIdx]
		primaryBuckets[r.primary] = append(primaryBuckets[r.primary], p.op)
		for _, m := range r.mirrors {
			mirrorBuckets[m] = append(mirrorBuckets[m], p.op)
		}
	}

	started := time.Now()
	if err := batchWriteBuckets(mvTD, primaryBuckets); err != nil {
		releaseRoutes()
		return status.Errorf(codes.Internal, "mv %s primary: %v", mv.Name, err)
	}
	if err := batchWriteBuckets(mvTD, mirrorBuckets); err != nil {
		releaseRoutes()
		return status.Errorf(codes.Internal, "mv %s mirror: %v", mv.Name, err)
	}
	releaseRoutes()
	s.mvObserveDuration(mv.Name, "batch", started)
	return nil
}

// applyMVEagerDelete cascades a base delete to every attached EAGER
// MV. Computes the MV key from the same base item the delete request
// carried (the catalog's KeySchema for the base table guarantees the
// fields the MV will need are present in the request).
func (s *GRPCServer) applyMVEagerDelete(td types.TableDescriptor, baseKey types.Item) error {
	if len(td.MaterializedViews) == 0 || s.cat == nil {
		return nil
	}
	for _, viewName := range td.MaterializedViews {
		mv, err := s.cat.DescribeView(viewName)
		if err != nil {
			return status.Errorf(codes.Internal, "mv lookup %s: %v", viewName, err)
		}
		if mv.RefreshPolicy.Mode != types.RefreshModeEager {
			continue
		}
		mvItem := deriveMVItem(mv, baseKey)
		if mvItem == nil {
			continue
		}
		mvKey := itemKeyOnly(mvItem, mv.KeySchema)
		started := time.Now()
		if err := s.deleteMVRow(mv, mvKey); err != nil {
			return status.Errorf(codes.Internal, "mv %s delete: %v", mv.Name, err)
		}
		s.mvObserveDuration(mv.Name, "delete", started)
	}
	return nil
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
	return types.TableDescriptor{
		Name:      mv.Name,
		KeySchema: mv.KeySchema,
	}
}

func (s *GRPCServer) writeMVRow(mv types.MaterializedViewDescriptor, mvItem types.Item) error {
	td := mvSyntheticTableDescriptor(mv)
	pkBytes, err := pkBytesFromItem(mvItem, td.KeySchema)
	if err != nil {
		return err
	}
	targets, err := s.writeTargetsForPK(pkBytes)
	if err != nil {
		return err
	}
	defer targets.Release()
	return targets.PutItemWith(td, mvItem, pebble.PutOptions{})
}

func (s *GRPCServer) deleteMVRow(mv types.MaterializedViewDescriptor, mvKey types.Item) error {
	td := mvSyntheticTableDescriptor(mv)
	pkBytes, err := pkBytesFromItem(mvKey, td.KeySchema)
	if err != nil {
		return err
	}
	targets, err := s.writeTargetsForPK(pkBytes)
	if err != nil {
		return err
	}
	defer targets.Release()
	return targets.DeleteItemWith(td, mvKey, pebble.DeleteOptions{})
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
