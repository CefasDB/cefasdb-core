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
