package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/tracing"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// refreshSingleFlight guards a single in-flight RefreshComplete per
// view. Concurrent attempts coalesce on the same channel; a long
// refresh in progress short-circuits subsequent triggers without
// returning an error so the scheduler can keep its cadence.
var refreshSingleFlight = struct {
	mu      sync.Mutex
	pending map[string]chan struct{}
}{
	pending: map[string]chan struct{}{},
}

// RefreshMaterializedView is the operator-facing RPC: invoke a
// REFRESH COMPLETE for a named view regardless of its policy. Used
// by ON_DEMAND views, by ad-hoc operator triggers (CLI in PR C), and
// by the scheduler (MV-7) for SCHEDULED views.
func (s *GRPCServer) RefreshMaterializedView(ctx context.Context, req *cefaspb.RefreshMaterializedViewRequest) (*cefaspb.RefreshMaterializedViewResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "RefreshMaterializedView")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	rows, err := s.refreshComplete(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	return &cefaspb.RefreshMaterializedViewResponse{RowsIndexed: rows}, nil
}

// refreshComplete is the in-process driver: re-derives every base row
// into the MV and overwrites the MV's rows. Single-flight per view
// name. Updates LastRefreshAtUnix + Status on success; flags Failed
// on error so the next tick or RPC retries.
func (s *GRPCServer) refreshComplete(ctx context.Context, viewName string) (int64, error) {
	mv, err := s.cat.DescribeView(viewName)
	if err != nil {
		return 0, mapStorageErr(err)
	}

	// Single-flight: if another goroutine is refreshing the same
	// view, wait on its result rather than starting a parallel
	// rebuild. Reduces wasted IO and avoids overwriting in
	// inconsistent order.
	refreshSingleFlight.mu.Lock()
	if existing, busy := refreshSingleFlight.pending[viewName]; busy {
		refreshSingleFlight.mu.Unlock()
		select {
		case <-existing:
			return 0, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	done := make(chan struct{})
	refreshSingleFlight.pending[viewName] = done
	refreshSingleFlight.mu.Unlock()
	defer func() {
		refreshSingleFlight.mu.Lock()
		delete(refreshSingleFlight.pending, viewName)
		refreshSingleFlight.mu.Unlock()
		close(done)
	}()

	return s.runCompleteRefresh(ctx, mv)
}

// runCompleteRefresh drives a COMPLETE rescan + status transitions
// without re-entering refreshSingleFlight. Callers that already
// hold the single-flight (e.g. refreshFast's stale-cursor
// fallback in #541) invoke this directly to avoid self-deadlock.
func (s *GRPCServer) runCompleteRefresh(ctx context.Context, mv types.MaterializedViewDescriptor) (int64, error) {
	// Update status → building so observability sees the rebuild in
	// progress. Best-effort: if the catalog update fails the refresh
	// still runs and lifts status later.
	building := mv
	building.Status = types.MVStatusBuilding
	_ = s.cat.UpdateView(building)

	rows, err := s.driveRefresh(ctx, mv)
	if err != nil {
		failed := mv
		failed.Status = types.MVStatusFailed
		_ = s.cat.UpdateView(failed)
		return rows, err
	}

	mv.Status = types.MVStatusActive
	mv.LastRefreshAtUnix = time.Now().Unix()
	if err := s.cat.UpdateView(mv); err != nil {
		return rows, mapStorageErr(err)
	}
	return rows, nil
}

// driveRefresh scans the base table and writes each derived row into
// the MV. Reuses the eager hook's deriveMVItem + writeMVRow so the
// refresh path produces the same MV rows as a write-time application.
//
// Single-node / no-manager mode: scan s.db directly.
// Multi-shard mode: iterate every shard the node hosts locally and
// scan each one. Cross-shard fan-in via PeerScanShard is out of
// scope for v1 — operators run refresh on a node that hosts the
// base's shards or accept partial rebuilds in RF<N clusters.
func (s *GRPCServer) driveRefresh(ctx context.Context, mv types.MaterializedViewDescriptor) (int64, error) {
	td, err := s.cat.Describe(mv.BaseTable)
	if err != nil {
		return 0, mapStorageErr(err)
	}
	_ = td // base TableDescriptor not currently needed beyond presence check

	var rows int64
	scan := func(items []types.Item) error {
		for _, base := range items {
			if err := ctx.Err(); err != nil {
				return err
			}
			derived := deriveMVItem(mv, base)
			if derived == nil {
				continue
			}
			if err := s.writeMVRow(ctx, mv, derived); err != nil {
				return fmt.Errorf("mv %s write: %w", mv.Name, err)
			}
			rows++
		}
		return nil
	}

	if s.manager == nil {
		items, err := s.db.ScanTable(mv.BaseTable, 0)
		if err != nil {
			return 0, mapStorageErr(err)
		}
		if err := scan(items); err != nil {
			return rows, err
		}
		return rows, nil
	}

	for _, sh := range s.manager.Shards() {
		if sh == nil || sh.Storage == nil {
			continue
		}
		items, err := sh.Storage.ScanTable(mv.BaseTable, 0)
		if err != nil {
			return rows, mapStorageErr(err)
		}
		if err := scan(items); err != nil {
			return rows, err
		}
	}
	return rows, nil
}
