package server

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/tracing"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// rebuildGISingleFlight guards a concurrent RebuildGlobalIndex per
// name so a second invocation joins the in-flight rebuild instead
// of duplicating the scan.
var rebuildGISingleFlight = struct {
	mu      sync.Mutex
	pending map[string]chan struct{}
}{pending: make(map[string]chan struct{})}

// RebuildGlobalIndex scans the base table, derives a pointer row
// from every base item, and writes it to the index's owning shard.
// Pointer writes are upserts so the rebuild is idempotent and
// concurrent-safe with live mutations from the Phase 2 hook
// (last-writer-wins per (indexed value, base PK)).
//
// v1 limitations (locked in ADR 0005 §3):
//   - Single-node scan: walks the local shards via ScanTable. A
//     non-coordinator node only sees the rows whose base shard it
//     hosts; operators run rebuild against a node that hosts the
//     base's shards or run it on every node.
//   - No resume cursor: a crashed rebuild restarts from scratch.
//     Idempotency keeps the data correct; only the cost is wasted.
func (s *GRPCServer) RebuildGlobalIndex(ctx context.Context, req *cefaspb.RebuildGlobalIndexRequest) (*cefaspb.RebuildGlobalIndexResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "RebuildGlobalIndex")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	rows, err := s.rebuildGlobalIndex(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	return &cefaspb.RebuildGlobalIndexResponse{RowsIndexed: rows}, nil
}

func (s *GRPCServer) rebuildGlobalIndex(ctx context.Context, name string) (int64, error) {
	if s.cat == nil {
		return 0, status.Error(codes.FailedPrecondition, "catalog not attached")
	}
	gi, err := s.cat.DescribeGlobalIndex(name)
	if err != nil {
		return 0, mapStorageErr(err)
	}
	base, err := s.cat.Describe(gi.BaseTable)
	if err != nil {
		return 0, mapStorageErr(err)
	}

	rebuildGISingleFlight.mu.Lock()
	if existing, busy := rebuildGISingleFlight.pending[name]; busy {
		rebuildGISingleFlight.mu.Unlock()
		select {
		case <-existing:
			return 0, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	done := make(chan struct{})
	rebuildGISingleFlight.pending[name] = done
	rebuildGISingleFlight.mu.Unlock()
	defer func() {
		rebuildGISingleFlight.mu.Lock()
		delete(rebuildGISingleFlight.pending, name)
		rebuildGISingleFlight.mu.Unlock()
		close(done)
	}()

	// Mark building → active on success / failed on error.
	building := gi
	building.Status = types.GlobalIndexStatusBuilding
	_, _ = s.cat.UpdateGlobalIndex(building)

	rows, err := s.driveGIBackfill(ctx, gi, base)
	if err != nil {
		failed := gi
		failed.Status = types.GlobalIndexStatusFailed
		_, _ = s.cat.UpdateGlobalIndex(failed)
		return rows, err
	}
	gi.Status = types.GlobalIndexStatusActive
	if _, err := s.cat.UpdateGlobalIndex(gi); err != nil {
		return rows, mapStorageErr(err)
	}
	return rows, nil
}

// driveGIBackfill walks the base table's local rows and writes a
// pointer row for each one. In single-node mode it scans s.db
// directly; in multi-shard mode it iterates every locally-hosted
// shard. Rows whose owning shard is on another node are skipped
// here — operators backfill from a node that hosts the shard or
// run rebuild on every node.
func (s *GRPCServer) driveGIBackfill(ctx context.Context, gi types.GlobalIndexDescriptor, base types.TableDescriptor) (int64, error) {
	var rows int64

	scan := func(items []types.Item) error {
		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return err
			}
			derived := deriveGIItem(gi, base.KeySchema, item)
			if derived == nil {
				continue
			}
			if err := s.writeGIRow(ctx, gi, base.KeySchema.PK, derived); err != nil {
				return err
			}
			rows++
		}
		return nil
	}

	if s.manager == nil {
		items, err := s.db.ScanTable(gi.BaseTable, 0)
		if err != nil {
			return rows, mapStorageErr(err)
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
		items, err := sh.Storage.ScanTable(gi.BaseTable, 0)
		if err != nil {
			return rows, mapStorageErr(err)
		}
		if err := scan(items); err != nil {
			return rows, err
		}
	}
	return rows, nil
}
