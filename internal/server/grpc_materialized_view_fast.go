package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// fastRefreshBatchLimit caps the number of change records the FAST
// consumer drains per tick. Bounded so a backlog cannot block the
// scheduler goroutine for arbitrary time.
const fastRefreshBatchLimit = 4096

// refreshFast applies the delta in the base table's changelog since
// the last cursor into the MV. Complexity: O(B·V) per call where B
// is the number of change records since the cursor and V is the
// projected attribute set; strictly cheaper than COMPLETE refresh
// (which is O(|base|)) for any workload that mutates < |base| rows
// per interval.
//
// Algorithm (see issue #541):
//
//  1. Load cursor for mv.
//  2. Read change records on the base table after cursor.
//  3. For each record, derive priorMV + newMV via deriveMVItem.
//  4. Group by op (insert / delete / upsert).
//  5. Dispatch via dispatchMVBucket (reuses #535 / #537 plumbing).
//  6. Persist cursor on success.
//
// Single-flight per view via refreshSingleFlight, so a long tick
// blocks subsequent ticks for the same view without serializing
// unrelated views.
func (s *GRPCServer) refreshFast(ctx context.Context, viewName string) (int64, error) {
	mv, err := s.cat.DescribeView(viewName)
	if err != nil {
		return 0, mapStorageErr(err)
	}
	if mv.RefreshPolicy.Mode != types.RefreshModeFast {
		return 0, status.Errorf(codes.FailedPrecondition, "view %s not in FAST mode", viewName)
	}

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

	cursor, err := s.readMVCursor(viewName)
	if err != nil {
		return 0, err
	}

	catDB := s.catalogDB()
	if catDB == nil {
		return 0, status.Error(codes.FailedPrecondition, "catalog storage unavailable")
	}

	records, err := catDB.ChangeRecordsAfter(mv.BaseTable, cursor, 0, 0)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "read changelog: %v", err)
	}
	if len(records) == 0 {
		return 0, s.touchMVLastRefresh(mv)
	}
	if len(records) > fastRefreshBatchLimit {
		records = records[:fastRefreshBatchLimit]
	}

	ops, err := buildFastMVOps(mv, records)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "build fast ops: %v", err)
	}
	if len(ops) > 0 {
		if err := s.applyMVEagerBatchOneView(ctx, mv, ops); err != nil {
			return 0, status.Errorf(codes.Internal, "apply fast ops: %v", err)
		}
	}

	lastIndex := records[len(records)-1].Index
	if err := s.writeMVCursor(viewName, lastIndex); err != nil {
		return int64(len(records)), status.Errorf(codes.Internal, "persist cursor: %v", err)
	}
	if err := s.touchMVLastRefresh(mv); err != nil {
		return int64(len(records)), err
	}
	return int64(len(records)), nil
}

// buildFastMVOps translates change records to base-table BatchOps
// that applyMVEagerBatchOneView can process. Put + delete derive the
// MV item via the same path as the EAGER hook, so semantics match
// across modes.
//
// Pre-image (when present) is used to delete the previous MV row on
// PK shifts. Streams disabled on the base limits the consumer to
// upsert semantics — fine for our PK-swap grammar where the base
// key carries the MV PK directly.
func buildFastMVOps(mv types.MaterializedViewDescriptor, records []pebble.ChangeRecord) ([]pebble.BatchOp, error) {
	ops := make([]pebble.BatchOp, 0, len(records))
	for _, rec := range records {
		switch rec.Op {
		case pebble.ChangePut:
			item := rec.Item
			if rec.NewItem != nil {
				item = rec.NewItem
			}
			if item == nil {
				continue
			}
			// PK shift: emit a delete for the prior MV row when the
			// old image's derived key differs from the new.
			if rec.OldItem != nil {
				priorMV := deriveMVItem(mv, rec.OldItem)
				newMV := deriveMVItem(mv, item)
				if priorMV != nil && newMV != nil && !mvKeysEqual(priorMV, newMV, mv.KeySchema) {
					ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpDelete, Key: itemKeyOnly(priorMV, mv.KeySchema)})
				}
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpPut, Item: item})
		case pebble.ChangeDelete:
			key := rec.Key
			if rec.OldItem != nil {
				key = rec.OldItem
			}
			if key == nil {
				continue
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpDelete, Key: key})
		}
	}
	return ops, nil
}

func mvKeysEqual(a, b types.Item, ks types.KeySchema) bool {
	if a == nil || b == nil {
		return false
	}
	if pkA, pkB := a[ks.PK], b[ks.PK]; !attrEqual(pkA, pkB) {
		return false
	}
	if ks.SK == "" {
		return true
	}
	return attrEqual(a[ks.SK], b[ks.SK])
}

func attrEqual(a, b types.AttributeValue) bool {
	if a.T != b.T {
		return false
	}
	if a.S != b.S || a.N != b.N {
		return false
	}
	if len(a.B) != len(b.B) {
		return false
	}
	for i := range a.B {
		if a.B[i] != b.B[i] {
			return false
		}
	}
	return true
}

func (s *GRPCServer) readMVCursor(name string) (uint64, error) {
	db := s.catalogDB()
	if db == nil {
		return 0, nil
	}
	raw, err := db.Get(storage.KeyMVRefreshCursor(name))
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read mv cursor: %w", err)
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("mv cursor: invalid length %d", len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

func (s *GRPCServer) writeMVCursor(name string, index uint64) error {
	db := s.catalogDB()
	if db == nil {
		return fmt.Errorf("mv cursor: catalog storage unavailable")
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], index)
	return db.Set(storage.KeyMVRefreshCursor(name), buf[:])
}

// catalogDB returns the catalog's backing pebble store. In multi-
// shard mode this is shard 0's storage; in single-node mode it is
// s.db. The cursor and changelog reads both target this store.
func (s *GRPCServer) catalogDB() *pebble.DB {
	if s.manager != nil {
		if sh, ok := s.manager.Shard(0); ok && sh != nil && sh.Storage != nil {
			return sh.Storage
		}
	}
	return s.db
}

func (s *GRPCServer) touchMVLastRefresh(mv types.MaterializedViewDescriptor) error {
	mv.Status = types.MVStatusActive
	mv.LastRefreshAtUnix = time.Now().Unix()
	if err := s.cat.UpdateView(mv); err != nil {
		return mapStorageErr(err)
	}
	return nil
}
