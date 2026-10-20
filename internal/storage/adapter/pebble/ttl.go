package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// planTTL emits the TTL pointer mutations for a single primary
// write. The pointer key sorts entries by expire time so the reaper
// can range-scan the soonest-to-expire entries cheaply. When the TTL
// attribute is missing or zero, no pointer is written — same sparse
// semantics every other index follows.
func planTTL(
	table string,
	ks types.KeySchema,
	ttlAttr string,
	prior, next types.Item,
) ([]storage.IndexOp, error) {
	if ttlAttr == "" {
		return nil, nil
	}
	priorKey, err := ttlKey(table, ks, ttlAttr, prior)
	if err != nil {
		return nil, fmt.Errorf("ttl (prior): %w", err)
	}
	nextKey, err := ttlKey(table, ks, ttlAttr, next)
	if err != nil {
		return nil, fmt.Errorf("ttl (next): %w", err)
	}
	var ops []storage.IndexOp
	if priorKey != nil && nextKey != nil && storage.BytesEqual(priorKey, nextKey) {
		return nil, nil
	}
	if priorKey != nil {
		ops = append(ops, storage.IndexOp{Op: storage.IndexOpDelete, Key: priorKey})
	}
	if nextKey != nil {
		ops = append(ops, storage.IndexOp{Op: storage.IndexOpSet, Key: nextKey, Value: nil})
	}
	return ops, nil
}

func ttlKey(table string, ks types.KeySchema, ttlAttr string, item types.Item) ([]byte, error) {
	if item == nil {
		return nil, nil
	}
	av, ok := item[ttlAttr]
	if !ok {
		return nil, nil
	}
	if av.T != types.AttrN {
		return nil, fmt.Errorf("TTL attribute %q must be numeric", ttlAttr)
	}
	expire, err := strconv.ParseUint(av.N, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("TTL %q: parse %q: %w", ttlAttr, av.N, err)
	}
	if expire == 0 {
		return nil, nil
	}
	pk, sk, err := primaryKeyBytes(item, ks)
	if err != nil {
		return nil, err
	}
	return storage.KeyTTL(table, expire, pk, sk), nil
}

// ReaperConfig configures the background TTL sweep.
type ReaperConfig struct {
	// Interval between sweeps. Defaults to 60s.
	Interval time.Duration

	// BatchSize caps the number of entries reaped per sweep. Keeps
	// each tick bounded so the reaper never starves the write path.
	BatchSize int

	// Now overrides the wall-clock for tests.
	Now func() time.Time
}

func (c ReaperConfig) interval() time.Duration {
	if c.Interval > 0 {
		return c.Interval
	}
	return 60 * time.Second
}

func (c ReaperConfig) batch() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return 1024
}

func (c ReaperConfig) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// LeaderGate is satisfied by *raft.DB. The reaper runs only on the
// leader so followers don't double-delete; single-node deployments
// pass nil and the reaper always runs.
type LeaderGate interface {
	IsLeader() bool
}

// Reaper sweeps expired TTL pointers and deletes the matching
// primary items. One goroutine per cefas process; tables are scanned
// in the order the catalog returns them.
type Reaper struct {
	db     *DB
	cat    catalogSource
	leader LeaderGate
	cfg    ReaperConfig
	stop   chan struct{}
}

// catalogSource is the minimal catalog surface the reaper needs.
// Real catalog satisfies it; tests use a faked one.
type catalogSource interface {
	List() []types.TableDescriptor
}

// NewReaper wires a Reaper. Pass leader=nil for single-node mode.
func NewReaper(db *DB, cat catalogSource, leader LeaderGate, cfg ReaperConfig) *Reaper {
	return &Reaper{db: db, cat: cat, leader: leader, cfg: cfg, stop: make(chan struct{})}
}

// Run blocks until ctx is cancelled. Use as `go r.Run(ctx)`.
func (r *Reaper) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.interval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-t.C:
			_ = r.Tick(ctx)
		}
	}
}

// Stop signals Run to exit. Safe to call once.
func (r *Reaper) Stop() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
}

// Tick performs one sweep across every table. Exposed so tests can
// drive the reaper deterministically.
func (r *Reaper) Tick(ctx context.Context) (result error) {
	leader := r.leader == nil || r.leader.IsLeader()
	r.db.ttlMu.Lock()
	r.db.ttlStatus.Enabled = true
	r.db.ttlStatus.Leader = leader
	r.db.ttlStatus.BacklogPresent = false
	r.db.ttlMu.Unlock()
	if !leader {
		return nil
	}
	r.db.ttlMu.Lock()
	r.db.ttlStatus.LastAttemptAt = r.cfg.now().UTC()
	r.db.ttlMu.Unlock()
	defer func() {
		r.db.ttlMu.Lock()
		defer r.db.ttlMu.Unlock()
		if result != nil {
			r.db.ttlStatus.Failures++
		} else {
			r.db.ttlStatus.Sweeps++
			r.db.ttlStatus.LastSuccessAt = r.cfg.now().UTC()
		}
	}()
	if r.db.repl != nil {
		if err := r.db.ReadBarrier(5 * time.Second); err != nil {
			return err
		}
	}
	now := r.cfg.now()
	for _, td := range r.cat.List() {
		if td.TTLAttribute == "" {
			continue
		}
		if err := r.sweepTable(ctx, td, now); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reaper) sweepTable(ctx context.Context, td types.TableDescriptor, now time.Time) error {
	unix := now.Unix()
	if unix < 0 {
		return nil
	}
	lower, upper := storage.PrefixTTLBefore(td.Name, uint64(unix))
	it, err := r.db.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()

	type victim struct {
		ttlKey []byte
		pkHash []byte
		sk     []byte
	}
	var victims []victim
	for valid := it.First(); valid && len(victims) < r.cfg.batch(); valid = it.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		k := append([]byte(nil), it.Key()...)
		pkHash, sk, ok := storage.ParseTTLKey(td.Name, k)
		if !ok {
			continue
		}
		victims = append(victims, victim{
			ttlKey: k,
			pkHash: append([]byte(nil), pkHash...),
			sk:     append([]byte(nil), sk...),
		})
	}
	if err := it.Error(); err != nil {
		return err
	}
	if it.Valid() && len(victims) >= r.cfg.batch() {
		r.db.ttlMu.Lock()
		r.db.ttlStatus.BacklogPresent = true
		r.db.ttlMu.Unlock()
	}

	if len(victims) == 0 {
		return nil
	}
	// The primary item key is built from pk_hash8 + sk. We already
	// stored the hash directly in the TTL key, so we can reconstruct
	// the primary key with the storage prefix + hash + sk.
	keys := make([][]byte, 0, len(victims))
	for _, v := range victims {
		key := append([]byte(storage.TableBase(td.Name)+storage.SegPrimary), v.pkHash...)
		keys = append(keys, append(key, v.sk...))
	}
	unlock := lockPrimaryKeys(keys)
	defer unlock()
	b := r.db.Batch()
	defer b.Close()
	var deletedKeys [][]byte
	var stale uint64
	for i, v := range victims {
		if b.Len() >= 8<<20 {
			r.db.ttlMu.Lock()
			r.db.ttlStatus.BacklogPresent = true
			r.db.ttlMu.Unlock()
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		primaryKey := keys[i]
		var oldItem types.Item
		raw, err := r.db.Get(primaryKey)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if raw != nil {
			oldItem, err = storage.DecodeItem(raw)
			if err != nil {
				return fmt.Errorf("decode ttl victim: %w", err)
			}
		}
		if err := b.Delete(v.ttlKey, nil); err != nil {
			return err
		}
		currentKey, err := ttlKey(td.Name, td.KeySchema, td.TTLAttribute, oldItem)
		if err != nil {
			return err
		}
		// A renewed/replaced row can leave an older pointer after recovery.
		// Clean that pointer, but never delete a different current generation.
		if oldItem == nil || !bytes.Equal(currentKey, v.ttlKey) || !itemExpired(td, oldItem, now) {
			stale++
			continue
		}
		gsi, err := storage.PlanGSI(td.Name, td.KeySchema, td.GSIs, oldItem, nil)
		if err != nil {
			return err
		}
		lsi, err := storage.PlanLSI(td.Name, td.KeySchema, td.LSIs, oldItem, nil)
		if err != nil {
			return err
		}
		spatial, err := planSpatial(td.Name, td.KeySchema, td.SpatialIndexes, oldItem, nil)
		if err != nil {
			return err
		}
		text, err := planText(td, oldItem, nil)
		if err != nil {
			return err
		}
		for _, ops := range [][]storage.IndexOp{gsi, lsi, spatial, text} {
			if err := applyIndexOps(b, ops); err != nil {
				return err
			}
		}
		if err := b.Delete(primaryKey, nil); err != nil {
			return err
		}
		deletedKeys = append(deletedKeys, primaryKey)
		if oldItem != nil && r.db.shouldAppendChangeRecord(td) {
			rec := newChangeRecord(td, ChangeDelete, keyItemFromItem(oldItem, td.KeySchema), oldItem, nil)
			rec.BatchID = r.db.nextBatchID()
			if _, err := r.db.appendChangeRecord(b, rec); err != nil {
				return fmt.Errorf("ttl change log: %w", err)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.db.CommitBatch(b); err != nil {
		return err
	}
	if isMemoryTable(td) {
		for _, key := range deletedKeys {
			r.db.memoryDelete(td.Name, key)
		}
	}
	r.db.ttlMu.Lock()
	r.db.ttlStatus.Deleted += uint64(len(deletedKeys))
	r.db.ttlStatus.StalePointers += stale
	r.db.ttlMu.Unlock()
	return nil
}
