package pebble

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"

	pebbledb "github.com/cockroachdb/pebble"

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
			if r.leader != nil && !r.leader.IsLeader() {
				continue
			}
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
func (r *Reaper) Tick(ctx context.Context) error {
	now := uint64(r.cfg.now().Unix())
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

func (r *Reaper) sweepTable(ctx context.Context, td types.TableDescriptor, now uint64) error {
	lower, upper := storage.PrefixTTLBefore(td.Name, now)
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

	if len(victims) == 0 {
		return nil
	}
	// The primary item key is built from pk_hash8 + sk. We already
	// stored the hash directly in the TTL key, so we can reconstruct
	// the primary key with the storage prefix + hash + sk.
	b := r.db.Batch()
	defer b.Close()
	for _, v := range victims {
		primaryKey := append([]byte(nil), []byte(storage.TableBase(td.Name)+storage.SegPrimary)...)
		primaryKey = append(primaryKey, v.pkHash...)
		primaryKey = append(primaryKey, v.sk...)
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
		if err := b.Delete(primaryKey, nil); err != nil {
			return err
		}
		if err := b.Delete(v.ttlKey, nil); err != nil {
			return err
		}
		if oldItem != nil && r.db.shouldAppendChangeRecord(td) {
			if _, err := r.db.appendChangeRecord(b, newChangeRecord(td, ChangeDelete, keyItemFromItem(oldItem, td.KeySchema), oldItem, nil)); err != nil {
				return fmt.Errorf("ttl change log: %w", err)
			}
		}
	}
	return r.db.CommitBatch(b)
}

// _ keeps pebbledb referenced for future raw-iterator paths.
var _ = (*pebbledb.Iterator)(nil)
