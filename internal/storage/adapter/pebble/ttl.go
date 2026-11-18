package pebble

import (
	"bytes"
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
	if _, err := r.db.expireNativeImages(now); err != nil {
		return err
	}
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

const (
	// ttlReaperMaxChunkBytes caps one reaper commit. Smaller fsynced batches
	// keep the reaper from monopolising the WAL and let writers group-commit
	// between chunks (FU-01).
	ttlReaperMaxChunkBytes = 1 << 20
	// ttlReaperMaxSweepBytes keeps the historical per-sweep work bound.
	ttlReaperMaxSweepBytes = 8 << 20
	// ttlReaperLockGroup is the number of victims whose primary-key locks are
	// held together. A group is planned and committed before its locks are
	// released, so a writer never loses a renewed row to a stale delete, and
	// no sweep holds more than this many of the 64 lock shards.
	ttlReaperLockGroup = 16
	// pebbleBatchHeaderLen is the fixed Pebble batch header (seqnum+count).
	pebbleBatchHeaderLen = 12
)

type ttlVictim struct {
	ttlKey []byte
	pkHash []byte
	sk     []byte
}

// ttlSweep is the per-table sweep accumulator. Counters advance only for
// chunks that committed, so a failed sweep never over-reports progress.
type ttlSweep struct {
	r       *Reaper
	td      types.TableDescriptor
	now     time.Time
	bytes   int  // planned bytes this sweep (ttlReaperMaxSweepBytes)
	stopped bool // the sweep byte budget was reached
}

func (r *Reaper) sweepTable(ctx context.Context, td types.TableDescriptor, now time.Time) error {
	unix := now.Unix()
	if unix < 0 {
		return nil
	}
	victims, err := r.collectVictims(ctx, td, uint64(unix))
	if err != nil || len(victims) == 0 {
		return err
	}
	// The primary item key is built from pk_hash8 + sk. We already
	// stored the hash directly in the TTL key, so we can reconstruct
	// the primary key with the storage prefix + hash + sk.
	keys := make([][]byte, 0, len(victims))
	for _, v := range victims {
		key := append([]byte(storage.TableBase(td.Name)+storage.SegPrimary), v.pkHash...)
		keys = append(keys, append(key, v.sk...))
	}
	sw := &ttlSweep{r: r, td: td, now: now}
	for start := 0; start < len(victims) && !sw.stopped; start += ttlReaperLockGroup {
		end := min(start+ttlReaperLockGroup, len(victims))
		if err := sw.group(ctx, victims[start:end], keys[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reaper) collectVictims(ctx context.Context, td types.TableDescriptor, unix uint64) ([]ttlVictim, error) {
	lower, upper := storage.PrefixTTLBefore(td.Name, unix)
	it, err := r.db.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var victims []ttlVictim
	for valid := it.First(); valid && len(victims) < r.cfg.batch(); valid = it.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		k := append([]byte(nil), it.Key()...)
		pkHash, sk, ok := storage.ParseTTLKey(td.Name, k)
		if !ok {
			continue
		}
		victims = append(victims, ttlVictim{
			ttlKey: k,
			pkHash: append([]byte(nil), pkHash...),
			sk:     append([]byte(nil), sk...),
		})
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if it.Valid() && len(victims) >= r.cfg.batch() {
		r.db.setTTLBacklog()
	}
	return victims, nil
}

func (d *DB) setTTLBacklog() {
	d.ttlMu.Lock()
	d.ttlStatus.BacklogPresent = true
	d.ttlMu.Unlock()
}

// ttlChunk is one commit unit: planned mutations plus the change records
// whose sequences are allocated only at commit time.
type ttlChunk struct {
	b        *pebbledb.Batch
	records  []ChangeRecord
	recBytes int
	deleted  [][]byte
	stale    uint64
}

func (c *ttlChunk) size() int { return c.b.Len() + c.recBytes }
func (c *ttlChunk) empty() bool {
	return c.b.Empty() && len(c.records) == 0
}

// group plans and commits one lock group in chunks of at most
// ttlReaperMaxChunkBytes (a single victim larger than that commits alone).
func (sw *ttlSweep) group(ctx context.Context, victims []ttlVictim, keys [][]byte) error {
	db := sw.r.db
	unlock := lockPrimaryKeys(keys)
	defer unlock()
	chunk := &ttlChunk{b: db.Batch()}
	defer func() { _ = chunk.b.Close() }()
	scratch := db.Batch()
	defer scratch.Close()
	measure := db.Batch()
	defer measure.Close()
	for i, v := range victims {
		if sw.bytes >= ttlReaperMaxSweepBytes {
			db.setTTLBacklog()
			sw.stopped = true
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		scratch.Reset()
		rec, hasRec, deleted, err := sw.planVictim(scratch, v, keys[i])
		if err != nil {
			return err
		}
		recBytes := 0
		if hasRec {
			if recBytes, err = sw.recordBytes(measure, rec); err != nil {
				return err
			}
		}
		victimBytes := scratch.Len() - pebbleBatchHeaderLen + recBytes
		if !chunk.empty() && chunk.size()+victimBytes > ttlReaperMaxChunkBytes {
			if err := sw.commit(ctx, chunk); err != nil {
				return err
			}
		}
		if err := chunk.b.Apply(scratch, nil); err != nil {
			return err
		}
		if hasRec {
			chunk.records = append(chunk.records, rec)
			chunk.recBytes += recBytes
		}
		if deleted {
			chunk.deleted = append(chunk.deleted, keys[i])
		} else {
			chunk.stale++
		}
		sw.bytes += victimBytes
	}
	if chunk.empty() {
		return nil
	}
	return sw.commit(ctx, chunk)
}

// planVictim writes one victim's deletes into b. It allocates nothing: the
// change record is returned for allocation at commit time.
func (sw *ttlSweep) planVictim(b *pebbledb.Batch, v ttlVictim, primaryKey []byte) (ChangeRecord, bool, bool, error) {
	db, td := sw.r.db, sw.td
	var oldItem types.Item
	raw, err := db.Get(primaryKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return ChangeRecord{}, false, false, err
	}
	if raw != nil {
		oldItem, err = storage.DecodeItem(raw)
		if err != nil {
			return ChangeRecord{}, false, false, fmt.Errorf("decode ttl victim: %w", err)
		}
	}
	if err := b.Delete(v.ttlKey, nil); err != nil {
		return ChangeRecord{}, false, false, err
	}
	currentKey, err := ttlKey(td.Name, td.KeySchema, td.TTLAttribute, oldItem)
	if err != nil {
		return ChangeRecord{}, false, false, err
	}
	// A renewed/replaced row can leave an older pointer after recovery.
	// Clean that pointer, but never delete a different current generation.
	if oldItem == nil || !bytes.Equal(currentKey, v.ttlKey) || !itemExpired(td, oldItem, sw.now) {
		return ChangeRecord{}, false, false, nil
	}
	gsi, err := storage.PlanGSI(td.Name, td.KeySchema, td.GSIs, oldItem, nil)
	if err != nil {
		return ChangeRecord{}, false, false, err
	}
	lsi, err := storage.PlanLSI(td.Name, td.KeySchema, td.LSIs, oldItem, nil)
	if err != nil {
		return ChangeRecord{}, false, false, err
	}
	spatial, err := planSpatial(td.Name, td.KeySchema, td.SpatialIndexes, oldItem, nil)
	if err != nil {
		return ChangeRecord{}, false, false, err
	}
	text, err := planText(td, oldItem, nil)
	if err != nil {
		return ChangeRecord{}, false, false, err
	}
	for _, ops := range [][]storage.IndexOp{gsi, lsi, spatial, text} {
		if err := applyIndexOps(b, ops); err != nil {
			return ChangeRecord{}, false, false, err
		}
	}
	if err := b.Delete(primaryKey, nil); err != nil {
		return ChangeRecord{}, false, false, err
	}
	if err := deleteNativeCustody(b, td, oldItem); err != nil {
		return ChangeRecord{}, false, false, err
	}
	if !db.shouldAppendChangeRecord(td) {
		return ChangeRecord{}, false, true, nil
	}
	rec := newChangeRecord(td, ChangeDelete, keyItemFromItem(oldItem, td.KeySchema), oldItem, nil)
	rec.BatchID = db.nextBatchID()
	// The timestamp is stamped with the sequence at commit time
	// (appendChangeRecord), never at planning time: a planned timestamp
	// could precede that of a sequence another writer allocated earlier.
	return rec, true, true, nil
}

// recordBytes is an upper bound of the batch bytes rec adds once it has a
// sequence: it is written with the widest possible sequence. Sequence and
// timestamp are fixed-width in the record and the expiration key, so a
// placeholder timestamp (any positive value) sizes the committed record.
func (sw *ttlSweep) recordBytes(measure *pebbledb.Batch, rec ChangeRecord) (int, error) {
	measure.Reset()
	rec.Index = ^uint64(0)
	rec.UnixNano = time.Now().UnixNano()
	if _, err := sw.r.db.writeChangeRecord(measure, rec); err != nil {
		return 0, fmt.Errorf("ttl change log: %w", err)
	}
	return measure.Len() - pebbleBatchHeaderLen, nil
}

// commit allocates the chunk's change sequences and commits it. The
// native-stream reservation covers only allocation through commit, never
// the cold reads of planning, so stream readers are not held back by them.
func (sw *ttlSweep) commit(ctx context.Context, chunk *ttlChunk) error {
	db, td := sw.r.db, sw.td
	if err := ctx.Err(); err != nil {
		return err
	}
	err := func() error {
		streamDone := db.beginNativeStreamWrite(td)
		defer streamDone()
		for _, rec := range chunk.records {
			if _, err := db.appendChangeRecord(chunk.b, rec); err != nil {
				return fmt.Errorf("ttl change log: %w", err)
			}
		}
		return db.CommitBatch(chunk.b)
	}()
	if err != nil {
		return err
	}
	if isMemoryTable(td) {
		for _, key := range chunk.deleted {
			db.memoryDelete(td.Name, key)
		}
	}
	db.ttlMu.Lock()
	db.ttlStatus.Deleted += uint64(len(chunk.deleted))
	db.ttlStatus.StalePointers += chunk.stale
	db.ttlMu.Unlock()
	chunk.b.Reset()
	chunk.records = chunk.records[:0]
	chunk.recBytes = 0
	chunk.deleted = nil
	chunk.stale = 0
	return nil
}
