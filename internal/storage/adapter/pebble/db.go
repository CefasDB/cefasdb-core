package pebble

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/CefasDb/cefasdb/internal/storage"

	pebbledb "github.com/cockroachdb/pebble"
)

// Replicator is satisfied by anything that knows how to ship a
// pebble.Batch.Repr through Raft and wait for it to apply locally.
// AttachReplicator routes every Set / Delete / CommitBatch through
// Replicate instead of committing locally — same contract as codeq.
//
// In Phase 1 nothing implements this; in Phase 4 the raft.DB wires in.
type Replicator interface {
	IsLeader() bool
	Replicate(repr []byte) error
	LeaderHTTPAddr() string
}

// Compile-time assertion: *DB satisfies the engine boundary
// declared in internal/storage. Keeping the assertion local to the
// adapter means a future change to the Reader / Writer interfaces
// surfaces here at build time, not at the consumer site.
var _ storage.Engine = (*DB)(nil)

// ErrNotLeader is returned by write APIs when a replicator is attached
// and this node is not the leader. The concrete value is *NotLeaderError
// carrying the leader's HTTP URL when known.
var ErrNotLeader = errors.New("cefas/storage: not leader")

// ErrNotFound is the sentinel for missing keys — re-exported from Pebble.
var ErrNotFound = pebbledb.ErrNotFound

type NotLeaderError struct {
	LeaderURL string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderURL != "" {
		return "cefas/storage: not leader (try " + e.LeaderURL + ")"
	}
	return "cefas/storage: not leader"
}

func (e *NotLeaderError) LeaderHTTPAddr() string { return e.LeaderURL }
func (e *NotLeaderError) Is(target error) bool   { return target == ErrNotLeader }

// Options configures the engine. Mirrors codeq's Options for parity.
type Options struct {
	Path string
	// FsyncOnCommit forces fsync on every batch commit. Defaults to
	// false (NoSync) for max throughput. Production deployments that
	// need crash durability flip this on.
	FsyncOnCommit bool
	// Profile selects a named Pebble performance profile. Supported
	// values: default, balanced, write-heavy, raft.
	Profile string
	// Tuning overrides individual Pebble profile values. Zero values
	// inherit the selected profile/default.
	Tuning PebbleTuning
	// Backpressure slows or rejects caller-facing writes when Pebble
	// LSM pressure crosses configured thresholds.
	Backpressure BackpressureOptions
	// StreamRetention bounds the logical DynamoDB Streams retention
	// window. The physical changelog is preserved for PITR/backup.
	StreamRetention StreamRetentionOptions
	// ChangeLogMode controls physical changelog writes. "always" preserves
	// PITR/backup records for every write; "streams-only" only writes records
	// for stream-enabled tables; "off" disables changelog writes.
	ChangeLogMode string
	// Lanes configures the read/write worker lanes above this Pebble handle.
	Lanes LaneOptions
}

// DB wraps a *pebble.DB with cefas-specific helpers: a group-commit
// coalescer (collapses N concurrent CommitBatch calls into one Pebble
// Commit to amortize the commitPipeline mutex), and a replicator hook
// for Raft mode. Same architecture as codeq.
type DB struct {
	db       *pebbledb.DB
	path     string
	commitCh chan *commitReq
	stopCh   chan struct{}
	stopped  chan struct{}

	syncOpt *pebbledb.WriteOptions
	repl    Replicator
	bp      backpressureController
	lanes   *dbLanes

	backupMu             sync.Mutex
	activeBackupRestores map[string]int

	memMu     sync.RWMutex
	memTables map[string]map[string][]byte
	memLoaded map[string]bool

	// changeIndex is monotonically advanced by appendChangeRecord via
	// atomic.Add. Seeded in Open from max(persisted ChangeCounterKey,
	// MAX(KeyChangeLog)) so a stale counter from a crash never overlaps
	// existing keys. Access on the hot path is lock-free.
	changeIndex atomic.Uint64

	streamRetention StreamRetentionOptions
	changeLogMode   string

	// streamTables remembers which tables have produced at least one
	// stream-enabled change record. The background retention loop reads
	// this set to know which tables to trim. Populated by
	// appendChangeRecord whenever rec.StreamRecord is true.
	streamTablesMu sync.RWMutex
	streamTables   map[string]struct{}

	// retentionStopCh signals the background retention loop to exit;
	// retentionStopped is closed by the loop when it has finished.
	// Both are nil when the loop is disabled (Interval < 0).
	retentionStopCh  chan struct{}
	retentionStopped chan struct{}
}

type commitReq struct {
	batch *pebbledb.Batch
	done  chan error
}

const (
	// defaultMergeBatch is the starting cap. The commit loop adapts in
	// [minMergeBatch, maxMergeBatchCap] based on observed queue pressure
	// — see commitLoop.
	defaultMergeBatch = 64
	minMergeBatch     = 16
	maxMergeBatchCap  = 256
	mergeStepUp       = 32
	mergeStepDown     = 8

	commitChanBuf = 1024
)

// Open creates or opens the Pebble database at opts.Path. Pebble takes
// an exclusive file lock on the directory — one process per path.
func Open(opts Options) (*DB, error) {
	pOpts := newPebbleOptions(opts)
	d, err := pebbledb.Open(opts.Path, pOpts)
	if err != nil {
		return nil, fmt.Errorf("pebble open %s: %w", opts.Path, err)
	}

	syncOpt := pebbledb.NoSync
	if opts.FsyncOnCommit {
		syncOpt = pebbledb.Sync
	}

	wrapper := &DB{
		db:                   d,
		path:                 opts.Path,
		commitCh:             make(chan *commitReq, commitChanBuf),
		stopCh:               make(chan struct{}),
		stopped:              make(chan struct{}),
		syncOpt:              syncOpt,
		bp:                   newBackpressureController(opts.Backpressure),
		lanes:                newDBLanes(opts.Lanes),
		streamRetention:      normalizeStreamRetentionOptions(opts.StreamRetention),
		changeLogMode:        normalizeChangeLogMode(opts.ChangeLogMode),
		activeBackupRestores: make(map[string]int),
		memTables:            make(map[string]map[string][]byte),
		memLoaded:            make(map[string]bool),
		streamTables:         make(map[string]struct{}),
	}
	go wrapper.commitLoop()
	wrapper.startRetentionLoop()
	if err := wrapper.seedChangeIndex(); err != nil {
		_ = wrapper.Close()
		return nil, fmt.Errorf("seed change index: %w", err)
	}
	return wrapper, nil
}

// AttachReplicator hooks a replication delegate (typically *raft.DB)
// onto this DB. After this call every Set / Delete / CommitBatch flows
// through r.Replicate instead of the local coalescer. Reads stay local.
// Must be called before any concurrent writes are in flight.
func (d *DB) AttachReplicator(r Replicator) { d.repl = r }

func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	close(d.stopCh)
	<-d.stopped
	d.stopRetentionLoop()
	if d.lanes != nil {
		d.lanes.Close()
	}
	return d.db.Close()
}

// Raw exposes the underlying pebble.DB. Reserved for tests, migrations
// and the future Raft snapshot path (pebble.NewSnapshot over the cefas/
// range). Production code should go through the typed helpers.
func (d *DB) Raw() *pebbledb.DB { return d.db }

// Metrics returns Pebble's point-in-time engine metrics. The values are
// intended for observability collectors and diagnostics.
func (d *DB) Metrics() pebbledb.Metrics {
	if d == nil || d.db == nil {
		return pebbledb.Metrics{}
	}
	m := d.db.Metrics()
	if m == nil {
		return pebbledb.Metrics{}
	}
	return *m
}

// Get returns the value at key. Returns (nil, ErrNotFound) on miss.
//
// Point reads bypass the read lane: the lane round-trip (~500 ns
// channel send + worker pickup + done signal) costs more than the
// Pebble Get itself, and Pebble's block-cache is already concurrent-
// safe. The lane stays in place for Iter/Scan/Query — long-running
// work where serialization keeps tail latency bounded.
func (d *DB) Get(key []byte) ([]byte, error) {
	return d.getNoLane(key)
}

func (d *DB) getNoLane(key []byte) ([]byte, error) {
	v, closer, err := d.db.Get(key)
	if err == pebbledb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	closer.Close()
	return out, nil
}

// Has reports membership without copying the value. Bypasses the read
// lane for the same reason as Get; see the Get docstring.
func (d *DB) Has(key []byte) (bool, error) {
	return d.hasNoLane(key)
}

func (d *DB) hasNoLane(key []byte) (bool, error) {
	_, closer, err := d.db.Get(key)
	if err == pebbledb.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// Set writes a single key/value. With a replicator attached, it flows
// through Replicate as a 1-op batch.
func (d *DB) Set(key, value []byte) error {
	if d.repl != nil {
		if !d.repl.IsLeader() {
			return &NotLeaderError{LeaderURL: d.repl.LeaderHTTPAddr()}
		}
		b := d.db.NewBatch()
		defer b.Close()
		if err := b.Set(key, value, nil); err != nil {
			return err
		}
		return d.repl.Replicate(b.Repr())
	}
	return d.runWrite(func() error {
		return d.db.Set(key, value, d.syncOpt)
	})
}

// Delete removes a key.
func (d *DB) Delete(key []byte) error {
	if d.repl != nil {
		if !d.repl.IsLeader() {
			return &NotLeaderError{LeaderURL: d.repl.LeaderHTTPAddr()}
		}
		b := d.db.NewBatch()
		defer b.Close()
		if err := b.Delete(key, nil); err != nil {
			return err
		}
		return d.repl.Replicate(b.Repr())
	}
	return d.runWrite(func() error {
		return d.db.Delete(key, d.syncOpt)
	})
}

// Batch returns a fresh Pebble batch. Caller MUST Close it after either
// committing via CommitBatch or discarding.
func (d *DB) Batch() *pebbledb.Batch { return d.db.NewBatch() }

// CommitBatch atomically applies the batch. Without a replicator the
// batch goes through the group-commit coalescer; with a replicator it
// flows through Replicate (the FSM applies it on every node, including
// this one). Caller still owns b and must Close it after this returns.
//
// Submission bypasses the write lane: the lane was designed to throttle
// small independent ops (Set/Delete/Get/Has), and routing CommitBatch
// through it caps in-flight requests at lane.write.workers and starves
// commitLoop's maxMergeBatch coalescer. Callers pay only the cheap
// goroutine park on <-req.done, which scales freely.
func (d *DB) CommitBatch(b *pebbledb.Batch) error {
	if d.repl != nil {
		if !d.repl.IsLeader() {
			return &NotLeaderError{LeaderURL: d.repl.LeaderHTTPAddr()}
		}
		return d.repl.Replicate(b.Repr())
	}
	req := &commitReq{batch: b, done: make(chan error, 1)}
	select {
	case d.commitCh <- req:
	case <-d.stopCh:
		return fmt.Errorf("db closed")
	}
	return <-req.done
}

// ApplyCommittedBatch applies a batch that has already been committed through
// the replication layer. It intentionally bypasses CommitBatch so followers and
// the leader FSM do not feed an already-committed Raft entry back into Raft.
func (d *DB) ApplyCommittedBatch(repr []byte) error {
	if len(repr) == 0 {
		return nil
	}
	return d.runWrite(func() error {
		batch := d.db.NewBatch()
		defer batch.Close()
		if err := batch.SetRepr(append([]byte(nil), repr...)); err != nil {
			return fmt.Errorf("fsm apply: SetRepr: %w", err)
		}
		if err := batch.Commit(pebbledb.NoSync); err != nil {
			return fmt.Errorf("fsm apply: commit: %w", err)
		}
		return nil
	})
}

// commitLoop is the single goroutine that owns the Pebble write side. It
// merges up to mergeLimit concurrent requests into one Pebble Commit
// to collapse commitPipeline mutex contention. Same pattern as codeq.
//
// mergeLimit adapts after every commit: if the drain saturated the
// current limit and commitCh is more than 3/4 full, raise by
// mergeStepUp (capped at maxMergeBatchCap) to amortize fsync/WAL over
// more callers; if the drain pulled less than a quarter of the limit,
// lower by mergeStepDown (floored at minMergeBatch) to keep tail
// latency bounded under light load.
func (d *DB) commitLoop() {
	defer close(d.stopped)
	mergeLimit := defaultMergeBatch
	for {
		var first *commitReq
		select {
		case <-d.stopCh:
			for {
				select {
				case req := <-d.commitCh:
					req.done <- fmt.Errorf("db closed")
				default:
					return
				}
			}
		case first = <-d.commitCh:
		}

		merged := d.db.NewBatch()
		if err := merged.Apply(first.batch, nil); err != nil {
			first.done <- err
			_ = merged.Close()
			continue
		}
		reqs := []*commitReq{first}

	drain:
		for len(reqs) < mergeLimit {
			select {
			case more := <-d.commitCh:
				if err := merged.Apply(more.batch, nil); err != nil {
					more.done <- err
					break drain
				}
				reqs = append(reqs, more)
			default:
				break drain
			}
		}

		// Adapt mergeLimit for the next iteration. Hysteresis: only
		// step up when the drain fully saturated the current cap and
		// the channel is genuinely backed up; only step down when the
		// drain barely picked anything.
		if len(reqs) >= mergeLimit && len(d.commitCh) >= cap(d.commitCh)*3/4 {
			if mergeLimit+mergeStepUp <= maxMergeBatchCap {
				mergeLimit += mergeStepUp
			} else {
				mergeLimit = maxMergeBatchCap
			}
		} else if len(reqs) <= mergeLimit/4 {
			if mergeLimit-mergeStepDown >= minMergeBatch {
				mergeLimit -= mergeStepDown
			} else {
				mergeLimit = minMergeBatch
			}
		}

		err := merged.Commit(d.syncOpt)
		_ = merged.Close()
		for _, r := range reqs {
			r.done <- err
		}
	}
}

// Iter returns a new iterator scoped to [lower, upper). Caller MUST Close.
func (d *DB) Iter(lower, upper []byte) (*pebbledb.Iterator, error) {
	return d.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
}

// Health is a cheap liveness probe.
func (d *DB) Health(ctx context.Context) error {
	if d == nil || d.db == nil {
		return fmt.Errorf("db not open")
	}
	_, _, err := d.db.Get([]byte(storage.Namespace + "__health__"))
	if err != nil && err != pebbledb.ErrNotFound {
		return err
	}
	_ = ctx
	return nil
}
