package pebble

import (
	"container/list"
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
	// AdaptiveMode enables the workload-mode observer/tuner. When true, a
	// background goroutine samples read/write ops per second and adjusts
	// commitLoop's mergeLimit and the retention loop's interval to fit
	// the observed mode (idle / read-heavy / write-heavy / mixed). Off
	// by default; counter increments and the goroutine are skipped when
	// disabled, so the hot path pays nothing.
	AdaptiveMode bool
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

	slSharesMu       sync.RWMutex
	slSharesResolver ServiceLevelSharesResolver
	slSharesCache    sync.Map

	backupMu             sync.Mutex
	activeBackupRestores map[string]int

	memMu     sync.RWMutex
	memTables map[string]map[string][]byte
	memLoaded map[string]bool

	atomicDedupMu  sync.Mutex
	atomicDedupLRU *list.List
	atomicDedup    map[string]*list.Element

	// changeIndex is monotonically advanced by appendChangeRecord via
	// atomic.Add. Seeded in Open from max(persisted ChangeCounterKey,
	// MAX(KeyChangeLog)) so a stale counter from a crash never overlaps
	// existing keys. Access on the hot path is lock-free.
	changeIndex atomic.Uint64

	// batchSeqCounter is the monotonic per-process source of
	// idempotency markers (#524). Combined with a nanosecond
	// timestamp so the value remains unique across restarts.
	batchSeqCounter atomic.Uint64

	streamRetention         StreamRetentionOptions
	streamRetentionResolver func(table string) int64
	changeLogMode           string

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

	// workload runs the adaptive observer/tuner when Options.AdaptiveMode
	// is set. Nil otherwise — callers must check before recording or
	// reading tuning values.
	workload *workloadMode
}

type commitReq struct {
	batch *pebbledb.Batch
	done  chan error
}

const (
	// Static defaults / bounds for the commit-loop merge cap. With
	// AdaptiveMode off, commitLoop uses defaultMergeBatch as a constant.
	// With AdaptiveMode on, workloadMode picks values in
	// [minMergeBatch, maxMergeBatchCap] per observed mode.
	defaultMergeBatch = 64
	minMergeBatch     = 16
	maxMergeBatchCap  = 256
	commitChanBuf     = 1024
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
		atomicDedupLRU:       list.New(),
		atomicDedup:          make(map[string]*list.Element),
		streamTables:         make(map[string]struct{}),
	}
	if opts.AdaptiveMode {
		wrapper.workload = newWorkloadMode(wrapper.streamRetention.Interval)
		wrapper.workload.start()
	}
	go wrapper.commitLoop()
	wrapper.startRetentionLoop()
	if err := wrapper.seedChangeIndex(); err != nil {
		_ = wrapper.Close()
		return nil, fmt.Errorf("seed change index: %w", err)
	}
	return wrapper, nil
}

// AttachStreamRetentionResolver registers a per-table retention
// resolver consulted by the retention loop (#521). When non-nil, a
// positive return value overrides the cluster-default retention for
// that table; zero means "no override, use the default". Catalog
// integrations register this so StreamSpecification.RetentionSeconds
// flows into the loop without the pebble layer importing catalog.
func (d *DB) AttachStreamRetentionResolver(fn func(table string) int64) {
	d.streamRetentionResolver = fn
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
	if d.workload != nil {
		d.workload.stop()
	}
	if d.lanes != nil {
		d.lanes.Close()
	}
	return d.db.Close()
}

// WorkloadSnapshot returns the adaptive workload mode observer's current
// state, or the zero value when adaptive mode is off.
func (d *DB) WorkloadSnapshot() WorkloadModeSnapshot {
	if d == nil {
		return WorkloadModeSnapshot{}
	}
	return d.workload.Snapshot()
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
	if d.workload != nil {
		d.workload.recordRead()
	}
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
	if d.workload != nil {
		d.workload.recordWrite()
	}
	if d.repl != nil {
		if !d.repl.IsLeader() {
			return &NotLeaderError{LeaderURL: d.repl.LeaderHTTPAddr()}
		}
		return d.repl.Replicate(b.Repr())
	}
	return d.commitLocal(b)
}

// CommitBatchLocal commits the batch to this node's pebble store
// without consulting the replicator. Use for writes that are
// intentionally RF=1 (materialized-view cascade). The caller is
// responsible for ensuring the receiving node is the right owner —
// followers will not see the resulting state.
func (d *DB) CommitBatchLocal(b *pebbledb.Batch) error {
	if d.workload != nil {
		d.workload.recordWrite()
	}
	return d.commitLocal(b)
}

func (d *DB) commitLocal(b *pebbledb.Batch) error {
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
// merges up to maxMergeBatch concurrent requests into one Pebble Commit
// to collapse commitPipeline mutex contention. Same pattern as codeq.
func (d *DB) commitLoop() {
	defer close(d.stopped)
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

		// MergeLimit() returns defaultMergeBatch when adaptive mode is off
		// and the per-mode cap when on (workloadMode keeps it in
		// [minMergeBatch, maxMergeBatchCap]).
		limit := d.workload.MergeLimit()
	drain:
		for len(reqs) < limit {
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

		err := merged.Commit(d.syncOpt)
		_ = merged.Close()
		for _, r := range reqs {
			r.done <- err
		}
	}
}

// Iter returns a new iterator scoped to [lower, upper). Caller MUST Close.
func (d *DB) Iter(lower, upper []byte) (*pebbledb.Iterator, error) {
	return d.IterCtx(context.Background(), lower, upper)
}

// IterCtx returns a new iterator after admitting the request to the
// service level read lane. Caller MUST Close.
func (d *DB) IterCtx(ctx context.Context, lower, upper []byte) (*pebbledb.Iterator, error) {
	var it *pebbledb.Iterator
	err := d.runReadCtx(ctx, func() error {
		var err error
		it, err = d.iterNoLane(lower, upper)
		return err
	})
	return it, err
}

func (d *DB) iterNoLane(lower, upper []byte) (*pebbledb.Iterator, error) {
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
