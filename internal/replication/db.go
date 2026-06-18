package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	hraft "github.com/hashicorp/raft"
)

// Config configures a raft.DB.
//
// SelfID + BindAddr are required. PeerAddrs lists every voter in the
// cluster (including SelfID); only the first-started node should set
// Bootstrap=true. After bootstrap subsequent nodes join via AddVoter
// from the current leader.
type Config struct {
	// Path is the parent directory for raft state. Snapshots live
	// under Path/snapshots; the Pebble DB itself is supplied via
	// AttachPebble below — the storage layer owns it.
	Path string

	SelfID    string
	BindAddr  string
	Bootstrap bool

	// PeerAddrs maps raft ServerID → raft bind address. Used for
	// initial bootstrap configuration.
	PeerAddrs map[string]string

	// PeerHTTPAddrs maps raft ServerID → HTTP base URL ("http://host:port").
	// When set, LeaderHTTPAddr returns the leader's HTTP URL so HTTP
	// middleware can redirect writes with 307.
	PeerHTTPAddrs map[string]string

	// Tunable raft timeouts. Zero values fall back to documented
	// defaults below.
	HeartbeatMS     int
	ElectionMS      int
	LeaderLeaseMS   int
	CommitMS        int
	SnapshotEntries uint64

	// ApplyTimeout bounds each Replicate call's raft round-trip.
	ApplyTimeout time.Duration

	// LogOutput is the destination for raft's internal logs. Defaults
	// to os.Stderr; tests pass io.Discard to keep output quiet.
	LogOutput interface {
		Write(p []byte) (n int, err error)
	}

	// StreamLayer, when non-nil, is used instead of the default
	// TCP transport. Multi-Raft mode passes a per-shard
	// hraft.StreamLayer routed by a shared MuxAcceptor so every
	// shard shares one OS-level port. Single-Raft mode leaves this
	// nil and Open builds its own NewTCPTransport on BindAddr.
	StreamLayer hraft.StreamLayer
}

func (c Config) heartbeat() time.Duration {
	if c.HeartbeatMS > 0 {
		return time.Duration(c.HeartbeatMS) * time.Millisecond
	}
	return 1000 * time.Millisecond
}

func (c Config) election() time.Duration {
	if c.ElectionMS > 0 {
		return time.Duration(c.ElectionMS) * time.Millisecond
	}
	return 1000 * time.Millisecond
}

func (c Config) leaderLease() time.Duration {
	if c.LeaderLeaseMS > 0 {
		return time.Duration(c.LeaderLeaseMS) * time.Millisecond
	}
	return 500 * time.Millisecond
}

func (c Config) commitTimeout() time.Duration {
	if c.CommitMS > 0 {
		return time.Duration(c.CommitMS) * time.Millisecond
	}
	return 50 * time.Millisecond
}

func (c Config) snapshotInterval() time.Duration { return 120 * time.Second }

func (c Config) snapshotThreshold() uint64 {
	if c.SnapshotEntries > 0 {
		return c.SnapshotEntries
	}
	return 8192
}

func (c Config) applyTimeout() time.Duration {
	if c.ApplyTimeout > 0 {
		return c.ApplyTimeout
	}
	return 10 * time.Second
}

// ErrNotLeader is returned by writes when this node is not the
// current raft leader.
var ErrNotLeader = errors.New("cefas/raft: not leader")

// ErrAsyncApplyQueueFull is returned by best-effort replication when
// the local async apply queue is saturated. The caller has already
// committed locally, so this protects the write path from reintroducing
// Raft backpressure in write-consistency=one mode.
var ErrAsyncApplyQueueFull = errors.New("cefas/raft: async apply queue full")

// ErrNotFound mirrors pebble.ErrNotFound so callers don't need to
// import pebble.
var ErrNotFound = pebbledb.ErrNotFound

type AsyncReplicationStats struct {
	Submitted     uint64
	Dropped       uint64
	ApplyErrors   uint64
	QueueDepth    int
	QueueCapacity int
}

// DB attaches raft replication onto a *pebble.DB the storage layer
// owns. The storage.DB.AttachReplicator wiring forwards CommitBatch
// to DB.Replicate so writes flow through the raft log. Raft log/stable
// metadata may use a separate Pebble instance to avoid competing with
// user data compaction.
type DB struct {
	pebble        *pebbledb.DB
	raftPebble    *pebbledb.DB
	raft          *hraft.Raft
	fsm           *fsm
	trans         hraft.Transport
	cfg           Config
	publisher     *Publisher
	snapshotStore hraft.SnapshotStore

	leaderCh chan bool
	stopCh   chan struct{}

	// Apply coalescer: Replicate submits to applyCh; one loop pops
	// requests, merges concurrent ones into a single pebble.Batch,
	// and submits a single raft.Apply with the merged Repr. Collapses
	// N small log entries and N FSM commits into one.
	applyCh      chan *applyReq
	applyStopped chan struct{}

	asyncSubmitted   atomic.Uint64
	asyncDropped     atomic.Uint64
	asyncApplyErrors atomic.Uint64
}

type applyReq struct {
	repr []byte
	done chan error
}

const (
	// raftMergeBatch caps how many concurrent Replicate calls merge
	// into one raft.Apply. 128 keeps tail-latency bounded while
	// amortising AE round-trip + FSM Apply + Pebble commit overhead
	// over many submitters.
	raftMergeBatch   = 128
	raftApplyChanBuf = 1024
)

// Open wires raft on top of the caller's data Pebble store. When
// raftStores is supplied, raft log/stable metadata use raftStores[0];
// otherwise they fall back to pdb for legacy shared-store deployments.
// Close does not close either Pebble store; callers retain ownership.
func Open(ctx context.Context, cfg Config, pdb *pebbledb.DB, raftStores ...*pebbledb.DB) (*DB, error) {
	if pdb == nil {
		return nil, fmt.Errorf("raft: pdb is required")
	}
	if cfg.SelfID == "" {
		return nil, fmt.Errorf("raft: SelfID is required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("raft: BindAddr is required")
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("raft: Path is required (for snapshot dir)")
	}
	raftPDB := pdb
	if len(raftStores) > 0 && raftStores[0] != nil {
		raftPDB = raftStores[0]
	}

	logOutput := cfg.LogOutput
	if logOutput == nil {
		logOutput = os.Stderr
	}

	d := &DB{
		pebble:       pdb,
		raftPebble:   raftPDB,
		cfg:          cfg,
		leaderCh:     make(chan bool, 8),
		stopCh:       make(chan struct{}),
		applyCh:      make(chan *applyReq, raftApplyChanBuf),
		applyStopped: make(chan struct{}),
	}

	logs := newLogStore(raftPDB)
	stable := newStableStore(raftPDB)
	snaps, err := newSnapshotStore(cfg.Path + "/snapshots")
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}
	d.fsm = newFSM(pdb)
	d.snapshotStore = snaps

	var t hraft.Transport
	if cfg.StreamLayer != nil {
		// Multi-Raft: a shared MuxAcceptor provided this shard's
		// StreamLayer. NewNetworkTransport drives raft's wire
		// protocol on top.
		t = hraft.NewNetworkTransport(cfg.StreamLayer, 3, 10*time.Second, asWriter(logOutput))
	} else {
		tcp, err := hraft.NewTCPTransport(cfg.BindAddr, nil, 3, 10*time.Second, asWriter(logOutput))
		if err != nil {
			return nil, fmt.Errorf("tcp transport: %w", err)
		}
		t = tcp
	}
	d.trans = t

	rcfg := hraft.DefaultConfig()
	rcfg.LocalID = hraft.ServerID(cfg.SelfID)
	rcfg.HeartbeatTimeout = cfg.heartbeat()
	rcfg.ElectionTimeout = cfg.election()
	rcfg.LeaderLeaseTimeout = cfg.leaderLease()
	rcfg.CommitTimeout = cfg.commitTimeout()
	rcfg.SnapshotInterval = cfg.snapshotInterval()
	rcfg.SnapshotThreshold = cfg.snapshotThreshold()
	rcfg.LogOutput = asWriter(logOutput)

	if cfg.Bootstrap {
		hasState, err := hraft.HasExistingState(logs, stable, snaps)
		if err != nil {
			return nil, fmt.Errorf("HasExistingState: %w", err)
		}
		if !hasState {
			peers := cfg.PeerAddrs
			if len(peers) == 0 {
				peers = map[string]string{cfg.SelfID: cfg.BindAddr}
			}
			var configuration hraft.Configuration
			for id, addr := range peers {
				configuration.Servers = append(configuration.Servers, hraft.Server{
					Suffrage: hraft.Voter,
					ID:       hraft.ServerID(id),
					Address:  hraft.ServerAddress(addr),
				})
			}
			if err := hraft.BootstrapCluster(rcfg, logs, stable, snaps, t, configuration); err != nil {
				return nil, fmt.Errorf("BootstrapCluster: %w", err)
			}
		}
	}

	r, err := hraft.NewRaft(rcfg, d.fsm, logs, stable, snaps, t)
	if err != nil {
		return nil, fmt.Errorf("NewRaft: %w", err)
	}
	d.raft = r

	go d.forwardLeaderChanges()
	go d.applyLoop()

	_ = ctx
	return d, nil
}

func asWriter(w interface{ Write(p []byte) (int, error) }) *osFileLike {
	return &osFileLike{w: w}
}

// osFileLike adapts an io.Writer to the *os.File parameter hashicorp/raft
// historically expected. raft only calls Write on it.
type osFileLike struct {
	w interface{ Write(p []byte) (int, error) }
}

func (f *osFileLike) Write(p []byte) (int, error) { return f.w.Write(p) }

func (d *DB) forwardLeaderChanges() {
	src := d.raft.LeaderCh()
	for {
		select {
		case <-d.stopCh:
			return
		case isLeader, ok := <-src:
			if !ok {
				return
			}
			select {
			case d.leaderCh <- isLeader:
			default:
				// Buffer full; drop. Consumers only need to know the
				// current state, not full history.
			}
		}
	}
}

// Close stops raft and the transport. Does NOT close the Pebble store
// — the storage layer keeps ownership.
func (d *DB) Close() error {
	if d == nil {
		return nil
	}
	select {
	case <-d.stopCh:
	default:
		close(d.stopCh)
	}
	if d.raft != nil {
		if err := d.raft.Shutdown().Error(); err != nil {
			return fmt.Errorf("raft shutdown: %w", err)
		}
	}
	if closer, ok := d.trans.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	<-d.applyStopped
	return nil
}

// LocalAddr returns the bound address of the raft transport (useful in
// tests that pass "127.0.0.1:0" to grab an ephemeral port).
func (d *DB) LocalAddr() net.Addr {
	if d.trans == nil {
		return nil
	}
	return netAddrFromString(string(d.trans.LocalAddr()))
}

// IsLeader reports whether this node is the current raft leader.
// Safe on a nil receiver — the single-node server path wraps a
// (*DB)(nil) in a LeaderGate interface, so without this guard the
// metrics collector segfaults seconds after the server binds.
func (d *DB) IsLeader() bool {
	if d == nil || d.raft == nil {
		return false
	}
	return d.raft.State() == hraft.Leader
}

// LeaderObservation returns a channel that receives true on becoming
// leader, false on losing leadership.
func (d *DB) LeaderObservation() <-chan bool { return d.leaderCh }

// LeaderInfo returns the current leader's id and bind address according
// to local state. Empty strings mean no leader is known yet.
func (d *DB) LeaderInfo() (id, addr string) {
	if d.raft == nil {
		return "", ""
	}
	rawAddr, rawID := d.raft.LeaderWithID()
	return string(rawID), string(rawAddr)
}

// AttachPublisher wires a CDC Publisher onto the FSM. After this
// call every committed batch emits ChangeEvents the publisher fans
// out to its subscribers.
func (d *DB) AttachPublisher(p *Publisher) {
	d.publisher = p
	if d.fsm != nil {
		d.fsm.AttachPublisher(p)
	}
}

// Publisher returns the attached publisher (nil if none).
func (d *DB) Publisher() *Publisher { return d.publisher }

// SnapshotMetadata mirrors hraft.SnapshotMeta with cefas-friendly
// names so the API layer can re-encode it without importing the
// hashicorp/raft package.
type SnapshotMetadata struct {
	ID          string
	Index       uint64
	Term        uint64
	UnixSeconds int64
	SizeBytes   int64
}

// ListSnapshots returns metadata for every retained raft snapshot.
// Used by the admin PITR surface.
func (d *DB) ListSnapshots() ([]SnapshotMetadata, error) {
	if d.snapshotStore == nil {
		return nil, nil
	}
	metas, err := d.snapshotStore.List()
	if err != nil {
		return nil, err
	}
	out := make([]SnapshotMetadata, 0, len(metas))
	for _, m := range metas {
		out = append(out, SnapshotMetadata{
			ID:        m.ID,
			Index:     m.Index,
			Term:      m.Term,
			SizeBytes: m.Size,
		})
	}
	return out, nil
}

// LeaderHTTPAddr returns the leader's HTTP base URL when known, or "".
// Satisfies storage.Replicator.LeaderHTTPAddr.
func (d *DB) LeaderHTTPAddr() string {
	id, _ := d.LeaderInfo()
	if id == "" || len(d.cfg.PeerHTTPAddrs) == 0 {
		return ""
	}
	return d.cfg.PeerHTTPAddrs[id]
}

// SelfID returns the configured raft ServerID for this node.
func (d *DB) SelfID() string { return d.cfg.SelfID }

// BindAddr returns the raft bind address for this node.
func (d *DB) BindAddr() string { return d.cfg.BindAddr }

// WaitLeader blocks until this node IS the leader, or ctx is done.
func (d *DB) WaitLeader(ctx context.Context) error {
	for !d.IsLeader() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.leaderCh:
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil
}

// WaitFollower blocks until this node is NOT the leader.
func (d *DB) WaitFollower(ctx context.Context) error {
	for d.IsLeader() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.leaderCh:
		case <-time.After(20 * time.Millisecond):
		}
	}
	return nil
}

// AddVoter asks the current leader to add `id` at `addr` as a voter.
// Only callable on the leader. `prevIndex == 0` means "no previous
// index check" — the safe default for fresh joins.
func (d *DB) AddVoter(id, addr string, timeout time.Duration) error {
	if !d.IsLeader() {
		return ErrNotLeader
	}
	f := d.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, timeout)
	return f.Error()
}

// RemoveServer asks the leader to remove `id` from the cluster.
func (d *DB) RemoveServer(id string, timeout time.Duration) error {
	if !d.IsLeader() {
		return ErrNotLeader
	}
	f := d.raft.RemoveServer(hraft.ServerID(id), 0, timeout)
	return f.Error()
}

// TransferLeadership asks the local leader to hand leadership to the
// supplied voter. It is a no-op when the requested target is this node.
func (d *DB) TransferLeadership(id, addr string, timeout time.Duration) error {
	if d == nil || d.raft == nil {
		return fmt.Errorf("raft: not initialised")
	}
	if id == "" || addr == "" {
		return fmt.Errorf("raft: leadership transfer target id and addr are required")
	}
	if id == d.cfg.SelfID {
		return nil
	}
	if !d.IsLeader() {
		return ErrNotLeader
	}
	done := make(chan error, 1)
	go func() {
		done <- d.raft.LeadershipTransferToServer(hraft.ServerID(id), hraft.ServerAddress(addr)).Error()
	}()
	if timeout <= 0 {
		return <-done
	}
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("raft: leadership transfer to %s timed out after %s", id, timeout)
	}
}

// Barrier waits for all currently-committed log entries to be applied
// on this node. Used by tests that need a quiescent state, and by the
// strong-consistency read path in the storage layer.
func (d *DB) Barrier(timeout time.Duration) error {
	if d.raft == nil {
		return fmt.Errorf("raft: not initialised")
	}
	f := d.raft.Barrier(timeout)
	return f.Error()
}

// Replicate ships a serialized pebble.Batch through raft and waits for
// it to commit + apply locally. Returns ErrNotLeader if this node
// isn't the leader.
//
// Concurrent calls coalesce through applyLoop: multiple in-flight
// Replicates queue on applyCh and the loop merges up to raftMergeBatch
// into a single raft.Apply. Each caller still gets its own done
// channel and an independent error.
func (d *DB) Replicate(repr []byte) error {
	if !d.IsLeader() {
		return ErrNotLeader
	}
	if len(repr) == 0 {
		return nil
	}
	req := &applyReq{
		repr: repr,
		done: make(chan error, 1),
	}
	select {
	case d.applyCh <- req:
	case <-d.stopCh:
		return fmt.Errorf("raft: db closing")
	}
	return <-req.done
}

// ReplicateAsync submits a batch to the raft log without waiting for
// quorum/apply. It is used by eventual-write mode after the leader has
// committed the batch locally. Async requests share the same coalescer
// as Replicate so concurrent local acks collapse into fewer raft.Apply
// calls instead of hammering Raft with one log append per user batch.
func (d *DB) ReplicateAsync(repr []byte) error {
	if !d.IsLeader() {
		return ErrNotLeader
	}
	if len(repr) == 0 {
		return nil
	}
	return d.enqueueAsyncApply(repr)
}

func (d *DB) enqueueAsyncApply(repr []byte) error {
	if d == nil || len(repr) == 0 {
		return nil
	}
	cp := append([]byte(nil), repr...)
	req := &applyReq{repr: cp}
	select {
	case <-d.stopCh:
		return fmt.Errorf("raft: db closing")
	default:
	}
	select {
	case d.applyCh <- req:
		d.asyncSubmitted.Add(1)
		return nil
	case <-d.stopCh:
		return fmt.Errorf("raft: db closing")
	default:
		d.asyncDropped.Add(1)
		return ErrAsyncApplyQueueFull
	}
}

func (d *DB) AsyncReplicationStats() AsyncReplicationStats {
	if d == nil {
		return AsyncReplicationStats{}
	}
	stats := AsyncReplicationStats{
		Submitted:   d.asyncSubmitted.Load(),
		Dropped:     d.asyncDropped.Load(),
		ApplyErrors: d.asyncApplyErrors.Load(),
	}
	if d.applyCh != nil {
		stats.QueueDepth = len(d.applyCh)
		stats.QueueCapacity = cap(d.applyCh)
	}
	return stats
}

// applyLoop merges concurrent Replicate calls into a single raft.Apply.
// Producer batches are independent (different keys → different
// payloads), so merge order within a single apply call is irrelevant;
// across calls, raft's log ordering guarantees the usual sequential
// semantics.
//
// Tail-latency: a submitter that arrives just after the loop kicked
// off a merge pays one cycle of wait. With raftMergeBatch=128 the
// per-merge wait is dwarfed by the AE round-trip we save by sharing.
func (d *DB) applyLoop() {
	defer close(d.applyStopped)
	for {
		var first *applyReq
		select {
		case <-d.stopCh:
			for {
				select {
				case req := <-d.applyCh:
					d.completeApply(req, fmt.Errorf("raft: db closing"))
				default:
					return
				}
			}
		case first = <-d.applyCh:
		}

		merged := d.pebble.NewBatch()
		if err := merged.SetRepr(append([]byte(nil), first.repr...)); err != nil {
			d.completeApply(first, fmt.Errorf("raft apply: setrepr first: %w", err))
			_ = merged.Close()
			continue
		}
		reqs := []*applyReq{first}

	drain:
		for len(reqs) < raftMergeBatch {
			select {
			case more := <-d.applyCh:
				tmp := d.pebble.NewBatch()
				if err := tmp.SetRepr(append([]byte(nil), more.repr...)); err != nil {
					d.completeApply(more, fmt.Errorf("raft apply: setrepr merge: %w", err))
					_ = tmp.Close()
					break drain
				}
				if err := merged.Apply(tmp, nil); err != nil {
					d.completeApply(more, fmt.Errorf("raft apply: merge: %w", err))
					_ = tmp.Close()
					break drain
				}
				_ = tmp.Close()
				reqs = append(reqs, more)
			default:
				break drain
			}
		}

		// Leadership may have shifted between the IsLeader check and
		// here; bail out cleanly. Submitters retry against the new
		// leader through the existing not-leader path.
		if !d.IsLeader() {
			_ = merged.Close()
			for _, r := range reqs {
				d.completeApply(r, ErrNotLeader)
			}
			continue
		}

		cp := append([]byte(nil), merged.Repr()...)
		_ = merged.Close()
		f := d.raft.Apply(cp, d.cfg.applyTimeout())
		err := f.Error()
		if err != nil {
			err = fmt.Errorf("raft apply: %w", err)
		} else if resp := f.Response(); resp != nil {
			if applyErr, ok := resp.(error); ok && applyErr != nil {
				err = applyErr
			}
		}
		for _, r := range reqs {
			d.completeApply(r, err)
		}
	}
}

func (d *DB) completeApply(req *applyReq, err error) {
	if req == nil {
		return
	}
	if req.done != nil {
		req.done <- err
		return
	}
	if err != nil {
		d.asyncApplyErrors.Add(1)
	}
}

func netAddrFromString(s string) net.Addr {
	addr, err := net.ResolveTCPAddr("tcp", s)
	if err != nil {
		return nil
	}
	return addr
}
