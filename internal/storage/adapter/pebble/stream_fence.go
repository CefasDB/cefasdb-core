package pebble

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// Native-stream committed high-water mark (FU-01).
//
// A native-stream writer (text indexes + stream enabled) registers an
// in-flight reservation immediately before it can allocate a change
// sequence and releases it after its commit has returned. The reservation
// carries a floor: the allocator value + 1 observed under fence.mu. Every
// sequence the writer allocates is >= that floor because the allocator is
// monotonic and the writer only allocates after registering.
//
// The committed watermark is
//
//	min(floor of every live reservation) - 1, or the allocator value if none.
//
// Readers of native tables bound their snapshot at the watermark computed
// before the snapshot is taken, so a cursor can never advance past a native
// sequence that was allocated but not yet committed, whatever table committed
// a later sequence. Writers no longer serialize: they commit concurrently and
// group-commit through commitLoop/Replicate. Registration never blocks, so the
// primary-key lock order (ascending atomicMu shards) is the only lock order.

// ErrReadUnavailable is returned when a stream read cannot establish its
// consistency point before the caller's context ends. It is retryable; the
// HTTP stream surface maps it to 503 and gRPC to Unavailable, like a leader
// change.
var ErrReadUnavailable = errors.New("cefas/storage: stream read consistency point unavailable")

// ReadWaitError carries the context error that ended a read wait.
type ReadWaitError struct{ Cause error }

func (e *ReadWaitError) Error() string {
	if e.Cause == nil {
		return ErrReadUnavailable.Error()
	}
	return ErrReadUnavailable.Error() + ": " + e.Cause.Error()
}

func (e *ReadWaitError) Is(target error) bool { return target == ErrReadUnavailable }
func (e *ReadWaitError) Unwrap() error        { return e.Cause }

// defaultReadBarrierTimeout is the pre-existing native read barrier budget.
const defaultReadBarrierTimeout = 5 * time.Second

// fenceHoldBuckets are the upper bounds (seconds) of the hold-time
// histogram: allocation-to-commit time of native-stream writes.
var fenceHoldBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// FenceObserver receives one native-stream reservation hold duration in
// seconds. prometheus.Observer satisfies it.
type FenceObserver interface{ Observe(float64) }

type fenceObserverBox struct{ obs FenceObserver }

type streamFence struct {
	mu       sync.Mutex
	nextTok  uint64
	inflight map[uint64]uint64 // reservation token -> sequence floor

	registrations atomic.Uint64
	observer      atomic.Pointer[fenceObserverBox]
}

// StreamFenceStats is a point-in-time view of the native-stream watermark.
type StreamFenceStats struct {
	InFlight      int
	Registrations uint64
	Allocated     uint64
	Watermark     uint64
}

func isNativeStreamWriter(td types.TableDescriptor) bool {
	return len(td.TextIndexes) > 0 && td.StreamSpecification != nil && td.StreamSpecification.StreamEnabled
}

// beginNativeStreamWrite reserves the next native-stream sequences for td.
// It must be called before appendChangeRecord and released only after the
// batch commit has returned (successfully or not). It never blocks on other
// writers or readers.
func (d *DB) beginNativeStreamWrite(td types.TableDescriptor) func() {
	if !isNativeStreamWriter(td) {
		return func() {}
	}
	f := &d.streamFence
	f.mu.Lock()
	if f.inflight == nil {
		f.inflight = make(map[uint64]uint64)
	}
	f.nextTok++
	tok := f.nextTok
	f.inflight[tok] = d.changeIndex.Load() + 1
	f.mu.Unlock()
	f.registrations.Add(1)
	start := time.Now()
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.inflight, tok)
			f.mu.Unlock()
			if box := f.observer.Load(); box != nil && box.obs != nil {
				box.obs.Observe(time.Since(start).Seconds())
			}
		})
	}
}

// nativeStreamWatermark returns the highest sequence S such that every
// native-stream sequence <= S that was ever allocated has committed or been
// abandoned. Monotonic: a new reservation's floor exceeds the allocator value
// at registration, which bounds every earlier watermark.
func (d *DB) nativeStreamWatermark() uint64 {
	f := &d.streamFence
	f.mu.Lock()
	defer f.mu.Unlock()
	return d.watermarkLocked()
}

func (d *DB) watermarkLocked() uint64 {
	f := &d.streamFence
	allocated := d.changeIndex.Load()
	if len(f.inflight) == 0 {
		return allocated
	}
	lowest := ^uint64(0)
	for _, floor := range f.inflight {
		if floor < lowest {
			lowest = floor
		}
	}
	// floor >= 1, and floor-1 <= allocated because floor was allocated+1
	// at registration and the allocator only grows.
	return lowest - 1
}

// StreamFenceStats reports the native-stream watermark state for metrics.
func (d *DB) StreamFenceStats() StreamFenceStats {
	if d == nil {
		return StreamFenceStats{}
	}
	f := &d.streamFence
	f.mu.Lock()
	defer f.mu.Unlock()
	return StreamFenceStats{
		InFlight:      len(f.inflight),
		Registrations: f.registrations.Load(),
		Allocated:     d.changeIndex.Load(),
		Watermark:     d.watermarkLocked(),
	}
}

// AttachStreamFenceObserver installs (or replaces) the hold-time observer.
// Passing nil disables observation.
func (d *DB) AttachStreamFenceObserver(obs FenceObserver) {
	if d == nil {
		return
	}
	if obs == nil {
		d.streamFence.observer.Store(nil)
		return
	}
	if cur := d.streamFence.observer.Load(); cur != nil && cur.obs == obs {
		return
	}
	d.streamFence.observer.Store(&fenceObserverBox{obs: obs})
}

// FenceHoldBuckets returns the recommended histogram bounds for the
// native-stream hold-time observer.
func FenceHoldBuckets() []float64 { return append([]float64(nil), fenceHoldBuckets...) }

// nativeStreamReadBound resolves the read bound for a native table: the read
// barrier first (honouring ctx), then the watermark. The caller must take
// its Pebble snapshot after this returns. fenced is false for tables without
// text indexes, which keep the historical unfenced read.
func (d *DB) nativeStreamReadBound(ctx context.Context, table string) (bound uint64, fenced bool, err error) {
	td, err := d.visibilityDescriptor(table)
	if err != nil {
		return 0, false, err
	}
	if len(td.TextIndexes) == 0 {
		return 0, false, nil
	}
	if err := d.ReadBarrierCtx(ctx, defaultReadBarrierTimeout); err != nil {
		return 0, false, err
	}
	return d.nativeStreamWatermark(), true, nil
}

// ReadBarrierCtx is ReadBarrier bounded by ctx: a cancelled or expired
// context returns promptly with a *ReadWaitError (errors.Is
// ErrReadUnavailable and the context error).
func (d *DB) ReadBarrierCtx(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return &ReadWaitError{Cause: err}
	}
	if d.repl == nil {
		return nil
	}
	if !d.repl.IsLeader() {
		return &NotLeaderError{LeaderURL: d.repl.LeaderHTTPAddr()}
	}
	barrier, ok := d.repl.(interface{ Barrier(time.Duration) error })
	if !ok {
		return ErrNotLeader
	}
	deadline, hasDeadline := ctx.Deadline()
	shortened := false
	if hasDeadline {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &ReadWaitError{Cause: context.DeadlineExceeded}
		}
		if remaining < timeout {
			timeout, shortened = remaining, true
		}
	}
	if ctx.Done() == nil {
		return barrier.Barrier(timeout)
	}
	done := make(chan error, 1)
	go func() { done <- barrier.Barrier(timeout) }()
	select {
	case err := <-done:
		if err != nil && shortened && !time.Now().Before(deadline.Add(-time.Millisecond)) {
			return &ReadWaitError{Cause: context.DeadlineExceeded}
		}
		return err
	case <-ctx.Done():
		// The barrier goroutine exits within timeout; its result is dropped.
		return &ReadWaitError{Cause: ctx.Err()}
	}
}
