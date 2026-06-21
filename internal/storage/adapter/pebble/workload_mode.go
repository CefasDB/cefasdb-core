package pebble

import (
	"sync"
	"sync/atomic"
	"time"
)

// WorkloadMode classifies the rolling read/write mix seen by a DB.
// The classifier transitions only after two consecutive observation
// windows agree, so a transient burst does not retune the loop.
type WorkloadMode int32

const (
	ModeUnknown WorkloadMode = iota
	ModeIdle
	ModeReadHeavy
	ModeWriteHeavy
	ModeMixed
)

func (m WorkloadMode) String() string {
	switch m {
	case ModeIdle:
		return "idle"
	case ModeReadHeavy:
		return "read-heavy"
	case ModeWriteHeavy:
		return "write-heavy"
	case ModeMixed:
		return "mixed"
	default:
		return "unknown"
	}
}

// workloadTuning is the set of knobs the tuner adjusts per mode.
// Stored atomically so commitLoop and the retention loop can read it
// without taking a lock.
type workloadTuning struct {
	mergeLimit           atomic.Int32
	retentionIntervalNS  atomic.Int64
	mode                 atomic.Int32
	transitions          atomic.Uint64
	lastObservedWriteOPS atomic.Uint64
	lastObservedReadOPS  atomic.Uint64
}

// WorkloadModeSnapshot is a point-in-time view, for observability.
type WorkloadModeSnapshot struct {
	Mode                string
	MergeLimit          int32
	RetentionIntervalMS int64
	Transitions         uint64
	WriteOpsPerSec      uint64
	ReadOpsPerSec       uint64
}

// workloadMode owns the observer, classifier and tuner. It is opt-in
// via Options.AdaptiveMode; when disabled the DB never allocates one
// and the hot path keeps reading the static defaults.
type workloadMode struct {
	writeCounter atomic.Uint64
	readCounter  atomic.Uint64

	tuning workloadTuning

	stopCh  chan struct{}
	stopped chan struct{}

	// classifier state — owned by the goroutine, no lock needed.
	pendingMode WorkloadMode
	pendingHits int
}

const (
	workloadWindow              = 1 * time.Second
	workloadIdleOpsPerSec       = 100
	workloadReadHeavyRatio      = 5.0
	workloadWriteHeavyRatio     = 5.0
	workloadTransitionThreshold = 2

	// Mode-specific tuning targets. Pebble compaction tunables cannot be
	// changed at runtime; these only adjust knobs the CefasDB layer owns.
	mergeLimitIdle       int32 = 16
	mergeLimitReadHeavy  int32 = 32
	mergeLimitWriteHeavy int32 = 192
	mergeLimitMixed      int32 = 64

	retentionIdleMS       int64 = 60000
	retentionReadHeavyMS  int64 = 60000
	retentionWriteHeavyMS int64 = 90000
	retentionMixedMS      int64 = 30000
)

func newWorkloadMode(defaultRetention time.Duration) *workloadMode {
	wm := &workloadMode{
		stopCh:      make(chan struct{}),
		stopped:     make(chan struct{}),
		pendingMode: ModeUnknown,
	}
	// Start in MIXED — neutral defaults until the first window observes.
	wm.tuning.mode.Store(int32(ModeMixed))
	wm.tuning.mergeLimit.Store(mergeLimitMixed)
	wm.tuning.retentionIntervalNS.Store(defaultRetention.Nanoseconds())
	return wm
}

func (w *workloadMode) start() {
	go w.loop()
}

func (w *workloadMode) stop() {
	if w == nil {
		return
	}
	select {
	case <-w.stopCh:
		return
	default:
		close(w.stopCh)
	}
	<-w.stopped
}

// recordWrite/recordRead are called by the hot path. With AdaptiveMode
// off these are never called (DB.workload is nil) and the hot path pays
// zero overhead.
func (w *workloadMode) recordWrite() { w.writeCounter.Add(1) }
func (w *workloadMode) recordRead()  { w.readCounter.Add(1) }

// MergeLimit returns the current adaptive cap for commitLoop. Falls
// back to the static default when the workloadMode pointer is nil.
func (w *workloadMode) MergeLimit() int {
	if w == nil {
		return defaultMergeBatch
	}
	v := w.tuning.mergeLimit.Load()
	if v < minMergeBatch {
		return minMergeBatch
	}
	if v > maxMergeBatchCap {
		return maxMergeBatchCap
	}
	return int(v)
}

// RetentionInterval returns the current adaptive retention loop tick.
// Falls back to the static option when nil.
func (w *workloadMode) RetentionInterval(static time.Duration) time.Duration {
	if w == nil {
		return static
	}
	ns := w.tuning.retentionIntervalNS.Load()
	if ns <= 0 {
		return static
	}
	return time.Duration(ns)
}

// Snapshot returns a copy of the current tuning state for metrics /
// debugging. Safe to call concurrently.
func (w *workloadMode) Snapshot() WorkloadModeSnapshot {
	if w == nil {
		return WorkloadModeSnapshot{Mode: ModeUnknown.String()}
	}
	mode := WorkloadMode(w.tuning.mode.Load())
	return WorkloadModeSnapshot{
		Mode:                mode.String(),
		MergeLimit:          w.tuning.mergeLimit.Load(),
		RetentionIntervalMS: w.tuning.retentionIntervalNS.Load() / int64(time.Millisecond),
		Transitions:         w.tuning.transitions.Load(),
		WriteOpsPerSec:      w.tuning.lastObservedWriteOPS.Load(),
		ReadOpsPerSec:       w.tuning.lastObservedReadOPS.Load(),
	}
}

// loop runs the observe → classify → tune cycle once per
// workloadWindow. Goroutine-local state (pendingMode/pendingHits) keeps
// hysteresis decisions out of the atomic path.
func (w *workloadMode) loop() {
	defer close(w.stopped)
	t := time.NewTicker(workloadWindow)
	defer t.Stop()
	var prevWrite, prevRead uint64
	for {
		select {
		case <-w.stopCh:
			return
		case <-t.C:
			curWrite := w.writeCounter.Load()
			curRead := w.readCounter.Load()
			deltaWrite := curWrite - prevWrite
			deltaRead := curRead - prevRead
			prevWrite, prevRead = curWrite, curRead

			w.tuning.lastObservedWriteOPS.Store(deltaWrite)
			w.tuning.lastObservedReadOPS.Store(deltaRead)

			next := classifyWorkload(deltaWrite, deltaRead)
			w.applyClassification(next)
		}
	}
}

func classifyWorkload(writeOps, readOps uint64) WorkloadMode {
	total := writeOps + readOps
	if total < workloadIdleOpsPerSec {
		return ModeIdle
	}
	if readOps == 0 || (writeOps > 0 && float64(writeOps)/float64(readOps) > workloadWriteHeavyRatio) {
		return ModeWriteHeavy
	}
	if writeOps == 0 || float64(readOps)/float64(writeOps) > workloadReadHeavyRatio {
		return ModeReadHeavy
	}
	return ModeMixed
}

func (w *workloadMode) applyClassification(next WorkloadMode) {
	current := WorkloadMode(w.tuning.mode.Load())
	if next == current {
		w.pendingMode = ModeUnknown
		w.pendingHits = 0
		return
	}
	if next != w.pendingMode {
		w.pendingMode = next
		w.pendingHits = 1
		return
	}
	w.pendingHits++
	if w.pendingHits < workloadTransitionThreshold {
		return
	}
	// Commit the transition.
	w.pendingMode = ModeUnknown
	w.pendingHits = 0
	w.tuning.mode.Store(int32(next))
	w.tuning.transitions.Add(1)
	w.applyTuningForMode(next)
}

func (w *workloadMode) applyTuningForMode(mode WorkloadMode) {
	switch mode {
	case ModeIdle:
		w.tuning.mergeLimit.Store(mergeLimitIdle)
		w.tuning.retentionIntervalNS.Store(retentionIdleMS * int64(time.Millisecond))
	case ModeReadHeavy:
		w.tuning.mergeLimit.Store(mergeLimitReadHeavy)
		w.tuning.retentionIntervalNS.Store(retentionReadHeavyMS * int64(time.Millisecond))
	case ModeWriteHeavy:
		w.tuning.mergeLimit.Store(mergeLimitWriteHeavy)
		w.tuning.retentionIntervalNS.Store(retentionWriteHeavyMS * int64(time.Millisecond))
	case ModeMixed:
		w.tuning.mergeLimit.Store(mergeLimitMixed)
		w.tuning.retentionIntervalNS.Store(retentionMixedMS * int64(time.Millisecond))
	}
}

// Guard against accidental copies of workloadTuning.
var _ = sync.Mutex{}
