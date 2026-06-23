package pebble

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CefasDb/cefasdb/pkg/types"
)

const (
	defaultLaneWriteWorkers = 8
	defaultLaneWriteQueue   = 2048

	// Read lane floors. The actual defaults scale with GOMAXPROCS via
	// defaultLaneReadWorkers / defaultLaneReadQueue below — the prior
	// hardcoded 8 capped a 24-shard / 512-reader bench at 192 in-flight
	// reads and starved mixed-read.
	minLaneReadWorkers = 16
	minLaneReadQueue   = 4096

	// perSLQueueSize bounds how many jobs may park behind a single
	// service level before submit blocks. Set generously so that the
	// dispatcher, not the submit channel, is the throttle point.
	perSLQueueSize = 1024
)

func defaultLaneReadWorkers() int {
	n := runtime.GOMAXPROCS(0) * 2
	if n < minLaneReadWorkers {
		return minLaneReadWorkers
	}
	return n
}

func defaultLaneReadQueue() int {
	n := runtime.GOMAXPROCS(0) * 256
	if n < minLaneReadQueue {
		return minLaneReadQueue
	}
	return n
}

type laneJob struct {
	enqueued time.Time
	run      func() error
	done     chan error
}

// slQueue is the per-service-level FIFO that feeds the DRR
// scheduler. shares decides the deficit increment per round; the
// dispatcher accumulates deficit and drains up to deficit jobs per
// round before yielding. shares is atomic so ensureQueue can
// refresh it concurrently with the dispatcher loop. deficit stays
// dispatcher-local — only the dispatcher goroutine reads or writes.
type slQueue struct {
	name    string
	shares  atomic.Int32
	deficit int
	jobs    chan laneJob

	queued atomic.Int64
	served atomic.Uint64
}

type laneExecutor struct {
	name        string
	workers     int
	fastDefault chan laneJob // single-queue fast path when only "default" SL is in use

	mu       sync.RWMutex
	queues   map[string]*slQueue
	drrOrder []string

	dispatch  chan laneJob // dispatcher → workers
	refreshCh chan struct{}
	closeCh   chan struct{}

	closed atomic.Bool
	wg     sync.WaitGroup

	queued      atomic.Int64
	active      atomic.Int64
	ops         atomic.Uint64
	queueWaitNs atomic.Uint64
}

type dbLanes struct {
	read  *laneExecutor
	write *laneExecutor
}

// LaneSnapshot is a point-in-time view of one DB lane. QueueWaitNs is
// cumulative so Prometheus can sample it as a monotonic counter-like
// gauge. ServiceLevels lists the per-SL queue state when DRR is
// active; it is empty when the fast path is in use.
type LaneSnapshot struct {
	Lane          string
	Workers       int
	QueueDepth    int64
	Active        int64
	Ops           uint64
	QueueWaitNs   uint64
	ServiceLevels []SLLaneSnapshot
}

// SLLaneSnapshot is the per-SL view inside a LaneSnapshot.
type SLLaneSnapshot struct {
	Name       string
	Shares     int
	QueueDepth int64
	Served     uint64
}

func newDBLanes(opts LaneOptions) *dbLanes {
	if NormalizeLaneMode(opts.Mode) != LaneModeOn {
		return nil
	}
	readWorkers := opts.ReadWorkers
	if readWorkers <= 0 {
		readWorkers = defaultLaneReadWorkers()
	}
	writeWorkers := opts.WriteWorkers
	if writeWorkers <= 0 {
		writeWorkers = defaultLaneWriteWorkers
	}
	readQueue := opts.ReadQueue
	if readQueue <= 0 {
		readQueue = defaultLaneReadQueue()
	}
	writeQueue := opts.WriteQueue
	if writeQueue <= 0 {
		writeQueue = defaultLaneWriteQueue
	}
	return &dbLanes{
		read:  newLaneExecutor("read", readWorkers, readQueue),
		write: newLaneExecutor("write", writeWorkers, writeQueue),
	}
}

func newLaneExecutor(name string, workers, fastQueue int) *laneExecutor {
	l := &laneExecutor{
		name:        name,
		workers:     workers,
		fastDefault: make(chan laneJob, fastQueue),
		queues:      make(map[string]*slQueue),
		dispatch:    make(chan laneJob, workers*2),
		refreshCh:   make(chan struct{}, 1),
		closeCh:     make(chan struct{}),
	}
	l.wg.Add(workers + 1)
	go l.dispatcher()
	for i := 0; i < workers; i++ {
		go l.worker()
	}
	return l
}

// dispatcher implements the deficit-round-robin scheduler when more
// than one service level is registered. In the single-SL fast path
// it just forwards from fastDefault to the worker channel — the
// degenerate case is FIFO, identical in throughput to the old
// single-queue executor.
func (l *laneExecutor) dispatcher() {
	defer l.wg.Done()
	for {
		// Always drain the fast-default channel first: it is the
		// path for un-tagged callers and for the implicit "default"
		// SL, and every closed-channel signal lands here.
		select {
		case job, ok := <-l.fastDefault:
			if !ok {
				// fastDefault closed → close dispatch channel and
				// stop. Worker loops will drain remaining work then
				// exit.
				close(l.dispatch)
				return
			}
			l.dispatch <- job
			continue
		case <-l.closeCh:
			close(l.dispatch)
			return
		default:
		}

		l.mu.RLock()
		snap := make([]*slQueue, 0, len(l.queues))
		for _, name := range l.drrOrder {
			snap = append(snap, l.queues[name])
		}
		l.mu.RUnlock()

		if len(snap) == 0 {
			// Block until a job hits fastDefault or a new SL queue
			// is registered.
			select {
			case job, ok := <-l.fastDefault:
				if !ok {
					close(l.dispatch)
					return
				}
				l.dispatch <- job
			case <-l.refreshCh:
			case <-l.closeCh:
				close(l.dispatch)
				return
			}
			continue
		}

		served := false
		for _, q := range snap {
			q.deficit += int(q.shares.Load())
			for q.deficit > 0 {
				select {
				case job := <-q.jobs:
					q.queued.Add(-1)
					select {
					case l.dispatch <- job:
						q.deficit--
						q.served.Add(1)
						served = true
					case <-l.closeCh:
						close(l.dispatch)
						return
					}
				default:
					// Empty SL queue — drop the unused deficit
					// rather than carry it forward so a long-idle
					// queue cannot accumulate a burst quota.
					q.deficit = 0
					goto nextSL
				}
			}
		nextSL:
		}
		if !served {
			select {
			case job, ok := <-l.fastDefault:
				if !ok {
					close(l.dispatch)
					return
				}
				l.dispatch <- job
			case <-l.refreshCh:
			case <-l.closeCh:
				close(l.dispatch)
				return
			}
		}
	}
}

func (l *laneExecutor) worker() {
	defer l.wg.Done()
	for job := range l.dispatch {
		l.queued.Add(-1)
		l.active.Add(1)
		l.queueWaitNs.Add(uint64(time.Since(job.enqueued).Nanoseconds()))
		err := job.run()
		l.ops.Add(1)
		l.active.Add(-1)
		job.done <- err
	}
}

// submit enqueues fn under the implicit default service level with
// shares=1. Equivalent to the historical submit(fn) entry point.
func (l *laneExecutor) submit(fn func() error) error {
	return l.submitSL(types.DefaultServiceLevelName, 1, fn)
}

// submitSL enqueues fn under the named service level. shares is the
// DRR weight; non-default SLs allocate (and reuse) a per-SL queue
// on first call. Submitting under "default" with shares=1 stays on
// the fast path.
func (l *laneExecutor) submitSL(slName string, shares int, fn func() error) error {
	if l == nil {
		return fn()
	}
	if l.closed.Load() {
		return fmt.Errorf("db lane %s closed", l.name)
	}
	job := laneJob{
		enqueued: time.Now(),
		run:      fn,
		done:     make(chan error, 1),
	}
	l.queued.Add(1)
	if slName == "" || slName == types.DefaultServiceLevelName {
		select {
		case l.fastDefault <- job:
		case <-l.closeCh:
			l.queued.Add(-1)
			return fmt.Errorf("db lane %s closed", l.name)
		}
		return <-job.done
	}
	q := l.ensureQueue(slName, shares)
	q.queued.Add(1)
	select {
	case q.jobs <- job:
	case <-l.closeCh:
		l.queued.Add(-1)
		q.queued.Add(-1)
		return fmt.Errorf("db lane %s closed", l.name)
	}
	// Nudge the dispatcher if it is parked waiting for work.
	select {
	case l.refreshCh <- struct{}{}:
	default:
	}
	return <-job.done
}

func (l *laneExecutor) ensureQueue(slName string, shares int) *slQueue {
	if shares <= 0 {
		shares = 1
	}
	l.mu.RLock()
	q, ok := l.queues[slName]
	l.mu.RUnlock()
	if ok {
		q.shares.Store(int32(shares))
		return q
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if q, ok := l.queues[slName]; ok {
		q.shares.Store(int32(shares))
		return q
	}
	q = &slQueue{
		name: slName,
		jobs: make(chan laneJob, perSLQueueSize),
	}
	q.shares.Store(int32(shares))
	l.queues[slName] = q
	l.drrOrder = append(l.drrOrder, slName)
	sort.Strings(l.drrOrder)
	return q
}

func (l *laneExecutor) Close() {
	if l == nil {
		return
	}
	if !l.closed.CompareAndSwap(false, true) {
		return
	}
	close(l.closeCh)
	close(l.fastDefault)
	l.wg.Wait()
}

func (l *laneExecutor) snapshot() LaneSnapshot {
	if l == nil {
		return LaneSnapshot{}
	}
	out := LaneSnapshot{
		Lane:        l.name,
		Workers:     l.workers,
		QueueDepth:  l.queued.Load(),
		Active:      l.active.Load(),
		Ops:         l.ops.Load(),
		QueueWaitNs: l.queueWaitNs.Load(),
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.queues) == 0 {
		return out
	}
	out.ServiceLevels = make([]SLLaneSnapshot, 0, len(l.queues))
	for _, name := range l.drrOrder {
		q := l.queues[name]
		out.ServiceLevels = append(out.ServiceLevels, SLLaneSnapshot{
			Name:       q.name,
			Shares:     int(q.shares.Load()),
			QueueDepth: q.queued.Load(),
			Served:     q.served.Load(),
		})
	}
	return out
}

func (l *dbLanes) Close() {
	if l == nil {
		return
	}
	l.read.Close()
	l.write.Close()
}

func (d *DB) runRead(fn func() error) error {
	return d.runReadSL(types.DefaultServiceLevelName, 1, fn)
}

func (d *DB) runWrite(fn func() error) error {
	return d.runWriteSL(types.DefaultServiceLevelName, 1, fn)
}

// runReadSL submits fn under the named service level. Callers with
// a request context should resolve the SL via auth.ServiceLevelFromContext
// and the shares via the catalog before calling.
func (d *DB) runReadSL(sl string, shares int, fn func() error) error {
	if d == nil || d.lanes == nil || d.lanes.read == nil {
		return fn()
	}
	return d.lanes.read.submitSL(sl, shares, fn)
}

// runWriteSL mirrors runReadSL for the write lane.
func (d *DB) runWriteSL(sl string, shares int, fn func() error) error {
	if d == nil || d.lanes == nil || d.lanes.write == nil {
		return fn()
	}
	return d.lanes.write.submitSL(sl, shares, fn)
}

func (d *DB) LaneStats() []LaneSnapshot {
	if d == nil || d.lanes == nil {
		return nil
	}
	return []LaneSnapshot{
		d.lanes.read.snapshot(),
		d.lanes.write.snapshot(),
	}
}
