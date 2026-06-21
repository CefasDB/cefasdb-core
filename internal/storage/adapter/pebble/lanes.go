package pebble

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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

type laneExecutor struct {
	name    string
	workers int
	jobs    chan laneJob

	mu     sync.RWMutex
	closed bool
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
// cumulative so Prometheus can sample it as a monotonic counter-like gauge.
type LaneSnapshot struct {
	Lane        string
	Workers     int
	QueueDepth  int64
	Active      int64
	Ops         uint64
	QueueWaitNs uint64
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

func newLaneExecutor(name string, workers, queue int) *laneExecutor {
	l := &laneExecutor{
		name:    name,
		workers: workers,
		jobs:    make(chan laneJob, queue),
	}
	l.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go l.worker()
	}
	return l
}

func (l *laneExecutor) worker() {
	defer l.wg.Done()
	for job := range l.jobs {
		l.queued.Add(-1)
		l.active.Add(1)
		l.queueWaitNs.Add(uint64(time.Since(job.enqueued).Nanoseconds()))
		err := job.run()
		l.ops.Add(1)
		l.active.Add(-1)
		job.done <- err
	}
}

func (l *laneExecutor) submit(fn func() error) error {
	if l == nil {
		return fn()
	}
	job := laneJob{
		enqueued: time.Now(),
		run:      fn,
		done:     make(chan error, 1),
	}
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return fmt.Errorf("db lane %s closed", l.name)
	}
	l.queued.Add(1)
	l.jobs <- job
	l.mu.RUnlock()
	return <-job.done
}

func (l *laneExecutor) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	close(l.jobs)
	l.mu.Unlock()
	l.wg.Wait()
}

func (l *laneExecutor) snapshot() LaneSnapshot {
	if l == nil {
		return LaneSnapshot{}
	}
	return LaneSnapshot{
		Lane:        l.name,
		Workers:     l.workers,
		QueueDepth:  l.queued.Load(),
		Active:      l.active.Load(),
		Ops:         l.ops.Load(),
		QueueWaitNs: l.queueWaitNs.Load(),
	}
}

func (l *dbLanes) Close() {
	if l == nil {
		return
	}
	l.read.Close()
	l.write.Close()
}

func (d *DB) runRead(fn func() error) error {
	if d == nil || d.lanes == nil || d.lanes.read == nil {
		return fn()
	}
	return d.lanes.read.submit(fn)
}

func (d *DB) runWrite(fn func() error) error {
	if d == nil || d.lanes == nil || d.lanes.write == nil {
		return fn()
	}
	return d.lanes.write.submit(fn)
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
