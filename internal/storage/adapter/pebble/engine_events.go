package pebble

import (
	"sync/atomic"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
)

// engineEvents accumulates Pebble EventListener signals that point-in-time
// Metrics() cannot show: write stalls (memtable/L0 back-pressure that blocks
// every commit) and slow-disk warnings. All fields are lock-free so the
// listener never adds work to Pebble's commit path.
type engineEvents struct {
	stalls         atomic.Uint64
	stallNanos     atomic.Uint64 // completed stalls only
	stallStartNano atomic.Int64  // monotonic nanos (+1) of the active stall, 0 if none
	diskSlow       atomic.Uint64
	mono           func() int64 // tests override; monotonic nanoseconds
}

var engineEventsBase = time.Now()

// EngineEventStats is the cumulative Pebble event view for metrics.
type EngineEventStats struct {
	WriteStalls       uint64
	WriteStallSeconds float64 // completed stalls plus the active one so far
	WriteStallActive  bool
	DiskSlowEvents    uint64
}

// clock returns monotonic nanoseconds, never 0, so 0 can mean "no stall".
func (e *engineEvents) clock() int64 {
	if e.mono != nil {
		return e.mono() + 1
	}
	return int64(time.Since(engineEventsBase)) + 1
}

func (e *engineEvents) listener() pebbledb.EventListener {
	return pebbledb.EventListener{
		WriteStallBegin: func(pebbledb.WriteStallBeginInfo) { e.stallBegin() },
		WriteStallEnd:   e.stallEnd,
		DiskSlow:        func(pebbledb.DiskSlowInfo) { e.diskSlow.Add(1) },
	}
}

func (e *engineEvents) stallBegin() {
	if e.stallStartNano.CompareAndSwap(0, e.clock()) {
		e.stalls.Add(1)
	}
}

func (e *engineEvents) stallEnd() {
	start := e.stallStartNano.Swap(0)
	if start == 0 {
		return
	}
	if d := e.clock() - start; d > 0 {
		e.stallNanos.Add(uint64(d))
	}
}

func (e *engineEvents) snapshot() EngineEventStats {
	if e == nil {
		return EngineEventStats{}
	}
	nanos := e.stallNanos.Load()
	start := e.stallStartNano.Load()
	active := start != 0
	if active {
		if d := e.clock() - start; d > 0 {
			nanos += uint64(d)
		}
	}
	return EngineEventStats{
		WriteStalls:       e.stalls.Load(),
		WriteStallSeconds: float64(nanos) / float64(time.Second),
		WriteStallActive:  active,
		DiskSlowEvents:    e.diskSlow.Load(),
	}
}

// EngineEvents returns cumulative Pebble stall and slow-disk counters.
func (d *DB) EngineEvents() EngineEventStats {
	if d == nil {
		return EngineEventStats{}
	}
	return d.events.snapshot()
}
