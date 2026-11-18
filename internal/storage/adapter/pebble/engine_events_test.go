package pebble

import (
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
)

func TestEngineEventsCountWriteStalls(t *testing.T) {
	var clock int64
	e := &engineEvents{mono: func() int64 { return clock }}
	l := e.listener()
	l.WriteStallEnd() // unmatched end is ignored
	clock = 100
	l.WriteStallBegin(pebbledb.WriteStallBeginInfo{})
	l.WriteStallBegin(pebbledb.WriteStallBeginInfo{}) // duplicate begin counts once
	clock = 100 + int64(1500*time.Millisecond)
	if s := e.snapshot(); !s.WriteStallActive || s.WriteStalls != 1 || s.WriteStallSeconds != 1.5 {
		t.Fatalf("active stall snapshot = %+v", s)
	}
	l.WriteStallEnd()
	clock += int64(time.Hour)
	if s := e.snapshot(); s.WriteStallActive || s.WriteStalls != 1 || s.WriteStallSeconds != 1.5 {
		t.Fatalf("ended stall snapshot = %+v", s)
	}
	l.DiskSlow(pebbledb.DiskSlowInfo{})
	if s := e.snapshot(); s.DiskSlowEvents != 1 {
		t.Fatalf("disk slow = %+v", s)
	}
	db, err := Open(Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.events == nil || db.EngineEvents() != (EngineEventStats{}) {
		t.Fatalf("Open did not wire engine events: %+v", db.EngineEvents())
	}
}
