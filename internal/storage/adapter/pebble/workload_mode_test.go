package pebble

import (
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestClassifyWorkload(t *testing.T) {
	cases := []struct {
		name        string
		write, read uint64
		want        WorkloadMode
	}{
		{"idle", 10, 5, ModeIdle},
		{"write heavy ratio", 10000, 100, ModeWriteHeavy},
		{"write only", 5000, 0, ModeWriteHeavy},
		{"read heavy ratio", 100, 10000, ModeReadHeavy},
		{"read only", 0, 5000, ModeReadHeavy},
		{"mixed", 1000, 800, ModeMixed},
	}
	for _, c := range cases {
		if got := classifyWorkload(c.write, c.read); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestWorkloadModeHysteresisHoldsBeforeTransition(t *testing.T) {
	w := newWorkloadMode(30 * time.Second)
	// Start: ModeMixed.
	if got := WorkloadMode(w.tuning.mode.Load()); got != ModeMixed {
		t.Fatalf("initial mode = %s, want mixed", got)
	}
	// One observation in IDLE does not transition.
	w.applyClassification(ModeIdle)
	if got := WorkloadMode(w.tuning.mode.Load()); got != ModeMixed {
		t.Fatalf("after 1 IDLE: mode = %s, want mixed (hysteresis holds)", got)
	}
	// Two consecutive IDLE observations transition.
	w.applyClassification(ModeIdle)
	if got := WorkloadMode(w.tuning.mode.Load()); got != ModeIdle {
		t.Fatalf("after 2 IDLE: mode = %s, want idle", got)
	}
	if got := w.tuning.mergeLimit.Load(); got != mergeLimitIdle {
		t.Fatalf("mergeLimit = %d, want %d (idle)", got, mergeLimitIdle)
	}
}

func TestWorkloadModeAlternatingResetsPending(t *testing.T) {
	w := newWorkloadMode(30 * time.Second)
	// 1× IDLE, then 1× WRITE_HEAVY, then 1× IDLE — none persists.
	w.applyClassification(ModeIdle)
	w.applyClassification(ModeWriteHeavy)
	w.applyClassification(ModeIdle)
	if got := WorkloadMode(w.tuning.mode.Load()); got != ModeMixed {
		t.Fatalf("alternating: mode = %s, want mixed (no transition committed)", got)
	}
}

func TestWorkloadModeRecordsCountersWhenEnabled(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		AdaptiveMode:    true,
		StreamRetention: StreamRetentionOptions{Interval: -1},
	})
	if db.workload == nil {
		t.Fatal("AdaptiveMode=true: db.workload must be non-nil")
	}
	td := types.TableDescriptor{Name: "X", KeySchema: types.KeySchema{PK: "id"}}
	if err := db.PutItemWith(td, types.Item{"id": streamS("k")}, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := db.Get([]byte("nope")); err != nil && err != ErrNotFound {
		t.Fatalf("get: %v", err)
	}
	if w := db.workload.writeCounter.Load(); w == 0 {
		t.Fatal("write counter did not increment")
	}
	if r := db.workload.readCounter.Load(); r == 0 {
		t.Fatal("read counter did not increment")
	}
}

func TestWorkloadModeOffSkipsObserver(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		StreamRetention: StreamRetentionOptions{Interval: -1},
	})
	if db.workload != nil {
		t.Fatal("AdaptiveMode=false: db.workload must be nil (zero overhead)")
	}
	snap := db.WorkloadSnapshot()
	if snap.Mode != "unknown" {
		t.Fatalf("snapshot mode = %q, want unknown", snap.Mode)
	}
}

func TestWorkloadModeSnapshotShape(t *testing.T) {
	w := newWorkloadMode(30 * time.Second)
	w.applyClassification(ModeWriteHeavy)
	w.applyClassification(ModeWriteHeavy)
	snap := w.Snapshot()
	if snap.Mode != "write-heavy" {
		t.Fatalf("mode = %q, want write-heavy", snap.Mode)
	}
	if snap.MergeLimit != mergeLimitWriteHeavy {
		t.Fatalf("mergeLimit = %d, want %d", snap.MergeLimit, mergeLimitWriteHeavy)
	}
	if snap.Transitions != 1 {
		t.Fatalf("transitions = %d, want 1", snap.Transitions)
	}
}
