package pebble

import (
	"testing"

	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestProfileTuningCanBeOverridden(t *testing.T) {
	opts := newPebbleOptions(Options{
		Profile: ProfileWriteHeavy,
		Tuning: PebbleTuning{
			MemTableSizeBytes:        32 << 20,
			MaxConcurrentCompactions: 3,
		},
	})
	if opts.MemTableSize != 32<<20 {
		t.Fatalf("MemTableSize = %d, want %d", opts.MemTableSize, uint64(32<<20))
	}
	if got := opts.MaxConcurrentCompactions(); got != 3 {
		t.Fatalf("MaxConcurrentCompactions = %d, want 3", got)
	}
	if opts.Cache == nil {
		t.Fatal("profile should configure a block cache")
	}
}

func TestEvaluatePressureThresholds(t *testing.T) {
	opts := BackpressureOptions{
		Enabled:                     true,
		WarningL0Files:              10,
		CriticalL0Files:             20,
		WarningCompactionDebtBytes:  100,
		CriticalCompactionDebtBytes: 200,
		WarningReadAmp:              5,
		CriticalReadAmp:             9,
	}
	state, reason := EvaluatePressure(PebbleMetricsView{L0Files: 12}, opts)
	if state != PressureWarning || reason != "l0_files" {
		t.Fatalf("warning pressure = (%d,%q)", state, reason)
	}
	state, reason = EvaluatePressure(PebbleMetricsView{CompactionDebtBytes: 220}, opts)
	if state != PressureCritical || reason != "compaction_debt" {
		t.Fatalf("critical pressure = (%d,%q)", state, reason)
	}
}

func TestCompactTableCoversTableRange(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ks := types.KeySchema{PK: "id"}
	if err := db.PutItem("events", ks, types.Item{"id": {T: types.AttrS, S: "a"}}); err != nil {
		t.Fatal(err)
	}
	res, err := db.CompactTable("events", false)
	if err != nil {
		t.Fatal(err)
	}
	lower, upper := storage.PrefixTable("events")
	if string(res.Lower) != string(lower) || string(res.Upper) != string(upper) {
		t.Fatalf("range = %q..%q, want %q..%q", res.Lower, res.Upper, lower, upper)
	}
	if res.Elapsed < 0 {
		t.Fatalf("elapsed = %s", res.Elapsed)
	}
}
