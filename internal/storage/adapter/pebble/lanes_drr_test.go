package pebble

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLaneExecutor_DRRSharesRatio is the foundation acceptance test
// for #498: two service levels saturate the executor simultaneously
// and the served ratio must match the shares (oltp=80 / olap=20).
//
// The harness keeps both queues saturated for a fixed duration —
// submitters loop until a stop signal fires so neither side drains
// early and the cumulative counters reflect the steady-state DRR
// ratio rather than a tail with one SL running solo.
func TestLaneExecutor_DRRSharesRatio(t *testing.T) {
	const (
		oltpShares      = 80
		olapShares      = 20
		submittersPerSL = 32
		duration        = 600 * time.Millisecond
		tolerance       = 0.08
	)
	l := newLaneExecutor("test", 4, 64)
	defer l.Close()

	var olapServed, oltpServed atomic.Uint64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	work := func(slShares int, slName string, counter *atomic.Uint64) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			err := l.submitSL(slName, slShares, func() error {
				counter.Add(1)
				time.Sleep(200 * time.Microsecond)
				return nil
			})
			if err != nil {
				return
			}
		}
	}

	for i := 0; i < submittersPerSL; i++ {
		wg.Add(2)
		go work(olapShares, "olap", &olapServed)
		go work(oltpShares, "oltp", &oltpServed)
	}
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	total := float64(olapServed.Load() + oltpServed.Load())
	if total == 0 {
		t.Fatal("no jobs served")
	}
	olapRatio := float64(olapServed.Load()) / total
	oltpRatio := float64(oltpServed.Load()) / total
	wantOlap := float64(olapShares) / float64(olapShares+oltpShares)
	wantOltp := float64(oltpShares) / float64(olapShares+oltpShares)

	if abs(olapRatio-wantOlap) > tolerance {
		t.Errorf("olap ratio %.3f off target %.3f (±%.3f)", olapRatio, wantOlap, tolerance)
	}
	if abs(oltpRatio-wantOltp) > tolerance {
		t.Errorf("oltp ratio %.3f off target %.3f (±%.3f)", oltpRatio, wantOltp, tolerance)
	}
}

// TestLaneExecutor_FastPathOnly proves the implicit default SL path
// stays single-queue / no-DRR — the snapshot shows zero per-SL
// queues when only the fast path is exercised, guarding the
// "no regression for un-tagged callers" claim.
func TestLaneExecutor_FastPathOnly(t *testing.T) {
	l := newLaneExecutor("test", 2, 16)
	defer l.Close()
	for i := 0; i < 64; i++ {
		if err := l.submit(func() error { return nil }); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	snap := l.snapshot()
	if len(snap.ServiceLevels) != 0 {
		t.Errorf("ServiceLevels = %v, want empty for fast-path-only traffic", snap.ServiceLevels)
	}
	if snap.Ops != 64 {
		t.Errorf("Ops = %d, want 64", snap.Ops)
	}
}

// TestLaneExecutor_SnapshotIncludesPerSL verifies the per-SL view
// surfaces once a non-default SL has been used.
func TestLaneExecutor_SnapshotIncludesPerSL(t *testing.T) {
	l := newLaneExecutor("test", 1, 16)
	defer l.Close()
	for i := 0; i < 5; i++ {
		if err := l.submitSL("batch", 3, func() error { return nil }); err != nil {
			t.Fatalf("submitSL: %v", err)
		}
	}
	snap := l.snapshot()
	if len(snap.ServiceLevels) != 1 {
		t.Fatalf("ServiceLevels len = %d, want 1", len(snap.ServiceLevels))
	}
	sl := snap.ServiceLevels[0]
	if sl.Name != "batch" || sl.Shares != 3 {
		t.Errorf("SL = %+v", sl)
	}
	if sl.Served < 5 {
		t.Errorf("Served = %d, want >= 5", sl.Served)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
