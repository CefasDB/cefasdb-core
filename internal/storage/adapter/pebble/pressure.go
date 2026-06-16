package pebble

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type PressureState int

const (
	PressureNormal PressureState = iota
	PressureWarning
	PressureCritical
)

var ErrThrottled = errors.New("cefas/storage: write throttled")

type ThrottleError struct {
	State  PressureState
	Reason string
	Delay  time.Duration
}

func (e *ThrottleError) Error() string {
	if e.Reason == "" {
		return ErrThrottled.Error()
	}
	return fmt.Sprintf("%s: %s", ErrThrottled, e.Reason)
}

func (e *ThrottleError) Is(target error) bool { return target == ErrThrottled }

type PressureSnapshot struct {
	Enabled bool
	State   PressureState
	Reason  string
	Delay   time.Duration
}

type backpressureController struct {
	opts BackpressureOptions
	mu   sync.Mutex
	last PressureSnapshot
}

func newBackpressureController(opts BackpressureOptions) backpressureController {
	opts = normalizeBackpressureOptions(opts)
	return backpressureController{
		opts: opts,
		last: PressureSnapshot{Enabled: opts.Enabled},
	}
}

func (b *backpressureController) snapshot() PressureSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.last
}

func (b *backpressureController) evaluate(pm PebbleMetricsView) PressureSnapshot {
	if !b.opts.Enabled {
		return PressureSnapshot{}
	}
	state, reason := EvaluatePressure(pm, b.opts)
	delay := time.Duration(0)
	switch state {
	case PressureWarning:
		delay = b.opts.WarningDelay
	case PressureCritical:
		delay = b.opts.CriticalDelay
	}
	snap := PressureSnapshot{
		Enabled: true,
		State:   state,
		Reason:  reason,
		Delay:   delay,
	}
	b.mu.Lock()
	b.last = snap
	b.mu.Unlock()
	return snap
}

// PebbleMetricsView is the subset of Pebble metrics used by write
// backpressure. It lets tests exercise the threshold logic without
// opening a real Pebble DB.
type PebbleMetricsView struct {
	L0Files             int64
	CompactionDebtBytes uint64
	ReadAmp             int
	CompactionsRunning  int64
}

func (d *DB) currentPressureView() PebbleMetricsView {
	pm := d.Metrics()
	return PebbleMetricsView{
		L0Files:             pm.Levels[0].NumFiles,
		CompactionDebtBytes: pm.Compact.EstimatedDebt,
		ReadAmp:             pm.ReadAmp(),
		CompactionsRunning:  pm.Compact.NumInProgress,
	}
}

func (d *DB) checkWritePressure() error {
	if d == nil || !d.bp.opts.Enabled {
		return nil
	}
	snap := d.bp.evaluate(d.currentPressureView())
	if snap.Delay > 0 {
		time.Sleep(snap.Delay)
	}
	if snap.State == PressureCritical && d.bp.opts.RejectOnCritical {
		return &ThrottleError{State: snap.State, Reason: snap.Reason, Delay: snap.Delay}
	}
	return nil
}

func (d *DB) WritePressure() PressureSnapshot {
	if d == nil || !d.bp.opts.Enabled {
		return PressureSnapshot{}
	}
	return d.bp.evaluate(d.currentPressureView())
}

func EvaluatePressure(pm PebbleMetricsView, opts BackpressureOptions) (PressureState, string) {
	opts = normalizeBackpressureOptions(opts)
	if !opts.Enabled {
		return PressureNormal, ""
	}
	switch {
	case opts.CriticalL0Files > 0 && pm.L0Files >= opts.CriticalL0Files:
		return PressureCritical, "l0_files"
	case opts.CriticalCompactionDebtBytes > 0 && pm.CompactionDebtBytes >= opts.CriticalCompactionDebtBytes:
		return PressureCritical, "compaction_debt"
	case opts.CriticalReadAmp > 0 && pm.ReadAmp >= opts.CriticalReadAmp:
		return PressureCritical, "read_amp"
	case opts.WarningL0Files > 0 && pm.L0Files >= opts.WarningL0Files:
		return PressureWarning, "l0_files"
	case opts.WarningCompactionDebtBytes > 0 && pm.CompactionDebtBytes >= opts.WarningCompactionDebtBytes:
		return PressureWarning, "compaction_debt"
	case opts.WarningReadAmp > 0 && pm.ReadAmp >= opts.WarningReadAmp:
		return PressureWarning, "read_amp"
	default:
		return PressureNormal, ""
	}
}

func BackpressureReasons() []string {
	return []string{"l0_files", "compaction_debt", "read_amp"}
}
