package server

import (
	"context"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// mvScheduler iterates SCHEDULED materialized views, invoking
// refreshComplete when each view's interval has elapsed since the
// last refresh.
//
// One goroutine per server. Tick cadence is fixed at 30s; this is
// the smallest window an operator can ask for is REFRESH EVERY 30
// SECONDS, anything finer would burn CPU on busy polling. ON_DEMAND
// views are not scheduled — they only refresh via the explicit RPC.
//
// The scheduler delegates the single-flight guard to
// refreshSingleFlight (defined alongside refreshComplete), so a
// long-running refresh blocks subsequent scheduled triggers for the
// same view without blocking other views.
type mvScheduler struct {
	srv      *GRPCServer
	interval time.Duration

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func newMVScheduler(srv *GRPCServer, interval time.Duration) *mvScheduler {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &mvScheduler{srv: srv, interval: interval, stopCh: make(chan struct{})}
}

// Start kicks off the scheduler goroutine. Safe to call once.
func (m *mvScheduler) Start(ctx context.Context) {
	m.wg.Add(1)
	go m.loop(ctx)
}

// Stop cancels the scheduler. Idempotent.
func (m *mvScheduler) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
}

func (m *mvScheduler) loop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

func (m *mvScheduler) tick(ctx context.Context) {
	if m.srv == nil || m.srv.cat == nil {
		return
	}
	now := time.Now().Unix()
	for _, mv := range m.srv.cat.ListViews("") {
		if mv.RefreshPolicy.Mode != types.RefreshModeScheduled {
			continue
		}
		if mv.Status == types.MVStatusPaused {
			continue
		}
		if !dueForRefreshUnix(mv, now) {
			continue
		}
		// Fire-and-forget: refreshComplete has its own single-flight
		// guard. Errors are surfaced via mv.Status = failed.
		go func(name string) {
			_, _ = m.srv.refreshComplete(ctx, name)
		}(mv.Name)
	}
}

// dueForRefresh decides whether the view's interval has elapsed
// since the last successful refresh. A view that has never been
// refreshed (LastRefreshAtUnix == 0) fires immediately; otherwise
// we wait at least IntervalSeconds.
func dueForRefreshUnix(mv types.MaterializedViewDescriptor, nowUnix int64) bool {
	if mv.RefreshPolicy.IntervalSeconds <= 0 {
		return false
	}
	if mv.LastRefreshAtUnix == 0 {
		return true
	}
	return nowUnix-mv.LastRefreshAtUnix >= mv.RefreshPolicy.IntervalSeconds
}
