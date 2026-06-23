package server

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/pkg/types"
)

type fakeQuotaCatalog struct {
	get func(name string) (types.ServiceLevelDescriptor, error)
}

func (f *fakeQuotaCatalog) GetServiceLevel(name string) (types.ServiceLevelDescriptor, error) {
	return f.get(name)
}

func TestQuotaController_DefaultSLNoCaps(t *testing.T) {
	q := NewSLQuotaController(&fakeQuotaCatalog{
		get: func(name string) (types.ServiceLevelDescriptor, error) {
			return types.ServiceLevelDescriptor{}, nil
		},
	}, nil)
	release, err := q.Begin(types.DefaultServiceLevelName)
	if err != nil {
		t.Fatalf("Begin default: %v", err)
	}
	release()
}

func TestQuotaController_MaxInFlightRejects(t *testing.T) {
	cat := &fakeQuotaCatalog{
		get: func(name string) (types.ServiceLevelDescriptor, error) {
			return types.ServiceLevelDescriptor{Name: name, MaxInFlight: 2}, nil
		},
	}
	var throttles atomic.Int64
	observer := func(sl, reason string) {
		if reason == "max_in_flight" {
			throttles.Add(1)
		}
	}
	q := NewSLQuotaController(cat, observer)

	// Acquire 2 slots; both should pass.
	r1, err := q.Begin("olap")
	if err != nil {
		t.Fatalf("Begin 1: %v", err)
	}
	r2, err := q.Begin("olap")
	if err != nil {
		t.Fatalf("Begin 2: %v", err)
	}
	// Third should hit the cap.
	if _, err := q.Begin("olap"); !errors.Is(err, ErrSLQuotaExceeded) {
		t.Fatalf("Begin 3 = %v, want ErrSLQuotaExceeded", err)
	}
	if got := throttles.Load(); got != 1 {
		t.Errorf("throttle observer = %d, want 1", got)
	}
	r1()
	// Now there is room.
	r3, err := q.Begin("olap")
	if err != nil {
		t.Fatalf("Begin after release: %v", err)
	}
	r2()
	r3()
}

func TestQuotaController_RowsPerSecRejects(t *testing.T) {
	cat := &fakeQuotaCatalog{
		get: func(name string) (types.ServiceLevelDescriptor, error) {
			return types.ServiceLevelDescriptor{Name: name, MaxRowsPerSec: 5}, nil
		},
	}
	q := NewSLQuotaController(cat, nil)

	// Burst is sized at the rate (=5) by our constructor; first 5
	// in fast succession pass, 6th hits the cap.
	for i := 0; i < 5; i++ {
		r, err := q.Begin("batch")
		if err != nil {
			t.Fatalf("Begin %d: %v", i, err)
		}
		r()
	}
	if _, err := q.Begin("batch"); !errors.Is(err, ErrSLQuotaExceeded) {
		t.Fatalf("Begin past burst = %v, want ErrSLQuotaExceeded", err)
	}
}

func TestQuotaController_InvalidateRefreshes(t *testing.T) {
	var maxInFlight atomic.Int64
	maxInFlight.Store(2)
	cat := &fakeQuotaCatalog{
		get: func(name string) (types.ServiceLevelDescriptor, error) {
			return types.ServiceLevelDescriptor{Name: name, MaxInFlight: int(maxInFlight.Load())}, nil
		},
	}
	q := NewSLQuotaController(cat, nil)

	// Initial cap = 2.
	r1, _ := q.Begin("olap")
	r2, _ := q.Begin("olap")
	if _, err := q.Begin("olap"); !errors.Is(err, ErrSLQuotaExceeded) {
		t.Fatalf("pre-bump: 3rd Begin = %v", err)
	}
	r1()
	r2()

	// Bump the cap and invalidate.
	maxInFlight.Store(5)
	q.Invalidate("olap")

	// Now 5 should fit.
	releases := []func(){}
	for i := 0; i < 5; i++ {
		r, err := q.Begin("olap")
		if err != nil {
			t.Fatalf("post-bump Begin %d: %v", i, err)
		}
		releases = append(releases, r)
	}
	if _, err := q.Begin("olap"); !errors.Is(err, ErrSLQuotaExceeded) {
		t.Fatalf("post-bump: 6th Begin = %v, want ErrSLQuotaExceeded", err)
	}
	for _, r := range releases {
		r()
	}
}

func TestQuotaController_PausedRejectsWithErrSLPaused(t *testing.T) {
	cat := &fakeQuotaCatalog{
		get: func(name string) (types.ServiceLevelDescriptor, error) {
			return types.ServiceLevelDescriptor{Name: name, Shares: 10, Paused: true}, nil
		},
	}
	var pauseEvents atomic.Int64
	observer := func(sl, reason string) {
		if reason == "paused" {
			pauseEvents.Add(1)
		}
	}
	q := NewSLQuotaController(cat, observer)
	if _, err := q.Begin("olap"); !errors.Is(err, ErrSLPaused) {
		t.Fatalf("paused Begin = %v, want ErrSLPaused", err)
	}
	if got := pauseEvents.Load(); got != 1 {
		t.Errorf("observer count = %d, want 1", got)
	}
}

func TestQuotaController_PausedSurvivesHotReload(t *testing.T) {
	var paused atomic.Bool
	cat := &fakeQuotaCatalog{
		get: func(name string) (types.ServiceLevelDescriptor, error) {
			return types.ServiceLevelDescriptor{
				Name:   name,
				Shares: 10,
				Paused: paused.Load(),
			}, nil
		},
	}
	q := NewSLQuotaController(cat, nil)

	// Pre-pause: requests pass.
	r, err := q.Begin("batch")
	if err != nil {
		t.Fatalf("pre-pause Begin: %v", err)
	}
	r()

	// Pause via catalog + invalidate.
	paused.Store(true)
	q.Invalidate("batch")

	if _, err := q.Begin("batch"); !errors.Is(err, ErrSLPaused) {
		t.Fatalf("post-pause Begin = %v, want ErrSLPaused", err)
	}

	// Resume + invalidate.
	paused.Store(false)
	q.Invalidate("batch")
	if _, err := q.Begin("batch"); err != nil {
		t.Fatalf("post-resume Begin: %v", err)
	}
}

func TestQuotaController_SLWithoutCapsShortCircuits(t *testing.T) {
	cat := &fakeQuotaCatalog{
		get: func(name string) (types.ServiceLevelDescriptor, error) {
			return types.ServiceLevelDescriptor{Name: name, Shares: 10}, nil
		},
	}
	q := NewSLQuotaController(cat, nil)

	for i := 0; i < 1000; i++ {
		r, err := q.Begin("noisy")
		if err != nil {
			t.Fatalf("Begin %d: %v", i, err)
		}
		r()
	}
	_ = time.Second // satisfy import
}
