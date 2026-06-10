package plugin

import (
	"context"
	"fmt"
	"sync"
)

// Lifecycle is an optional interface plugins implement when they need
// startup / shutdown hooks (warm a cache, flush buffered state, close
// a network handle). Plugins that don't need it just don't implement
// it; the Manager skips them.
type Lifecycle interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// Manager invokes Start / Stop hooks across every Lifecycle plugin in
// a Registry. Ordering is registration-deterministic via List(); Stop
// runs in reverse for symmetry with idiomatic resource ordering.
type Manager struct {
	r       *Registry
	mu      sync.Mutex
	started []Lifecycle
}

// NewManager wires a manager to a registry. v1 supports a single
// manager per registry.
func NewManager(r *Registry) *Manager {
	return &Manager{r: r}
}

// Start invokes Start on every Lifecycle plugin. The first error
// aborts the sweep + leaves successfully-started plugins running so
// the caller can decide whether to Stop them.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.r.List() {
		lc, ok := p.(Lifecycle)
		if !ok {
			continue
		}
		if err := lc.Start(ctx); err != nil {
			return fmt.Errorf("plugin %q start: %w", p.Manifest().Name, err)
		}
		m.started = append(m.started, lc)
	}
	return nil
}

// Stop invokes Stop on every previously-started Lifecycle plugin, in
// reverse start order. Errors are collected; every Stop runs even if
// an earlier one failed (best-effort drain).
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for i := len(m.started) - 1; i >= 0; i-- {
		if err := m.started[i].Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.started = nil
	return firstErr
}
