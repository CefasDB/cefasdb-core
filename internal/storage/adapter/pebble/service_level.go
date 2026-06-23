package pebble

import (
	"context"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// ServiceLevelSharesResolver resolves a service level name to the DRR
// share weight used by the Pebble read/write lanes.
type ServiceLevelSharesResolver func(name string) (int, error)

// AttachServiceLevelSharesResolver wires a lazy catalog-backed resolver
// for non-default service-level shares.
func (d *DB) AttachServiceLevelSharesResolver(fn ServiceLevelSharesResolver) {
	if d == nil {
		return
	}
	d.slSharesMu.Lock()
	d.slSharesResolver = fn
	d.slSharesMu.Unlock()
	d.clearServiceLevelShares()
}

// InvalidateServiceLevelShares drops the cached shares for name. An
// empty name clears the whole cache.
func (d *DB) InvalidateServiceLevelShares(name string) {
	if d == nil {
		return
	}
	if name == "" {
		d.clearServiceLevelShares()
		return
	}
	d.slSharesCache.Delete(name)
}

func (d *DB) runReadCtx(ctx context.Context, fn func() error) error {
	sl, shares := d.serviceLevelLane(ctx)
	return d.runReadSL(sl, shares, fn)
}

func (d *DB) runWriteCtx(ctx context.Context, fn func() error) error {
	sl, shares := d.serviceLevelLane(ctx)
	return d.runWriteSL(sl, shares, fn)
}

func (d *DB) serviceLevelLane(ctx context.Context) (string, int) {
	if ctx == nil {
		ctx = context.Background()
	}
	sl := auth.ServiceLevelFromContext(ctx)
	return sl, d.serviceLevelShares(sl)
}

func (d *DB) serviceLevelShares(name string) int {
	if name == "" || name == types.DefaultServiceLevelName {
		return 1
	}
	if v, ok := d.slSharesCache.Load(name); ok {
		if shares, ok := v.(int); ok && shares > 0 {
			return shares
		}
	}
	resolver := d.serviceLevelSharesResolver()
	if resolver == nil {
		return 1
	}
	shares, err := resolver(name)
	if err != nil || shares <= 0 {
		shares = 1
	}
	d.slSharesCache.Store(name, shares)
	return shares
}

func (d *DB) serviceLevelSharesResolver() ServiceLevelSharesResolver {
	if d == nil {
		return nil
	}
	d.slSharesMu.RLock()
	defer d.slSharesMu.RUnlock()
	return d.slSharesResolver
}

func (d *DB) clearServiceLevelShares() {
	d.slSharesCache.Range(func(key, _ any) bool {
		d.slSharesCache.Delete(key)
		return true
	})
}
