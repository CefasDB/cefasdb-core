package server

import (
	"errors"
	"sync"
	"sync/atomic"

	"golang.org/x/time/rate"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// ErrSLQuotaExceeded is the sentinel returned by SLQuotaController.Begin
// when a request would breach the service level's MaxInFlight or rate
// caps. The interceptor maps it to codes.ResourceExhausted with a
// message naming the SL so clients can react.
var ErrSLQuotaExceeded = errors.New("service level quota exceeded")

// SLQuotaCatalog is the slice of catalog.Catalog the quota controller
// needs. Decoupled via interface so tests can drive it without a
// pebble store.
type SLQuotaCatalog interface {
	GetServiceLevel(name string) (types.ServiceLevelDescriptor, error)
}

// SLThrottleObserver is called when a request is rejected by the
// quota controller. Receivers typically increment a metric counter
// labelled by service_level. Optional — nil is fine.
type SLThrottleObserver func(serviceLevel, reason string)

// SLQuotaController enforces per-service-level caps:
//
//   - MaxInFlight    — concurrent requests admitted to handlers.
//   - MaxRowsPerSec  — token-bucket rate cap on row-rate (RPS).
//   - MaxBytesPerSec — token-bucket rate cap on byte-rate (approx).
//
// Phase 4 of #489: the interceptor calls Begin once per request after
// the SL has been resolved (#497). Begin either returns a release
// func (defer on success) or ErrSLQuotaExceeded.
//
// Hot reload: subscribes to SLQuotaCatalog's update channel via
// Invalidate; the next Begin for that SL rebuilds the bucket from
// the fresh descriptor.
type SLQuotaController struct {
	catalog  SLQuotaCatalog
	observer SLThrottleObserver

	buckets sync.Map // string -> *slBucket
}

type slBucket struct {
	name string

	maxInFlight    int
	maxRowsPerSec  int64
	maxBytesPerSec int64

	rowsLimiter  *rate.Limiter
	bytesLimiter *rate.Limiter

	inFlight atomic.Int64

	// version is bumped on Invalidate so a subsequent Begin can
	// discard the cached bucket and rebuild from the catalog.
	version atomic.Uint64
	stale   atomic.Bool
}

// NewSLQuotaController wires a quota controller against a catalog.
// observer may be nil; it is invoked exactly once per rejected
// request with the offending SL name and the cap that fired
// ("max_in_flight" / "max_rows_per_sec" / "max_bytes_per_sec").
func NewSLQuotaController(cat SLQuotaCatalog, observer SLThrottleObserver) *SLQuotaController {
	return &SLQuotaController{catalog: cat, observer: observer}
}

// Begin admits a request under the named service level. On success
// the caller must invoke the returned release function (typically
// via defer) so in-flight accounting decrements.
//
// Default SL has no caps and short-circuits to a no-op release.
func (c *SLQuotaController) Begin(slName string) (release func(), err error) {
	if c == nil {
		return func() {}, nil
	}
	if slName == "" || slName == types.DefaultServiceLevelName {
		return func() {}, nil
	}
	bucket, err := c.bucketFor(slName)
	if err != nil {
		return func() {}, nil
	}
	if bucket == nil {
		return func() {}, nil
	}

	if bucket.maxInFlight > 0 {
		cur := bucket.inFlight.Add(1)
		if cur > int64(bucket.maxInFlight) {
			bucket.inFlight.Add(-1)
			c.fireObserver(slName, "max_in_flight")
			return func() {}, ErrSLQuotaExceeded
		}
		release = func() { bucket.inFlight.Add(-1) }
	} else {
		release = func() {}
	}

	if bucket.rowsLimiter != nil && !bucket.rowsLimiter.Allow() {
		release()
		c.fireObserver(slName, "max_rows_per_sec")
		return func() {}, ErrSLQuotaExceeded
	}
	if bucket.bytesLimiter != nil && !bucket.bytesLimiter.Allow() {
		release()
		c.fireObserver(slName, "max_bytes_per_sec")
		return func() {}, ErrSLQuotaExceeded
	}
	return release, nil
}

// Invalidate marks the cached bucket for slName as stale. The next
// Begin call rebuilds it from the current catalog descriptor.
// Called from the catalog's SL-update callback so caps land in <1s.
func (c *SLQuotaController) Invalidate(slName string) {
	if v, ok := c.buckets.Load(slName); ok {
		v.(*slBucket).stale.Store(true)
	}
}

// bucketFor returns the cached bucket for slName, rebuilding it on
// miss or when stale. Returns nil when the SL exists but declares no
// caps (no-throttle short-circuit).
func (c *SLQuotaController) bucketFor(slName string) (*slBucket, error) {
	if v, ok := c.buckets.Load(slName); ok {
		bucket := v.(*slBucket)
		if !bucket.stale.Load() {
			return bucket, nil
		}
	}
	sl, err := c.catalog.GetServiceLevel(slName)
	if err != nil {
		return nil, err
	}
	if sl.MaxInFlight <= 0 && sl.MaxRowsPerSec <= 0 && sl.MaxBytesPerSec <= 0 {
		c.buckets.Delete(slName)
		return nil, nil
	}
	bucket := &slBucket{
		name:           slName,
		maxInFlight:    sl.MaxInFlight,
		maxRowsPerSec:  sl.MaxRowsPerSec,
		maxBytesPerSec: sl.MaxBytesPerSec,
	}
	if sl.MaxRowsPerSec > 0 {
		burst := int(sl.MaxRowsPerSec)
		if burst < 1 {
			burst = 1
		}
		bucket.rowsLimiter = rate.NewLimiter(rate.Limit(sl.MaxRowsPerSec), burst)
	}
	if sl.MaxBytesPerSec > 0 {
		burst := int(sl.MaxBytesPerSec)
		if burst < 1 {
			burst = 1
		}
		bucket.bytesLimiter = rate.NewLimiter(rate.Limit(sl.MaxBytesPerSec), burst)
	}
	bucket.version.Store(1)
	c.buckets.Store(slName, bucket)
	return bucket, nil
}

func (c *SLQuotaController) fireObserver(sl, reason string) {
	if c.observer != nil {
		c.observer(sl, reason)
	}
}
