// Audience store: durable, replication-friendly backing for the
// dedup + freqcap state the audience plugin exposes. Two layouts
// live in the same store:
//
//	__audience__/<id>/d/<scope>/<key>           → 8-byte expireUnixNano  (dedup row)
//	__audience__/<id>/f/<scope>/<key>/<tsUnix>  → 8-byte expireUnixNano  (freqcap hit)
//
// The `id` segment partitions multiple audience plugin instances on
// the same storage backend; ephemeral mode skips storage entirely.
//
// Why a value-resident expiry instead of the engine's TTL pointer
// layout? The engine reaper sweeps by table descriptor (#core/ttl);
// audience rows are plugin-private and shouldn't appear in the
// catalog. Keeping the expiry inline lets a small audience-store
// reaper drop stale rows without polluting the table list, and
// presence checks short-circuit on read for rows whose pointer
// hasn't been swept yet.
package audience

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Backend is the minimal storage surface the durable audience store
// needs. *storage.DB satisfies it via a thin adapter (see
// PebbleBackend); tests use MemoryBackend.
type Backend interface {
	Get(key []byte) (value []byte, ok bool, err error)
	Set(key, value []byte) error
	Delete(key []byte) error
	// Scan visits every key in [lower, upper) in lexicographic order.
	// Returning false from fn stops the scan. fn must not retain
	// key/value past the call — copy if needed.
	Scan(lower, upper []byte, fn func(key, value []byte) bool) error
}

// StoreMode picks the persistence behaviour for an audience plugin.
type StoreMode string

const (
	// ModeEphemeral keeps state in process memory and is the legacy
	// behaviour. Use for dev + tests.
	ModeEphemeral StoreMode = "ephemeral"
	// ModeDurable persists state via a Backend (typically Pebble +
	// Raft) and survives restarts.
	ModeDurable StoreMode = "durable"
)

// StoreOptions controls a durable audience store.
type StoreOptions struct {
	// ID disambiguates rows when multiple audience plugins share a
	// backend. Required for ModeDurable; ignored otherwise.
	ID string
	// Backend supplies the storage layer. Required for ModeDurable.
	Backend Backend
	// SweepInterval controls how often the reaper goroutine evicts
	// expired rows. Defaults to 60s. The plugin business path never
	// sweeps inline; expired rows are filtered out at read time.
	SweepInterval time.Duration
	// SweepBatch caps the number of rows the reaper deletes per
	// sweep. Defaults to 1024.
	SweepBatch int
	// Now overrides the wall clock for tests.
	Now func() time.Time
}

// Store is the dedup + freqcap durable backing. Safe for concurrent
// use. Construction is decoupled from the Plugin so tests can drive
// the reaper deterministically.
type Store struct {
	mode StoreMode
	id   string
	be   Backend
	now  func() time.Time

	// Ephemeral fast-path state. Used iff mode == ModeEphemeral.
	memMu    sync.Mutex
	memDedup map[string]int64   // bucket → expireUnixNano
	memFreq  map[string][]int64 // bucket → hit timestamps (nanos)

	sweepCfg sweepCfg
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once

	// Observable counters surfaced to operators.
	dedupRows int64
	freqRows  int64
	reaperLag int64 // nanoseconds: now - oldestSweptExpiry
}

type sweepCfg struct {
	interval time.Duration
	batch    int
}

// ErrBackendRequired is returned when ModeDurable is used without a
// Backend; callers must wire one in StoreOptions.
var ErrBackendRequired = errors.New("audience: durable mode needs a Backend")

// NewMemoryStore returns an ephemeral Store that retains the v1
// in-memory behaviour. Construction always succeeds.
func NewMemoryStore(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{
		mode:     ModeEphemeral,
		now:      now,
		memDedup: map[string]int64{},
		memFreq:  map[string][]int64{},
	}
}

// NewDurableStore wires a Store onto a Backend. The reaper is not
// started automatically — call Start(ctx) when you want background
// sweeping. ID + Backend must be non-empty.
func NewDurableStore(opts StoreOptions) (*Store, error) {
	if opts.Backend == nil {
		return nil, ErrBackendRequired
	}
	if opts.ID == "" {
		return nil, fmt.Errorf("audience: durable store needs an ID")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	interval := opts.SweepInterval
	if interval <= 0 {
		interval = 60 * time.Second
	}
	batch := opts.SweepBatch
	if batch <= 0 {
		batch = 1024
	}
	return &Store{
		mode:     ModeDurable,
		id:       opts.ID,
		be:       opts.Backend,
		now:      now,
		sweepCfg: sweepCfg{interval: interval, batch: batch},
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}, nil
}

// Mode reports the persistence mode this store runs in.
func (s *Store) Mode() StoreMode { return s.mode }

// Start launches the background reaper goroutine. Call exactly
// once per Store; pair with Stop. The audience plugin itself never
// sweeps — this goroutine is the only place that physically removes
// expired rows.
func (s *Store) Start(ctx context.Context) {
	if s == nil || s.mode != ModeDurable {
		return
	}
	go func() {
		defer close(s.stopped)
		t := time.NewTicker(s.sweepCfg.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case <-t.C:
				_ = s.Sweep(ctx)
			}
		}
	}()
}

// Stop signals the reaper to exit. Safe to call multiple times.
func (s *Store) Stop() {
	if s == nil || s.mode != ModeDurable {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stop)
		<-s.stopped
	})
}

// ---------- Dedup ----------

// CheckDedup is the gating call: returns true when (scope, key) is
// new in the TTL window and records the entry. Returns false if a
// live row already exists.
func (s *Store) CheckDedup(scope, key string, ttl time.Duration) (bool, error) {
	if scope == "" || key == "" {
		return false, fmt.Errorf("audience: dedup needs scope + key")
	}
	if ttl <= 0 {
		return false, fmt.Errorf("audience: dedup ttl must be positive")
	}
	now := s.now()
	expire := now.Add(ttl)
	bucket := scope + "/" + key

	if s.mode == ModeEphemeral {
		s.memMu.Lock()
		defer s.memMu.Unlock()
		if exp, ok := s.memDedup[bucket]; ok && exp > now.UnixNano() {
			return false, nil
		}
		s.memDedup[bucket] = expire.UnixNano()
		return true, nil
	}

	k := s.dedupKey(scope, key)
	v, ok, err := s.be.Get(k)
	if err != nil {
		return false, err
	}
	if ok {
		exp := decodeExpiry(v)
		if exp > now.UnixNano() {
			return false, nil
		}
	}
	if err := s.be.Set(k, encodeExpiry(expire.UnixNano())); err != nil {
		return false, err
	}
	if !ok {
		atomic.AddInt64(&s.dedupRows, 1)
	}
	return true, nil
}

// ---------- FreqCap ----------

// CheckFreqCap admits a hit when the count over the window stays at
// or below `limit`. Returns false when the new hit would cross the
// cap. The hit is durably recorded when allowed.
func (s *Store) CheckFreqCap(scope, key string, limit int, window time.Duration) (bool, error) {
	if scope == "" || key == "" {
		return false, fmt.Errorf("audience: freqcap needs scope + key")
	}
	if limit <= 0 || window <= 0 {
		return false, fmt.Errorf("audience: freqcap limit + window must be positive")
	}
	now := s.now()
	cutoff := now.Add(-window).UnixNano()
	bucket := scope + "/" + key

	if s.mode == ModeEphemeral {
		s.memMu.Lock()
		defer s.memMu.Unlock()
		hits := s.memFreq[bucket]
		keep := hits[:0]
		for _, t := range hits {
			if t > cutoff {
				keep = append(keep, t)
			}
		}
		if len(keep) >= limit {
			s.memFreq[bucket] = keep
			return false, nil
		}
		keep = append(keep, now.UnixNano())
		s.memFreq[bucket] = keep
		return true, nil
	}

	// Durable path: count live hits in the window via a prefix scan,
	// then insert a new row keyed by tsNano. Expiry = now+window so
	// the reaper can drop the row once it's outside any future window.
	lower, upper := s.freqPrefix(scope, key)
	count := 0
	err := s.be.Scan(lower, upper, func(k, v []byte) bool {
		if decodeExpiry(v) <= now.UnixNano() {
			return true
		}
		ts, ok := parseFreqKey(k)
		if !ok {
			return true
		}
		if ts > cutoff {
			count++
			if count >= limit {
				return false
			}
		}
		return true
	})
	if err != nil {
		return false, err
	}
	if count >= limit {
		return false, nil
	}
	hitKey := s.freqKey(scope, key, now.UnixNano())
	if err := s.be.Set(hitKey, encodeExpiry(now.Add(window).UnixNano())); err != nil {
		return false, err
	}
	atomic.AddInt64(&s.freqRows, 1)
	return true, nil
}

// Sweep evicts expired rows. Exposed for deterministic tests; the
// reaper goroutine calls it on its own interval.
func (s *Store) Sweep(ctx context.Context) error {
	if s.mode != ModeDurable {
		return nil
	}
	now := s.now().UnixNano()
	prefix := []byte(s.scopePrefix())
	upper := prefixUpperBytes(prefix)
	var victims [][]byte
	var oldest int64
	err := s.be.Scan(prefix, upper, func(k, v []byte) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		exp := decodeExpiry(v)
		if exp <= now {
			if oldest == 0 || exp < oldest {
				oldest = exp
			}
			cp := make([]byte, len(k))
			copy(cp, k)
			victims = append(victims, cp)
			if len(victims) >= s.sweepCfg.batch {
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	for _, k := range victims {
		if err := s.be.Delete(k); err != nil {
			return err
		}
		if isDedupKey(k, s.id) {
			atomic.AddInt64(&s.dedupRows, -1)
		} else {
			atomic.AddInt64(&s.freqRows, -1)
		}
	}
	if oldest > 0 {
		atomic.StoreInt64(&s.reaperLag, now-oldest)
	} else {
		atomic.StoreInt64(&s.reaperLag, 0)
	}
	return nil
}

// Metrics reports counters operators can scrape. The durable mode
// surfaces the actual row counts via atomic counters maintained on
// every mutation; ephemeral mode walks the maps under lock.
type Metrics struct {
	DedupRows int64
	FreqRows  int64
	// ReaperLag is the gap between now and the oldest expiry the
	// last sweep observed, in nanoseconds. Zero when the reaper has
	// caught up.
	ReaperLag time.Duration
}

// Metrics returns a snapshot. Cheap; safe under load.
func (s *Store) Metrics() Metrics {
	if s.mode == ModeEphemeral {
		s.memMu.Lock()
		defer s.memMu.Unlock()
		var freq int64
		for _, h := range s.memFreq {
			freq += int64(len(h))
		}
		return Metrics{
			DedupRows: int64(len(s.memDedup)),
			FreqRows:  freq,
		}
	}
	return Metrics{
		DedupRows: atomic.LoadInt64(&s.dedupRows),
		FreqRows:  atomic.LoadInt64(&s.freqRows),
		ReaperLag: time.Duration(atomic.LoadInt64(&s.reaperLag)),
	}
}

// ---------- key layout ----------

const audiencePrefix = "cefas/__audience__/"

func (s *Store) scopePrefix() string {
	return audiencePrefix + s.id + "/"
}

func (s *Store) dedupKey(scope, key string) []byte {
	return []byte(s.scopePrefix() + "d/" + scope + "/" + key)
}

func (s *Store) freqPrefix(scope, key string) (lower, upper []byte) {
	p := []byte(s.scopePrefix() + "f/" + scope + "/" + key + "/")
	return p, prefixUpperBytes(p)
}

func (s *Store) freqKey(scope, key string, tsNano int64) []byte {
	p := s.scopePrefix() + "f/" + scope + "/" + key + "/"
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(tsNano))
	out := make([]byte, 0, len(p)+8)
	out = append(out, p...)
	out = append(out, b[:]...)
	return out
}

func parseFreqKey(k []byte) (int64, bool) {
	if len(k) < 8 {
		return 0, false
	}
	tail := k[len(k)-8:]
	return int64(binary.BigEndian.Uint64(tail)), true
}

func isDedupKey(k []byte, id string) bool {
	// Layout: cefas/__audience__/<id>/d/...
	prefix := audiencePrefix + id + "/d/"
	return len(k) >= len(prefix) && string(k[:len(prefix)]) == prefix
}

func encodeExpiry(unixNano int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(unixNano))
	return b[:]
}

func decodeExpiry(v []byte) int64 {
	if len(v) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(v[:8]))
}

// prefixUpperBytes returns the smallest key strictly greater than
// every key starting with p. Mirrors storage.prefixUpper to avoid an
// internal import.
func prefixUpperBytes(p []byte) []byte {
	u := make([]byte, len(p))
	copy(u, p)
	for i := len(u) - 1; i >= 0; i-- {
		if u[i] < 0xff {
			u[i]++
			return u[:i+1]
		}
	}
	return nil
}

// ---------- in-memory backend ----------

// MemoryBackend is a Backend implementation that lives in process
// memory. Used by tests and by ephemeral-but-store-backed setups
// when callers want to share scope wiring without persistence.
type MemoryBackend struct {
	mu sync.Mutex
	m  map[string][]byte
}

// NewMemoryBackend returns an empty MemoryBackend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{m: map[string][]byte{}}
}

func (b *MemoryBackend) Get(key []byte) ([]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.m[string(key)]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

func (b *MemoryBackend) Set(key, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	b.m[string(key)] = cp
	return nil
}

func (b *MemoryBackend) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.m, string(key))
	return nil
}

func (b *MemoryBackend) Scan(lower, upper []byte, fn func(k, v []byte) bool) error {
	b.mu.Lock()
	keys := make([]string, 0, len(b.m))
	lo := string(lower)
	hi := string(upper)
	for k := range b.m {
		if k >= lo && (upper == nil || k < hi) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	// Snapshot values to release the lock before fn runs.
	values := make([][]byte, len(keys))
	for i, k := range keys {
		v := b.m[k]
		cp := make([]byte, len(v))
		copy(cp, v)
		values[i] = cp
	}
	b.mu.Unlock()
	for i, k := range keys {
		if !fn([]byte(k), values[i]) {
			return nil
		}
	}
	return nil
}

// Len returns the entry count. Useful for tests.
func (b *MemoryBackend) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.m)
}

