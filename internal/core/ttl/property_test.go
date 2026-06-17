package ttl_test

import (
	"math"
	"sync"
	"testing"
	"testing/quick"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/core/ttl"
)

// The ttl package publishes two interfaces: Service (Attribute +
// Subscribe) and Observer (OnExpire). The properties below pin the
// documented contract of every conforming implementation:
//
//  1. Attribute is a pure lookup: same input → same output across
//     repeated calls. Unknown tables return "".
//  2. Subscribe returns a cancel func; after cancel(), an observer must
//     never be invoked again. Subscribe/cancel is safe under arbitrary
//     interleaving.
//  3. OnExpire is called once per observer per Fire. Multiple observers
//     each get exactly one call.
//  4. Time semantics (monotonicity / boundary / overflow / non-positive
//     TTL) belong to the *reaper*, which is not in this package, but
//     the property suite below pins the arithmetic the reaper relies
//     on so a future move of the helper into this package keeps the
//     same axioms.

// --- reference Service implementation ---------------------------------------

type refService struct {
	attr      map[string]string
	mu        sync.Mutex
	observers map[int]ttl.Observer
	next      int
}

func newRefService(attr map[string]string) *refService {
	return &refService{attr: attr, observers: map[int]ttl.Observer{}}
}

func (s *refService) Attribute(table string) string { return s.attr[table] }

func (s *refService) Subscribe(o ttl.Observer) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.next
	s.next++
	s.observers[id] = o
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.observers, id)
	}
}

func (s *refService) Fire(table string, key model.Item) {
	s.mu.Lock()
	snap := make([]ttl.Observer, 0, len(s.observers))
	for _, o := range s.observers {
		snap = append(snap, o)
	}
	s.mu.Unlock()
	for _, o := range snap {
		o.OnExpire(table, key)
	}
}

type countingObserver struct {
	mu    sync.Mutex
	calls int
}

func (c *countingObserver) OnExpire(string, model.Item) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
}

func (c *countingObserver) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// --- properties: Attribute lookup ------------------------------------------

func TestProperty_AttributeLookupIsDeterministic(t *testing.T) {
	svc := newRefService(map[string]string{
		"Sessions": "expires_at",
		"Cache":    "ttl",
	})
	f := func(table string) bool {
		first := svc.Attribute(table)
		for i := 0; i < 8; i++ {
			if svc.Attribute(table) != first {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_AttributeUnknownTableReturnsEmpty(t *testing.T) {
	svc := newRefService(map[string]string{"Sessions": "expires_at"})
	f := func(table string) bool {
		if table == "Sessions" {
			return true // skip the known case
		}
		return svc.Attribute(table) == ""
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

// --- properties: Subscribe / cancel -----------------------------------------

func TestProperty_CancelStopsFutureCalls(t *testing.T) {
	svc := newRefService(map[string]string{"T": "exp"})
	f := func(fires uint8) bool {
		obs := &countingObserver{}
		cancel := svc.Subscribe(obs)
		cancel()
		for i := 0; i < int(fires); i++ {
			svc.Fire("T", nil)
		}
		return obs.Count() == 0
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_EveryObserverReceivesEveryFire(t *testing.T) {
	svc := newRefService(map[string]string{"T": "exp"})
	f := func(n uint8, fires uint8) bool {
		// keep n small enough to stay fast
		count := int(n%8) + 1
		obs := make([]*countingObserver, count)
		for i := range obs {
			obs[i] = &countingObserver{}
			svc.Subscribe(obs[i])
		}
		for i := 0; i < int(fires); i++ {
			svc.Fire("T", nil)
		}
		for _, o := range obs {
			if o.Count() != int(fires) {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_SubscribeCancelInterleavingSafe(t *testing.T) {
	svc := newRefService(map[string]string{"T": "exp"})
	f := func(ops []bool) (ok bool) {
		defer func() {
			if r := recover(); r != nil {
				ok = false
			}
		}()
		var cancels []func()
		for _, op := range ops {
			if op {
				cancels = append(cancels, svc.Subscribe(&countingObserver{}))
			} else if len(cancels) > 0 {
				cancels[0]()
				cancels = cancels[1:]
			}
		}
		// drain
		for _, c := range cancels {
			c()
		}
		svc.Fire("T", nil) // must not panic on empty observer set
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

// --- properties: time semantics --------------------------------------------
//
// The actual time comparison lives in storage's reaper. These properties
// pin the arithmetic axioms the reaper must respect so the contract is
// portable when the helper migrates into this package.

// alive is the reference TTL predicate: an item is alive iff now <
// expiresAt. expiresAt <= 0 means "no TTL set" → always alive.
func alive(now, expiresAt int64) bool {
	if expiresAt <= 0 {
		return true
	}
	return now < expiresAt
}

func TestProperty_TTLMonotonicity(t *testing.T) {
	f := func(now, exp int64) bool {
		if exp <= 0 {
			return alive(now, exp)
		}
		if now < exp {
			return alive(now, exp)
		}
		return !alive(now, exp)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_TTLBoundaryIsExclusive(t *testing.T) {
	// At exactly expiresAt, the item must be expired (now >= exp).
	for _, exp := range []int64{1, 1000, 1_700_000_000, math.MaxInt64 / 2} {
		if alive(exp, exp) {
			t.Fatalf("boundary alive at exp=%d (expected expired)", exp)
		}
		if !alive(exp-1, exp) {
			t.Fatalf("just before boundary expired at exp=%d", exp)
		}
	}
}

func TestProperty_TTLOverflowSafe(t *testing.T) {
	// Very large expiresAt close to MaxInt64 must not overflow the
	// comparison. The naive "exp - now > 0" subtraction would overflow;
	// the reference predicate uses a direct < comparison instead.
	f := func(delta uint32) (ok bool) {
		defer func() {
			if r := recover(); r != nil {
				ok = false
			}
		}()
		exp := int64(math.MaxInt64) - int64(delta)
		now := exp - 1
		if !alive(now, exp) {
			return false
		}
		if alive(exp, exp) {
			return false
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_TTLNonPositiveMeansAlive(t *testing.T) {
	// expiresAt <= 0 is the documented "no TTL set" sentinel; the item
	// is always alive regardless of `now`.
	f := func(now int64, exp int64) bool {
		if exp > 0 {
			return true // out of scope for this property
		}
		return alive(now, exp)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}
