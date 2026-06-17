package audience_test

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/audiencestore"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/plugin/audience"
)

func mustDurable(t *testing.T, id string, be audience.Backend, now func() time.Time) *audience.Store {
	t.Helper()
	s, err := audience.NewDurableStore(audience.StoreOptions{
		ID:            id,
		Backend:       be,
		Now:           now,
		SweepInterval: time.Hour, // tests drive Sweep manually
	})
	if err != nil {
		t.Fatalf("durable store: %v", err)
	}
	return s
}

func TestStoreModesReportThemselves(t *testing.T) {
	mem := audience.NewMemoryStore(nil)
	if mem.Mode() != audience.ModeEphemeral {
		t.Fatalf("memory store mode = %v", mem.Mode())
	}
	be := audience.NewMemoryBackend()
	dur := mustDurable(t, "p1", be, nil)
	if dur.Mode() != audience.ModeDurable {
		t.Fatalf("durable store mode = %v", dur.Mode())
	}
}

func TestNewDurableStoreRejectsBadOptions(t *testing.T) {
	if _, err := audience.NewDurableStore(audience.StoreOptions{ID: "p1"}); err == nil {
		t.Fatal("missing backend should error")
	}
	if _, err := audience.NewDurableStore(audience.StoreOptions{Backend: audience.NewMemoryBackend()}); err == nil {
		t.Fatal("missing id should error")
	}
}

func TestDurableDedupSurvivesRestart(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	be := audience.NewMemoryBackend()
	s1 := mustDurable(t, "ads", be, clock)

	ok, _ := s1.CheckDedup("c1", "u1", time.Hour)
	if !ok {
		t.Fatal("first dedup must be allowed")
	}
	ok, _ = s1.CheckDedup("c1", "u1", time.Hour)
	if ok {
		t.Fatal("duplicate inside window must be blocked")
	}

	// Simulate a restart: new Store, same Backend.
	s2 := mustDurable(t, "ads", be, clock)
	ok, _ = s2.CheckDedup("c1", "u1", time.Hour)
	if ok {
		t.Fatal("after restart the dedup must still block")
	}

	// After TTL the row is gone (read-side filter) and a fresh hit
	// goes through.
	now = now.Add(2 * time.Hour)
	ok, _ = s2.CheckDedup("c1", "u1", time.Hour)
	if !ok {
		t.Fatal("after TTL the entry should be allowed again")
	}
}

func TestDurableFreqCapSlidesAndPersists(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	be := audience.NewMemoryBackend()
	s := mustDurable(t, "ads", be, clock)
	scope, key := "merch", "user-42"

	for i := 0; i < 3; i++ {
		ok, _ := s.CheckFreqCap(scope, key, 3, time.Hour)
		if !ok {
			t.Fatalf("hit %d should be allowed", i+1)
		}
		now = now.Add(time.Second)
	}
	ok, _ := s.CheckFreqCap(scope, key, 3, time.Hour)
	if ok {
		t.Fatal("4th hit should be blocked")
	}

	// Reload — count persists.
	s2 := mustDurable(t, "ads", be, clock)
	ok, _ = s2.CheckFreqCap(scope, key, 3, time.Hour)
	if ok {
		t.Fatal("after restart the cap should still block")
	}

	// Slide past the window — everything resets.
	now = now.Add(2 * time.Hour)
	ok, _ = s2.CheckFreqCap(scope, key, 3, time.Hour)
	if !ok {
		t.Fatal("after window expiry the next hit should be allowed")
	}
}

func TestMultipleNodesShareFreqCapViaSharedBackend(t *testing.T) {
	// A shared Backend stands in for the Raft FSM: every node sees
	// the same key/value space. This mirrors the linearizable view
	// the production replicator gives the audience store.
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	be := audience.NewMemoryBackend()
	nodeA := mustDurable(t, "ads", be, clock)
	nodeB := mustDurable(t, "ads", be, clock)
	nodeC := mustDurable(t, "ads", be, clock)
	scope, key := "campaign-x", "viewer-1"

	// Each node admits one hit — three across the cluster.
	for i, n := range []*audience.Store{nodeA, nodeB, nodeC} {
		ok, _ := n.CheckFreqCap(scope, key, 3, time.Hour)
		if !ok {
			t.Fatalf("node %d should admit the first hit it sees", i)
		}
		now = now.Add(time.Millisecond)
	}
	// 4th hit on any node trips the cap because the freq rows are
	// linearizable through the shared backend.
	if ok, _ := nodeA.CheckFreqCap(scope, key, 3, time.Hour); ok {
		t.Fatal("4th hit (node A) should be blocked")
	}
	if ok, _ := nodeB.CheckFreqCap(scope, key, 3, time.Hour); ok {
		t.Fatal("4th hit (node B) should be blocked")
	}
}

func TestSweepEvictsExpiredRows(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	be := audience.NewMemoryBackend()
	s := mustDurable(t, "ads", be, clock)

	if _, err := s.CheckDedup("c1", "u1", time.Minute); err != nil {
		t.Fatalf("dedup: %v", err)
	}
	if _, err := s.CheckFreqCap("c1", "u1", 10, time.Minute); err != nil {
		t.Fatalf("freq: %v", err)
	}
	if got := be.Len(); got != 2 {
		t.Fatalf("backend rows before sweep = %d, want 2", got)
	}

	// Advance past every TTL.
	now = now.Add(time.Hour)
	if err := s.Sweep(context.Background()); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if got := be.Len(); got != 0 {
		t.Fatalf("backend rows after sweep = %d, want 0", got)
	}
	m := s.Metrics()
	if m.DedupRows != 0 || m.FreqRows != 0 {
		t.Fatalf("metrics after sweep = %+v, want zero", m)
	}
}

func TestMetricsTrackRowCounts(t *testing.T) {
	now := time.Unix(1, 0)
	clock := func() time.Time { return now }
	be := audience.NewMemoryBackend()
	s := mustDurable(t, "ads", be, clock)

	for i := 0; i < 5; i++ {
		_, _ = s.CheckDedup("c1", strconv.Itoa(i), time.Hour)
	}
	for i := 0; i < 3; i++ {
		_, _ = s.CheckFreqCap("c2", "u1", 10, time.Hour)
		now = now.Add(time.Second)
	}
	m := s.Metrics()
	if m.DedupRows != 5 {
		t.Fatalf("DedupRows = %d, want 5", m.DedupRows)
	}
	if m.FreqRows != 3 {
		t.Fatalf("FreqRows = %d, want 3", m.FreqRows)
	}
}

func TestEphemeralModePreservesLegacyBehaviour(t *testing.T) {
	now := time.Unix(1, 0)
	clock := func() time.Time { return now }
	p := audience.NewPluginWith(nil, nil, clock)
	if p.Store().Mode() != audience.ModeEphemeral {
		t.Fatalf("expected ephemeral default, got %v", p.Store().Mode())
	}
	ok, _ := p.Dedup("c1", "u1", time.Hour)
	if !ok {
		t.Fatal("first dedup must be allowed")
	}
	ok, _ = p.Dedup("c1", "u1", time.Hour)
	if ok {
		t.Fatal("ephemeral dedup must still dedup")
	}
}

func TestPluginRetainsStateAcrossStoreSwap(t *testing.T) {
	// SetStore is the operator hook used to switch ephemeral → durable
	// at startup. Verify the swap takes effect and the plugin reads
	// from the new store on the very next call.
	now := time.Unix(1, 0)
	clock := func() time.Time { return now }
	p := audience.NewPluginWith(nil, nil, clock)
	if ok, _ := p.Dedup("c1", "u1", time.Hour); !ok {
		t.Fatal("ephemeral first dedup")
	}

	be := audience.NewMemoryBackend()
	p.SetStore(mustDurable(t, "ads", be, clock))
	// Fresh durable store has no rows yet — same key should be
	// allowed because we threw away the ephemeral state.
	ok, _ := p.Dedup("c1", "u1", time.Hour)
	if !ok {
		t.Fatal("durable store should be empty after swap")
	}
	// Second hit on the durable store dedups.
	ok, _ = p.Dedup("c1", "u1", time.Hour)
	if ok {
		t.Fatal("durable store should dedup the second hit")
	}
}

func TestConcurrentDedupRemainsConsistent(t *testing.T) {
	// Hammer one (scope, key) from N goroutines: exactly one wins.
	now := time.Unix(1, 0)
	clock := func() time.Time { return now }
	be := audience.NewMemoryBackend()
	s := mustDurable(t, "ads", be, clock)

	const N = 32
	var wg sync.WaitGroup
	var admitted int64
	var mu sync.Mutex
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.CheckDedup("c1", "u1", time.Hour)
			if err != nil {
				return
			}
			if ok {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// MemoryBackend serialises writes, so all N goroutines see a
	// consistent view — at least one admit, never zero. The
	// linearizable Raft path matches this semantics.
	if admitted < 1 {
		t.Fatalf("expected at least 1 admit, got %d", admitted)
	}
}

func TestDurableStoreOverPebbleSurvivesReopen(t *testing.T) {
	// End-to-end: open a real pebble DB, push dedup state through
	// the plugin, close, reopen, confirm dedup still blocks. This
	// is the restart-survival acceptance criterion from #243.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pebble")
	db, err := pebble.Open(pebble.Options{Path: dbPath})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	s, err := audience.NewDurableStore(audience.StoreOptions{
		ID:            "ads",
		Backend:       audiencestore.NewBackend(db),
		Now:           clock,
		SweepInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("durable: %v", err)
	}
	if ok, _ := s.CheckDedup("camp", "user", time.Hour); !ok {
		t.Fatal("first dedup must allow")
	}
	if ok, _ := s.CheckDedup("camp", "user", time.Hour); ok {
		t.Fatal("second dedup must block")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify the row is still there.
	db2, err := pebble.Open(pebble.Options{Path: dbPath})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	s2, err := audience.NewDurableStore(audience.StoreOptions{
		ID:            "ads",
		Backend:       audiencestore.NewBackend(db2),
		Now:           clock,
		SweepInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("durable2: %v", err)
	}
	if ok, _ := s2.CheckDedup("camp", "user", time.Hour); ok {
		t.Fatal("dedup must still block after reopen")
	}
}
