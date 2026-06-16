package cluster

import (
	"errors"
	"strings"
	"testing"
)

// TestRouterReturnsErrorForUncoveredToken pins the post-panic
// behaviour of Router.ShardForUint64: when the active catalog leaves a
// token uncovered by every shard range, the call must return
// ErrNoShardForToken instead of panicking in production.
//
// ValidatePlacement enforces full token coverage at construction time,
// so the only way to reach the defensive branch in normal operation is
// state corruption — to exercise it deterministically the test
// bypasses NewRouterFromCatalog and assembles the Router fields
// directly. This lives in package cluster (not cluster_test) precisely
// so we can drive that internal path without weakening the public
// constructor.
func TestRouterReturnsErrorForUncoveredToken(t *testing.T) {
	t.Parallel()

	r := &Router{
		catalog: PlacementCatalog{
			Version:  PlacementVersion,
			Epoch:    42,
			Strategy: PlacementStrategyTokenRange,
			Shards: []ShardPlacement{
				{ID: 0, State: ShardStateActive, Epoch: 42, Ranges: []TokenRange{{Start: 0, End: 100}}},
			},
		},
		ranges: []routeRange{
			{shardID: 0, rng: TokenRange{Start: 0, End: 100}},
		},
	}

	id, err := r.ShardForUint64(500)
	if err == nil {
		t.Fatalf("expected error for uncovered token, got shard %d", id)
	}
	if !errors.Is(err, ErrNoShardForToken) {
		t.Fatalf("error = %v, want wraps ErrNoShardForToken", err)
	}
	if !strings.Contains(err.Error(), "epoch=42") || !strings.Contains(err.Error(), "token=500") {
		t.Fatalf("error message missing epoch/token diagnostics: %v", err)
	}
}

// TestRouterShardForPKPropagatesUncoveredError confirms that the
// pkBytes-based wrapper bubbles the uncovered-token error up unchanged
// so callers can react with errors.Is.
func TestRouterShardForPKPropagatesUncoveredError(t *testing.T) {
	t.Parallel()

	r := &Router{
		catalog: PlacementCatalog{
			Version:  PlacementVersion,
			Epoch:    7,
			Strategy: PlacementStrategyTokenRange,
			Shards: []ShardPlacement{
				{ID: 0, State: ShardStateActive, Epoch: 7, Ranges: []TokenRange{{Start: 1, End: 2}}},
			},
		},
		ranges: []routeRange{
			{shardID: 0, rng: TokenRange{Start: 1, End: 2}},
		},
	}

	if _, err := r.ShardForPK([]byte("anything")); !errors.Is(err, ErrNoShardForToken) {
		t.Fatalf("error = %v, want wraps ErrNoShardForToken", err)
	}
}

// TestRouterLegacyModuloStillReturnsNoError keeps the happy-path
// invariant for the legacy modulo strategy where every token is
// covered by definition.
func TestRouterLegacyModuloStillReturnsNoError(t *testing.T) {
	t.Parallel()

	r, err := NewRouterFromCatalog(PlacementCatalog{
		Version:  PlacementVersion,
		Epoch:    1,
		Strategy: PlacementStrategyLegacyModulo,
		Shards: []ShardPlacement{
			{ID: 0, State: ShardStateActive, Epoch: 1},
			{ID: 1, State: ShardStateActive, Epoch: 1},
		},
	})
	if err != nil {
		t.Fatalf("NewRouterFromCatalog: %v", err)
	}
	for _, tok := range []uint64{0, 1, 1 << 31, ^uint64(0)} {
		id, err := r.ShardForUint64(tok)
		if err != nil {
			t.Fatalf("ShardForUint64(%d): %v", tok, err)
		}
		if id > 1 {
			t.Fatalf("ShardForUint64(%d) = %d, want 0 or 1", tok, id)
		}
	}
}
