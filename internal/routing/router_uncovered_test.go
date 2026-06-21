package routing

import (
	"errors"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/placement"
)

// TestRouterReturnsErrorForUncoveredToken pins the post-panic
// behaviour of Router.ShardForUint64: when the active catalog leaves a
// token uncovered by every shard range, the call must return
// ErrNoShardForToken instead of panicking in production.
func TestRouterReturnsErrorForUncoveredToken(t *testing.T) {
	t.Parallel()

	r := &Router{
		catalog: placement.PlacementCatalog{
			Version:  placement.PlacementVersion,
			Epoch:    42,
			Strategy: placement.PlacementStrategyTokenRange,
			Shards: []placement.ShardPlacement{
				{ID: 0, State: placement.ShardStateActive, Epoch: 42, Ranges: []placement.TokenRange{{Start: 0, End: 100}}},
			},
		},
		normalRanges: []routeRange{
			{shardID: 0, rng: placement.TokenRange{Start: 0, End: 100}},
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
		catalog: placement.PlacementCatalog{
			Version:  placement.PlacementVersion,
			Epoch:    7,
			Strategy: placement.PlacementStrategyTokenRange,
			Shards: []placement.ShardPlacement{
				{ID: 0, State: placement.ShardStateActive, Epoch: 7, Ranges: []placement.TokenRange{{Start: 1, End: 2}}},
			},
		},
		normalRanges: []routeRange{
			{shardID: 0, rng: placement.TokenRange{Start: 1, End: 2}},
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

	r, err := NewRouterFromCatalog(placement.PlacementCatalog{
		Version:  placement.PlacementVersion,
		Epoch:    1,
		Strategy: placement.PlacementStrategyLegacyModulo,
		Shards: []placement.ShardPlacement{
			{ID: 0, State: placement.ShardStateActive, Epoch: 1},
			{ID: 1, State: placement.ShardStateActive, Epoch: 1},
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
