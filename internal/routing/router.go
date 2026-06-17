// Package routing maps a partition key to a shard ID using the
// active placement catalog. The Router is purely read-only — it
// consumes a placement.PlacementCatalog snapshot built by the
// cluster Manager and serves ShardForPK / ShardForUint64 lookups
// from a sorted token-range table.
package routing

import (
	"errors"
	"fmt"

	"github.com/cespare/xxhash/v2"

	"github.com/CefasDb/cefasdb/internal/placement"
)

// ErrNoShardForToken is returned by Router.ShardForPK / ShardForUint64
// when the active placement catalog leaves a token uncovered by every
// shard's range. This indicates the catalog is corrupted or out of
// sync with the caller — ValidatePlacement is supposed to guarantee
// full coverage at construction time.
var ErrNoShardForToken = errors.New("routing: no shard owns token under active placement")

// Router maps a partition key (canonical byte form, same input the
// storage layer's pkHash8 consumes) onto a shard ID using the active
// placement catalog.
type Router struct {
	catalog placement.PlacementCatalog
	ranges  []routeRange
}

type routeRange struct {
	shardID uint32
	rng     placement.TokenRange
}

// NewRouter returns a router that distributes keys over `n` shards.
// n == 0 is treated as 1 (single-shard / dev mode).
//
// Invariant guard: DefaultPlacement is internally constructed from
// well-formed inputs and must always pass ValidatePlacement. A panic
// here means DefaultPlacement itself is broken — a programmer error
// that should fail fast at boot, not be returned as a runtime error.
func NewRouter(n int) *Router {
	r, err := NewRouterFromCatalog(placement.DefaultPlacement(n, "", nil, nil, placement.NodeCapacity{}, placement.PlacementStrategyTokenRange))
	if err != nil {
		panic(fmt.Errorf("routing: DefaultPlacement produced an invalid catalog: %w", err))
	}
	return r
}

func NewRouterFromCatalog(cat placement.PlacementCatalog) (*Router, error) {
	cat.Normalize()
	if err := placement.ValidatePlacement(cat); err != nil {
		return nil, err
	}
	r := &Router{catalog: cat.Clone()}
	if cat.Strategy == placement.PlacementStrategyTokenRange {
		for _, sh := range cat.Shards {
			if !sh.State.Routable() {
				continue
			}
			for _, rng := range sh.Ranges {
				r.ranges = append(r.ranges, routeRange{shardID: sh.ID, rng: rng})
			}
		}
	}
	return r, nil
}

// Count returns the configured shard count.
func (r *Router) Count() int {
	if r == nil || len(r.catalog.Shards) == 0 {
		return 0
	}
	return len(r.catalog.Shards)
}

func (r *Router) Catalog() placement.PlacementCatalog {
	if r == nil {
		return placement.PlacementCatalog{}
	}
	return r.catalog.Clone()
}

func (r *Router) Epoch() uint64 {
	if r == nil {
		return 0
	}
	return r.catalog.Epoch
}

func (r *Router) Version() uint64 {
	if r == nil {
		return 0
	}
	return r.catalog.Version
}

func (r *Router) Strategy() string {
	if r == nil {
		return ""
	}
	return r.catalog.Strategy
}

func (r *Router) ShardIDs() []uint32 {
	if r == nil {
		return nil
	}
	out := make([]uint32, 0, len(r.catalog.Shards))
	for _, sh := range r.catalog.Shards {
		out = append(out, sh.ID)
	}
	return out
}

// ShardForPK returns the shard ID owning the supplied partition key
// bytes. The mapping uses xxhash64 tokens, the same hash family the
// storage key encoder already uses for pk_hash8.
//
// Returns ErrNoShardForToken (wrapped with the epoch / token for
// diagnosis) when the active catalog leaves the token uncovered.
func (r *Router) ShardForPK(pkBytes []byte) (uint32, error) {
	return r.ShardForUint64(r.TokenForPK(pkBytes))
}

func (r *Router) TokenForPK(pkBytes []byte) uint64 {
	return xxhash.Sum64(pkBytes)
}

// ShardForUint64 routes a precomputed hash directly. Useful when the
// caller already has the pkHash8 in hand.
//
// Returns ErrNoShardForToken (wrapped with the epoch / token for
// diagnosis) when the active catalog leaves the token uncovered.
func (r *Router) ShardForUint64(h uint64) (uint32, error) {
	if r == nil || len(r.catalog.Shards) == 0 {
		return 0, nil
	}
	if r.catalog.Strategy == placement.PlacementStrategyLegacyModulo {
		if len(r.catalog.Shards) == 1 {
			return 0, nil
		}
		return uint32(h % uint64(len(r.catalog.Shards))), nil
	}
	for _, rr := range r.ranges {
		if rr.rng.Contains(h) {
			return rr.shardID, nil
		}
	}
	return 0, fmt.Errorf("%w: epoch=%d token=%d", ErrNoShardForToken, r.catalog.Epoch, h)
}

// GroupID returns the mux groupID for a shard. Reserved IDs:
//
//	0..shardCount-1 → data shards
//
// The router exposes this as a method so the bootstrap code stays
// honest about which IDs are in use.
func (r *Router) GroupID(shard uint32) uint32 { return shard }
