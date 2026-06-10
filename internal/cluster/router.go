// Package cluster contains the multi-Raft routing layer: a Router
// that hashes a request's partition key onto a shard ID, and a
// Manager that owns one storage.DB + raft.DB per shard.
//
// Multi-Raft motivation: every shard runs its own consensus group so
// write throughput scales horizontally. All shards share a single
// MuxAcceptor (one TCP port per node) and the same physical Pebble
// directory tree (one subdir per shard).
package cluster

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Router maps a partition key (canonical byte form, same input the
// storage layer's pkHash8 consumes) onto a shard ID using the active
// placement catalog.
type Router struct {
	catalog PlacementCatalog
	ranges  []routeRange
}

type routeRange struct {
	shardID uint32
	rng     TokenRange
}

// NewRouter returns a router that distributes keys over `n` shards.
// n == 0 is treated as 1 (single-shard / dev mode).
func NewRouter(n int) *Router {
	r, err := NewRouterFromCatalog(DefaultPlacement(n, "", nil, nil, NodeCapacity{}, PlacementStrategyTokenRange))
	if err != nil {
		panic(err)
	}
	return r
}

func NewRouterFromCatalog(cat PlacementCatalog) (*Router, error) {
	cat.normalize()
	if err := ValidatePlacement(cat); err != nil {
		return nil, err
	}
	r := &Router{catalog: cat.Clone()}
	if cat.Strategy == PlacementStrategyTokenRange {
		for _, sh := range cat.Shards {
			if !sh.State.routable() {
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

func (r *Router) Catalog() PlacementCatalog {
	if r == nil {
		return PlacementCatalog{}
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
func (r *Router) ShardForPK(pkBytes []byte) uint32 {
	return r.ShardForUint64(r.TokenForPK(pkBytes))
}

func (r *Router) TokenForPK(pkBytes []byte) uint64 {
	return xxhash.Sum64(pkBytes)
}

// ShardForUint64 routes a precomputed hash directly. Useful when the
// caller already has the pkHash8 in hand.
func (r *Router) ShardForUint64(h uint64) uint32 {
	if r == nil || len(r.catalog.Shards) == 0 {
		return 0
	}
	if r.catalog.Strategy == PlacementStrategyLegacyModulo {
		if len(r.catalog.Shards) == 1 {
			return 0
		}
		return uint32(h % uint64(len(r.catalog.Shards)))
	}
	for _, rr := range r.ranges {
		if rr.rng.Contains(h) {
			return rr.shardID
		}
	}
	panic(fmt.Sprintf("cluster: placement epoch %d has no shard for token %d", r.catalog.Epoch, h))
}

// GroupID returns the mux groupID for a shard. Reserved IDs:
//
//	0..shardCount-1 → data shards
//
// The router exposes this as a method so the bootstrap code stays
// honest about which IDs are in use.
func (r *Router) GroupID(shard uint32) uint32 { return shard }
