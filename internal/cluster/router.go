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
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// Router maps a partition key (canonical byte form, same input the
// storage layer's pkHash8 consumes) onto a shard ID. Static shard
// count for v1 — resharding is a separate epic.
type Router struct {
	shardCount uint32
}

// NewRouter returns a router that distributes keys over `n` shards.
// n == 0 is treated as 1 (single-shard / dev mode).
func NewRouter(n int) *Router {
	if n <= 0 {
		n = 1
	}
	return &Router{shardCount: uint32(n)}
}

// Count returns the configured shard count.
func (r *Router) Count() int { return int(r.shardCount) }

// ShardForPK returns the shard ID owning the supplied partition key
// bytes. The mapping uses xxhash64 → mod N, which is uniform over
// reasonable PK distributions and matches the pk_hash8 already
// computed inside the storage key encoder, so a future "place each
// shard on a contiguous hash range" rebalancer can adopt the same
// hash without rebuilding any data.
func (r *Router) ShardForPK(pkBytes []byte) uint32 {
	if r.shardCount == 1 {
		return 0
	}
	return uint32(xxhash.Sum64(pkBytes) % uint64(r.shardCount))
}

// ShardForUint64 routes a precomputed hash directly. Useful when the
// caller already has the pkHash8 in hand.
func (r *Router) ShardForUint64(h uint64) uint32 {
	if r.shardCount == 1 {
		return 0
	}
	return uint32(h % uint64(r.shardCount))
}

// GroupID returns the mux groupID for a shard. Reserved IDs:
//
//	0..shardCount-1 → data shards
//
// The router exposes this as a method so the bootstrap code stays
// honest about which IDs are in use.
func (r *Router) GroupID(shard uint32) uint32 { return shard }

// _ keeps encoding/binary referenced — the router's bytes-to-shard
// helpers may grow to operate on big-endian hash slices.
var _ = binary.BigEndian
