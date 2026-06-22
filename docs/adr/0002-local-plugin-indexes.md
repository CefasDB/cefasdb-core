# ADR 0002 — Local plugin indexes (ScyllaDB-style)

* Status: proposed
* Date: 2026-06-21
* Tracks: epic #475

## Context

Plugin-backed indexes (bloom, hll, geohash, ann, trigram, etc.) keep
their state in a per-process `map[descriptorKey]*State` inside the
plugin instance. Until #466, multi-node `CreateIndex` failed before
the plugin ever Built, so the per-process-state limitation was
invisible: a node that did not own all shards locally rejected the
attach outright.

With #466 in place, attach succeeds. End-to-end testing on the 8-node
RF=3 cluster (commit `b23646d`) exposed two follow-up problems:

1. **Descriptor visibility** — the descriptor itself replicates
   correctly, because `PutPluginIndexDescriptor` rides shard-0 raft
   and shard 0 has `RF=N` by placement design. Every node's
   `lookupPluginIndexDescriptor` returns the descriptor after a
   single `CreateIndex`. **No work needed here.**
2. **Plugin state replication** — the in-memory state is only
   populated on the node that ran `Build`. Other nodes lookup the
   descriptor, call `plugin.StateFor`, get an empty newly-created
   state, and serve `Count: 0` for every query. This is the bug to
   close.

## Decision

Adopt the ScyllaDB **local secondary index** model.

Each node Builds and serves the slice of the index corresponding to
the shards it hosts locally. The coordinator joins the slices on read
via cross-node fanout (see Phase 2 of the epic, #478). Writes already
update local state via `pluginIndexMutationHook` — no change there.

This is one of three plugin-index categories the design now
distinguishes explicitly:

| Category | Status | Notes |
|---|---|---|
| GSI native (DynamoDB-style) | already works | Co-located with base item in the same Pebble shard via secondary keys |
| **Local plugin index** | **this ADR** | Per-node state, fanout on read |
| Global plugin index | out of scope, separate ADR if/when a workload demands it | Would shard the index by indexed value; needs a new `ShardedIndexPlugin` interface and 2-phase write across base table + index table |

### Why local, not global

- **Resolves every plugin in tree today** (bloom, hll, geohash, ann,
  trigram). None of them have a natural sharding key by indexed
  value that would benefit from global distribution.
- **Write path stays free**: `pluginIndexMutationHook` already runs
  on the leader of the affected shard. Local-only state means the
  hook updates the same in-memory map the next read on that node
  will hit.
- **Read fanout cost is bounded**: N node RPC calls in parallel,
  dominated by the slowest peer. Acceptable for the workloads in
  play (geo audience, count-distinct, top-k); revisit if we later
  ship plugins where the per-query latency budget can't absorb it.
- **Global is its own epic** — needs sharding for indexed values,
  reconciliation, 2-phase write. Not blocking any current workload.

## Considered alternatives

### A. Global plugin indexes (ScyllaDB SI-style)

Each plugin defines a shard-by function over the indexed value;
descriptor + state live as their own raft-replicated key space.

- Pro: query against a known index value is O(1) RPC to the right
  shard, no fanout.
- Pro: index data scales independently of base table sharding.
- Con: writes become 2-phase (base shard + index shard). Latency
  penalty + reconciliation needed for partial failures.
- Con: every plugin has to expose a sharding key extractor. Many
  current plugins (bloom, hll) have no natural one — would need a
  hash-of-indexed-value, which defeats the point.
- Con: large refactor of plugin interface, descriptor schema,
  catalog migration.

Rejected for now: heavy for no current win. Re-evaluate when a
workload appears that needs global semantics.

### B. Pebble-backed plugin state

Each plugin persists its state into Pebble; reads page in on demand.

- Pro: state survives restart without rebuild; less memory pressure.
- Con: every plugin gains a serialization codec. Geohash buckets,
  bloom bitmaps, HLL register arrays all serialize differently.
  Adds I/O to every Update.
- Con: still per-node — does not solve the replication problem this
  ADR addresses.

Rejected for now: orthogonal to the gap. Worth opening as a
separate epic if memory becomes the bottleneck after Phase 3.

### C. Coordinator-only build (single home node per index)

One node owns the index; queries route through it.

- Pro: smallest change.
- Con: single point of failure per index.
- Con: cross-shard fan-in for Build is exactly what #466 solved, so
  this would put us right back at the limitation the epic just
  removed.

Rejected: defeats the purpose of distribution.

## Consequences

### Positive

- Multi-node `CreateIndex` from any node populates that node's local
  slice. Restart rebuilds it; queries on that node return correct
  per-shard results.
- After Phase 2 lands (#478), queries from any node see the full
  table because the coordinator fans out to all replicas.
- Writes pay no extra cross-node cost — the mutation hook keeps
  updating the local state of the leader.
- No new plugin interface. Existing plugin authors keep the same
  `Build` / `Update` / `Query` contract; the server is the only
  layer that becomes shard-aware.

### Negative / risks

- **Query latency** grows by one cross-node RPC fanout (Phase 2). For
  small index queries this is dominated by the slowest replica;
  expected 2-5x single-node baseline. Documented per query type in
  Phase 2's PR.
- **Memory per node** carries the items for every shard the node
  hosts. With RF=3 on 8 nodes the per-node load is `3/8` of the
  table — measurable but bounded. Add `cefas_plugin_index_local_items`
  gauge so operators can size it.
- **Restart latency**: rehydration runs in a background goroutine on
  startup; queries arriving before rebuild completes hit the lazy
  Build path. First-query latency on cold start scales with the
  table row count.
- **Split-brain on a transitional placement**: if a shard is moving
  between nodes, both source and destination may briefly have local
  state. Dedup by primary key in the coordinator (Phase 2) handles
  this — same mechanism #466 already uses.

### Neutral

- Adds one map (`pluginIndexLocal.entries`) and one goroutine path
  per descriptor on startup.

## Rollout

1. **Phase 1 (#477)** — this PR: replace `indexItemSourceFor` with
   `localIndexItemSourceFor` (drop the cross-shard fan-in for
   Build); add `ensurePluginIndexLocalState` + lazy / eager Build
   path; wire the eager Build through `hydratePluginIndexCatalog`;
   call `ensurePluginIndexLocalState` from `GeoAudience` before
   binding the index. GeoAudience continues to return only the
   local-node slice — observable but does not regress.
2. **Phase 2 (#478)** — `Replica.QueryIndex` RPC + coordinator
   fanout for `GeoAudience` / `TopK` / `Estimate`. After this lands,
   query from any node returns the full table-wide result.
3. **Phase 3 (#480)** — delete `peerScanShardIntoSet` from the
   plugin Build path (no longer used by `localIndexItemSourceFor`).
   `PeerScanShard` itself stays for the public `Scan` handler
   refactor in #470.

## References

- Epic: https://github.com/CefasDB/cefasdb-core/issues/475
- Phase 1: https://github.com/CefasDB/cefasdb-core/issues/477
- Phase 2: https://github.com/CefasDB/cefasdb-core/issues/478
- Phase 3: https://github.com/CefasDB/cefasdb-core/issues/480
- Predecessor epic (cross-shard scan): #466 / PR #471, #472, #473
- Pre-existing fact validated in field: `PutPluginIndexDescriptor`
  via shard-0 raft replicates the descriptor to every node, so the
  original Phase 1 (#476) was closed without any change required.
- ScyllaDB local secondary index reference:
  https://docs.scylladb.com/manual/stable/using-scylla/secondary-indexes/local-secondary-indexes.html
