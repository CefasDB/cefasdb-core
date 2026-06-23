# ADR 0005 — Global Secondary Indexes

## Status

Accepted (Phase 1 / catalog only). Phases 2-6 follow and lock their
own implementation details in this same ADR.

## Context

CefasDB today ships two index families that look superficially
alike but solve different problems:

- **Native GSI** (DynamoDB-style) — index entries co-locate with
  the base row in the same pebble shard. Cheap on write; reads that
  do *not* carry the base partition key fan out across every shard.
- **Local plugin indexes** (epic #475, shipped) — per-node sketch
  state (bloom / hll / geohash / ANN) joined via `Replica.QueryIndex`
  fanout. Read-side aggregation.

[ScyllaDB's "Global" secondary index](https://docs.scylladb.com/manual/stable/features/secondary-indexes.html)
is a third shape: the **index has its own partitioning**, keyed by
the indexed value. A read against that value lands on exactly one
shard; the cost is paid on the write — every base mutation may
cross-shard hop to the index's owning shard.

Materialized views (#488) already proved the cross-shard cascade
infrastructure (#535 / #537), so the engineering risk for GSI is
mostly DDL plumbing and one new code path on the read side.

## Decisions (locked here, refined per phase)

### 1. New catalog object: `GlobalIndexDescriptor`

| Field | Purpose |
|---|---|
| `Name` | unique identity across tables / views / indexes |
| `BaseTable` | the source rows |
| `IndexedColumn` | the value that becomes the index's partition key |
| `ProjectedColumns` | columns inlined in the index entry; non-projected reads pay one extra base `GetItem` |
| `Shards`, `ReplicationFactor` | per-index placement (Phase 5 / #514); zero inherits from base |
| `Status` | building / active / failed / paused |
| `Paused` | admin pause flag (#515) |

Persisted on shard 0 under `KeyGlobalIndex(name)`.

### 2. DDL grammar

```
CREATE INDEX idx_email AS GLOBAL ON Users (email)
  [PROJECT (id, name)]
  [WITH SHARDS=N [, REPLICATION_FACTOR=R]]

DROP INDEX idx_email
```

`AS GLOBAL` disambiguates from the existing native GSI (which is
declared on `CreateTable`). The grammar will accept other index
shapes (`AS LOCAL`, etc.) without a parser break the day they
arrive.

### 3. Write semantics

EAGER, blocking, cross-shard cascade. The Phase 2 hook fires on
PutItem / BatchWriteItem / UpdateItem / DeleteItem before the
response returns — covering all four upfront avoids the #507 gap
we hit on MV.

Pointer-only entries (`<base_pk, projection>`); non-projected
reads pay one extra `GetItem`. Async + queued maintenance is a
follow-up ADR once we have a workload that demands it.

### 4. Read semantics

`Query` against the index partition key routes directly to the
index's owning shard — no fanout. Phase 3 / #512.

### 5. Backward compatibility

Existing native GSI keeps working. The two shapes are independently
declared and independently maintained. The query planner picks the
GLOBAL index when the request names it explicitly; falling back to
native GSI / scan is unchanged.

### 6. Acceptance gates

- Phase 1 (this PR): catalog round-trips, DDL parses, gRPC RPCs
  available. No write hook firing yet.
- Phase 2: base mutation produces an index entry on the right shard
  before returning.
- Phase 3: query landing on one shard.
- Phase 4: backfill of a populated base; resumable / idempotent.
- Phase 5: independent placement.
- Phase 6: pause / resume / rebuild + bench guardrail.

The end-to-end bench target (locked here): base-write throughput
regression with one GLOBAL index attached ≤ 15% on the 8-node
RF=3 cluster. Higher than the MV target because every base mutation
crosses a shard.

## Out of scope (for the epic)

- Strongly-consistent reads against a stale index.
- Indexes on materialized views.
- `ALTER INDEX` (drop + recreate in v1).
- Index on the index (no recursion).

## References

- Epic #509.
- ADR 0003 (materialized views) — same architectural family.
- #535 / #537 — cross-shard cascade plumbing reused for the write
  hook.
