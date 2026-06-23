# ADR 0003 — Materialized Views with pluggable refresh policy

* Status: proposed
* Date: 2026-06-22
* Tracks: epic #488

## Context

CefasDB already exposes two flavours of secondary lookup:

- **GSI native** (DynamoDB-style) — secondary keys live in the same
  Pebble shard as the base item. Cheap on write; reads that don't
  carry the base partition key fan out across every shard.
- **Local plugin index** (epic #475) — per-node sketch/state for
  bloom / hll / geohash / ANN, queried via `Replica.QueryIndex`
  fanout. Powerful for probabilistic / similarity reads, but not a
  general row projection.

Neither is a **materialized view** in the relational sense: a table
whose rows are derived from another table, partitioned by its own
key, kept in sync by the write path, and read with the same SQL
machinery as any other table.

ScyllaDB ships materialized views ([docs](https://docs.scylladb.com/manual/stable/features/materialized-views.html))
with an always-eager refresh contract: every base write triggers a
synchronous MV write before returning. That model maximises
read-your-write at the cost of write latency.

Many workloads do not need read-your-write on the view — dashboards,
audit logs, periodic reports — and would happily trade staleness for
lower write pressure. Oracle / PostgreSQL recognise this with
`REFRESH FAST/COMPLETE ON COMMIT/ON DEMAND/ON SCHEDULE`.

## Decision

CefasDB ships materialized views with a **per-view refresh policy**:

| Mode | When the hook runs | Lag | Write cost |
|---|---|---|---|
| `EAGER` (default) | every base mutation, synchronously | 0 (read-your-write) | high — comparable to plugin-index hook (~10%, ref #487) |
| `EVERY N <unit>` (scheduled) | a background worker re-derives the MV at fixed intervals | up to N | zero on hot path; periodic CPU burst |
| `ON DEMAND` | explicit `RefreshMaterializedView` RPC | unbounded until invoked | zero |

Default is `EAGER` to match ScyllaDB's expectation for callers migrating
from there. Operators opt into `EVERY` / `ON DEMAND` per view.

Reads from a view always succeed without blocking on a pending
refresh; a response header `x-cefas-mv-staleness-seconds` exposes
`now − LastRefreshAt` so callers can decide whether to trust the
data.

### Storage shape

A materialized view is a sibling object in the catalog:

```go
type MaterializedViewDescriptor struct {
    Name                string
    BaseTable           string
    KeySchema           KeySchema   // own PK + optional SK
    ProjectedAttributes []string    // empty → all base attributes
    GroupBy             []string    // aggregate views: must match key
    Aggregations        []MaterializedViewAggregation
    RefreshPolicy       RefreshPolicy
    Status              string      // building | active | paused | failed
    LastRefreshAtUnix   int64
}
```

The descriptor is persisted under `cefas/internal/mv/<name>` and
replicated via shard-0 raft (same path as table descriptors). The
base table's descriptor carries the list of attached MV names so the
write hook can find them without scanning every view.

Views show up in the table catalog via the existing `Describe` /
`Query` / `Scan` codepaths once their own key schema is in place;
they ride the same routing layer as regular tables.

### Write coordination

EAGER hook (Phase 2 / #491):

1. After the base write commits on its leader, look up every MV
   attached to the base table.
2. Filter by `RefreshPolicy.Mode == EAGER`. Non-eager MVs increment
   a skip counter and exit early (zero hot-path cost beyond the
   filter — keeps mixed-mode tables honest).
3. For each EAGER MV: derive the row, route the MV's PK to its
   owning shard, write via `targets.PutItemWith`.
4. Block the caller's response until every EAGER MV write has
   succeeded. On failure: log, increment error metric, return
   error to the caller.

Aggregating EAGER MVs support `COUNT(*)` and `SUM(col)` only. The
`GROUP BY` list must match the MV primary key, and aggregate output
columns are stored as counter columns. The write hook captures the
old image when needed, combines old/new contributions per MV key,
then applies the resulting deltas through the internal
`Replica.AtomicUpdateMV` path. Updates that keep the same group become
a net SUM delta with no COUNT change; group moves decrement the old
group and increment the new group. Deleting the final row in a group
leaves a zero-valued aggregate row; compaction/removal of zero rows is
left to a future maintenance policy.

SCHEDULED / ON_DEMAND writes do not touch the hot path. They go
through a shared **refresh-complete engine** (Phase 4 / #493)
invoked by either:

- a heap-driven scheduler keyed by `NextDueAt = LastRefreshAt +
  IntervalSeconds` (Phase 7 / #502), or
- an explicit `RefreshMaterializedView` RPC (Phase 6 / #495).

The engine uses the cross-shard fan-in already shipped for plugin
indexes (`cluster.Manager.PeerScanShard`, epic #466) to read the
base table from every shard, re-derive each MV row, and write to
the MV's shard. Resumable via a checkpoint key in the MV
descriptor.

### Independent placement (Phase 5 / #494)

By default an MV inherits the base table's placement (shard count,
RF). A future-proof `WITH SHARDS=N REPLICATION_FACTOR=R` clause
lets the MV declare its own — useful when the read pattern differs
sharply from the base (e.g. read-heavy join target on a write-heavy
base).

## Considered alternatives

### A. Always-eager (ScyllaDB-style)

- Pro: simpler — one consistency model.
- Pro: read-your-write everywhere.
- Con: every workload pays write tax even when the workload would
  tolerate lag.
- Con: no escape valve for OLAP-style views that summarise a
  high-write base.

Rejected: gives up an obvious optimisation that doesn't cost much
complexity.

### B. Always-async (Cassandra-style with eventual consistency)

- Pro: zero write overhead on hot path.
- Con: surprises ScyllaDB / DynamoDB-shape users.
- Con: harder to reason about when read-your-write is actually
  needed (audit, OLTP).

Rejected: the default should preserve the strongest natural
contract.

### C. FAST incremental refresh in v1

Oracle's `REFRESH FAST` consumes a delta log (materialized view
log) to apply only changed rows since `LastRefreshAt`. CefasDB
could feed it from the existing changelog stream.

- Pro: cheaper than COMPLETE when delta « base.
- Con: adds a delta-source contract, retention rules, and recovery
  semantics on top of the catalog change.
- Con: not all workloads have streams enabled — fallback to
  COMPLETE needed anyway.

Deferred: v1 ships COMPLETE only. FAST becomes a follow-up when a
real workload pays for the complexity.

### D. View on a view

Recursive views (a view derived from another view). Oracle / Postgres
support it.

- Pro: composable.
- Con: write coordination becomes a topological sort; failure modes
  multiply.

Rejected for v1. Composing in user-space (two independent views over
the same base) handles most cases.

## Consequences

### Positive

- Operators can pick the right consistency/cost trade-off per view.
- Default is the strongest contract (EAGER) — zero surprise to callers
  migrating from ScyllaDB.
- Reads against any mode use the same SQL and the same routing path.
- Backfill engine is reusable: CREATE-time initial population, periodic
  refresh, and operator-triggered refresh share one implementation.

### Negative / risks

- EAGER MV adds write latency proportional to attached MV count + their
  routing distance. Budget: ≤ 10% per attached EAGER MV — locked by
  the same gate as #487's plugin-index hook target.
- SCHEDULED MV introduces a staleness window users have to reason
  about. The `x-cefas-mv-staleness-seconds` header makes it explicit;
  unsophisticated callers still need a runbook.
- Independent placement (Phase 5) opens N extra raft groups per MV.
  Storage cost is base + view; operators need observability into MV
  growth.

### Neutral

- Per-MV descriptor carries the policy, so altering the policy without
  recreating the view is feasible — out of scope for v1.

## Locked decisions

- **Default refresh policy**: `EAGER`.
- **Refresh kind in v1**: `COMPLETE` only (FAST follow-up).
- **Atomicity for EAGER**: blocking; async + queued is a future ADR.
- **MV must carry the base PK in its row schema**: yes (otherwise
  same base row maps to multiple MV rows with no deterministic
  delete).
- **Filter / join in view**: out of scope for v1.
- **Aggregates in view**: `COUNT(*)` and `SUM(col)` for EAGER views
  only; MIN / MAX / AVG and general query-time GROUP BY stay out of
  scope.
- **Multiple MVs per base table**: yes, each independently
  maintained per its own policy.
- **Schema evolution**: ALTER on a base column the MV depends on
  requires drop + recreate (v1 limitation).
- **Staleness visibility**: response header
  `x-cefas-mv-staleness-seconds` on Query/Scan against any MV.

## Rollout

1. **Phase 1 (#490, this PR's first slice)** — schema, catalog,
   DDL, gRPC handlers, ADR. Descriptors persist + replicate; no
   write hook yet.
2. **Phase 2 (#491)** — EAGER write maintenance (this PR's second
   slice).
3. **Phase 3 (#492)** — read path + staleness header (this PR's
   third slice).
4. **Phase 4 (#493)** — refresh-complete engine.
5. **Phase 5 (#494)** — independent placement.
6. **Phase 6 (#495)** — admin RPCs (Pause / Resume / Refresh).
7. **Phase 7 (#502)** — refresh scheduler.

## References

- Epic: https://github.com/CefasDB/cefasdb-core/issues/488
- Phase issues: #490, #491, #492, #493, #494, #495, #502
- ScyllaDB MV docs: https://docs.scylladb.com/manual/stable/features/materialized-views.html
- Oracle REFRESH FAST / COMPLETE / ON COMMIT / ON DEMAND: precedent for
  per-view refresh policy.
- Predecessor cross-shard scan epic (#466) — provides
  `PeerScanShard` used by the refresh engine.
