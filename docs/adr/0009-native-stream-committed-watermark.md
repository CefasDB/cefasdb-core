# ADR 0009: native stream committed watermark and bounded TTL chunks

Status: accepted for implementation (FU-01, CFP-070 follow-up). Amends the
"Native stream continuity" paragraph of ADR 0008. No wire, on-disk key,
configuration-default or public API change.

## Context and evidence

ADR 0008 serialized every native-stream write (text indexes + stream enabled)
on one store-wide mutex from before the primary-key locks until its commit
returned, fsync included. Readers took the same mutex plus a 5 s Raft barrier.
Production (single replica, `fsyncOnCommit`, pd-standard) showed the cost:
native writes never group-committed, the TTL reaper held the mutex and up to
all 64 primary-key lock shards for a whole sweep (thousands of cold reads and
one fsynced batch of up to 8 MiB), stream readiness reads queued behind both
and missed 3-5 s budgets, and the waits ignored the request context. The
benchmark below shows the serialized ceiling locally (24 writers, fsync on):
~193 ops/s, writer p99 150-280 ms, stream reader p99 ~150 ms.

## Decision

### Committed watermark instead of a mutex

A native-stream writer registers a reservation immediately before its first
change-sequence allocation (after its primary-key locks, cold reads,
condition evaluation and index planning); the reservation's floor is the
allocator value + 1 read under the fence lock. It is released after its
commit returns, committed or failed. A writer that fails before allocating
never registers. Writers commit concurrently and group-commit (commitLoop,
or the Raft apply loop).

The watermark is `min(live floors) - 1`, or the allocator value when nothing
is in flight. Invariant: every native sequence `<= watermark` has committed
or was abandoned. Proof sketch: a live writer's sequences are all `>=` its
floor `>` watermark; a writer not yet registered has allocated nothing, and
any later floor exceeds the allocator value seen by an earlier watermark, so
the watermark is also monotonic.

Readers of tables with text indexes (`StreamRecords`, `changeRecordsAfter`,
`ScanCDC`) take the watermark, then run the read barrier, then open their
Pebble snapshot, and stop the scan at the watermark. Every native sequence
up to it is therefore in the snapshot, and `nextSequence` cannot pass an
allocated but uncommitted native sequence, whichever table committed a later
one. `CommittedChangeIndex` keeps its meaning (`max(persisted counter, last
changelog key)`) capped at the watermark, taken before its barrier too, so a
LATEST iterator cannot skip an in-flight native write.

The watermark-before-barrier order is load-bearing on a multi-voter group
(review finding P2-1). A sequence at or below the watermark has committed or
was abandoned by a writer whose `Replicate` had already returned. Raft only
fails a future after its entry was appended (`ErrLeadershipLost`) or when it
was never appended (`ErrNotLeader`, enqueue timeout), so an abandoned entry
is already in the log ahead of the reader's barrier entry: if a later leader
commits it, the barrier applies it before the snapshot; if the barrier
commits, the entry is either in the committed prefix or can never commit.
With the opposite order a writer could allocate `s`, append behind the
reader's barrier, lose leadership after the barrier applied, release, and
let the reader's watermark pass `s`; the next leader would then commit `s`
below the cursor. `TestNativeStreamReadBoundAbandonedEntryAndLeaderChange`
models that log (an explicit log, a parked append, leadership loss and a new
leader that commits or truncates the abandoned entry) and fails if the order
is reverted. Readers never wait for writers. A cursor may
briefly not see a later committed write while an earlier one is in flight;
it sees it on the next poll. Conditions for fencing are unchanged: writers on
text+stream tables, readers on text tables.

Registration never blocks, so the only lock order left is the ascending
primary-key shard order. Waiting on the watermark is reserved for
point-in-time restore (below); stream readers never wait for writers. A Raft snapshot install now reseeds the allocator
(`ReseedChangeIndex`, raise-only) so the watermark of a promoted node covers
the installed changelog.

### Context-bounded reads

`ReadBarrierCtx` bounds the barrier by the request deadline and returns
promptly on cancellation with `ErrReadUnavailable`, which wraps the context
error. The stream HTTP surface maps it to 503 and gRPC to `Unavailable`, the
existing retryable answers for a leader change or a draining store. The HTTP and
gRPC DescribeStream, GetShardIterator and GetRecords handlers pass their
request context.

### Change timestamps and point-in-time restore

A sequence and its `UnixNano` are now assigned together under one short
allocation lock, and the timestamp is clamped to be non-decreasing, so within
a process timestamps never decrease in sequence order. Timestamp PITR stops
the changelog scan at the first record newer than its target and relies on
that order; the old mutex gave it only for native writers, and concurrent
non-native writers could invert by nanoseconds. A caller-supplied timestamp
(test fixtures backdating records) is kept outside the clamp; no production
writer supplies one.

PITR replay (`replayChangesToTarget`) needs every record through its target
index (explicit, or the allocator when only a timestamp is given). It uses
`changeRecordsThroughCtx`, which for a native table waits for the watermark
to reach the target (woken by reservation releases, bounded by 30 s and the
context) and fails with `ErrReadUnavailable` instead of returning a range cut
short at the watermark. It never stops short silently.

### TTL reaper chunks

The reaper takes no stream fence across a sweep. Victims are processed in lock
groups of 16 (at most 16 of the 64 primary-key shards held at once). A group
is planned (cold reads, index deletes) without allocating sequences and
committed in chunks of at most 1 MiB (a single victim larger than that commits
alone). The delete change records are sized with the widest sequence first
(sequence and timestamp are fixed-width), then allocated and stamped under a
reservation only immediately before each chunk commit; a record is never
stamped at planning time, so a slow plan cannot carry an older timestamp past
a sequence another writer allocated earlier. Group locks are held until their chunks commit, so a renewed row can
never be removed by a stale plan. The 8 MiB per-sweep budget and
`BacklogPresent` semantics stay. `Deleted` and `StalePointers` advance per
committed chunk, so a failed sweep reports only durable progress.

### Telemetry

Pebble `WriteStallBegin/End` and `DiskSlow` events feed
`cefas_pebble_write_stalls_total`, `cefas_pebble_write_stall_seconds_total`,
`cefas_pebble_write_stall_active` and `cefas_pebble_disk_slow_total`.
`cefas_stream_fence_inflight`, `cefas_stream_fence_lag_sequences` and the
`cefas_stream_fence_hold_seconds` histogram (reservation, taken immediately
before the first sequence allocation, through commit return) describe the
watermark. All are labelled by shard, like the existing Pebble gauges. The
histogram is attached when a collector starts, before its first tick
(`RunShardCollector` re-attaches idempotently for shards opened later, which
are observed from the following tick). Raft log stores (`<shard>:raft`, and
`raft` in single-Raft mode through `RunRaftLogStorageCollector`) export the
stall counters but no fence series, which would always be zero.

## Consequences

Measured locally (Apple M4 Max, APFS, fsync on, 24 writers, 3000 ops x 3):
~2050 ops/s, writer p99 ~30 ms, concurrent stream reader p99 ~4 ms
(`BenchmarkNativeStreamConcurrentWriters`). Production numbers on pd-standard
must be read from the new metrics after rollout.

Rollback is a plain revert: nothing persisted changes shape.

Review status: GO for a single replica; the multi-voter NO-GO (P2-1) is
addressed by the watermark-before-barrier order above.

### Known pre-existing holes (out of scope)

- Non-native stream tables remain unfenced.
- The persisted change counter can be written out of order by concurrent
  batches (recovery takes the max with a changelog scan).
- `GetRecords` runs the read barrier twice per request (one extra Raft round
  trip, no correctness impact).
- A writer holding a stale descriptor (text indexes or stream added
  concurrently) writes unfenced until it sees the new descriptor.
- A newly elected leader can allocate a sequence already used by an entry it
  has not applied yet (its allocator is raised on apply, not on election);
  the timestamp clamp is also per process, so a leader change can step
  timestamps back by the clock skew between nodes.
- The materialized-view COMPLETE-refresh cursor handling is unchanged here.
- Store selection for stream reads with `cluster.shards: 1` is unchanged
  here; the watermark is per store and never spans stores.
