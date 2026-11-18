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

A native-stream writer registers a reservation after its primary-key locks
and before it allocates any change sequence; the reservation's floor is the
allocator value + 1 read under the fence lock. It is released after its
commit returns, committed or failed. Writers commit concurrently and
group-commit (commitLoop, or the Raft apply loop).

The watermark is `min(live floors) - 1`, or the allocator value when nothing
is in flight. Invariant: every native sequence `<= watermark` has committed
or was abandoned. Proof sketch: a live writer's sequences are all `>=` its
floor `>` watermark; a writer not yet registered has allocated nothing, and
any later floor exceeds the allocator value seen by an earlier watermark, so
the watermark is also monotonic.

Readers of tables with text indexes (`StreamRecords`, `changeRecordsAfter`,
`ScanCDC`) run the read barrier, take the watermark, then open their Pebble
snapshot, and stop the scan at the watermark. Every native sequence up to it
is therefore in the snapshot, and `nextSequence` cannot pass an allocated but
uncommitted native sequence, whichever table committed a later one.
`CommittedChangeIndex` keeps its meaning (`max(persisted counter, last
changelog key)`) capped at the watermark, so a LATEST iterator cannot skip
an in-flight native write. Readers never wait for writers. A cursor may
briefly not see a later committed write while an earlier one is in flight;
it sees it on the next poll. Conditions for fencing are unchanged: writers on
text+stream tables, readers on text tables.

Registration never blocks, so the only lock order left is the ascending
primary-key shard order. A Raft snapshot install now reseeds the allocator
(`ReseedChangeIndex`, raise-only) so the watermark of a promoted node covers
the installed changelog.

### Context-bounded reads

`ReadBarrierCtx` bounds the barrier by the request deadline and returns
promptly on cancellation with `ErrReadUnavailable`, which wraps the context
error. The stream HTTP surface maps it to 503 and gRPC to `Unavailable`, the
existing retryable answers for a leader change or a draining store. The HTTP and
gRPC DescribeStream, GetShardIterator and GetRecords handlers pass their
request context.

### TTL reaper chunks

The reaper takes no stream fence across a sweep. Victims are processed in lock
groups of 16 (at most 16 of the 64 primary-key shards held at once). A group
is planned (cold reads, index deletes) without allocating sequences and
committed in chunks of at most 1 MiB (a single victim larger than that commits
alone). The delete change records are sized with the widest sequence first,
then allocated under a reservation only immediately before each chunk
commit. Group locks are held until their chunks commit, so a renewed row can
never be removed by a stale plan. The 8 MiB per-sweep budget and
`BacklogPresent` semantics stay. `Deleted` and `StalePointers` advance per
committed chunk, so a failed sweep reports only durable progress.

### Telemetry

Pebble `WriteStallBegin/End` and `DiskSlow` events feed
`cefas_pebble_write_stalls_total`, `cefas_pebble_write_stall_seconds_total`,
`cefas_pebble_write_stall_active` and `cefas_pebble_disk_slow_total`.
`cefas_stream_fence_inflight`, `cefas_stream_fence_lag_sequences` and the
`cefas_stream_fence_hold_seconds` histogram (reservation through commit)
describe the watermark. All are labelled by shard, like the existing Pebble
gauges.

## Consequences

Measured locally (Apple M4 Max, APFS, fsync on, 24 writers, 3000 ops x 3):
~2050 ops/s, writer p99 ~30 ms, concurrent stream reader p99 ~4 ms
(`BenchmarkNativeStreamConcurrentWriters`). Production numbers on pd-standard
must be read from the new metrics after rollout.

Rollback is a plain revert: nothing persisted changes shape.
Out of scope, pre-existing: non-native stream tables remain unfenced; the
persisted change counter can be written out of order by concurrent batches
(recovery takes the max with a changelog scan).
