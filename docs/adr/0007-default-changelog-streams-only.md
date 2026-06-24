# ADR 0007 - Default changelog mode is `streams-only`

## Status

Accepted.

## Context

Every storage mutation passes through `appendChangeRecord`, which adds
two writes to the user batch when the changelog is active:

1. `KeyChangeLog(rec.Index)` — the change record itself, sized by view
   type (`NEW_AND_OLD_IMAGES` carries old + new item bytes).
2. `ChangeCounterKey` — a single fixed key rewritten on every mutation.

The two extra writes go through the raft log, doubling the effective
batch size for replication and roughly doubling the L0 churn the
compactor has to absorb. Until this ADR, the default mode was
`always`: every put / delete on every table — stream-enabled or not —
paid the tax.

An 8-node bench (24 shards, RF=3, 64 write workers, 5m sustained)
showed `write_only` regressing from 186K rows/s (June 19) to 112K rows/s
after a quarter of CDC, GSI, MV, and service-level features landed.
The single largest contributor was the changelog table: a control bench
with `-storage-changelog-mode=streams-only` recovered the throughput.

## Decisions

### 1. Default mode is `streams-only`

`normalizeChangeLogMode("")` now returns `ChangeLogModeStreamsOnly`.
A mutation only enters the changelog when its table's
`StreamSpecification.StreamEnabled` is true.

Operators that need changelog records for non-stream tables (PITR over
arbitrary tables, retroactive audit) opt in via:

- config: `storage.changeLogMode: always`
- env: `STORAGE_CHANGELOG_MODE=always`
- flag: `-storage-changelog-mode=always`

`off` continues to disable changelog writes entirely.

### 2. PITR / backup contract

`CreateBackup` records the changelog high-water (`ChangeIndex`,
`ChangeUnixNano`) at the checkpoint. Under `streams-only` the high-water
only advances on stream-enabled writes, so PITR replay covers
stream-enabled tables. Backups of non-stream tables are still valid
point-in-time checkpoints; they just cannot be replayed forward via
the changelog.

Operators that depend on per-mutation PITR for every table must set
`always`.

### 3. Counter key elimination

`appendChangeRecord` no longer writes `storage.ChangeCounterKey`. The
recovery path already maintains `d.changeIndex` from the largest
`KeyChangeLog` suffix on disk (`scanMaxChangeIndex`) and takes the max
with the persisted counter when present. Old deployments keep their
stale `ChangeCounterKey` and the new code reads it for forward
compatibility — the scan always wins.

Removing the counter write drops one KV per mutation and eliminates a
hot single-key rewrite that thrashed memtable + block cache.

## Consequences

- Throughput recovers on write-heavy workloads where most tables do not
  consume the changelog.
- Existing deployments that rely on the implicit `always` default must
  set `STORAGE_CHANGELOG_MODE=always` (or the equivalent flag / yaml
  key) to keep the prior behaviour.
- Backup metadata over non-stream tables no longer carries a
  per-mutation high-water; the high-water still advances on
  stream-enabled mutations and on `always` deployments.
- `loadPersistedChangeIndex` remains for forward compatibility but is
  no longer the source of truth.
