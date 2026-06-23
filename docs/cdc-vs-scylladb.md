# CefasDB Streams vs ScyllaDB CDC

Reference notes for operators and contributors that need to know
which ScyllaDB CDC concepts map cleanly onto CefasDB Streams,
which differ in shape, and which are tracked as explicit gaps.

Closes #520 (the issue itself was already a rendered alignment
table — this file persists it in tree so the next person looking
does not have to dig into closed GitHub issues).

## Alignment table

| ScyllaDB CDC concept | CefasDB equivalent | Status |
|---|---|---|
| CDC log table (auto-populated sibling) | Unified changelog keyspace under `cefas/admin/change/log/` | Aligned (different shape, same semantics) |
| Pre-image (capture prior row state) | `OLD_IMAGE` view type | Aligned |
| Post-image (capture new row state) | `NEW_IMAGE` view type | Aligned |
| Full row capture | `NEW_AND_OLD_IMAGES` view type | Aligned |
| Keys-only option | `KEYS_ONLY` view type | Aligned |
| Per-row CDC (one entry per affected row) | Per-row, indexed by monotonic `changeIndex` | Aligned |
| Background retention sweep | `ApplyStreamRetention()` loop | Aligned |
| Configurable per-table CDC TTL | **Global retention only** today | **Gap → #521** |
| DELTA capture (only modified columns) | Not implemented | **Gap → #522** |
| CDC as a queryable table | `Scan / Query` against `<base>_cdc` (#556) | **Done in #523** |
| Explicit idempotency markers (`batch_id`, `seq_in_batch`, `op_kind`) | Implicit via `Index` (monotonic) | **Gap → #524** |
| Runtime CDC toggle (enable / disable post-CreateTable) | Create-time only via `StreamSpecification` | **Gap → #525** |

## Key files

| Concern | File | Notes |
|---|---|---|
| Changelog write path | `internal/storage/adapter/pebble/changelog.go` | `appendChangeRecord` + `applyStreamRecordFields` |
| Retention loop | `internal/storage/adapter/pebble/changelog.go:343` | `ApplyStreamRetention` |
| Stream config | `internal/storage/adapter/pebble/options.go` | `StreamRetentionOptions` |
| Stream descriptor | `pkg/types/types.go` | `StreamSpecification` |
| Read RPCs | `internal/server/grpc_streams.go` | `GetShardIterator` / `GetRecords` |
| Queryable alias | `internal/server/grpc_cdc_scan.go` | `<base>_cdc` table |
| Streams proto | `pkg/protocol/cefas.proto` | `StreamSpecification`, `GetRecords*` |

## Trade-offs locked

- **Per-row, not per-partition**: CefasDB emits one `ChangeRecord`
  per row affected. A multi-row `BatchWriteItem` produces N
  records, all with monotonic `Index`. ScyllaDB has the same
  per-row semantic — the difference is the surface: ScyllaDB
  exposes the CDC table separately, CefasDB exposes both `Scan`
  on `<base>_cdc` and `GetRecords` RPC.

- **Single changelog keyspace**: every stream-enabled table's
  records live under one shared prefix
  (`cefas/admin/change/log/<index>`) with `table` tagged on each
  record. Per-table retention iterates this prefix filtering by
  `Table == name`. Per-table physical separation is not planned
  — the retention complexity outweighs the locality benefit at
  the workloads we have measured.

- **Retention is logical, not physical**: the loop bumps
  `StreamRetentionStats.OldestSequence`; physical bytes survive
  for PITR / backup. Reads below `OldestSequence` get
  `ErrStreamTrimmed`. ScyllaDB shares this convention.

## Gap roadmap

1. **#521 per-table retention** — extend `StreamSpecification`
   with `RetentionSeconds`; loop reads the override first.
2. **#524 idempotency markers** — add `batch_id`, `seq_in_batch`,
   `op_kind` fields to `ChangeRecord` so consumers can dedup
   without inferring from `Index`.
3. **#525 runtime CDC toggle** — `UpdateStreamSpecification` RPC
   to enable / disable / view-type change without recreating the
   table.
4. **#522 DELTA capture** — fifth view type that emits only the
   columns that changed (computed at write time when we already
   hold `oldItem` + `newItem`).

## What this isn't

This is not an ADR. ADRs go in `docs/adr/` and lock decisions
behind an explicit number. This file is operator-facing
reference material — the alignment status drifts as gaps close;
edit in place rather than open a new ADR per status change.
