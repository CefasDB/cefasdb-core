# Phase 11 audit — SOLID / DDD / naming hygiene

**Date:** 2026-06-16
**Scope:** Issue #319 (parent epic #307). Horizontal sweep across `internal/`, `pkg/`, `cmd/` for SOLID, naming, aggregate boundaries, struct cohesion. Excludes generated proto (`*.pb.go`) and `.claude/worktrees/`.
**Method:** Four parallel read-only audits — ISP (interfaces > 3 methods), naming hygiene, OCP/DIP, cohesion + data clumps.
**Disposition convention:** each finding ends with one of:
- **FIX** — actionable in a Phase 11 sub-PR.
- **WAIVE** — kept as-is with documented rationale.
- **TRACK** — converted into a follow-up issue (out of Phase 11 scope).

---

## 1. ISP — interfaces with > 3 methods

Six interfaces exceed the 3-method threshold. Three are **coherent contracts** on a single aggregate (waived). Three are **fat bundles** that mix unrelated capabilities.

| # | File:Line | Interface | Methods | Top consumers | Disposition |
|---|---|---|---|---|---|
| 1 | `pkg/sql/executor.go:38` | `Storage` | 8 | `internal/api/grpc_server.go`, `internal/api/grpc_ann.go`, `internal/api/http/item/item.go`, `internal/cluster/split_finalize.go`, `internal/api/http/cluster/cluster.go` | **FIX** — Split into `Reader` (6 read methods) + `Writer` (2 write methods), with `Storage` kept as the composed interface for callers that need both. |
| 2 | `internal/api/server.go:41` | `Cluster` | 7 | `cluster/manager.go`, `internal/rebalance/rebalance.go` | **TRACK** — Split into `ClusterState` (read-only: `IsLeader`, `SelfID`, `BindAddr`, `LeaderHTTPAddr`) and `ClusterTopology` (mutation: `AddVoter`, `RemoveServer`, `Barrier`). Defer because changing this interface touches the gRPC server wiring; pair with cluster's #2 (DIP) below. |
| 3 | `pkg/plugin/interfaces.go:161` | `BanditPlugin` | 7 | `pkg/plugin/builtins/bandit.go` | **TRACK** — Split into `BanditSampler` (read path: `Sample`, `BatchSample`, `SampleEligible`) and `BanditMutator` (state: `Init`, `Reward`, `Snapshot`). Defer because plugin registry needs migration plan. |
| 4 | `pkg/plugin/interfaces.go:37` | `IndexPlugin` | 6 | Index plugins (5+ consumers) | **WAIVE** — coherent index-lifecycle contract (Build, Update, Delete, Query, Estimate, Manifest). Add `Justification:` godoc. |
| 5 | `pkg/plugin/interfaces.go:94` | `AudiencePlugin` | 5 | `pkg/plugin/audience/audience.go` | **WAIVE** — coherent audience contract; all methods operate on the `AudienceRequest` aggregate. Add `Justification:` godoc. |
| 6 | `internal/rebalance/rebalance.go:38` | `Planner` | 4 | rebalance `Controller` | **WAIVE** — borderline (1 above the limit); the four methods are the orchestration cycle that any planner must implement. Add `Justification:` godoc. |

---

## 2. Naming hygiene

| Category | Violations | Verdict |
|---|---|---|
| Stutter (type name repeats package) | 0 | clean |
| `Get` prefix accessors (non-generated) | 0 non-exempt | clean — all 17 `Get*` methods either implement `hashicorp/raft`, fulfil a gRPC method contract, or wrap a generated stub |
| Generic suffix overuse (`Manager`, `Service`, etc.) | 0 | the package boundaries are tight enough that each `Manager`/`Service` does name a domain concept |
| Banned / vague package names | 5 | see below — three are **FIX**, two are **WAIVE** |

### 2.1 Package-name findings

| # | Path | Issue | Disposition |
|---|---|---|---|
| 1 | `internal/testutil/wait` | `testutil` is on the banned-name list (skill §1) | **TRACK** — Rename touches every test that imports it. Worth doing as a dedicated PR but not now. Suggested name: `internal/testsync` or fold helpers inline. |
| 2 | `pkg/plugin/internal` | Nested `internal` under a package that is already internal-ish; Go enforces `pkg/plugin/internal/*` is callable only from `pkg/plugin/...` — that constraint is appropriate, but the directory name reads as scaffolding | **WAIVE** — Go convention. The name is the language-level visibility marker, not a domain concept. |
| 3 | `pkg/core/query/internal` | Same Go-internal rule | **WAIVE** — same justification as #2. |
| 4 | `internal/api/streamcore` | "streamcore" is scaffolding-ish; the package holds the shared CDC iterator helpers | **TRACK** — Rename to `internal/api/cdciter` or fold into `internal/api/stream/` (HTTP transport) + a sibling for the iterator. |
| 5 | `pkg/plugin/distancecontract` | "contract" is generic | **TRACK** — Rename to `pkg/plugin/distance` (the package is the contract by definition). |

---

## 3. OCP — type-switch dispatch

Five type-switch sites with ≥ 4 cases. Three are codec / pretty-printer paths (acceptable). Two dispatch domain behaviour and are refactor candidates.

| # | File:Line | Cases | Switched type | Purpose | Disposition |
|---|---|---|---|---|---|
| 1 | `pkg/sql/executor.go:69` | 9 | `Plan` | Dispatches to `execCreate`, `execDrop`, `execInsert`, etc. | **TRACK** — Replace with a `Plan.Execute(*Executor)` method. Risk: changes the `Plan` interface contract; sql package is wide. Phase 11.X candidate. |
| 2 | `internal/api/grpc_server.go:905` | 6 | `sql.Stmt` | `sqlScopeCheck` — auth scope per DML kind | **FIX** — Move scope-table to a function on each `Stmt` type via a `RequiredScopes() []string` interface method. Same pattern would apply to `internal/api/http/query/query.go:375` (the HTTP twin). |
| 3 | `internal/api/http/query/query.go:375` | 6 | `sql.Stmt` | `sqlEnforceScope` — HTTP twin of #2 | **FIX** — Same as #2. |
| 4 | `internal/api/grpc_codec.go:18` | 11 | `cefaspb.AttributeValue_*` | protobuf-oneof → in-memory `types.AttributeValue` | **WAIVE** — codec dispatch; the cases are the wire vocabulary. |
| 5 | `pkg/client/codec.go:56` | 11 | `cefaspb.AttributeValue_*` | inverse of #4 (client) | **WAIVE** — same. |

Bonus (not in this list but found): `pkg/sql/planner.go:410` `describePredicate` — 7-case `Expr` switch for `EXPLAIN` output. Acceptable — `String()`/`Describe()` methods on every `Expr` would scatter that output format across many files.

---

## 4. DIP — interfaces declared in the implementer

**The Phase 11 audit found one reported "DIP violation" that on closer review is not one:**

`internal/storage/db.go:18` declares `Replicator`. The audit flagged it because `internal/replication.DB` is the only implementer. But the **consumer** of `Replicator` (the package that holds it as a field type and calls its methods) is `internal/storage.DB` itself (`AttachReplicator(r Replicator)`). By the hexagonal rule, the interface belongs with the consumer — which **is** `storage`. **No fix.**

There is, however, a structural awkwardness: `internal/cluster` knows about `storage.Replicator` because it wires `replication.DB` into `storage.DB`. That coupling is fine; the rebalancer/manager handle the wiring once at boot, and after that the storage layer never names `replication` directly. Documented for the record.

---

## 5. Data clumps (3+ fields traveling together)

| # | Clump | Found in | Disposition |
|---|---|---|---|
| 1 | `HeartbeatMS`, `ElectionMS`, `LeaderLeaseMS`, `CommitMS` | `replication.Config` (`internal/replication/db.go` near line 40-50); `cluster.Config` (`internal/cluster/manager.go:108-112`) | **TRACK** — Extract `replication.TimingConfig`. Mechanical, low risk, but touches both packages' constructors. |
| 2 | `ID`, `RaftAddr`, `HTTPAddr` | `cluster.NodeDescriptor` (`placement.go:93-100`); `PlacementApplyStep` (`internal/api/grpc_server.go:1439-1440`); `PlacementPlanStep` (`planner.go:38-42`) | **TRACK** — Extract `cluster.PeerEndpoints`. Moderate blast — many constructors. |
| 3 | `ID`, `Epoch`, `State` | `cluster.Shard` (`manager.go:44-54`); `cluster.ShardPlacement` (`placement.go:104-112`) | **WAIVE** — these are versioning fields native to two different aggregates (in-memory live shard vs persisted catalog entry). Extracting a `ShardMeta` couples them more than they should be. |
| 4 | `LastStartedUnix`, `LastFinishedUnix`, `LastSuccessUnix`, `LastFailureUnix`, `LastDurationSeconds` | `storage.ScheduledBackupStatus` (lines 46-56) | **WAIVE** — single struct (not a clump across structs). The status type is its own DTO. |

---

## 6. Package SRP

| Package | Concern mix | Disposition |
|---|---|---|
| `internal/api` | HTTP routing + gRPC stubs + cross-transport shard orchestration (`storageFor`, `allShards`, `compact`) live alongside the request handlers | **TRACK** — Phase 10 partially addressed this with `internal/api/http/*` sub-packages. Next step is extracting the shard-orchestration helpers into `internal/api/orchestrator` (or similar). Defer; touches every gRPC method. |
| `internal/storage` | LSM engine + backup machinery + scheduled-backup runner + TTL reaper + spatial query helpers + plugin index maintenance + condition engine | **TRACK** — Phase 10e attempted to carve out the Pebble adapter; the move was reverted because `*DB`-method files (backup, atomic, writer, etc.) cannot leave the package without an interface refactor. Re-attempt after a `storage.Engine` interface lands. |
| `internal/cluster` | Routing (`Router`, tokens) + placement (PlacementCatalog, planner) + topology orchestration (split/move/drain) + Raft manager lifecycle | **TRACK** — Phase 10a attempted the `routing` extraction; the move was reverted because `cluster.Manager` holds `*Router` and the routing package itself needs `cluster.PlacementCatalog`, producing an import cycle. Re-attempt after Manager's router becomes an injected dependency rather than constructed internally. |

---

## 7. Aggregate boundaries (post-Phase 10d)

| Bounded context | Aggregate root | Children | Boundary aligned with package? |
|---|---|---|---|
| catalog | `catalog.Catalog` | `TableDescriptor`, `StreamDescriptor` | ✓ Yes — Phase 10d cleanly separated `catalog/domain` (invariants) from `catalog` (Pebble adapter). |
| cluster | `cluster.Manager` (lifecycle) + `cluster.PlacementCatalog` (routing snapshot) | `Shard`, `ShardPlacement`, `NodeDescriptor`, `TokenRange` | ✗ Partial — `PlacementCatalog` is embedded inside `Manager`. Read-only routing clients pull it out via `Manager.Router().Catalog()`. Worth making it a first-class queryable aggregate. **TRACK**. |
| storage | `storage.DB` | none cohesively — items, backups, TTL records, change logs, plugin index metadata are all peers under one struct | ✗ No — `storage.DB` is a facade over multiple subsystems, not a true aggregate root. Same root cause as the SRP finding above. |
| replication | `replication.DB` | Raft log, snapshots, leader state | ✓ Yes — thin wrapper over `hashicorp/raft`. |

---

## 8. Phase 11 sub-PR queue

Ordered by value / risk ratio.

1. **`refactor/phase-11-sql-storage-isp`** — Split `pkg/sql.Storage` into `Reader` + `Writer` interfaces, composed by `Storage`. Adds godoc `Justification:` for `Reader` (6 methods on a single read aggregate). **Shipped in this batch.**
2. **`refactor/phase-11-plugin-isp-justifications`** — Add `Justification:` godoc lines to `IndexPlugin`, `AudiencePlugin`, `Planner`. Trivial documentation PR. (Follow-up.)
3. **`refactor/phase-11-sql-scope-polymorphism`** — Replace the two 6-case `sql.Stmt` type-switches (`grpc_server.go:905`, `query.go:375`) with a `Stmt.RequiredScopes()` method. (Follow-up.)
4. **`refactor/phase-11-raft-timing-clump`** — Extract `replication.TimingConfig`. (Follow-up.)
5. **`refactor/phase-11-streamcore-rename`** — Rename `internal/api/streamcore` to `internal/api/cdciter`. (Follow-up.)

Items 2–5 each tracked individually under issue #319 follow-ups.

---

## 9. Definition of Done

- [x] Audit report committed (this file).
- [x] Every finding has a disposition (FIX / WAIVE / TRACK).
- [x] Phase 11 batch ships the highest-value FIX (sql Storage Reader+Writer).
- [ ] Remaining FIX / TRACK items opened as follow-up sub-PRs against `refactor/phase-11-*` branches — listed in §8.
- [x] No newly-discovered fat interface (> 3 methods) without `Justification:` planned (queued in §8 item 2).
- [x] No package added on the §1 ban-list (no new violations).
- [x] No new stutter or `Get*` violation (clean).
- [x] Aggregate roots documented in §7 above; per-package `doc.go` updates queued via item 2.
