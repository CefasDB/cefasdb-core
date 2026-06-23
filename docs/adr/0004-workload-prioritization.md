# ADR 0004 — Workload Prioritization

## Status

Accepted (Phase 1 / catalog only). Scheduling enforcement lands in
#498 (Phase 3). This ADR locks the contract, the SL identity model,
the catalog placement, the scheduling algorithm choice, and the
backward-compatibility commitment.

## Context

CefasDB shares a single pool of read / write lanes across every
caller. A 64-worker bulk load and a 1-QPS interactive query queue on
the same lane workers; under saturation the interactive client
starves. ScyllaDB ships workload prioritization as a first-class
catalog object — service levels with shares + per-SL caps + per-SL
lane scheduling — and the same scaffolding maps cleanly onto our
existing per-shard lane infrastructure (`internal/storage/adapter/
pebble/lanes.go`).

Epic #489 spans 6 phases. This ADR lands with Phase 1 (#496) so
later phases can refer back to the locked choices instead of
relitigating each commit.

## Decision

### 1. A service level is a first-class catalog object

`ServiceLevelDescriptor` is persisted on shard 0 under
`KeyServiceLevel(name)` and replicated like every other catalog
record. Fields:

| Field | Purpose | Phase |
|---|---|---|
| `Name` | unique identity (`oltp`, `olap`, `batch`, …) | 1 |
| `Shares` | DRR weight; positive integer; relative across SLs | 3 |
| `MaxInFlight` | per-node concurrent in-flight cap | 4 |
| `MaxRowsPerSec` | hard rate cap, rows | 4 |
| `MaxBytesPerSec` | hard rate cap, bytes | 4 |

A caller resolves to exactly one SL (resolution chain in Phase 2,
#497).

### 2. Default service level is implicit

The name `default` is reserved. It always exists with `Shares=1` and
no caps. `CREATE SERVICE LEVEL default` and `DROP SERVICE LEVEL
default` are rejected with `ErrServiceLevelReserved`. This keeps
every pre-existing connection working with zero migration —
backward compatibility is non-negotiable.

### 3. Scheduling algorithm: Deficit Round-Robin (DRR)

Alternatives considered:

| Algorithm | Pros | Cons | Decision |
|---|---|---|---|
| **DRR** | O(1) per packet, work-conserving, simple to reason about, supports starvation-free fair use | quantum size needs tuning per cluster | **chosen** |
| WFQ (weighted fair queueing) | strict bandwidth guarantees | O(log n) per packet, virtual-time bookkeeping is complex | rejected for v1 |
| CFS (completely fair) | beautiful theory | tuned for CPU, doesn't translate to mixed CPU+IO lane work | rejected |
| Hierarchical token bucket | flexible | needs two control loops (token refill + scheduling) | rejected |

DRR's quantum will be sized in Phase 3 (#498) at install time per
lane width; the locked decision here is the algorithm choice itself.

### 4. CPU and IO shares are unified

Pebble's commit pipeline mixes CPU work (encode, batch, fsync) with
IO (LSM flush, compaction). Splitting CPU shares from IO shares
would require either (a) two control loops with their own deficit
counters, or (b) instrumenting every micro-step. Both are out of
scope for v1.

A single `Shares` field governs the SL's quantum in the per-shard
lane scheduler. If a workload turns out to need IO-only or CPU-only
shaping, a future ADR can split this in two.

### 5. Caps are per-node, not cluster-wide

`MaxInFlight` / `MaxRowsPerSec` / `MaxBytesPerSec` apply per server
process. Cluster-wide caps require a coordinator (a single token
bucket) that becomes a write-path bottleneck and adds a leader
dependency. Per-node caps × node count = approximate global cap;
operators size accordingly.

### 6. Raft Apply is exempt

Raft Apply on the FSM must not be subject to per-SL queueing. A
caller's SL controls how their request enters the server; once the
batch is in the log, applying it to followers is cluster invariant.
Phase 3 wires SL lane partitioning at the RPC handler entrance, not
at the storage commit path.

### 7. Catalog-only in Phase 1

`CREATE / ALTER / DROP / LIST SERVICE LEVEL` works end-to-end via
SQL DDL + the gRPC handlers in `grpc_service_level.go`, persisted to
shard 0 via raft like every other descriptor. No scheduler change
yet — every request still uses the global lane. A Phase 1 test
proves this explicitly.

## Consequences

- **Phase 2** (#497) wires caller → SL resolution (metadata header,
  bearer claim, table tag, default fallback). No enforcement still.
- **Phase 3** (#498) partitions per-shard lanes by SL using DRR.
- **Phase 4** (#499) wires `MaxInFlight` / `Max*PerSec` caps;
  exceeding returns `codes.ResourceExhausted`.
- **Phase 5** (#500) exposes per-SL metrics + admin pause/resume.
- **Phase 6** (#501) is the chaos validation gate: one tenant
  floods, the other stays within SLO.

## Alternatives rejected

- **Static SL per connection at handshake** — would require client
  changes and a connection multiplex (e.g. SNI by SL). Resolution
  chain via metadata header in Phase 2 is the same expressive
  power without breaking existing clients.
- **SL as an auth scope** — auth scopes are about *what you can do*;
  SLs are about *how fast you can do it*. Conflating the two leaks
  resource policy into IAM.
- **Cluster-wide caps with a coordinator service** — see §5; not
  shipping in v1.
