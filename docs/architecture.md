# Architecture overview

CEFAS is a DynamoDB-compatible key/value + document store with a
plugin-architected search and similarity surface. The engine is small
on purpose — specialized indexes, distance operators, approximate
counters, and audience workflows live as plugins behind a stable
boundary.

## Components

```mermaid
flowchart LR
  subgraph Client
    CLI[cefas CLI]
    SDK[Go SDK / pkg/client]
  end

  subgraph Server [cefas-server]
    GRPC[gRPC handlers<br/>pkg/api]
    Catalog[Catalog<br/>internal/catalog]
    Storage[Pebble storage<br/>internal/storage]
    Raft[Raft replication<br/>internal/raft]
    Manager[Multi-shard manager<br/>internal/cluster]
    Planner[Query planner<br/>pkg/core/query]
    Registry[Plugin registry<br/>pkg/plugin]
  end

  subgraph Plugins [Plugins]
    direction TB
    Idx[Index plugins<br/>bloom · trigram · geohash · …]
    Dist[Distance plugins<br/>levenshtein · cosine · haversine · …]
    Est[Estimator plugins<br/>hll · cms]
    Aud[Audience plugin<br/>select · dedup · freqcap]
  end

  CLI --> SDK --> GRPC
  GRPC --> Catalog
  GRPC --> Storage
  GRPC --> Planner
  GRPC --> Registry
  Storage --> Raft
  Storage --> Manager
  Planner --> Registry
  Registry --> Idx
  Registry --> Dist
  Registry --> Est
  Registry --> Aud
```

## Request lifecycle (write)

```mermaid
sequenceDiagram
  participant C as CLI / SDK
  participant G as gRPC handler
  participant Cat as Catalog
  participant S as Storage (Pebble)
  participant R as Raft (if attached)
  participant P as Plugin registry

  C->>G: PutItem (table, item, condition)
  G->>Cat: Describe(table) — fetch KeySchema, indexes
  G->>S: PutItemWith(td, item, opts)
  S->>R: Replicate(batch.Repr())
  R-->>S: Applied at majority
  S->>P: notify update hooks (TTL, plugin indexes)
  G-->>C: PutItemResponse
```

The same shape applies for `UpdateItem` (server translates an
UpdateExpression into a cefas SQL UPDATE then runs it through the
executor), `DeleteItem`, `BatchWriteItem`, and `TransactWriteItems`.

## Request lifecycle (read)

```mermaid
sequenceDiagram
  participant C as CLI / SDK
  participant G as gRPC handler
  participant Pl as Planner
  participant Reg as Plugin registry
  participant Idx as Index plugin (e.g. trigram)
  participant Dist as Distance plugin (e.g. levenshtein)

  C->>G: Query / TopK / GeoAudience
  G->>Pl: Plan(statement)
  Pl->>Reg: resolve operator → plugin
  Pl->>Idx: candidate set generation
  Pl->>Dist: post-filter / ranking
  G-->>C: streamed Items / TopKResponse
```

## Storage layout

Pebble holds every cefas namespace under a small set of prefixes:

| Prefix | Contents |
|---|---|
| `cefas/catalog/<table>` | Persisted `TableDescriptor` JSON. |
| `cefas/data/<table>/…` | Primary key/value rows. |
| `cefas/gsi/<table>/<index>/…` | Built-in GSI pointers. |
| `cefas/lsi/<table>/<index>/…` | Built-in LSI pointers. |
| `cefas/spatial/<table>/<index>/…` | Built-in geohash/Z-order pointers. |
| `cefas/ttl/<table>/<ttlAttr>/…` | TTL reaper bucket index. |
| `cefas/admin/backups/<name>` | Admin-named backup metadata. |

Plugin-backed indexes own their on-disk format. In v1 most of them
keep state in process memory; persistence via the
`pkg/core/ttl` + pebble-bucket seams is wired in follow-up work.

## Consensus + sharding

- Single-node mode: writes commit through the per-DB group-commit
  coalescer in `internal/storage/db.go`.
- Raft mode: a `Replicator` interface routes every write through
  `internal/raft/db.go`. Reads stay local.
- Multi-shard mode: `internal/cluster/manager.go` partitions tables
  by `pk_hash8` and replicates each shard with its own raft group.

## Plugin boundary

Plugins compile in via blank-imports in `pkg/plugin/builtins/`:

```go
import (
  _ "github.com/osvaldoandrade/cefas/pkg/plugin/bloom"
  _ "github.com/osvaldoandrade/cefas/pkg/plugin/trigram"
  // …
)
```

Each plugin's `init()` self-registers against `plugin.Default`. The
import-graph tests (`pkg/core/coregraph_test.go`,
`pkg/plugin/plugingraph_test.go`) guarantee plugins never reach back
into `internal/*` or engine packages — they only depend on
`pkg/core/*`. See [boundaries](boundaries.md) for the per-feature
classification.
