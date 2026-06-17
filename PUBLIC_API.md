# Public-API contract: what lives in `pkg/`

This document is the source of truth for which packages under `pkg/`
are part of the public Go API surface that downstream consumers
(third-party modules, the `npm` wrapper, custom integrations) can
`go get` and import directly.

The audit was produced as **PR 4** of the repo-restructure series
and informs **PR 5** (move internal-only packages into `internal/`)
and **PR 6** (protocol/API cleanup).

## Audit results

In-repo importer counts measured with
`grep -rln '"github.com/osvaldoandrade/cefas/<pkg>"' --include="*.go" .`
on commit `9af22a9` (post PR 3).

### Top-level `pkg/`

| Package | In-repo importers | External-API status | Action | Rationale |
|---|---:|---|---|---|
| `pkg/api/` (empty wrapper) | 0 | n/a — no `.go` files at this level | **FLATTEN** in PR 6 | The directory only exists to hold `proto/`. Hoist `pkg/api/proto/` to `pkg/protocol/` and drop the empty wrapper. |
| `pkg/api/proto/` | 49 | **KEEP** as the gRPC wire contract | Rename to `pkg/protocol/` in PR 6 | Generated protobuf code that defines the gRPC service surface. Any Go client of cefas builds against it. |
| `pkg/client/` | 25 | **KEEP** — the Go SDK | None | The blessed in-process Go client (`Client`, `TableClient`, etc.). Largest documented exported surface; downstream code goes through this. |
| `pkg/config/` | 5 | **KEEP** — declarative config schema | None | Defines the YAML/env-vars/flag schema for `cefasdb`. Operators may build wrappers that compose `config.Config` values; embedding teams may load it. |
| `pkg/core/` (top-level) | 0 | move | **MOVE → `internal/core/`** in PR 5 | Top level holds only `doc.go` + two graph/satisfaction tests; no exported types referenced externally. The real content is in sub-packages — see below. |
| `pkg/core/condition/` | 2 | private — only internal `pkg/sql` and `internal/server` consume | **MOVE → `internal/core/condition/`** | Condition-expression parser. Used by the SQL executor and the gRPC handler; no external use. |
| `pkg/core/index/` | 36 | private | **MOVE → `internal/core/index/`** | `index.Descriptor` and helpers — consumed by plugins (`pkg/plugin/*`) and internal storage paths. **Note**: moving this requires the plugin packages to re-import from the new path; treat as part of the same PR. |
| `pkg/core/model/` | 98 | private | **MOVE → `internal/core/model/`** | The aggregate-root domain IDs (NodeID, ShardID, StreamShardID, KeySchema). Most-imported package in the repo. **Move with care**: large blast radius. |
| `pkg/core/query/` | 16 | private | **MOVE → `internal/core/query/`** | Query primitives (`DistanceOp`, `TopKResult`, planner helpers). |
| `pkg/core/query/mmr/` | 4 | private | **MOVE → `internal/core/query/mmr/`** | Max-Marginal-Relevance re-ranker. |
| `pkg/core/stream/` | 1 | private | **MOVE → `internal/core/stream/`** | Stream primitives. |
| `pkg/core/ttl/` | 2 | private | **MOVE → `internal/core/ttl/`** | TTL primitives. |
| `pkg/ddbjson/` | 24 | **KEEP** — DynamoDB JSON wire format | None | Public-API parity with AWS DynamoDB JSON. External clients porting from `aws-sdk-go` need this. |
| `pkg/plugin/` | 12 (+ sub-package importers) | **KEEP** — third-party plugin SDK | None | The contract any external plugin author writes against. Sub-packages (`audience`, `bandit`, `bloom`, etc.) are the built-in plugin implementations and are also public — third-party operators may import them by name. |
| `pkg/plugin/internal/{hashfield,pkid,vecattr}/` | various | private (Go-internal rule applies) | None | The `internal` sub-package keeps these reachable only from `pkg/plugin/...` per Go's visibility rule. Already correctly scoped. |
| `pkg/query/` | 0 | empty directory | **DELETE** in PR 5 | Empty placeholder with no `.go` files. Cruft from an earlier reshape. |
| `pkg/sql/` | 8 | private — only `internal/server` + `cmd/cefasctl` consume | **MOVE → `internal/sql/`** in PR 5 | SQL parser/planner/executor. The wire surface is the gRPC `ExecuteStatement` RPC; no external user imports the Go package directly. |
| `pkg/types/` | 109 | **KEEP** — DTO surface for the public API | None | `TableDescriptor`, `Item`, `AttributeValue`, `StreamDescriptor`, every error sentinel returned by the gRPC handler. The lingua franca of the public surface; the SDK (`pkg/client`) and the wire codec (`pkg/ddbjson`) both depend on it. |

## Summary

After PR 5 + PR 6 land, `pkg/` will contain exactly:

```
pkg/
├── client/      ← the Go SDK
├── config/      ← config schema for operators
├── ddbjson/     ← DynamoDB JSON wire format
├── plugin/      ← third-party plugin SDK + built-ins
├── protocol/    ← gRPC wire format (renamed from pkg/api/proto)
└── types/       ← public DTO vocabulary
```

Six packages, each with a clear external contract.

## Shim policy (informs PR 5)

For every `pkg/core/*` and `pkg/sql` moved to `internal/`:

- **No shim** for packages with zero external importers in this repo
  AND no documented external use. The move is mechanical (`git mv`
  + import-path update across in-repo consumers).
- **Type-alias shim** in the old location if a sub-package is
  exceptionally widely-imported AND the move risks silent breakage
  for an unknown external user — leave a deprecated `pkg/X/doc.go`
  that re-exports the canonical `internal/X` types with a clear
  `// Deprecated: ...` notice. Drop after one minor release.

Decision per package, to be confirmed with the operator before PR 5
executes:

| Package | Shim? | Rationale |
|---|---|---|
| `pkg/sql` | **No** | 8 in-repo importers, no `npm/` or third-party use known. |
| `pkg/core/condition` | No | 2 importers, internal. |
| `pkg/core/index` | **Yes** (deprecated alias) | 36 importers (including plugin authors who may write external plugins against it). |
| `pkg/core/model` | **Yes** (deprecated alias) | 98 importers; the aggregate-root IDs are the kind of type a downstream tool might keep. |
| `pkg/core/query` | **Yes** (minimal shim — DistanceOp only) | 16 internal importers + the third-party plugin contract uses `query.DistanceOp`. Discovered during PR 5 execution; the shim re-exports just that one interface. |
| `pkg/core/query/mmr` | No | 4 importers, internal. |
| `pkg/core/stream` | No | 1 importer. |
| `pkg/core/ttl` | No | 2 importers. |

The aliases in `pkg/core/{index,model,query}` keep the
`go get`-stability promise for plugin authors who may have written
custom plugins against them while we migrate.
