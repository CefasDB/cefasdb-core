# ADR-0002: bounded-context boundaries (pkg/core, internal, pkg/client)

- **Status**: accepted
- **Date**: 2026-06-16
- **Tracking issue**: #317
- **Parent epic**: #307
- **Related**: ADR-0005 (value-object IDs), ADR-0006 (HTTP + gRPC server code under internal/api)

## Context

The cefas module grew over phases 3–7 from a single flat `pkg/api`
into a layered layout with three intended audiences: the pure
domain (importable by anyone), the server (importable by no one
outside the binary), and the SDK (importable by external repos).
The boundaries existed in commit history but not in writing.

ADR-0006 records the `pkg/api → internal/api` move (#334) and
explicitly names the bounded-context question as deferred to a
separate ADR. This document discharges that debt.

The phases that established the current shape:

| PR(s) | What it established |
|---|---|
| #284 | `pkg/core` extraction baseline (domain split from API) |
| #325, #335, #336, #337, #338, #339 | phase-3 per-resource handler carve-out under `internal/api/http/<resource>/` |
| #334 | `pkg/api → internal/api` move (recorded in ADR-0006) |
| #345 – #352 | phase-7 SDK split: `pkg/client` carved from the server tree |

Without an ADR, future contributors have nothing to point to when
they wonder why `internal/api` cannot import `pkg/client`, why
`pkg/core` cannot reach into `internal/catalog`, or why `pkg/api`
contains only `proto/`.

## Decision

Adopt three bounded contexts, enforced by Go's `internal/` rule and
by the directory layout. The contexts are:

### 1. `pkg/core/` — domain

Pure domain model. Contains:

- `condition/` — predicate expressions over items
- `index/` — index descriptors and capability metadata
- `model/` — value objects, including the IDs from ADR-0005
- `query/` — query AST
- `stream/` — change-stream record shapes
- `ttl/` — time-to-live policy

No package under `pkg/core/` references HTTP, gRPC, storage, raft,
or any wire concern. The graph test
`pkg/core/coregraph_test.go` enforces this by failing the build if
any `pkg/core/...` file imports `github.com/osvaldoandrade/cefas/internal/...`
or `github.com/osvaldoandrade/cefas/internal/api`.

Both server (`internal/api/`) and SDK (`pkg/client/`) depend on
`pkg/core/`.

### 2. `pkg/api/proto/`, `pkg/client/`, `pkg/plugin/`, `pkg/ddbjson/`, `pkg/sql/`, `pkg/types/` — published surface

- `pkg/api/proto/` — generated protobuf stubs. The wire contract.
  Everything else under `pkg/api/` was moved out by #334; this
  directory remains because the SDK and external tooling import
  it.
- `pkg/client/` — the Go SDK. Wraps the gRPC stubs in a
  hand-written ergonomic API. External repositories consume this
  package directly.
- `pkg/plugin/` — pluggable distance functions and index
  implementations. Loaded at boot by the server and consumed by
  external plugin authors.
- `pkg/ddbjson/`, `pkg/sql/`, `pkg/types/` — wire-format adjacent
  utilities (DynamoDB JSON, SQL parsing, shared primitive
  helpers) used by both the server and the SDK.

These packages are the **only** import targets a downstream
project may use. They share the constraint that they must not
import anything under `internal/`.

### 3. `internal/` — server-private

Server-only code. Go's compiler refuses to let any package outside
this module import a package under `internal/`. Inside the module,
nothing outside the binary entry points (`cmd/cefas-server`) should
need these packages either.

The current contents:

- `api/` — HTTP + gRPC handlers (post-#334), with one subpackage
  per resource under `api/http/<resource>/` (post-#325, #335–#339)
- `auth/` — request authentication
- `catalog/` — placement catalog, source of truth for shard/node
  assignment
- `cluster/` — cluster membership, drain, decommission
- `metrics/` — Prometheus collectors and labels
- `raft/` — replicated log
- `rebalancer/` — shard rebalancing
- `spatial/` — geohash and spatial helpers
- `storage/` — KV storage interface and adapters
- `tracing/` — OpenTelemetry wiring
- `audiencestore/` — audience-segment persistence
- `bootstrap/` — server-side bootstrap and wiring

### 4. `cmd/` — binary entry points

Three binaries:

- `cefas-server` — the long-running server
- `cefas-cli` — operator CLI (with its own `cmd/cefas-cli/internal/`
  for CLI-private helpers)
- `cefas-loadtest` — load-generation harness

`cmd/` is the only place where dependencies are wired into a
running process. No package outside `cmd/` imports anything under
`cmd/`.

## Invariants

The decision is the boundary; these are the rules a reviewer
checks on every PR:

1. **`pkg/core/` never imports `internal/`.** Enforced by
   `pkg/core/coregraph_test.go`.
2. **`pkg/client/` never imports `internal/`.** Enforced by Go's
   `internal/` visibility rule for external consumers; in-module
   use is enforced by review. `pkg/client/client_test.go` is the
   one exception — it stands up a test server using
   `internal/catalog`, `internal/metrics`, and `internal/storage`.
   The test binary is not part of the SDK's distributable surface.
3. **`internal/api/http/<resource>/` never imports
   `internal/api/`.** Dependencies flow parent → child only. Each
   per-resource handler package depends on `internal/api/` types
   only via constructor injection from `internal/api/server.go`.
4. **`cmd/` is the only place where binaries wire dependencies;
   nothing else imports `cmd/`.** Enforced by Go's `internal/`
   rule via `cmd/cefas-cli/internal/` and by review elsewhere.

## Consequences

### Positive

- A new contributor can read this document and the directory
  layout and infer where new code belongs without reading PR
  history.
- The compiler enforces invariant 2 (and invariant 4 for the CLI)
  via `internal/`. Invariants 1 and 3 are enforced by graph tests
  or by review against an explicit rule rather than tacit
  understanding.
- The pkg/internal split distinguishes "code we ship to external
  consumers" from "code that happens to be reusable inside the
  module". New packages start in `internal/` by default;
  promotion to `pkg/` requires an explicit external consumer.

### Negative

- The boundary is partly documentation rather than mechanical
  enforcement. Invariants 1 and 3 rely on graph tests and review
  rather than the compiler, so a violation that slips review
  reaches main if the graph test does not cover the offending
  edge.
- `pkg/client/client_test.go` legitimately needs server-private
  packages to spin a test server. The exception is documented
  here so a future cleanup does not "fix" it by inverting the
  dependency.
- The split locks in a current snapshot. If `internal/spatial/`
  or `internal/audiencestore/` later needs to be consumed by a
  sibling product, it must move to `pkg/` (a wire-visible event)
  rather than gain a quiet external importer.

### Neutral

- No runtime behaviour change. The split is structural; binaries
  produced before and after this ADR are byte-equivalent.
- The package names inside `internal/api/` stay as they are
  (per ADR-0006). This ADR documents the boundary, not a rename.

## Verification

```
$ go build ./...
$ go test ./...
```

The graph tests (`pkg/core/coregraph_test.go`,
`pkg/plugin/plugingraph_test.go`) fail the build if invariant 1
or its plugin equivalent is violated. The remaining invariants
are checked by review against this document.
