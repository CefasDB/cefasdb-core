# ADR-0003: per-resource HTTP handler decomposition

- **Status**: accepted
- **Date**: 2026-06-16
- **Tracking issue**: #317
- **Related**: ADR-0006 (pkg→internal/api move, phase 3 deferred work)
- **Phases**: 3a table (#325), 3b item (#346), 3c query (#336), 3d stream (#339), 3e backup (#335), 3f cluster (#338)

## Context

After ADR-0006 landed the pkg→internal move, the umbrella issue
explicitly deferred "phase 3a..h": per-resource extraction of the HTTP
handler surface out of `internal/api/server.go`.

That file was 2 014 LOC of mixed concerns:

- table CRUD
- item operations (get / put / delete / batch)
- queries (`Query`, `Scan`, spatial fan-out)
- stream iterators (HTTP + shared iterator state)
- backup and restore
- cluster admin (add / remove voter, drain, decommission)

Cyclomatic and cognitive complexity of several handler methods
routinely exceeded the playbook §9 thresholds (cyclomatic > 15,
cognitive > 20). Test coverage was difficult because helpers were
methods on `*Server`, so any new test had to build a full server with
storage, catalog, auth, metrics, and middleware wired in.

The structural carve-out from ADR-0006 left the right home
(`internal/api/http/<resource>/`) but did not move any handler. That
is what this ADR records.

## Decision

Each HTTP resource lives in its own sub-package under
`internal/api/http/<resource>/`. The shape per package is fixed:

```
internal/api/http/<resource>/
├── handlers.go        // Handlers struct + New(...) constructor + HandleX methods
└── handlers_test.go   // HTTP-level tests using net/http/httptest
```

The `Handlers` struct holds typed dependencies (a `Deps` struct on
some resources, per-arg constructor on simpler ones) plus
function-typed fields for cross-cutting helpers that still live on
`*Server`.

Allowed imports inside each sub-package:

- `internal/catalog`, `internal/auth`, `internal/storage`
- `internal/api/http/httpx` (shared `WriteJSON` / `WriteErr`)
- `pkg/types`

Forbidden import: `internal/api/` itself. The sub-packages are leaves,
not siblings of the server. This breaks the circular temptation of
"just call back into `*Server` for one helper".

### Wiring

`Server.Routes(mux)` instantiates each resource's `Handlers` once and
binds routes through the existing `register(...)` helper, which keeps
the auth + metrics middleware chain identical to the pre-split
behaviour:

```go
tableH := tablehttp.New(s.cat, s.storageFor, ...)
register(mux, http.MethodPost, "/v1/tables", tableH.HandleCreate)
register(mux, http.MethodGet,  "/v1/tables/{name}", tableH.HandleDescribe)
// …
```

No new middleware layer; no per-package router; no `chi`-style
sub-routes. The seam is the constructor, not the dispatch.

### Helper injection

Several handlers need server-level helpers that depend on `*Server`
state (storage routing, shard fan-out, catalog fan-out, strong-read
gating). Rather than export those helpers or duplicate them, they are
injected into the `Handlers` struct as function-typed fields:

- `StorageFor       func(catalog.Table) (storage.Engine, error)`
- `BatchWriteByShard func(ctx, batch) (BatchResult, error)`
- `SpatialAllShards func(ctx, query) ([]Row, error)`
- `EnsureStrongRead func(ctx) error`
- `FanOutCatalog    func(ctx, op) error`

The resource sub-package never sees `*Server`. The server stays the
owner of the cross-cutting state; the sub-package is a pure
request/response shaper.

### Stream variant: `internal/api/streamcore/`

Stream handlers needed a shared core: iterator codec, pagination
cursor, shard resolution. That core is consumed by both the HTTP
handlers (`internal/api/http/stream/`) and the gRPC handlers (still in
`internal/api/grpc_*.go`). Duplicating it would have created two
sources of truth for the iterator wire format.

We extracted `internal/api/streamcore/` as the single source of truth.
Both HTTP and gRPC depend on it; neither depends on the other. This
is a consequence of the pattern, not an exception — when two callers
need the same core logic, the core becomes a package, not a method on
`*Server`.

## Consequences

### Positive

- `internal/api/server.go` shrunk from 2 014 LOC to ~150 LOC: the
  `Server` struct, attach methods, helper definitions, and `Routes()`.
- Every per-resource file sits well under the §1 file-size cap and
  the §9 complexity thresholds.
- HTTP-level tests are now feasible per resource — `httptest` + a
  `Handlers` with mocked function fields, no full server boot.
- The auth + metrics middleware path is unchanged; rollout was a pure
  refactor with no observable wire impact.
- New resources follow the template by construction — there is no
  "where does this handler go" debate.

### Negative

- Six PRs to land the full carve-out (#325, #346, #336, #339, #335,
  #338). Each one had to land its own request types, tests, and
  fixture updates. The strangler-fig pace was deliberate to keep
  each PR ≤ 600 LOC of diff and reversible.
- Function-typed helper fields are weaker than method dispatch — a
  nil field panics at request time, not at compile time. Each
  constructor asserts non-nil for the fields it requires.
- `streamcore/` is now a third location for iterator logic (HTTP
  package, gRPC files, core). The win is single source of truth; the
  cost is the indirection.

### Neutral

- Package name per resource is `<resource>http` (e.g. `tablehttp`,
  `itemhttp`), matching the import alias used at the call site in
  `Routes()`.

## Out of scope (recorded as follow-up)

- gRPC handlers still live flat in `internal/api/grpc_*.go`. The
  per-resource split on the gRPC side requires the `serverkit/`
  shared-state extraction called out in ADR-0006. Tracked against
  #311 / #307.
- Plugin and bandit handlers (phase 3g / 3h in the original plan) are
  small enough that the carve-out was not prioritised; they remain in
  `server.go` until a behavioural change forces the move.

## Verification

```
$ go build ./...
$ go vet ./...
$ go test ./...
```

All green across the packages touched by phases 3a..3f.
