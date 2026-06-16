# ADR-0006: HTTP + gRPC server code under internal/api

- **Status**: accepted
- **Date**: 2026-06-16
- **Tracking issue**: #311
- **Related**: ADR-0002 (bounded contexts, deferred)

## Context

`pkg/api/` was the catch-all for everything API-shaped: 45 Go files
mixing HTTP handlers (`server.go`, `streams.go`,
`range_metrics.go`), gRPC handlers (`grpc_server.go` plus
~25 `grpc_*.go`), generated proto stubs (`pkg/api/proto/`), helpers,
and HTTP sub-packages (`http/table/`, `http/httpx/`).

Per the playbook §1 rule:

> `pkg/` is ONLY for code intended for import by OTHER modules. If
> in doubt, use `internal/`.

The only `pkg/api` callers from outside the module candidate set
are:

| Importer | Imports |
|---|---|
| `cmd/cefas-server/main.go` | `pkg/api` for HTTP/gRPC server impls |
| `pkg/client/client_test.go` | `pkg/api` to spin a test server |
| `cmd/cefas-cli/cmd/...` | `pkg/api` for some types |
| `pkg/core/coregraph_test.go` | `pkg/api` for boundary tests |
| `pkg/plugin/plugingraph_test.go` | same |

Every consumer is **inside this module**. No external project
imports `pkg/api`. The package was misplaced.

`pkg/api/proto/` is different — `pkg/client` (the SDK) imports it,
and the SDK is the actual external surface. Generated proto stubs
also need to stay in a stable import path for external callers.

## Decision

Move every `pkg/api` file except `pkg/api/proto/` to
`internal/api/`. The package name stays `api`; only the import path
changes. The proto package stays at `pkg/api/proto/` as the SDK's
public boundary.

```
pkg/api/                            (before)
├── proto/                          → stays (SDK consumes)
├── http/
│   ├── httpx/                      → moved to internal/api/http/httpx
│   └── table/                      → moved to internal/api/http/table
├── server.go                       → moved to internal/api/server.go
├── grpc_server.go                  → moved
├── grpc_*.go (25 files)            → moved
├── streams.go, range_metrics.go    → moved
└── ...
```

## Why not split `grpc_*.go` into a subpackage in this PR

The 25 `grpc_*.go` files all live in the same `package api` because
they each have method receivers on the unexported `GRPCServer`
struct and call ~15 unexported helpers (`s.cat`, `s.db`,
`mapStorageErr`, `requireScope`, `streamShardIteratorPayload`,
`encodeStreamShardIterator`, `observeStreamIteratorFailure`,
`streamTableForARN`, `findStreamShard`, …).

Go has no "subdirectory in the same package" concept — files in a
subdirectory are a separate package. Splitting `grpc_*.go` into
`internal/api/grpc/` requires either:

1. **Exporting ~30 identifiers** the helpers and the server fields
   touch. That trades one organisational problem for a much larger
   API-surface problem.
2. **Extracting a shared `internal/api/serverkit/` package** that
   the HTTP server and the gRPC server both depend on, holding the
   common state and helpers. That is a real refactor and warrants
   its own PR with its own design ADR.

Deferred to a follow-up. Recorded as "remaining work" against #311
so the next pass starts with the shared-state extraction.

## What this PR does NOT close

Issue #311 (phase 3) asks for **per-resource handler extraction**
from `server.go` (2 007 LOC) and `grpc_server.go` (1 833 LOC) into
per-resource subpackages (`item/`, `query/`, `stream/`, `backup/`,
`plugin/`, `bandit/`, `cluster/`). Phase 3a (#325) extracted
`table/`. This PR delivers the structural pkg→internal move that
makes the remaining extraction sit in the right place, but does
not perform the remaining extractions themselves — each one needs:

- A request-type relocation
- An auth/metrics middleware decision
- Handler tests (HTTP-level tests didn't exist before #325)

Doing all eight resources at once would balloon this PR past any
reasonable review size. Each per-resource extraction will land as a
focused PR (3b..h) on top of this move.

## Consequences

### Positive

- The import graph documents intent: `internal/api/` is server
  code; `pkg/api/proto/` is the SDK boundary.
- `pkg/` no longer hosts code that has no external consumers.
- `internal/api/` becomes the staging area for the resource-level
  reorganisation (phase 3b..h) without a wider rename later.

### Negative

- 6 import-path changes across the codebase (5 in non-test code).
  Mechanical sed; CI proves correctness.
- gRPC files still flat. A future PR will extract a `serverkit/`
  shared package and split server / grpc into their own
  subpackages.

### Neutral

- Package name stays `api`. The semantic move is "where it lives",
  not "what it's called".
- No behavioural change; wire format unchanged.

## Verification

```
$ go build ./...
$ go vet ./...
$ go test ./...
```

All green across the 52 packages.
