# Public-API contract: what lives in `pkg/`

This document is the source of truth for which packages under `pkg/`
are part of the public Go API surface that downstream consumers
(third-party modules, the `npm` wrapper, custom integrations) can
`go get` and import directly.

The audit was first produced as **PR 4** of the repo-restructure
series. After **PR 7** (module-path migration + final package
hygiene) the public surface is locked down to four packages.

## Final public surface

| Package | Role |
|---|---|
| `pkg/client/` | The Go SDK. The blessed in-process client that talks to a `cefasdb` over HTTP/JSON or gRPC. |
| `pkg/plugin/` | The plugin SDK third-party plugin authors implement against — `Lifecycle`, `Descriptor`, `IndexPlugin`, `AudiencePlugin`, `BanditPlugin`, `DistancePlugin`, the registration API, and the test harness. |
| `pkg/protocol/` | Generated protobuf + the `.proto` source. Defines `cefas.v1` on the wire. |
| `pkg/types/` | Public DTO vocabulary — `TableDescriptor`, `Item`, `AttributeValue`, `StreamDescriptor`, and every error sentinel the gRPC handler surfaces. |

That's it. Four packages, each with a clear external contract.

## What moved to `internal/` in PR 7

| Old path | New path | Why |
|---|---|---|
| `pkg/config/` | `internal/config/` | Boot/runtime schema for `cefasdb` only — operators load it via the binary, not via a Go import. |
| `pkg/ddbjson/` | `internal/compat/ddbjson/` | DynamoDB-JSON encode/decode helpers — wire-format adapter code, not a public Go contract. |
| `pkg/plugin/<23 built-ins>/` | `internal/plugin/builtin/<each>/` | The implementations (cosine, euclidean, geohash, jaccard, …) cefasdb ships with. The SDK in `pkg/plugin/` is the contract; the built-ins are the reference implementations. |
| `pkg/plugin/builtins/` | `internal/plugin/builtin/registry/` | Side-effect-import registry that wires every built-in into `plugin.Default`. The binary imports it at startup. |
| `pkg/plugin/internal/{hashfield,pkid,vecattr}/` | `internal/plugin/internal/<each>/` | Helpers shared across the built-ins, never reached from out-of-tree plugins. |
| `pkg/core/{index,model,query}/` shims | — (deleted) | Deprecated migration shims added in PR 5. The PR 7 module-path migration is itself a hard break, so the shims served their purpose. External plugin authors import `internal/core/*` directly (or, for the `model.X = types.X` aliases, just `pkg/types`). |
| `pkg/core/` | — (deleted) | Empty after the shim removal. |
| `pkg/sql/` | `internal/sql/` | Moved in PR 5; remains internal. |
| `pkg/api/proto/` | `pkg/protocol/` | Renamed in PR 6. |

## Module path migration (PR 7 commit A)

```
github.com/osvaldoandrade/cefas
              ↓
github.com/CefasDb/cefasdb
```

Touches every `.go` file, `go.mod`, the `.proto`'s `option
go_package`, `scripts/genproto.sh`, deploy (Dockerfile, Helm
chart), CI (`.github/workflows/release.yml`), `.golangci.yml`,
`Makefile`, `npm/` wrapper, and the in-repo docs. The protobuf
generated files (`cefas.pb.go`, `cefas_grpc.pb.go`) are
regenerated via `protoc` so the embedded `FileDescriptorProto`
keeps a valid length prefix.

External Go importers update their imports + `go get` paths;
gRPC clients in any language see no change (the on-the-wire
`package cefas.v1` and every message/RPC name + field number is
unchanged).

## Architecture invariants enforced by tests

`pkg/plugin/plugingraph_test.go` enumerates the engine surfaces
plugin code must never import — `internal/server`,
`internal/sql`, `internal/storage`, `internal/cluster`,
`internal/placement`, `internal/routing`, `internal/replication`,
`internal/catalog`, `internal/rebalance`, `internal/bootstrap`,
`internal/metrics`, `internal/config`, `internal/compat`, and
`pkg/client`. The `internal/core/*` packages are the shared
plugin kernel (Descriptor, Lifecycle, DistanceOp, the
model-aliased types) and are intentionally allowed.

## Wire-format stability (unchanged across the whole restructure)

- Protobuf `package cefas.v1;`
- gRPC service name `cefas.v1.Cefas`
- Every message and RPC name and field number
- HTTP JSON request/response shapes (DynamoDB-compatible)
- Persisted Pebble keys (catalog, primary rows, GSI/LSI pointers, streams, TTL, plugin indexes)
- Raft transport and snapshot format
- Metric names (still `cefas_*`)
- Error sentinels and their text
- `cefasdb` CLI flag names and config keys
