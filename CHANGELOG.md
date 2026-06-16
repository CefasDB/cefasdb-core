# Changelog

All notable changes to cefas are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `pkg/core/model/ids.go`: value-object types `ShardID`, `NodeID`,
  `StreamShardID`, `TableID`. Every VO ships with a validating
  constructor (`New*`), a test/fixture form (`Must*`), `String()`,
  and `encoding.TextMarshaler` / `TextUnmarshaler` that preserve the
  legacy wire form byte-for-byte. ADR-0005 documents the design.
- `ShardID` reserves `MaxUint32` for the new `UnroutedShardID`
  sentinel surfaced by `pkg/api/range_metrics.go` to flag operations
  the router could not place.
- `internal/testutil/wait` package (phase 2a): `Eventually`, `Never`
  and `For` helpers replace bare `time.Sleep` polling loops in the
  unit-test suite.
- `pkg/plugin/distancecontract.Run` (phase 2b): shared LSP contract
  exercised by every distance plugin (cosine, euclidean, hamming,
  jaccard, jarowinkler, levenshtein, manhattan, damerau, haversine).
- `internal/cluster/PlanStrategy` interface + registry (phase 4a):
  every `PlacementOperation*` constant is wired to one strategy
  file under `internal/cluster/plan_*.go`.
- `Makefile`, `.golangci.yml` v2, `.testcoverage.yml`, `.editorconfig`
  and a new `.github/workflows/ci.yml` (phase 0a). Quality gates run
  as warn-only until phase-0f flips them to blocking.
- `pkg/api/http/table` (phase 3a) + `pkg/api/http/httpx`: first
  extraction of the `pkg/api/server.go` handler monolith.
- ADR-0001 (refactor baseline) and ADR-0005 (value-object IDs)
  under `docs/adr/`.

### Changed

- **Wire-format-stable VO cascade.** Every signature taking a shard
  id, node id, stream shard id or table name across
  `internal/metrics`, `internal/cluster`, `pkg/api` and
  `internal/catalog` now consumes a `model.*` value object. JSON
  payloads, Prometheus label series, signed stream iterator tokens
  and gRPC string fields all stay byte-for-byte identical via the
  VOs' `TextMarshaler` / `TextUnmarshaler`.
- `pkg/api/server.go` shrinks from 2 085 LOC to ~2 000 (phase 3a) as
  the table resource moves into its own sub-package.
- `internal/cluster/planner.go` shrinks from 1 191 LOC to 171
  (phases 4a-4c) via the `PlanStrategy` extraction plus three
  per-concern files: `apply.go`, `tokens.go`, `helpers.go`. Every
  `planX` function sits below the playbook §9 ≤ 40-LOC cap.
- `Router.ShardForPK` / `ShardForUint64` (phase 1) return
  `(uint32, error)` instead of panicking on uncovered tokens.
  New typed sentinel `ErrNoShardForToken` wraps the diagnostic
  string with `epoch=N token=N`.

### Breaking

These are **v1 wire** behavioural sharpenings the VO migration
introduced. No production clusters exist; no API version bump is
warranted.

- `GetShardIterator` (HTTP + gRPC) returns `InvalidArgument` for
  shard ids that don't match the `shardId-NNNNNNNNNNNN` pattern.
  The previous behaviour returned `NotFound`, conflating "id is
  malformed" with "id is well-formed but unknown".
- `POST /v1/cluster/AddVoter` and `POST /v1/cluster/RemoveServer`
  reject empty or whitespace-bearing `id` at decode time instead of
  passing the value through to the underlying raft library.

### Deprecated

- `pkg/types.StreamShardIDSingle` (string const). Prefer
  `model.StreamShardIDSingle` (`model.StreamShardID`) and call
  `.String()` only at the wire boundary. Both values resolve to
  `"shardId-000000000000"`.

### Tooling

- Default `go test ./...` no longer runs
  `internal/cluster/elasticity_chaos_load_test.go` (now behind
  `//go:build integration`). The integration suite runs in a
  dedicated CI job with a 15-minute timeout.
- New `integration` CI job (warn-only until baseline stable).
