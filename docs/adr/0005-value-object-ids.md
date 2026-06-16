# ADR-0005: value-object IDs

- **Status**: accepted
- **Date**: 2026-06-16
- **Phases**: 5a (#329), 5b.1 (#330), 5c.1 (#331), 5b.2+5c.2 (#332), 5d+5e (this PR)
- **Tracking issue**: #313

## Context

Before phase 5 the codebase passed identifiers as bare primitives at
every layer:

- shard ids as `uint32`
- node ids as `string`
- stream-shard ids as `string` (with a free-standing
  `types.StreamShardIDSingle = "shardId-000000000000"` const)
- table ids as `string`

That choice produced concrete bugs the type system could not catch:

- `rangeMetricRoute` returned `"0"` for both "no cluster manager"
  and "shard 0" — two semantically distinct cases collapsed into one
  string (caught and pinned in #330).
- `createStreamShardIterator` validated `shardID == ""` inside the
  function body; two boundary handlers (HTTP, gRPC) each repeated
  the same check.
- `placementNodeActiveReferences(cat, nodeID string)` accepted any
  string, including those that would never round-trip as legal node
  ids.

The audit (issue #313) called for a v2 wire bump as the vehicle for
this cleanup. After phase-5a..c shipped the safer surface change we
re-scoped (see "Decision" below) because no production cluster
exists yet — the v2 bump was paying a migration cost with no
benefit.

## Decision

Introduce four value objects in a new package
`pkg/core/model/ids.go`:

- `ShardID         struct{ v uint32 }`
- `NodeID          struct{ v string }`
- `StreamShardID   struct{ v string }`
- `TableID         struct{ v string }`

Every VO is a **struct, not a type alias**, so callers must go
through the validating constructor (`NewShardID` /  `MustShardID`
etc.). Each VO implements `encoding.TextMarshaler` /
`TextUnmarshaler` with the same byte sequence the bare primitive
used to emit, so JSON / gRPC payloads stay byte-for-byte identical.

`ShardID` reserves `MaxUint32` for `UnroutedShardID`, the sentinel
used by `pkg/api/range_metrics.go` to flag operations the router
could not place.

## Cascade strategy (Strangler Fig)

We cascaded the VOs through the codebase one surface at a time:

| Phase | Scope | PR |
|---|---|---|
| 5a | introduce types + tests | #329 |
| 5b.1 | `internal/metrics` signatures | #330 |
| 5c.1 | `pkg/api/streams` + parameter object | #331 |
| 5b.2 + 5c.2 | `internal/cluster` (drain, decommission, apply) + `pkg/api` cluster admin requests | #332 |
| 5d + 5e | `internal/catalog` source-of-truth + ADR + CHANGELOG | this PR |

At every step the wire format stayed identical (TextMarshaler / TextUnmarshaler), so each PR was reversible by `git revert` with no client-side migration.

## Decision: do NOT bump to v2

The phase-5 plan originally required:

- gRPC `cefas.v1` → `cefas.v2`
- HTTP `/v1/` → `/v2/`
- Go module path `github.com/osvaldoandrade/cefas` →
  `…/cefas/v2`
- Major bump of `cefas-cli` and `npm/package.json`
- Migration guide `docs/migration/v1-to-v2.md`

We dropped all of the above because **no production cluster
exists**. The v2 bump was justified as "if we have to break the
wire, bundle every breaking change here". Without production:

- No clients to migrate.
- No `/v2/` namespace adds value over `/v1/` because the surface
  contract has not gone GA.
- The module-path bump would force a churn of every internal
  import (hundreds of lines) for zero observable benefit.

The two breaking changes we did absorb on the v1 surface in phase
5c.1 are documented in `CHANGELOG.md`:

- `GetShardIterator` returns `InvalidArgument` (was `NotFound`) for
  malformed shard ids that don't match the `shardId-NNNNNNNNNNNN`
  pattern.
- `addVoterRequest.ID` / `removeServerRequest.ID` reject empty /
  whitespace-bearing node ids at decode time instead of at the
  underlying raft library.

## Consequences

### Positive

- The compiler catches misuse: a `NodeID` cannot be passed where a
  `TableID` is expected, a metric label slot cannot be fed a
  `NodeID`, a wire string cannot be interpreted as a routing
  decision.
- Validation lives in one place per VO (the constructor). Three
  HTTP/gRPC handlers that previously repeated `if id == ""` checks
  now delegate to the constructor.
- `UnroutedShardID` makes the "I could not route this operation"
  signal first-class; `range_metrics.go` no longer conflates it
  with "shard 0" (the bug surfaced in #330).
- The legacy primitive constant `types.StreamShardIDSingle` is
  retained as a deprecated alias kept in sync with
  `model.StreamShardIDSingle.String()`, so non-migrated callers
  keep compiling without forcing a tree-wide rewrite.

### Negative

- Two source of truths during the migration window:
  `pkg/types.StreamShardIDSingle` (string const) and
  `model.StreamShardIDSingle` (VO). The string const is documented
  as deprecated; production code in `internal/catalog/catalog.go`
  already references the VO via `.String()`.
- `model` cannot import `types` (cycle), so the legacy const
  cannot be redirected through the VO at the language level.
  Documentation is the only enforcement.
- A few internal helpers (`PlacementCatalog.Nodes` map keyed by
  string, `PlacementCatalog.Shards[i].ID` as `uint32`) still use
  primitives because migrating the catalog struct fields ripples
  through proto regeneration and would warrant a wire bump. The
  current design has the VO at every function boundary; the field
  migration is deferred to phase 10 (DDD bounded-context reorg).

### Neutral

- `cefas-cli` and the npm package keep their current major
  version. No client migration required.

## Alternatives considered

1. **Type aliases (`type ShardID = uint32`)**. Rejected: aliases
   accept any primitive of the same type, defeating the safety
   argument.

2. **Bump to v2 anyway**. Rejected: the cost (every internal
   import path changes, every test fixture updates, every client
   reference breaks) was incurred against zero current users.
   Documented in CHANGELOG.md so a future v2 cut can bundle this
   work with a real reason.

3. **Wide field migration in one PR**. Rejected: would have
   required either a proto regeneration or wide TextMarshaler
   plumbing on map keys. Strangler-fig per surface let each PR
   stay ≤ 400 LOC added and reversible.
