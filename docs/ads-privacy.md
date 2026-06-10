# Ads audience privacy model

CEFAS hosts ads audiences as a managed substrate: a campaign owner
selects + delivers via the engine; raw audience identity never
crosses the boundary into a downstream reporting surface.

## Posture in one paragraph

The engine does the matching. The CLI / SDK / reporting surfaces only
receive aggregated, threshold-gated outputs. There is no
`list-audience-members` operation. Estimates use HyperLogLog; group
counts enforce a minimum cohort size. Dedup + frequency-cap state is
stored server-side and never serialized into responses.

## Guarantees

| Guarantee | Where it's enforced |
|---|---|
| No raw audience export | The CLI / SDK surface exposes `GeoAudience` (streams individual rows only to authorised callers holding `cefas:item:read:<table>`) and aggregate / estimate verbs (never per-user). No `dump-cohort` verb. |
| Approximate reach only | `cohort estimate` is a HyperLogLog cardinality, not a member list. |
| Minimum cohort size on aggregation | `audience.Aggregate` returns `ErrMinGroupSize` for the whole call if any group would fall below the threshold (see `pkg/plugin/audience/aggregate.go`). No partial result, so callers can't infer the small group. |
| Dedup is server-side state | `cefas dedup put` records `(scope, key, ttl)`; callers receive only the `Allowed` bool. The state never round-trips. |
| Frequency cap is server-side state | Same shape as dedup: the response is the boolean verdict, not the underlying counter. |
| Index data stays server-side | Plugin-backed indexes (Roaring cohorts, MinHash, geohash, …) are reachable only through `Query` / `Estimate` / `Select` — never `Export`. |

## Threat model

What we explicitly try to prevent:

1. A reporting tool inferring small cohorts. Mitigated by
   `--min-group-size` on `aggregate`.
2. A campaign owner enumerating users in a cohort. Mitigated by the
   absence of any cohort-export verb + the per-table read scope check
   on `GeoAudience`.
3. A delivery loop re-delivering to the same user. Mitigated by
   `dedup` + `freqcap` server-side.
4. A timing oracle revealing whether a specific user is in a cohort.
   Mitigated only insofar as `Dedup` returns a uniform boolean — no
   per-key latency variation beyond bookkeeping.

What we explicitly do not address in v1:

- Differential-privacy noise injection on aggregation. The min-group
  floor is k-anonymity-like, not formal DP.
- Side-channel timing attacks on `Dedup` / `FreqCap` at scale.
- Identifying individuals via cross-dataset linkage with other
  sources. CEFAS treats each `(scope, key)` opaquely.

## Server-side enforcement points

| Surface | Code |
|---|---|
| Aggregate min-group-size | `pkg/plugin/audience/aggregate.go` |
| Dedup TTL bookkeeping | `pkg/plugin/audience/audience.go` |
| FreqCap sliding window | `pkg/plugin/audience/audience.go` |
| Per-table read scope check on GeoAudience | `pkg/api/grpc_plugin_ops.go::GeoAudience` |
| No raw export verb exists | `pkg/api/proto/cefas.proto` (intentional absence — there is no `ListCohortMembers` or similar) |

## Why these choices

CEFAS owns the audience substrate but the data plane belongs to the
caller. The pattern follows the industry-standard "the platform sees
identifiers; reporting sees aggregates" split, scaled down to a
single self-hosted engine.
