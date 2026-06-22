# ADR 0001 — Cross-shard scan via internal ScanShard RPC

* Status: proposed
* Date: 2026-06-21
* Tracks: epic #466, Phase 1 #467

## Context

Server-side fan-in operations call `cluster.Manager.ReadShards` (alias
`scatterReadStores`), which only returns when **every** logical shard
has a local replica on the coordinator node. In a cluster with
`ReplicationFactor < node_count` no single node owns all shards, so
those operations fail with `NoLocalReplicaError` before producing any
output. Two callers hit this today:

- `GRPCServer.indexItemSourceFor` in `internal/server/grpc_plugin_ops.go`
  — runs when `CreateIndex` attaches a plugin-backed index (bloom, hll,
  …) to an existing table. The Build step needs to see every pre-
  existing item exactly once.
- `GRPCServer.Scan` in `internal/server/grpc_server.go` — public scan
  RPC. Same call, same failure mode; out of scope for this ADR but
  covered by the same RPC once we ship it (issue #470).

The bypass merged with #458 lets the bench tooling skip plugin-index
attach when no node accepts it, which keeps `bench_ab.sh` running but
hides the missing capability: in a real multi-node `RF<N` cluster
plugin indexes cannot be built at all. The TODO left in
`indexItemSourceFor` records the gap explicitly.

The fix has to work for the build path now. Two follow-ups already
exist for later (#469, #470), so the design must be reusable, not
custom-fitted to plugin index Build.

## Decision

Introduce an internal-only gRPC service:

```protobuf
service Replica {
  rpc ScanShard(ScanShardRequest) returns (stream Item);
}

message ScanShardRequest {
  string table = 1;
  uint32 shard_id = 2;
}
```

The handler is served on the same gRPC listener as `cefas.v1.Cefas`.
It refuses requests for shards the node does not host (returns
`UNAVAILABLE` with a `NoLocalReplicaError`-derived message so the
caller can try another peer). For shards it does host, it iterates
`db.Iter(PrefixPrimaryAll(table))`, decodes each value, and pushes it
on the stream. Cancellation propagates from the gRPC stream context
straight into the iterator loop.

Phase 2 (#468) wraps this with `cluster.Manager.PeerScanShard`, which
selects a peer that hosts the shard (preferring `LeaderHint`, then
voters, then non-voters) and surfaces the stream to the coordinator.
Phase 3 (#469) replaces `scatterReadStores` in `indexItemSourceFor`
with a fan-in that calls `db.ScanTable` for local shards and
`PeerScanShard` for remote shards, deduping by primary key as the
current code already does.

## Considered alternatives

### A. Distributed Build with plugin-side Merge

Each node Builds the plugin index from its local shards; the
coordinator calls a new `IndexPlugin.Merge` method to combine the
per-node states.

- Pro: no item bytes cross the wire — only the compact plugin state
  (bloom bitmap, hll register array). Cheap on the network.
- Pro: scales naturally with shard count.
- Con: every plugin has to implement `Merge`. Bloom and HLL can; many
  others (LSM-on-LSM, custom GSI implementations) cannot — Merge is
  not a free property of an index.
- Con: doubles the plugin contract surface. Versioning, migration, and
  third-party plugin authoring all get harder.
- Con: does not help the public `Scan` RPC at all, which also needs
  cross-shard fan-in.

Rejected for now: too heavy for the immediate need, does not generalise
to Scan, raises the plugin contract bar.

### B. Local-only Build with documented tradeoff

`indexItemSourceFor` Builds from local shards only; document that
plugin indexes started on multi-node clusters do not see pre-existing
items in remote shards, and require a separate rebuild path.

- Pro: ~50 LOC, no proto change, no cluster work.
- Con: silently inconsistent indexes. Anyone running `CreateIndex` on a
  populated multi-node cluster gets a partial index without the system
  telling them.
- Con: forces a second mechanism (rebuild) to exist, which has to do
  exactly what ScanShard does anyway.

Rejected: the user pushed back explicitly on "sacrificing the right
solution for a short-term shortcut."

### C. Reuse the existing `Scan` RPC for fan-in

The coordinator calls `Cefas.Scan` on each peer.

- Pro: no proto change.
- Con: `Scan` returns everything that node serves — not scoped to a
  single shard. The coordinator cannot ask peer P for "only the items
  in shard 7"; it would receive duplicates and waste bandwidth.
- Con: `Scan` itself has the same single-node-only limitation, so
  invoking it across peers does not actually solve the problem.

Rejected: would not work.

## Consequences

### Positive

- `CreateIndex` works on any cluster topology, including `RF<N`.
- Phase 3 deletes the TODO in `indexItemSourceFor` without trading
  correctness for shipability.
- The same RPC unblocks the public `Scan` refactor in #470 — one piece
  of plumbing solves both callers.
- Stream-based, so memory stays bounded.

### Negative / risks

- New gRPC surface area. `Replica` is served on the same listener
  as the public service. Until we ship cluster mTLS (out of scope of
  this ADR; new issue to follow), anything that can reach the gRPC
  port can call `ScanShard` and read whole tables. Operators relying
  on the existing scope checks need to know.
- Adds one more cross-node hop in the Build path. For a freshly
  created table this is irrelevant. For a populated table the network
  cost is proportional to row count × payload size — comparable to
  what a centralised Build would do anyway, just over the wire.
- Partial failure semantics are strict: if any peer is unreachable
  during fan-in, Build fails. The alternative (proceed with warning)
  would leave the index silently incomplete, which is exactly the
  failure mode this ADR is designed to remove.

### Neutral

- Adds two generated files (`*_grpc.pb.go` for the new service).

## Auth and exposure

The new service does not call `requireAnyScope`. Today the production
binary does not register `AuthInterceptor` either, so the public
`cefas.v1.Cefas` surface is reachable on the gRPC port without a
bearer token; adding scope checks to `Replica` would not raise the
bar versus what already ships. When auth is turned on (the
interceptor is wired up in tests and ready to be enabled), the
service name `cefas.v1.Replica` will need to land in the
`skipMethods` set, or `requireAnyScope` will need to be invoked
explicitly with a dedicated cluster scope.

The right long-term answer is cluster mTLS or a shared cluster
token. That work is tracked as a separate follow-up; this ADR scopes
the immediate fix to the data-path fan-in.

## Rollout

1. Phase 1 (this ADR) — proto + handler + tests, no caller change.
2. Phase 2 — `PeerScanShard` helper in `cluster.Manager`, with peer
   selection and stream wiring.
3. Phase 3 — switch `indexItemSourceFor` to the helper, delete the
   TODO. Verify with `bench_ab.sh master HEAD WITH_PLUGIN_INDEX=bloom
   RUNS=3` that plugin index attach now succeeds on the 8-node RF=3
   cluster (the assertion #458 could not make).
4. Follow-up #470 — same treatment for `Scan`.
5. Follow-up (new issue) — mTLS / shared token for `Replica`.

## References

- Epic: https://github.com/CefasDB/cefasdb-core/issues/466
- Phase 1: https://github.com/CefasDB/cefasdb-core/issues/467
- Phase 2: https://github.com/CefasDB/cefasdb-core/issues/468
- Phase 3: https://github.com/CefasDB/cefasdb-core/issues/469
- Follow-up Scan: https://github.com/CefasDB/cefasdb-core/issues/470
- Bypass that ships today: PR #465, issue #458
- Symptom in the field: `attach plugin index "bloom": rpc error: code = Unavailable desc = cluster: node n1 has no local replica for shard 1 (voters=[n2 n3 n4] nonVoters=[])`
