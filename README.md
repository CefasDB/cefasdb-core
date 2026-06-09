# cefas

Embedded, replicated database with a DynamoDB-like API.

- **Storage**: [cockroachdb/pebble](https://github.com/cockroachdb/pebble) (LSM)
- **Replication**: [hashicorp/raft](https://github.com/hashicorp/raft) (planned, Phase 4)
- **API**: gRPC + HTTP/JSON, modelled on DynamoDB (PutItem, GetItem, Query, ...).
- **Indexing**: PK + Sort Key range scans (Phase 1), GSI (Phase 2), geohash / Z-order (Phase 3).

Design and architectural patterns reuse the Raft+Pebble integration shipped in
[codeq](https://github.com/osvaldoandrade/codeq). See `/Users/ova/.claude/plans/dapper-yawning-turtle.md`.

## Layout

```
cmd/cefas-server   Server binary
cmd/cefas-cli      Admin / smoke-test CLI
pkg/api            gRPC + HTTP gateway
pkg/types          Public Item / AttributeValue / KeySchema
pkg/query          Query planner + executor
internal/storage   Pebble wrapper, key encoder, item codec
internal/raft      FSM, LogStore, StableStore, snapshot (Phase 4)
internal/index     GSI writer, geohash, Z-order
internal/catalog   Table descriptor persistence
```

## Phase 1 scope

Single-node, no Raft, in-process server with HTTP/JSON endpoints:
`CreateTable`, `PutItem`, `GetItem`, `DeleteItem`, `Query`.
