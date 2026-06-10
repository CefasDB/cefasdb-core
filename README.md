# cefas

**cefas is a distributed, multi-model database for low-latency operational workloads.**
It combines O(1) primary-key access, range and secondary-index queries, and native
multidimensional indexing under a single SQL-compatible surface — backed by a replicated
log and an LSM storage engine that survive disk, machine, and datacenter failures.

- [What is cefas?](#what-is-cefas)
- [Docs](#docs)
- [Getting Started](#getting-started)
- [Client Drivers and SDKs](#client-drivers-and-sdks)
- [Deployment](#deployment)
- [Need Help?](#need-help)
- [Contributing](#contributing)
- [Design](#design)
- [Roadmap](#roadmap)
- [License](#license)

## What is cefas?

cefas is a single database that serves three workloads from one storage layer:

- **Key/value primary access** with constant-time `Get` and `Put` on a partition key and
  optional sort key.
- **Range and secondary-index queries** with global secondary indexes that stay
  transactionally consistent with the primary write.
- **Multidimensional and geospatial queries** with first-class geohash and Z-order
  indexes, exposed through both a structured request API and a SQL extension.

Items use a flexible attribute model — strings, numbers, binary, sets, lists, nested
maps — so application schemas can evolve without table migrations. A SQL query
interface targeting `SELECT`, `INSERT`, `UPDATE`, `DELETE` and spatial predicates lets
teams use the same skills they bring to a traditional RDBMS.

Replication is built in. cefas groups writes into atomic batches and ships them through
a Raft consensus log; every node holds a complete or partitioned copy of the keyspace
and can serve reads locally. Failure of a node, a disk, or an entire availability zone
is handled transparently, with no manual intervention required.

Workloads cefas is designed for:

- Operational data stores that need single-digit-millisecond reads and writes.
- Geospatial systems — fleets, deliveries, location-aware personalization — that need
  proximity queries without bolting on a second database.
- Event and time-series stores keyed by a partition with a sort key per timestamp.
- Multi-tenant SaaS backends where every tenant gets predictable performance under load.

## Docs

User and operator documentation lives in the GitHub Wiki:

- **Wiki Home** — <https://github.com/osvaldoandrade/cefas/wiki>
- **Get Started** — <https://github.com/osvaldoandrade/cefas/wiki/Get-Started-Overview>
- **Concepts and Architecture** — <https://github.com/osvaldoandrade/cefas/wiki/Concepts-Overview>
- **Plugins and Extensions** — <https://github.com/osvaldoandrade/cefas/wiki/Plugins-Overview>
- **Interfaces** — <https://github.com/osvaldoandrade/cefas/wiki/Interfaces-Overview>
- **Operations** — <https://github.com/osvaldoandrade/cefas/wiki/Operations-Overview>

## Getting Started

### Build from source

cefas is written in Go and ships as a single static binary. From a checkout of this
repository:

```sh
go build ./cmd/cefas-server
go build ./cmd/cefas-cli
```

### Start a local node

```sh
./cefas-server -data ./cefas-data -http :8080
```

This launches a single-node instance on `localhost:8080`. The first start creates
the data directory automatically.

### Create a table and write an item

```sh
curl -X POST localhost:8080/v1/tables \
  -d '{"name":"events","keySchema":{"pk":"user_id","sk":"ts"}}'

curl -X POST localhost:8080/v1/PutItem \
  -d '{"table":"events","item":{"user_id":{"S":"alice"},"ts":{"N":"100"},"event":{"S":"login"}}}'

curl -X POST localhost:8080/v1/Query \
  -d '{"table":"events","pkValue":{"S":"alice"}}'
```

### Run a local cluster

Multi-node, replicated deployments are managed through `cefas-cli`. Cluster bootstrap,
node addition, and shard placement are covered in the Operations Guide.

### Configuration

cefas accepts flags, environment variables, and a YAML configuration file (highest
precedence first). The most important settings are:

| Setting           | Flag       | Default          | Description                                    |
| ----------------- | ---------- | ---------------- | ---------------------------------------------- |
| Data directory    | `-data`    | `./cefas-data`   | Filesystem path for the local storage engine.  |
| HTTP listen addr  | `-http`    | `:8080`          | Address for the JSON API.                      |
| Fsync on commit   | `-fsync`   | `false`          | Trade throughput for crash-durable writes.     |

## Client Drivers and SDKs

cefas speaks two protocols on the wire:

- **HTTP/JSON** — a structured request API suitable for any HTTP client.
- **gRPC** — typed, streaming RPCs defined in `pkg/api/proto/cefas.proto`.

Officially supported SDKs:

- **Go** — `github.com/osvaldoandrade/cefas/pkg/client`.

Additional language SDKs can be generated directly from the `.proto` definition with
the standard gRPC tooling.

## Deployment

cefas runs anywhere Go binaries run. Common deployment shapes:

- **Embedded** — link `internal/storage` into a host process for tests, demos, and
  edge workloads.
- **Single-node server** — `cefas-server` behind a load balancer for development and
  small production tiers.
- **Replicated cluster** — three or more `cefas-server` nodes participating in Raft,
  fronted by any TCP-aware load balancer.
- **Container orchestration** — official Docker images and a Helm chart for Kubernetes
  StatefulSet deployments.

Deployment guides cover sizing, storage class selection, backup strategy, and rolling
upgrades.

## Need Help?

- **Issues and discussions** are tracked on this repository. Open an
  [issue](https://github.com/osvaldoandrade/cefas/issues) for bug reports, feature
  requests, or design questions.
- **Troubleshooting** common errors, cluster bring-up, and performance investigations
  is documented in the Operations Guide.

## Contributing

Contributions are welcome. The
[good first issue](https://github.com/osvaldoandrade/cefas/issues?q=is%3Aopen+label%3Agood-first-issue)
label tracks self-contained tasks suitable for newcomers; larger initiatives are
organized as Epics in the issue tracker.

Before submitting a pull request:

1. Discuss substantial changes in an issue first.
2. Run `go test ./...` and `go vet ./...`. Add tests covering new behavior.
3. Keep commits focused and well-described; the PR template walks through the rest.

## Design

cefas is structured as a small set of cooperating layers, each independently testable:

- **Storage engine** — an LSM-tree with a 256 MiB block cache, bloom-filtered point
  lookups, and a group-commit coalescer that amortizes write-path overhead across
  concurrent producers.
- **Key encoder** — a namespaced layout where partition keys are hashed into uniform
  buckets and sort keys are appended as canonical bytes, so range scans within a
  partition follow natural lexicographic order.
- **Consensus** — a Raft replicated log carrying opaque write batches. Followers apply
  every committed batch atomically; reads are served from the local replica with an
  optional strong-consistency mode.
- **Multi-Raft sharding** — partitions are spread across independent Raft groups,
  multiplexed over a single transport per node. This scales write throughput
  horizontally while keeping per-shard guarantees intact.
- **Indexing** — global secondary indexes and spatial (geohash, Z-order) indexes are
  written in the same atomic batch as the primary record, so reads through any index
  see a consistent snapshot of the data.
- **Query** — a SQL parser and planner translate user queries into operator trees
  that push predicates to the cheapest available index; spatial predicates compile
  directly into prefix scans over the spatial indexes.
- **Identity** — every request is authenticated against a configured identity provider
  and authorized against per-operation, per-table scope strings.

## Roadmap

Active development is organized into Epics on the
[issue tracker](https://github.com/osvaldoandrade/cefas/labels/epic):

- Global secondary indexes and conditional writes
- Spatial indexing (geohash and Z-order)
- Raft replication
- Multi-Raft sharding
- gRPC API and SDKs
- SQL query layer
- Identity and access management integration
- Observability and operations tooling

## License

See [`LICENSE`](LICENSE) for terms.
