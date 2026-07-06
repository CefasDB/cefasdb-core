# CefasDB

CefasDB is a NoSQL document database written in Go. It exposes
HTTP/JSON and gRPC APIs, accepts SQL through `ExecuteStatement`, and
can run as a single binary or as a Raft multi-shard cluster for
horizontal scale. It targets teams that need predictable
millisecond-class reads on operational data and prefer to run the
binary themselves.

The project ships two binaries:

- `cefasdb` — the database server.
- `cefas` — the CLI client, mirroring the AWS DynamoDB CLI surface.

User and operator documentation lives at [docs.cefasdb.com].

[docs.cefasdb.com]: https://docs.cefasdb.com

## Install

The fastest path is to run the server in a container and install the
CLI on the host.

### Server (Docker)

```sh
docker run --rm -p 8080:8080 -p 9090:9090 \
  ghcr.io/cefasdb/cefasdb:latest \
  -http :8080 -grpc :9090 -grpc-reflection
```

For a persistent local client-server stack, use Docker Compose:

```sh
docker compose -f deploy/docker-compose.yml up -d
```

This starts one CefasDB node with a modest single-client profile for
Electron + Node applications. Host clients can use `127.0.0.1:9090`
for gRPC or `http://127.0.0.1:8080` for HTTP/SQL. A Node service in
the same Compose project should use `cefasdb:9090`.

The default Compose profile is intentionally not a cluster and not a
benchmark setup: it caps CefasDB at 4 CPUs / 4 GiB, uses durable
single-node writes, and keeps Prometheus/Grafana behind the optional
`observability` profile.

For a local three-node multi-Raft stack with replication factor 3 and
automatic rebalancing enabled, use the cluster Compose file:

```sh
docker compose -p cefas-rf3 \
  -f deploy/docker-compose.cluster.yml \
  --profile observability \
  up -d
```

The RF=3 stack exposes gRPC on `127.0.0.1:9191`, `:9192`, and `:9193`;
Grafana is on `http://127.0.0.1:3001`. It uses separate volumes from
the single-node stack. Do not point it directly at an existing
single-node volume: multi-Raft stores data under `/var/lib/cefas/shards`,
so existing single-node tables must be backed up and migrated instead
of relying on an in-place layout switch.

Images are published per release with the tags `<version>`,
`v<version>`, and `latest`. Pin to a specific release:

```sh
docker pull ghcr.io/cefasdb/cefasdb:0.8.5
```

### CLI (npm)

```sh
npm install -g @cefasdb/cefas
cefas --help
```

Node.js 18+ is required only for the installer wrapper; the installed
command is the native Go binary.

### From source

The Makefile builds both binaries into `./bin`:

```sh
git clone https://github.com/CefasDb/cefasdb-core
cd cefasdb-core
make build              # produces ./bin/cefasdb and ./bin/cefas
```

Other helpful targets (`make help` lists everything):

```sh
make server             # cefasdb only
make cli                # cefas only
make install            # both binaries into $GOBIN
make clean              # remove ./bin and cover.out
```

Go 1.25+ is required.

## Run

```sh
./bin/cefasdb \
  -data ./cefas-data \
  -http :8080 \
  -grpc :9090 \
  -grpc-reflection
```

In another shell:

```sh
./bin/cefas --endpoint localhost:9090 --insecure list-tables
```

For the full flag surface:

```sh
./bin/cefasdb --help
./bin/cefas   --help
```

## APIs

CefasDB exposes the same surface on three transports:

- gRPC on `:9090` (service `cefas.v1.Cefas`, defined in
  [`pkg/protocol/cefas.proto`](pkg/protocol/cefas.proto)). Reflection
  is enabled with `-grpc-reflection`.
- HTTP/JSON on `:8080` via grpc-gateway. Each RPC is reachable at
  `POST /v1/<RpcName>` with a JSON body that mirrors the proto.
- SQL through `ExecuteStatement` (PartiQL-compatible).

Generated godoc for every exported package is published at
[docs.cefasdb.com/api/](https://docs.cefasdb.com/api/).

## Test

```sh
make test               # race + shuffle, atomic coverage to cover.out
make cover              # enforce thresholds from .testcoverage.yml
make ci                 # vet + lint + test + cover + sec
```

`make help` lists the full set of quality targets (`fmt`, `lint`,
`vet`, `mut`, `sec`, `bench`).

## Community

- [GitHub Discussions](https://github.com/orgs/CefasDb/discussions) —
  questions, proposals, release announcements.
- [Issues](https://github.com/CefasDb/cefasdb-core/issues) — bugs and
  feature requests against the server and CLI.

## License

MIT. See [`LICENSE`](LICENSE).
