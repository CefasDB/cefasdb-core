# cefas

cefas is a high-performance NoSQL key-value and document database for
operational workloads. It is designed for predictable millisecond-class access,
horizontal scale, and a small operational footprint while still giving teams
direct control over deployment, storage, replication, and extensions.

The repository ships two main binaries:

- `cefas-server`: a Go database server backed by Pebble, with HTTP/JSON and gRPC APIs.
- `cefas`: a command-line client distributed as a prebuilt Go binary through npm.

Long-form documentation lives in the GitHub Wiki.

- [Wiki Home](https://github.com/osvaldoandrade/cefas/wiki)
- [Get Started](https://github.com/osvaldoandrade/cefas/wiki/Get-Started-Overview)
- [Concepts and Architecture](https://github.com/osvaldoandrade/cefas/wiki/Concepts-Overview)
- [Plugins and Extensions](https://github.com/osvaldoandrade/cefas/wiki/Plugins-Overview)
- [Interfaces](https://github.com/osvaldoandrade/cefas/wiki/Interfaces-Overview)
- [Operations](https://github.com/osvaldoandrade/cefas/wiki/Operations-Overview)

## What It Does

cefas stores flexible documents behind a partition key and optional sort key. On
top of that storage model it provides:

- Point reads and writes through `GetItem`, `PutItem`, `DeleteItem`, and batch APIs.
- Partition queries with optional sort-key ranges and consistent-read routing.
- Conditional writes, item updates, table metadata, TTL, backup, and restore support.
- SQL execution through the server query layer.
- Plugin-backed indexes for text, probabilistic, vector, spatial, and audience use cases.
- Cluster operations for Raft-backed replicated deployments.
- Prometheus metrics, optional OTLP tracing, and bearer-token authorization.

The item wire format is a compact typed JSON shape. Examples use tags such as
`S` for strings, `N` for numbers, `BOOL` for booleans, `L` for lists, and `M`
for maps.

## Install The CLI

The npm package installs the `cefas` CLI by downloading the matching prebuilt
binary from GitHub Releases.

```sh
npm install -g @osvaldoandrade/cefas
cefas --help
```

Node.js 18 or newer is required for the installer wrapper. The installed command
is the native Go CLI.

## Build Locally

```sh
go build -o ./bin/cefas-server ./cmd/cefas-server
go build -o ./bin/cefas ./cmd/cefas-cli
```

Run the local server with HTTP and gRPC enabled:

```sh
./bin/cefas-server \
  -data ./cefas-data \
  -http :8080 \
  -grpc :9090 \
  -grpc-reflection
```

In another shell, point the CLI at the local gRPC endpoint:

```sh
./bin/cefas --endpoint localhost:9090 --insecure list-tables
```

## First Table

Create a table with a partition key and sort key:

```sh
cefas --endpoint localhost:9090 --insecure create-table \
  --table-name Users \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --attribute-definitions AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --key-schema AttributeName=sk,KeyType=RANGE
```

Write and read an item:

```sh
cefas --endpoint localhost:9090 --insecure put-item \
  --table-name Users \
  --item '{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"},"name":{"S":"Ova"}}'

cefas --endpoint localhost:9090 --insecure get-item \
  --table-name Users \
  --key '{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"}}'
```

Run a partition query:

```sh
cefas --endpoint localhost:9090 --insecure query \
  --table-name Users \
  --pk-value '{"S":"USER#1"}' \
  --limit 25
```

## Plugins And Indexes

cefas includes a plugin registry for index, distance, estimator, and audience
operations. The built-in server registers plugins such as `trigram`, `bloom`,
`geohash`, vector distance operators, cardinality sketches, and frequency tools.

```sh
cefas --endpoint localhost:9090 --insecure list-plugins

cefas --endpoint localhost:9090 --insecure create-index \
  --table Users \
  --name user_name_trigram \
  --type trigram \
  --field name
```

Operational commands include `topk`, `aggregate`, `cohort`, `geo audience`,
`dedup`, `freqcap`, `explain`, `describe-index`, and `rebuild-index`.

## Run With Docker

The demo Compose stack runs `cefas-server`, Prometheus, and Grafana:

```sh
docker compose -f deploy/docker-compose.yml up --build
```

Ports:

- `8080`: HTTP API and `/metrics`
- `9090`: gRPC API
- `9091`: Prometheus
- `3000`: Grafana, default login `admin` / `admin`

Kubernetes deployment files live under `deploy/helm/cefas`.

## Configuration

`cefas-server` accepts flags, environment variables, and a YAML file. Precedence is:

```text
flags > CEFAS_* environment variables > YAML file > defaults
```

Common server flags:

| Flag | Default | Purpose |
| ---- | ------- | ------- |
| `-data` | `./cefas-data` | Pebble data directory. |
| `-http` | `:8080` | HTTP listen address. |
| `-grpc` | empty | gRPC listen address. Empty disables gRPC. |
| `-fsync` | `false` | Fsync on commit for stronger crash durability. |
| `-config` | empty | YAML config file path. |
| `-metrics-disabled` | `false` | Disable Prometheus metrics. |
| `-tracing-endpoint` | empty | OTLP/gRPC collector endpoint. |

The CLI reads `~/.cefas/config.yaml`, `CEFAS_*` environment variables, and global
flags such as `--endpoint`, `--token`, `--token-file`, `--ca`, `--insecure`,
`--output`, and `--timeout`.

Cluster placement planning commands return dry-run plans for shard elasticity
operations. They do not apply data movement or Raft membership changes:

```sh
cefas cluster plan split --shard 0
cefas cluster plan move --shard 0 --source-node n1 --target-node n4 --min-voters 3
cefas cluster plan drain --node n1 --target-node n4 --min-voters 3
```

## Project Layout

```text
cmd/cefas-server        server entrypoint
cmd/cefas-cli           CLI entrypoint and subcommands
internal/storage        Pebble storage, keys, TTL, backup, restore, indexes
internal/raft           Raft replication, snapshots, change stream plumbing
internal/cluster        multi-shard routing and cluster manager
pkg/api                 HTTP and gRPC API implementation
pkg/client              Go client used by the CLI
pkg/sql                 SQL parser, planner, and executor
pkg/plugin              plugin interfaces, registry, and built-ins
npm                     npm wrapper for installing the prebuilt CLI
deploy                  Docker, Compose, Helm, Prometheus, and Grafana assets
```

## Development

Run the test suite with an isolated Go build cache when your environment restricts
the default cache directory:

```sh
GOCACHE=/tmp/cefas-gocache go test ./...
```

Useful checks before publishing changes:

```sh
go test ./...
go vet ./...
go build ./cmd/cefas-server
go build ./cmd/cefas-cli
```

Releases are produced by GitHub Actions. The release workflow builds the CLI,
publishes GitHub Release assets, and publishes the npm package from `npm/`.

## License

See [`LICENSE`](LICENSE) for terms.
