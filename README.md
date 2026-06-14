# CefasDB

CefasDB is a high-performance NoSQL key-value and document database for
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
- [DynamoDB Streams Compatibility](docs/dynamodb-streams.md)

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

## Vectors, ANN, Memory Tables, And PITR

Native vector attributes use the `V` tag with an optional dimension marker:

```json
{"id":{"S":"d1"},"emb":{"V":[0.1,0.2,0.3],"D":3}}
```

Declare vector dimensions at table creation time to make writes fail fast on
dimension mismatches. Use `--storage-class memory` for a Raft-replicated table
that keeps a process-local in-memory read copy while preserving the normal
write-ahead batch path for replication, backups, and restart recovery.

```sh
cefas --endpoint localhost:9090 --insecure create-table \
  --table-name Documents \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --attribute-definitions 'AttributeName=emb,AttributeType=V<3>' \
  --key-schema AttributeName=id,KeyType=HASH \
  --storage-class memory
```

Create a unified ANN index with explicit algorithm and metric parameters. The
`ann` descriptor uses the vector LSH index internally today and keeps the metric
available for `top-k` and SQL planning.

```sh
cefas --endpoint localhost:9090 --insecure create-index \
  --table Documents \
  --name emb_ann \
  --type ann \
  --field emb \
  --dim 3 \
  --algorithm lsh \
  --metric cosine

cefas --endpoint localhost:9090 --insecure top-k \
  --table Documents \
  --by "ann(emb, :q)" \
  --k 10 \
  --query '{"V":[0.1,0.2,0.3],"D":3}'
```

SQL can rank by the ANN index directly:

```sql
SELECT id FROM Documents ORDER BY emb ANN OF [0.1,0.2,0.3] LIMIT 10;
```

Backups record table stats, shard coverage, placement epoch, and the storage
change index captured at checkpoint time. Point-in-time restore uses the backup
checkpoint plus retained local changelog entries. Retain both the backup
checkpoint directory and the live `cefas/admin/change/log/` history for every
target point you need to restore; a target change index or timestamp before the
backup high-water mark or beyond retained history is rejected.

```sh
cefas --endpoint localhost:9090 --insecure create-backup --backup-name nightly

cefas --endpoint localhost:9090 --insecure restore-table-from-backup \
  --backup-name nightly \
  --source-table-name Documents \
  --target-table-name Documents_recovered \
  --target-change-index 12345
```

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

Hot range tracking is configured under `metrics.*` in YAML or `CEFAS_METRICS_*`
environment variables. The defaults keep Prometheus cardinality bounded at
`shard_count * 64` buckets. Useful overrides include
`metrics.hotspotBuckets`, `metrics.hotspotWindow`,
`metrics.hotspotCoolingWindow`, `metrics.hotspotReadThreshold`,
`metrics.hotspotWriteThreshold`, `metrics.hotspotBytesThreshold`,
`metrics.hotspotLatencyThreshold`, and
`metrics.hotspotCompactionDebtThresholdBytes`.

The autonomous rebalancer is opt-in with `rebalancer.enabled` or
`CEFAS_REBALANCER_ENABLED=true`. It consumes `HotRanges` and placement state on
a fixed interval, proposes deterministic split/range-move/drain plans, and
enforces `rebalancer.maxConcurrentOperations` plus `rebalancer.minInterval`.
Use `rebalancer.mode: dry-run` to log decisions, `manual` with
`rebalancer.manualPlanDir` to write plans for approval, or `auto` to apply safe
plans directly.

The CLI reads `~/.cefas/config.yaml`, `CEFAS_*` environment variables, and global
flags such as `--endpoint`, `--token`, `--token-file`, `--ca`, `--insecure`,
`--output`, and `--timeout`.

Cluster placement planning commands return dry-run plans for shard elasticity
operations:

```sh
cefas cluster plan split --shard 0 --min-voters 3
cefas cluster plan range-move --source-shard 0 --range-start 0 --range-end 9223372036854775808 --min-voters 3
cefas cluster plan move --shard 0 --source-node n1 --target-node n4 --min-voters 3
cefas cluster plan drain --node n1 --min-voters 3
cefas cluster plan decommission --node n1
```

When `split`, `range-move`, or `drain` does not receive explicit target voters,
the planner selects active nodes deterministically from node weight, CPU, memory,
disk, tags, shard count, range load, and zone anti-affinity. Draining and
decommissioned nodes are never selected as new targets. Use repeated
`--target-voter` flags on split/range-move or repeated `--target-node` flags on
drain to override the policy.

Split, move, drain, and decommission plans can be applied after review. Drain
moves shard memberships off the node and leaves it in `draining`; decommission
is a separate final metadata step that is refused while any active shard voter,
non-voter, or leader-hint reference remains. `cefas cluster status` includes
`DrainProgress` with the remaining blockers and `HotRanges` with bounded
token-bucket read/write, bytes, latency, compaction-debt, and throttling
summaries. Applying a split opens the child shard online and publishes the
transition catalog; it does not copy or activate the child range yet.

```sh
cefas cluster apply --plan file://split-plan.json --yes
cefas cluster apply --plan file://move-plan.json --yes
```

Placement audit can run online with bounded storage sampling. It reports token
coverage gaps, overlapping active owners, primary keys stored on the wrong
shard, and primary rows whose shard is missing the table catalog descriptor.
The report includes a checksum of the bounded primary-key sample. The emitted
`repairPlan` is explicit and review-only; repair actions are not applied by the
audit endpoint.

```sh
curl -s -X POST "$CEFAS_HTTP/v1/cluster/placement/audit" \
  -H "Authorization: Bearer $CEFAS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"maxPrimaryKeysPerShard":4096,"maxIssues":200,"includeRepairPlan":true}'
```

Once the child shard is open and writes to the split range are paused, finalize
the split to copy the child range, activate both shards, and clean the range
from the parent:

```sh
cefas cluster split finalize --parent-shard 0 --child-shard 1 --expected-epoch 2 --writes-quiesced --yes
```

Elasticity chaos/load validation runs locally through `go test` and does not
require external services. The suite exercises live split, live range move, and
node drain while writes are active, injects restart-style failures in copy,
catch-up, catalog publish, and cleanup phases, and verifies routed reads, item
counts, checksums, GSI query results, routing epochs, p99 latency, throughput,
errors, and a consistency verdict. The autonomous rebalancer gate runs a fixed
skewed workload before and after the hotspot-driven split, then fails unless the
max shard share drops from full concentration to a balanced post-rebalance
distribution. Set `CEFAS_ELASTICITY_REPORT` and `CEFAS_REBALANCER_REPORT` to
persist repeatable JSON reports for CI artifacts:

```sh
CEFAS_ELASTICITY_REPORT=reports/elasticity-chaos-load.json \
CEFAS_REBALANCER_REPORT=reports/rebalancer-skew.json \
  go test ./internal/cluster -run 'TestElasticityChaosLoadSuite|TestAutonomousRebalancerSkewReductionGate' -count=1 -v
```

Admin-named backups include a versioned manifest. The manifest records the
requested table set, the captured table set, and a deterministic row
count/checksum for each table in the checkpoint. Restore validates the source
table against that manifest before creating the target catalog entry or copying
rows. Backups created before manifests remain listable with
`manifest_status=legacy`. Retention can run as a dry-run first; when deletion
cannot remove a checkpoint directory it reports `partialCleanup` and
`cleanupError` explicitly.

```sh
cefas create-backup --backup-name before-maintenance
cefas list-backups
cefas restore-table-from-backup \
  --backup-name before-maintenance \
  --source-table-name Users \
  --target-table-name Users_restored
cefas restore-table-from-backup \
  --backup-name before-maintenance \
  --source-table-name Users \
  --target-table-name Users_restored \
  --dry-run
cefas apply-backup-retention --keep-latest 7 --max-age 720h --dry-run
cefas delete-backup --backup-name before-maintenance
cefas-server \
  -backup-scheduler-enabled \
  -backup-scheduler-dry-run \
  -backup-scheduler-interval 1h \
  -backup-scheduler-name-template 'hourly-{{timestamp}}' \
  -backup-scheduler-retention-keep-latest 24 \
  -backup-scheduler-retention-dry-run
cefas cluster status
curl -s -X POST "$CEFAS_HTTP/v1/RestoreTableFromBackup" \
  -H "Authorization: Bearer $CEFAS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"backupName":"before-maintenance","sourceTableName":"Users","targetTableName":"Users_restored","dryRun":true}'
curl -s -X POST "$CEFAS_HTTP/v1/ApplyBackupRetention" \
  -H "Authorization: Bearer $CEFAS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"keepLatest":7,"keepLatestSet":true,"maxAge":"720h","dryRun":true}'
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
