# Benchmarks

## 8-node matrix

`scripts/bench/bench_8node_matrix.sh` is the standard local gate for the
storage and raft throughput work. It builds a local Docker image, generates an
8-node compose file under the result directory, starts a clean cluster, runs the
same workload matrix, and writes a Markdown summary plus raw JSON/log/metrics
artifacts.

Default matrix:

- smoke: 10k seed writes plus 10s read/write mixed check
- write-only: 64 write workers for 5 minutes
- read seed: 300k writes for a stable read keyspace
- read-only: 512 read workers for 5 minutes
- mixed: 64 write workers plus 512 read workers for 5 minutes
- placement: 8 nodes, 24 shards, RF=3 unless `REPLICATION_FACTOR` is overridden

Typical baseline run:

```bash
ALLOW_FAILURES=1 scripts/bench/bench_8node_matrix.sh
```

Use `ALLOW_FAILURES=1` when measuring a known-bad baseline. The script still
captures JSON reports, logs, metrics, Docker stats, and `summary.md`. By
default the compose cluster and volumes are removed at exit; set
`KEEP_CLUSTER=1` when you need to inspect a live failed cluster.

Useful overrides:

```bash
RESULT_DIR=/tmp/cefas-bench/pr-next \
PROJECT=cefas-pr-next \
REPLICATION_FACTOR=3 \
scripts/bench/bench_8node_matrix.sh
```

Short harness validation:

```bash
RESULT_DIR=/tmp/cefas-bench/smoke \
PROJECT=cefas-bench-smoke \
SMOKE_DURATION=2s \
WRITE_DURATION=5s \
READ_DURATION=5s \
MIXED_DURATION=5s \
READ_SEED_ITEMS=1000 \
WRITE_WORKERS=4 \
READ_WORKERS=8 \
SHARDS=8 \
ALLOW_FAILURES=1 \
scripts/bench/bench_8node_matrix.sh
```

Continuous phase sampling:

```bash
PHASE_SAMPLE_INTERVAL=15 \
ALLOW_FAILURES=1 \
scripts/bench/bench_8node_matrix.sh
```

When enabled, each phase writes timestamped Prometheus and Docker stats samples
under `metrics/<phase>_series/`. The summary also includes per-phase maxima for
read amplification, L0 files, compaction debt, compactions in progress, and
backpressure state.
