#!/usr/bin/env bash
set -euo pipefail

PROJECT="${PROJECT:-cefas-cluster}"
COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker-compose.cluster.yml}"
RESULT_DIR="${RESULT_DIR:-/tmp/cefas-bench}"
BIN="${BIN:-/tmp/cefas-loadtest}"
ADDRS="${ADDRS:-localhost:9191,localhost:9192,localhost:9193}"

RESET_CLUSTER="${RESET_CLUSTER:-0}"

BULK_ITEMS="${BULK_ITEMS:-2000000}"
BULK_READS="${BULK_READS:-500000}"
BULK_BATCH_SIZE="${BULK_BATCH_SIZE:-500}"
BULK_WORKERS="${BULK_WORKERS:-64}"
BULK_READ_WORKERS="${BULK_READ_WORKERS:-64}"
BULK_PAYLOAD_BYTES="${BULK_PAYLOAD_BYTES:-256}"

SOAK_DURATION="${SOAK_DURATION:-15m}"
SOAK_BATCH_SIZE="${SOAK_BATCH_SIZE:-500}"
SOAK_WORKERS="${SOAK_WORKERS:-64}"
SOAK_READ_WORKERS="${SOAK_READ_WORKERS:-64}"
SOAK_PAYLOAD_BYTES="${SOAK_PAYLOAD_BYTES:-256}"
SOAK_WRITE_RATE="${SOAK_WRITE_RATE:-15000}"
SOAK_READ_RATE="${SOAK_READ_RATE:-20000}"
LATENCY_SAMPLE_RATE="${LATENCY_SAMPLE_RATE:-10}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

mkdir -p "$RESULT_DIR"

if [[ "$RESET_CLUSTER" == "1" ]]; then
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v
fi

docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up --build -d

GOCACHE="${GOCACHE:-/tmp/cefas-gocache}" go build -o "$BIN" ./cmd/cefas-loadtest

echo "Running bulk benchmark: $RUN_ID"
"$BIN" \
  -addrs "$ADDRS" \
  -table "BenchBulk_${RUN_ID}" \
  -items "$BULK_ITEMS" \
  -reads "$BULK_READS" \
  -batch-size "$BULK_BATCH_SIZE" \
  -workers "$BULK_WORKERS" \
  -read-workers "$BULK_READ_WORKERS" \
  -payload-bytes "$BULK_PAYLOAD_BYTES" \
  -latency-sample-rate 1 \
  -progress 10s \
  -label "bulk-${RUN_ID}" \
  -json-output "$RESULT_DIR/bulk_${RUN_ID}.json"

echo "Running soak benchmark: $RUN_ID"
"$BIN" \
  -addrs "$ADDRS" \
  -table "BenchSoak_${RUN_ID}" \
  -items 0 \
  -reads 0 \
  -write-duration "$SOAK_DURATION" \
  -read-duration "$SOAK_DURATION" \
  -batch-size "$SOAK_BATCH_SIZE" \
  -workers "$SOAK_WORKERS" \
  -read-workers "$SOAK_READ_WORKERS" \
  -write-rate "$SOAK_WRITE_RATE" \
  -read-rate "$SOAK_READ_RATE" \
  -payload-bytes "$SOAK_PAYLOAD_BYTES" \
  -latency-sample-rate "$LATENCY_SAMPLE_RATE" \
  -progress 30s \
  -label "soak-${RUN_ID}" \
  -json-output "$RESULT_DIR/soak_${RUN_ID}.json"

docker compose -p "$PROJECT" -f "$COMPOSE_FILE" ps

echo "Results:"
echo "  $RESULT_DIR/bulk_${RUN_ID}.json"
echo "  $RESULT_DIR/soak_${RUN_ID}.json"
