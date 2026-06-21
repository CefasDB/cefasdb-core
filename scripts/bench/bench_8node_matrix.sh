#!/usr/bin/env bash
set -euo pipefail

PROJECT="${PROJECT:-cefas-bench8}"
RESULT_DIR="${RESULT_DIR:-/tmp/cefas-bench/8node}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE="${IMAGE:-cefasdb-core:bench-${RUN_ID}}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
RESET_CLUSTER="${RESET_CLUSTER:-1}"
ALLOW_FAILURES="${ALLOW_FAILURES:-0}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"

NODES="${NODES:-8}"
SHARDS="${SHARDS:-24}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
STORAGE_PROFILE="${STORAGE_PROFILE:-write-heavy}"
SERVER_EXTRA_ARGS="${SERVER_EXTRA_ARGS:-}"

SMOKE_ITEMS="${SMOKE_ITEMS:-10000}"
SMOKE_DURATION="${SMOKE_DURATION:-10s}"
WRITE_DURATION="${WRITE_DURATION:-5m}"
READ_SEED_ITEMS="${READ_SEED_ITEMS:-300000}"
READ_DURATION="${READ_DURATION:-5m}"
MIXED_DURATION="${MIXED_DURATION:-5m}"

BATCH_SIZE="${BATCH_SIZE:-500}"
WRITE_WORKERS="${WRITE_WORKERS:-64}"
READ_WORKERS="${READ_WORKERS:-512}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}"
PAYLOAD_MODE="${PAYLOAD_MODE:-repeat}"
LATENCY_SAMPLE_RATE="${LATENCY_SAMPLE_RATE:-100}"
PROGRESS_INTERVAL="${PROGRESS_INTERVAL:-30s}"
CLIENT_ROUTE_AWARE_READS="${CLIENT_ROUTE_AWARE_READS:-0}"
WITH_STREAM="${WITH_STREAM:-0}"
WITH_PLUGIN_INDEX="${WITH_PLUGIN_INDEX:-}"
PHASE_SAMPLE_INTERVAL="${PHASE_SAMPLE_INTERVAL:-0}"
PHASE_SAMPLE_FILTER="${PHASE_SAMPLE_FILTER:-^(cefas_pebble_|cefas_storage_lane_|cefas_backpressure_|cefas_raft_|cefas_op_|go_memstats_|process_)}"
RPC_TIMEOUT="${RPC_TIMEOUT:-30s}"
WRITE_RATE="${WRITE_RATE:-0}"
READ_RATE="${READ_RATE:-0}"

HTTP_PORT_BASE="${HTTP_PORT_BASE:-18280}"
GRPC_PORT_BASE="${GRPC_PORT_BASE:-9290}"

COMPOSE_FILE="${COMPOSE_FILE:-${RESULT_DIR}/docker-compose.${RUN_ID}.yml}"
ROUTE_BIN="${ROUTE_BIN:-${RESULT_DIR}/cefas-route-loadtest}"
SUMMARY_FILE="${SUMMARY_FILE:-${RESULT_DIR}/summary.md}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

require_cmd docker
require_cmd go
require_cmd curl
require_cmd jq

mkdir -p "$RESULT_DIR" "$RESULT_DIR/logs" "$RESULT_DIR/reports" "$RESULT_DIR/metrics" "$RESULT_DIR/status"

node_map() {
  local out=""
  local i
  for i in $(seq 1 "$NODES"); do
    if [[ -n "$out" ]]; then
      out+=","
    fi
    out+="n${i}=localhost:$((GRPC_PORT_BASE + i))"
  done
  printf '%s' "$out"
}

peer_map() {
  local out=""
  local i
  for i in $(seq 1 "$NODES"); do
    if [[ -n "$out" ]]; then
      out+=","
    fi
    out+="n${i}=cefas-node-${i}:7000"
  done
  printf '%s' "$out"
}

http_peer_map() {
  local out=""
  local i
  for i in $(seq 1 "$NODES"); do
    if [[ -n "$out" ]]; then
      out+=","
    fi
    out+="n${i}=http://cefas-node-${i}:8080"
  done
  printf '%s' "$out"
}

emit_extra_command_args() {
  if [[ -z "$SERVER_EXTRA_ARGS" ]]; then
    return
  fi
  local args
  # shellcheck disable=SC2206
  args=($SERVER_EXTRA_ARGS)
  local arg
  for arg in "${args[@]}"; do
    printf '      - "%s"\n' "$arg"
  done
}

write_compose() {
  local peers http_peers i
  peers="$(peer_map)"
  http_peers="$(http_peer_map)"
  {
    cat <<YAML
x-cefas-common: &cefas-common
  image: ${IMAGE}
  pull_policy: never
  environment:
    CEFAS_STORAGE_PROFILE: ${STORAGE_PROFILE}
  expose:
    - "7000"
    - "8080"
    - "9090"

services:
YAML
    for i in $(seq 1 "$NODES"); do
      cat <<YAML
  cefas-node-${i}:
    <<: *cefas-common
    command:
      - "-data"
      - "/var/lib/cefas"
      - "-http"
      - ":8080"
      - "-grpc"
      - ":9090"
      - "-grpc-reflection"
      - "-raft-bootstrap"
      - "-raft-id"
      - "n${i}"
      - "-mux"
      - "cefas-node-${i}:7000"
      - "-shards"
      - "${SHARDS}"
      - "-replication-factor"
      - "${REPLICATION_FACTOR}"
YAML
      emit_extra_command_args
      cat <<YAML
      - "-raft-peers"
      - "${peers}"
      - "-raft-http-peers"
      - "${http_peers}"
    ports:
      - "$((HTTP_PORT_BASE + i)):8080"
      - "$((GRPC_PORT_BASE + i)):9090"
    volumes:
      - cefas-node-${i}-data:/var/lib/cefas

YAML
    done
    echo "volumes:"
    for i in $(seq 1 "$NODES"); do
      echo "  cefas-node-${i}-data:"
    done
  } > "$COMPOSE_FILE"
}

build_binaries() {
  go build -o "$ROUTE_BIN" ./cmd/cefas-route-loadtest
}

build_image() {
  if [[ "$BUILD_IMAGE" != "1" ]]; then
    return
  fi
  docker build \
    -f deploy/Dockerfile \
    -t "$IMAGE" \
    --build-arg "VERSION=bench-${RUN_ID}" \
    --build-arg "COMMIT=$(git rev-parse --short HEAD)" \
    .
}

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

cleanup_cluster() {
  if [[ "$KEEP_CLUSTER" == "1" ]]; then
    return
  fi
  compose down -v >/dev/null 2>&1 || true
}

ACTIVE_SAMPLER_PID=""

stop_phase_sampler() {
  local pid="${1:-}"
  if [[ -z "$pid" ]]; then
    return
  fi
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
}

cleanup() {
  stop_phase_sampler "${ACTIVE_SAMPLER_PID:-}"
  cleanup_cluster
}

trap cleanup EXIT

wait_for_cluster() {
  local attempt ready leaders
  for attempt in $(seq 1 90); do
    ready=0
    leaders=0
    local i
    for i in $(seq 1 "$NODES"); do
      local port=$((HTTP_PORT_BASE + i))
      if curl -fsS "http://localhost:${port}/metrics" >/dev/null 2>&1; then
        ready=$((ready + 1))
        leaders=$((leaders + $(curl -fsS "http://localhost:${port}/metrics" 2>/dev/null | awk '/cefas_raft_is_leader\{/ && $NF == 1 {c++} END {print c+0}')))
      fi
    done
    if [[ "$ready" -eq "$NODES" && "$leaders" -ge "$SHARDS" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "cluster did not become ready: ready=${ready:-0}/${NODES} leaders=${leaders:-0}/${SHARDS}" >&2
  return 1
}

capture_snapshot() {
  local phase="$1"
  local out="$RESULT_DIR/metrics/${phase}"
  mkdir -p "$out"
  {
    echo "phase=${phase}"
    echo "run_id=${RUN_ID}"
    echo "commit=$(git rev-parse --short HEAD)"
    echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    compose ps || true
  } > "$out/compose_ps.txt"

  compose ps -q | xargs docker stats --no-stream > "$out/docker_stats.txt" 2>/dev/null || true

  local i
  for i in $(seq 1 "$NODES"); do
    local port=$((HTTP_PORT_BASE + i))
    curl -fsS "http://localhost:${port}/metrics" > "$out/node${i}.prom" 2>/dev/null || true
    awk '/cefas_raft_is_leader\{/ && $NF == 1 {print}' "$out/node${i}.prom" > "$out/node${i}_leaders.prom" || true
    awk '/cefas_backpressure_state|cefas_pebble_compaction_debt_bytes|cefas_raft_/ {print}' "$out/node${i}.prom" > "$out/node${i}_storage_raft.prom" || true
  done
}

capture_phase_sample_once() {
  local phase="$1"
  local seq="$2"
  local out="$RESULT_DIR/metrics/${phase}_series"
  local ts
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$out"
  {
    echo "phase=${phase}"
    echo "sample=${seq}"
    echo "run_id=${RUN_ID}"
    echo "commit=$(git rev-parse --short HEAD)"
    echo "captured_at=${ts}"
    echo
    compose ps || true
  } > "$out/${seq}_${ts}_compose_ps.txt"

  compose ps -q | xargs docker stats --no-stream --format \
    'name={{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}} net={{.NetIO}} block={{.BlockIO}} pids={{.PIDs}}' \
    > "$out/${seq}_${ts}_docker_stats.txt" 2>/dev/null || true

  local i
  for i in $(seq 1 "$NODES"); do
    local port=$((HTTP_PORT_BASE + i))
    curl -fsS "http://localhost:${port}/metrics" \
      | awk -v re="$PHASE_SAMPLE_FILTER" '$0 ~ re {print}' \
      > "$out/${seq}_${ts}_node${i}.prom" 2>/dev/null || true
  done
}

start_phase_sampler() {
  local phase="$1"
  ACTIVE_SAMPLER_PID=""
  if [[ "$PHASE_SAMPLE_INTERVAL" == "0" || -z "$PHASE_SAMPLE_INTERVAL" ]]; then
    return
  fi
  if ! [[ "$PHASE_SAMPLE_INTERVAL" =~ ^[0-9]+$ ]] || [[ "$PHASE_SAMPLE_INTERVAL" -le 0 ]]; then
    echo "PHASE_SAMPLE_INTERVAL must be a positive integer number of seconds, or 0 to disable sampling" >&2
    exit 2
  fi

  capture_phase_sample_once "$phase" "0000"
  (
    seq_n=1
    while true; do
      sleep "$PHASE_SAMPLE_INTERVAL" || exit 0
      capture_phase_sample_once "$phase" "$(printf '%04d' "$seq_n")"
      seq_n=$((seq_n + 1))
    done
  ) &
  ACTIVE_SAMPLER_PID="$!"
}

record_status() {
  local phase="$1"
  local status="$2"
  printf '%s\n' "$status" > "$RESULT_DIR/status/${phase}.status"
}

run_phase() {
  local phase="$1"
  shift
  local log="$RESULT_DIR/logs/${phase}.log"
  echo "== ${phase} =="
  capture_snapshot "${phase}_before"
  start_phase_sampler "$phase"
  set +e
  "$@" 2>&1 | tee "$log"
  local status=${PIPESTATUS[0]}
  set -e
  stop_phase_sampler "$ACTIVE_SAMPLER_PID"
  ACTIVE_SAMPLER_PID=""
  record_status "$phase" "$status"
  capture_snapshot "${phase}_after"
  if [[ "$status" -ne 0 ]]; then
    echo "phase ${phase} failed with status ${status}" >&2
  fi
  return "$status"
}

status_label() {
  local phase="$1"
  local file="$RESULT_DIR/status/${phase}.status"
  if [[ ! -f "$file" ]]; then
    printf 'NOT_RUN'
    return
  fi
  if [[ "$(cat "$file")" == "0" ]]; then
    printf 'PASS'
  else
    printf 'FAIL'
  fi
}

append_report_rows() {
  local phase="$1"
  local json="$2"
  local label
  label="$(status_label "$phase")"
  if [[ ! -f "$json" ]]; then
    printf '| %s | %s | n/a | n/a | n/a | n/a | n/a | n/a | n/a |\n' "$phase" "$label" >> "$SUMMARY_FILE"
    return
  fi
  jq -r --arg status "$label" --arg phase "$phase" '
    def rate:
      ((.throughput_per_second // .throughput_units_per_second // 0) | floor | tostring) + "/s";
    def ms($value):
      if $value == null then
        "n/a"
      else
        (((($value * 10) | round) / 10) | tostring) + "ms"
      end;
    .phases[] |
    "| \($phase)/\(.name) | \($status) | \(.units) | \(rate) | \(.errors) | \(.found // "n/a") | \(ms(.latency_p50_ms // .p50_ms)) | \(ms(.latency_p95_ms // .p95_ms)) | \(ms(.latency_p99_ms // .p99_ms)) |"
  ' "$json" >> "$SUMMARY_FILE"
}

phase_sample_interval_label() {
  if [[ "$PHASE_SAMPLE_INTERVAL" == "0" || -z "$PHASE_SAMPLE_INTERVAL" ]]; then
    printf 'disabled'
  else
    printf '%ss' "$PHASE_SAMPLE_INTERVAL"
  fi
}

client_route_aware_args() {
  if [[ "$CLIENT_ROUTE_AWARE_READS" == "1" ]]; then
    printf '%s\n' "-client-route-aware-reads"
  fi
}

with_stream_args() {
  if [[ "$WITH_STREAM" == "1" ]]; then
    printf '%s\n' "-with-stream"
  fi
}

with_plugin_index_args() {
  if [[ -n "$WITH_PLUGIN_INDEX" ]]; then
    printf '%s\n%s\n' "-with-plugin-index" "$WITH_PLUGIN_INDEX"
  fi
}

phase_sample_file_count() {
  local phase="$1"
  local dir="$RESULT_DIR/metrics/${phase}_series"
  if [[ ! -d "$dir" ]]; then
    printf '0'
    return
  fi
  find "$dir" -type f -name '*.prom' | wc -l | tr -d ' '
}

phase_sample_metric_max() {
  local phase="$1"
  local metric="$2"
  local dir="$RESULT_DIR/metrics/${phase}_series"
  if [[ ! -d "$dir" ]]; then
    printf 'n/a'
    return
  fi
  local files=("$dir"/*.prom)
  if [[ ! -e "${files[0]}" ]]; then
    printf 'n/a'
    return
  fi
  awk -v metric="$metric" '
    $1 == metric || index($1, metric "{") == 1 {
      value = $NF + 0
      if (!seen || value > max) {
        max = value
        seen = 1
      }
    }
    END {
      if (!seen) {
        printf "n/a"
      } else if (max == int(max)) {
        printf "%.0f", max
      } else {
        printf "%.2f", max
      }
    }
  ' "${files[@]}"
}

append_phase_sample_summary() {
  if [[ "$PHASE_SAMPLE_INTERVAL" == "0" || -z "$PHASE_SAMPLE_INTERVAL" ]]; then
    return
  fi
  {
    echo
    echo "Phase samples:"
    echo
    echo "| Phase | Prom files | Max read amp | Max L0 files | Max compaction debt bytes | Max compactions in progress | Max lane queue | Max lane active | Max backpressure state |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  } >> "$SUMMARY_FILE"

  local phase
  for phase in smoke write_only read_seed read_only mixed; do
    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
      "$phase" \
      "$(phase_sample_file_count "$phase")" \
      "$(phase_sample_metric_max "$phase" "cefas_pebble_read_amp")" \
      "$(phase_sample_metric_max "$phase" "cefas_pebble_l0_files")" \
      "$(phase_sample_metric_max "$phase" "cefas_pebble_compaction_debt_bytes")" \
      "$(phase_sample_metric_max "$phase" "cefas_pebble_compactions_in_progress")" \
      "$(phase_sample_metric_max "$phase" "cefas_storage_lane_queue_depth")" \
      "$(phase_sample_metric_max "$phase" "cefas_storage_lane_active_workers")" \
      "$(phase_sample_metric_max "$phase" "cefas_backpressure_state")" \
      >> "$SUMMARY_FILE"
  done
}

write_summary() {
  {
    echo "# Cefas 8-node benchmark ${RUN_ID}"
    echo
    echo "- commit: \`$(git rev-parse --short HEAD)\`"
    echo "- project: \`${PROJECT}\`"
    echo "- image: \`${IMAGE}\`"
    echo "- nodes: \`${NODES}\`"
    echo "- shards: \`${SHARDS}\`"
    echo "- replication factor: \`${REPLICATION_FACTOR}\`"
    echo "- storage profile: \`${STORAGE_PROFILE}\`"
    echo "- server extra args: \`${SERVER_EXTRA_ARGS:-none}\`"
    echo "- batch size: \`${BATCH_SIZE}\`"
    echo "- payload bytes: \`${PAYLOAD_BYTES}\`"
    echo "- payload mode: \`${PAYLOAD_MODE}\`"
    echo "- write workers: \`${WRITE_WORKERS}\`"
    echo "- read workers: \`${READ_WORKERS}\`"
    echo "- write rate: \`${WRITE_RATE}\`"
    echo "- read rate: \`${READ_RATE}\`"
    echo "- client route-aware reads: \`${CLIENT_ROUTE_AWARE_READS}\`"
    echo "- with stream: \`${WITH_STREAM}\`"
    echo "- with plugin index: \`${WITH_PLUGIN_INDEX:-none}\`"
    echo "- phase sample interval: \`$(phase_sample_interval_label)\`"
    echo "- keep cluster: \`${KEEP_CLUSTER}\`"
    echo
    echo "| Phase | Status | Units | Throughput | Errors | Found | p50 | p95 | p99 |"
    echo "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  } > "$SUMMARY_FILE"
  append_report_rows "smoke" "$RESULT_DIR/reports/smoke_${RUN_ID}.json"
  append_report_rows "write_only" "$RESULT_DIR/reports/write_only_${RUN_ID}.json"
  append_report_rows "read_seed" "$RESULT_DIR/reports/read_seed_${RUN_ID}.json"
  append_report_rows "read_only" "$RESULT_DIR/reports/read_only_${RUN_ID}.json"
  append_report_rows "mixed" "$RESULT_DIR/reports/mixed_${RUN_ID}.json"
  append_phase_sample_summary
  {
    echo
    echo "Artifacts:"
    echo "- compose: \`${COMPOSE_FILE}\`"
    echo "- reports: \`${RESULT_DIR}/reports\`"
    echo "- logs: \`${RESULT_DIR}/logs\`"
    echo "- metrics: \`${RESULT_DIR}/metrics\`"
  } >> "$SUMMARY_FILE"
}

NODE_MAP="$(node_map)"
WRITE_TABLE="BenchWrite_${RUN_ID}"
READ_TABLE="BenchRead_${RUN_ID}"
SMOKE_TABLE="BenchSmoke_${RUN_ID}"

write_compose
build_binaries
build_image

if [[ "$RESET_CLUSTER" == "1" ]]; then
  compose down -v >/dev/null 2>&1 || true
fi
compose up -d
wait_for_cluster
capture_snapshot "cluster_ready"

failures=0

run_phase "smoke" "$ROUTE_BIN" \
  -nodes "$NODE_MAP" \
  $(client_route_aware_args) $(with_stream_args) $(with_plugin_index_args) \
  -table "$SMOKE_TABLE" \
  -items "$SMOKE_ITEMS" \
  -mixed-duration "$SMOKE_DURATION" \
  -mixed-writes=true \
  -mixed-reads=true \
  -batch-size "$BATCH_SIZE" \
  -workers 16 \
  -read-workers 64 \
  -write-rate 0 \
  -read-rate 0 \
  -payload-bytes "$PAYLOAD_BYTES" \
  -payload-mode "$PAYLOAD_MODE" \
  -rpc-timeout "$RPC_TIMEOUT" \
  -latency-sample-rate "$LATENCY_SAMPLE_RATE" \
  -progress "$PROGRESS_INTERVAL" \
  -label "smoke-${RUN_ID}" \
  -json-output "$RESULT_DIR/reports/smoke_${RUN_ID}.json" || failures=$((failures + 1))

run_phase "write_only" "$ROUTE_BIN" \
  -nodes "$NODE_MAP" \
  $(client_route_aware_args) $(with_stream_args) $(with_plugin_index_args) \
  -table "$WRITE_TABLE" \
  -items 0 \
  -mixed-duration "$WRITE_DURATION" \
  -mixed-writes=true \
  -mixed-reads=false \
  -batch-size "$BATCH_SIZE" \
  -workers "$WRITE_WORKERS" \
  -read-workers "$READ_WORKERS" \
  -write-rate "$WRITE_RATE" \
  -read-rate "$READ_RATE" \
  -payload-bytes "$PAYLOAD_BYTES" \
  -payload-mode "$PAYLOAD_MODE" \
  -rpc-timeout "$RPC_TIMEOUT" \
  -latency-sample-rate "$LATENCY_SAMPLE_RATE" \
  -progress "$PROGRESS_INTERVAL" \
  -label "write-only-${RUN_ID}" \
  -json-output "$RESULT_DIR/reports/write_only_${RUN_ID}.json" || failures=$((failures + 1))

run_phase "read_seed" "$ROUTE_BIN" \
  -nodes "$NODE_MAP" \
  $(client_route_aware_args) $(with_stream_args) $(with_plugin_index_args) \
  -table "$READ_TABLE" \
  -items "$READ_SEED_ITEMS" \
  -mixed-duration 0s \
  -mixed-writes=false \
  -mixed-reads=false \
  -batch-size "$BATCH_SIZE" \
  -workers "$WRITE_WORKERS" \
  -read-workers "$READ_WORKERS" \
  -write-rate "$WRITE_RATE" \
  -read-rate "$READ_RATE" \
  -payload-bytes "$PAYLOAD_BYTES" \
  -payload-mode "$PAYLOAD_MODE" \
  -rpc-timeout "$RPC_TIMEOUT" \
  -latency-sample-rate "$LATENCY_SAMPLE_RATE" \
  -progress "$PROGRESS_INTERVAL" \
  -label "read-seed-${RUN_ID}" \
  -json-output "$RESULT_DIR/reports/read_seed_${RUN_ID}.json" || failures=$((failures + 1))

run_phase "read_only" "$ROUTE_BIN" \
  -nodes "$NODE_MAP" \
  $(client_route_aware_args) $(with_stream_args) $(with_plugin_index_args) \
  -table "$READ_TABLE" \
  -items 0 \
  -keyspace "$READ_SEED_ITEMS" \
  -mixed-duration "$READ_DURATION" \
  -mixed-writes=false \
  -mixed-reads=true \
  -batch-size "$BATCH_SIZE" \
  -workers "$WRITE_WORKERS" \
  -read-workers "$READ_WORKERS" \
  -write-rate "$WRITE_RATE" \
  -read-rate "$READ_RATE" \
  -payload-bytes "$PAYLOAD_BYTES" \
  -payload-mode "$PAYLOAD_MODE" \
  -rpc-timeout "$RPC_TIMEOUT" \
  -latency-sample-rate "$LATENCY_SAMPLE_RATE" \
  -progress "$PROGRESS_INTERVAL" \
  -label "read-only-${RUN_ID}" \
  -json-output "$RESULT_DIR/reports/read_only_${RUN_ID}.json" || failures=$((failures + 1))

run_phase "mixed" "$ROUTE_BIN" \
  -nodes "$NODE_MAP" \
  $(client_route_aware_args) $(with_stream_args) $(with_plugin_index_args) \
  -table "$READ_TABLE" \
  -items 0 \
  -keyspace "$READ_SEED_ITEMS" \
  -mixed-duration "$MIXED_DURATION" \
  -mixed-writes=true \
  -mixed-reads=true \
  -batch-size "$BATCH_SIZE" \
  -workers "$WRITE_WORKERS" \
  -read-workers "$READ_WORKERS" \
  -write-rate "$WRITE_RATE" \
  -read-rate "$READ_RATE" \
  -payload-bytes "$PAYLOAD_BYTES" \
  -payload-mode "$PAYLOAD_MODE" \
  -rpc-timeout "$RPC_TIMEOUT" \
  -latency-sample-rate "$LATENCY_SAMPLE_RATE" \
  -progress "$PROGRESS_INTERVAL" \
  -label "mixed-${RUN_ID}" \
  -json-output "$RESULT_DIR/reports/mixed_${RUN_ID}.json" || failures=$((failures + 1))

capture_snapshot "final"
write_summary

echo "summary: $SUMMARY_FILE"
if [[ "$failures" -ne 0 && "$ALLOW_FAILURES" != "1" ]]; then
  exit 1
fi
