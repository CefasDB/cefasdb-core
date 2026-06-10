#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${OUT_DIR:-/tmp/cefas-bench/metrics}"
INTERVAL="${INTERVAL:-5}"
HTTP_ENDPOINTS="${HTTP_ENDPOINTS:-http://localhost:18081/metrics,http://localhost:18082/metrics,http://localhost:18083/metrics}"
CONTAINERS="${CONTAINERS:-cefas-cluster-cefas-node-1-1 cefas-cluster-cefas-node-2-1 cefas-cluster-cefas-node-3-1}"

mkdir -p "$OUT_DIR"

capture_prometheus() {
  local ts="$1"
  local idx=0
  IFS=',' read -r -a endpoints <<< "$HTTP_ENDPOINTS"
  for endpoint in "${endpoints[@]}"; do
    idx=$((idx + 1))
    curl -fsS "$endpoint" \
      | rg '^(cefas_pebble_|cefas_op_|cefas_raft_|go_memstats_|process_)' \
      > "$OUT_DIR/${ts}_node${idx}.prom" || true
  done
}

capture_docker() {
  local ts="$1"
  docker stats --no-stream --format \
    'name={{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}} net={{.NetIO}} block={{.BlockIO}} pids={{.PIDs}}' \
    $CONTAINERS > "$OUT_DIR/${ts}_docker_stats.txt" || true
}

while true; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  capture_prometheus "$ts"
  capture_docker "$ts"
  sleep "$INTERVAL"
done
