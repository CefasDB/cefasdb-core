#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-dry-run}"
CHART="${CHART:-dist/helm/cefas}"
RELEASE="${RELEASE:-cefas-resilience}"
NAMESPACE="${NAMESPACE:-cefas-resilience}"
APP="${APP:-${RELEASE}-cefas}"
SELECTOR="${SELECTOR:-app.kubernetes.io/name=cefas,app.kubernetes.io/instance=${RELEASE}}"
ARTIFACT_DIR="${ARTIFACT_DIR:-/tmp/cefas-resilience/$(date -u +%Y%m%dT%H%M%SZ)}"
SCENARIOS="${SCENARIOS:-healthy-baseline,pod-kill,node-drain,node-shutdown,orphan-process,disk-pressure,network-partition,two-node-failure}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
REPLICAS="${REPLICAS:-3}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"
SHARDS="${SHARDS:-24}"
DISABLE_CPU_LIMITS="${DISABLE_CPU_LIMITS:-true}"
ALLOW_DESTRUCTIVE="${ALLOW_DESTRUCTIVE:-0}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-5m}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-300}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-120s}"
DOCTOR_TIMEOUT="${DOCTOR_TIMEOUT:-45s}"
QUORUM_READY="${QUORUM_READY:-2}"
POST_INJECTION_SETTLE_SECONDS="${POST_INJECTION_SETTLE_SECONDS:-10}"
HELM_EXTRA_ARGS="${HELM_EXTRA_ARGS:-}"

RUN_STATUS=0
NAMESPACE_CREATED=0
CORDONED_NODES_FILE=""
SUMMARY_FILE=""

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

kubectl_cmd() {
  if [ -n "$KUBE_CONTEXT" ]; then
    kubectl --context "$KUBE_CONTEXT" "$@"
  else
    kubectl "$@"
  fi
}

helm_render_args() {
  printf '%s\n' \
    --namespace "$NAMESPACE" \
    --set resilience.enabled=true \
    --set resilience.replicas="$REPLICAS" \
    --set resilience.replicationFactor="$REPLICATION_FACTOR" \
    --set cluster.replicationFactor="$REPLICATION_FACTOR" \
    --set cluster.shards="$SHARDS" \
    --set manager.enabled=true \
    --set resourcePolicy.disableCPULimits="$DISABLE_CPU_LIMITS"
}

summary_escape() {
  printf '%s' "$1" | tr '\n' ' ' | sed 's/|/\\|/g'
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

init_artifacts() {
  mkdir -p "$ARTIFACT_DIR/scenarios"
  CORDONED_NODES_FILE="$ARTIFACT_DIR/cordoned-nodes.txt"
  SUMMARY_FILE="$ARTIFACT_DIR/summary.md"
  : >"$CORDONED_NODES_FILE"
  cat >"$SUMMARY_FILE" <<EOF
# CefasDB Kubernetes resilience suite

- mode: \`$MODE\`
- release: \`$RELEASE\`
- namespace: \`$NAMESPACE\`
- replicas: \`$REPLICAS\`
- replication factor: \`$REPLICATION_FACTOR\`
- shards: \`$SHARDS\`
- scenarios: \`$SCENARIOS\`

| Scenario | Status | Expected behavior | Artifacts | Notes |
| --- | --- | --- | --- | --- |
EOF
}

write_matrix_json() {
  cat >"$ARTIFACT_DIR/matrix.json" <<'JSON'
[
  {
    "scenario": "healthy-baseline",
    "failure": "none",
    "expectedService": "all database Pods ready; cefas-manager doctor reports healthy or degraded-but-serving",
    "recovery": "not applicable",
    "stopCondition": "doctor command fails or reports unsafe"
  },
  {
    "scenario": "pod-kill",
    "failure": "delete one cefasdb Pod without deleting PVCs",
    "expectedService": "replacement Pod starts and RF=3 remains serving during and after recovery",
    "recovery": "StatefulSet recreates the Pod; startup probe allows raft replay",
    "stopCondition": "ready quorum is not restored or doctor reports unsafe"
  },
  {
    "scenario": "node-drain",
    "failure": "cordon one Kubernetes node and drain CefasDB Pods from it",
    "expectedService": "one failure domain can be unavailable while reads and writes remain serving",
    "recovery": "uncordon the node and let Kubernetes reschedule",
    "stopCondition": "quorum-ready Pod count is lost or doctor reports unsafe"
  },
  {
    "scenario": "node-shutdown",
    "failure": "provider/Talos hook shuts down one node",
    "expectedService": "one physical host loss remains serving or explicitly degraded-but-serving",
    "recovery": "provider/Talos hook powers the node back on",
    "stopCondition": "doctor reports unsafe after one-node loss"
  },
  {
    "scenario": "orphan-process",
    "failure": "provider/Talos hook leaves or simulates a stale process with an old raft identity",
    "expectedService": "stale identity is fenced by the Kubernetes lease; active cluster remains serving",
    "recovery": "provider/Talos hook removes the stale process",
    "stopCondition": "stale process can serve or CPU-spins without a valid lease"
  },
  {
    "scenario": "disk-pressure",
    "failure": "provider/Talos hook applies disk pressure on one database node",
    "expectedService": "manager reports the pressure and the database stays serving or degraded-but-serving",
    "recovery": "provider/Talos hook removes pressure and confirms PVC health",
    "stopCondition": "Pods spin indefinitely or doctor reports unsafe for a one-node fault"
  },
  {
    "scenario": "network-partition",
    "failure": "provider/Talos hook partitions one database node from the cluster",
    "expectedService": "majority side remains serving and the isolated node is not accepted as healthy",
    "recovery": "provider/Talos hook removes the partition and raft catches up",
    "stopCondition": "split brain is observed or manager cannot identify unsafe state"
  },
  {
    "scenario": "two-node-failure",
    "failure": "two failure domains unavailable at the same time",
    "expectedService": "RF=3 fails safely with clear quorum or unsafe reporting",
    "recovery": "restore nodes/hooks and wait for raft catch-up",
    "stopCondition": "Pods CPU-spin without functional quorum or health reporting is ambiguous"
  }
]
JSON
}

append_result() {
  local scenario="$1"
  local status="$2"
  local expected="$3"
  local notes="$4"
  local dir="scenarios/$scenario"
  local status_label="$status"

  case "$status" in
    pass) status_label="PASS" ;;
    fail) status_label="FAIL" ;;
    skipped) status_label="SKIPPED" ;;
    planned) status_label="PLANNED" ;;
  esac

  printf '| `%s` | %s | %s | `%s` | %s |\n' \
    "$(summary_escape "$scenario")" \
    "$status_label" \
    "$(summary_escape "$expected")" \
    "$dir" \
    "$(summary_escape "$notes")" >>"$SUMMARY_FILE"

  mkdir -p "$ARTIFACT_DIR/$dir"
  cat >"$ARTIFACT_DIR/$dir/result.json" <<EOF
{
  "scenario": "$(json_escape "$scenario")",
  "status": "$(json_escape "$status")",
  "expected": "$(json_escape "$expected")",
  "notes": "$(json_escape "$notes")",
  "artifacts": "$(json_escape "$dir")"
}
EOF

  if [ "$status" = "fail" ]; then
    RUN_STATUS=1
  fi
}

capture() {
  local output="$1"
  shift
  "$@" >"$output" 2>&1
}

render_chart() {
  require_cmd helm
  local rendered="$ARTIFACT_DIR/rendered.yaml"
  local lint_out="$ARTIFACT_DIR/helm-lint.txt"
  log "rendering Helm chart into $rendered"
  helm lint "$CHART" >"$lint_out" 2>&1
  # shellcheck disable=SC2086
  helm template "$RELEASE" "$CHART" $(helm_render_args) $HELM_EXTRA_ARGS >"$rendered"
  grep -q 'kind: StatefulSet' "$rendered" || die "rendered chart is missing StatefulSet"
  grep -q 'kind: PodDisruptionBudget' "$rendered" || die "rendered chart is missing PodDisruptionBudget"
  grep -q 'replicationFactor: 3' "$rendered" || die "rendered chart is missing RF=3 config"
  grep -q 'peers:' "$rendered" || die "rendered chart is missing multi-node peers"
  grep -q 'app.kubernetes.io/name: cefas-manager' "$rendered" || die "rendered chart is missing manager deployment"
}

install_chart() {
  require_cmd kubectl
  require_cmd helm
  kubectl_cmd cluster-info >"$ARTIFACT_DIR/kubectl-cluster-info.txt" 2>&1

  if ! kubectl_cmd get namespace "$NAMESPACE" >/dev/null 2>&1; then
    NAMESPACE_CREATED=1
  fi

  log "installing $RELEASE in namespace $NAMESPACE"
  # shellcheck disable=SC2086
  helm upgrade --install "$RELEASE" "$CHART" \
    --namespace "$NAMESPACE" \
    --create-namespace \
    --set resilience.enabled=true \
    --set resilience.replicas="$REPLICAS" \
    --set resilience.replicationFactor="$REPLICATION_FACTOR" \
    --set cluster.replicationFactor="$REPLICATION_FACTOR" \
    --set cluster.shards="$SHARDS" \
    --set manager.enabled=true \
    --set resourcePolicy.disableCPULimits="$DISABLE_CPU_LIMITS" \
    $HELM_EXTRA_ARGS \
    >"$ARTIFACT_DIR/helm-upgrade.txt" 2>&1
}

wait_full_rollout() {
  log "waiting for StatefulSet and manager rollout"
  kubectl_cmd -n "$NAMESPACE" rollout status "statefulset/$APP" --timeout="$ROLLOUT_TIMEOUT" \
    >"$ARTIFACT_DIR/rollout-statefulset.txt" 2>&1
  kubectl_cmd -n "$NAMESPACE" rollout status "deploy/${APP}-manager" --timeout="$ROLLOUT_TIMEOUT" \
    >"$ARTIFACT_DIR/rollout-manager.txt" 2>&1
}

ready_pods_count() {
  local out
  out="$(kubectl_cmd -n "$NAMESPACE" get pods -l "$SELECTOR" -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>/dev/null || true)"
  printf '%s\n' "$out" | awk '$1 == "true" { n++ } END { print n + 0 }'
}

wait_ready_count() {
  local min_ready="$1"
  local deadline
  deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))
  while [ "$(ready_pods_count)" -lt "$min_ready" ]; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      return 1
    fi
    sleep 2
  done
  return 0
}

collect_snapshot() {
  local scenario="$1"
  local phase="$2"
  local dir="$ARTIFACT_DIR/scenarios/$scenario"
  mkdir -p "$dir"
  capture "$dir/${phase}-pods.txt" kubectl_cmd -n "$NAMESPACE" get pods -l "$SELECTOR" -o wide || true
  capture "$dir/${phase}-manager-pods.txt" kubectl_cmd -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=cefas-manager,app.kubernetes.io/instance=$RELEASE" -o wide || true
  capture "$dir/${phase}-events.txt" kubectl_cmd -n "$NAMESPACE" get events --sort-by=.lastTimestamp || true
  capture "$dir/${phase}-endpoints.txt" kubectl_cmd -n "$NAMESPACE" get endpoints "${APP}" "${APP}-headless" -o yaml || true
  capture "$dir/${phase}-pvc.txt" kubectl_cmd -n "$NAMESPACE" get pvc -l "$SELECTOR" -o wide || true
  capture "$dir/${phase}-nodes.txt" kubectl_cmd get nodes -o wide || true
}

collect_logs() {
  local scenario="$1"
  local dir="$ARTIFACT_DIR/scenarios/$scenario"
  mkdir -p "$dir"
  capture "$dir/cefas-logs.txt" kubectl_cmd -n "$NAMESPACE" logs "statefulset/$APP" --all-containers --tail=500 || true
  capture "$dir/manager-logs.txt" kubectl_cmd -n "$NAMESPACE" logs "deploy/${APP}-manager" --all-containers --tail=500 || true
}

doctor_command() {
  kubectl_cmd -n "$NAMESPACE" exec "deploy/${APP}-manager" -c cefas-manager -- \
    /usr/local/bin/cefas-manager \
      --endpoint="${APP}:9090" \
      --http-endpoint="http://${APP}:8080" \
      --namespace="$NAMESPACE" \
      --selector="$SELECTOR" \
      --timeout="$DOCTOR_TIMEOUT" \
      --insecure=true \
      doctor
}

run_doctor() {
  local scenario="$1"
  local dir="$ARTIFACT_DIR/scenarios/$scenario"
  mkdir -p "$dir"
  doctor_command >"$dir/doctor.json" 2>"$dir/doctor.err"
}

doctor_is_unsafe() {
  local file="$1"
  grep -q '"classification"[[:space:]]*:[[:space:]]*"unsafe"' "$file"
}

doctor_is_serving() {
  local scenario="$1"
  local dir="$ARTIFACT_DIR/scenarios/$scenario"
  if ! run_doctor "$scenario"; then
    return 1
  fi
  if doctor_is_unsafe "$dir/doctor.json"; then
    return 2
  fi
  return 0
}

doctor_reports_unsafe() {
  local scenario="$1"
  local dir="$ARTIFACT_DIR/scenarios/$scenario"
  if ! run_doctor "$scenario"; then
    return 1
  fi
  doctor_is_unsafe "$dir/doctor.json"
}

first_db_pod() {
  kubectl_cmd -n "$NAMESPACE" get pods -l "$SELECTOR" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

node_for_pod() {
  local pod="$1"
  kubectl_cmd -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true
}

record_cordoned_node() {
  local node="$1"
  if ! grep -qx "$node" "$CORDONED_NODES_FILE" 2>/dev/null; then
    printf '%s\n' "$node" >>"$CORDONED_NODES_FILE"
  fi
}

cleanup_cordoned_nodes() {
  if [ -z "$CORDONED_NODES_FILE" ] || [ ! -f "$CORDONED_NODES_FILE" ]; then
    return
  fi
  while IFS= read -r node; do
    [ -n "$node" ] || continue
    log "uncordon node $node"
    kubectl_cmd uncordon "$node" >/dev/null 2>&1 || true
  done <"$CORDONED_NODES_FILE"
}

cleanup_release() {
  if [ "$MODE" != "live" ]; then
    return
  fi
  cleanup_cordoned_nodes
  if [ "$KEEP_CLUSTER" = "1" ]; then
    log "KEEP_CLUSTER=1; leaving release and PVCs in place"
    return
  fi
  log "cleaning Helm release and PVCs"
  helm uninstall "$RELEASE" -n "$NAMESPACE" >"$ARTIFACT_DIR/helm-uninstall.txt" 2>&1 || true
  kubectl_cmd -n "$NAMESPACE" delete pvc -l "$SELECTOR" --ignore-not-found \
    >"$ARTIFACT_DIR/pvc-delete.txt" 2>&1 || true
  if [ "$NAMESPACE_CREATED" = "1" ]; then
    kubectl_cmd delete namespace "$NAMESPACE" --ignore-not-found \
      >"$ARTIFACT_DIR/namespace-delete.txt" 2>&1 || true
  fi
}

on_exit() {
  local status="$1"
  if [ "$MODE" = "live" ]; then
    collect_snapshot "suite" "final" || true
    cleanup_release || true
  fi
  exit "$status"
}

run_healthy_baseline() {
  local scenario="healthy-baseline"
  local expected="all Pods ready; manager doctor is not unsafe"
  collect_snapshot "$scenario" "before"
  if wait_ready_count "$REPLICAS" && doctor_is_serving "$scenario"; then
    append_result "$scenario" pass "$expected" "baseline serving"
  else
    collect_logs "$scenario"
    append_result "$scenario" fail "$expected" "baseline failed; see doctor and logs"
  fi
  collect_snapshot "$scenario" "after"
}

run_pod_kill() {
  local scenario="pod-kill"
  local expected="one database Pod kill recovers to full ready state without unsafe doctor"
  local pod
  collect_snapshot "$scenario" "before"
  pod="$(first_db_pod)"
  if [ -z "$pod" ]; then
    append_result "$scenario" fail "$expected" "no database Pod found"
    return
  fi
  log "deleting database Pod $pod"
  if ! capture "$ARTIFACT_DIR/scenarios/$scenario/delete-pod.txt" kubectl_cmd -n "$NAMESPACE" delete pod "$pod" --wait=false; then
    append_result "$scenario" fail "$expected" "kubectl delete pod failed"
    return
  fi
  if wait_full_rollout && wait_ready_count "$REPLICAS" && doctor_is_serving "$scenario"; then
    append_result "$scenario" pass "$expected" "deleted $pod and recovered"
  else
    collect_logs "$scenario"
    append_result "$scenario" fail "$expected" "pod kill did not recover cleanly"
  fi
  collect_snapshot "$scenario" "after"
}

run_node_drain() {
  local scenario="node-drain"
  local expected="one node drained; quorum remains ready and doctor is not unsafe"
  local pod node
  collect_snapshot "$scenario" "before"
  if [ "$ALLOW_DESTRUCTIVE" != "1" ]; then
    append_result "$scenario" skipped "$expected" "set ALLOW_DESTRUCTIVE=1 to cordon and drain a real node"
    return
  fi
  pod="$(first_db_pod)"
  node="$(node_for_pod "$pod")"
  if [ -z "$node" ]; then
    append_result "$scenario" fail "$expected" "could not resolve node for pod $pod"
    return
  fi
  log "cordon and drain node $node"
  if ! capture "$ARTIFACT_DIR/scenarios/$scenario/cordon.txt" kubectl_cmd cordon "$node"; then
    append_result "$scenario" fail "$expected" "kubectl cordon failed"
    return
  fi
  record_cordoned_node "$node"
  if ! capture "$ARTIFACT_DIR/scenarios/$scenario/drain.txt" kubectl_cmd drain "$node" --pod-selector="$SELECTOR" --ignore-daemonsets --delete-emptydir-data --force --timeout="$DRAIN_TIMEOUT"; then
    collect_logs "$scenario"
    append_result "$scenario" fail "$expected" "kubectl drain failed; see drain artifact"
    return
  fi
  if wait_ready_count "$QUORUM_READY" && doctor_is_serving "$scenario"; then
    append_result "$scenario" pass "$expected" "drained $node and kept quorum ready"
  else
    collect_logs "$scenario"
    append_result "$scenario" fail "$expected" "cluster did not stay serving after draining $node"
  fi
  collect_snapshot "$scenario" "after"
}

run_hooked_serving_scenario() {
  local scenario="$1"
  local inject_command="$2"
  local recover_command="$3"
  local expected="$4"
  local pod node
  collect_snapshot "$scenario" "before"
  if [ "$ALLOW_DESTRUCTIVE" != "1" ]; then
    append_result "$scenario" skipped "$expected" "set ALLOW_DESTRUCTIVE=1 and the scenario hook to inject this failure"
    return
  fi
  if [ -z "$inject_command" ]; then
    append_result "$scenario" skipped "$expected" "scenario hook is not configured"
    return
  fi
  pod="$(first_db_pod)"
  node="$(node_for_pod "$pod")"
  export TARGET_POD="$pod" TARGET_NODE="$node" NAMESPACE RELEASE APP SELECTOR KUBE_CONTEXT
  log "running $scenario injection hook"
  if ! sh -c "$inject_command" >"$ARTIFACT_DIR/scenarios/$scenario/inject-hook.txt" 2>&1; then
    append_result "$scenario" fail "$expected" "injection hook failed"
    return
  fi
  sleep "$POST_INJECTION_SETTLE_SECONDS"
  if wait_ready_count "$QUORUM_READY" && doctor_is_serving "$scenario"; then
    append_result "$scenario" pass "$expected" "failure injected and cluster stayed serving"
  else
    collect_logs "$scenario"
    append_result "$scenario" fail "$expected" "cluster did not satisfy serving expectation"
  fi
  if [ -n "$recover_command" ]; then
    log "running $scenario recovery hook"
    sh -c "$recover_command" >"$ARTIFACT_DIR/scenarios/$scenario/recover-hook.txt" 2>&1 || true
  fi
  collect_snapshot "$scenario" "after"
}

run_two_node_failure() {
  local scenario="two-node-failure"
  local expected="two-node loss fails safely with explicit unsafe/quorum reporting"
  local inject_command="${TWO_NODE_FAILURE_COMMAND:-}"
  local recover_command="${TWO_NODE_RECOVER_COMMAND:-}"
  collect_snapshot "$scenario" "before"
  if [ "$ALLOW_DESTRUCTIVE" != "1" ]; then
    append_result "$scenario" skipped "$expected" "set ALLOW_DESTRUCTIVE=1 and TWO_NODE_FAILURE_COMMAND to inject this fault"
    return
  fi
  if [ -z "$inject_command" ]; then
    append_result "$scenario" skipped "$expected" "TWO_NODE_FAILURE_COMMAND is not configured"
    return
  fi
  log "running two-node failure injection hook"
  if ! sh -c "$inject_command" >"$ARTIFACT_DIR/scenarios/$scenario/inject-hook.txt" 2>&1; then
    append_result "$scenario" fail "$expected" "two-node injection hook failed"
    return
  fi
  sleep "$POST_INJECTION_SETTLE_SECONDS"
  if doctor_reports_unsafe "$scenario"; then
    append_result "$scenario" pass "$expected" "doctor reported unsafe/quorum loss as expected"
  else
    collect_logs "$scenario"
    append_result "$scenario" fail "$expected" "doctor did not report explicit unsafe state"
  fi
  if [ -n "$recover_command" ]; then
    log "running two-node recovery hook"
    sh -c "$recover_command" >"$ARTIFACT_DIR/scenarios/$scenario/recover-hook.txt" 2>&1 || true
  fi
  collect_snapshot "$scenario" "after"
}

run_dry_scenario() {
  local scenario="$1"
  local expected="$2"
  mkdir -p "$ARTIFACT_DIR/scenarios/$scenario"
  append_result "$scenario" planned "$expected" "dry-run mode rendered Helm and wrote the acceptance matrix; no cluster mutation"
}

run_scenario() {
  local scenario="$1"
  case "$MODE:$scenario" in
    dry-run:healthy-baseline) run_dry_scenario "$scenario" "RF=3 chart renders StatefulSet, peers, PDB, and manager" ;;
    dry-run:pod-kill) run_dry_scenario "$scenario" "pod kill is part of the live acceptance suite" ;;
    dry-run:node-drain) run_dry_scenario "$scenario" "node drain is guarded by ALLOW_DESTRUCTIVE=1" ;;
    dry-run:node-shutdown) run_dry_scenario "$scenario" "node shutdown uses NODE_SHUTDOWN_COMMAND and NODE_RESTORE_COMMAND hooks" ;;
    dry-run:orphan-process) run_dry_scenario "$scenario" "orphan process uses ORPHAN_PROCESS_COMMAND and ORPHAN_RECOVER_COMMAND hooks" ;;
    dry-run:disk-pressure) run_dry_scenario "$scenario" "disk pressure uses DISK_PRESSURE_COMMAND and DISK_RECOVER_COMMAND hooks" ;;
    dry-run:network-partition) run_dry_scenario "$scenario" "network partition uses NETWORK_PARTITION_COMMAND and NETWORK_RECOVER_COMMAND hooks" ;;
    dry-run:two-node-failure) run_dry_scenario "$scenario" "two-node failure uses TWO_NODE_FAILURE_COMMAND and TWO_NODE_RECOVER_COMMAND hooks" ;;
    live:healthy-baseline) run_healthy_baseline ;;
    live:pod-kill) run_pod_kill ;;
    live:node-drain) run_node_drain ;;
    live:node-shutdown) run_hooked_serving_scenario "$scenario" "${NODE_SHUTDOWN_COMMAND:-}" "${NODE_RESTORE_COMMAND:-}" "one physical node shutdown remains serving or degraded-but-serving" ;;
    live:orphan-process) run_hooked_serving_scenario "$scenario" "${ORPHAN_PROCESS_COMMAND:-}" "${ORPHAN_RECOVER_COMMAND:-}" "stale raft identity is fenced and active cluster remains serving" ;;
    live:disk-pressure) run_hooked_serving_scenario "$scenario" "${DISK_PRESSURE_COMMAND:-}" "${DISK_RECOVER_COMMAND:-}" "single-node disk pressure is reported without losing service" ;;
    live:network-partition) run_hooked_serving_scenario "$scenario" "${NETWORK_PARTITION_COMMAND:-}" "${NETWORK_RECOVER_COMMAND:-}" "majority side remains serving and isolated node is not healthy" ;;
    live:two-node-failure) run_two_node_failure ;;
    *) append_result "$scenario" fail "known scenario" "unknown scenario or mode" ;;
  esac
}

main() {
  case "$MODE" in
    dry-run|live) ;;
    *) die "MODE must be dry-run or live" ;;
  esac

  init_artifacts
  write_matrix_json
  render_chart

  if [ "$MODE" = "live" ]; then
    trap 'on_exit $?' EXIT
    install_chart
    wait_full_rollout
    wait_ready_count "$REPLICAS" || die "database Pods did not become ready"
  fi

  for scenario in $(printf '%s' "$SCENARIOS" | tr ',' ' '); do
    [ -n "$scenario" ] || continue
    log "scenario: $scenario"
    run_scenario "$scenario"
  done

  log "summary: $SUMMARY_FILE"
  return "$RUN_STATUS"
}

main "$@"
