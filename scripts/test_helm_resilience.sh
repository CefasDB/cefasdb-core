#!/usr/bin/env bash
set -euo pipefail

CHART="${CHART:-dist/helm/cefas}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

require_grep() {
  local pattern="$1"
  local file="$2"
  if ! grep -q "$pattern" "$file"; then
    echo "missing pattern: $pattern" >&2
    echo "file: $file" >&2
    exit 1
  fi
}

expect_helm_failure() {
  local message="$1"
  shift
  local err_file="$TMP_DIR/error.log"
  if helm template cefas-bad "$CHART" "$@" >"$TMP_DIR/bad.yaml" 2>"$err_file"; then
    echo "helm template unexpectedly passed: $*" >&2
    exit 1
  fi
  require_grep "$message" "$err_file"
}

resilient="$TMP_DIR/resilient.yaml"
helm template cefas-resilient "$CHART" \
  --namespace cefas-test \
  --set resilience.enabled=true \
  --set cluster.shards=24 \
  >"$resilient"

require_grep "kind: PodDisruptionBudget" "$resilient"
require_grep "replicas: 3" "$resilient"
require_grep "replicationFactor: 3" "$resilient"
require_grep "peers:" "$resilient"
require_grep "httpPeers:" "$resilient"
require_grep "grpcPeers:" "$resilient"
require_grep "podAntiAffinity:" "$resilient"
require_grep "topologySpreadConstraints:" "$resilient"
require_grep "cefasdb.io/failure-domain" "$resilient"
require_grep "failureThreshold: 120" "$resilient"
require_grep "app.kubernetes.io/name: cefas-manager" "$resilient"

no_cpu_limit="$TMP_DIR/no-cpu-limit.yaml"
helm template cefas-perf "$CHART" \
  --namespace cefas-test \
  --set resilience.enabled=true \
  --set resourcePolicy.disableCPULimits=true \
  >"$no_cpu_limit"
if grep -q 'cpu: "1000m"' "$no_cpu_limit"; then
  echo "database CPU limit should be omitted when resourcePolicy.disableCPULimits=true" >&2
  exit 1
fi
require_grep 'memory: "2Gi"' "$no_cpu_limit"

expect_helm_failure "resilience.replicas must be >= 3" \
  --set resilience.enabled=true \
  --set resilience.replicas=2

expect_helm_failure "requires persistence.enabled=true" \
  --set resilience.enabled=true \
  --set persistence.enabled=false

expect_helm_failure "replication factor cannot exceed rendered database replicas" \
  --set resilience.enabled=true \
  --set cluster.replicationFactor=4

echo "helm resilience chart tests passed"
