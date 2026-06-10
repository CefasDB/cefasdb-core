#!/usr/bin/env bash
set -euo pipefail

PROFILES="${PROFILES:-default write-heavy}"
BASE_PROJECT="${BASE_PROJECT:-cefas-profile}"
BASE_RESULT_DIR="${BASE_RESULT_DIR:-/tmp/cefas-bench/profiles}"
COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker-compose.cluster.yml}"

for profile in $PROFILES; do
  safe_profile="${profile//[^a-zA-Z0-9_-]/-}"
  echo "== profile: $profile =="
  PROJECT="${BASE_PROJECT}-${safe_profile}" \
    RESULT_DIR="${BASE_RESULT_DIR}/${safe_profile}" \
    COMPOSE_FILE="$COMPOSE_FILE" \
    STORAGE_PROFILE="$profile" \
    RESET_CLUSTER="${RESET_CLUSTER:-1}" \
    scripts/bench_cluster.sh
done

echo "Profile comparison results under $BASE_RESULT_DIR"
