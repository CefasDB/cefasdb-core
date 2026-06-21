#!/usr/bin/env bash
# bench_ab.sh — run bench_8node_matrix.sh N times each for a baseline ref
# and a candidate ref, then drive compare_bench.sh over all dirs so the
# delta table is computed against medians rather than single-run noise.
#
# The harness on this machine has a per-side shot-to-shot variance of
# ~9-15% on write_only/mixed-write. A 1+1 A/B can't discriminate the
# 3-5% deltas most micro-optimisations produce. 3 runs each side, lower-
# median per phase, brings the discriminable floor down to ~3% by simple
# law of medians.
#
# Usage:
#   bash scripts/bench/bench_ab.sh BASE_REF CAND_REF
#
# BASE_REF, CAND_REF: git refs (branch/commit/tag). CAND_REF defaults to
# the current branch HEAD.
#
# Environment:
#   RUNS            runs per side                (default 3)
#   WRITE_DURATION  per-phase write duration     (default 1m)
#   READ_DURATION   per-phase read duration      (default 1m)
#   MIXED_DURATION  per-phase mixed duration     (default 1m)
#   WITH_STREAM     1 to create stream-enabled table (default 1)
#   CLIENT_ROUTE_AWARE_READS  0|1                (default 1)
#   PAYLOAD_BYTES   payload size                 (default 256)
#   SERVER_EXTRA_ARGS  extra cefasdb flags       (default empty)
#   RESULT_ROOT     parent dir for run artifacts (default /tmp/cefas-bench)
#   FAIL_ON_PCT     compare regression threshold (default 5.0)
#   TARGETS         compare TARGETS regex        (default .*)
#   KEEP_WORKTREE   1 to leave baseline worktree (default 0)

set -euo pipefail

if [[ $# -lt 1 ]]; then
  sed -n '2,32p' "$0" >&2
  exit 2
fi

BASE_REF="$1"
CAND_REF="${2:-HEAD}"
RUNS="${RUNS:-3}"
WRITE_DURATION="${WRITE_DURATION:-1m}"
READ_DURATION="${READ_DURATION:-1m}"
MIXED_DURATION="${MIXED_DURATION:-1m}"
WITH_STREAM="${WITH_STREAM:-1}"
CLIENT_ROUTE_AWARE_READS="${CLIENT_ROUTE_AWARE_READS:-1}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}"
SERVER_EXTRA_ARGS="${SERVER_EXTRA_ARGS:--pprof-addr :6060 -storage-changelog-mode streams-only}"
RESULT_ROOT="${RESULT_ROOT:-/tmp/cefas-bench}"
FAIL_ON_PCT="${FAIL_ON_PCT:-5.0}"
TARGETS="${TARGETS:-.*}"
KEEP_WORKTREE="${KEEP_WORKTREE:-0}"

REPO_ROOT="$(git rev-parse --show-toplevel)"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
BASE_DIRS_ROOT="${RESULT_ROOT}/ab-${TS}/base"
CAND_DIRS_ROOT="${RESULT_ROOT}/ab-${TS}/cand"
WORKTREE_DIR="/tmp/cefas-bench-wt-${TS}"
WORKTREE_BRANCH="bench-ab-base-${TS}"

mkdir -p "${BASE_DIRS_ROOT}" "${CAND_DIRS_ROOT}"

CAND_WORKTREE=""
CAND_BRANCH=""
cleanup() {
  if [[ "$KEEP_WORKTREE" == "1" ]]; then
    return
  fi
  git -C "$REPO_ROOT" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || true
  git -C "$REPO_ROOT" branch -D "$WORKTREE_BRANCH" 2>/dev/null || true
  if [[ -n "$CAND_WORKTREE" ]]; then
    git -C "$REPO_ROOT" worktree remove --force "$CAND_WORKTREE" 2>/dev/null || true
    git -C "$REPO_ROOT" branch -D "$CAND_BRANCH" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "bench_ab.sh: BASE_REF=$BASE_REF CAND_REF=$CAND_REF RUNS=$RUNS"
echo "bench_ab.sh: artifacts under ${RESULT_ROOT}/ab-${TS}"

git -C "$REPO_ROOT" worktree add -B "$WORKTREE_BRANCH" "$WORKTREE_DIR" "$BASE_REF" >&2

# Resolve CAND_REF to a working directory. HEAD means "use the current
# checkout in REPO_ROOT". Anything else: create a second worktree.
cand_dir="$REPO_ROOT"
if [[ "$CAND_REF" != "HEAD" ]]; then
  CAND_WORKTREE="/tmp/cefas-bench-wt-cand-${TS}"
  CAND_BRANCH="bench-ab-cand-${TS}"
  git -C "$REPO_ROOT" worktree add -B "$CAND_BRANCH" "$CAND_WORKTREE" "$CAND_REF" >&2
  cand_dir="$CAND_WORKTREE"
fi

run_side() {
  local label="$1"
  local source_dir="$2"
  local dest_root="$3"
  for i in $(seq 1 "$RUNS"); do
    local result_dir="${dest_root}/run-${i}"
    echo "=== ${label} run ${i}/${RUNS} → ${result_dir} ==="
    cd "$source_dir"
    RESULT_DIR="$result_dir" \
    NODES=8 SHARDS=24 REPLICATION_FACTOR=3 \
    WRITE_WORKERS=64 READ_WORKERS=512 \
    WRITE_DURATION="$WRITE_DURATION" READ_DURATION="$READ_DURATION" MIXED_DURATION="$MIXED_DURATION" \
    CLIENT_ROUTE_AWARE_READS="$CLIENT_ROUTE_AWARE_READS" WITH_STREAM="$WITH_STREAM" \
    PAYLOAD_BYTES="$PAYLOAD_BYTES" \
    SERVER_EXTRA_ARGS="$SERVER_EXTRA_ARGS" \
    ALLOW_FAILURES=1 \
      bash scripts/bench/bench_8node_matrix.sh
    cd "$REPO_ROOT"
  done
}

run_side "BASELINE" "$WORKTREE_DIR" "$BASE_DIRS_ROOT"
run_side "CANDIDATE" "$cand_dir" "$CAND_DIRS_ROOT"

base_args=()
for d in "${BASE_DIRS_ROOT}"/run-*; do
  base_args+=("$d")
done
cand_args=()
for d in "${CAND_DIRS_ROOT}"/run-*; do
  cand_args+=("$d")
done

echo "=== COMPARE (medians of ${RUNS} runs each side) ==="
FAIL_ON_PCT="$FAIL_ON_PCT" TARGETS="$TARGETS" \
  bash "$REPO_ROOT/scripts/bench/compare_bench.sh" "${base_args[@]}" -- "${cand_args[@]}"
