#!/usr/bin/env bash
# compare_bench.sh — A/B comparator for bench_8node_matrix.sh outputs.
#
# Reads reports/*.json from one or more baseline directories and one or
# more candidate directories, aggregates the median throughput and
# latency percentiles per (run, sub-phase), and prints a markdown delta
# table. Exits 1 when a target run regresses beyond FAIL_ON_PCT.
#
# Usage:
#   compare_bench.sh BASELINE_DIR [BASELINE_DIR ...] -- CANDIDATE_DIR [CANDIDATE_DIR ...]
#   compare_bench.sh BASELINE_DIR CANDIDATE_DIR        # short form for exactly 2 dirs
#
# Environment:
#   FAIL_ON_PCT  regression threshold for non-zero exit (default: 5.0)
#                evaluated against throughput delta (candidate vs baseline);
#                a negative delta below -FAIL_ON_PCT in any target run trips it.
#   TARGETS      egrep filter on "run/phase" deciding which lines gate exit
#                (default: ".*", i.e. every row counts).
#   SHOW_RUNS    when set to 1, prepends a header listing the input dirs.
#
# Dependencies: jq, awk, sort, sed.

set -euo pipefail

usage() {
  sed -n '2,21p' "$0" >&2
  exit 2
}

if [[ $# -lt 2 ]]; then
  usage
fi

FAIL_ON_PCT="${FAIL_ON_PCT:-5.0}"
TARGETS="${TARGETS:-.*}"
SHOW_RUNS="${SHOW_RUNS:-0}"

# Argument parsing: support both "A B" (2 dirs) and "A1 A2 -- B1 B2".
baseline_dirs=()
candidate_dirs=()
if printf '%s\n' "$@" | grep -qx -- "--"; then
  side="baseline"
  for arg in "$@"; do
    if [[ "$arg" == "--" ]]; then
      side="candidate"
      continue
    fi
    if [[ "$side" == "baseline" ]]; then
      baseline_dirs+=("$arg")
    else
      candidate_dirs+=("$arg")
    fi
  done
elif [[ $# -eq 2 ]]; then
  baseline_dirs=("$1")
  candidate_dirs=("$2")
else
  echo "error: with $# args you must use 'A -- B' separator" >&2
  usage
fi

if [[ ${#baseline_dirs[@]} -eq 0 || ${#candidate_dirs[@]} -eq 0 ]]; then
  echo "error: need at least one dir on each side" >&2
  usage
fi

require_dir() {
  local d="$1"
  if [[ ! -d "$d" ]]; then
    echo "error: not a directory: $d" >&2
    exit 2
  fi
  if ! compgen -G "$d/reports/*.json" >/dev/null; then
    echo "error: no reports/*.json under $d" >&2
    exit 2
  fi
}
for d in "${baseline_dirs[@]}" "${candidate_dirs[@]}"; do
  require_dir "$d"
done

# Emit one JSON line per (label, run, sub-phase, throughput, p50, p95, p99).
extract_side() {
  local label="$1"
  shift
  for d in "$@"; do
    for f in "$d"/reports/*.json; do
      [[ -f "$f" ]] || continue
      local base
      base="$(basename "$f")"
      jq -c --arg label "$label" --arg file "$base" '
        ($file | sub("_[0-9TZ]+\\.json$"; "")) as $run |
        .phases[] | {
          label: $label,
          run: $run,
          phase: .name,
          throughput: (.throughput_per_second // 0),
          p50: (.latency_p50_ms // 0),
          p95: (.latency_p95_ms // 0),
          p99: (.latency_p99_ms // 0),
          errors: (.errors // 0)
        }
      ' "$f"
    done
  done
}

agg_json="$(
  {
    extract_side BASELINE "${baseline_dirs[@]}"
    extract_side CANDIDATE "${candidate_dirs[@]}"
  } | jq -s '
    # median helper: lower-mid for even-length lists (stable, no float drift).
    def median(field): map(field) | sort | .[ (length - 1) / 2 | floor ];

    group_by(.label + "|" + .run + "|" + .phase)
    | map({
        label: .[0].label,
        run: .[0].run,
        phase: .[0].phase,
        n: length,
        throughput: median(.throughput),
        p50: median(.p50),
        p95: median(.p95),
        p99: median(.p99),
        errors: (map(.errors) | add)
      })
    | group_by(.run + "/" + .phase)
    | map({
        key: (.[0].run + "/" + .[0].phase),
        baseline: (map(select(.label == "BASELINE")) | .[0] // null),
        candidate: (map(select(.label == "CANDIDATE")) | .[0] // null)
      })
    | sort_by(.key)
  '
)"

# Render markdown table + collect regression rows for exit code.
if [[ "$SHOW_RUNS" == "1" ]]; then
  echo "_baseline_: ${baseline_dirs[*]}"
  echo "_candidate_: ${candidate_dirs[*]}"
  echo
fi

printf '| Run / Phase | Baseline thr/s | Candidate thr/s | Δ%% thr | Baseline p95 | Candidate p95 | Δ%% p95 | Baseline p99 | Candidate p99 | Δ%% p99 | Errors B/C |\n'
printf '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n'

rows_tsv="$(
  jq -r --arg targets "$TARGETS" '
    def pct_delta(cand; base): ((cand - base) / (if base == 0 then 1 else base end)) * 100;
    .[] |
    select(.baseline != null and .candidate != null) |
    . as $r |
    pct_delta($r.candidate.throughput; $r.baseline.throughput) as $dt |
    pct_delta($r.candidate.p95; $r.baseline.p95) as $d95 |
    pct_delta($r.candidate.p99; $r.baseline.p99) as $d99 |
    ($r.key | test($targets)) as $target |
    [
      $r.key,
      ($r.baseline.throughput | tostring),
      ($r.candidate.throughput | tostring),
      ($dt | tostring),
      ($r.baseline.p95 | tostring),
      ($r.candidate.p95 | tostring),
      ($d95 | tostring),
      ($r.baseline.p99 | tostring),
      ($r.candidate.p99 | tostring),
      ($d99 | tostring),
      ($r.baseline.errors | tostring) + "/" + ($r.candidate.errors | tostring),
      ($target | tostring)
    ] | @tsv
  ' <<<"$agg_json"
)"

worst_regression="0"
if [[ -n "$rows_tsv" ]]; then
  while IFS=$'\t' read -r key bthr cthr dthr bp95 cp95 d95 bp99 cp99 d99 errs is_target; do
    [[ -z "$key" ]] && continue
    printf '| %s | %.0f | %.0f | %+.2f%% | %.2f | %.2f | %+.2f%% | %.2f | %.2f | %+.2f%% | %s |\n' \
      "$key" "$bthr" "$cthr" "$dthr" "$bp95" "$cp95" "$d95" "$bp99" "$cp99" "$d99" "$errs"
    if [[ "$is_target" == "true" ]]; then
      if awk -v d="$dthr" -v w="$worst_regression" 'BEGIN{exit !(d < w)}'; then
        worst_regression="$dthr"
      fi
    fi
  done <<<"$rows_tsv"
fi

# Also list rows that exist on only one side (transparency).
only_one_side="$(
  jq -r '
    .[] | select(.baseline == null or .candidate == null) |
    "- only " + (if .baseline == null then "candidate" else "baseline" end) + ": " + .key
  ' <<<"$agg_json"
)"
if [[ -n "$only_one_side" ]]; then
  echo
  echo "_runs present on only one side_:"
  echo "$only_one_side"
fi

# Exit logic.
if awk -v w="$worst_regression" -v t="$FAIL_ON_PCT" 'BEGIN{exit !(w < -t)}'; then
  printf '\n**REGRESSION**: worst targeted throughput delta %.2f%% < -%.2f%% threshold.\n' \
    "$worst_regression" "$FAIL_ON_PCT" >&2
  exit 1
fi
