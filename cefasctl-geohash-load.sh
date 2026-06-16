#!/usr/bin/env bash
set -euo pipefail

# Heavier geohash load test via cefasctl.
#
# It creates a table, inserts many items with a lat_long map, creates a
# geohash plugin index over that field, inserts more items after the index
# exists, then queries geo audience by radius and emits performance metrics.
#
# Auth is delegated to cefasctl-smoke.sh in CEFAS_AUTH_ONLY mode so the same
# tmp/cefasctl-smoke.env and tmp/cefasdb.token are reused.
#
# Large run example:
#   REFRESH_AUTH=0 TOTAL_RECORDS=1000000 POST_INDEX_RECORDS=100000 \
#     BATCH_SIZE=500 LOAD_CONCURRENCY=16 PRINT_SAMPLE=0 ./cefasctl-geohash-load.sh
#
# For large result sets the script automatically applies --limit
# FULL_QUERY_MAX_RESULTS unless QUERY_LIMIT is set or ALLOW_FULL_LARGE_QUERY=1.
# Backfill/incremental validation is done with probe records outside the main
# radius, queried one-by-one with a small radius.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# -------- Connection/auth --------
CEFAS_BIN="${CEFAS_BIN:-${ROOT_DIR}/tmp/cefasctl}"
CEFAS_ENDPOINT="${CEFAS_ENDPOINT:-localhost:19090}"
CEFAS_INSECURE="${CEFAS_INSECURE:-true}"
CEFAS_CA="${CEFAS_CA:-}"
CEFAS_OUTPUT="${CEFAS_OUTPUT:-json}"
CEFAS_TIMEOUT="${CEFAS_TIMEOUT:-300s}"
CEFAS_TOKEN="${CEFAS_TOKEN:-}"
CEFAS_TOKEN_FILE="${CEFAS_TOKEN_FILE:-${ROOT_DIR}/tmp/cefasdb.token}"
REQUIRE_AUTH="${REQUIRE_AUTH:-1}"
REFRESH_AUTH="${REFRESH_AUTH:-1}"

# -------- Load shape --------
TABLE_NAME="${TABLE_NAME:-cefas_geo_load_$(date +%Y%m%d%H%M%S)}"
INDEX_NAME="${INDEX_NAME:-lat_long_geo}"
LOCATION_FIELD="${LOCATION_FIELD:-lat_long}"
STORAGE_CLASS="${STORAGE_CLASS:-disk}"
TOTAL_RECORDS="${TOTAL_RECORDS:-5000}"
NEAR_PERCENT="${NEAR_PERCENT:-40}"
POST_INDEX_RECORDS="${POST_INDEX_RECORDS:-500}"
BATCH_SIZE="${BATCH_SIZE:-500}"
LOAD_CONCURRENCY="${LOAD_CONCURRENCY:-4}"
GEOHASH_PRECISION="${GEOHASH_PRECISION:-5}"
RUN_CLEANUP="${RUN_CLEANUP:-1}"
VERBOSE_BATCHES="${VERBOSE_BATCHES:-0}"
KEEP_BATCH_FILES="${KEEP_BATCH_FILES:-0}"
PROGRESS_EVERY_BATCHES="${PROGRESS_EVERY_BATCHES:-10}"
PRINT_SAMPLE="${PRINT_SAMPLE:-5}"
QUERY_LIMIT="${QUERY_LIMIT:-0}"
VALIDATE_COUNTS="${VALIDATE_COUNTS:-auto}"
FULL_QUERY_MAX_RESULTS="${FULL_QUERY_MAX_RESULTS:-10000}"
ALLOW_FULL_LARGE_QUERY="${ALLOW_FULL_LARGE_QUERY:-0}"
VALIDATION_PROBES="${VALIDATION_PROBES:-1}"
PROBE_COUNT="${PROBE_COUNT:-4}"
PROBE_RADIUS_METERS="${PROBE_RADIUS_METERS:-75}"
PROBE_OFFSET_METERS="${PROBE_OFFSET_METERS:-5000}"
PROBE_QUERY_LIMIT="${PROBE_QUERY_LIMIT:-20}"

# Sao Paulo defaults. Precision 5 gives cells large enough for the default
# radius while still proving the plugin computed buckets from lat_long.
CENTER_LAT="${CENTER_LAT:--23.5505}"
CENTER_LON="${CENTER_LON:--46.6333}"
QUERY_RADIUS_METERS="${QUERY_RADIUS_METERS:-2000}"
NEAR_RADIUS_METERS="${NEAR_RADIUS_METERS:-1200}"
FAR_RADIUS_METERS="${FAR_RADIUS_METERS:-15000}"

WORK_DIR="${WORK_DIR:-${ROOT_DIR}/tmp/geohash-load-${TABLE_NAME}}"
METRICS_FILE="${METRICS_FILE:-${WORK_DIR}/performance-metrics.json}"
QUERY_OUTPUT_FILE="${QUERY_OUTPUT_FILE:-${WORK_DIR}/geo-audience-result.json}"
PROBE_RESULTS_FILE="${PROBE_RESULTS_FILE:-${WORK_DIR}/probe-validation.jsonl}"
created_table=0

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Erro: comando obrigatorio nao encontrado: $1" >&2
    exit 1
  fi
}

ensure_cli_and_auth() {
  if [[ ! -x "$CEFAS_BIN" || ( "$REQUIRE_AUTH" == "1" && "$REFRESH_AUTH" == "1" ) || ( "$REQUIRE_AUTH" == "1" && -z "$CEFAS_TOKEN" && ! -s "$CEFAS_TOKEN_FILE" ) ]]; then
    CEFAS_AUTH_ONLY=1 "$ROOT_DIR/cefasctl-smoke.sh" >/dev/null
  fi
}

common_args() {
  printf '%s\0' --endpoint "$CEFAS_ENDPOINT"
  printf '%s\0' --output "$CEFAS_OUTPUT"
  printf '%s\0' --timeout "$CEFAS_TIMEOUT"
  if [[ "$CEFAS_INSECURE" == "true" ]]; then
    printf '%s\0' --insecure
  elif [[ -n "$CEFAS_CA" ]]; then
    printf '%s\0' --ca "$CEFAS_CA"
  fi
  if [[ -n "$CEFAS_TOKEN" ]]; then
    printf '%s\0' --token "$CEFAS_TOKEN"
  elif [[ -f "$CEFAS_TOKEN_FILE" ]]; then
    printf '%s\0' --token-file "$CEFAS_TOKEN_FILE"
  fi
}

print_cmd() {
  local redact_next=0
  local arg
  printf '+ ' >&2
  printf '%q ' "$CEFAS_BIN" >&2
  for arg in "${COMMON_ARGS[@]}" "$@"; do
    if [[ "$redact_next" == "1" ]]; then
      printf '%q ' "REDACTED" >&2
      redact_next=0
      continue
    fi
    printf '%q ' "$arg" >&2
    if [[ "$arg" == "--token" ]]; then
      redact_next=1
    fi
  done
  printf '\n' >&2
}

run_cefas() {
  print_cmd "$@"
  "$CEFAS_BIN" "${COMMON_ARGS[@]}" "$@"
}

capture_cefas() {
  print_cmd "$@"
  "$CEFAS_BIN" "${COMMON_ARGS[@]}" "$@"
}

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%.0f\n", time() * 1000'
}

rate_per_sec() {
  local records="$1"
  local ms="$2"
  awk -v records="$records" -v ms="$ms" 'BEGIN {
    if (ms <= 0) {
      printf "0"
    } else {
      printf "%.2f", records * 1000 / ms
    }
  }'
}

file_size_bytes() {
  local file="$1"
  stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || wc -c < "$file"
}

bool_enabled() {
  [[ "$1" == "1" || "$1" == "true" || "$1" == "yes" ]]
}

run_batch_write() {
  local file="$1"
  if [[ "$VERBOSE_BATCHES" == "1" || "$VERBOSE_BATCHES" == "true" ]]; then
    print_cmd batch-write-item --table-name "$TABLE_NAME" --request-items "file://$file"
  fi
  "$CEFAS_BIN" "${COMMON_ARGS[@]}" batch-write-item --table-name "$TABLE_NAME" --request-items "file://$file" >/dev/null
}

run_batch_job() {
  local kind="$1"
  local phase="$2"
  local start="$3"
  local count="$4"
  local file="$5"
  local metrics_file="$6"
  local gen_start gen_end write_start write_end
  local gen_ms write_ms batch_ms bytes

  gen_start="$(now_ms)"
  make_batch "$kind" "$phase" "$start" "$count" "$file"
  gen_end="$(now_ms)"
  gen_ms=$((gen_end - gen_start))
  bytes="$(file_size_bytes "$file")"

  write_start="$(now_ms)"
  run_batch_write "$file"
  write_end="$(now_ms)"
  write_ms=$((write_end - write_start))
  batch_ms=$((gen_ms + write_ms))

  if [[ "$KEEP_BATCH_FILES" != "1" && "$KEEP_BATCH_FILES" != "true" ]]; then
    rm -f "$file"
  fi
  printf '%s\t%s\t%s\t%s\t%s\n' "$count" "$gen_ms" "$write_ms" "$batch_ms" "$bytes" > "$metrics_file"
}

cleanup() {
  if [[ "$RUN_CLEANUP" == "1" && "$created_table" == "1" ]]; then
    echo >&2
    echo "# Cleanup: removendo tabela $TABLE_NAME" >&2
    run_cefas delete-table --table-name "$TABLE_NAME" >/dev/null || true
  fi
}

validate_numbers() {
  if (( TOTAL_RECORDS <= 0 || POST_INDEX_RECORDS < 0 || BATCH_SIZE <= 0 || LOAD_CONCURRENCY <= 0 )); then
    echo "Erro: TOTAL_RECORDS, POST_INDEX_RECORDS, BATCH_SIZE ou LOAD_CONCURRENCY invalidos." >&2
    exit 1
  fi
  if (( NEAR_PERCENT < 0 || NEAR_PERCENT > 100 )); then
    echo "Erro: NEAR_PERCENT precisa ficar entre 0 e 100." >&2
    exit 1
  fi
  if (( GEOHASH_PRECISION < 1 || GEOHASH_PRECISION > 12 )); then
    echo "Erro: GEOHASH_PRECISION precisa ficar entre 1 e 12." >&2
    exit 1
  fi
  if (( PROGRESS_EVERY_BATCHES < 1 || PRINT_SAMPLE < 0 || QUERY_LIMIT < 0 || FULL_QUERY_MAX_RESULTS < 0 )); then
    echo "Erro: PROGRESS_EVERY_BATCHES, PRINT_SAMPLE, QUERY_LIMIT ou FULL_QUERY_MAX_RESULTS invalidos." >&2
    exit 1
  fi
  if (( PROBE_COUNT < 0 || PROBE_RADIUS_METERS <= 0 || PROBE_OFFSET_METERS <= QUERY_RADIUS_METERS || PROBE_QUERY_LIMIT <= 0 )); then
    echo "Erro: PROBE_COUNT, PROBE_RADIUS_METERS, PROBE_OFFSET_METERS ou PROBE_QUERY_LIMIT invalidos." >&2
    echo "      PROBE_OFFSET_METERS precisa ser maior que QUERY_RADIUS_METERS para nao alterar a contagem principal." >&2
    exit 1
  fi
}

make_batch() {
  local kind="$1"
  local phase="$2"
  local start="$3"
  local count="$4"
  local file="$5"
  LC_ALL=C awk \
    -v kind="$kind" \
    -v phase="$phase" \
    -v start="$start" \
    -v count="$count" \
    -v center_lat="$CENTER_LAT" \
    -v center_lon="$CENTER_LON" \
    -v near_radius="$NEAR_RADIUS_METERS" \
    -v far_radius="$FAR_RADIUS_METERS" '
    BEGIN {
      pi = atan2(0, -1)
      coslat = cos(center_lat * pi / 180.0)
      if (coslat < 0) coslat = -coslat
      if (coslat < 0.01) coslat = 0.01
      sep = ""
      print "["
      for (j = 0; j < count; j++) {
        idx = start + j
        angle = idx * 137.50776405 * pi / 180.0
        if (kind == "near") {
          radius_m = (0.15 + ((idx * 37) % 850) / 1000.0) * near_radius
        } else {
          radius_m = far_radius + ((idx * 53) % 5000)
        }
        lat = center_lat + (radius_m * cos(angle)) / 111320.0
        lon = center_lon + (radius_m * sin(angle)) / (111320.0 * coslat)
        id = sprintf("STORE#%s#%s#%06d", phase, kind, idx)
        printf "%s{\"PutRequest\":{\"Item\":{", sep
        printf "\"pk\":{\"S\":\"%s\"}", id
        printf ",\"phase\":{\"S\":\"%s\"}", phase
        printf ",\"kind\":{\"S\":\"%s\"}", kind
        printf ",\"name\":{\"S\":\"Geo Store %s %s %06d\"}", phase, kind, idx
        printf ",\"distance_m\":{\"N\":\"%.2f\"}", radius_m
        printf ",\"lat_long\":{\"M\":{\"lat\":{\"N\":\"%.6f\"},\"lon\":{\"N\":\"%.6f\"}}}", lat, lon
        printf "}}}"
        sep = ",\n"
      }
      print "\n]"
    }' > "$file"
}

probe_meta() {
  local phase="$1"
  local idx="$2"
  LC_ALL=C awk \
    -v phase="$phase" \
    -v idx="$idx" \
    -v center_lat="$CENTER_LAT" \
    -v center_lon="$CENTER_LON" \
    -v query_radius="$QUERY_RADIUS_METERS" \
    -v probe_offset="$PROBE_OFFSET_METERS" \
    -v probe_radius="$PROBE_RADIUS_METERS" '
    BEGIN {
      pi = atan2(0, -1)
      coslat = cos(center_lat * pi / 180.0)
      if (coslat < 0) coslat = -coslat
      if (coslat < 0.01) coslat = 0.01
      angle_deg = (idx - 1) * 90
      if (phase == "postprobe") {
        angle_deg += 45
      }
      radius_m = probe_offset + ((idx - 1) * probe_radius * 4)
      if (radius_m <= query_radius + probe_radius) {
        radius_m = query_radius + (idx * probe_radius * 4)
      }
      angle = angle_deg * pi / 180.0
      lat = center_lat + (radius_m * cos(angle)) / 111320.0
      lon = center_lon + (radius_m * sin(angle)) / (111320.0 * coslat)
      pk = sprintf("STORE#%s#probe#%03d", phase, idx)
      printf "%s %.6f %.6f %.2f\n", pk, lat, lon, radius_m
    }'
}

make_probe_batch() {
  local phase="$1"
  local count="$2"
  local file="$3"
  LC_ALL=C awk \
    -v phase="$phase" \
    -v count="$count" \
    -v center_lat="$CENTER_LAT" \
    -v center_lon="$CENTER_LON" \
    -v query_radius="$QUERY_RADIUS_METERS" \
    -v probe_offset="$PROBE_OFFSET_METERS" \
    -v probe_radius="$PROBE_RADIUS_METERS" '
    BEGIN {
      pi = atan2(0, -1)
      coslat = cos(center_lat * pi / 180.0)
      if (coslat < 0) coslat = -coslat
      if (coslat < 0.01) coslat = 0.01
      sep = ""
      print "["
      for (idx = 1; idx <= count; idx++) {
        angle_deg = (idx - 1) * 90
        if (phase == "postprobe") {
          angle_deg += 45
        }
        radius_m = probe_offset + ((idx - 1) * probe_radius * 4)
        if (radius_m <= query_radius + probe_radius) {
          radius_m = query_radius + (idx * probe_radius * 4)
        }
        angle = angle_deg * pi / 180.0
        lat = center_lat + (radius_m * cos(angle)) / 111320.0
        lon = center_lon + (radius_m * sin(angle)) / (111320.0 * coslat)
        id = sprintf("STORE#%s#probe#%03d", phase, idx)
        printf "%s{\"PutRequest\":{\"Item\":{", sep
        printf "\"pk\":{\"S\":\"%s\"}", id
        printf ",\"phase\":{\"S\":\"%s\"}", phase
        printf ",\"kind\":{\"S\":\"probe\"}"
        printf ",\"name\":{\"S\":\"Geo validation probe %s %03d\"}", phase, idx
        printf ",\"distance_m\":{\"N\":\"%.2f\"}", radius_m
        printf ",\"validation_probe\":{\"BOOL\":true}"
        printf ",\"lat_long\":{\"M\":{\"lat\":{\"N\":\"%.6f\"},\"lon\":{\"N\":\"%.6f\"}}}", lat, lon
        printf "}}}"
        sep = ",\n"
      }
      print "\n]"
    }' > "$file"
}

insert_probe_records() {
  local phase="$1"
  local file="$WORK_DIR/${phase}-validation-probes.json"
  local start_ms write_ms bytes
  if (( PROBE_COUNT == 0 )) || ! bool_enabled "$VALIDATION_PROBES"; then
    LAST_PROBE_WRITE_MS=0
    LAST_PROBE_BYTES=0
    return
  fi
  make_probe_batch "$phase" "$PROBE_COUNT" "$file"
  bytes="$(file_size_bytes "$file")"
  start_ms="$(now_ms)"
  run_batch_write "$file"
  write_ms=$(( $(now_ms) - start_ms ))
  if [[ "$KEEP_BATCH_FILES" != "1" && "$KEEP_BATCH_FILES" != "true" ]]; then
    rm -f "$file"
  fi
  LAST_PROBE_WRITE_MS="$write_ms"
  LAST_PROBE_BYTES="$bytes"
  echo "# Probes inseridos ${phase}: records=$PROBE_COUNT write_ms=$write_ms bytes=$bytes" >&2
}

run_probe_validation() {
  local phase idx pk lat lon radius_m out start_ms query_ms count hit failures=0 checked=0 total_query_ms=0 total_items=0
  : > "$PROBE_RESULTS_FILE"
  if (( PROBE_COUNT == 0 )) || ! bool_enabled "$VALIDATION_PROBES"; then
    PROBE_CHECKED=0
    PROBE_FAILURES=0
    PROBE_QUERY_MS=0
    PROBE_ITEMS_RETURNED=0
    PROBE_VALIDATION_ENABLED=false
    return
  fi
  PROBE_VALIDATION_ENABLED=true
  for phase in preprobe postprobe; do
    for (( idx = 1; idx <= PROBE_COUNT; idx++ )); do
      read -r pk lat lon radius_m < <(probe_meta "$phase" "$idx")
      out="${WORK_DIR}/probe-${phase}-${idx}.json"
      start_ms="$(now_ms)"
      capture_cefas geo audience \
        --table "$TABLE_NAME" \
        --index "$INDEX_NAME" \
        --center "${lat},${lon}" \
        --radius "${PROBE_RADIUS_METERS}m" \
        --limit "$PROBE_QUERY_LIMIT" > "$out"
      query_ms=$(( $(now_ms) - start_ms ))
      count="$(jq -r '.Count // (.Items | length)' "$out")"
      hit="$(jq -r --arg pk "$pk" '[.Items[] | select(.pk.S == $pk)] | length' "$out")"
      checked=$((checked + 1))
      total_query_ms=$((total_query_ms + query_ms))
      total_items=$((total_items + count))
      if [[ "$hit" != "1" ]]; then
        failures=$((failures + 1))
      fi
      jq -cn \
        --arg phase "$phase" \
        --arg pk "$pk" \
        --arg lat "$lat" \
        --arg lon "$lon" \
        --argjson expectedDistanceMeters "$radius_m" \
        --argjson queryRadiusMeters "$PROBE_RADIUS_METERS" \
        --argjson queryLimit "$PROBE_QUERY_LIMIT" \
        --argjson queryMs "$query_ms" \
        --argjson returnedCount "$count" \
        --argjson hitCount "$hit" \
        '{phase:$phase,pk:$pk,lat:($lat|tonumber),lon:($lon|tonumber),expectedDistanceMeters:$expectedDistanceMeters,queryRadiusMeters:$queryRadiusMeters,queryLimit:$queryLimit,queryMs:$queryMs,returnedCount:$returnedCount,hitCount:$hitCount,passed:($hitCount == 1)}' >> "$PROBE_RESULTS_FILE"
      echo "# Probe ${phase}/${idx}: hit=$hit returned=$count query_ms=$query_ms pk=$pk" >&2
      if [[ "$KEEP_BATCH_FILES" != "1" && "$KEEP_BATCH_FILES" != "true" ]]; then
        rm -f "$out"
      fi
    done
  done
  PROBE_CHECKED="$checked"
  PROBE_FAILURES="$failures"
  PROBE_QUERY_MS="$total_query_ms"
  PROBE_ITEMS_RETURNED="$total_items"
  if (( failures > 0 )); then
    echo "Erro: validacao de probes falhou em $failures de $checked consultas. Detalhes: $PROBE_RESULTS_FILE" >&2
    exit 1
  fi
}

load_records() {
  local kind="$1"
  local phase="$2"
  local total="$3"
  local inserted=0 completed=0 scheduled=0
  local batch_count batch_file metrics_file metrics_dir batches=0
  local load_start load_end job_failed=0
  local records=0 gen_total_ms=0 write_total_ms=0 batch_total_ms=0
  local batch_min_ms=0 batch_max_ms=0 generated_bytes=0
  local pids=()
  local pid
  metrics_dir="${WORK_DIR}/batch-metrics-${phase}-${kind}-$$-$(now_ms)"
  mkdir -p "$metrics_dir"

  wait_oldest_batch() {
    local oldest="${pids[0]}"
    if ! wait "$oldest"; then
      job_failed=1
    fi
    pids=("${pids[@]:1}")
    completed=$((completed + 1))
    if (( completed == 1 || completed == batches || completed % PROGRESS_EVERY_BATCHES == 0 )); then
      echo "# Concluidos ${phase}/${kind}: batches=${completed}/${batches} agendados=${scheduled}/${total} rps=$(rate_per_sec "$scheduled" "$(( $(now_ms) - load_start ))")" >&2
    fi
  }

  load_start="$(now_ms)"
  while (( inserted < total )); do
    batches=$((batches + 1))
    batch_count="$BATCH_SIZE"
    if (( inserted + batch_count > total )); then
      batch_count=$((total - inserted))
    fi
    batch_file="${WORK_DIR}/${phase}-${kind}-$((inserted + 1))-$((inserted + batch_count)).json"
    metrics_file="${metrics_dir}/batch-${batches}.tsv"
    run_batch_job "$kind" "$phase" $((inserted + 1)) "$batch_count" "$batch_file" "$metrics_file" &
    pids+=("$!")
    inserted=$((inserted + batch_count))
    scheduled="$inserted"
    if (( batches == 1 || inserted == total || batches % PROGRESS_EVERY_BATCHES == 0 )); then
      echo "# Agendados ${phase}/${kind}: ${scheduled}/${total} batches=$batches concurrency=$LOAD_CONCURRENCY rps=$(rate_per_sec "$scheduled" "$(( $(now_ms) - load_start ))")" >&2
    fi
    if (( ${#pids[@]} >= LOAD_CONCURRENCY )); then
      wait_oldest_batch
    fi
  done
  while (( ${#pids[@]} > 0 )); do
    wait_oldest_batch
  done
  if (( job_failed != 0 )); then
    echo "Erro: pelo menos um batch ${phase}/${kind} falhou." >&2
    exit 1
  fi
  load_end="$(now_ms)"
  read -r records batches gen_total_ms write_total_ms batch_total_ms batch_min_ms batch_max_ms generated_bytes < <(
    awk -F '\t' '
      {
        records += $1
        gen += $2
        write += $3
        batch += $4
        bytes += $5
        n++
        if (min == 0 || $4 < min) min = $4
        if ($4 > max) max = $4
      }
      END {
        printf "%d %d %d %d %d %d %d %d\n", records, n, gen, write, batch, min, max, bytes
      }
    ' "$metrics_dir"/*.tsv
  )
  rm -f "$metrics_dir"/*.tsv 2>/dev/null || true
  rmdir "$metrics_dir" 2>/dev/null || true

  LAST_LOAD_RECORDS="$records"
  LAST_LOAD_BATCHES="$batches"
  LAST_LOAD_MS=$((load_end - load_start))
  LAST_LOAD_GEN_MS="$gen_total_ms"
  LAST_LOAD_WRITE_MS="$write_total_ms"
  LAST_LOAD_BATCH_MIN_MS="$batch_min_ms"
  LAST_LOAD_BATCH_MAX_MS="$batch_max_ms"
  if (( batches > 0 )); then
    LAST_LOAD_BATCH_AVG_MS="$(awk -v total="$batch_total_ms" -v batches="$batches" 'BEGIN { printf "%.2f", total / batches }')"
  else
    LAST_LOAD_BATCH_AVG_MS="0"
  fi
  LAST_LOAD_RPS="$(rate_per_sec "$total" "$LAST_LOAD_MS")"
  LAST_LOAD_BYTES="$generated_bytes"
  echo "# Performance ${phase}/${kind}: records=$LAST_LOAD_RECORDS batches=$LAST_LOAD_BATCHES concurrency=$LOAD_CONCURRENCY wall_ms=$LAST_LOAD_MS records_per_sec=$LAST_LOAD_RPS gen_ms=$LAST_LOAD_GEN_MS write_ms=$LAST_LOAD_WRITE_MS bytes=$LAST_LOAD_BYTES batch_avg_ms=$LAST_LOAD_BATCH_AVG_MS batch_min_ms=$LAST_LOAD_BATCH_MIN_MS batch_max_ms=$LAST_LOAD_BATCH_MAX_MS" >&2
}

need_cmd awk
need_cmd jq
need_cmd perl
validate_numbers
ensure_cli_and_auth

COMMON_ARGS=()
while IFS= read -r -d '' arg; do
  COMMON_ARGS+=("$arg")
done < <(common_args)

mkdir -p "$WORK_DIR"
trap cleanup EXIT
total_start_ms="$(now_ms)"

pre_near=$((TOTAL_RECORDS * NEAR_PERCENT / 100))
pre_far=$((TOTAL_RECORDS - pre_near))
expected_count=$((pre_near + POST_INDEX_RECORDS))
probe_records=0
if (( PROBE_COUNT > 0 )) && bool_enabled "$VALIDATION_PROBES"; then
  probe_records=$((PROBE_COUNT * 2))
fi
pre_index_probe_records=0
if (( PROBE_COUNT > 0 )) && bool_enabled "$VALIDATION_PROBES"; then
  pre_index_probe_records="$PROBE_COUNT"
fi
pre_index_records=$((TOTAL_RECORDS + pre_index_probe_records))
effective_query_limit="$QUERY_LIMIT"
auto_query_limited=false
if (( QUERY_LIMIT == 0 && FULL_QUERY_MAX_RESULTS > 0 && expected_count > FULL_QUERY_MAX_RESULTS )) && [[ "$ALLOW_FULL_LARGE_QUERY" != "1" && "$ALLOW_FULL_LARGE_QUERY" != "true" ]]; then
  effective_query_limit="$FULL_QUERY_MAX_RESULTS"
  auto_query_limited=true
fi
load_pre_near_ms=0
load_pre_near_batches=0
load_pre_near_gen_ms=0
load_pre_near_write_ms=0
load_pre_near_rps=0
load_pre_near_bytes=0
load_pre_near_batch_avg_ms=0
load_pre_near_batch_min_ms=0
load_pre_near_batch_max_ms=0
load_pre_far_ms=0
load_pre_far_batches=0
load_pre_far_gen_ms=0
load_pre_far_write_ms=0
load_pre_far_rps=0
load_pre_far_bytes=0
load_pre_far_batch_avg_ms=0
load_pre_far_batch_min_ms=0
load_pre_far_batch_max_ms=0
load_post_near_ms=0
load_post_near_batches=0
load_post_near_gen_ms=0
load_post_near_write_ms=0
load_post_near_rps=0
load_post_near_bytes=0
load_post_near_batch_avg_ms=0
load_post_near_batch_min_ms=0
load_post_near_batch_max_ms=0
probe_pre_write_ms=0
probe_post_write_ms=0
probe_pre_bytes=0
probe_post_bytes=0
PROBE_VALIDATION_ENABLED=false
PROBE_CHECKED=0
PROBE_FAILURES=0
PROBE_QUERY_MS=0
PROBE_ITEMS_RETURNED=0

echo "# CefasDB geohash load test"
echo "# endpoint=$CEFAS_ENDPOINT table=$TABLE_NAME index=$INDEX_NAME cleanup=$RUN_CLEANUP"
echo "# total_pre=$TOTAL_RECORDS near_pre=$pre_near far_pre=$pre_far post_near=$POST_INDEX_RECORDS expected_geo_count=$expected_count"
echo "# center=${CENTER_LAT},${CENTER_LON} query_radius_m=$QUERY_RADIUS_METERS near_radius_m=$NEAR_RADIUS_METERS geohash_precision=$GEOHASH_PRECISION"
echo "# batch_size=$BATCH_SIZE load_concurrency=$LOAD_CONCURRENCY query_limit_requested=$QUERY_LIMIT query_limit_effective=$effective_query_limit auto_query_limited=$auto_query_limited metrics_file=$METRICS_FILE"
echo "# validation_probes=$VALIDATION_PROBES probe_count=$PROBE_COUNT probe_radius_m=$PROBE_RADIUS_METERS probe_offset_m=$PROBE_OFFSET_METERS probe_results_file=$PROBE_RESULTS_FILE"

create_table_start_ms="$(now_ms)"
run_cefas create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions "AttributeName=pk,AttributeType=S" \
  --key-schema "AttributeName=pk,KeyType=HASH" \
  --billing-mode PAY_PER_REQUEST \
  --storage-class "$STORAGE_CLASS"
create_table_ms=$(( $(now_ms) - create_table_start_ms ))
created_table=1

if (( pre_near > 0 )); then
  load_records near pre "$pre_near"
  load_pre_near_ms="$LAST_LOAD_MS"
  load_pre_near_batches="$LAST_LOAD_BATCHES"
  load_pre_near_gen_ms="$LAST_LOAD_GEN_MS"
  load_pre_near_write_ms="$LAST_LOAD_WRITE_MS"
  load_pre_near_rps="$LAST_LOAD_RPS"
  load_pre_near_bytes="$LAST_LOAD_BYTES"
  load_pre_near_batch_avg_ms="$LAST_LOAD_BATCH_AVG_MS"
  load_pre_near_batch_min_ms="$LAST_LOAD_BATCH_MIN_MS"
  load_pre_near_batch_max_ms="$LAST_LOAD_BATCH_MAX_MS"
fi
if (( pre_far > 0 )); then
  load_records far pre "$pre_far"
  load_pre_far_ms="$LAST_LOAD_MS"
  load_pre_far_batches="$LAST_LOAD_BATCHES"
  load_pre_far_gen_ms="$LAST_LOAD_GEN_MS"
  load_pre_far_write_ms="$LAST_LOAD_WRITE_MS"
  load_pre_far_rps="$LAST_LOAD_RPS"
  load_pre_far_bytes="$LAST_LOAD_BYTES"
  load_pre_far_batch_avg_ms="$LAST_LOAD_BATCH_AVG_MS"
  load_pre_far_batch_min_ms="$LAST_LOAD_BATCH_MIN_MS"
  load_pre_far_batch_max_ms="$LAST_LOAD_BATCH_MAX_MS"
fi

insert_probe_records preprobe
probe_pre_write_ms="$LAST_PROBE_WRITE_MS"
probe_pre_bytes="$LAST_PROBE_BYTES"

index_config="$(jq -cn --arg field "$LOCATION_FIELD" --argjson precision "$GEOHASH_PRECISION" '{field:$field,precision:$precision}')"
create_index_start_ms="$(now_ms)"
run_cefas create-index \
  --table "$TABLE_NAME" \
  --name "$INDEX_NAME" \
  --type geohash \
  --config "$index_config" \
  --pk pk
create_index_ms=$(( $(now_ms) - create_index_start_ms ))
create_index_rps="$(rate_per_sec "$pre_index_records" "$create_index_ms")"

run_cefas describe-index --table "$TABLE_NAME" --name "$INDEX_NAME"

if (( POST_INDEX_RECORDS > 0 )); then
  load_records near post "$POST_INDEX_RECORDS"
  load_post_near_ms="$LAST_LOAD_MS"
  load_post_near_batches="$LAST_LOAD_BATCHES"
  load_post_near_gen_ms="$LAST_LOAD_GEN_MS"
  load_post_near_write_ms="$LAST_LOAD_WRITE_MS"
  load_post_near_rps="$LAST_LOAD_RPS"
  load_post_near_bytes="$LAST_LOAD_BYTES"
  load_post_near_batch_avg_ms="$LAST_LOAD_BATCH_AVG_MS"
  load_post_near_batch_min_ms="$LAST_LOAD_BATCH_MIN_MS"
  load_post_near_batch_max_ms="$LAST_LOAD_BATCH_MAX_MS"
fi

insert_probe_records postprobe
probe_post_write_ms="$LAST_PROBE_WRITE_MS"
probe_post_bytes="$LAST_PROBE_BYTES"

geo_args=(geo audience
  --table "$TABLE_NAME"
  --index "$INDEX_NAME"
  --center "${CENTER_LAT},${CENTER_LON}"
  --radius "${QUERY_RADIUS_METERS}m")
if (( effective_query_limit > 0 )); then
  geo_args+=(--limit "$effective_query_limit")
fi

geo_query_start_ms="$(now_ms)"
capture_cefas "${geo_args[@]}" > "$QUERY_OUTPUT_FILE"
geo_query_ms=$(( $(now_ms) - geo_query_start_ms ))
geo_query_bytes="$(file_size_bytes "$QUERY_OUTPUT_FILE")"

actual_count="$(jq -r '.Count // (.Items | length)' "$QUERY_OUTPUT_FILE")"
far_count="$(jq -r '[.Items[] | select(.kind.S == "far")] | length' "$QUERY_OUTPUT_FILE")"
post_count="$(jq -r '[.Items[] | select(.phase.S == "post")] | length' "$QUERY_OUTPUT_FILE")"
geo_query_rps="$(rate_per_sec "$actual_count" "$geo_query_ms")"
query_limited=false
if (( effective_query_limit > 0 )); then
  query_limited=true
fi
exact_validation=true
if (( effective_query_limit > 0 && expected_count > effective_query_limit )); then
  exact_validation=false
fi
case "$VALIDATE_COUNTS" in
  0|false|no)
    exact_validation=false
    ;;
  1|true|yes)
    if (( effective_query_limit > 0 && expected_count > effective_query_limit )); then
      echo "Erro: VALIDATE_COUNTS=1 exige query sem limite efetivo ou limite >= expected_geo_count. Ajuste QUERY_LIMIT ou ALLOW_FULL_LARGE_QUERY=1." >&2
      exit 1
    fi
    exact_validation=true
    ;;
esac

echo "# GeoAudience count=$actual_count expected=$expected_count far_leaks_returned=$far_count post_hits_returned=$post_count query_ms=$geo_query_ms records_per_sec=$geo_query_rps bytes=$geo_query_bytes exact_validation=$exact_validation query_limit_effective=$effective_query_limit"
if (( PRINT_SAMPLE > 0 )); then
  jq --argjson sample "$PRINT_SAMPLE" '{Count, Sample: (.Items[:$sample] // [])}' "$QUERY_OUTPUT_FILE"
fi

if [[ "$exact_validation" == "true" && "$actual_count" != "$expected_count" ]]; then
  echo "Erro: geo audience retornou $actual_count itens; esperado $expected_count." >&2
  exit 1
fi
if [[ "$far_count" != "0" ]]; then
  echo "Erro: geo audience retornou $far_count registros far fora do raio na resposta." >&2
  exit 1
fi
if [[ "$exact_validation" == "true" && "$post_count" != "$POST_INDEX_RECORDS" ]]; then
  echo "Erro: manutencao incremental do indice retornou $post_count registros post; esperado $POST_INDEX_RECORDS." >&2
  exit 1
fi

run_probe_validation

total_end_ms="$(now_ms)"
total_ms=$((total_end_ms - total_start_ms))
inserted_records=$((TOTAL_RECORDS + POST_INDEX_RECORDS + probe_records))
probe_write_ms=$((probe_pre_write_ms + probe_post_write_ms))
ingest_ms=$((load_pre_near_ms + load_pre_far_ms + load_post_near_ms + probe_write_ms))
ingest_rps="$(rate_per_sec "$inserted_records" "$ingest_ms")"
total_rps="$(rate_per_sec "$inserted_records" "$total_ms")"
total_generated_bytes=$((load_pre_near_bytes + load_pre_far_bytes + load_post_near_bytes + probe_pre_bytes + probe_post_bytes))

metrics_json="$(jq -n \
  --arg table "$TABLE_NAME" \
  --arg endpoint "$CEFAS_ENDPOINT" \
  --arg index "$INDEX_NAME" \
  --arg locationField "$LOCATION_FIELD" \
  --arg metricsFile "$METRICS_FILE" \
  --arg queryOutputFile "$QUERY_OUTPUT_FILE" \
  --arg probeResultsFile "$PROBE_RESULTS_FILE" \
  --argjson totalPreRecords "$TOTAL_RECORDS" \
  --argjson postIndexRecords "$POST_INDEX_RECORDS" \
  --argjson probeRecords "$probe_records" \
  --argjson preIndexRecords "$pre_index_records" \
  --argjson insertedRecords "$inserted_records" \
  --argjson nearPercent "$NEAR_PERCENT" \
  --argjson batchSize "$BATCH_SIZE" \
  --argjson loadConcurrency "$LOAD_CONCURRENCY" \
  --argjson geohashPrecision "$GEOHASH_PRECISION" \
  --argjson expectedGeoCount "$expected_count" \
  --argjson actualGeoCount "$actual_count" \
  --argjson farLeaksReturned "$far_count" \
  --argjson postHitsReturned "$post_count" \
  --argjson queryLimitRequested "$QUERY_LIMIT" \
  --argjson queryLimitEffective "$effective_query_limit" \
  --argjson queryLimited "$query_limited" \
  --argjson autoQueryLimited "$auto_query_limited" \
  --argjson fullQueryMaxResults "$FULL_QUERY_MAX_RESULTS" \
  --argjson exactValidation "$exact_validation" \
  --argjson validationProbesEnabled "$PROBE_VALIDATION_ENABLED" \
  --argjson probeCountConfigured "$PROBE_COUNT" \
  --argjson probeRadiusMeters "$PROBE_RADIUS_METERS" \
  --argjson probeOffsetMeters "$PROBE_OFFSET_METERS" \
  --argjson probeQueryLimit "$PROBE_QUERY_LIMIT" \
  --argjson probeChecked "$PROBE_CHECKED" \
  --argjson probeFailures "$PROBE_FAILURES" \
  --argjson probeItemsReturned "$PROBE_ITEMS_RETURNED" \
  --argjson createTableMs "$create_table_ms" \
  --argjson createIndexMs "$create_index_ms" \
  --argjson createIndexRecordsPerSec "$create_index_rps" \
  --argjson geoQueryMs "$geo_query_ms" \
  --argjson geoQueryRecordsPerSec "$geo_query_rps" \
  --argjson geoQueryBytes "$geo_query_bytes" \
  --argjson ingestMs "$ingest_ms" \
  --argjson ingestRecordsPerSec "$ingest_rps" \
  --argjson probeWriteMs "$probe_write_ms" \
  --argjson probeQueryMs "$PROBE_QUERY_MS" \
  --argjson totalMs "$total_ms" \
  --argjson totalRecordsPerSec "$total_rps" \
  --argjson generatedBytes "$total_generated_bytes" \
  --argjson preNearRecords "$pre_near" \
  --argjson preNearBatches "$load_pre_near_batches" \
  --argjson preNearMs "$load_pre_near_ms" \
  --argjson preNearGenMs "$load_pre_near_gen_ms" \
  --argjson preNearWriteMs "$load_pre_near_write_ms" \
  --argjson preNearRps "$load_pre_near_rps" \
  --argjson preNearBytes "$load_pre_near_bytes" \
  --argjson preNearBatchAvgMs "$load_pre_near_batch_avg_ms" \
  --argjson preNearBatchMinMs "$load_pre_near_batch_min_ms" \
  --argjson preNearBatchMaxMs "$load_pre_near_batch_max_ms" \
  --argjson preFarRecords "$pre_far" \
  --argjson preFarBatches "$load_pre_far_batches" \
  --argjson preFarMs "$load_pre_far_ms" \
  --argjson preFarGenMs "$load_pre_far_gen_ms" \
  --argjson preFarWriteMs "$load_pre_far_write_ms" \
  --argjson preFarRps "$load_pre_far_rps" \
  --argjson preFarBytes "$load_pre_far_bytes" \
  --argjson preFarBatchAvgMs "$load_pre_far_batch_avg_ms" \
  --argjson preFarBatchMinMs "$load_pre_far_batch_min_ms" \
  --argjson preFarBatchMaxMs "$load_pre_far_batch_max_ms" \
  --argjson postNearRecords "$POST_INDEX_RECORDS" \
  --argjson postNearBatches "$load_post_near_batches" \
  --argjson postNearMs "$load_post_near_ms" \
  --argjson postNearGenMs "$load_post_near_gen_ms" \
  --argjson postNearWriteMs "$load_post_near_write_ms" \
  --argjson postNearRps "$load_post_near_rps" \
  --argjson postNearBytes "$load_post_near_bytes" \
  --argjson postNearBatchAvgMs "$load_post_near_batch_avg_ms" \
  --argjson postNearBatchMinMs "$load_post_near_batch_min_ms" \
  --argjson postNearBatchMaxMs "$load_post_near_batch_max_ms" \
  --argjson probePreWriteMs "$probe_pre_write_ms" \
  --argjson probePostWriteMs "$probe_post_write_ms" \
  --argjson probePreBytes "$probe_pre_bytes" \
  --argjson probePostBytes "$probe_post_bytes" \
  '{
    table: $table,
    endpoint: $endpoint,
    index: $index,
    locationField: $locationField,
    metricsFile: $metricsFile,
    queryOutputFile: $queryOutputFile,
    probeResultsFile: $probeResultsFile,
    config: {
      totalPreRecords: $totalPreRecords,
      postIndexRecords: $postIndexRecords,
      probeRecords: $probeRecords,
      preIndexRecords: $preIndexRecords,
      insertedRecords: $insertedRecords,
      nearPercent: $nearPercent,
      batchSize: $batchSize,
      loadConcurrency: $loadConcurrency,
      geohashPrecision: $geohashPrecision,
      queryLimitRequested: $queryLimitRequested,
      queryLimitEffective: $queryLimitEffective,
      queryLimited: $queryLimited,
      autoQueryLimited: $autoQueryLimited,
      fullQueryMaxResults: $fullQueryMaxResults
    },
    validation: {
      expectedGeoCount: $expectedGeoCount,
      actualGeoCount: $actualGeoCount,
      exactValidation: $exactValidation,
      farLeaksReturned: $farLeaksReturned,
      postHitsReturned: $postHitsReturned,
      probes: {
        enabled: $validationProbesEnabled,
        configuredPerPhase: $probeCountConfigured,
        checked: $probeChecked,
        failures: $probeFailures,
        itemsReturned: $probeItemsReturned,
        queryRadiusMeters: $probeRadiusMeters,
        offsetMeters: $probeOffsetMeters,
        queryLimit: $probeQueryLimit
      }
    },
    performance: {
      createTableMs: $createTableMs,
      createIndexMs: $createIndexMs,
      createIndexRecordsPerSec: $createIndexRecordsPerSec,
      geoQueryMs: $geoQueryMs,
      geoQueryRecordsPerSec: $geoQueryRecordsPerSec,
      geoQueryBytes: $geoQueryBytes,
      ingestMs: $ingestMs,
      ingestRecordsPerSec: $ingestRecordsPerSec,
      probeWriteMs: $probeWriteMs,
      probeQueryMs: $probeQueryMs,
      totalMs: $totalMs,
      totalRecordsPerSec: $totalRecordsPerSec,
      generatedBytes: $generatedBytes
    },
    phases: {
      preNear: {
        records: $preNearRecords,
        batches: $preNearBatches,
        totalMs: $preNearMs,
        generateMs: $preNearGenMs,
        writeMs: $preNearWriteMs,
        recordsPerSec: $preNearRps,
        generatedBytes: $preNearBytes,
        batchAvgMs: $preNearBatchAvgMs,
        batchMinMs: $preNearBatchMinMs,
        batchMaxMs: $preNearBatchMaxMs
      },
      preFar: {
        records: $preFarRecords,
        batches: $preFarBatches,
        totalMs: $preFarMs,
        generateMs: $preFarGenMs,
        writeMs: $preFarWriteMs,
        recordsPerSec: $preFarRps,
        generatedBytes: $preFarBytes,
        batchAvgMs: $preFarBatchAvgMs,
        batchMinMs: $preFarBatchMinMs,
        batchMaxMs: $preFarBatchMaxMs
      },
      postNear: {
        records: $postNearRecords,
        batches: $postNearBatches,
        totalMs: $postNearMs,
        generateMs: $postNearGenMs,
        writeMs: $postNearWriteMs,
        recordsPerSec: $postNearRps,
        generatedBytes: $postNearBytes,
        batchAvgMs: $postNearBatchAvgMs,
        batchMinMs: $postNearBatchMinMs,
        batchMaxMs: $postNearBatchMaxMs
      },
      probes: {
        preWriteMs: $probePreWriteMs,
        postWriteMs: $probePostWriteMs,
        preGeneratedBytes: $probePreBytes,
        postGeneratedBytes: $probePostBytes
      }
    }
  }')"

printf '%s\n' "$metrics_json" > "$METRICS_FILE"
echo "# Performance metrics:"
printf '%s\n' "$metrics_json"
echo "# Geohash load test concluido."
