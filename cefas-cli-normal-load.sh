#!/usr/bin/env bash
set -euo pipefail

# Baseline load test via cefas-cli, without geohash or plugin indexes.
#
# It creates a normal pk/sk table, inserts many items with BatchWriteItem,
# then measures plain Query, GetItem probes, and an optional Scan.
#
# Auth is delegated to cefas-cli-smoke.sh in CEFAS_AUTH_ONLY mode so the same
# tmp/cefas-cli-smoke.env and tmp/cefasdb.token are reused.
#
# Large run example:
#   REFRESH_AUTH=0 TOTAL_RECORDS=1000000 PARTITION_COUNT=1000 \
#     BATCH_SIZE=500 LOAD_CONCURRENCY=16 QUERY_PARTITIONS=10 \
#     QUERY_LIMIT=1000 GET_PROBES=20 PRINT_SAMPLE=0 ./cefas-cli-normal-load.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# -------- Connection/auth --------
CEFAS_BIN="${CEFAS_BIN:-${ROOT_DIR}/tmp/cefas-cli}"
CEFAS_ENDPOINT="${CEFAS_ENDPOINT:-localhost:19090}"
CEFAS_INSECURE="${CEFAS_INSECURE:-true}"
CEFAS_CA="${CEFAS_CA:-}"
CEFAS_OUTPUT="${CEFAS_OUTPUT:-json}"
CEFAS_TIMEOUT="${CEFAS_TIMEOUT:-300s}"
CEFAS_TOKEN="${CEFAS_TOKEN:-}"
CEFAS_TOKEN_FILE="${CEFAS_TOKEN_FILE:-${ROOT_DIR}/tmp/cefasdb.token}"
REQUIRE_AUTH="${REQUIRE_AUTH:-1}"
REFRESH_AUTH="${REFRESH_AUTH:-1}"

# -------- Load/query shape --------
TABLE_NAME="${TABLE_NAME:-cefas_normal_load_$(date +%Y%m%d%H%M%S)}"
STORAGE_CLASS="${STORAGE_CLASS:-disk}"
TOTAL_RECORDS="${TOTAL_RECORDS:-5000}"
PARTITION_COUNT="${PARTITION_COUNT:-100}"
BATCH_SIZE="${BATCH_SIZE:-500}"
LOAD_CONCURRENCY="${LOAD_CONCURRENCY:-4}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-64}"
RUN_CLEANUP="${RUN_CLEANUP:-1}"
VERBOSE_BATCHES="${VERBOSE_BATCHES:-0}"
KEEP_BATCH_FILES="${KEEP_BATCH_FILES:-0}"
PROGRESS_EVERY_BATCHES="${PROGRESS_EVERY_BATCHES:-10}"
PRINT_SAMPLE="${PRINT_SAMPLE:-3}"
CONSISTENT_READ="${CONSISTENT_READ:-1}"

# Plain read tests.
QUERY_PARTITIONS="${QUERY_PARTITIONS:-5}"
QUERY_LIMIT="${QUERY_LIMIT:-1000}"
GET_PROBES="${GET_PROBES:-10}"
RUN_SCAN="${RUN_SCAN:-1}"
SCAN_LIMIT="${SCAN_LIMIT:-1000}"
SCAN_STATUS="${SCAN_STATUS:-active}"

WORK_DIR="${WORK_DIR:-${ROOT_DIR}/tmp/normal-load-${TABLE_NAME}}"
METRICS_FILE="${METRICS_FILE:-${WORK_DIR}/performance-metrics.json}"
QUERY_RESULTS_FILE="${QUERY_RESULTS_FILE:-${WORK_DIR}/query-results.jsonl}"
GET_RESULTS_FILE="${GET_RESULTS_FILE:-${WORK_DIR}/get-results.jsonl}"
SCAN_OUTPUT_FILE="${SCAN_OUTPUT_FILE:-${WORK_DIR}/scan-result.json}"
created_table=0

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Erro: comando obrigatorio nao encontrado: $1" >&2
    exit 1
  fi
}

ensure_cli_and_auth() {
  if [[ ! -x "$CEFAS_BIN" || ( "$REQUIRE_AUTH" == "1" && "$REFRESH_AUTH" == "1" ) || ( "$REQUIRE_AUTH" == "1" && -z "$CEFAS_TOKEN" && ! -s "$CEFAS_TOKEN_FILE" ) ]]; then
    CEFAS_AUTH_ONLY=1 "$ROOT_DIR/cefas-cli-smoke.sh" >/dev/null
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

bool_enabled() {
  [[ "$1" == "1" || "$1" == "true" || "$1" == "yes" ]]
}

read_consistency_args() {
  if bool_enabled "$CONSISTENT_READ"; then
    printf '%s\0' --consistent-read
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

pk_for_id() {
  local id="$1"
  local partition=$(( ((id - 1) % PARTITION_COUNT) + 1 ))
  printf 'USER#%06d' "$partition"
}

sk_for_id() {
  local id="$1"
  printf 'ITEM#%012d' "$id"
}

expected_for_partition() {
  local partition="$1"
  local base=$((TOTAL_RECORDS / PARTITION_COUNT))
  local remainder=$((TOTAL_RECORDS % PARTITION_COUNT))
  local expected="$base"
  if (( partition <= remainder )); then
    expected=$((expected + 1))
  fi
  printf '%s\n' "$expected"
}

run_batch_write() {
  local file="$1"
  if [[ "$VERBOSE_BATCHES" == "1" || "$VERBOSE_BATCHES" == "true" ]]; then
    print_cmd batch-write-item --table-name "$TABLE_NAME" --request-items "file://$file"
  fi
  "$CEFAS_BIN" "${COMMON_ARGS[@]}" batch-write-item --table-name "$TABLE_NAME" --request-items "file://$file" >/dev/null
}

cleanup() {
  if [[ "$RUN_CLEANUP" == "1" && "$created_table" == "1" ]]; then
    echo >&2
    echo "# Cleanup: removendo tabela $TABLE_NAME" >&2
    run_cefas delete-table --table-name "$TABLE_NAME" >/dev/null || true
  fi
}

validate_numbers() {
  if (( TOTAL_RECORDS <= 0 || PARTITION_COUNT <= 0 || BATCH_SIZE <= 0 || LOAD_CONCURRENCY <= 0 )); then
    echo "Erro: TOTAL_RECORDS, PARTITION_COUNT, BATCH_SIZE ou LOAD_CONCURRENCY invalidos." >&2
    exit 1
  fi
  if (( PAYLOAD_BYTES < 0 || PROGRESS_EVERY_BATCHES < 1 || PRINT_SAMPLE < 0 )); then
    echo "Erro: PAYLOAD_BYTES, PROGRESS_EVERY_BATCHES ou PRINT_SAMPLE invalidos." >&2
    exit 1
  fi
  if (( QUERY_PARTITIONS < 0 || QUERY_LIMIT < 0 || GET_PROBES < 0 || SCAN_LIMIT < 0 )); then
    echo "Erro: QUERY_PARTITIONS, QUERY_LIMIT, GET_PROBES ou SCAN_LIMIT invalidos." >&2
    exit 1
  fi
}

make_batch() {
  local start="$1"
  local count="$2"
  local file="$3"
  LC_ALL=C awk \
    -v start="$start" \
    -v count="$count" \
    -v partitions="$PARTITION_COUNT" \
    -v payload_bytes="$PAYLOAD_BYTES" '
    BEGIN {
      payload = ""
      for (p = 0; p < payload_bytes; p++) payload = payload "x"
      sep = ""
      print "["
      for (j = 0; j < count; j++) {
        idx = start + j
        partition = ((idx - 1) % partitions) + 1
        pk = sprintf("USER#%06d", partition)
        sk = sprintf("ITEM#%012d", idx)
        status = (idx % 2 == 0) ? "active" : "inactive"
        category = sprintf("cat%03d", idx % 100)
        printf "%s{\"PutRequest\":{\"Item\":{", sep
        printf "\"pk\":{\"S\":\"%s\"}", pk
        printf ",\"sk\":{\"S\":\"%s\"}", sk
        printf ",\"status\":{\"S\":\"%s\"}", status
        printf ",\"category\":{\"S\":\"%s\"}", category
        printf ",\"score\":{\"N\":\"%d\"}", idx % 1000
        printf ",\"record_id\":{\"N\":\"%d\"}", idx
        if (payload_bytes > 0) {
          printf ",\"payload\":{\"S\":\"%s\"}", payload
        }
        printf "}}}"
        sep = ",\n"
      }
      print "\n]"
    }' > "$file"
}

run_batch_job() {
  local start="$1"
  local count="$2"
  local file="$3"
  local metrics_file="$4"
  local gen_start gen_end write_start write_end
  local gen_ms write_ms batch_ms bytes

  gen_start="$(now_ms)"
  make_batch "$start" "$count" "$file"
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

load_records() {
  local inserted=0 completed=0 scheduled=0
  local batch_count batch_file metrics_file metrics_dir batches=0
  local load_start load_end job_failed=0
  local records=0 gen_total_ms=0 write_total_ms=0 batch_total_ms=0
  local batch_min_ms=0 batch_max_ms=0 generated_bytes=0
  local pids=()
  metrics_dir="${WORK_DIR}/batch-metrics-$$-$(now_ms)"
  mkdir -p "$metrics_dir"

  wait_oldest_batch() {
    local oldest="${pids[0]}"
    if ! wait "$oldest"; then
      job_failed=1
    fi
    pids=("${pids[@]:1}")
    completed=$((completed + 1))
    if (( completed == 1 || completed == batches || completed % PROGRESS_EVERY_BATCHES == 0 )); then
      echo "# Concluidos insert: batches=${completed}/${batches} agendados=${scheduled}/${TOTAL_RECORDS} rps=$(rate_per_sec "$scheduled" "$(( $(now_ms) - load_start ))")" >&2
    fi
  }

  load_start="$(now_ms)"
  while (( inserted < TOTAL_RECORDS )); do
    batches=$((batches + 1))
    batch_count="$BATCH_SIZE"
    if (( inserted + batch_count > TOTAL_RECORDS )); then
      batch_count=$((TOTAL_RECORDS - inserted))
    fi
    batch_file="${WORK_DIR}/batch-$((inserted + 1))-$((inserted + batch_count)).json"
    metrics_file="${metrics_dir}/batch-${batches}.tsv"
    run_batch_job $((inserted + 1)) "$batch_count" "$batch_file" "$metrics_file" &
    pids+=("$!")
    inserted=$((inserted + batch_count))
    scheduled="$inserted"
    if (( batches == 1 || inserted == TOTAL_RECORDS || batches % PROGRESS_EVERY_BATCHES == 0 )); then
      echo "# Agendados insert: ${scheduled}/${TOTAL_RECORDS} batches=$batches concurrency=$LOAD_CONCURRENCY rps=$(rate_per_sec "$scheduled" "$(( $(now_ms) - load_start ))")" >&2
    fi
    if (( ${#pids[@]} >= LOAD_CONCURRENCY )); then
      wait_oldest_batch
    fi
  done
  while (( ${#pids[@]} > 0 )); do
    wait_oldest_batch
  done
  if (( job_failed != 0 )); then
    echo "Erro: pelo menos um batch de insert falhou." >&2
    exit 1
  fi
  load_end="$(now_ms)"
  read -r LOAD_RECORDS LOAD_BATCHES LOAD_GEN_MS LOAD_WRITE_MS batch_total_ms LOAD_BATCH_MIN_MS LOAD_BATCH_MAX_MS LOAD_BYTES < <(
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

  LOAD_MS=$((load_end - load_start))
  LOAD_RPS="$(rate_per_sec "$LOAD_RECORDS" "$LOAD_MS")"
  LOAD_BATCH_AVG_MS="$(awk -v total="$batch_total_ms" -v batches="$LOAD_BATCHES" 'BEGIN { if (batches <= 0) printf "0"; else printf "%.2f", total / batches }')"
  echo "# Performance insert: records=$LOAD_RECORDS batches=$LOAD_BATCHES concurrency=$LOAD_CONCURRENCY wall_ms=$LOAD_MS records_per_sec=$LOAD_RPS gen_ms=$LOAD_GEN_MS write_ms=$LOAD_WRITE_MS bytes=$LOAD_BYTES batch_avg_ms=$LOAD_BATCH_AVG_MS batch_min_ms=$LOAD_BATCH_MIN_MS batch_max_ms=$LOAD_BATCH_MAX_MS" >&2
}

run_query_tests() {
  local limit_args=()
  local consistency_args=()
  local partitions_to_query="$QUERY_PARTITIONS"
  local part pk pk_value sk_low sk_high out start_ms query_ms count bytes expected exact sample_printed=0
  local failures=0 checked=0 total_ms=0 total_items=0 total_bytes=0 exact_checks=0 partial_checks=0

  : > "$QUERY_RESULTS_FILE"
  if (( partitions_to_query > PARTITION_COUNT )); then
    partitions_to_query="$PARTITION_COUNT"
  fi
  if (( partitions_to_query == 0 )); then
    QUERY_CHECKED=0
    QUERY_FAILURES=0
    QUERY_TOTAL_MS=0
    QUERY_TOTAL_ITEMS=0
    QUERY_TOTAL_BYTES=0
    QUERY_EXACT_CHECKS=0
    QUERY_PARTIAL_CHECKS=0
    return
  fi
  if (( QUERY_LIMIT > 0 )); then
    limit_args=(--limit "$QUERY_LIMIT")
  fi
  while IFS= read -r -d '' arg; do
    consistency_args+=("$arg")
  done < <(read_consistency_args)

  mkdir -p "$WORK_DIR/queries"
  for (( part = 1; part <= partitions_to_query; part++ )); do
    pk="$(printf 'USER#%06d' "$part")"
    pk_value="$(jq -cn --arg pk "$pk" '{S:$pk}')"
    sk_low="$(jq -cn '{S:"ITEM#000000000000"}')"
    sk_high="$(jq -cn '{S:"ITEM#999999999999"}')"
    out="${WORK_DIR}/queries/query-${part}.json"
    start_ms="$(now_ms)"
    capture_cefas query \
      --table-name "$TABLE_NAME" \
      --pk-value "$pk_value" \
      --sk-low "$sk_low" \
      --sk-high "$sk_high" \
      "${limit_args[@]}" \
      "${consistency_args[@]}" > "$out"
    query_ms=$(( $(now_ms) - start_ms ))
    count="$(jq -r '.Count // (.Items | length)' "$out")"
    bytes="$(file_size_bytes "$out")"
    expected="$(expected_for_partition "$part")"
    exact=true
    if (( QUERY_LIMIT > 0 && expected > QUERY_LIMIT )); then
      exact=false
    fi
    if [[ "$exact" == "true" ]]; then
      exact_checks=$((exact_checks + 1))
      if [[ "$count" != "$expected" ]]; then
        failures=$((failures + 1))
      fi
    else
      partial_checks=$((partial_checks + 1))
      if (( count > QUERY_LIMIT )); then
        failures=$((failures + 1))
      fi
    fi
    checked=$((checked + 1))
    total_ms=$((total_ms + query_ms))
    total_items=$((total_items + count))
    total_bytes=$((total_bytes + bytes))
    jq -cn \
      --arg pk "$pk" \
      --argjson queryMs "$query_ms" \
      --argjson count "$count" \
      --argjson expected "$expected" \
      --argjson exact "$exact" \
      --argjson bytes "$bytes" \
      '{pk:$pk,queryMs:$queryMs,count:$count,expected:$expected,exactValidation:$exact,bytes:$bytes,passed:(if $exact then $count == $expected else true end)}' >> "$QUERY_RESULTS_FILE"
    echo "# Query pk=$pk count=$count expected=$expected exact=$exact query_ms=$query_ms bytes=$bytes" >&2
    if (( PRINT_SAMPLE > 0 && sample_printed == 0 )); then
      jq --argjson sample "$PRINT_SAMPLE" '{Count, Sample: (.Items[:$sample] // [])}' "$out"
      sample_printed=1
    fi
  done
  QUERY_CHECKED="$checked"
  QUERY_FAILURES="$failures"
  QUERY_TOTAL_MS="$total_ms"
  QUERY_TOTAL_ITEMS="$total_items"
  QUERY_TOTAL_BYTES="$total_bytes"
  QUERY_EXACT_CHECKS="$exact_checks"
  QUERY_PARTIAL_CHECKS="$partial_checks"
  QUERY_RPS="$(rate_per_sec "$total_items" "$total_ms")"
  if (( failures > 0 )); then
    echo "Erro: query normal falhou em $failures de $checked checks. Detalhes: $QUERY_RESULTS_FILE" >&2
    exit 1
  fi
}

run_get_tests() {
  local consistency_args=()
  local probes="$GET_PROBES"
  local i id pk sk key out start_ms get_ms hit bytes
  local failures=0 checked=0 total_ms=0 total_bytes=0

  : > "$GET_RESULTS_FILE"
  if (( probes > TOTAL_RECORDS )); then
    probes="$TOTAL_RECORDS"
  fi
  if (( probes == 0 )); then
    GET_CHECKED=0
    GET_FAILURES=0
    GET_TOTAL_MS=0
    GET_TOTAL_BYTES=0
    return
  fi
  while IFS= read -r -d '' arg; do
    consistency_args+=("$arg")
  done < <(read_consistency_args)

  mkdir -p "$WORK_DIR/gets"
  for (( i = 1; i <= probes; i++ )); do
    if (( probes == 1 )); then
      id=1
    else
      id=$((1 + ((i - 1) * (TOTAL_RECORDS - 1) / (probes - 1))))
    fi
    pk="$(pk_for_id "$id")"
    sk="$(sk_for_id "$id")"
    key="$(jq -cn --arg pk "$pk" --arg sk "$sk" '{pk:{S:$pk},sk:{S:$sk}}')"
    out="${WORK_DIR}/gets/get-${i}.json"
    start_ms="$(now_ms)"
    capture_cefas get-item \
      --table-name "$TABLE_NAME" \
      --key "$key" \
      "${consistency_args[@]}" > "$out"
    get_ms=$(( $(now_ms) - start_ms ))
    hit="$(jq -r 'if .Item then 1 else 0 end' "$out")"
    bytes="$(file_size_bytes "$out")"
    checked=$((checked + 1))
    total_ms=$((total_ms + get_ms))
    total_bytes=$((total_bytes + bytes))
    if [[ "$hit" != "1" ]]; then
      failures=$((failures + 1))
    fi
    jq -cn \
      --arg pk "$pk" \
      --arg sk "$sk" \
      --argjson id "$id" \
      --argjson getMs "$get_ms" \
      --argjson hit "$hit" \
      --argjson bytes "$bytes" \
      '{id:$id,pk:$pk,sk:$sk,getMs:$getMs,hit:$hit,bytes:$bytes,passed:($hit == 1)}' >> "$GET_RESULTS_FILE"
    echo "# Get probe id=$id pk=$pk sk=$sk hit=$hit get_ms=$get_ms bytes=$bytes" >&2
  done
  GET_CHECKED="$checked"
  GET_FAILURES="$failures"
  GET_TOTAL_MS="$total_ms"
  GET_TOTAL_BYTES="$total_bytes"
  GET_RPS="$(rate_per_sec "$checked" "$total_ms")"
  if (( failures > 0 )); then
    echo "Erro: get-item falhou em $failures de $checked probes. Detalhes: $GET_RESULTS_FILE" >&2
    exit 1
  fi
}

run_scan_test() {
  local limit_args=()
  local consistency_args=()
  local values out start_ms scan_ms count bytes

  SCAN_ENABLED=false
  SCAN_MS=0
  SCAN_COUNT=0
  SCAN_BYTES=0
  SCAN_RPS=0
  if ! bool_enabled "$RUN_SCAN"; then
    return
  fi
  SCAN_ENABLED=true
  if (( SCAN_LIMIT > 0 )); then
    limit_args=(--limit "$SCAN_LIMIT")
  fi
  while IFS= read -r -d '' arg; do
    consistency_args+=("$arg")
  done < <(read_consistency_args)

  values="$(jq -cn --arg status "$SCAN_STATUS" '{":status":{S:$status}}')"
  out="$SCAN_OUTPUT_FILE"
  start_ms="$(now_ms)"
  capture_cefas scan \
    --table-name "$TABLE_NAME" \
    --filter-expression "status = :status" \
    --expression-attribute-values "$values" \
    "${limit_args[@]}" \
    "${consistency_args[@]}" > "$out"
  scan_ms=$(( $(now_ms) - start_ms ))
  count="$(jq -r '.Count // (.Items | length)' "$out")"
  bytes="$(file_size_bytes "$out")"
  SCAN_MS="$scan_ms"
  SCAN_COUNT="$count"
  SCAN_BYTES="$bytes"
  SCAN_RPS="$(rate_per_sec "$count" "$scan_ms")"
  echo "# Scan status=$SCAN_STATUS count=$count limit=$SCAN_LIMIT scan_ms=$scan_ms records_per_sec=$SCAN_RPS bytes=$bytes" >&2
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

LOAD_RECORDS=0
LOAD_BATCHES=0
LOAD_MS=0
LOAD_GEN_MS=0
LOAD_WRITE_MS=0
LOAD_RPS=0
LOAD_BYTES=0
LOAD_BATCH_AVG_MS=0
LOAD_BATCH_MIN_MS=0
LOAD_BATCH_MAX_MS=0
QUERY_CHECKED=0
QUERY_FAILURES=0
QUERY_TOTAL_MS=0
QUERY_TOTAL_ITEMS=0
QUERY_TOTAL_BYTES=0
QUERY_EXACT_CHECKS=0
QUERY_PARTIAL_CHECKS=0
QUERY_RPS=0
GET_CHECKED=0
GET_FAILURES=0
GET_TOTAL_MS=0
GET_TOTAL_BYTES=0
GET_RPS=0
SCAN_ENABLED=false
SCAN_MS=0
SCAN_COUNT=0
SCAN_BYTES=0
SCAN_RPS=0

echo "# CefasDB normal load test"
echo "# endpoint=$CEFAS_ENDPOINT table=$TABLE_NAME cleanup=$RUN_CLEANUP"
echo "# total_records=$TOTAL_RECORDS partitions=$PARTITION_COUNT batch_size=$BATCH_SIZE load_concurrency=$LOAD_CONCURRENCY payload_bytes=$PAYLOAD_BYTES"
echo "# query_partitions=$QUERY_PARTITIONS query_limit=$QUERY_LIMIT get_probes=$GET_PROBES run_scan=$RUN_SCAN scan_limit=$SCAN_LIMIT consistent_read=$CONSISTENT_READ"
echo "# metrics_file=$METRICS_FILE query_results_file=$QUERY_RESULTS_FILE get_results_file=$GET_RESULTS_FILE"

create_table_start_ms="$(now_ms)"
run_cefas create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions "AttributeName=pk,AttributeType=S" \
  --attribute-definitions "AttributeName=sk,AttributeType=S" \
  --key-schema "AttributeName=pk,KeyType=HASH" \
  --key-schema "AttributeName=sk,KeyType=RANGE" \
  --billing-mode PAY_PER_REQUEST \
  --storage-class "$STORAGE_CLASS"
create_table_ms=$(( $(now_ms) - create_table_start_ms ))
created_table=1

load_records
run_query_tests
run_get_tests
run_scan_test

total_ms=$(( $(now_ms) - total_start_ms ))
total_rps="$(rate_per_sec "$TOTAL_RECORDS" "$total_ms")"

metrics_json="$(jq -n \
  --arg table "$TABLE_NAME" \
  --arg endpoint "$CEFAS_ENDPOINT" \
  --arg metricsFile "$METRICS_FILE" \
  --arg queryResultsFile "$QUERY_RESULTS_FILE" \
  --arg getResultsFile "$GET_RESULTS_FILE" \
  --arg scanOutputFile "$SCAN_OUTPUT_FILE" \
  --arg scanStatus "$SCAN_STATUS" \
  --argjson totalRecords "$TOTAL_RECORDS" \
  --argjson partitionCount "$PARTITION_COUNT" \
  --argjson batchSize "$BATCH_SIZE" \
  --argjson loadConcurrency "$LOAD_CONCURRENCY" \
  --argjson payloadBytes "$PAYLOAD_BYTES" \
  --argjson consistentRead "$(if bool_enabled "$CONSISTENT_READ"; then echo true; else echo false; fi)" \
  --argjson queryPartitions "$QUERY_PARTITIONS" \
  --argjson queryLimit "$QUERY_LIMIT" \
  --argjson getProbes "$GET_PROBES" \
  --argjson runScan "$(if bool_enabled "$RUN_SCAN"; then echo true; else echo false; fi)" \
  --argjson scanLimit "$SCAN_LIMIT" \
  --argjson createTableMs "$create_table_ms" \
  --argjson loadRecords "$LOAD_RECORDS" \
  --argjson loadBatches "$LOAD_BATCHES" \
  --argjson loadMs "$LOAD_MS" \
  --argjson loadGenMs "$LOAD_GEN_MS" \
  --argjson loadWriteMs "$LOAD_WRITE_MS" \
  --argjson loadRps "$LOAD_RPS" \
  --argjson loadBytes "$LOAD_BYTES" \
  --argjson loadBatchAvgMs "$LOAD_BATCH_AVG_MS" \
  --argjson loadBatchMinMs "$LOAD_BATCH_MIN_MS" \
  --argjson loadBatchMaxMs "$LOAD_BATCH_MAX_MS" \
  --argjson queryChecked "$QUERY_CHECKED" \
  --argjson queryFailures "$QUERY_FAILURES" \
  --argjson queryTotalMs "$QUERY_TOTAL_MS" \
  --argjson queryTotalItems "$QUERY_TOTAL_ITEMS" \
  --argjson queryTotalBytes "$QUERY_TOTAL_BYTES" \
  --argjson queryExactChecks "$QUERY_EXACT_CHECKS" \
  --argjson queryPartialChecks "$QUERY_PARTIAL_CHECKS" \
  --argjson queryRps "$QUERY_RPS" \
  --argjson getChecked "$GET_CHECKED" \
  --argjson getFailures "$GET_FAILURES" \
  --argjson getTotalMs "$GET_TOTAL_MS" \
  --argjson getTotalBytes "$GET_TOTAL_BYTES" \
  --argjson getRps "$GET_RPS" \
  --argjson scanEnabled "$SCAN_ENABLED" \
  --argjson scanMs "$SCAN_MS" \
  --argjson scanCount "$SCAN_COUNT" \
  --argjson scanBytes "$SCAN_BYTES" \
  --argjson scanRps "$SCAN_RPS" \
  --argjson totalMs "$total_ms" \
  --argjson totalRps "$total_rps" \
  '{
    table: $table,
    endpoint: $endpoint,
    metricsFile: $metricsFile,
    queryResultsFile: $queryResultsFile,
    getResultsFile: $getResultsFile,
    scanOutputFile: $scanOutputFile,
    config: {
      totalRecords: $totalRecords,
      partitionCount: $partitionCount,
      batchSize: $batchSize,
      loadConcurrency: $loadConcurrency,
      payloadBytes: $payloadBytes,
      consistentRead: $consistentRead,
      queryPartitions: $queryPartitions,
      queryLimit: $queryLimit,
      getProbes: $getProbes,
      runScan: $runScan,
      scanLimit: $scanLimit,
      scanStatus: $scanStatus
    },
    validation: {
      queryChecked: $queryChecked,
      queryFailures: $queryFailures,
      queryExactChecks: $queryExactChecks,
      queryPartialChecks: $queryPartialChecks,
      getChecked: $getChecked,
      getFailures: $getFailures
    },
    performance: {
      createTableMs: $createTableMs,
      insertMs: $loadMs,
      insertRecordsPerSec: $loadRps,
      queryMs: $queryTotalMs,
      queryItemsPerSec: $queryRps,
      getMs: $getTotalMs,
      getOpsPerSec: $getRps,
      scanMs: $scanMs,
      scanItemsPerSec: $scanRps,
      totalMs: $totalMs,
      totalRecordsPerSec: $totalRps
    },
    phases: {
      insert: {
        records: $loadRecords,
        batches: $loadBatches,
        totalMs: $loadMs,
        generateMs: $loadGenMs,
        writeMs: $loadWriteMs,
        recordsPerSec: $loadRps,
        generatedBytes: $loadBytes,
        batchAvgMs: $loadBatchAvgMs,
        batchMinMs: $loadBatchMinMs,
        batchMaxMs: $loadBatchMaxMs
      },
      query: {
        checkedPartitions: $queryChecked,
        totalMs: $queryTotalMs,
        returnedItems: $queryTotalItems,
        bytes: $queryTotalBytes,
        itemsPerSec: $queryRps
      },
      get: {
        checked: $getChecked,
        totalMs: $getTotalMs,
        bytes: $getTotalBytes,
        opsPerSec: $getRps
      },
      scan: {
        enabled: $scanEnabled,
        status: $scanStatus,
        limit: $scanLimit,
        totalMs: $scanMs,
        returnedItems: $scanCount,
        bytes: $scanBytes,
        itemsPerSec: $scanRps
      }
    }
  }')"

printf '%s\n' "$metrics_json" > "$METRICS_FILE"
echo "# Performance metrics:"
printf '%s\n' "$metrics_json"
echo "# Normal load test concluido."
