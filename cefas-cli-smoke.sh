#!/usr/bin/env bash
set -euo pipefail

# Smoke test via cefas-cli.
#
# Prod hoje expoe o gRPC internamente no Kubernetes. Para testar prod a partir
# da sua maquina, abra este port-forward em outro terminal:
#
#   kubectl port-forward svc/cefasdb 19090:9090 -n default
#
# O script tambem consegue obter um access token Tikti com audience "cefasdb"
# a partir de usuario/senha. Coloque credenciais locais em
# tmp/cefas-cli-smoke.env ou defina as variaveis CEFAS_AUTH_* no ambiente.
# Nao commite senhas nem tokens.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CEFAS_SMOKE_ENV_FILE="${CEFAS_SMOKE_ENV_FILE:-${ROOT_DIR}/tmp/cefas-cli-smoke.env}"

if [[ -f "$CEFAS_SMOKE_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$CEFAS_SMOKE_ENV_FILE"
  set +a
fi

# -------- Parametros de conexao --------
CEFAS_BIN="${CEFAS_BIN:-${ROOT_DIR}/tmp/cefas-cli}"
CEFAS_ENDPOINT="${CEFAS_ENDPOINT:-localhost:19090}"
CEFAS_INSECURE="${CEFAS_INSECURE:-true}"
CEFAS_CA="${CEFAS_CA:-}"
CEFAS_OUTPUT="${CEFAS_OUTPUT:-json}"
CEFAS_TIMEOUT="${CEFAS_TIMEOUT:-30s}"
CEFAS_TOKEN="${CEFAS_TOKEN:-}"
CEFAS_TOKEN_FILE="${CEFAS_TOKEN_FILE:-${ROOT_DIR}/tmp/cefasdb.token}"
REQUIRE_AUTH="${REQUIRE_AUTH:-1}"

# -------- Parametros de autenticacao Tikti --------
CEFAS_AUTH_FETCH="${CEFAS_AUTH_FETCH:-auto}"
CEFAS_AUTH_REFRESH="${CEFAS_AUTH_REFRESH:-1}"
CEFAS_AUTH_BASE_URL="${CEFAS_AUTH_BASE_URL:-https://api.storifly.ai/v1/accounts}"
CEFAS_AUTH_EXCHANGE_URL="${CEFAS_AUTH_EXCHANGE_URL:-}"
CEFAS_AUTH_API_KEY="${CEFAS_AUTH_API_KEY:-${TIKTI_API_KEY:-}}"
CEFAS_AUTH_EMAIL="${CEFAS_AUTH_EMAIL:-${TIKTI_EMAIL:-osvaldo@codecompany.com.br}}"
CEFAS_AUTH_PASSWORD="${CEFAS_AUTH_PASSWORD:-${TIKTI_PASSWORD:-}}"
CEFAS_AUTH_AUDIENCE="${CEFAS_AUTH_AUDIENCE:-cefasdb}"
CEFAS_AUTH_TENANT_ID="${CEFAS_AUTH_TENANT_ID:-${TIKTI_TENANT_ID:-}}"
CEFAS_AUTH_DEFAULT_TENANT_ID="${CEFAS_AUTH_DEFAULT_TENANT_ID:-default}"
CEFAS_AUTH_TTL_SECONDS="${CEFAS_AUTH_TTL_SECONDS:-3600}"
CEFAS_AUTH_SCOPES="${CEFAS_AUTH_SCOPES:-}"
CEFAS_AUTH_FALLBACK_SCOPES="${CEFAS_AUTH_FALLBACK_SCOPES:-cefas:table:create,cefas:table:drop,cefas:table:describe,cefas:item:read:*,cefas:item:write:*,cefas:item:delete:*,cefas:query:*,cefas:scan:*,cefas:spatial:*,cefas:cluster:admin}"
CEFAS_AUTH_ONLY="${CEFAS_AUTH_ONLY:-0}"
CEFAS_AUTH_DEBUG="${CEFAS_AUTH_DEBUG:-0}"

# -------- Parametros do teste --------
TABLE_NAME="${TABLE_NAME:-cefas_cli_smoke_$(date +%Y%m%d%H%M%S)}"
STORAGE_CLASS="${STORAGE_CLASS:-disk}"
STREAM_ENABLED="${STREAM_ENABLED:-true}"
STREAM_VIEW_TYPE="${STREAM_VIEW_TYPE:-NEW_AND_OLD_IMAGES}"
RUN_STREAMS="${RUN_STREAMS:-auto}"
RUN_VECTOR_OPS="${RUN_VECTOR_OPS:-1}"
RUN_CLEANUP="${RUN_CLEANUP:-1}"

PK_NAME="${PK_NAME:-pk}"
SK_NAME="${SK_NAME:-sk}"
PK_VALUE="${PK_VALUE:-USER#cli-smoke}"
SK_PROFILE="${SK_PROFILE:-PROFILE}"
SK_EVENT="${SK_EVENT:-EVENT#001}"
SK_EVENT_2="${SK_EVENT_2:-EVENT#002}"
STATUS_INITIAL="${STATUS_INITIAL:-new}"
STATUS_ACTIVE="${STATUS_ACTIVE:-active}"
SCORE_INITIAL="${SCORE_INITIAL:-10}"
SCORE_INCREMENT="${SCORE_INCREMENT:-5}"
TOPK_K="${TOPK_K:-2}"
SCAN_LIMIT="${SCAN_LIMIT:-10}"
STREAM_LIMIT="${STREAM_LIMIT:-100}"

ITEM_PROFILE="$(printf '{"%s":{"S":"%s"},"%s":{"S":"%s"},"name":{"S":"CLI Smoke"},"status":{"S":"%s"},"score":{"N":"%s"},"category":{"S":"profile"},"embedding":{"V":[0.10,0.20,0.30],"D":3}}' "$PK_NAME" "$PK_VALUE" "$SK_NAME" "$SK_PROFILE" "$STATUS_INITIAL" "$SCORE_INITIAL")"
ITEM_EVENT_1="$(printf '{"%s":{"S":"%s"},"%s":{"S":"%s"},"name":{"S":"CLI Event 1"},"status":{"S":"%s"},"score":{"N":"20"},"category":{"S":"event"},"embedding":{"V":[0.20,0.10,0.30],"D":3}}' "$PK_NAME" "$PK_VALUE" "$SK_NAME" "$SK_EVENT" "$STATUS_ACTIVE")"
ITEM_EVENT_2="$(printf '{"%s":{"S":"%s"},"%s":{"S":"%s"},"name":{"S":"CLI Event 2"},"status":{"S":"inactive"},"score":{"N":"30"},"category":{"S":"event"},"embedding":{"V":[0.90,0.10,0.00],"D":3}}' "$PK_NAME" "$PK_VALUE" "$SK_NAME" "$SK_EVENT_2")"
KEY_PROFILE="$(printf '{"%s":{"S":"%s"},"%s":{"S":"%s"}}' "$PK_NAME" "$PK_VALUE" "$SK_NAME" "$SK_PROFILE")"
PK_VALUE_JSON="$(printf '{"S":"%s"}' "$PK_VALUE")"
SK_LOW_JSON="$(printf '{"S":"%s"}' "$SK_EVENT")"
SK_HIGH_JSON="$(printf '{"S":"%s"}' "$SK_PROFILE")"
UPDATE_NAMES='{}'
UPDATE_VALUES="$(printf '{":status":{"S":"%s"},":inc":{"N":"%s"}}' "$STATUS_ACTIVE" "$SCORE_INCREMENT")"
SCAN_VALUES="$(printf '{":status":{"S":"%s"}}' "$STATUS_ACTIVE")"
PARTIQL_PARAMETERS="$(printf '[{"S":"%s"}]' "$PK_VALUE")"
QUERY_VECTOR='{"V":[0.10,0.20,0.30],"D":3}'

created_table=0

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Erro: comando obrigatorio nao encontrado: $1" >&2
    exit 1
  fi
}

build_cli_if_needed() {
  if [[ -x "$CEFAS_BIN" ]]; then
    return
  fi
  need_cmd go
  mkdir -p "$(dirname "$CEFAS_BIN")"
  echo "+ go build -o $CEFAS_BIN ./cmd/cefas-cli" >&2
  (cd "$ROOT_DIR" && go build -o "$CEFAS_BIN" ./cmd/cefas-cli)
}

append_api_key() {
  local url="$1"
  local key="$2"
  if [[ -z "$key" ]]; then
    printf '%s' "$url"
    return
  fi
  if [[ "$url" == *\?* ]]; then
    printf '%s&key=%s' "$url" "$key"
  else
    printf '%s?key=%s' "$url" "$key"
  fi
}

HTTP_STATUS=""
HTTP_BODY=""

post_json() {
  local url="$1"
  local payload="$2"
  local body_file
  body_file="$(mktemp "${TMPDIR:-/tmp}/cefas-http.XXXXXX")"
  HTTP_STATUS=""
  HTTP_BODY=""
  if ! HTTP_STATUS="$(curl -sS -o "$body_file" -w '%{http_code}' -X POST "$url" \
    -H 'Content-Type: application/json' \
    --data "$payload")"; then
    HTTP_BODY="$(<"$body_file")"
    rm -f "$body_file"
    return 1
  fi
  HTTP_BODY="$(<"$body_file")"
  rm -f "$body_file"
  if [[ "$HTTP_STATUS" -lt 200 || "$HTTP_STATUS" -ge 300 ]]; then
    return 1
  fi
}

response_error_message() {
  local fallback="$1"
  if [[ -z "$HTTP_BODY" ]]; then
    printf '%s' "$fallback"
    return
  fi
  if jq -e . >/dev/null 2>&1 <<<"$HTTP_BODY"; then
    jq -r 'if (.error? | type) == "object" then (.error.message // .message // .code // empty) else (.error // .message // .code // empty) end' <<<"$HTTP_BODY"
    return
  fi
  printf '%s' "$HTTP_BODY" | tr '\n' ' ' | cut -c 1-240
}

csv_to_json_array() {
  local values="$1"
  jq -cn --arg values "$values" \
    '$values | gsub(",";" ") | split(" ") | map(select(length > 0))'
}

append_unique() {
  local var_name="$1"
  local value="$2"
  local current item
  value="$(printf '%s' "$value" | xargs)"
  [[ -z "$value" ]] && return
  eval "current=\"\${$var_name:-}\""
  for item in $current; do
    if [[ "$item" == "$value" ]]; then
      return
    fi
  done
  eval "$var_name=\"\${current:+\$current }$value\""
}

should_fetch_auth_token() {
  if [[ -n "$CEFAS_TOKEN" || "$REQUIRE_AUTH" != "1" ]]; then
    return 1
  fi
  if [[ "$CEFAS_AUTH_FETCH" == "0" || "$CEFAS_AUTH_FETCH" == "false" ]]; then
    return 1
  fi
  if [[ "$CEFAS_AUTH_FETCH" == "1" || "$CEFAS_AUTH_FETCH" == "true" ]]; then
    return 0
  fi
  if [[ -n "$CEFAS_AUTH_PASSWORD" && ( "$CEFAS_AUTH_REFRESH" == "1" || "$CEFAS_AUTH_REFRESH" == "true" || ! -s "$CEFAS_TOKEN_FILE" ) ]]; then
    return 0
  fi
  return 1
}

fetch_auth_token() {
  need_cmd curl
  need_cmd jq

  if [[ -z "$CEFAS_AUTH_EMAIL" || -z "$CEFAS_AUTH_PASSWORD" ]]; then
    echo "Erro: defina CEFAS_AUTH_EMAIL e CEFAS_AUTH_PASSWORD para gerar o token automaticamente." >&2
    exit 1
  fi

  local auth_base login_payload signin_url signin_json id_token lookup_url lookup_json lookup_tenant
  local exchange_url exchange_json access_token token_dir tenant_candidates scope_candidates tenant scopes label exchange_payload message auth_failures
  auth_base="${CEFAS_AUTH_BASE_URL%/}"

  login_payload="$(jq -cn \
    --arg email "$CEFAS_AUTH_EMAIL" \
    --arg password "$CEFAS_AUTH_PASSWORD" \
    '{email:$email,password:$password,returnSecureToken:true}')"

  echo "# Auth: obtendo idToken Tikti para ${CEFAS_AUTH_EMAIL}" >&2
  if [[ -n "$CEFAS_AUTH_API_KEY" ]]; then
    signin_url="$(append_api_key "${auth_base}/signInWithPassword" "$CEFAS_AUTH_API_KEY")"
    if post_json "$signin_url" "$login_payload"; then
      signin_json="$HTTP_BODY"
    else
      echo "# Auth: signInWithPassword falhou; tentando signIn publico." >&2
      if post_json "${auth_base}/signIn" "$login_payload"; then
        signin_json="$HTTP_BODY"
      else
        message="$(response_error_message "login falhou")"
        echo "Erro: login Tikti falhou (status=${HTTP_STATUS:-curl}): $message" >&2
        exit 1
      fi
    fi
  else
    if post_json "${auth_base}/signIn" "$login_payload"; then
      signin_json="$HTTP_BODY"
    else
      message="$(response_error_message "login falhou")"
      echo "Erro: login Tikti falhou (status=${HTTP_STATUS:-curl}): $message" >&2
      exit 1
    fi
  fi

  id_token="$(printf '%s' "$signin_json" | jq -r '.idToken // .id_token // empty')"
  if [[ -z "$id_token" || "$id_token" == "null" ]]; then
    echo "Erro: resposta do login nao trouxe idToken." >&2
    exit 1
  fi

  if [[ -n "$CEFAS_AUTH_API_KEY" ]]; then
    lookup_url="$(append_api_key "${auth_base}/lookup" "$CEFAS_AUTH_API_KEY")"
    if post_json "$lookup_url" "$(jq -cn --arg idToken "$id_token" '{idToken:$idToken}')"; then
      lookup_json="$HTTP_BODY"
      lookup_tenant="$(printf '%s' "$lookup_json" | jq -r '.users[0].tenantId // .users[0].tenant // empty')"
    else
      lookup_tenant=""
    fi
  fi

  tenant_candidates=""
  append_unique tenant_candidates "$CEFAS_AUTH_TENANT_ID"
  append_unique tenant_candidates "$CEFAS_AUTH_DEFAULT_TENANT_ID"
  append_unique tenant_candidates "$lookup_tenant"
  append_unique tenant_candidates "__auto__"

  scope_candidates=""
  append_unique scope_candidates "$CEFAS_AUTH_SCOPES"
  append_unique scope_candidates "__client_default__"
  append_unique scope_candidates "$CEFAS_AUTH_FALLBACK_SCOPES"

  if [[ -n "$CEFAS_AUTH_EXCHANGE_URL" ]]; then
    exchange_url="$CEFAS_AUTH_EXCHANGE_URL"
  else
    exchange_url="${auth_base}/token/exchange"
  fi
  exchange_url="$(append_api_key "$exchange_url" "$CEFAS_AUTH_API_KEY")"
  echo "# Auth: trocando token para audience=${CEFAS_AUTH_AUDIENCE}" >&2

  auth_failures=""
  for tenant in $tenant_candidates; do
    for scopes in $scope_candidates; do
      if [[ "$tenant" == "__auto__" ]]; then
        tenant=""
      fi
      if [[ "$scopes" == "__client_default__" ]]; then
        scopes=""
        label="client-default"
      else
        label="explicit"
      fi
      exchange_payload="$(jq -cn \
        --arg idToken "$id_token" \
        --arg audience "$CEFAS_AUTH_AUDIENCE" \
        --arg tenantId "$tenant" \
        --argjson ttl "$CEFAS_AUTH_TTL_SECONDS" \
        --argjson scopes "$(csv_to_json_array "$scopes")" \
        '{idToken:$idToken,audience:$audience}
         + (if ($scopes | length) > 0 then {scopes:$scopes} else {} end)
         + (if $tenantId != "" then {tenantId:$tenantId} else {} end)
         + (if $ttl > 0 then {ttlSeconds:$ttl} else {} end)')"

      if post_json "$exchange_url" "$exchange_payload"; then
        exchange_json="$HTTP_BODY"
        access_token="$(printf '%s' "$exchange_json" | jq -r '.accessToken // .access_token // empty')"
        if [[ -n "$access_token" && "$access_token" != "null" ]]; then
          token_dir="$(dirname "$CEFAS_TOKEN_FILE")"
          mkdir -p "$token_dir"
          umask 077
          printf '%s\n' "$access_token" > "$CEFAS_TOKEN_FILE"
          chmod 600 "$CEFAS_TOKEN_FILE" 2>/dev/null || true
          echo "# Auth: token salvo em $CEFAS_TOKEN_FILE" >&2
          return
        fi
        HTTP_STATUS="200"
        HTTP_BODY="$exchange_json"
        message="resposta sem accessToken"
      else
        message="$(response_error_message "token/exchange falhou")"
      fi
      message="# Auth: exchange falhou (tenant=${tenant:-auto}, scopes=$label, status=${HTTP_STATUS:-curl}): $message"
      if [[ "$CEFAS_AUTH_DEBUG" == "1" || "$CEFAS_AUTH_DEBUG" == "true" ]]; then
        echo "$message" >&2
      else
        auth_failures="${auth_failures}${message}"$'\n'
      fi
    done
  done

  echo "Erro: nao consegui obter accessToken para audience=${CEFAS_AUTH_AUDIENCE}." >&2
  if [[ -n "$auth_failures" && "$CEFAS_AUTH_DEBUG" != "1" && "$CEFAS_AUTH_DEBUG" != "true" ]]; then
    printf '%s' "$auth_failures" >&2
  fi
  echo "Verifique se o client Tikti ${CEFAS_AUTH_AUDIENCE} existe no tenant correto e se seus scopes permitem CefasDB." >&2
  exit 1
}

ensure_auth_token() {
  if should_fetch_auth_token; then
    fetch_auth_token
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
  "$CEFAS_BIN" "${COMMON_ARGS[@]}" "$@"
}

cleanup() {
  if [[ "$RUN_CLEANUP" == "1" && "$created_table" == "1" ]]; then
    echo >&2
    echo "# Cleanup: removendo tabela $TABLE_NAME" >&2
    run_cefas delete-table --table-name "$TABLE_NAME" >/dev/null || true
  fi
}

build_cli_if_needed

if [[ "$RUN_STREAMS" != "0" && "$RUN_STREAMS" != "false" ]]; then
  need_cmd jq
fi

ensure_auth_token

if [[ "$CEFAS_AUTH_ONLY" == "1" || "$CEFAS_AUTH_ONLY" == "true" ]]; then
  echo "# Auth concluida."
  exit 0
fi

if [[ "$REQUIRE_AUTH" == "1" && -z "$CEFAS_TOKEN" && ! -f "$CEFAS_TOKEN_FILE" ]]; then
  echo "Erro: auth obrigatoria. Salve o token em $CEFAS_TOKEN_FILE ou defina CEFAS_TOKEN." >&2
  echo "Ou defina CEFAS_AUTH_EMAIL/CEFAS_AUTH_PASSWORD para gerar token automaticamente." >&2
  echo "Para testar um servidor local sem auth: REQUIRE_AUTH=0 ./cefas-cli-smoke.sh" >&2
  exit 1
fi

COMMON_ARGS=()
while IFS= read -r -d '' arg; do
  COMMON_ARGS+=("$arg")
done < <(common_args)

trap cleanup EXIT

echo "# CefasDB CLI smoke test"
echo "# endpoint=$CEFAS_ENDPOINT table=$TABLE_NAME output=$CEFAS_OUTPUT cleanup=$RUN_CLEANUP"

run_cefas version
run_cefas list-tables

run_cefas create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions "AttributeName=${PK_NAME},AttributeType=S" \
  --attribute-definitions "AttributeName=${SK_NAME},AttributeType=S" \
  --attribute-definitions "AttributeName=embedding,AttributeType=V<3>" \
  --key-schema "AttributeName=${PK_NAME},KeyType=HASH" \
  --key-schema "AttributeName=${SK_NAME},KeyType=RANGE" \
  --billing-mode PAY_PER_REQUEST \
  --storage-class "$STORAGE_CLASS" \
  --stream-specification "StreamEnabled=${STREAM_ENABLED},StreamViewType=${STREAM_VIEW_TYPE}"
created_table=1

run_cefas describe-table --table-name "$TABLE_NAME"

run_cefas put-item \
  --table-name "$TABLE_NAME" \
  --item "$ITEM_PROFILE"

run_cefas put-item \
  --table-name "$TABLE_NAME" \
  --item "$ITEM_EVENT_1"

run_cefas put-item \
  --table-name "$TABLE_NAME" \
  --item "$ITEM_EVENT_2"

run_cefas get-item \
  --table-name "$TABLE_NAME" \
  --key "$KEY_PROFILE" \
  --consistent-read

run_cefas update-item \
  --table-name "$TABLE_NAME" \
  --key "$KEY_PROFILE" \
  --update-expression "SET status = :status, ADD score :inc" \
  --expression-attribute-names "$UPDATE_NAMES" \
  --expression-attribute-values "$UPDATE_VALUES" \
  --return-values ALL_NEW

run_cefas query \
  --table-name "$TABLE_NAME" \
  --pk-value "$PK_VALUE_JSON" \
  --sk-low "$SK_LOW_JSON" \
  --sk-high "$SK_HIGH_JSON" \
  --limit "$SCAN_LIMIT" \
  --consistent-read

run_cefas scan \
  --table-name "$TABLE_NAME" \
  --filter-expression "status = :status" \
  --expression-attribute-values "$SCAN_VALUES" \
  --limit "$SCAN_LIMIT" \
  --consistent-read

run_cefas execute-statement \
  --statement "SELECT * FROM ${TABLE_NAME} WHERE ${PK_NAME} = ?" \
  --parameters "$PARTIQL_PARAMETERS"

if [[ "$RUN_VECTOR_OPS" == "1" ]]; then
  run_cefas top-k \
    --table "$TABLE_NAME" \
    --by "cosine(embedding, :query)" \
    --k "$TOPK_K" \
    --query "$QUERY_VECTOR"

  run_cefas recommend \
    --table "$TABLE_NAME" \
    --by "cosine(embedding, :query)" \
    --query "$QUERY_VECTOR" \
    --candidate-limit 10 \
    --filter "status = '${STATUS_ACTIVE}'" \
    --limit "$TOPK_K" \
    --lambda 0.7 \
    --dedup-key category \
    --dedup-ttl-seconds 3600 \
    --freqcap-key category \
    --freqcap-limit 10 \
    --freqcap-window-seconds 3600
fi

if [[ "$RUN_STREAMS" != "0" && "$RUN_STREAMS" != "false" ]]; then
  if [[ "$RUN_STREAMS" == "1" || "$RUN_STREAMS" == "true" ]]; then
    streams_json="$(run_cefas list-streams --table-name "$TABLE_NAME" --limit 10)"
  elif ! streams_json="$(capture_cefas list-streams --table-name "$TABLE_NAME" --limit 10 2>/dev/null)"; then
    if [[ "$RUN_STREAMS" == "1" || "$RUN_STREAMS" == "true" ]]; then
      exit 1
    fi
    echo "# Streams: list-streams indisponivel neste servidor; pulando validacao de streams." >&2
    streams_json=""
  fi
fi

if [[ -n "${streams_json:-}" ]]; then
  echo "$streams_json"
  stream_arn="$(printf '%s' "$streams_json" | jq -r '.Streams[0].StreamArn // empty')"
  if [[ -z "$stream_arn" ]]; then
    echo "Erro: list-streams nao retornou StreamArn para $TABLE_NAME" >&2
    exit 1
  fi

  stream_desc_json="$(run_cefas describe-stream --stream-arn "$stream_arn" --limit 10)"
  echo "$stream_desc_json"
  shard_id="$(printf '%s' "$stream_desc_json" | jq -r '.StreamDescription.Shards[0].ShardId // empty')"
  if [[ -z "$shard_id" ]]; then
    echo "Erro: describe-stream nao retornou ShardId para $stream_arn" >&2
    exit 1
  fi

  iterator_json="$(run_cefas get-shard-iterator \
    --stream-arn "$stream_arn" \
    --shard-id "$shard_id" \
    --shard-iterator-type TRIM_HORIZON)"
  echo "$iterator_json"
  shard_iterator="$(printf '%s' "$iterator_json" | jq -r '.ShardIterator // empty')"
  if [[ -z "$shard_iterator" ]]; then
    echo "Erro: get-shard-iterator nao retornou ShardIterator" >&2
    exit 1
  fi

  run_cefas get-records \
    --shard-iterator "$shard_iterator" \
    --limit "$STREAM_LIMIT"
fi

run_cefas delete-item \
  --table-name "$TABLE_NAME" \
  --key "$KEY_PROFILE"

echo "# Smoke test concluido."
