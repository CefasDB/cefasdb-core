#!/usr/bin/env bash
set -euo pipefail

# Opens the Cefas CLI REPL from inside a shell-capable pod in the hack cluster.
# Defaults match the current hack cluster layout:
#   namespace: cefasdb
#   toolbox pod: cefasdb-rf3-bench
#   Cefas gRPC service: cefasdb-geo-cefas:9090

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

KUBE_CONTEXT="${KUBE_CONTEXT:-hack}"
NAMESPACE="${NAMESPACE:-cefasdb}"
TOOLBOX_POD="${TOOLBOX_POD:-cefasdb-rf3-bench}"
TOOLBOX_IMAGE="${TOOLBOX_IMAGE:-alpine:3.20}"
CREATE_TOOLBOX_POD="${CREATE_TOOLBOX_POD:-auto}"

CEFAS_ENDPOINT="${CEFAS_ENDPOINT:-cefasdb-geo-cefas:9090}"
CEFAS_OUTPUT="${CEFAS_OUTPUT:-table}"
CEFAS_TIMEOUT="${CEFAS_TIMEOUT:-30s}"
CEFAS_INSECURE="${CEFAS_INSECURE:-true}"
CEFAS_TOKEN="${CEFAS_TOKEN:-}"

TARGET_GOOS="${TARGET_GOOS:-linux}"
TARGET_GOARCH="${TARGET_GOARCH:-}"
LOCAL_CLI="${LOCAL_CLI:-}"
REMOTE_CLI="${REMOTE_CLI:-/tmp/cefas-cli}"
BUILD_CLI="${BUILD_CLI:-auto}"

LOCAL_TOKEN_FILE="${LOCAL_TOKEN_FILE:-${ROOT_DIR}/tmp/cefasdb.token}"
REMOTE_TOKEN_FILE="${REMOTE_TOKEN_FILE:-/tmp/cefasdb.token}"
COPY_TOKEN_FILE="${COPY_TOKEN_FILE:-auto}"

KUBECTL=(kubectl)
if [[ -n "$KUBE_CONTEXT" ]]; then
  KUBECTL+=(--context "$KUBE_CONTEXT")
fi

usage() {
  cat <<'EOF'
Usage:
  ./hack-cefas-cli-repl.sh

Useful overrides:
  TOOLBOX_POD=<pod>              Pod with /bin/sh and tar/chmod. Default: cefasdb-rf3-bench
  CEFAS_ENDPOINT=<host:port>     Internal gRPC endpoint. Default: cefasdb-geo-cefas:9090
  CEFAS_TOKEN=<token>            Pass bearer token directly to the CLI.
  LOCAL_TOKEN_FILE=<path>        Copied to the pod when present. Default: tmp/cefasdb.token
  TARGET_GOARCH=<arch>           Build target arch. Auto-detected, fallback arm64.
  BUILD_CLI=1                    Force rebuild the local Linux CLI before copying.
  COPY_TOKEN_FILE=0              Do not copy a token file.

Examples:
  ./hack-cefas-cli-repl.sh
  CEFAS_OUTPUT=json ./hack-cefas-cli-repl.sh
  TOOLBOX_POD=my-debug-pod CEFAS_ENDPOINT=cefasdb-geo-cefas:9090 ./hack-cefas-cli-repl.sh
EOF
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Erro: comando obrigatorio nao encontrado: $1" >&2
    exit 1
  fi
}

detect_target_arch() {
  if [[ -n "$TARGET_GOARCH" ]]; then
    return
  fi

  TARGET_GOARCH="$("${KUBECTL[@]}" get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}' 2>/dev/null || true)"
  TARGET_GOARCH="${TARGET_GOARCH:-arm64}"
}

can_create_toolbox_pod() {
  case "$CREATE_TOOLBOX_POD" in
    0 | [Ff][Aa][Ll][Ss][Ee] | [Nn][Oo] | [Oo][Ff][Ff])
      return 1
      ;;
    [Aa][Uu][Tt][Oo] | 1 | [Tt][Rr][Uu][Ee] | [Yy][Ee][Ss] | [Oo][Nn])
      return 0
      ;;
    *)
      echo "Erro: CREATE_TOOLBOX_POD invalido: $CREATE_TOOLBOX_POD" >&2
      exit 1
      ;;
  esac
}

create_toolbox_pod() {
  echo "+ criando pod toolbox $NAMESPACE/$TOOLBOX_POD com imagem $TOOLBOX_IMAGE" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" run "$TOOLBOX_POD" \
    --image="$TOOLBOX_IMAGE" \
    --restart=Never \
    --command -- sleep 2147483647
}

ensure_toolbox_pod() {
  if ! "${KUBECTL[@]}" get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "Erro: namespace nao encontrado no cluster $KUBE_CONTEXT: $NAMESPACE" >&2
    exit 1
  fi

  if ! "${KUBECTL[@]}" -n "$NAMESPACE" get pod "$TOOLBOX_POD" >/dev/null 2>&1; then
    if ! can_create_toolbox_pod; then
      echo "Erro: pod $NAMESPACE/$TOOLBOX_POD nao existe." >&2
      exit 1
    fi
    create_toolbox_pod
  else
    phase="$("${KUBECTL[@]}" -n "$NAMESPACE" get pod "$TOOLBOX_POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    reason="$("${KUBECTL[@]}" -n "$NAMESPACE" get pod "$TOOLBOX_POD" -o jsonpath='{.status.containerStatuses[0].state.terminated.reason}' 2>/dev/null || true)"
    if [[ "$phase" == "Failed" || "$phase" == "Succeeded" ]]; then
      if ! can_create_toolbox_pod; then
        echo "Erro: pod $NAMESPACE/$TOOLBOX_POD esta em estado $phase${reason:+/$reason}." >&2
        exit 1
      fi
      echo "+ removendo pod toolbox anterior $NAMESPACE/$TOOLBOX_POD em estado $phase${reason:+/$reason}" >&2
      "${KUBECTL[@]}" -n "$NAMESPACE" delete pod "$TOOLBOX_POD" --wait=true >/dev/null
      create_toolbox_pod
    fi
  fi

  echo "+ aguardando $NAMESPACE/$TOOLBOX_POD ficar Ready" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready "pod/$TOOLBOX_POD" --timeout=90s >/dev/null

  if ! "${KUBECTL[@]}" -n "$NAMESPACE" exec "$TOOLBOX_POD" -- /bin/sh -c 'command -v tar >/dev/null && command -v chmod >/dev/null' >/dev/null 2>&1; then
    cat >&2 <<EOF
Erro: $NAMESPACE/$TOOLBOX_POD precisa ter /bin/sh, tar e chmod.
Os pods cefasdb-geo-cefas-* atuais sao distroless e nao tem shell.
Use TOOLBOX_POD=cefasdb-rf3-bench ou deixe o script criar um pod Alpine.
EOF
    exit 1
  fi
}

ensure_local_cli() {
  detect_target_arch

  if [[ -z "$LOCAL_CLI" ]]; then
    LOCAL_CLI="${ROOT_DIR}/tmp/cefas-cli-${TARGET_GOOS}-${TARGET_GOARCH}"
  fi

  case "$BUILD_CLI" in
    0 | [Ff][Aa][Ll][Ss][Ee] | [Nn][Oo] | [Oo][Ff][Ff])
      if [[ ! -x "$LOCAL_CLI" ]]; then
        echo "Erro: LOCAL_CLI nao existe ou nao e executavel: $LOCAL_CLI" >&2
        exit 1
      fi
      return
      ;;
    1 | [Tt][Rr][Uu][Ee] | [Yy][Ee][Ss] | [Oo][Nn])
      ;;
    [Aa][Uu][Tt][Oo])
      if [[ -x "$LOCAL_CLI" ]]; then
        return
      fi
      ;;
    *)
      echo "Erro: BUILD_CLI invalido: $BUILD_CLI" >&2
      exit 1
      ;;
  esac

  need_cmd go
  mkdir -p "$(dirname "$LOCAL_CLI")"
  echo "+ construindo $LOCAL_CLI para ${TARGET_GOOS}/${TARGET_GOARCH}" >&2
  (
    cd "$ROOT_DIR"
    CGO_ENABLED=0 GOOS="$TARGET_GOOS" GOARCH="$TARGET_GOARCH" \
      go build -trimpath -o "$LOCAL_CLI" ./cmd/cefasctl
  )
}

copy_cli_to_pod() {
  echo "+ copiando CLI para $NAMESPACE/$TOOLBOX_POD:$REMOTE_CLI" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" cp "$LOCAL_CLI" "${TOOLBOX_POD}:${REMOTE_CLI}"
  "${KUBECTL[@]}" -n "$NAMESPACE" exec "$TOOLBOX_POD" -- chmod 0755 "$REMOTE_CLI"
}

copy_token_to_pod_if_present() {
  USE_REMOTE_TOKEN_FILE=0

  case "$COPY_TOKEN_FILE" in
    0 | [Ff][Aa][Ll][Ss][Ee] | [Nn][Oo] | [Oo][Ff][Ff])
      return
      ;;
    [Aa][Uu][Tt][Oo] | 1 | [Tt][Rr][Uu][Ee] | [Yy][Ee][Ss] | [Oo][Nn])
      ;;
    *)
      echo "Erro: COPY_TOKEN_FILE invalido: $COPY_TOKEN_FILE" >&2
      exit 1
      ;;
  esac

  if [[ -n "$CEFAS_TOKEN" ]]; then
    return
  fi

  if [[ -f "$LOCAL_TOKEN_FILE" ]]; then
    echo "+ copiando token local para $NAMESPACE/$TOOLBOX_POD:$REMOTE_TOKEN_FILE" >&2
    "${KUBECTL[@]}" -n "$NAMESPACE" cp "$LOCAL_TOKEN_FILE" "${TOOLBOX_POD}:${REMOTE_TOKEN_FILE}"
    "${KUBECTL[@]}" -n "$NAMESPACE" exec "$TOOLBOX_POD" -- chmod 0600 "$REMOTE_TOKEN_FILE" >/dev/null 2>&1 || true
    USE_REMOTE_TOKEN_FILE=1
  fi
}

open_repl() {
  echo "+ abrindo REPL no pod $NAMESPACE/$TOOLBOX_POD -> $CEFAS_ENDPOINT" >&2
  "${KUBECTL[@]}" -n "$NAMESPACE" exec -it "$TOOLBOX_POD" -- \
    env \
      CEFAS_ENDPOINT="$CEFAS_ENDPOINT" \
      CEFAS_OUTPUT="$CEFAS_OUTPUT" \
      CEFAS_TIMEOUT="$CEFAS_TIMEOUT" \
      CEFAS_INSECURE="$CEFAS_INSECURE" \
      CEFAS_TOKEN="$CEFAS_TOKEN" \
      REMOTE_CLI="$REMOTE_CLI" \
      REMOTE_TOKEN_FILE="$REMOTE_TOKEN_FILE" \
      USE_REMOTE_TOKEN_FILE="$USE_REMOTE_TOKEN_FILE" \
    /bin/sh -lc '
      set -eu
      if [ ! -x "$REMOTE_CLI" ]; then
        echo "cefas-cli nao encontrado em $REMOTE_CLI" >&2
        exit 1
      fi

      set -- --endpoint "$CEFAS_ENDPOINT" --output "$CEFAS_OUTPUT" --timeout "$CEFAS_TIMEOUT"
      case "$CEFAS_INSECURE" in
        1|true|TRUE|yes|YES|on|ON) set -- "$@" --insecure ;;
      esac
      if [ -n "${CEFAS_TOKEN:-}" ]; then
        set -- "$@" --token "$CEFAS_TOKEN"
      elif [ "$USE_REMOTE_TOKEN_FILE" = "1" ]; then
        set -- "$@" --token-file "$REMOTE_TOKEN_FILE"
      fi

      exec "$REMOTE_CLI" "$@"
    '
}

main() {
  case "${1:-}" in
    -h | --help)
      usage
      exit 0
      ;;
    "")
      ;;
    *)
      echo "Erro: argumento desconhecido: $1" >&2
      usage >&2
      exit 1
      ;;
  esac

  need_cmd kubectl
  ensure_toolbox_pod
  ensure_local_cli
  copy_cli_to_pod
  copy_token_to_pod_if_present
  open_repl
}

main "$@"
