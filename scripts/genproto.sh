#!/usr/bin/env bash
# Regenerate cefas gRPC stubs.
#
# Expects protoc, protoc-gen-go, protoc-gen-go-grpc on PATH. Install:
#   brew install protobuf
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

protoc \
  --proto_path=pkg/protocol \
  --go_out=. \
  --go_opt=module=github.com/osvaldoandrade/cefas \
  --go-grpc_out=. \
  --go-grpc_opt=module=github.com/osvaldoandrade/cefas \
  pkg/protocol/cefas.proto

echo "generated:"
ls -1 pkg/protocol/*.pb.go
