# syntax=docker/dockerfile:1.6

# Multi-stage build: pin the Go toolchain that produced our module
# graph, then copy a static binary into a minimal runtime image.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src

# Dependencies first for cache efficiency.
COPY go.mod go.sum ./
COPY third_party/identity-middleware/go.mod third_party/identity-middleware/go.sum ./third_party/identity-middleware/
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Then the source tree.
COPY . .

# CGO is off so the resulting binary is statically linked and the
# runtime image stays distroless-thin.
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/cefasdb ./cmd/cefasdb

RUN mkdir -p /out/var/lib/cefas && touch /out/var/lib/cefas/.keep

# Runtime: distroless static. tini handles signals; we still
# trap SIGTERM in the binary for graceful shutdown.
FROM gcr.io/distroless/static:nonroot

ARG VERSION=dev
ARG COMMIT=unknown

LABEL org.opencontainers.image.source="https://github.com/osvaldoandrade/cefas" \
      org.opencontainers.image.title="cefas" \
      org.opencontainers.image.description="cefas database server" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=build /out/cefasdb /usr/local/bin/cefasdb
COPY --from=build --chown=65532:65532 /out/var/lib/cefas /var/lib/cefas

# Default data directory is bind-mounted by docker-compose / k8s; we
# only declare it here so anyone running `docker run` knows.
VOLUME ["/var/lib/cefas"]

# Sensible default: serve HTTP on 8080, gRPC on 9090, metrics on
# the HTTP listener.
EXPOSE 8080 9090

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/cefasdb"]
CMD ["-data", "/var/lib/cefas", "-http", ":8080", "-grpc", ":9090"]
