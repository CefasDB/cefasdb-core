# Multi-stage build: pin the Go toolchain that produced our module
# graph, then copy a static binary into a minimal runtime image.
FROM golang:1.25-alpine AS build
WORKDIR /src

# Dependencies first for cache efficiency.
COPY go.mod go.sum ./
RUN go mod download

# Then the source tree.
COPY . .

# CGO is off so the resulting binary is statically linked and the
# runtime image stays distroless-thin.
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/cefas-server ./cmd/cefas-server

# Runtime: distroless static. tini handles signals; we still
# trap SIGTERM in the binary for graceful shutdown.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/cefas-server /usr/local/bin/cefas-server

# Default data directory is bind-mounted by docker-compose / k8s; we
# only declare it here so anyone running `docker run` knows.
VOLUME ["/var/lib/cefas"]

# Sensible default: serve HTTP on 8080, gRPC on 9090, metrics on
# the HTTP listener.
EXPOSE 8080 9090

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/cefas-server"]
CMD ["-data", "/var/lib/cefas", "-http", ":8080", "-grpc", ":9090"]
