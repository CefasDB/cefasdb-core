MODULE := github.com/CefasDb/cefasdb
COVERAGE_FILE := cover.out
GOBIN := $(shell go env GOPATH)/bin
BIN_DIR := ./bin
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X main.Version=$(VERSION)"

.PHONY: help build server cli install clean fmt lint vet test cover mut sec bench helm-test k8s-resilience tools ci

help: ## List available targets.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: server cli ## Build the cefasdb server and the cefas CLI into ./bin.

server: ## Build the cefasdb server into ./bin/cefasdb.
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/cefasdb ./cmd/cefasdb

cli: ## Build the cefas CLI into ./bin/cefas.
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/cefas ./cmd/cefasctl

install: build ## Install both binaries into $GOBIN.
	go install $(LDFLAGS) ./cmd/cefasdb ./cmd/cefasctl

clean: ## Remove built binaries and the coverage profile.
	rm -rf $(BIN_DIR) $(COVERAGE_FILE)

tools: ## Install developer tools used by other targets.
	go install mvdan.cc/gofumpt@latest
	go install github.com/daixiang0/gci@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/vladopajic/go-test-coverage/v2@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	go install github.com/google/osv-scanner/cmd/osv-scanner@latest

fmt: ## Format the code (gofumpt + gci + goimports).
	@command -v gofumpt >/dev/null 2>&1 || go install mvdan.cc/gofumpt@latest
	@command -v gci >/dev/null 2>&1 || go install github.com/daixiang0/gci@latest
	@command -v goimports >/dev/null 2>&1 || go install golang.org/x/tools/cmd/goimports@latest
	gofumpt -w .
	gci write --skip-generated -s standard -s default -s "prefix($(MODULE))" .
	goimports -w .

vet: ## Run go vet.
	go vet ./...

lint: ## Run golangci-lint.
	@command -v golangci-lint >/dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run --timeout=5m

test: ## Run race + shuffle tests with atomic coverage.
	go test -race -count=1 -shuffle=on -covermode=atomic -coverpkg=./... -coverprofile=$(COVERAGE_FILE) ./...

cover: test ## Enforce coverage thresholds from .testcoverage.yml.
	@command -v go-test-coverage >/dev/null 2>&1 || go install github.com/vladopajic/go-test-coverage/v2@latest
	go-test-coverage --config .testcoverage.yml

mut: ## Run mutation tests (gremlins).
	@command -v gremlins >/dev/null 2>&1 || go install github.com/go-gremlins/gremlins/cmd/gremlins@latest
	gremlins unleash --tags='!integration'

sec: ## Run govulncheck, gosec, osv-scanner.
	@command -v govulncheck >/dev/null 2>&1 || go install golang.org/x/vuln/cmd/govulncheck@latest
	@command -v gosec >/dev/null 2>&1 || go install github.com/securego/gosec/v2/cmd/gosec@latest
	@command -v osv-scanner >/dev/null 2>&1 || go install github.com/google/osv-scanner/cmd/osv-scanner@latest
	govulncheck ./...
	gosec -severity=medium -confidence=medium ./...
	osv-scanner --lockfile go.mod

bench: ## Run benchmarks across all packages.
	go test -run=^$$ -bench=. -benchmem ./...

helm-test: ## Render-test Helm resilience profiles.
	scripts/test_helm_resilience.sh

k8s-resilience: ## Run the Kubernetes resilience suite in dry-run mode by default.
	scripts/k8s_resilience_suite.sh

ci: vet lint test cover sec ## Full quality gate (mirror of CI workflow).
