SHELL             := /bin/bash
BIN_DIR           := bin
BIN               := $(BIN_DIR)/tether
PKG               := github.com/LinZiyang666/tether
VERSION           ?= v0.0.0-dev
LDFLAGS           := -s -w -X $(PKG)/internal/proto.ReleaseVersion=$(VERSION)
GOLANGCI_VERSION  ?= v1.62.2

.PHONY: all build test lint tools tidy clean nats-server-install nats-dev

all: build

build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/tether

test:
	go test ./...

lint:
	@command -v golangci-lint >/dev/null || { \
	  echo "error: golangci-lint not found. Run: make tools"; exit 1; }
	golangci-lint run

tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_VERSION)

# nats-server is a development-time dependency (only needed to manually exercise
# `tether serve` against a real broker; the Go test suite uses an embedded
# server via nats-server/v2/test and does NOT need this binary).
NATS_SERVER_VERSION ?= v2.10.22

nats-server-install:
	go install github.com/nats-io/nats-server/v2@$(NATS_SERVER_VERSION)

nats-dev:
	@command -v nats-server >/dev/null || { \
	  echo "error: nats-server not found. Run: make nats-server-install"; exit 1; }
	nats-server -js -DV

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR)
