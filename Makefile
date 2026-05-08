SHELL             := /bin/bash
BIN_DIR           := bin
BIN               := $(BIN_DIR)/tether
PKG               := github.com/LinZiyang666/tether
VERSION           ?= v0.0.0-dev
LDFLAGS           := -s -w -X $(PKG)/internal/proto.ReleaseVersion=$(VERSION)
# golangci-lint v2 is required because golangci-lint v1.x is built with Go 1.23
# and refuses to lint a Go 1.25 module ("language version ... is lower than the
# targeted Go version"). go.mod is at Go 1.25 because nats-io/jwt/v2 needs it.
GOLANGCI_VERSION  ?= v2.5.0

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
	@command -v curl >/dev/null || { echo "error: curl is required for tools install"; exit 1; }
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
	  | sh -s -- -b $$(go env GOPATH)/bin $(GOLANGCI_VERSION)

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
