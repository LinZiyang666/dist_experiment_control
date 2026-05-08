SHELL             := /bin/bash
BIN_DIR           := bin
BIN               := $(BIN_DIR)/tether
PKG               := github.com/LinZiyang666/tether
VERSION           ?= v0.0.0-dev
LDFLAGS           := -s -w -X $(PKG)/internal/proto.ReleaseVersion=$(VERSION)
GOLANGCI_VERSION  ?= v1.62.2

.PHONY: all build test lint tools tidy clean

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

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR)
