SHELL    := /bin/bash
BIN_DIR  := bin
BIN      := $(BIN_DIR)/tether
PKG      := github.com/LinZiyang666/tether
VERSION  ?= v0.0.0-dev
LDFLAGS  := -s -w -X main.Version=$(VERSION)

.PHONY: all build test lint tidy clean

all: build

build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/tether

test:
	go test ./...

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR)
