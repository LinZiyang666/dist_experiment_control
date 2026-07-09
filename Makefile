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

.PHONY: all build test e2e lint tools tidy clean nats-server-install nats-dev

all: build

build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/tether

test:
	go test ./...

# P11 / architecture line 2141: P2-P10 e2e suites are the regression net for cross-phase
# behavior. `make e2e` runs the whole matrix via test/e2e/all_phases_test.go (TestAllPhases =
# subtest per phase, SERIAL; each DN/leaf matrix = its own -race subprocess, SERIAL). Serial is
# the documented release-gate posture: the heavy -race clustered-JS / raft / PTY subprocesses
# are timing-sensitive and starve under contention — parallelizing them was tried, measured, and
# reverted (the clustered-JS matrices flake "routed JS server not ready" under any concurrent
# heavy matrix; see the note in all_phases_test.go). For fast LOCAL iteration run the ONE suite
# you touched: `go test ./test/pX/...`, or `go test -tags dN_integration -race ./test/dN/`. The
# test is gated by the e2e_matrix build tag so a bare `go test ./...` doesn't recursively fork.
# -timeout 20m: the matrix runs SERIALLY in ONE test binary (all_phases + the D-matrices), so the whole
# suite shares the OUTER go-test deadline — NOT the default 10m, which the ~10min serial runtime tips over
# on a loaded CI runner (each subtest still has its own inner timeout: phaseTimeout for the forked phase
# subprocesses, per-suite deadlines for the D-matrices — so a genuine hang is still caught quickly).
e2e:
	go test -count=1 -tags e2e_matrix -timeout 20m -v ./test/e2e/...

lint:
	@GOPATH_BIN="$$(go env GOPATH)/bin/golangci-lint"; \
	  if [ -x "$$GOPATH_BIN" ]; then LINT="$$GOPATH_BIN"; \
	  else LINT="$$(command -v golangci-lint 2>/dev/null)"; fi; \
	  test -n "$$LINT" && test -x "$$LINT" || { echo "error: golangci-lint not found. Run: make tools"; exit 1; }; \
	  "$$LINT" run

# Build golangci-lint from source with the LOCAL Go toolchain (Go 1.25) via the
# module proxy. This avoids both (a) the prebuilt v1.x binary that refuses Go
# 1.25 modules and (b) a network dependency on raw.githubusercontent.com (the
# install.sh host), which may be firewalled.
tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)

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
