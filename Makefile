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

.PHONY: all build test e2e-one lint tools tidy clean nats-server-install nats-dev e2e-parallel

all: build

build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/tether

test:
	go test ./...

# P11 / architecture line 2141: P2-P10 e2e suites are the regression net for cross-phase
# behavior. THE GATE IS `make e2e-parallel`. Green there means done; do not re-run serially to
# "confirm".
#
# DO NOT RUN THE FULL SERIAL MATRIX. Not a preference — the full serial run has been the gate for
# years and caught NONE of the four defect classes that a loaded parallel run exposed: 17 raft
# timings 7-20x shorter than production (one 25ms leader lease), two "observed leadership then
# assumed it held" races in test/d3 and test/d7, and a port TOCTOU in the tunnel harness. See
# docs/reviews/parallel-flake-rootcause.md. Eighteen minutes that finds nothing, while leaving
# everyone believing the matrix was verified, is a ritual, not a safety net. The parallel runner
# additionally refuses to start when any top-level test has no unit and fails the round when the
# scheduled and reported sets differ — the serial runner has neither check.
#
# Serial is for ONE thing: isolating a single test the parallel run flagged. `go test
# ./test/pX/...`, `go test -tags dN_integration -race ./test/dN/`, `-run TestXxx`. Stop there;
# do not escalate to the whole matrix. The parallel runner takes -run too and marks the round
# PARTIAL RUN so a subset is never mistaken for a gate.
#
# A separate older note said parallelizing "was tried, measured, and reverted" because the
# clustered-JS matrices flake under a concurrent heavy matrix. Those flakes were real and the
# causal story was wrong (it blamed starvation on a 97.5%-idle box; the mechanism is NUMA
# scheduling) — but the operational conclusion it reached was right. The
# test is gated by the e2e_matrix build tag so a bare `go test ./...` doesn't recursively fork.
# -timeout 20m: the matrix runs SERIALLY in ONE test binary (all_phases + the D-matrices), so the whole
# suite shares the OUTER go-test deadline — NOT the default 10m, which the ~10min serial runtime tips over
# on a loaded CI runner (each subtest still has its own inner timeout: phaseTimeout for the forked phase
# subprocesses, per-suite deadlines for the D-matrices — so a genuine hang is still caught quickly).
# THE FULL SERIAL TARGET IS GONE. There used to be an `e2e:` here that ran the whole matrix in
# one serial binary. It is not deprecated, not discouraged, not "kept for emergencies" — it is
# removed, because a target that exists gets run, and this one cost eighteen minutes to find
# nothing while leaving everyone convinced the matrix had been verified.
#
# What replaces it: `make e2e-parallel` for the gate, `make e2e-one T=<TestName>` below to
# isolate a single matrix the gate flagged.
#
# e2e-one deliberately has no "all" mode. T is mandatory and unset is an error, so getting the
# old behaviour back requires typing a regex that matches everything — an act nobody performs by
# accident, which is the entire point.
e2e-one:
	@test -n "$(T)" || { \
		echo "usage: make e2e-one T=<TestName>    (e.g. T=TestD5Matrix, T=TestAllPhases)"; \
		echo ""; \
		echo "There is no full-matrix serial target. The gate is 'make e2e-parallel'."; \
		echo "This target exists ONLY to isolate one matrix the gate already flagged."; \
		exit 1; \
	}
	go test -count=1 -tags e2e_matrix -timeout 20m -v -run '^$(T)$$' ./test/e2e

# e2e-parallel runs the same work concurrently, each unit pinned to its own set of
# whole physical cores on a single NUMA node (test/e2e/parallel).
#
# Final external-review configuration (strict parser fallback + automatic worker
# count): 2m42s-2m54s vs 18m21s serial on the 44-core host, 99 units, 15/15
# top-level matrices represented, three consecutive rounds ALL PASS.
#
# An earlier "2m22s / 7.9x" was retracted: it was measured while the splitter
# silently dropped TestAllPhases and with it all 11 phase suites (p1-p10, p13),
# so a third of the gate never ran. TestAllPhases alone takes 1m49s and is now
# among the slowest units — that gap IS the retracted speedup. split.go no longer
# name-filters its run-whole fallback, and the runner hard-fails at startup if any
# top-level test in the serial gate has no unit, and at the end of each round if
# any unit produced no result. Both checks exist because the loss they catch is
# silent: a run that skips work looks exactly like a run that is fast.
#
# THIS IS THE GATE. It asserts top-level coverage at startup, fails closed on
# unsupported command shapes/flags, and reconciles scheduled-vs-reported identities
# at the end. Under load it exercises the contention that actually finds bugs. The suites that once
# flaked here were not runner noise: each was a real defect in the test harness, and all
# four classes are fixed (docs/reviews/parallel-flake-rootcause.md), verified by three
# consecutive final-review rounds, 297 units, zero failures.
#
# Earlier measurements on the 44-core dev box (serial baseline 18m36s). NOTE: every
# row below predates the TestAllPhases fix, so each is missing the 11 phase suites
# and understates its true time — kept for the SHAPE of the result, not the values:
#
#   plain parallel, 10 workers            4m48s   ceiling = D5 at 4m50s
#   + package split, 10 workers           5m11s   SLOWER: `go test ./a/... ./b/...`
#                                                 already runs packages concurrently,
#                                                 so this re-does the toolchain's work
#                                                 and pays extra process overhead
#   + 8-way name sharding, 10 workers     4m09s   ceiling drops 4m37s -> 58s
#   + 8-way sharding, 20 workers          2m22s   106 units (missing TestAllPhases)
#
# The final strict-fallback configuration measures 2m42s-2m54s / 99 units / ALL PASS
# against an 18m21s serial baseline — about 6.4x using the conservative 2m54s run.
#
# The bottleneck was never between packages: internal/broker alone is 4m37s of D4's
# 4m45s, because Go runs tests WITHIN a package serially (496 tests, 0.7 cores of
# real CPU — the rest is waiting). Sharding by test name gives each shard its own
# process, which is safer than adding t.Parallel(): no shared memory, and per-test
# global state stays per-process.
#
# Flags: -workers N, -shards N, -split, -repeat N (flake hunting), -dry-run.
e2e-parallel:
	go run ./test/e2e/parallel -split -shards 8 $(E2EPAR_FLAGS)

# The version is enforced, not just recorded. golangci-lint bundles its own
# staticcheck, so a different version silently changes WHICH checks run: 2.12
# added SA1019 for parser.ParseDir (deprecated in Go 1.25) and reports an issue
# on code that 2.5.0 passes. Without this gate, `make lint` is green on one
# machine and red on another with the same commit, and whoever has the newer
# binary ends up "fixing" code to satisfy a check the repo never adopted.
# Fail closed and point at `make tools`, which installs exactly $(GOLANGCI_VERSION).
lint:
	@GOPATH_BIN="$$(go env GOPATH)/bin/golangci-lint"; \
	  if [ -x "$$GOPATH_BIN" ]; then LINT="$$GOPATH_BIN"; \
	  else LINT="$$(command -v golangci-lint 2>/dev/null)"; fi; \
	  test -n "$$LINT" && test -x "$$LINT" || { echo "error: golangci-lint not found. Run: make tools"; exit 1; }; \
	  WANT="$$(printf '%s' '$(GOLANGCI_VERSION)' | sed 's/^v//')"; \
	  HAVE="$$("$$LINT" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"; \
	  if [ "$$HAVE" != "$$WANT" ]; then \
	    echo "error: golangci-lint $$HAVE found at $$LINT, but this repo pins $$WANT."; \
	    echo "       Lint results are not comparable across versions. Run: make tools"; \
	    exit 1; \
	  fi; \
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
