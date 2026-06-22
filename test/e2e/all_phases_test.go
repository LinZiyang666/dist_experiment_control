//go:build e2e_matrix

// Package e2e_test runs the full P1-P10 e2e suite as one matrix
// (architecture P11 line 2141: "P2-P10 各 phase 测试合并到 test/
// e2e/, CI 每夜跑"). Implemented as subtests that go-test
// subprocess-invoke each phase package, so:
//
//   - one entry point: `go test -tags e2e_matrix ./test/e2e/...`
//     runs everything (or `make e2e` via the Makefile target);
//   - the per-phase packages stay intact for fast iteration
//     (`go test ./test/p7/...` is much shorter than `make e2e`);
//   - a hang or long e2e in one phase doesn't drag the others —
//     each subtest is its own subprocess with its own timeout;
//   - failures bubble up with the phase name in t.Run output, so
//     CI logs show which phase regressed without re-runs.
//
// Behind the e2e_matrix build tag so a bare `go test ./...` (and
// therefore `make test`) does NOT recurse into this file: this
// test forks `go test ./test/pX/...` per phase, which would
// itself re-enter `./test/e2e/...` and explode if not gated.
package e2e_test

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// phaseTimeout caps each subprocess. test/p3 is the slow one
// (~22s for the full auth_callout matrix); 90s gives 4× headroom.
const phaseTimeout = 90 * time.Second

// allPhases is the architecture-mandated set. P0 has no e2e
// (scaffold only); P1 covers proto + identity + makefile, kept
// for completeness so a stale dependency in proto immediately
// surfaces here too.
var allPhases = []string{
	"p1", "p2", "p3", "p4", "p5",
	"p6", "p7", "p8", "p9", "p10",
	"p13",
}

func TestAllPhases(t *testing.T) {
	// Phases run SERIALLY. The first cut had t.Parallel() and
	// finished in ~24s, but P5's PTY attach handshake (architecture
	// C.5.1, 3s deadline) is contention-sensitive: with 10 phase
	// subprocesses each spawning embedded NATS + multiple
	// goroutines per test, attach_timeout fires under load and
	// `make e2e` flakes. P11 is a release gate — repeatability
	// beats wall-clock. Sequential is ~80s on this box, still
	// well inside any nightly budget.
	for _, phase := range allPhases {
		t.Run(phase, func(t *testing.T) {
			runPhase(t, phase)
		})
	}
}

// TestTransferDefaultsMatrix wires the transfer-unrestrict default-inversion
// (open-by-default push/pull + explicit-`[]`-disabled off-switch) into the
// -tags e2e_matrix cross-phase regression net. The transfer e2e itself lives
// in test/cli_e2e (a `make test` hard gate); the phase matrix is otherwise a
// `./test/pN` runner and transfer is a post-1.0 feature increment, so rather
// than mint a fake phase this subprocess re-runs just the open/off-switch
// cases here. A silent re-disable of the open default thus fails `make e2e`
// too, not only `make test`.
func TestTransferDefaultsMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-count=1", "-timeout", phaseTimeout.String(),
		"-run", "TestTransfer_TierA_OpenByDefault|TestTransfer_TierB_OpenByDefault|TestTransfer_TierA_OffSwitchDisables",
		"./test/cli_e2e/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("transfer defaults failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// TestRemoteFSMatrix folds the remote-fs-resilience leaf increment
// (docs/reviews/remote-fs-resilience-plan.md) into the -tags e2e_matrix
// regression net. Like TestTransferDefaultsMatrix, rather than mint a fake
// phase it re-runs the feature's hermetic suites as a subprocess (under -race,
// since the spawn watchdog + sticky/self-healing probe are concurrency
// surfaces): a regression in PATH sanitization, the abandon/ceiling watchdog,
// the broker Safe round-trip, or Component I thus fails `make e2e` too, not just
// `make test`.
// TestProxyDialMatrix runs the post-1.0 proxy-aware-dial leaf under -race, like
// the other leaf matrices (the dialer is invoked concurrently on reconnect, so
// the plan mandates -race — which the no-race runPhase default would not give).
// The integration test (real TLS nats-server reached through a fake CONNECT
// proxy) also guards the load-bearing "CustomDialer carries TCP before the
// TLS/WebSocket handshake" fact against a nats.go createConn-order regression.
func TestProxyDialMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", phaseTimeout.String(),
		"./internal/proxydial/...", "./test/proxydial/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("proxydial matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// TestD1Matrix folds the distributed-broker D1 state layer (docs/reviews/
// d1-plan.md) into the -tags e2e_matrix regression net under -race. D1 is the
// consensus heart (Raft FSM + SQLite Apply + snapshot/restore), and its load-
// bearing gate is the §13.4 real-SIGKILL kill-9 crash-consistency matrix — a
// subprocess test that self-forks the (race-instrumented) test binary, so it
// needs its OWN budget well beyond the 90s phase cap and must NOT go through the
// no-race allPhases runPhase. A regression in the same-txn applied_index
// invariant, the idempotent re-apply, the online-backup snapshot/restore, or the
// WAL-concurrency path thus fails `make e2e` too, not only `make test`.
func TestD1Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "240s",
		"./internal/cluster/...", "./test/cluster/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d1 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// TestD2Matrix runs the D2 op-set / Plan-Apply migration surface under -race: the
// mutator packages (per-op Plan* + the equivalence/differential harnesses), the
// cluster FSM, and the determinism CHA Apply-reachability lint. Kept as a dedicated
// -race subprocess (not in allPhases) like TestD1Matrix.
func TestD2Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "300s",
		"./internal/cluster/...", "./internal/port/...", "./internal/proc/...",
		"./internal/node/...", "./internal/session/...", "./internal/agentprov/...",
		"./test/cluster/...", "./test/determinism/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d2 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

func TestRemoteFSMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))

	run := func(label string, args ...string) {
		base := []string{"test", "-race", "-count=1", "-timeout", phaseTimeout.String()}
		cmd := exec.Command("go", append(base, args...)...)
		cmd.Dir = repoRoot
		var buf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &buf, &buf
		if err := cmd.Run(); err != nil {
			t.Fatalf("remote-fs %s failed: %v\n--- output ---\n%s", label, err, buf.String())
		}
	}

	// Whole feature-core packages (review F13: pty abandon-recover, the
	// test/concurrency spawnsafe gate, and the cmd/tether config tests must run
	// under -race here, not only spawnsafe).
	run("spawnsafe", "./internal/spawnsafe/...")
	run("pty", "./internal/pty/...")
	run("concurrency", "-run", "Spawnsafe", "./test/concurrency/...")
	run("cmd-config", "-run", "RemoteFS|AgentYAML_remoteFS|ParseOptDuration|SafeFlagOrdering", "./cmd/tether/...")
	// Targeted wiring cases across agent + proto + p4.
	run("wiring",
		"-run", "RemoteFS|BuildExecCmd|BoundedHomeRead|StateStore_loadNoLock|StartBounded|ReplayPortsFromState|AgentNew_rejectsBadRemoteFSMode|SafeField|SafeReqRoundTrips",
		"./internal/agent/...", "./internal/proto/...", "./test/p4/...")
}

// TestProxyTunnelReconnectMatrix folds the proxy-tunnel-reconnect leaf
// increment (docs/reviews/proxy-tunnel-reconnect-plan.md) into the
// -tags e2e_matrix regression net. The data-plane reconnect supervisor +
// readiness-liveness seam are concurrency surfaces, so the subset runs under
// -race: a regression in the reconnect loop (DENY-terminal taxonomy, transient
// retry, ctx-cancel, no-leak / no-double-owner), the broker token-lookup
// transient split (Fix C), the repairProxy not-ready suppression (Fix D), or
// the agent readiness hook (Fix B) thus fails `make e2e` too, not only `make test`.
func TestProxyTunnelReconnectMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))

	run := func(label string, args ...string) {
		base := []string{"test", "-race", "-count=1", "-timeout", phaseTimeout.String()}
		cmd := exec.Command("go", append(base, args...)...)
		cmd.Dir = repoRoot
		var buf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &buf, &buf
		if err := cmd.Run(); err != nil {
			t.Fatalf("proxy-tunnel-reconnect %s failed: %v\n--- output ---\n%s", label, err, buf.String())
		}
	}

	// Fix A: tunnel reconnect mechanics (drop→rebind, DENY taxonomy, ctx-cancel,
	// churn no-leak, reconnect-vs-Open race) — the whole reconnect surface.
	run("tunnel", "-run", "Reconnect|Deny", "./internal/tunnel/...")
	// Fix C + Fix D: broker token-lookup transient split + repairProxy gate.
	run("broker", "-run", "TunnelTokenLookup|RepairProxy", "./internal/broker/...")
	// Fix B: agent readiness hook (port filter + lock-order deadlock guard).
	run("agent", "-run", "ProxyReadinessHook", "./internal/agent/...")
	// Full-stack: real TunnelExposeAdapter + broker tunnel server + a severable
	// relay — false-online recovery after a data-plane drop, and the disable-
	// during-drop no-resurrection security invariant.
	run("p13-fullstack", "-run", "FalseOnlineRecoversAfterTunnelDrop|DisableDuringTunnelDropStaysDown", "./test/p13/...")
}

func runPhase(t *testing.T, phase string) {
	t.Helper()
	// go test sets cwd to the package dir (test/e2e/), so the
	// `./test/pX/...` path the subprocess receives must be
	// resolved against the repo root, not the test pkg dir. Climb
	// two levels off this source file's path to find the repo.
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))

	// -count=1 disables go's test cache so each `make e2e` run is
	// a real exercise, not a cached PASS replay (architecture
	// P11 acceptance: "CI 夜跑稳定 ≥ 7 天" — we want each night
	// to actually re-run, not stamp the same cached pass).
	cmd := exec.Command("go", "test", "-count=1", "-timeout", phaseTimeout.String(),
		"./test/"+phase+"/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("phase %s failed: %v\n--- output ---\n%s", phase, err, buf.String())
	}
}
