//go:build e2e_matrix

// Package e2e_test runs the full P1-P10 e2e suite as one matrix
// (architecture P11 line 2141: "P2-P10 各 phase 测试合并到 test/
// e2e/, CI 每夜跑"). Implemented as subtests that go-test
// subprocess-invoke each phase package, so:
//
//   - one entry point: `go test -tags e2e_matrix ./test/e2e/...`
//     runs everything (or `make e2e-parallel` via the Makefile target);
//   - the per-phase packages stay intact for fast iteration
//     (`go test ./test/p7/...` is much shorter than `make e2e-parallel`);
//
// KNOWN REDUNDANCY, registered rather than fixed (S3 §9.6 / line-2 plan E5).
//
// The D-matrices re-run whole packages that other matrices already ran: internal/cluster is executed
// once inside each of five D suites and internal/broker twice, which L07-F5 measured at roughly 940
// redundant test-function executions per full matrix. S3 looked at de-duplicating it and declined --
// not because the redundancy is imaginary, but because each D suite runs under a DIFFERENT `-tags`
// set, so two runs of "the same" package are two different build configurations and dropping one
// silently narrows coverage. Verifying which pairs are genuinely identical costs more than the minutes
// it would save, and the parallel runner already absorbs most of the wall-clock.
//
// It is written down here because an unregistered known redundancy gets rediscovered: the next person
// to profile the matrix finds the same 940 and repeats the same investigation. If it is ever revisited,
// the thing to check FIRST is whether the -tags sets actually select the same files.
//
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

// NOTE on speed — CORRECTED 2026-07 (the 2026-06 note below was an attribution error).
//
// The original note said parallelism "was tried + measured and REVERTED" because D5/D8 starve
// their embedded-JS meta-group formation ("routed JS server not ready") when a concurrent matrix
// shares the machine, flaking at -parallel 2 (D8) and 4 (D5). The FLAKES were real. The cause
// was not resource starvation:
//
//   - a full serial e2e run leaves this host 97.5% CPU-idle, disks at 0% util, 196 GB RAM free;
//   - what -parallel changes is SCHEDULING, not supply. GOMAXPROCS bounds how many threads run,
//     never WHERE they run, so two test processes still interleave across both sockets — and
//     cross-NUMA access here costs 2.1x local (distance 21 vs 10). Lock-heavy raft/JS handshakes
//     with hard deadlines are precisely the workload that notices.
//
// With each matrix pinned to an exclusive set of whole physical cores on ONE NUMA node
// (test/e2e/parallel, `make e2e-parallel`), measured on this 44-core box:
//
//   serial                                  18m21s
//   strict fallback + 8-way shards, auto     2m54s   ~6.4x, 99 units, ALL PASS,
//                                                    coverage self-check 15/15
//
// An earlier version of this table read 2m22s / 7.9x. It was wrong, and how it was
// wrong is the useful part: test/e2e/parallel required a "Matrix" name suffix on
// its run-whole fallback, and TestAllPhases keeps its exec.Command in runPhase() —
// a different function — so it parsed to nothing, matched no suffix, and was
// dropped. Every one of those runs skipped all 11 phase suites and finished fast
// and green. TestAllPhases takes 1m49s on its own; that is most of the difference
// between the two numbers. The fallback no longer name-filters, and the runner now
// refuses to start unless every top-level test here has a unit.
//
// The per-package split being SLOWER is the useful measurement: `go test ./a/... ./b/...`
// already runs its packages concurrently, so splitting them re-implements what the
// toolchain does and adds process overhead. The real ceiling was inside one package —
// internal/broker is 4m37s of D4's 4m45s, because Go runs tests within a package
// serially (496 tests at 0.7 cores of real CPU; the rest is waiting). Sharding by test
// NAME moves it: each shard is its own process, which is safer than adding t.Parallel()
// since nothing is shared and per-test globals stay per-process.
//
// D8 passed at 28s/36s and D5 at 4m50s/4m44s — the two suites the note said could not tolerate
// 2-way and 4-way.
//
// The matrices that flaked under the parallel runner turned out to share ONE defect, and calling
// it "the d7 harness" (as this note first did) understated it by a factor of seventeen: every
// raft-driving suite in the repo had invented its own timings — 50ms, 60ms, 80ms, 100ms, 150ms,
// with one 25ms leader lease — against a production heartbeat of 1000ms and a lease of 500ms.
// d7 was simply the first one caught. All 17 sites now reference
// cluster.Multinode{Heartbeat,Election,LeaderLease}Timeout, and
// test/determinism.TestRaftTimingsUseProductionConstants fails the build if a new one appears.
//
// The mechanism, using d7 as the example: it drove raft with a 60ms heartbeat timeout while the
// suite is required to run under -race, which slows memory access 5-10x. A GC pause was enough to unseat
// the leader mid-AddNode. It was first raised to 300ms, which took d7 from ~1/7 failures to 6/6
// green serially and 30/30 across two full parallel rounds; external review M4 then pointed out
// that 300ms is still a third of production's MultinodeHeartbeatTimeout, i.e. a harness tuned to
// the edge of what -race allows rather than to what production actually does, so it now matches
// production at 1000ms. Runtime is unchanged either way — the timeout bounds failure detection,
// not the happy path. Running the matrices concurrently is what made this frequent enough to
// diagnose at all; three earlier attempts had each treated a symptom (wait longer, re-elect,
// transfer leadership back) without asking why leadership kept moving.
//
// This is the same shape as simcluster's "drills must run serially", which was believed for a
// long time and turned out to be fs.inotify.max_user_instances exhaustion.
//
// `make e2e-parallel` IS the gate (CLAUDE.md §5). Running this whole matrix serially is
// forbidden: it was the gate for years and caught none of the four defect classes a loaded
// parallel run exposed. Use this file's suites individually (`go test ./test/pX/...`) only to
// isolate something the parallel run already flagged.

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
	// attach_timeout fires. Repeatability beats wall-clock HERE,
	// inside one matrix: the parallelism that matters is between
	// matrices, and test/e2e/parallel provides it — this unit is
	// scheduled onto a wide worker precisely so its serial phases
	// each get enough machine. Sequential is ~80s on this box.
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
// cases here. A silent re-disable of the open default thus fails `make e2e-parallel`
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
// the broker Safe round-trip, or Component I thus fails `make e2e-parallel` too, not just
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
// WAL-concurrency path thus fails `make e2e-parallel` too, not only `make test`.
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

// TestD3Matrix runs the D3 NATS-cluster-layer surface under -race: the mTLS raft
// transport + fail-closed predicate (internal/cluster), the conf renderer + parser +
// reconciler (internal/natsconf), the RF1 ACL templates + proto SSOT (internal/auth,
// internal/proto), the handler seams (internal/authcallout), and the real ≥2-node
// behavioral suite (test/d3). Dedicated -race subprocess like TestD1/D2Matrix; the
// explicit 300s timeout overrides the suite default for the cluster election waits.
//
// B5 COVERAGE NOTE (roadmap §7.6 requires this to be stated, not assumed).
// The glob below said `./internal/natscluster/...` until that package merged into
// internal/natsconf. The two failure directions are NOT symmetric and that is why this
// note exists: leaving the stale path is LOUD (`FAIL [setup failed]`, exit 1), but
// DELETING it without adding the successor is SILENT — `make test` still covers the
// package, `go vet` passes, and the parallel runner's scheduled-vs-reported
// reconciliation compares identities WITHIN each unit, both derived from this same
// command, so a shrunk package list reconciles cleanly.
//
// So: `./internal/natsconf/...` replaces it and ALSO newly brings in the parser +
// reconciler surface, which previously ran only under `make test` (no -race). Coverage
// strictly INCREASES. Verified by comparing the -race matrix's package set and its test
// function set before and after; the diff is additions only.
func TestD3Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "300s",
		"./internal/cluster/...", "./internal/natsconf/...", "./internal/authcallout/...",
		"./internal/auth/...", "./internal/proto/...", "./test/d3/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d3 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// TestD4Matrix runs the D4 write-forwarding surface under -race: the ReqID dedup
// ledger + appliedDedup + GC (internal/cluster), the self-sufficient ReconcileBatch +
// byte-identical replay (internal/proc), the live-vs-op audit equivalence + forward
// adapter (internal/broker), and the combined routed-NATS + mTLS-raft forwarding suite
// (test/d4). Dedicated -race subprocess like TestD1/D2/D3Matrix; the 300s timeout
// covers the multi-node election + leadership-transfer waits.
func TestD4Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "300s",
		"./internal/cluster/...", "./internal/proc/...", "./internal/broker/...",
		"./internal/storage/...", "./test/d4/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d4 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// TestD5Matrix runs the D5 re-derivable-audit-publish + JS replica-reconfig surface under
// -race: the replicated audit-publish cursor + read primitives (internal/cluster), the
// replica-factor helper + reconfig (internal/jsstream), the leader-only publisher loop +
// dedup-id keying + AllAtTarget predicate (internal/broker), and the combined routed-NATS +
// clustered-JetStream + mTLS-raft behavioral suite (test/d5). Dedicated -race subprocess
// like TestD1/D2/D3/D4Matrix; the 300s timeout covers the JS-meta-group + election waits.
func TestD5Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	// -tags d5_integration builds the HEAVY clustered-JetStream behavioral suite (test/d5):
	// it is gated out of the parallel `make test` (where ~30 concurrent package binaries
	// starve the embedded JS clusters into timeouts) and runs only here, in its own
	// dedicated -race subprocess (uncontended). The cheap d5 guard/window tests are NOT
	// gated and run in `make test` too.
	cmd := exec.Command("go", "test", "-race", "-count=1", "-tags", "d5_integration", "-timeout", "300s",
		"./internal/cluster/...", "./internal/proc/...", "./internal/jsstream/...",
		"./internal/broker/...", "./test/d5/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d5 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

func TestPhaseFluidityMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	// -tags phasefluidity_integration builds the v0.4.2 phase-fluidity LIFECYCLE drill (external-review
	// R5): a failed-join staged nonvoter survives a leader kill/restart and stays online-removable
	// (never a force-single dead end) — the gap the sequential unit tests could not reach. Gated out of
	// `make test` (a real raft node + restart); runs here in its own -race subprocess. The companion
	// real-mTLS rebind + shrink→regrow drills ride TestD5Matrix / TestD9Matrix (their d5/d9 tags +
	// ./test/d5/... / ./test/d9/... globs already build them).
	cmd := exec.Command("go", "test", "-race", "-count=1", "-tags", "phasefluidity_integration", "-timeout", "120s",
		"-run", "TestPhaseFluidityLifecycle", "./internal/broker/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("phase-fluidity matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

func TestD6Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	// -tags d6_integration builds the D6 data-plane end-to-end suite (test/d6):
	// real tunnel servers (stable pinned certs) + a real agent tunnel.Client +
	// real broker tunnelTokenLookups over a shared replicated-state DB, proving
	// ladder enforcement / rehome failover / cert pinning / catch-up. It is gated
	// out of the parallel `make test` and runs only here in its own -race
	// subprocess; the cheap d6 guard/ladder/cert-verify tests run in `make test`.
	cmd := exec.Command("go", "test", "-race", "-count=1", "-tags", "d6_integration", "-timeout", "300s",
		"./test/d6/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d6 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

func TestD7Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	// -tags d7_integration builds the D7 cluster-lifecycle suite (test/d7): a real
	// multi-node raft cluster proving the two-phase membership change (dynamic
	// AddVoter + roster replication to the follower), the forged-sig POISON-SKIP read
	// on a FOLLOWER's DB (never panics, stays live), the no-silent-fork half-state,
	// and force-single->recover (kill the cluster, recover a survivor, restart
	// writable with no applied_index regression). Gated out of the parallel
	// `make test` (real raft elections starve under contention) and run only here in
	// its own -race subprocess; the cheap d7 op/applier/offline/guard/status tests run
	// in `make test`.
	cmd := exec.Command("go", "test", "-race", "-count=1", "-tags", "d7_integration", "-timeout", "300s",
		"./test/d7/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d7 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

func TestD8Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	// -tags d8_integration builds the D8 distributed-transfer ‖ replicated-alerts suite
	// (test/d8): a real routed-NATS + clustered-JetStream + mTLS-raft cluster proving the
	// leader-gated alert reconcile loop (replication_degraded raise/clear REPLICATES; a
	// transient unobserved pass never false-clears), the cluster-level ack replicating with
	// the authenticated actor, re-derivable transfer audit (OpTransferAudit replays exactly
	// once across an election via the q<reqID>:xfer dedup), the VerifyLeader-confirmed
	// cluster-health gate (no false-positive on a healthy cluster), and EXIT-A (a completed
	// tier-B object at R=n survives killing its home broker). Gated out of the parallel
	// `make test` (clustered JS + raft elections starve under contention) and run only here
	// in its own -race subprocess; the cheap d8 guard + the alert/audit/gate unit tests run
	// in `make test`.
	cmd := exec.Command("go", "test", "-race", "-count=1", "-tags", "d8_integration", "-timeout", "300s",
		"./test/d8/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d8 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

func TestD9Matrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	// -tags d9_integration builds the D9 production-cutover suite (test/d9): it seeds +
	// bootstraps a single-voter cluster DB via clusteroffline.InitFromExisting, then starts a
	// broker in CLUSTER MODE (the real serve.go path: detect → cluster.NewProduction over mTLS
	// raft + §15 secrets → attach every seam → leader-gated loops) against an embedded
	// JetStream NATS, and proves the cutover stands up (the single voter leads) AND an
	// authoritative control write (session.create) routes through node.Propose, commits to the
	// FSM-owned WAL, and a duplicate is rejected (it truly committed). Replaces the deleted
	// build-and-prove guards' "production wires NO cluster" obligation with the positive proof.
	// Gated out of the parallel `make test` (clustered raft elections starve under contention)
	// and run only here in its own -race subprocess; the cheap two-mode + detection + audit
	// unit tests run in `make test`.
	cmd := exec.Command("go", "test", "-race", "-count=1", "-tags", "d9_integration", "-timeout", "300s",
		"./test/d9/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d9 matrix failed: %v\n--- output ---\n%s", err, buf.String())
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
	// B9: widened from `-run Spawnsafe` to include the leak gates.
	//
	// test/concurrency holds 26 tests, ELEVEN of which are the repo's goroutine- and fd-leak gates — and
	// the filter here matched exactly one of them (TestSpawnsafePolicy_concurrentGenSwap). `make test` is
	// a bare `go test ./...` with no -race, and this `run` helper is the only place -race is applied. So
	// the gates CLAUDE.md §5 requires every concurrency-touching change to pass had themselves never run
	// under -race in any gate, while the F13 note above this block claims they must.
	//
	// This ADDS coverage, so roadmap §7.6 (which forbids silently REDUCING matrix coverage) is satisfied
	// rather than merely not violated.
	run("concurrency", "-run", "Spawnsafe|Leak|FDStable", "./test/concurrency/...")
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
// the agent readiness hook (Fix B) thus fails `make e2e-parallel` too, not only `make test`.
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

	// -count=1 disables go's test cache so each `make e2e-parallel` run is
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
