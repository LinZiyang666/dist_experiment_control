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
// REVISITED 2026-09-01 (docs/reviews/test-system-overhaul-plan.md §0 A8). The paragraph above is kept
// as written because it is the registered reasoning of its time; what follows is what checking it
// found. The -tags sets do NOT select different files for the shared packages: D1–D4 pass no -tags at
// all (see their exec.Command calls below), and for every d*_integration tag in ALL_TEST_TAGS (d5–d9;
// NOT phasefluidity_integration — see two paragraphs down) the build closure of internal/cluster,
// internal/broker, internal/proc and test/cluster is byte-identical to the untagged one. Reproduce
// (the numbers are deliberately NOT written here — they rot; the commands do not):
//
//	for t in "" d5_integration d6_integration d7_integration d8_integration d9_integration; do
//	  go list -deps -test ${t:+-tags $t} \
//	    -f '{{.ImportPath}} {{.GoFiles}} {{.Imports}}' ./internal/cluster | sort | md5sum
//	done
//	# (-test emits the `pkg [pkg.test]` variants, whose GoFiles already include the _test.go files,
//	#  so GoFiles + Imports over the -deps closure IS the build identity.)
//	grep -rl 'go:build.*_integration' internal/ cmd/ | grep -v _test.go | wc -l    # 0
//	grep -c '^func Test' internal/cluster/*_test.go | awk -F: '{s+=$2} END {print s}'   # ×4 extra runs
//
// The one tag that DOES change a shared package's file set is phasefluidity_integration (it adds
// internal/broker/phasefluidity_lifecycle_test.go), which is why de-duplication cannot be a static
// list of "known-identical" pairs: it has to be proved per run from the go list closure, and the
// parallel runner is the only place that can do that without touching this file. This file's package
// lists are NOT edited to de-duplicate — deleting a glob here is silent (see the B5 note on
// TestD3Matrix), and `make e2e-one T=TestD5Matrix` must keep meaning the whole D5 surface.
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
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/transferharness"
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
	// internal/clusteroffline joined 2026-09-01: it starts real raft nodes in seven tests and was
	// in no -race matrix at all (test/e2e/parallel/inventory_test.go's attribution check would
	// have said so, had it existed). The -timeout moved 240s → 300s with it: clusteroffline alone
	// measures ~81s under -race on the maintainer's 44-core box (`go test -race -count=1
	// ./internal/clusteroffline/`), and the old budget was sized for the other two packages.
	// origin: docs/reviews/test-system-overhaul-plan.md B6.
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "300s",
		"./internal/cluster/...", "./internal/clusteroffline/...", "./test/cluster/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("d1 matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// TestChaosMatrix runs test/chaos under -race. The 22 chaos tests carry no build tag, so they run in
// `make test` — but WITHOUT -race, and they are the tests that bounce NATS, kill brokers and crash
// agents mid-flight: docs/testing-standards.md T6's whole point. Nothing ran them under -race until
// 2026-09-01. origin: docs/reviews/test-system-overhaul-plan.md B6 (§-1 F12).
func TestChaosMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "300s",
		"./test/chaos/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("chaos matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// TestLeakRiskMatrix runs the leak-risky packages that no other matrix runs WHOLE under -race:
// internal/agent (imports internal/tunnel and internal/pty; 257 tests) and internal/tunnel (yamux;
// 43 tests). TestRemoteFSMatrix and TestProxyTunnelReconnectMatrix run both — through `-run`
// filters that select 14 and 12 of those tests, and agent's one leak-gate test
// (TestWedgedTeardownRepeatsWithoutLeaking) was outside both. The runner's attribution gate
// (test/e2e/parallel/inventory_test.go) called that "covered" until 2026-09-01 on the strength of a
// hand-written map; it now accepts only a parsed unit with -race and no -run, which is what this
// is. ~30s on the maintainer's box. origin: test-system-overhaul internal review L1-F2 / L4-F2 /
// L6-F3.
// The handoff primitive also runs WHOLE under -race here: its real subprocess
// exec tests check descriptor ownership and five rounds of lifecycle cleanup.
// origin: docs/reviews/hot-update-plan.md §4.1.
// The workload registry owns wait/signal and explicit pipe resources, and its
// whole-package run includes the five-round process lifecycle resource guard.
// origin: docs/reviews/hot-update-plan.md §4.2.
// The reliable-exec request registry, the exec runtime (real children, memfd
// replays and pumps) and its client (real NATS) join for the same reason: each
// carries a five-round resource guard that no other matrix runs under -race.
// origin: docs/reviews/hot-update-stable-tree-plan.md §4 5b.
// The agent image layer (the composed same-PID cut: runtime + process table +
// spawn verdicts + courier outbox, with its real-exec round trips and two
// five-round resource guards) joins for the same reason.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §6.
// The reliable PTY actor, master owner and local terminal adapter each carry
// lifecycle leak guards that must run as whole packages under the race detector.
// origin: hot-update S4 plan §8.4 / §10 / T47.
func TestLeakRiskMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "300s",
		"./internal/agent/...", "./internal/tunnel/...", "./internal/handoff/...", "./internal/workload/...",
		"./internal/procrequest/...", "./internal/execsession/...", "./internal/execclient/...", "./internal/agentimage/...",
		"./internal/ptysession/...", "./internal/pty/...", "./internal/terminalio/...")
	// The additional graph leaves have whole race ownership here; T52 keeps
	// its frozen actor denominator and direct lifecycle obligations unchanged.
	// origin: hot-update S7 plan §11.3 / T52; INTEG-M graph-owner handoff.
	graph := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"./internal/xferagent/...", "./internal/xferagentimage/...", "./internal/xferserve/...",
		"./internal/xfercapacity/...", "./internal/xferid/...", "./internal/xferproof/...")
	t.Run("transfer-graph", func(t *testing.T) {
		graph.Dir = repoRoot
		var buf bytes.Buffer
		graph.Stdout, graph.Stderr = &buf, &buf
		if err := testharness.RunProcess(graph); err != nil {
			t.Fatalf("transfer graph race suite failed: %v\n%s", err, buf.String())
		}
	})
	// origin: hot-update S10 plan §11.2 / P30; S8 FLOW/SS CENSUS adds data-owner imports.
	// Exact transaction witnesses supplement this whole-package race closure.
	census := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "./internal/agentupgradeport/...")
	t.Run("flow-ss-census", func(t *testing.T) {
		census.Dir = repoRoot
		var buf bytes.Buffer
		census.Stdout, census.Stderr = &buf, &buf
		if err := testharness.RunProcess(census); err != nil {
			t.Fatalf("flow/SS census race suite failed: %v\n%s", err, buf.String())
		}
	})
	// origin: hot-update S10 plan §10.2 P08 — real custody and contender lifecycles.
	witness := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "./internal/storewitness/...", "./internal/drillobserve/...")
	t.Run("store-witness", func(t *testing.T) {
		witness.Dir = repoRoot
		var buf bytes.Buffer
		witness.Stdout, witness.Stderr = &buf, &buf
		if err := testharness.RunProcess(witness); err != nil {
			t.Fatalf("store witness race suite failed: %v\n%s", err, buf.String())
		}
	})
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("leak-risk matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// origin: hot-update S5 plan §8 / T50 / §10.2 — run every owner's complete
// test set so partial-write, revoke and resource-failure tests remain covered.
// The lifecycle suite uses exact partitions with independent source coverage;
// its cumulative fixture cost exceeds one 600-second package unit.
// origin: hot-update S6 FIX-MATRIX / lead decision / §9.3 / T54.
// The real pending-ready watchdog needs 120 seconds plus two current-state cuts;
// the literal timeout is also inspected by the parallel runner's controls.
func TestHotUpdateFlowMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"./internal/rawsocket/...", "./internal/sealedstate/...", "./internal/flow/...",
		"./internal/tunnel/...", "./internal/agent/ssproxy/...", "./internal/agentimage/...",
		"./internal/agentboot/...", "./internal/agent/...",
		"./internal/broker/...", "./internal/execsession/...")
	t.Run("owners", func(t *testing.T) {
		runOwnerMatrixPartitions(t, repoRoot, cmd)
	})
	// Each exact selector is independently pinned by the partition coverage guard.
	// origin: hot-update S6 FIX-MATRIX / §9.3 / T54.
	runMatrixPartitions(t, repoRoot, []*exec.Cmd{
		// origin: hot-update S10 plan §10.2 / P30; MAKETEST-GATES cancellation proof is in the discovered closure.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCancellationProofPreservesNativeRecordShape$", "./test/hotupdate/..."),
		// origin: hot-update S8 plan §11.2; S10 plan §11.1 — merged exact closure.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestArtifactStageRefusesFailedCurrentBuild$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityCompareThenRecreateRejectsWrite$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityParallelFlowTransferTermination$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityParallelTerminationOracleControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityParallelTerminationRequiresSuppliedEvidence$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionAcceptsNativeFreshQuery$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionAcceptsUpstreamFreshObservation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionPreservesHistoricalBarriers$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionPreservesObservedOutcomeFailures$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsEarlierUnknownOutcome$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsForeignResourceCustody$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsOldGenerationCustody$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsUnsettledCustody$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestControlBudgetKeepsRefusalSeparateFromConnectionRetry$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestControlBudgetRejectsEarlierBusinessRefusal$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceChecksCommandBindingsIndependently$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceRejectsContradictoryProjections$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceRejectsUndecodableRawReceipts$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackFlowSupplyOutlivesSchedule$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLateCleanupCannotDeleteNewIncarnation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyIssuerRouteChange$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyIssuerRouteOracleControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyUpgradePolicyWithOldCLI$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyUpgradeWireRequiresQuiescenceFence$", "./test/hotupdate/..."),
		// origin: hot-update S10 plan §11.1 / P30 — compatibility evidence admission keeps an exact owner.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeCompatibilityRequiresSuppliedEvidence$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeConfigurationParsersCurrentAndPrevious$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeGoCheckpointDecoderContinuation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeNonemptyAuthorityDecoder$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradePreviousArtifactProvenanceControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireArgvManifest$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireArgvManifestControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireEightQuadrants$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireProbeUsesOriginalCTLIdentity$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireReceiptOracleControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptBindsIndependentSupplyManifest$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptReaderRejectsUnboundSource$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptRejectsModifiedHistoricalSource$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptRequiresCompleteHistoricalIdentity$", "./test/hotupdate/..."),

		// origin: hot-update S8 plan §11.2 P23; S10 plan §10.2 P08/P27 — keep closure coverage exact.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestExecPreservesDomainLeaseThroughCommit$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackStoreWitnessPrecedesReplay$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackCompatibilityMatrixIsComplete$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerACKBeforeReplay$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAppliedIndexMonotonic$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAuthFirstReadyBarrier$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAuthFlushRequiresServerAcknowledgement$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAuthWitnessRejectsStaticConnect$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerJSCurrentWitness$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerJSWitnessRejectsSameNameRecreation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerLockHeldAcrossExec$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerPreflightNeverOpensStore$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerShutdownReceiptOrder$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerStoreLockProbePositiveControl$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRaftSnapshotRPCJoinWithinCut$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRawProbeDetectsDroppedAndDuplicatedBytes$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestSSProbeBuildIncludesFixtureTags$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestSSReferenceRejectsNonceReuse$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestSoleVoterReloadWithinBudget$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestHotUpdateFixturesSuperviseEveryProcess$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestProcessCommandDiscoveryRecognizesLaunchExpressions$", "./test/hotupdate/..."),
		// S8 P21 child supervisor, S10 evidence command and fullstack acceptance
		// plan/budget/contract tests live in the ./test/hotupdate/... closure; one
		// exact partition each keeps the closure's exactly-once coverage.
		// origin: hot-update S8 plan §11.2 P21; S10 plan §10.2 P30/P32
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorHarnessUsesSharedLaunchers$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorJoinsCancelledAndFailedCommands$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorReceiver$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorSurvivesTestTimeout$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceArchiveReadKeepsConfinedRoot$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceCloseFailureRejectsArchive$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackBudgetIncludesScheduledAndCensoredSlots$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackCompletionRequiresCurrentStamp$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackContractsFailClosed$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackPlanRequiresEveryRuntime$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackResourceJudgeSeparatesRetention$", "./test/hotupdate/..."),
	})
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

// TestReliableExecMatrix runs the real-binary reliable-exec suite under -race:
// a real ctl and a real agent subprocess over a real NATS wire, with the
// carrier faults injected for real (agent connection kicks, nats-server and
// broker restarts, a relay that drops the ctl right after a chosen request
// frame, the auth_callout black window). The cli_e2e package is otherwise
// run here only through TestTransferDefaultsMatrix's `-run` filter, so
// without this unit the S1 exit criteria would never execute inside
// `make e2e-parallel` and never under -race.
// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.1.
func TestReliableExecMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	// Each five-round node upgrade owns an independent 600s unit; keeping
	// them in the cumulative exec filter exhausted the shared suite budget.
	// origin: hot-update S8 plan lead L21 / S8 LEGACY-ROUTE-TESTS ledger request
	runMatrixPartitions(t, repoRoot, []*exec.Cmd{
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^(TestExecReliable|TestCtlConnect)", "./test/cli_e2e/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestNodeUpgradeCarriesLiveExecAcrossExec$", "./test/cli_e2e/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestNodeUpgradeGenerationChainKeepsLiveExec$", "./test/cli_e2e/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestNodeUpgradeDefersOnBusyResource$", "./test/cli_e2e/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestNodeUpgradeRefusesLegacyCandidateWithLiveTasks$", "./test/cli_e2e/..."),
	})
}

// TestReliableRunMatrix exercises the real binary and controlling-terminal pair
// across reliable PTY recovery. Its separate filter keeps every ReliableExec unit
// intact while including the inner binary's overlay and race build receipts.
// origin: hot-update S4 plan §8.3 / §10 / T47.
func TestReliableRunMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"-run", "^TestRunReliable", "./test/cli_e2e/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("reliable run matrix failed: %v\n--- output ---\n%s", err, buf.String())
	}
}

// The seven suite-local integration files keep repeated real upgrades and the
// five-round 120s watchdog out of the default package run. Reference tests stay
// in the original untagged tuple. The runner shards this whole-package race
// declaration by every discovered top-level test, preserving full coverage.
// origin: hot-update S5 plan §8 / §10.2 / T50.
func TestHotUpdateIntegrationMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-tags", "hotupdate_integration", "-timeout", "600s", "./test/hotupdate/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// Every S6 owner runs its complete test set under race. The lifecycle and
// P13 cohorts use exact per-test partitions; the coverage guard rejects missing
// or overlapping selectors. A filtered CLI tuple
// would miss the cancellation hooks, and a leaf without a risky import still
// owns inherited descriptors. Existing exec/flow matrices remain independent.
// origin: hot-update S6 plan §9.3 / T54
func TestHotUpdateBrokerMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"./internal/brokerimage/...", "./internal/brokerimage/contractstub/...",
		"./internal/brokermaint/...", "./internal/controlbudget/...", "./internal/listenerimage/...", "./internal/clusterio/...",
		"./internal/storage/...", "./internal/cluster/...", "./internal/tunnel/...", "./internal/adminsock/...",
		"./internal/httplisten/...", "./internal/subhttp/...", "./internal/brokermetrics/...", "./internal/clustermanifest/...",
		"./internal/broker/...", "./internal/brokercontrol/...", "./internal/brokercontrol/maintenance/...", "./internal/agent/...", "./internal/agentboot/...", "./internal/cli/...",
		"./internal/natsinbox/...", "./internal/proxydial/...", "./internal/execclient/...", "./internal/clusterupgrade/...",
		"./cmd/tether/...", "./test/clusterharness/...", "./test/cli_e2e/...", "./internal/testharness/...")
	t.Run("owners", func(t *testing.T) {
		runOwnerMatrixPartitions(t, repoRoot, cmd)
	})
	// Each exact selector is independently pinned by the partition coverage guard.
	// origin: hot-update S6 FIX-MATRIX / §9.3 / T54.
	runMatrixPartitions(t, repoRoot, []*exec.Cmd{
		// origin: S8 internal review SIGNED-ROLL-FIXTURES F1–F5; S8 plan L21.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestDriveUpgradeActuallyStartsAKeeper$", "./cmd/tether/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAgentlessClusterCompletesAWholeRoll$", "./cmd/tether/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestARealAgentStillGetsReExecdOnAMixedCluster$", "./cmd/tether/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAgentlessDriveIsGovernedByThePlanNotALiveProbe$", "./cmd/tether/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerDaemonRollLeavesAgentUntouched$", "./cmd/tether/..."),
		// origin: hot-update S10 plan §10.2 / P30; MAKETEST-GATES cancellation proof is in the discovered closure.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCancellationProofPreservesNativeRecordShape$", "./test/hotupdate/..."),
		// origin: hot-update S8 plan §11.2; S10 plan §11.1 — merged exact closure.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestArtifactStageRefusesFailedCurrentBuild$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityCompareThenRecreateRejectsWrite$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityParallelFlowTransferTermination$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityParallelTerminationOracleControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAuthorityParallelTerminationRequiresSuppliedEvidence$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionAcceptsNativeFreshQuery$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionAcceptsUpstreamFreshObservation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionPreservesHistoricalBarriers$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionPreservesObservedOutcomeFailures$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsEarlierUnknownOutcome$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsForeignResourceCustody$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsOldGenerationCustody$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCompletionRejectsUnsettledCustody$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestControlBudgetKeepsRefusalSeparateFromConnectionRetry$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestControlBudgetRejectsEarlierBusinessRefusal$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceChecksCommandBindingsIndependently$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceRejectsContradictoryProjections$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceRejectsUndecodableRawReceipts$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackFlowSupplyOutlivesSchedule$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLateCleanupCannotDeleteNewIncarnation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyIssuerRouteChange$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyIssuerRouteOracleControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyUpgradePolicyWithOldCLI$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestLegacyUpgradeWireRequiresQuiescenceFence$", "./test/hotupdate/..."),
		// origin: hot-update S10 plan §11.1 / P30 — compatibility evidence admission keeps an exact owner.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeCompatibilityRequiresSuppliedEvidence$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeConfigurationParsersCurrentAndPrevious$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeGoCheckpointDecoderContinuation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeNonemptyAuthorityDecoder$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradePreviousArtifactProvenanceControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireArgvManifest$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireArgvManifestControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireEightQuadrants$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireProbeUsesOriginalCTLIdentity$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeWireReceiptOracleControls$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptBindsIndependentSupplyManifest$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptReaderRejectsUnboundSource$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptRejectsModifiedHistoricalSource$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestWireReceiptRequiresCompleteHistoricalIdentity$", "./test/hotupdate/..."),

		// origin: hot-update S8 plan §11.2 P23; S10 plan §10.2 P08/P27 — keep closure coverage exact.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestExecPreservesDomainLeaseThroughCommit$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackStoreWitnessPrecedesReplay$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackCompatibilityMatrixIsComplete$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerACKBeforeReplay$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAppliedIndexMonotonic$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAuthFirstReadyBarrier$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAuthFlushRequiresServerAcknowledgement$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerAuthWitnessRejectsStaticConnect$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerJSCurrentWitness$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerJSWitnessRejectsSameNameRecreation$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerLockHeldAcrossExec$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerPreflightNeverOpensStore$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerShutdownReceiptOrder$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBrokerStoreLockProbePositiveControl$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRaftSnapshotRPCJoinWithinCut$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRawProbeDetectsDroppedAndDuplicatedBytes$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestSSProbeBuildIncludesFixtureTags$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestSSReferenceRejectsNonceReuse$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestSoleVoterReloadWithinBudget$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestHotUpdateFixturesSuperviseEveryProcess$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestProcessCommandDiscoveryRecognizesLaunchExpressions$", "./test/hotupdate/..."),
		// Same eleven closure tests as TestHotUpdateFlowMatrix; each matrix
		// partitions the whole ./test/hotupdate/... closure independently.
		// origin: hot-update S8 plan §11.2 P21; S10 plan §10.2 P30/P32
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorHarnessUsesSharedLaunchers$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorJoinsCancelledAndFailedCommands$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorReceiver$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestChildSupervisorSurvivesTestTimeout$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceArchiveReadKeepsConfinedRoot$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestEvidenceCloseFailureRejectsArchive$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackBudgetIncludesScheduledAndCensoredSlots$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackCompletionRequiresCurrentStamp$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackContractsFailClosed$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackPlanRequiresEveryRuntime$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFullstackResourceJudgeSeparatesRetention$", "./test/hotupdate/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestBrokerExecOriginalFlowCohort$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestBrokerFlowCohortLeakCycles$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestBrokerFlowCohortOracleControls$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestExposeRecoversAcrossBrokerAndNATSRestarts$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestProxyDisableDuringTunnelDropStaysDown$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestProxyStartupKeepsProductLivenessWindow$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestProxyFalseOnlineRecoversAfterTunnelDrop$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestProxyJoinAfterOn$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestProxyPublishesUnreadyEdgeOnTunnelDrop$", "./test/p13/..."),
		exec.Command("go", "test", "-race", "-tags", "p13_integration", "-count=1", "-timeout", "600s", "-run", "^TestProxySubscriptionE2E$", "./test/p13/..."),
	})
}

// Each subprocess retains its own 600-second timeout. Subtest paths also let
// e2e-one isolate a partition without rerunning unrelated data-plane owners.
// No test bodies share memory across these commands.
// origin: hot-update S6 FIX-MATRIX / §9.3 / T54.
func runMatrixPartitions(t *testing.T, root string, commands []*exec.Cmd) {
	t.Helper()
	for _, cmd := range commands {
		pkg := filepath.Base(strings.TrimSuffix(cmd.Args[len(cmd.Args)-1], "/..."))
		selector := cmd.Args[len(cmd.Args)-2]
		name := strings.TrimSuffix(strings.TrimPrefix(selector, "^"), "$")
		t.Run(pkg+"/"+name, func(t *testing.T) {
			cmd.Dir = root
			var buf bytes.Buffer
			cmd.Stdout, cmd.Stderr = &buf, &buf
			if err := cmd.Run(); err != nil {
				t.Fatalf("partition %s failed: %v\n--- output ---\n%s", name, err, buf.String())
			}
		})
	}
}

// Whole-package declarations preserve race ownership. The existing runner
// partitions tagged integration by each discovered Test, with a 600s budget.
// Later owners add tuples only when their real tests land; T52 stays red until
// every risk package has both its lifecycle receipt and race ownership.
// origin: hot-update S7 plan §11.3 / T52 / lead L17
func TestTransferContinuationMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"./internal/jsstream/...",
		"./internal/transferpath/...",
		"./internal/drillobserve/...",
		"./internal/agent/...",
		"./internal/broker/...",
		"./internal/brokerimage/...",
		"./internal/brokercontrol/...",
		"./internal/authcallout/...",
		"./internal/cli/...",
		"./internal/xfercontract/...",
		"./internal/xfercontract/contractstub/...",
		"./test/cli_e2e/...",
		// The S7 leaves landed with their direct five-round lifecycle tests, so
		// each now carries its whole-package race tuple (P35 pending rows).
		// origin: hot-update S7 plan §11.3 / T52 / P35
		"./internal/filetransfer/...",
		"./internal/xferauth/...",
		"./internal/xferbridge/...",
		"./internal/xferbucket/...",
		"./internal/xferclient/...",
		"./internal/xfercontrol/...",
		"./internal/xfercourier/...",
		"./internal/xferfinal/...",
		"./internal/xfergc/...",
		"./internal/xferhistory/...",
		"./internal/xferimage/...",
		"./internal/xferledger/...",
		"./internal/xferreceive/...",
		"./internal/xfersend/...",
		"./internal/xfertrack/...",
		"./internal/xferworker/...",
		// origin: hot-update S7 plan §11.3 / T52; FIX-INLINE actor and link owners.
		"./internal/xferagent/...", "./internal/xferserve/...",
		"./test/transferharness/...",
	)
	t.Run("owners", func(t *testing.T) {
		runOwnerMatrixPartitions(t, repoRoot, cmd)
	})
	cmd = exec.Command("go", "test", "-race", "-count=1", "-tags", "hotupdate_integration", "-timeout", "600s", "./test/hotupdate/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// origin: hot-update S8 plan §11.2; S9 plan §12; S10 plan §10.2 — whole owner coverage.
func TestHotUpdateInfraMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go",
		"test",
		"-race",
		"-count=1",
		"-timeout",
		"600s",
		"./internal/infraartifact/...",
		// origin: hot-update S9 plan §12 / P36; verified admin readback owns real sockets.
		"./internal/caddyconfig/...",
		// origin: hot-update S9 plan §12 / P22/P24/P36; retain whole-owner race declarations.
		"./internal/infraattach/...",
		// origin: hot-update S9 plan §12 / P36; registered composition owners retain whole-package race.
		"./internal/infracompose/...",
		"./internal/infraauth/...",
		"./internal/infracli/...",
		"./internal/infracontract/...",
		"./internal/infrahost/...",
		"./internal/infralaunch/...",
		"./internal/inframaint/...",
		"./internal/infrapriv/...",
		"./internal/infrareadiness/...",
		"./internal/infraroot/...",
		"./internal/infratransfer/...",
		"./internal/infratransport/...",
		"./internal/infraunit/...",
		"./internal/infraupgrade/...",
		"./internal/ingressmove/...",
		// origin: hot-update S9 plan §12 / P18; FIX-SEAL recovery receipt.
		"./internal/infrarepair/...", "./internal/ingressswitch/...",
		"./cmd/tether-infra-helper/...",
		"./cmd/tether-infra-launch/...",
		// origin: hot-update S9 plan §12 / P36; INSTRUMENTS-3 native physical observer.
		"./test/simcluster/drills/lib/infraobserve/...",
		"./test/infraharness/...")
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := testharness.RunProcess(cmd); err != nil {
		t.Fatalf("TestHotUpdateInfraMatrix failed: %v\n%s", err, buf.String())
	}
}

// origin: hot-update S8 plan §11.2; S9 plan §12; S10 plan §10.2 — whole owner coverage.
func TestHotUpdateInfraNATSMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"-tags", "infranats_integration", "./test/infranats/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// origin: S9 internal review rv-business F7; hot-update S9 plan §9.5 / T86.
func TestHotUpdateInfraCaddyMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"-tags", "infracaddy_integration", "./test/infracaddy/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// origin: S9 internal review rv-business F7; hot-update S9 plan §9.5 / T86.
func TestHotUpdateInfraBusinessMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"-tags", "infrabusiness_integration", "./test/infrabusiness/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// origin: S9 internal review rv-business F7; hot-update S9 plan §9.5 / T86.
func TestHotUpdateInfraPrivilegeMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s",
		"-tags", "infraprivilege_integration", "./test/infraprivilege/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// Keep literal whole-suite commands above for the parallel runner and inventory.
// Direct matrix invocations must discover the same tagged race closure, then give
// each Test its own 600s process. Missing supply remains a native test failure.
// origin: S9 internal review rv-business F7; hot-update S9 plan §9.5 / T86.
func runInfraMatrixPartitions(t *testing.T, root string, declaration *exec.Cmd, separatelyOwned ...string) {
	t.Helper()
	partitionPkg := ""
	run := func(extra ...string) ([]byte, error) {
		// Clone only the unstarted declaration's inputs: exec.Cmd cannot be
		// reused after listing, and its process/pipe state belongs to one run.
		args := append([]string(nil), declaration.Args...)
		if partitionPkg != "" {
			args = nil
			for _, arg := range declaration.Args {
				if !strings.HasPrefix(arg, "./") {
					args = append(args, arg)
				}
			}
			args = append(args, partitionPkg)
		}
		cmd := &exec.Cmd{Path: declaration.Path, Args: append(args, extra...), Dir: root}
		var buf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &buf, &buf
		err := testharness.RunProcess(cmd)
		return buf.Bytes(), err
	}
	out, err := run("-list", "^Test")
	if err != nil {
		t.Fatalf("infra matrix discovery failed: %v\n%s", err, out)
	}
	// These whole owners include fuzz functions whose seeds ran before
	// partitioning. Preserve that replay without treating a helper Test as
	// evidence that all seed functions were discovered.
	// origin: hot-update S7 plan lead L17 / T52; ledger pass 4 seed census.
	for _, arg := range declaration.Args {
		if arg != "./internal/broker/..." && arg != "./internal/agent/..." && arg != "./cmd/tether/..." && arg != "./internal/infrahost/..." && arg != "./test/hotupdate/..." && arg != "./test/hotupdate/acceptance/..." && arg != "./test/hotupdate/evidencecmd/..." && arg != "./test/fullstackharness/..." {
			continue
		}
		seeds, err := run("-list", "^Fuzz")
		if err != nil {
			t.Fatalf("owner fuzz discovery failed: %v\n%s", err, seeds)
		}
		out = append(append(out, '\n'), seeds...)
		// Benchmarks are an explicit part of S10's package/name/tag union.
		// Keep their discovery separate from Test/Fuzz replay so a benchmark
		// cannot disappear merely because -run does not execute it.
		// origin: hot-update S10 plan §10.2 P30 / §11.1
		benchmarks, err := run("-list", "^Benchmark")
		if err != nil {
			t.Fatalf("owner benchmark discovery failed: %v\n%s", err, benchmarks)
		}
		out = append(append(out, '\n'), benchmarks...)
		break
	}
	names := matrixListedTests(string(out))
	if len(names) == 0 {
		t.Fatal("infra matrix discovery contains no tests")
	}
	for _, entry := range names {
		name := entry
		partitionPkg = ""
		if pkg, test, ok := strings.Cut(entry, ":"); ok {
			partitionPkg, name = pkg, test
		}
		explicit := false
		for _, owned := range separatelyOwned {
			if name == owned {
				explicit = true
				break
			}
		}
		if explicit {
			continue
		}
		selectors := transferharness.BoundarySelectors(partitionPkg, name, matrixPartitionTags(declaration.Args))
		if len(selectors) == 0 {
			selectors = []string{"^" + name + "$"}
		}
		for _, selector := range selectors {
			t.Run(entry+"/"+selector, func(t *testing.T) {
				args := []string{"-run", selector}
				if strings.HasPrefix(name, "Benchmark") {
					args = append(args, "-bench", "^"+name+"$", "-benchtime", "1x")
				}
				if out, err := run(args...); err != nil {
					t.Fatalf("infra matrix partition %s failed: %v\n%s", selector, err, out)
				}
			})
		}
	}
}

// Keep serial and parallel selection on the declaration's exact build tag.
func matrixPartitionTags(args []string) string {
	for i, arg := range args {
		if arg == "-tags" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "-tags=") {
			return strings.TrimPrefix(arg, "-tags=")
		}
	}
	return ""
}

// Go's footer binds names to the real package, including external-package tests.
// Recursive children may legally reuse a parent's name. Executing that name on
// /... twice would duplicate effects and consume the same budget twice.
// origin: hot-update S10 plan §10.2 P30 / §11.1
func matrixListedTests(out string) []string {
	var names, pending []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if (strings.HasPrefix(line, "Test") || strings.HasPrefix(line, "Fuzz") || strings.HasPrefix(line, "Benchmark")) && !strings.ContainsAny(line, " \t") {
			pending = append(pending, line)
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && (fields[0] == "ok" || fields[0] == "?") && strings.HasPrefix(fields[1], "github.com/LinZiyang666/tether/") {
			pkg := "./" + strings.TrimPrefix(fields[1], "github.com/LinZiyang666/tether/")
			if len(pending) == 0 {
				names = append(names, pkg+":")
			}
			for _, name := range pending {
				names = append(names, pkg+":"+name)
			}
			pending = nil
		}
	}
	names = append(names, pending...)
	tested, seen := map[string]bool{}, map[string]bool{}
	for _, entry := range names {
		if pkg, name, ok := strings.Cut(entry, ":"); ok && name != "" {
			tested[pkg] = true
		}
	}
	var unique []string
	for _, entry := range names {
		pkg, name, ok := strings.Cut(entry, ":")
		if seen[entry] || ok && name == "" && tested[pkg] {
			continue
		}
		seen[entry] = true
		unique = append(unique, entry)
	}
	return unique
}

// origin: hot-update S8 plan §11.2; S9 plan §12; S10 plan §10.2 — whole owner coverage.
func TestFullstackAcceptanceMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go",
		"test",
		"-race",
		"-count=1",
		"-timeout",
		"600s",
		"./test/hotupdate/acceptance/...", "./test/hotupdate/evidencecmd/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// origin: hot-update S8 plan §11.2; S9 plan §12; S10 plan §10.2 — whole owner coverage.
func TestUpgradeTransactionMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go",
		"test",
		"-race",
		"-count=1",
		"-timeout",
		"600s",
		"./internal/upgradeartifact/...",
		"./internal/upgradeauthority/...",
		"./internal/upgradebudget/...",
		"./internal/upgradecli/...",
		// origin: hot-update S8 cluster-roll — cover signed parent routing and inherited custody.
		"./internal/brokerupgrade/...",
		"./internal/brokerupgradeport/...",
		"./internal/upgradecompat/...",
		"./internal/upgradecut/...",
		"./internal/upgradehost/...",
		"./internal/upgradelegacy/...",
		"./internal/upgradelock/...",
		"./internal/upgradeobserve/...",
		"./internal/upgradeport/...",
		"./internal/upgraderoll/...",
		"./internal/upgradestore/...",
		"./internal/upgradetx/...",
		"./internal/upgradewire/...",
		// origin: hot-update S8 plan §11.2 / P14–P17 production adapters.
		"./internal/agentupgrade/...", "./internal/upgradeprotocol/...")
	runMatrixPartitions(t, repoRoot, []*exec.Cmd{
		// origin: hot-update S8 plan §11.2 / FIX-AGENTWIRE; each granted cut has its own 600s race unit.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAgentTransactionCutCarriesAdjunctAndLease$", "./internal/agent/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestTransactionBootImportsOnlyTheAcceptedCut$", "./internal/agent/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFinishBootReturnsTransactionImageToSource$", "./internal/agent/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAbandonedTransactionCutThawsWithinBudget$", "./internal/agent/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestCandidateABIEpochsDecideWithoutNumericCeiling$", "./internal/agentupgrade/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestPublicRefusalCarriesOnlyClosedReasons$", "./internal/agentupgrade/..."),
		// Exact discovery also closes five earlier witness gaps in this owner.
		// origin: S10 internal review GATE-FINAL F3
		// origin: hot-update S8 plan §11.2 / S8 UPGRADE-MAKETEST accepted reverse custody.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestActivationNoticeRetainsTheAcceptedCutRecord$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestParentLeaseQueriesAndReleaseShareOneFence$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestReverseRetirementUsesItsCurrentFrozenExport$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestDescribeRuntimeNamesTheWholeOriginalScope$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestIdleDomainImagesRefuseRetainedResponsibility$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestImportedCutSamplesShareTheRegistryBarrier$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestStartReplyPrecedesOnlyTheExec$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestTransferCensusNamesEveryRetainedEntry$", "./internal/agentupgradeport/..."),
		// origin: S8 FREEZE-WINDOW / S10 plan §10.2 P30 — exact new evidence tests.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFinalReaderEvidenceKeepsThePublicationBoundary$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestPublicationEvidenceDoesNotSurviveAWriteDuringVerification$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestPublicationEvidenceRejectsChangedBytesAndAuthority$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestPublicationVerificationStaysOutsideTheFreezeWindow$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAgentPreflightProbesUnchangedBytesBeforeFreeze$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAgentReaderEvidenceNeverCachesAProbeForOtherBytes$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRunningImageDigestIsBoundToTheExecutingInode$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRunningImageDigestKeepsOnlySuccessfulProcessEvidence$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestReversePreflightResolvesOnlyTheRecordedPin$", "./internal/agentupgradeport/..."),
		// origin: hot-update S8 plan §11.2 / P32; R256 incarnation and original reverse authority.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAgentCensusCarriesCurrentIncarnation$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestAgentFreshCensusAuthorizesTheOriginalReverse$", "./internal/agentupgradeport/..."),

		// origin: hot-update S8 plan §11.2 / P32; FLOW/SS CENSUS exact identity and lease witnesses.
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRuntimeCensusCarriesFlowIdentities$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFlowSSCensusClosesOriginalScopeAndEveryReceipt$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestRuntimeCensusRequiresSSPrivateIdentities$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFlowSSCensusKeepsUnknownOwnerRefusal$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestSSCensusPreservesIdleMaterialAndCutIdentity$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestFlowCensusKeepsTerminalReceiptsAndRetirement$", "./internal/agentupgradeport/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestBorrowedRuntimeLeaseKeepsOriginalReservation$", "./internal/agentupgradeport/..."),

		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeTransactionCLIUsesFreshCurrentProof$", "./test/cli_e2e/..."),
		exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "-run", "^TestUpgradeUnknownQueryAndWaitHaveDistinctExitCodes$", "./test/cli_e2e/..."),
	})
	cmd.Dir = repoRoot
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	if err := testharness.RunProcess(cmd); err != nil {
		t.Fatalf("TestUpgradeTransactionMatrix failed: %v\n%s", err, buf.String())
	}
}

// The independent harness owns process leases, executable/OFD readers, drivers
// and collectors. Each discovered Test receives its own bounded race process.
// origin: hot-update S10 plan §11.1 / P04–P05 / P18 / P30.
func TestFullstackHarnessMatrix(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "test", "-race", "-count=1", "-timeout", "600s", "./test/fullstackharness/...")
	runInfraMatrixPartitions(t, repoRoot, cmd)
}

// Whole declarations retain their compile/race ownership in the inventory.
// Clusterharness also owns repeated real cuts; N3 alone measured 337s, so its
// complete discovered roster needs independent units before cumulative work can
// consume the unchanged 600s budget.
// origin: hot-update S8 plan lead L21 / S8 UPGRADE-MAKETEST matrix handoff.
// Direct entry points also isolate the two measured heavy packages, so disabling
// runner splitting cannot recreate their cumulative 813s/689s races.
// origin: hot-update S7 plan lead L17 / T52; ledger pass 4 measured owners.
func runOwnerMatrixPartitions(t *testing.T, root string, declaration *exec.Cmd) {
	t.Helper()
	var flags, packages []string
	for _, arg := range declaration.Args[1:] {
		if strings.HasPrefix(arg, "./") {
			packages = append(packages, arg)
		} else {
			flags = append(flags, arg)
		}
	}
	if len(packages) == 0 {
		t.Fatal("owner matrix declaration contains no packages")
	}
	for _, pkg := range packages {
		t.Run(strings.TrimPrefix(pkg, "./"), func(t *testing.T) {
			cmd := &exec.Cmd{Path: declaration.Path, Args: append(append([]string{declaration.Args[0]}, flags...), pkg), Dir: root}
			// origin: hot-update S8 plan lead L21 / S8 LEGACY-ROUTE-TESTS; preserve the complete CLI roster.
			// Direct matrix runs retain the same agent partitions as the splitter.
			// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F4; plan L17 / §11.1
			if pkg == "./internal/broker/..." || pkg == "./internal/agent/..." || pkg == "./cmd/tether/..." || pkg == "./test/cli_e2e/..." || pkg == "./test/clusterharness/..." {
				// Exact roll partitions run separately in the same broker matrix.
				// All remaining CLI Tests and fuzz seeds retain dynamic ownership.
				// origin: S8 internal review SIGNED-ROLL-FIXTURES F1–F5; S8 plan L21.
				if pkg == "./cmd/tether/..." && strings.Contains(t.Name(), "TestHotUpdateBrokerMatrix/") {
					runInfraMatrixPartitions(t, root, cmd, "TestDriveUpgradeActuallyStartsAKeeper", "TestAgentlessClusterCompletesAWholeRoll", "TestARealAgentStillGetsReExecdOnAMixedCluster", "TestAgentlessDriveIsGovernedByThePlanNotALiveProbe", "TestBrokerDaemonRollLeavesAgentUntouched")
				} else {
					runInfraMatrixPartitions(t, root, cmd)
				}
				return
			}
			var buf bytes.Buffer
			cmd.Stdout, cmd.Stderr = &buf, &buf
			if err := testharness.RunProcess(cmd); err != nil {
				t.Fatalf("owner matrix package %s failed: %v\n%s", pkg, err, buf.String())
			}
		})
	}
}
