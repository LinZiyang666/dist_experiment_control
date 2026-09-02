package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// leak_assert_shape_test.go — every goroutine/fd leak assertion must exercise its subject N>=5 times (B9).
//
// THE ARITHMETIC THIS ENFORCES
// ----------------------------
// internal/testharness.AssertNoGoroutineLeak passes when the delta is <= GoroutineLeakTolerance (2), and
// that floor is correct: it is derived from the runtime's own shifting helper goroutines, and tightening
// it trades a real signal for false positives. The consequence is arithmetic rather than stylistic — a
// subject exercised ONCE can leak up to two goroutines per exercise and stay green forever, which is
// exactly what TestTunnelServerCloseWithActiveSessionNoLeak did: it opened one tunnel session while the
// leak it was named after (the yamux watcher, the per-conn bridge) is per-session shaped.
//
// WHY A GATE AND NOT A ONE-OFF FIX
// --------------------------------
// The roadmap's obligation (S1-refactor-roadmap.md:517) is "every leak assertion exercises the subject
// N>=5 times", and the first attempt at it fixed ONE call site while the shared helper's doc asserted the
// whole repo complied. The internal review enumerated twelve call sites and found six still at one
// exercise. A gate is the difference between a claim and a property; without it, site thirteen arrives
// with the same defect and the same confident comment.
//
// WHAT IT LOOKS FOR, AND WHY THAT SHAPE
// -------------------------------------
// The first design of this check matched a single expression — a comparison containing
// runtime.NumGoroutine() against `ident + INT`. That form appears in roughly six places repo-wide and in
// NONE of the four copies B9 deleted, which all wrote `last = runtime.NumGoroutine()` on one line and
// `if last-before <= 2` on another. A gate that cannot see the shape it was written to outlaw is worse
// than no gate, because its passing is read as evidence.
//
// So this scans for the CALL, not the comparison: any call to a leak-assertion helper (by name), inside a
// test function, must be preceded by a QUALIFYING loop — or be listed below with a reason.
//
// WHAT "QUALIFYING" MEANS, AND WHY THE FIRST VERSION WAS TOO LOOSE
// ----------------------------------------------------------------
// The first version accepted "the body contains any for/range at all". The mutation-honesty audit
// pointed out that this admits the most common false shape in the repo:
//
//	for _, tc := range cases {          // <- a loop, so the old gate was satisfied
//	    t.Run(tc.name, func(t *testing.T) {
//	        before := runtime.NumGoroutine()
//	        doTheThingOnce()             // <- exercised ONCE per subtest
//	        assertNoGoroutineLeak(t, tc.name, before)
//	    })
//	}
//
// Every subtest here takes its own baseline, exercises once, and asserts once. The loop iterates
// SUB-CASES, not exercises, and the ±2 tolerance swallows a per-exercise leak in each of them. So the
// predicate now has three load-bearing conjuncts:
//
//	(1) the loop must NOT contain the leak-assertion call. A loop that contains it is a loop of
//	    baseline-exercise-assert triples, which is precisely the shape above.
//	(2) the loop must run at least leakExerciseRoundsFloor (5) times, and that count must be STATICALLY
//	    RESOLVABLE from the same function or file. See loopRounds for the four shapes the repo actually
//	    uses; anything it cannot resolve is reported, because "it happens to iterate enough times today"
//	    is not a property a reader can check.
//	(3) the loop must contain that test's exact site-scoped exercise anchor. Five empty iterations, or
//	    five calls to unrelated setup followed by one real subject call, prove no leak sample size.
//
// None is substitutable: (1) rejects subcase loops, (2) proves sample size, and (3) binds that size to
// the subject rather than to arbitrary syntax.
//
// A NOTE ON CALIBRATING (2), because getting it wrong is expensive in both directions
// -----------------------------------------------------------------------------------
// The first attempt at (2) accepted only `for i := 0; i < <int literal>; i++`. Run against the tree it
// reported four offenders, and all four were CORRECT code the predicate could not read: `for range K`
// with `const K = 20`, `for round := 0; round < leakExercises; round++` with a function-local const, and
// a range over a slice built as `make([]int, leakExerciseRounds)`. A gate that forces correct code to be
// rewritten to suit its parser trains people to edit the gate, which is how a gate dies. So loopRounds
// resolves constants and make-lengths rather than demanding one blessed spelling — and it still returns
// 0, i.e. reports, for anything genuinely opaque.

// leakExerciseRoundsFloor is the N the roadmap requires. It is duplicated from
// test/concurrency's leakExerciseRounds rather than imported because this package cannot import a
// _test.go identifier from another package — and TestLeakRoundsConstantMatchesTheFloor below fails if
// the two ever drift, so the duplication cannot rot silently.
const leakExerciseRoundsFloor = 5

// leakAssertHelpers are the assertion entry points. Local shims delegating to internal/testharness keep
// their own names, so both spellings are listed.
//
// NOT listed, deliberately:
//
//	waitGoroutineBaseline    it is not an assertion. It is a CEILING used to wait for the process to
//	                         quiesce BEFORE the real baseline is taken (internal/cluster/transport_test.go
//	                         :167 and :486 pass NumGoroutine()+8). Requiring a repeated exercise around it
//	                         would demand a loop that asserts nothing. It used to be listed here AND
//	                         exempted below, which is two mechanisms disagreeing about one fact.
//	assertNoGoroutineGrowth  no such identifier exists anywhere in this repo. It was listed as though it
//	                         did, which makes the helper set look more complete than it is — the exact
//	                         failure mode this gate exists to prevent, one level up.
var leakAssertHelpers = map[string]bool{
	"assertNoGoroutineLeak": true,
	"assertNoFDLeak":        true,
	"AssertNoGoroutineLeak": true,
	"AssertNoFDLeak":        true,
	"pollGoroutinesAtMost":  true,
}

// singleExerciseExemptions are leak assertions that legitimately run their subject once, WITH the reason.
// An entry is a claim that a per-exercise leak in that subject is structurally impossible or is covered
// elsewhere — not a note that repeating it would be inconvenient.
//
// The key is "<repo-relative file>:<the test function's name>", and
// TestSingleExerciseExemptionsAreLiveAndSiteScoped below fails on any entry that does not name a real
// test function that really calls a leak helper. It is EMPTY today; the previous single entry named a
// helper rather than a test function, so it could never match anything (see leakAssertHelpers above for
// where that fact now lives instead).
var singleExerciseExemptions = map[string]string{}

// leakExerciseAnchors names one load-bearing call that must occur inside the qualifying loop at every
// live assertion site. Counting arbitrary loop bodies proved only "something iterated": an empty loop,
// or five rounds of unrelated setup followed by one real exercise, false-greened. The exact
// file:function key keeps this list site-scoped; the live/stale checks below make additions, renames and
// deletions fail closed.
var leakExerciseAnchors = map[string]string{
	// origin: gotcha #72 — the anchor is the ladder itself: a loop that merely dialt sockets would
	// prove nothing about whether a WEDGED teardown leaks its closer goroutine and socket.
	"internal/agent/conn_teardown_leak_test.go:TestWedgedTeardownRepeatsWithoutLeaking":              "Do",
	"internal/broker/alert_reconcile_test.go:TestD8AlertReconcileRunCancels":                         "Run",
	"internal/broker/transfer_audit_forward_test.go:TestD8TransferAuditForwardLeakGate":              "emitTransferAudit",
	"internal/clusteroffline/restore_test.go:TestRestoredNodeRepeatedStartStopDoesNotLeakGoroutines": "New",
	"internal/tunnel/tunnel_reconnect_test.go:TestTunnelReconnectChurnNoGoroutineLeak":               "DropTransport",
	"internal/tunnel/tunnel_reconnect_test.go:TestTunnelReconnectVsConcurrentOpenSamePort":           "Open",
	"test/concurrency/goroutine_leak_rounds_test.go:TestAdminsockRepeatedStartCloseNoGoroutineLeak":  "Start",
	"test/concurrency/goroutine_leak_rounds_test.go:TestAgentRepeatedRunNoGoroutineLeak":             "runOnce",
	"test/concurrency/goroutine_leak_rounds_test.go:TestTunnelServerRepeatedCloseNoGoroutineLeak":    "Start",
	"test/concurrency/goroutine_leak_test.go:TestAdminsockServerCloseNoGoroutineLeak":                "Start",
	"test/concurrency/goroutine_leak_test.go:TestAgentRunCancelNoGoroutineLeak":                      "Run",
	"test/concurrency/goroutine_leak_test.go:TestBrokerImmediateCancelNoGoroutineLeak":               "Run",
	"test/concurrency/goroutine_leak_test.go:TestBrokerRepeatedRunNoGoroutineLeak":                   "startBrokerNoTunnel",
	"test/concurrency/goroutine_leak_test.go:TestBrokerRunCancelNoGoroutineLeak":                     "startBrokerNoTunnel",
	"test/concurrency/goroutine_leak_test.go:TestTunnelServerCloseNoGoroutineLeak":                   "Start",
	"test/concurrency/goroutine_leak_test.go:TestTunnelServerCloseWithActiveSessionNoLeak":           "Open",
	"test/concurrency/spawnsafe_stress_test.go:TestSpawnsafePolicy_concurrentGenSwap":                "Prepare",
	"test/d4/forward_test.go:TestD4ForwardChurnNoGoroutineLeak":                                      "Forward",
	"test/d5/publisher_test.go:TestD5RunPublishesAndNoLeak":                                          "Run",
}

func TestEveryLeakAssertionExercisesItsSubjectRepeatedly(t *testing.T) {
	root := repoRoot(t)

	var offenders []string
	seenAnchors := map[string]bool{}
	scanned := 0
	err := walkTestFilesForLeakShape(t, root, func(rel string, fn *ast.FuncDecl) {
		assertPos, calls := leakAssertCallPos(fn.Body)
		if !calls {
			return
		}
		scanned++
		key := rel + ":" + fn.Name.Name
		anchor := leakExerciseAnchors[key]
		if anchor == "" {
			offenders = append(offenders, key+" (no site-scoped exercise anchor)")
			return
		}
		seenAnchors[key] = true
		if qualifyingLoopBefore(fn.Body, assertPos, anchor) {
			return
		}
		if _, exempt := singleExerciseExemptions[key]; exempt {
			return
		}
		offenders = append(offenders, key+" (missing looped call "+anchor+")")
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// NON-VACUITY over the live tree, and here that is LEGITIMATE: this gate's success state still
	// contains leak assertions (unlike the naming freeze, whose success state empties the thing it
	// counts — see docs/testing-standards.md G2b). If the scan stops finding them, it has gone blind: a
	// renamed helper, a moved package, or a walk that no longer reaches the suites.
	if scanned < 8 {
		t.Fatalf("found only %d test(s) calling a leak-assertion helper; expected at least 8. The scan has "+
			"gone blind (helper renamed, package moved, or the walk stopped reaching the suites) — which "+
			"would make every assertion below vacuous.", scanned)
	}
	for key, anchor := range leakExerciseAnchors {
		if !seenAnchors[key] {
			t.Errorf("leakExerciseAnchors lists %s -> %s, but that test no longer has a live leak assertion; "+
				"remove or update the stale anchor in the same change", key, anchor)
		}
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d leak assertion(s) do not exercise their subject in a qualifying loop:\n  %s\n\n"+
			"The tolerance is ±2 by derivation (see internal/testharness/leakgate.go), so a subject "+
			"exercised once can leak up to two goroutines per exercise and stay green forever. At N "+
			"exercises a per-exercise leak of 1 shows up as +N.\n"+
			"A qualifying loop (a) runs >= %d times with a bound this gate can resolve from the same "+
			"function — an int literal, a local const, the leakExerciseRounds constant, or the length of "+
			"a locally-built slice — and (b) does NOT contain the leak assertion itself. A "+
			"`for _, tc := range cases { t.Run(... assert ...) }` fails (b): it loops SUB-CASES, and each "+
			"one still exercises the subject exactly once.\n"+
			"Wrap the subject in such a loop, or add an entry to singleExerciseExemptions WITH a reason.",
			len(offenders), strings.Join(offenders, "\n  "), leakExerciseRoundsFloor)
	}
}

// TestSingleExerciseExemptionsAreLiveAndSiteScoped keeps the exemption list from rotting into a
// permanent allow-everything. Every key must name a test function that EXISTS and that actually calls a
// leak helper — otherwise the entry is decoration, and its reason is being trusted for nothing.
//
// This is not hypothetical bookkeeping: the list's only historical entry keyed a HELPER
// (internal/cluster/transport_test.go:waitGoroutineBaseline) while the scan only ever iterates Test*
// functions, so it could never match. It read as a considered exemption for two batches.
// leakGuardNames are the identifiers that count as "this package has a goroutine leak guard at all"
// for TestLeakGateCoversRiskyPackages. It is leakAssertHelpers PLUS waitGoroutineBaseline: the shape
// gate above deliberately excludes waitGoroutineBaseline because it is a ceiling, not an
// exercise-N-times assertion — but as a COVERAGE question ("does internal/cluster have anything that
// fails when goroutines do not return after Shutdown?") it is exactly that thing, and it is what
// transport_test.go relies on. Two gates, two questions, two lists.
var leakGuardNames = map[string]bool{
	"assertNoGoroutineLeak": true, "assertNoFDLeak": true,
	"AssertNoGoroutineLeak": true, "AssertNoFDLeak": true,
	"pollGoroutinesAtMost": true, "waitGoroutineBaseline": true,
}

// riskyImportPaths are the imports that make a package a leak risk: a raft node, a tunnel, a PTY
// or a yamux session each own goroutines that outlive the call that created them.
var riskyImportPaths = []string{
	`"github.com/hashicorp/raft"`,
	`"github.com/hashicorp/yamux"`,
	`"github.com/LinZiyang666/tether/internal/tunnel"`,
	`"github.com/LinZiyang666/tether/internal/pty"`,
}

// leakCoverageExceptions: risky packages allowed to have no leak guard, each with a reason. Empty on
// the day the gate landed: internal/broker gained two shared-helper assertions (converted from inline
// polls), internal/clusteroffline gained a five-round restart test, internal/cluster is covered by
// waitGoroutineBaseline, internal/agent and internal/tunnel already had shared-helper assertions.
var leakCoverageExceptions = map[string]string{}

// leakCoverageExceptionsCap pins the table both ways (internal review L1-F8). Package-keyed: an
// entry covers every future test in that package, which is why the cap moves only with a reason.
const leakCoverageExceptionsCap = 0

// TestLeakGateCoversRiskyPackages: every production package that imports a risky dependency has at
// least one test file calling a leak guard. CLAUDE.md §5 says "触碰隧道/PTY/reconcile/传输/Raft 必须带
// -race + 泄漏门"; on 2026-09-01 three of the five packages that import those things (broker, cluster
// as counted by the shape gate, clusteroffline) had no call the gate could see. The rule was prose.
// origin: docs/reviews/test-system-overhaul-plan.md B5 (distributed D3).
//
// gate-control: TestLeakCoveragePredicateSeesTheShapes
func TestLeakGateCoversRiskyPackages(t *testing.T) {
	root := repoRoot(t)
	if n := len(leakCoverageExceptions); n != leakCoverageExceptionsCap {
		t.Errorf("leakCoverageExceptions has %d entries, cap says %d — move the cap in the same change, with the reason", n, leakCoverageExceptionsCap)
	}
	risky := map[string]bool{}
	guarded := map[string]bool{}
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if n := d.Name(); n == "testdata" || n == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(p, ".go") {
				return nil
			}
			src, rerr := os.ReadFile(p)
			if rerr != nil {
				return rerr
			}
			rel, _ := filepath.Rel(root, filepath.Dir(p))
			pkg := filepath.ToSlash(rel)
			if strings.HasSuffix(p, "_test.go") {
				if leakGuardCalled(string(src)) {
					guarded[pkg] = true
				}
				return nil
			}
			if importsAnyRisky(string(src)) {
				risky[pkg] = true
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	if len(risky) < 4 {
		t.Fatalf("only %d risky package(s) found (%v) — the import scan has gone blind", len(risky), risky)
	}
	var uncovered []string
	for pkg := range risky {
		if guarded[pkg] {
			continue
		}
		if _, ok := leakCoverageExceptions[pkg]; ok {
			continue
		}
		uncovered = append(uncovered, pkg)
	}
	sort.Strings(uncovered)
	if len(uncovered) > 0 {
		t.Errorf("%d package(s) import raft/yamux/tunnel/pty but no test in them calls a leak guard:\n  %s\n\n"+
			"Add a leak assertion (internal/testharness.AssertNoGoroutineLeak, in a >=5-round exercise loop) "+
			"to a test that starts and stops the thing, or register the package in leakCoverageExceptions "+
			"WITH a reason.", len(uncovered), strings.Join(uncovered, "\n  "))
	}
	for pkg := range leakCoverageExceptions {
		if !risky[pkg] {
			t.Errorf("leakCoverageExceptions names %s, which imports nothing risky — delete the entry", pkg)
		}
		if guarded[pkg] {
			t.Errorf("leakCoverageExceptions names %s, which now has a leak guard — delete the entry", pkg)
		}
	}
}

func importsAnyRisky(src string) bool {
	for _, imp := range riskyImportPaths {
		if strings.Contains(src, imp) {
			return true
		}
	}
	return false
}

// leakGuardCalled reports whether some `Test*` function in src calls a leak guard — inside the
// test's own body (closures included), never in a helper. The first version accepted a call
// anywhere in the file, so a guard parked in a helper nobody calls, or in a test-shaped function
// with a lower-case tail, counted as coverage (internal review L1-F10). Reachability inside the
// test body (an `if false`) is not judged: that is a lie of a different kind, and a reviewer's.
func leakGuardCalled(src string) bool {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, 0)
	if err != nil {
		return false
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || fd.Body == nil || !isTestFuncName(fd.Name.Name) || !strings.HasPrefix(fd.Name.Name, "Test") {
			continue
		}
		found := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				if leakGuardNames[fn.Name] {
					found = true
				}
			case *ast.SelectorExpr:
				if leakGuardNames[fn.Sel.Name] {
					found = true
				}
			}
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func TestLeakCoveragePredicateSeesTheShapes(t *testing.T) {
	if !importsAnyRisky("package x\nimport \"github.com/hashicorp/raft\"\n") {
		t.Fatal("raft import not seen as risky")
	}
	if importsAnyRisky("package x\nimport \"github.com/hashicorp/go-hclog\"\n") {
		t.Fatal("unrelated import read as risky")
	}
	for _, src := range []string{
		"package x\nfunc TestA(t *testing.T) { testharness.AssertNoGoroutineLeak(t, \"a\", 0) }\n",
		"package x\nfunc TestB(t *testing.T) { assertNoFDLeak(t, \"b\", 0, 0) }\n",
		"package x\nfunc TestC(t *testing.T) { waitGoroutineBaseline(t, 0, 0) }\n",
	} {
		if !leakGuardCalled(src) {
			t.Fatalf("guard call not seen in %q", src)
		}
	}
	if leakGuardCalled("package x\nfunc TestD(t *testing.T) { _ = runtime.NumGoroutine() }\n") {
		t.Fatal("a bare NumGoroutine read is not a guard")
	}
	// Coverage is a property of TESTS: a guard in a helper nobody calls, or in a function whose name
	// go test would not run, is not coverage (internal review L1-F10).
	for _, src := range []string{
		"package x\nfunc leakCheck(t *testing.T) { testharness.AssertNoGoroutineLeak(t, \"a\", 0) }\nfunc TestE(t *testing.T) {}\n",
		"package x\nfunc Testify(t *testing.T) { testharness.AssertNoGoroutineLeak(t, \"a\", 0) }\n",
	} {
		if leakGuardCalled(src) {
			t.Fatalf("a guard outside a Test body counted as coverage: %q", src)
		}
	}
	// …but a guard inside a closure within the test body is the test's own call.
	if !leakGuardCalled("package x\nfunc TestF(t *testing.T) { t.Cleanup(func() { assertNoFDLeak(t, \"f\", 0, 0) }) }\n") {
		t.Fatal("a guard in a closure inside the test body must count")
	}
}

func TestSingleExerciseExemptionsAreLiveAndSiteScoped(t *testing.T) {
	root := repoRoot(t)

	live := map[string]bool{}
	err := walkTestFilesForLeakShape(t, root, func(rel string, fn *ast.FuncDecl) {
		if _, calls := leakAssertCallPos(fn.Body); calls {
			live[rel+":"+fn.Name.Name] = true
		}
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	for key, reason := range singleExerciseExemptions {
		if !strings.Contains(key, ":Test") {
			t.Errorf("exemption key %q does not name a Test function. The scan iterates Test* functions "+
				"only, so a key naming a helper can never match and the exemption is dead. If the point "+
				"is that a HELPER is not an assertion, remove it from leakAssertHelpers instead.", key)
			continue
		}
		if !live[key] {
			t.Errorf("exemption %q (%q) names no live leak assertion — the test was renamed, deleted, or "+
				"no longer calls a leak helper. Remove the entry in the same commit; a stale exemption is "+
				"how an allow-list decays into an allow-everything.", key, reason)
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("exemption %q carries no reason. An exemption without one is just a slower absence "+
				"of a gate (docs/testing-standards.md G3).", key)
		}
	}
}

// TestQualifyingLoopPredicateDiscriminates is the self-check, over SYNTHESIZED sources. It has to be
// synthesized: the tree's success state is "every call site already qualifies", so there is no real
// negative example to point at, and a non-vacuity assertion made against real input would fail exactly
// when the codebase is correct (docs/testing-standards.md G2/G2b).
//
// The four rows are the four ways this predicate can be wrong, and the SUB-CASE row is the one the
// mutation-honesty audit found the first version getting wrong.
func TestQualifyingLoopPredicateDiscriminates(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
		why  string
	}{
		{
			name: "literal-bound loop before the assertion",
			body: `before := 0
	for i := 0; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why:  "the canonical shape: N exercises, then one assertion",
		},
		{
			name: "leakExerciseRounds-bound loop before the assertion",
			body: `before := 0
	for i := 0; i < leakExerciseRounds; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why:  "the constant is the preferred spelling and must be accepted",
		},
		{
			name: "SUB-CASE loop that CONTAINS the assertion",
			body: `for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := 0
			exercise()
			assertNoGoroutineLeak(t, tc.name, before)
		})
	}`,
			want: false,
			why: "THE LOAD-BEARING ROW. This is a loop of baseline-exercise-assert triples: each subtest " +
				"exercises the subject exactly once and the ±2 tolerance swallows a per-exercise leak in " +
				"every one of them. The first version of this gate accepted it because it only asked " +
				"'is there a loop anywhere in the body'",
		},
		{
			name: "loop that runs fewer than the floor",
			body: `before := 0
	for i := 0; i < 2; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why:  "2 exercises against a ±2 tolerance still cannot separate a per-exercise leak of 1",
		},
		{
			name: "range-over-int with a local const (the tunnel suites' spelling)",
			body: `before := 0
	const K = 20
	for range K {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "correct code the first version of this predicate could not read. A gate that forces a " +
				"rewrite of correct code teaches people to edit the gate",
		},
		{
			name: "range over a slice built with make(_, leakExerciseRounds)",
			body: `before := 0
	ports := make([]int, leakExerciseRounds)
	for _, p := range ports {
		exercise(p)
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "TestTunnelServerCloseWithActiveSessionNoLeak's real shape: the round count IS the slice " +
				"length, and it is visible one line up",
		},
		{
			name: "range over a slice built with make(_, 2)",
			body: `before := 0
	ports := make([]int, 2)
	for _, p := range ports {
		exercise(p)
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "resolving the length must not mean ACCEPTING it — 2 exercises against a ±2 tolerance " +
				"still proves nothing",
		},
		{
			name: "range over an opaque slice",
			body: `before := 0
	for _, p := range portsFromSomewhereElse() {
		exercise(p)
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "'it happens to have 6 entries today' is not a property a reader can check; name the " +
				"count or use the constant",
		},
		{
			name: "loop AFTER the assertion",
			body: `before := 0
	exercise()
	assertNoGoroutineLeak(t, "x", before)
	for i := 0; i < 10; i++ {
		cleanup()
	}`,
			want: false,
			why:  "a loop that runs after the assertion exercised nothing before it",
		},
		{
			name: "unrelated loop before the leak baseline",
			body: `for i := 0; i < 5; i++ {
		unrelatedSetup()
	}
	before := 0
	exercise()
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a loop that completes before the leak baseline cannot contribute to the measured " +
				"delta, so accepting it makes the strengthened leak gate false-green",
		},
		{
			name: "loop bound is five but starts at four",
			body: `before := 0
	for i := 4; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "the condition's upper bound is not the iteration count; this loop exercises the " +
				"subject once and remains hidden by the +/-2 noise floor",
		},
		// The rest of the derivation, added alongside the two reviewer rows above. Each is a way the
		// three clauses can disagree with the bound — the class the reviewer's row belongs to.
		{
			name: "non-unit step: bound 10 but only 5 iterations",
			body: `before := 0
	for i := 0; i < 10; i += 2 {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why:  "10/2 == 5, which meets the floor; deriving the count has to work in both directions",
		},
		{
			name: "non-unit step that falls SHORT: bound 8, step 2, 4 iterations",
			body: `before := 0
	for i := 0; i < 8; i += 2 {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "reading the bound (8) would accept this; the count is 4 and a per-exercise leak of 1 " +
				"is still inside the ±2 floor at four exercises",
		},
		{
			name: "no initializer: start value unknowable",
			body: `before := 0
	i := someStartingPoint()
	for ; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why:  "`for ; i < 5; i++` could start at 4 or at -100; the bound alone proves nothing",
		},
		{
			name: "condition tests a different variable than the initializer sets",
			body: `before := 0
	for i := 0; j < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why:  "three clauses that do not agree on a variable are not a derivable count",
		},
		{
			name: "body can break out early",
			body: `before := 0
	for i := 0; i < 5; i++ {
		if done() {
			break
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "with a break the written bound is an UPPER limit, not the number of exercises that " +
				"happened — the loop may have run once",
		},
		{
			name: "a nested loop's break does not disqualify the outer loop",
			body: `before := 0
	for i := 0; i < 5; i++ {
		for {
			break
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "that break binds to the inner loop. Treating it as an early exit would reject correct " +
				"code and push people to edit the gate, which is how a gate dies",
		},
		{
			name: "a nested loop can break the labelled outer loop",
			body: `before := 0
outer:
	for i := 0; i < 5; i++ {
		for {
			exercise()
			break outer
		}
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a labelled break inside a nested loop binds to the OUTER loop. Pruning every nested " +
				"loop from the control-flow walk mistakes this one real exercise for five",
		},
		{
			name: "a return inside a switch exits the whole test",
			body: `before := 0
	for i := 0; i < 5; i++ {
		switch {
		case done():
			return
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a switch owns an unlabelled break, but it does not own return or goto. Pruning the whole " +
				"switch hides an early function exit and turns the written bound into a false count",
		},
		{
			name: "continue can bypass the exercise on every iteration",
			body: `before := 0
	for i := 0; i < 5; i++ {
		if skip() {
			continue
		}
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "the loop can complete five iterations while exercising the subject zero times. The gate " +
				"is meant to prove repeated exercise, not merely repeated evaluation of a loop body",
		},
		{
			name: "an empty loop is not five exercises",
			body: `before := 0
	for i := 0; i < 5; i++ {
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "a five-iteration loop is not evidence that the leak-producing subject ran at all. " +
				"Deleting the load-bearing call must not make a release gate greener",
		},
		{
			name: "an unrelated loop after the baseline is not five exercises",
			body: `before := 0
	for i := 0; i < 5; i++ {
		collectMetrics()
	}
	exercise()
	assertNoGoroutineLeak(t, "x", before)`,
			want: false,
			why: "position after the baseline proves only that the loop contributes to the measurement " +
				"window, not that it exercises the subject whose leak is asserted",
		},
		{
			name: "the baseline is re-taken AFTER a setup loop",
			body: `for i := 0; i < 5; i++ {
		unrelatedSetup()
	}
	before := 0
	for i := 0; i < 5; i++ {
		exercise()
	}
	assertNoGoroutineLeak(t, "x", before)`,
			want: true,
			why: "the reviewer's anchor row with a REAL exercise loop after the baseline — anchoring must " +
				"reject the setup loop without also rejecting the genuine one that follows it",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := "package p\n\nconst leakExerciseRounds = 5\n\nfunc TestSynthetic(t *testing.T) {\n\t" +
				tc.body + "\n}\n"
			f, err := parser.ParseFile(token.NewFileSet(), "synthetic_test.go", src, 0)
			if err != nil {
				t.Fatalf("parse: %v\n%s", err, src)
			}
			var fn *ast.FuncDecl
			for _, d := range f.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "TestSynthetic" {
					fn = fd
				}
			}
			if fn == nil {
				t.Fatal("synthetic test function not found")
			}
			pos, calls := leakAssertCallPos(fn.Body)
			if !calls {
				t.Fatal("the synthetic body must call a leak helper, or this row tests nothing")
			}
			if got := qualifyingLoopBefore(fn.Body, pos, "exercise"); got != tc.want {
				t.Errorf("qualifies = %v, want %v.\nwhy it matters: %s", got, tc.want, tc.why)
			}
		})
	}
}

// TestLeakRoundsConstantMatchesTheFloor pins that this package's duplicated floor still equals the
// constant the suites actually loop on. Cross-package _test.go identifiers cannot be imported, so the
// number is re-read from source; a silent drift would make the gate demand a different N from the one
// the tests use.
func TestLeakRoundsConstantMatchesTheFloor(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "internal", "testharness", "leakgate.go")
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range vs.Names {
			if name.Name != "LeakExerciseRounds" || i >= len(vs.Values) {
				continue
			}
			lit, ok := vs.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.INT {
				continue
			}
			found = true
			v, _ := strconv.Atoi(lit.Value)
			if v != leakExerciseRoundsFloor {
				t.Errorf("testharness.LeakExerciseRounds = %d but this gate's floor is %d. They describe "+
					"the same obligation; a gate demanding an N the suites do not use is a gate nobody "+
					"can satisfy without editing the gate.", v, leakExerciseRoundsFloor)
			}
		}
		return true
	})
	if !found {
		t.Fatalf("could not find `LeakExerciseRounds = <int>` in %s — this cross-check has gone blind, "+
			"so the two numbers can now drift silently", path)
	}
}

// TestLeakAssertShapeGateSeesTheHistoricalShape is the mutation the FIRST design of this gate could not
// see, expressed as a permanent test rather than a one-off experiment. The four helper copies B9 deleted
// all wrote the baseline and the comparison on separate lines, which a single-expression matcher misses
// entirely — so this pins that the CALL-based scan sees a call with no qualifying loop around it.
func TestLeakAssertShapeGateSeesTheHistoricalShape(t *testing.T) {
	const src = `package p

func TestHistoricalShape(t *testing.T) {
	before := runtime.NumGoroutine()
	srv := start()
	srv.Close()
	assertNoGoroutineLeak(t, "historical", before)
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "synthetic_test.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		pos, calls := leakAssertCallPos(fd.Body)
		if !calls {
			t.Fatal("the historical shape must be RECOGNISED as a leak assertion; a scan that does not " +
				"see it reports the whole repo compliant")
		}
		if qualifyingLoopBefore(fd.Body, pos, "exercise") {
			t.Error("the historical single-exercise shape must NOT qualify — it is the exact shape this " +
				"gate was written to outlaw")
		}
	}
}

// walkTestFilesForLeakShape visits every Test* function in every _test.go file under root.
func walkTestFilesForLeakShape(t *testing.T, root string, visit func(rel string, fn *ast.FuncDecl)) error {
	t.Helper()
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			visit(rel, fn)
		}
		return nil
	})
}

// leakAssertCallPos reports the position of the FIRST leak-assertion call in body, and whether there is
// one at all. The first is the right one to anchor on: a qualifying loop must precede the earliest
// assertion, since anything after it cannot have contributed to that assertion's delta.
func leakAssertCallPos(body *ast.BlockStmt) (token.Pos, bool) {
	pos := token.NoPos
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			name = fun.Name
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		}
		if leakAssertHelpers[name] && (pos == token.NoPos || call.Pos() < pos) {
			pos = call.Pos()
		}
		return true
	})
	return pos, pos != token.NoPos
}

// qualifyingLoopBefore reports whether body contains a loop that
//
//	(a) starts AFTER the leak baseline was captured, and before assertPos;
//	(b) does NOT contain assertPos;
//	(c) runs at least leakExerciseRoundsFloor times, derived from init/cond/post.
//	(d) contains the exact site-scoped exercise anchor.
//
// (b) rejects the table-driven shape, where the loop iterates sub-cases and each iteration does its own
// baseline-exercise-assert. (a) and the "derived" half of (c) are external review B2-5, and both were
// real false-greens in the first version:
//
//	(a) `for i := 0; i < 5; i++ { unrelatedSetup() }` THEN `before := ...` THEN one exercise. Five
//	    iterations, all of them finished before the baseline was taken, so none can contribute to the
//	    measured delta. The old predicate only asked "is there a loop before the assertion".
//	(b) `for i := 4; i < 5; i++` runs ONCE while the condition's upper bound reads as five. The old
//	    loopRounds returned the RHS of the condition, i.e. it reported the bound and called it a count.
//
// Both are now negative controls in TestQualifyingLoopPredicateDiscriminates and must stay there: they
// are the two ways this gate can claim compliance it has not verified.
func qualifyingLoopBefore(body *ast.BlockStmt, assertPos token.Pos, exerciseAnchor string) bool {
	env := intEnv(body)
	baseline, ok := leakBaselinePos(body, assertPos)
	if !ok {
		// FAIL CLOSED. Without a locatable baseline there is no way to tell an exercise loop from a setup
		// loop, and this gate's entire job is to not report unverified compliance. A test whose baseline
		// is not a plain assignment to a variable the assertion names can say so in
		// singleExerciseExemptions, with a reason.
		return false
	}
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		var pos, end token.Pos
		switch loop := n.(type) {
		case *ast.ForStmt:
			pos, end = loop.Pos(), loop.End()
		case *ast.RangeStmt:
			pos, end = loop.Pos(), loop.End()
		default:
			return true
		}
		if pos < baseline {
			return true // finished before the measurement started: it cannot be the exercise
		}
		if pos > assertPos {
			return true // a loop that runs after the assertion exercised nothing before it
		}
		if pos < assertPos && assertPos < end {
			return true // the assertion is INSIDE this loop: sub-cases, not exercises
		}
		if loopRounds(n, env) >= leakExerciseRoundsFloor && loopContainsCall(n, exerciseAnchor) {
			found = true
			return false
		}
		return true
	})
	return found
}

func loopContainsCall(loop ast.Node, want string) bool {
	found := false
	ast.Inspect(loop, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			name = fun.Name
		case *ast.SelectorExpr:
			name = fun.Sel.Name
		}
		if name == want {
			found = true
			return false
		}
		return true
	})
	return found
}

// leakBaselinePos returns the position at which the leak baseline was captured — the end of the LAST
// assignment, before assertPos, to any variable the assertion call mentions.
//
// The assertion's own arguments are what identify the baseline: assertNoGoroutineLeak(t, label, before),
// assertNoFDLeak(t, label, fdBefore) and pollGoroutinesAtMost(baseline+2, d) all name it, the last one
// inside an expression. So every ident in the call's arguments is a candidate, and the newest assignment
// to any of them is the measurement point.
//
// Returns ok=false when no such assignment exists, which the caller treats as "unprovable" rather than
// as "no constraint".
func leakBaselinePos(body *ast.BlockStmt, assertPos token.Pos) (token.Pos, bool) {
	var call *ast.CallExpr
	ast.Inspect(body, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok || c.Pos() != assertPos {
			return true
		}
		call = c
		return false
	})
	if call == nil {
		return token.NoPos, false
	}
	names := map[string]bool{}
	for _, arg := range call.Args {
		ast.Inspect(arg, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				names[id.Name] = true
			}
			return true
		})
	}
	best := token.NoPos
	ast.Inspect(body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || as.Pos() > assertPos {
			return true
		}
		for _, lhs := range as.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok || !names[id.Name] {
				continue
			}
			if as.End() > best {
				best = as.End()
			}
		}
		return true
	})
	return best, best != token.NoPos
}

// intEnv resolves the integer names visible inside one test body, so loopRounds can read the spellings
// the repo actually uses instead of demanding one. Three sources, all local and all visible in the same
// diff as any change to them:
//
//	const K = 20                                  -> K = 20
//	x := make([]int, leakExerciseRounds)          -> len(x) = 5
//	xs := []T{a, b, c, d, e}                      -> len(xs) = 5
//
// Plus leakExerciseRounds / LeakExerciseRounds, the shared constant, which is defined in another package
// and pinned to this file's floor by TestLeakRoundsConstantMatchesTheFloor.
//
// Anything else resolves to 0 and is therefore REPORTED. That is the intended bias: an unresolvable
// bound is not proof of N>=5, and the fix is to name the count rather than to widen this function.
func intEnv(body *ast.BlockStmt) map[string]int {
	env := map[string]int{
		"leakExerciseRounds": leakExerciseRoundsFloor,
		"LeakExerciseRounds": leakExerciseRoundsFloor,
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.ValueSpec: // const K = 20 / var K = 20
			for i, name := range node.Names {
				if i < len(node.Values) {
					if v := intValue(node.Values[i], env); v > 0 {
						env[name.Name] = v
					}
				}
			}
		case *ast.AssignStmt:
			for i, rhs := range node.Rhs {
				if i >= len(node.Lhs) {
					break
				}
				id, ok := node.Lhs[i].(*ast.Ident)
				if !ok || id.Name == "_" {
					continue
				}
				if v := lengthOf(rhs, env); v > 0 {
					env[id.Name] = v
				}
			}
		}
		return true
	})
	return env
}

// lengthOf resolves the element count of a slice-producing expression: `make(T, N)` and `[]T{…}`.
func lengthOf(e ast.Expr, env map[string]int) int {
	switch node := e.(type) {
	case *ast.CallExpr:
		if id, ok := node.Fun.(*ast.Ident); ok && id.Name == "make" && len(node.Args) >= 2 {
			return intValue(node.Args[1], env)
		}
	case *ast.CompositeLit:
		if _, ok := node.Type.(*ast.ArrayType); ok {
			return len(node.Elts)
		}
	}
	return 0
}

// intValue resolves an int literal or a name already in env; 0 means "not statically visible".
func intValue(e ast.Expr, env map[string]int) int {
	switch node := e.(type) {
	case *ast.BasicLit:
		if node.Kind != token.INT {
			return 0
		}
		v, err := strconv.Atoi(node.Value)
		if err != nil {
			return 0
		}
		return v
	case *ast.Ident:
		return env[node.Name]
	}
	return 0
}

// loopRounds returns the statically DERIVED iteration count of a for or range statement, or 0 when it
// cannot be derived:
//
//	for i := S; i < N; i += K    -> ceil((N-S)/K)     (i++ is K=1; <= adds one)
//	for range N                  -> N                 (range-over-int)
//	for _, x := range xs         -> len(xs)           when xs's length is in env
//
// external review B2-5: this used to return the RHS of the condition and call it a count, so
// `for i := 4; i < 5; i++` — one iteration — reported five. The bound is not the count; the count comes
// from all three clauses, and a loop missing any of them is unprovable rather than assumed.
//
// A loop whose body can exit early is also unprovable: `break` or `return` means the written bound is an
// upper limit, not the number of exercises that happened. `continue` is fine (the iteration still
// completed) and so is a nested loop's own break, which is why the walk stops descending into nested
// loops and switches.
func loopRounds(n ast.Node, env map[string]int) int {
	switch loop := n.(type) {
	case *ast.ForStmt:
		if bodyCanExitEarly(loop.Body) {
			return 0
		}
		return forRounds(loop, env)
	case *ast.RangeStmt:
		if bodyCanExitEarly(loop.Body) {
			return 0
		}
		// `for range 20` and `for range K` — range-over-int, and the spelling the tunnel suites use.
		if v := intValue(loop.X, env); v > 0 {
			return v
		}
		// `for _, p := range publicPorts` — resolvable when the slice was built locally.
		if id, ok := loop.X.(*ast.Ident); ok {
			return env[id.Name]
		}
	}
	return 0
}

// forRounds derives the count of a three-clause for statement. Every clause must name the SAME variable
// and resolve to an integer, or the answer is 0.
func forRounds(loop *ast.ForStmt, env map[string]int) int {
	v, start, ok := loopInitVar(loop.Init, env)
	if !ok {
		return 0
	}
	bin, ok := loop.Cond.(*ast.BinaryExpr)
	if !ok || (bin.Op != token.LSS && bin.Op != token.LEQ) {
		return 0
	}
	lhs, ok := bin.X.(*ast.Ident)
	if !ok || lhs.Name != v {
		return 0 // the condition tests a different variable than the initializer sets
	}
	end := intValue(bin.Y, env)
	if end == 0 {
		return 0
	}
	if bin.Op == token.LEQ {
		end++
	}
	step, ok := loopStep(loop.Post, v)
	if !ok || step <= 0 {
		return 0
	}
	if end <= start {
		return 0
	}
	span := end - start
	rounds := span / step
	if span%step != 0 {
		rounds++
	}
	return rounds
}

// loopInitVar reads `i := <int>` (or `i = <int>`) and returns the variable name and its start value.
// A missing or non-integer initializer makes the loop unprovable — `for ; i < 5; i++` could start
// anywhere.
func loopInitVar(init ast.Stmt, env map[string]int) (name string, start int, ok bool) {
	as, isAssign := init.(*ast.AssignStmt)
	if !isAssign || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
		return "", 0, false
	}
	id, isIdent := as.Lhs[0].(*ast.Ident)
	if !isIdent {
		return "", 0, false
	}
	// A literal 0 start is the common case and intValue returns 0 for "unresolvable" too, so read the
	// literal directly rather than through intValue's sentinel.
	switch rhs := as.Rhs[0].(type) {
	case *ast.BasicLit:
		if rhs.Kind != token.INT {
			return "", 0, false
		}
		v, err := strconv.Atoi(rhs.Value)
		if err != nil {
			return "", 0, false
		}
		return id.Name, v, true
	case *ast.Ident:
		if v, known := env[rhs.Name]; known {
			return id.Name, v, true
		}
	}
	return "", 0, false
}

// loopStep reads `i++`, `i += K` and `i = i + K` for the loop variable and returns K.
func loopStep(post ast.Stmt, v string) (int, bool) {
	switch p := post.(type) {
	case *ast.IncDecStmt:
		id, ok := p.X.(*ast.Ident)
		if !ok || id.Name != v || p.Tok != token.INC {
			return 0, false
		}
		return 1, true
	case *ast.AssignStmt:
		if len(p.Lhs) != 1 || len(p.Rhs) != 1 {
			return 0, false
		}
		id, ok := p.Lhs[0].(*ast.Ident)
		if !ok || id.Name != v {
			return 0, false
		}
		if p.Tok == token.ADD_ASSIGN {
			lit, isLit := p.Rhs[0].(*ast.BasicLit)
			if !isLit || lit.Kind != token.INT {
				return 0, false
			}
			k, err := strconv.Atoi(lit.Value)
			if err != nil {
				return 0, false
			}
			return k, true
		}
	}
	return 0, false
}

// bodyCanExitEarly reports whether a loop body contains control flow that makes the written bound an
// UPPER LIMIT rather than a count of exercises.
//
// EXTERNAL REVIEW RB2-2 — THE FIRST VERSION WAS UNSOUND IN THREE WAYS
// --------------------------------------------------------------------
// It pruned every nested loop, switch, select and type switch on the assumption that any exit inside
// belongs to that construct. That assumption is false three times over, and each falsification was
// supplied as a synthesized row that the gate accepted as "at least five exercises":
//
//	break outer   inside a nested loop  -> exits THIS loop after one exercise (labels ignore nesting)
//	return        inside a switch       -> exits the whole test; a switch owns `break`, not `return`
//	continue      before the exercise   -> five iterations, zero exercises
//
// The third is the subtlest and the reviewer stated the principle exactly: "A continue is not harmless
// unless the gate proves the load-bearing exercise occurs before every continue path." This gate does
// not do reachability analysis, so it does not try to prove that — it REPORTS instead, which is the
// direction an unprovable case has to fail in.
//
// The rule now, with nesting tracked explicitly rather than by pruning:
//
//	return                      always      (a FuncLit's return belongs to the closure, so FuncLits are skipped)
//	goto                        always      (target unknowable from here)
//	any LABELLED break/continue always      (the label may name this loop; resolving it is not worth the
//	                                        machinery when the conservative answer costs one comment)
//	unlabelled break            iff no enclosing loop/switch/select inside this body owns it
//	unlabelled continue         iff no enclosing loop inside this body owns it
func bodyCanExitEarly(body *ast.BlockStmt) bool {
	var walk func(n ast.Node, loopDepth, breakOwnerDepth int) bool
	walk = func(n ast.Node, loopDepth, breakOwnerDepth int) bool {
		switch node := n.(type) {
		case nil:
			return false

		case *ast.FuncLit:
			// A return inside a closure returns from the CLOSURE. Its breaks/continues cannot bind to
			// anything out here either, because Go forbids branching out of a function literal.
			return false

		case *ast.ForStmt:
			return walk(node.Body, loopDepth+1, breakOwnerDepth+1)
		case *ast.RangeStmt:
			return walk(node.Body, loopDepth+1, breakOwnerDepth+1)

		case *ast.SwitchStmt:
			// A switch owns an unlabelled `break` but NOT `continue`, `return` or `goto`. Keep walking
			// with the break-owner depth raised and the loop depth unchanged — pruning the whole switch is
			// what hid the return.
			return walk(node.Body, loopDepth, breakOwnerDepth+1)
		case *ast.TypeSwitchStmt:
			return walk(node.Body, loopDepth, breakOwnerDepth+1)
		case *ast.SelectStmt:
			return walk(node.Body, loopDepth, breakOwnerDepth+1)

		case *ast.BranchStmt:
			switch node.Tok {
			case token.GOTO:
				return true
			case token.BREAK:
				return node.Label != nil || breakOwnerDepth == 0
			case token.CONTINUE:
				return node.Label != nil || loopDepth == 0
			}
			return false

		case *ast.ReturnStmt:
			return true
		}

		// Everything else: recurse into children at the same depths.
		found := false
		ast.Inspect(n, func(c ast.Node) bool {
			if found || c == nil || c == n {
				return !found
			}
			switch c.(type) {
			case *ast.FuncLit, *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt,
				*ast.SelectStmt, *ast.BranchStmt, *ast.ReturnStmt:
				if walk(c, loopDepth, breakOwnerDepth) {
					found = true
				}
				return false // walk() handled this subtree at the right depths
			}
			return true
		})
		return found
	}
	return walk(body, 0, 0)
}
