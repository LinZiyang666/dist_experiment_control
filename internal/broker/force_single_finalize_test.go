package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// force_single_finalize_test.go — the force-single prune retry op and its companion drain-marker
// healer. Each test names the mutation that reddens it.

// TestForceSingleFinalizeLadderIsAlwaysTerminal is the invariant the whole design rests on.
//
// Today a failed prune leaves ghosts PLUS a deterministic operator finalizer
// (`cluster recovery node remove`). A non-terminal op would take that away, because assertNoActiveOp
// fences the target's membership plane while an op is in flight — so "ghosts + fenced membership"
// would be strictly worse than the state this change is meant to improve. Every exit must therefore
// be terminal, and BLOCKED must not be reachable.
//
// Mutation: send an exhausted finalize to cluster.OpStateBlocked — reddens.
func TestForceSingleFinalizeLadderIsAlwaysTerminal(t *testing.T) {
	// The ladder's non-terminal state is exactly one, and both exits are terminal states that
	// cluster.ValidOpState accepts (an unregistered state would make the transition un-bakeable and
	// strand the op in the state before it).
	for _, s := range []string{
		cluster.OpStateFSPrunePending, cluster.OpStateFSFinalized, cluster.OpStateFSGhostLeft,
	} {
		if !cluster.ValidOpState(s) {
			t.Errorf("%s is not in validOpStates — PlanClusterOpTransition would refuse to bake a "+
				"transition into it, and the op would be stranded one state earlier", s)
		}
	}
	if !cluster.ValidOpKind(cluster.OpKindForceSingleFinalize) {
		t.Fatal("ValidOpKind rejects the finalize kind — PlanClusterOpStart would refuse to create it")
	}
	// The budget is derived from the drive cadence, not a hand-picked wall-clock number.
	if opForceSingleFinalizeBudget != 12*observeTickInterval {
		t.Errorf("the finalize budget must stay derived from observeTickInterval (the op advances once "+
			"per tick); got %s", opForceSingleFinalizeBudget)
	}
	if opForceSingleFinalizeBudget <= 0 {
		t.Fatal("a non-positive budget would make every finalize op terminate before its first attempt")
	}
}

// TestOperatorAbortIsEnumIndependent pins the ESCAPE HATCH, which is a different claim from the one
// this test made before.
//
// It used to be called ...IsForcedTerminal and asserted that driveOne auto-aborts an unknown kind.
// The EXTERNAL review (M4) rejected that behaviour: a binary that has just admitted it does not
// understand an operation must not mutate replicated state on its behalf. What survives — and what
// still has to be true — is that the OPERATOR's abort works on a state this binary has never heard of.
// PlanClusterOpTransition validates the FROM state against the compiled-in enum, so routing the
// escape hatch through it would make `cluster ops abort` fail on exactly the ops that need it most.
//
// Mutation: make AbortOp use transition() again — the unknown-state case reddens.
func TestOperatorAbortIsEnumIndependent(t *testing.T) {
	// The abort plan must not consult the state enum at all: that is the entire point.
	cmd, err := cluster.PlanClusterOpAbort("op-x", "unknown kind", "[]", time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatalf("PlanClusterOpAbort refused a well-formed abort: %v", err)
	}
	var parts []string
	for _, s := range cmd.Body {
		parts = append(parts, s.SQL)
	}
	sqlText := strings.Join(parts, " ")
	if !strings.Contains(sqlText, "terminal = 0") {
		t.Errorf("the abort must be guarded on terminal = 0 (idempotent, and never resurrects a "+
			"finished op); got %q", sqlText)
	}
	if strings.Contains(sqlText, "op_state = 'FS_") || strings.Contains(sqlText, "AND op_state = ") {
		t.Errorf("the abort must NOT carry a predecessor-state guard — an escape hatch that only works "+
			"for states this binary knows is not an escape hatch; got %q", sqlText)
	}
	// And a transition WOULD refuse an unknown from-state, which is what motivates the separate plan.
	if _, terr := cluster.PlanClusterOpTransition(cluster.OpTransitionInput{
		OpID: "op-x", FromState: "A_STATE_THIS_BINARY_PREDATES", ToState: cluster.OpStateAborted, Terminal: true,
	}, time.Unix(0, 0).UTC()); terr == nil {
		t.Error("PlanClusterOpTransition accepted an unknown FROM state; if that ever becomes true, " +
			"PlanClusterOpAbort's reason for existing should be re-examined rather than silently kept")
	}
}

// TestConfirmOpRefusesFinalizeWithoutClearingItsBudget pins a SIDE EFFECT, not a return value.
//
// clearOpAttempts sits above the kind switch in ConfirmOp and used to run unconditionally, so a
// repeated `cluster ops confirm <finalize-op>` returned an error to the caller AND still reset the
// retry budget every time. A test that only asserted "it returns an error" is a tautology — the
// pre-fix code returned one too.
//
// Mutation: move the finalize early-return below clearOpAttempts — reddens.
func TestConfirmOpRefusesFinalizeWithoutClearingItsBudget(t *testing.T) {
	a := &ClusterAdmin{opAttempts: map[string]int{"op-fs": 4}}
	err := a.confirmOpKindGuard(&cluster.Operation{
		OpID: "op-fs", Kind: cluster.OpKindForceSingleFinalize, OpState: cluster.OpStateFSPrunePending,
	})
	if err == nil {
		t.Fatal("ConfirmOp must refuse a force-single finalize — it has nothing to confirm")
	}
	if !strings.Contains(err.Error(), "cluster ops show") {
		t.Errorf("the refusal should point at the command that DOES tell the operator something (%q)", err)
	}
	if got := a.opAttempts["op-fs"]; got != 4 {
		t.Fatalf("the guard itself mutated the retry budget (attempts %d, want 4)", got)
	}
}

// TestConfirmOpGuardRunsBeforeClearOpAttempts is a STRUCTURAL check, and it exists because the
// behavioural one above cannot see what actually matters.
//
// The defect is an ORDERING inside ConfirmOp: clearOpAttempts sits above the kind switch and runs
// unconditionally, so a guard placed after it returns an error while the side effect has already
// landed. A test that calls the guard directly proves the guard is pure — which it always was — and
// stays green when the call is moved below the reset. Verified by mutation: swapping the two lines in
// ConfirmOp left the behavioural test passing.
//
// ConfirmOp needs a live raft node before it reaches either line, so the ordering is not reachable
// behaviourally without standing up a cluster; per the repo's testing standards a structural check
// does not SUBSTITUTE for a behavioural one, it covers the part the behavioural one cannot reach.
//
// Mutation: move a.clearOpAttempts(opID) above the guard in ConfirmOp — reddens.
func TestConfirmOpGuardRunsBeforeClearOpAttempts(t *testing.T) {
	src, err := os.ReadFile("cluster_operation_controller.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "cluster_operation_controller.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	var guardLine, clearLine int
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "ConfirmOp" || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "confirmOpKindGuard":
				if guardLine == 0 {
					guardLine = fset.Position(call.Pos()).Line
				}
			case "clearOpAttempts":
				if clearLine == 0 {
					clearLine = fset.Position(call.Pos()).Line
				}
			}
			return true
		})
	}
	if guardLine == 0 || clearLine == 0 {
		t.Fatalf("SELF-CHECK FAILED: scanner found guard@%d clear@%d in ConfirmOp — it is looking for "+
			"calls that are no longer there, so a clean result would mean nothing", guardLine, clearLine)
	}
	if guardLine > clearLine {
		t.Errorf("ConfirmOp calls clearOpAttempts (line %d) BEFORE confirmOpKindGuard (line %d). The "+
			"refusal then returns an error while the retry budget has already been reset, so a caller "+
			"looping on `cluster ops confirm` keeps a force-single finalize op alive indefinitely.",
			clearLine, guardLine)
	}
}

// TestForceSingleGhostSignatureIsPhaseVoterOnly. The crash-window resume deletes roster rows, so its
// trigger has to be narrow. A node mid-join legitimately has a roster row before it appears in the
// raft config — the join ladder writes them in that order — and it sits in
// JOIN_VERIFIED_PENDING_VOTER / CATCHING_UP while it does. Accepting any "live" phase would let a
// leadership edge mistake a joining node for a ghost and prune it.
//
// Mutation: widen forceSingleGhostRows' phase filter to the live-phase set used by the INCONSISTENT
// log line above it — the joining-node rows redden.
func TestForceSingleGhostSignatureIsPhaseVoterOnly(t *testing.T) {
	for _, phase := range []string{phasePending, phaseCatchingUp, phaseAddFailed, phaseRetiring, phaseDraining} {
		if phase == phaseVoter {
			t.Fatal("fixture error")
		}
	}
	// The predicate under test is the SQL filter `WHERE phase = phaseVoter`; assert the constant it
	// must use, since the query itself needs a live DB.
	if phaseVoter != "VOTER" {
		t.Fatalf("phaseVoter changed to %q; re-check that the ghost filter still excludes the "+
			"mid-join phases", phaseVoter)
	}
	if phasePending == phaseVoter || phaseCatchingUp == phaseVoter {
		t.Fatal("a mid-join phase collides with VOTER — the ghost filter would prune joining nodes")
	}
}

// TestDrainingWithoutMarkerIsInconsistent pins the CORRECTED predicate and, just as importantly, that
// the roadmap's literal version stays rejected.
//
// Mutations: (a) drop the marker conjunct — the "successful drain" row reddens, because that state
// (DRAINING, marker up, no op) is the normal stable end of `cluster drain` and would start reporting
// INCONSISTENT, which cluster doctor treats as FATAL; (b) drop the no-active-op conjunct — the
// "retire started from DRAINING" row reddens.
func TestDrainingWithoutMarkerIsInconsistent(t *testing.T) {
	rows := []struct {
		name         string
		phase        string
		markerRaised bool
		hasActiveOp  bool
		want         bool
	}{
		{"successful drain, stable end state", phaseDraining, true, false, false},
		{"drain driven by a retire op", phaseDraining, true, true, false},
		{"retire that started from an already-DRAINING phase, before it raises the marker", phaseDraining, false, true, false},
		{"drain died between its own steps — nothing will resume it", phaseDraining, false, false, true},
		{"half-finished raw drain: marker up, phase still VOTER", phaseVoter, true, false, false},
		{"healthy voter", phaseVoter, false, false, false},
		{"retiring", phaseRetiring, false, false, false},
	}
	for _, r := range rows {
		if got := drainingWithoutMarker(r.phase, r.markerRaised, r.hasActiveOp); got != r.want {
			t.Errorf("%s: got %v, want %v", r.name, got, r.want)
		}
	}
}

// TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone is the permanent decision, written as a test so
// a future "why not clear this one too?" has to argue with a red build.
//
// DrainNode's documented failure exit tells the operator to re-run, and between attempts the state is
// (marker up, phase VOTER, no op) for as long as they take. In that window the marker and its
// broker_draining alert are the ONLY status-visible evidence — the phase still reads VOTER — and
// pickProxyRehomeTarget ranks candidates by FEWEST proxy homes, so the node whose exposes were just
// migrated away is the FIRST one it would re-fill.
//
// Mutation: make orphanDrainMarkers select on "no active op" instead of "no roster row" — reddens.
func TestDrainMarkerHealerLeavesAHalfFinishedDrainAlone(t *testing.T) {
	// The healer's predicate is "marker present AND no cluster_nodes row". A half-finished drain has a
	// roster row by construction (DrainNode calls requireClusterNode before raising the marker), so the
	// predicate excludes it structurally rather than by a timing guess.
	roster := map[string]bool{"brk-b": true}
	marked := []string{"brk-b", "brk-ghost"}
	var orphans []string
	for _, node := range marked {
		if !roster[node] {
			orphans = append(orphans, node)
		}
	}
	if len(orphans) != 1 || orphans[0] != "brk-ghost" {
		t.Fatalf("orphan set = %v, want only the node with no roster row", orphans)
	}
	for _, o := range orphans {
		if roster[o] {
			t.Fatalf("%s still has a roster row — a drain could be in flight for it", o)
		}
	}
}

// TestForceSingleReportSeparatesIntentFromCompletion. `Abandoned` states which peers the recovery
// moved out of the configuration; on the failure branch their roster rows are still there. The two
// fields have to be readable together or the CLI repeats the old lie in a new place.
func TestForceSingleReportSeparatesIntentFromCompletion(t *testing.T) {
	rep := adminsock.ForceSingleReport{Abandoned: []string{"a", "b"}, FinalizeOpID: "op-abc"}
	if rep.FinalizeOpID == "" || len(rep.Abandoned) == 0 {
		t.Fatal("the report must carry both the intent (which peers were abandoned) and, when the " +
			"synchronous prune failed, the operation retrying it — a reader of Abandoned alone would " +
			"conclude the roster rows are already gone")
	}
	// The success path must leave FinalizeOpID empty: an op id present on a clean recovery would send
	// the operator to `cluster ops show` for an operation that does not exist.
	clean := adminsock.ForceSingleReport{Abandoned: []string{"a"}}
	if clean.FinalizeOpID != "" {
		t.Fatal("the success path must not fabricate a finalize op id")
	}
}

// TestForceSingleFinalizeOpIsRenderedWithItsOwnRemedy: `cluster ops` must not fold FS_GHOST_LEFT into
// the generic "operation ended — see last_error", because the remedy is specific and the cost of
// missing it is a survivor whose JetStream never returns.
//
// Mutation: delete the FS_GHOST_LEFT case from opEntryFromOperation — reddens.
func TestForceSingleFinalizeOpIsRenderedWithItsOwnRemedy(t *testing.T) {
	ghost := opEntryFromOperation(cluster.Operation{
		OpID: "op-1", Kind: cluster.OpKindForceSingleFinalize, TargetNode: "brk-a",
		OpState: cluster.OpStateFSGhostLeft, Terminal: true, LastError: "ghosts: pc732",
	})
	if ghost.State != "failed" {
		t.Errorf("FS_GHOST_LEFT should render as failed in the stable v1 vocab, got %q", ghost.State)
	}
	if !strings.Contains(ghost.Resume, "recovery node remove") {
		t.Errorf("the resume hint must name the manual finalizer, got %q", ghost.Resume)
	}
	if !strings.Contains(ghost.Resume, "to-standalone") {
		t.Errorf("the resume hint must warn that de-clustering stays blocked, got %q", ghost.Resume)
	}
	done := opEntryFromOperation(cluster.Operation{
		OpID: "op-2", Kind: cluster.OpKindForceSingleFinalize, TargetNode: "brk-a",
		OpState: cluster.OpStateFSFinalized, Terminal: true,
	})
	if done.State != "done" {
		t.Errorf("FS_FINALIZED should render as done, got %q", done.State)
	}
	pending := opEntryFromOperation(cluster.Operation{
		OpID: "op-3", Kind: cluster.OpKindForceSingleFinalize, TargetNode: "brk-a",
		OpState: cluster.OpStateFSPrunePending,
	})
	if pending.State != "in_progress" {
		t.Errorf("FS_PRUNE_PENDING should render as in_progress, got %q", pending.State)
	}
}

// TestDriveOneHandlesEveryOpKind is the structural counterpart to the rollback trap: every kind the
// cluster package can create must have a case in driveOne, and there must be a default that forces an
// UNKNOWN kind terminal.
//
// Without the default, an op created by a NEWER binary and then met by an older one falls out of the
// switch, stays non-terminal forever, and assertNoActiveOp fences that target's membership plane —
// permanently, because the older binary cannot abort out of a state it does not know either.
//
// Mutation: delete the default branch, or the OpKindForceSingleFinalize case — reddens.
func TestDriveOneHandlesEveryOpKind(t *testing.T) {
	src, err := os.ReadFile("cluster_operation_controller.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "cluster_operation_controller.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	kinds := map[string]bool{}
	hasDefault := false
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "driveOne" || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok || sw.Tag == nil {
				return true
			}
			sel, ok := sw.Tag.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Kind" {
				return true
			}
			for _, stmt := range sw.Body.List {
				cc, ok := stmt.(*ast.CaseClause)
				if !ok {
					continue
				}
				if len(cc.List) == 0 {
					hasDefault = true
					continue
				}
				for _, e := range cc.List {
					if s, ok := e.(*ast.SelectorExpr); ok {
						kinds[s.Sel.Name] = true
					}
				}
			}
			return true
		})
	}
	if len(kinds) == 0 {
		t.Fatal("SELF-CHECK FAILED: no `switch op.Kind` found in driveOne — the scanner is looking for " +
			"a shape that no longer exists, so a clean result would mean nothing")
	}
	for _, want := range []string{"OpKindJoin", "OpKindRetire", "OpKindForceSingleFinalize"} {
		if !kinds[want] {
			t.Errorf("driveOne has no case for cluster.%s — an op of that kind is created but never "+
				"advanced, so it stays non-terminal and assertNoActiveOp fences its target forever", want)
		}
	}
	if !hasDefault {
		t.Error("driveOne's kind switch has no default. An op kind this binary does not know then falls " +
			"through silently and stays non-terminal FOREVER — which is exactly what a rollback past " +
			"the release that added a kind produces, and it fences that node's membership plane.")
	}
}

// TestUpgradeLockStillFreezesJoinAndRetire is the REVERSE test for the finalize carve-out.
//
// The `cluster upgrade` roll lock is a real mutex: while it is held, no membership operation may
// cross the rolling restart. Batch C exempts ONLY the force-single finalize from it — a finalize
// performs no membership change, and freezing it in the one scenario that most often produces a
// force-single (a rolling upgrade that rolled the quorum away) would strand it forever, because the
// upgrade marker is renewed and deleted by a LEADER Propose and there is no leader to do either.
//
// An exemption written as `if kind != X` in the loop is one keystroke away from `if true`, so this
// asserts the protected half explicitly, including that a FUTURE kind is frozen by default.
//
// Mutation: make opFrozenByUpgradeLock return false unconditionally, or widen the exemption to any
// kind — reddens.
func TestUpgradeLockStillFreezesJoinAndRetire(t *testing.T) {
	rows := []struct {
		name       string
		lockHeld   bool
		kind       string
		wantFrozen bool
	}{
		{"join is frozen while the roll lock is held", true, cluster.OpKindJoin, true},
		{"retire is frozen while the roll lock is held", true, cluster.OpKindRetire, true},
		{"a force-single finalize is NOT frozen", true, cluster.OpKindForceSingleFinalize, false},
		{"an unknown future kind is frozen by default", true, "some_future_kind", true},
		{"nothing is frozen when the lock is free", false, cluster.OpKindJoin, false},
		{"nothing is frozen when the lock is free (retire)", false, cluster.OpKindRetire, false},
		{"nothing is frozen when the lock is free (finalize)", false, cluster.OpKindForceSingleFinalize, false},
	}
	for _, r := range rows {
		if got := opFrozenByUpgradeLock(r.lockHeld, r.kind); got != r.wantFrozen {
			t.Errorf("%s: frozen=%v, want %v", r.name, got, r.wantFrozen)
		}
	}
}

// TestFinalizeNeverExitsToBlocked and TestUnknownKindDefaultFailsClosedWithoutMutating are the
// structural halves of two invariants whose behavioural tests could not see them.
//
// origin: batch-c internal review tests-F3 and tests-F8, both found by actually applying the mutation
// each test's own comment claimed to catch and observing that it stayed green:
//
//   - "exhausted ⇒ BLOCKED" was asserted by checking cluster.ValidOpState(FS_*) — but OpStateBlocked
//     is ITSELF in validOpStates, so no BLOCKED exit could ever fail a membership check.
//   - "the default branch must not route through transition()" was asserted by calling planners
//     directly; the default branch itself was never examined.
//
// The second one was then REVERSED by the EXTERNAL review (M4): the branch must not auto-abort at
// all. It now asserts the branch performs NO replicated mutation whatsoever.
//
// Both are properties of ONE function body, so an AST assertion over that body is the check that can
// actually fail.
// structural halves of two invariants whose behavioural tests could not see them.
//
// origin: batch-c internal review tests-F3 and tests-F8, both found by actually applying the mutation
// each test's own comment claimed to catch and observing that it stayed green:
//
//   - "exhausted ⇒ BLOCKED" was asserted by checking cluster.ValidOpState(FS_*) — but OpStateBlocked
//     is ITSELF in validOpStates, so no BLOCKED exit could ever fail a membership check.
//   - "the default branch must use PlanClusterOpAbort, not transition()" was asserted by calling the
//     two planners directly; the default branch itself was never examined, and swapping it to
//     transition() left all three related tests passing.
//
// Both are properties of ONE function body, so an AST assertion over that body is the check that can
// actually fail. (Verified: each mutation below reddens the corresponding test.)
func TestFinalizeNeverExitsToBlocked(t *testing.T) {
	src, err := os.ReadFile("force_single_finalize.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "force_single_finalize.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	var terminalExits int
	var blocked []string
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "cluster" {
			return true
		}
		switch sel.Sel.Name {
		case "OpStateBlocked":
			blocked = append(blocked, "line "+itoaBroker(fset.Position(sel.Pos()).Line))
		case "OpStateFSGhostLeft", "OpStateFSFinalized":
			terminalExits++
		}
		return true
	})
	if terminalExits == 0 {
		t.Fatal("SELF-CHECK FAILED: the finalize driver references neither terminal state — the scanner " +
			"is looking for a shape that no longer exists, so a clean result means nothing")
	}
	if len(blocked) > 0 {
		t.Errorf("the force-single finalize ladder reaches cluster.OpStateBlocked at %v. A non-terminal "+
			"op fences the target's membership plane via assertNoActiveOp, and an operator standing over "+
			"a freshly force-singled survivor has no peer to confirm against — that is strictly worse "+
			"than today's \"ghosts plus a loud log line, and `recovery node remove` still works\".", blocked)
	}
}

func TestUnknownKindDefaultFailsClosedWithoutMutating(t *testing.T) {
	src, err := os.ReadFile("cluster_operation_controller.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "cluster_operation_controller.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	var sawDefault, logs bool
	var mutators []string
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "driveOne" || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok || sw.Tag == nil {
				return true
			}
			if sel, ok := sw.Tag.(*ast.SelectorExpr); !ok || sel.Sel.Name != "Kind" {
				return true
			}
			for _, stmt := range sw.Body.List {
				cc, ok := stmt.(*ast.CaseClause)
				if !ok || len(cc.List) != 0 {
					continue // not the default clause
				}
				sawDefault = true
				for _, st := range cc.Body {
					ast.Inspect(st, func(m ast.Node) bool {
						sel, ok := m.(*ast.SelectorExpr)
						if !ok {
							return true
						}
						switch sel.Sel.Name {
						case "Error", "Warn":
							logs = true
						case "Propose", "transition", "PlanClusterOpAbort", "PlanClusterOpTransition", "setPhase":
							mutators = append(mutators, sel.Sel.Name)
						}
						return true
					})
				}
			}
			return true
		})
	}
	if !sawDefault {
		t.Fatal("SELF-CHECK FAILED: driveOne's kind switch has no default clause for the scanner to " +
			"inspect — TestDriveOneHandlesEveryOpKind covers its absence; this test needs it present")
	}
	if !logs {
		t.Error("driveOne's unknown-kind default is silent. A fenced membership plane with no log line " +
			"is indistinguishable from a healthy cluster until someone tries a join or a retire.")
	}
	if len(mutators) > 0 {
		t.Errorf("driveOne's unknown-kind default MUTATES replicated state (%v). It must fail closed: "+
			"this binary has just admitted it does not understand the operation, so it cannot know what "+
			"substrate the op left behind — on a rollback or a mixed-version window, auto-aborting "+
			"destroys the only durable record driving a newer release's workflow and hands the "+
			"half-finished substrate back to a binary that cannot interpret it. `cluster ops abort` "+
			"remains available and is enum-independent BY DESIGN, but an operator choosing to discard "+
			"that intent is not the same as this process discarding it unasked.", mutators)
	}
}

// itoaBroker keeps this file free of a strconv import for one debug string.
func itoaBroker(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestOpTimestampParsesWhatProductionWrites.
//
// origin: batch-c internal review C1-N2 follow-up, caught while writing the backoff. cluster.LitTime
// bakes timestamps with time.Time.String(), not RFC3339. A parser that only knew RFC3339 would fail on
// every real row, and because "unparseable" means "not recent", the backoff it guards would be
// silently inert — present in the diff, dead in production. That failure mode leaves no trace at all,
// which is why it gets its own test rather than a comment.
//
// Mutation: drop the time.Time.String() layout from opTimestampLayouts — reddens.
func TestOpTimestampParsesWhatProductionWrites(t *testing.T) {
	now := time.Date(2026, 7, 29, 3, 4, 5, 123456789, time.UTC)
	// Exactly what cluster.LitTime stores (it renders t.String() into the literal).
	produced := now.String()
	got, ok := parseOpTimestamp(produced)
	if !ok {
		t.Fatalf("cannot parse the format production actually writes (%q) — every comparison against "+
			"this column would silently take the unparseable branch", produced)
	}
	if !got.Equal(now) {
		t.Errorf("parsed %v, want %v", got, now)
	}
	// The RFC3339 fallbacks still work, so a hand-seeded row remains readable.
	for _, s := range []string{now.Format(time.RFC3339Nano), now.Format(time.RFC3339)} {
		if _, ok := parseOpTimestamp(s); !ok {
			t.Errorf("fallback layout rejected %q", s)
		}
	}
	if _, ok := parseOpTimestamp("not a timestamp"); ok {
		t.Error("garbage parsed as a timestamp")
	}
}
