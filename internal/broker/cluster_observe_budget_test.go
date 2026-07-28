package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// cluster_observe_budget_test.go — the leader tick's JS observations must all carry a deadline (B7).
//
// WHY THIS IS A GATE AND NOT JUST A FIX
// -------------------------------------
// Two calls shared this path and only one was obvious. `streamObserve` (reached from StatusReport ->
// topoConvergedForOp) was the one the plan set out to bound; `clusterStreamsReady` (reached from
// a.streamsReady -> driveInFlightOperations) was on the same tick and was missed. Bounding one of two
// unbounded calls leaves the path structurally unbounded while the diff looks like a fix — so the
// durable artefact is not the two `context.WithTimeout` calls, it is a check that a THIRD one cannot
// appear.
//
// The AST check below is deliberately narrow: `context.Background()` inside this file, not "anywhere
// in the package". Elsewhere in the broker a Background context is correct (a process-lifetime loop
// root). What is not correct is handing one to a JS observation that a 5-second tick is waiting on.

// TestReplicaObserveBudgetScalesWithSessions pins the RELATION, not the numbers. Compare
// xferCrossHomeReapAge = 3 * transferTimeoutTierB, pinned the same way in reconcile_passes_test.go.
func TestReplicaObserveBudgetScalesWithSessions(t *testing.T) {
	// The RELATION, not the numbers. A constant budget was the original defect (internal review B7-02):
	// ObserveReplicas walks every session's history stream serially, so a fixed deadline covers a small
	// broker and expires routinely on a large one — and a routine expiry rendered as a MEASURED replica
	// deficit. What must hold is that the budget grows with the work.
	base := clusterReplicaObserveBudget(0)
	if base != clusterReplicaObserveBase {
		t.Errorf("with no sessions the budget must be the base (%v), got %v", clusterReplicaObserveBase, base)
	}
	if base != clusterJSPlaceProbeTimeout {
		t.Errorf("the base is defined AS the sibling JS probe's budget (%v) because it is the same kind of "+
			"work against the same meta; it is now %v. Inventing a second number here creates two "+
			"constants kept in agreement by hand.", clusterJSPlaceProbeTimeout, base)
	}

	// Strictly increasing in the session count, up to the ceiling.
	prev := base
	for _, n := range []int{1, 5, 20, 50} {
		got := clusterReplicaObserveBudget(n)
		if got < prev {
			t.Errorf("budget(%d)=%v is smaller than budget for fewer sessions (%v) — the budget must not "+
				"shrink as the work grows", n, got, prev)
		}
		if got > clusterReplicaObserveCeiling {
			t.Errorf("budget(%d)=%v exceeds the ceiling %v; an uncapped budget is the unbounded call this "+
				"replaced, wearing a different hat", n, got, clusterReplicaObserveCeiling)
		}
		prev = got
	}
	if clusterReplicaObserveBudget(1) <= base {
		t.Error("one session must cost MORE than zero sessions, or the per-session term is not wired and " +
			"the budget is a constant again")
	}

	// The ceiling must actually bind at a plausible fleet size, or it is decoration.
	if clusterReplicaObserveBudget(10_000) != clusterReplicaObserveCeiling {
		t.Errorf("the ceiling does not bind at 10k sessions (got %v) — an unbounded derived deadline is "+
			"exactly what this code path must never hand the leader tick",
			clusterReplicaObserveBudget(10_000))
	}

	// A negative count must not produce a shorter-than-base budget (defensive: the count comes from a
	// best-effort DB read that returns 0 on error, but a future caller might not).
	if clusterReplicaObserveBudget(-5) != base {
		t.Errorf("a negative session count must clamp to the base, got %v", clusterReplicaObserveBudget(-5))
	}

	// A zero or negative budget would make every observation fail instantly and, because both call
	// sites are fail-closed, would freeze every convergence ladder in the cluster.
	if base <= 0 {
		t.Fatalf("a non-positive observation budget (%v) fails every observation instantly, and both call "+
			"sites read a failed observation as NOT ready", base)
	}
}

// TestNoUnboundedJSObservationOnTheLeaderTick is the structural half. It complements the behavioural
// bound rather than substituting for it (testing-standards S1: "structural checks and behavioural
// checks cannot substitute for each other") — the behavioural half is that both call sites are
// fail-closed on a deadline, which their own callers' tests cover.
func TestNoUnboundedJSObservationOnTheLeaderTick(t *testing.T) {
	const file = "clusterwrite.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	// Non-vacuity FIRST, and over synthesized expectations rather than over whatever happens to be in
	// the tree: the scanner must be able to see the two ObserveReplicas call sites it exists to police.
	// If a rename or a move makes them invisible, this gate must fail loudly rather than pass by
	// finding nothing to complain about.
	observeCalls := 0
	unbounded := []string{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "ObserveReplicas" {
			return true
		}
		observeCalls++
		if len(call.Args) != 1 {
			unbounded = append(unbounded, fset.Position(call.Pos()).String()+" (unexpected arity)")
			return true
		}
		// The argument must not be a bare context.Background() / context.TODO().
		inner, ok := call.Args[0].(*ast.CallExpr)
		if !ok {
			return true // an identifier (a ctx variable) — fine, its deadline is the caller's business
		}
		isel, ok := inner.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := isel.X.(*ast.Ident)
		if !ok || pkg.Name != "context" {
			return true
		}
		if isel.Sel.Name == "Background" || isel.Sel.Name == "TODO" {
			unbounded = append(unbounded,
				fset.Position(call.Pos()).String()+" ("+isel.Sel.Name+")")
		}
		return true
	})

	if observeCalls < 2 {
		t.Fatalf("found only %d ObserveReplicas call site(s) in %s; expected at least 2 (the streamObserve "+
			"seam and clusterStreamsReady). This gate has gone blind — a rename or a move, not a clean tree.",
			observeCalls, file)
	}
	if len(unbounded) > 0 {
		t.Errorf("%d JS replica observation(s) on the leader tick take a bare Background context:\n  %v\n"+
			"ObserveReplicas walks every session's history stream, so an unbounded one can hold the "+
			"leader-maintenance tick — and therefore every in-flight membership operation — indefinitely. "+
			"Use clusterReplicaObserveBudget(...).", len(unbounded), unbounded)
	}
}
