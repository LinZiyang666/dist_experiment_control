package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
)

// cluster_observe_budget_test.go — cluster-maintenance JS observations must all carry a deadline (B7).
//
// WHY THIS IS A GATE AND NOT JUST A FIX
// -------------------------------------
// Two calls shared the operation-controller path and only one was obvious. `streamObserve` (reached from StatusReport ->
// topoConvergedForOp) was the one the plan set out to bound; `clusterStreamsReady` (reached from
// a.streamsReady -> driveInFlightOperations) was on the same tick and was missed. Bounding one of two
// unbounded calls leaves the path structurally unbounded while the diff looks like a fix — so the
// durable artefact is not the two `context.WithTimeout` calls, it is a check that a THIRD one cannot
// appear.
//
// External review F1 found the literal third call: the alert reconciler's observe wrapper received a
// process-lifetime loop context and passed it directly into the serial observation. It now owns the same
// derived deadline. The AST check below is deliberately narrow to all three calls in clusterwrite.go:
// it pins both the call count and three `WithTimeout(..., clusterReplicaObserveBudget(...))` contexts.

// TestReplicaObserveBudgetScalesWithStreams pins the RELATION, not the numbers. Compare
// xferCrossHomeReapAge = 3 * transferTimeoutTierB, pinned the same way in reconcile_passes_test.go.
//
// origin: p-b2 internal review m4. This test used to end in "...ScalesWithSessions", and its failure
// messages named a "per-session term" and a "session count" — after the term itself had been renamed to
// PerStream precisely because, in the commit's own words, the old name was where the mismatch hid. A
// stale name in a gate's OUTPUT is worse than one in its source: the output is what the next reader sees
// first, and it pointed at a constant that no longer exists.
func TestReplicaObserveBudgetScalesWithStreams(t *testing.T) {
	// The RELATION, not the numbers. A constant budget was the original defect (internal review B7-02):
	// ObserveReplicas walks the events stream, every session's history stream and every OBJ_xfer stream
	// serially, so a fixed deadline covers a small broker and expires routinely on a large one — and a
	// routine expiry rendered as a MEASURED replica deficit. What must hold is that the budget grows with
	// the work.
	base := clusterReplicaObserveBudget(0)
	if base != clusterReplicaObserveBase {
		t.Errorf("with no streams the budget must be the base (%v), got %v", clusterReplicaObserveBase, base)
	}
	if base != clusterJSPlaceProbeTimeout {
		t.Errorf("the base is defined AS the sibling JS probe's budget (%v) because it is the same kind of "+
			"work against the same meta; it is now %v. Inventing a second number here creates two "+
			"constants kept in agreement by hand.", clusterJSPlaceProbeTimeout, base)
	}

	// Strictly increasing in the stream count, up to the ceiling.
	prev := base
	for _, n := range []int{1, 5, 20, 50} {
		got := clusterReplicaObserveBudget(n)
		if got < prev {
			t.Errorf("budget(%d)=%v is smaller than budget for fewer streams (%v) — the budget must not "+
				"shrink as the work grows", n, got, prev)
		}
		if got > clusterReplicaObserveCeiling {
			t.Errorf("budget(%d)=%v exceeds the ceiling %v; an uncapped budget is the unbounded call this "+
				"replaced, wearing a different hat", n, got, clusterReplicaObserveCeiling)
		}
		prev = got
	}
	if clusterReplicaObserveBudget(1) <= base {
		t.Error("one stream must cost MORE than zero streams, or the clusterReplicaObservePerStream term " +
			"is not wired and the budget is a constant again")
	}

	// The ceiling must actually bind at a plausible fleet size, or it is decoration.
	if clusterReplicaObserveBudget(10_000) != clusterReplicaObserveCeiling {
		t.Errorf("the ceiling does not bind at 10k streams (got %v) — an unbounded derived deadline is "+
			"exactly what this code path must never hand the leader tick",
			clusterReplicaObserveBudget(10_000))
	}

	// A negative count must not produce a shorter-than-base budget (defensive: the count comes from a
	// best-effort DB read that returns 0 on error, but a future caller might not).
	if clusterReplicaObserveBudget(-5) != base {
		t.Errorf("a negative stream count must clamp to the base, got %v", clusterReplicaObserveBudget(-5))
	}

	// A zero or negative budget would make every observation fail instantly and, because both call
	// sites are fail-closed, would freeze every convergence ladder in the cluster.
	if base <= 0 {
		t.Fatalf("a non-positive observation budget (%v) fails every observation instantly, and both call "+
			"sites read a failed observation as NOT ready", base)
	}
}

// TestObserveBudgetCountsXferStreamsNotJustSessions pins the fix for external review RB2 doubt 3.
//
// origin: batch B2 external re-review doubt 3 (post-release technical-debt cleanup)
//
// THE MISMATCH
// ------------
// The scaling term used to be `COUNT(*) FROM sessions WHERE state='ACTIVE'`. That predicts the
// per-session history streams EXACTLY — AuditPublisherConfig.ListSIDs is literally that query — but it
// counts the `OBJ_xfer-*` streams as ZERO, and ObserveReplicas walks every one of those too via
// ListXferStreams. Those streams can OUTLIVE the session that created them (an orphaned transfer
// bucket), so a transfer-heavy broker was handed a deadline sized for a fraction of the round trips it
// was about to make, and "routinely unobserved" followed.
//
// The scaling input is now the PREVIOUS observation's own stream count, which already includes
// events + history + xfer. This test pins that the count reaching the budget is the STREAM count, by
// driving cacheReplicaSnapshot with a report whose stream count exceeds any plausible session count and
// checking the budget responds.
//
// origin: p-b2 internal review M3, m5, m6, n1. The first version of this test asserted on
// observedStreamCount() and then composed the budget BY HAND. That left observeStreamCountForBudget —
// the function the fix actually added, and the only thing the two call sites call — with ZERO test
// references: deleting its cached-count branch outright was a byte-exact revert of the fix and the whole
// package stayed green. Everything below goes through the real entry point for that reason. The three
// other defects were of a piece with it: the count was only ever driven UPWARD (so "record the current
// count" and "record a high-water mark" were indistinguishable), the unobserved-report probe carried no
// streams (so it was satisfied by the `len(Streams)==0` conjunct and never touched the `!rep.Observed`
// one it was named for), and one assertion sat below a t.Fatalf that had already pinned the exact value.
func TestObserveBudgetCountsXferStreamsNotJustSessions(t *testing.T) {
	nStreams := func(n int) []jsstream.StreamReplicaState {
		out := make([]jsstream.StreamReplicaState, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, jsstream.StreamReplicaState{Actual: 3, Target: 3})
		}
		return out
	}

	// Two ACTIVE sessions, so the fallback term (sessions + events) is a KNOWN 3 and every later
	// assertion distinguishes "read the stream count" from "fell back to the session count".
	db := openDB(t)
	now := time.Now().Unix()
	for _, sid := range []string{"s1", "s2"} {
		if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) `+
			`VALUES(?,?,'o','p','ACTIVE',?)`, sid, sid, now); err != nil {
			t.Fatalf("seed session %s: %v", sid, err)
		}
	}
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}}

	// Before the first observation there is nothing cached, so the budget falls back to the old estimate:
	// one history stream per ACTIVE session, plus the events stream.
	if got := b.observeStreamCountForBudget(); got != 3 {
		t.Fatalf("with no observation yet and 2 ACTIVE sessions the budget input must fall back to 3 "+
			"(sessions + events), got %d", got)
	}

	// An observation dominated by transfer streams: one events stream, one session history, and eight
	// live OBJ_xfer buckets. The session-count term would size this as 3; the stream count is 10.
	b.cacheReplicaSnapshot(ReplicaReport{Streams: nStreams(10), Observed: true})

	if got := b.observeStreamCountForBudget(); got != 10 {
		t.Fatalf("budget input = %d, want 10. The session count here is 2, so anything but 10 means the "+
			"OBJ_xfer streams are back to costing nothing — the exact defect RB2 doubt 3 named.", got)
	}

	// And the budget must actually respond to it, not merely store it.
	if clusterReplicaObserveBudget(b.observeStreamCountForBudget()) <= clusterReplicaObserveBudget(1) {
		t.Errorf("ten streams must buy more budget than one (%v vs %v) — otherwise the per-stream term is "+
			"not wired to the quantity it names",
			clusterReplicaObserveBudget(b.observeStreamCountForBudget()), clusterReplicaObserveBudget(1))
	}

	// The count TRACKS the last observation; it is not a high-water mark. A broker that once ran 500
	// streams and now runs 3 must not keep sizing its deadline for 500 — that budget is an upper bound, so
	// an inflated one silently stops bounding anything.
	b.cacheReplicaSnapshot(ReplicaReport{Streams: nStreams(4), Observed: true})
	if got := b.observeStreamCountForBudget(); got != 4 {
		t.Errorf("after an observation of 4 streams the budget input is %d, want 4; the cache records the "+
			"LAST observation, not the largest one ever seen", got)
	}

	// An UNOBSERVED report must not overwrite a good count: a transient meta-not-ready tick would
	// otherwise shrink the next tick's budget precisely when the cluster is already struggling. The probe
	// CARRIES streams, so only the `!rep.Observed` half of the guard can reject it — with a stream-less
	// report this assertion would pass even if that half were deleted.
	b.cacheReplicaSnapshot(ReplicaReport{Streams: nStreams(1), Observed: false})
	if got := b.observeStreamCountForBudget(); got != 4 {
		t.Errorf("an UNOBSERVED report overwrote the cached stream count (now %d, was 4). A failed "+
			"observation must not shrink the next one's deadline — that is a feedback loop that makes a "+
			"struggling cluster fail faster.", got)
	}

	// The other half of the same guard: an observation that enumerated nothing also leaves the cache
	// alone. (It is load-bearing twice over — rep.Streams[0] is read unguarded below it.)
	b.cacheReplicaSnapshot(ReplicaReport{Observed: true})
	if got := b.observeStreamCountForBudget(); got != 4 {
		t.Errorf("a report with no streams overwrote the cached count (now %d, was 4)", got)
	}

	// A failed observation that discovered the COMPLETE work set before per-stream collection timed out
	// must teach the next budget that count. Observed remains false, so this cannot update the replica
	// posture gauge or open a readiness gate.
	b.cacheReplicaSnapshot(ReplicaReport{StreamCount: 17, Observed: false})
	if got := b.observeStreamCountForBudget(); got != 17 {
		t.Errorf("failed observation discovered 17 streams but next budget input is %d; preserving the "+
			"old short count makes an OBJ_xfer growth timeout repeat forever", got)
	}
}

// TestObserveBudgetDoesNotRegressBelowLiveSessionFloor protects the other side of the predictor.
//
// origin: batch B2 debt external review F1. The observed stream count includes transfer buckets, but
// it is also stale by definition. Using it exclusively means a cache of 1 followed by a burst to 100
// ACTIVE sessions gets 3.25s, while the old session-count implementation gave that same observation
// 28.25s. If the short observation expires, ObserveReplicas returns an empty report plus an error and
// observeAndCache deliberately does not update the cache, so the claimed "next tick self-corrects"
// cannot happen: every later tick can repeat the same undersized deadline indefinitely.
//
// The safe predictor is therefore the maximum of two independently useful lower bounds: the last
// enumerated stream count (which sees OBJ_xfer streams even when state collection later times out) and
// the current sessions+events floor (which sees growth since that enumeration). This test makes the
// regression causal by driving the real DB count above a deliberately stale cache.
func TestObserveBudgetDoesNotRegressBelowLiveSessionFloor(t *testing.T) {
	db := openDB(t)
	now := time.Now().Unix()
	for i := 0; i < 12; i++ {
		sid := "burst-" + strconv.Itoa(i)
		if _, err := db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state,created_at) `+
			`VALUES(?,?,'o','p','ACTIVE',?)`, sid, sid, now); err != nil {
			t.Fatalf("seed session %s: %v", sid, err)
		}
	}
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}}
	b.cacheReplicaSnapshot(ReplicaReport{
		Streams:  []jsstream.StreamReplicaState{{Actual: 3, Target: 3}},
		Observed: true,
	})

	const want = 13 // 12 current history streams + the events stream
	if got := b.observeStreamCountForBudget(); got != want {
		t.Fatalf("budget input after session growth = %d, want %d. A stale successful observation must "+
			"not buy LESS time than the live session floor; if the resulting short observation times "+
			"out, it cannot refresh this cache and the leader can remain permanently under-budgeted.",
			got, want)
	}
}

// TestNoUnboundedJSObservationOnClusterMaintenance is the structural half. It complements the behavioural
// bound rather than substituting for it (testing-standards S1: "structural checks and behavioural
// checks cannot substitute for each other") — the behavioural half is that all THREE call sites are
// fail-closed on a deadline, which their own callers' tests cover. Two of the three are the operation
// controller's; the third is the alert reconciler's observe wrapper, which external review F1 found
// taking the process-lifetime loop context directly.
func TestNoUnboundedJSObservationOnClusterMaintenance(t *testing.T) {
	const file = "clusterwrite.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	// Non-vacuity FIRST, and over synthesized expectations rather than over whatever happens to be in
	// the tree: the scanner must see all three ObserveReplicas call sites and all three derived contexts.
	// If a rename or a move makes them invisible, this gate must fail loudly rather than pass by
	// finding nothing to complain about.
	observeCalls := 0
	budgetContexts := 0
	unbounded := []string{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "WithTimeout" {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "context" && len(call.Args) == 2 {
				if budget, ok := call.Args[1].(*ast.CallExpr); ok {
					if id, ok := budget.Fun.(*ast.Ident); ok && id.Name == "clusterReplicaObserveBudget" {
						budgetContexts++
					}
				}
			}
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

	if observeCalls != 3 || budgetContexts != 3 {
		t.Fatalf("found %d ObserveReplicas call site(s) and %d derived deadline context(s) in %s; want "+
			"exactly 3 of each (alert observe cache, streamObserve and clusterStreamsReady). A count "+
			"change requires reviewing the new/moved call, not silently accepting an unbounded path.",
			observeCalls, budgetContexts, file)
	}
	if len(unbounded) > 0 {
		t.Errorf("%d JS replica observation(s) on a cluster-maintenance loop take a bare Background context:\n  %v\n"+
			"ObserveReplicas walks every session's history stream, so an unbounded one can hold the "+
			"cluster-maintenance loop — including membership progress or alert reconciliation — indefinitely. "+
			"Use clusterReplicaObserveBudget(...).", len(unbounded), unbounded)
	}
}
