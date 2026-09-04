package broker

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// G69 / #67 sub-face 4 — the join terminal gate must not declare SERVING before the JetStream meta can
// place an asset at the current target replica factor.
//
// FIXTURE NOTES (plan §6, all load-bearing):
//   - g3AdminWithSelf, NOT a bare NewClusterAdmin: the terminal arm calls deriveAndConvergeSeedsFromRoster,
//     and on a fixture where that errors every pin below would pass for the WRONG reason (the op would be
//     routed to BLOCKED by blockAfterAttempts rather than held by the gate under test).
//   - TopoTargetGen: 0 so topoConvergedForOp SHORT-CIRCUITS TRUE — these tests are about the SECOND
//     conjunct, so the first must be out of the way. Each test asserts that precondition explicitly:
//     if a future change removes the short-circuit, these would silently become no-gate tests.
//   - g3AdminWithSelf does not set admin.now; every test assigns it (topo_advance_test.go's idiom).
func jsGateAdmin(t *testing.T, opID string, deadline time.Time, now *time.Time) (*ClusterAdmin, *cluster.Operation) {
	t.Helper()
	admin := g3AdminWithSelf(t, "host.example")
	admin.now = func() time.Time { return *now }

	in := cluster.OpStartInput{
		OpID: opID, Kind: cluster.OpKindJoin, TargetNode: "joiner-1",
		InitState: cluster.OpStateNatsRolledOut, Confirmed: true,
		TopoTargetGen:   0, // ⇒ topoConvergedForOp returns true immediately (asserted below)
		CatchupDeadline: deadline.UnixNano(),
		Timeline:        `[{"s":"NATS_ROLLED_OUT"}]`,
	}
	if err := admin.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, *now)
	}); err != nil {
		t.Fatalf("seed %s: %v", opID, err)
	}
	op, err := cluster.OperationByID(admin.node.RODB(), opID)
	if err != nil || op == nil {
		t.Fatalf("read back seeded op: %v", err)
	}
	if ok, why := admin.topoConvergedForOp(op, true); !ok {
		t.Fatalf("fixture precondition failed: the TOPOLOGY gate must already be converged so these "+
			"tests exercise the JS-placement conjunct and nothing else (%s)", why)
	}
	return admin, op
}

func jsGateReload(t *testing.T, admin *ClusterAdmin, opID string) *cluster.Operation {
	t.Helper()
	op, err := cluster.OperationByID(admin.node.RODB(), opID)
	if err != nil || op == nil {
		t.Fatalf("reload op: %v", err)
	}
	return op
}

// joinSub is the substrate a join sees at NATS_ROLLED_OUT: the joiner has been promoted, so there are
// at least two voters.
func joinSub() substrate {
	return substrate{phase: "VOTER", inRaft: true, isVoter: true, numVoters: 2}
}

// TestJoinDoesNotServeWhileJSCannotPlace is P1, the RED-FIRST root-cause adjudication.
//
// Written against the pre-fix tree it MUST fail: driveJoin's OpStateNatsRolledOut branch has exactly one
// gate (topoAdvance, i.e. topology-generation convergence = "the rendered nats.conf rolled out and the
// live process loaded it") and no conjunct anywhere asks whether the JetStream meta can place an asset.
// So the op reaches terminal SERVING on tick 1 while the first CreateObjectStore(Replicas: N) can still
// fail — which is exactly what the deploy tier measured as "grow, then push, intermittently refused".
func TestJoinDoesNotServeWhileJSCannotPlace(t *testing.T) {
	now := time.Now()
	admin, op := jsGateAdmin(t, "op-js-hold", now.Add(2*time.Minute), &now)

	calls := 0
	admin.jsPlaceableFn = func() (bool, string) {
		calls++
		return false, "the JS meta has assigned 1/2 peers to the events stream"
	}

	for i := range 3 {
		admin.driveJoin(op, joinSub())
		op = jsGateReload(t, admin, "op-js-hold")
		if op.OpState == cluster.OpStateServing || op.Terminal {
			t.Fatalf("tick %d: the join reached %s (terminal=%v) while the JS meta could not place an "+
				"R=N asset. The terminal gate checks only topology-generation convergence — a loaded "+
				"nats.conf is not a placeable meta — so `cluster add` returns rc=0 at a moment when the "+
				"first tier-B push is still refused (gotcha #67 sub-face 4)", i+1, op.OpState, op.Terminal)
		}
		now = now.Add(5 * time.Second) // the observe-loop tick interval
	}
	if calls == 0 {
		t.Fatal("the placement probe was never consulted — the conjunct is not wired into driveJoin")
	}
	if !strings.Contains(op.LastError, "assigned 1/2") {
		t.Fatalf("an operator reading `cluster ops show` must see WHY the join is waiting; last_error=%q", op.LastError)
	}
}

// TestJoinAdvancesAtDeadlineInsteadOfWedging is P2 — the #45 pin, and the most important one here.
// The gate is a BOUNDED WAIT whose expiry outcome is ADVANCE. Turning it into a fail-closed hold (or
// into an OpStateBlocked escalation) reddens this.
func TestJoinAdvancesAtDeadlineInsteadOfWedging(t *testing.T) {
	now := time.Now()
	// The waiting phase below must stay OUTSIDE jsGateExpiryReserve (the G-1 fix degrades early on
	// purpose), so the window has to be comfortably longer than the reserve.
	deadline := now.Add(jsGateExpiryReserve + 2*time.Minute)
	admin, op := jsGateAdmin(t, "op-js-deadline", deadline, &now)
	admin.jsPlaceableFn = func() (bool, string) { return false, "the JS meta has assigned 1/2 peers to the events stream" }

	// Wait ticks BEFORE the deadline must cost no raft writes: the conjunct sits ahead of both
	// Propose-bearing steps, and recordOpError is change-gated on op.LastError.
	admin.driveJoin(op, joinSub())
	op = jsGateReload(t, admin, "op-js-deadline")
	idxBefore, err := admin.node.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		now = now.Add(time.Second)
		admin.driveJoin(op, joinSub())
		op = jsGateReload(t, admin, "op-js-deadline")
	}
	idxAfter, err := admin.node.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if op.Terminal {
		t.Fatal("went terminal before the deadline")
	}
	if idxAfter != idxBefore {
		t.Fatalf("waiting burned %d raft writes (%d -> %d) — an idle gate must not propose; the "+
			"conjunct must sit ahead of both Propose steps and recordOpError must stay change-gated",
			idxAfter-idxBefore, idxBefore, idxAfter)
	}

	// Past the deadline: ADVANCE, with the evidence preserved.
	now = deadline.Add(time.Second)
	admin.driveJoin(op, joinSub())
	op = jsGateReload(t, admin, "op-js-deadline")

	if op.OpState != cluster.OpStateServing || !op.Terminal {
		t.Fatalf("at the deadline the join must ADVANCE to terminal SERVING, got %s (terminal=%v). "+
			"Holding it would recreate gotcha #45: assertNoActiveOp then fences the next membership "+
			"operation and the grow/shrink spine wedges with nothing an operator can act on. "+
			"Escalating to BLOCKED is worse still — --auto-confirm-catchup defaults to 0, so the very "+
			"first blocked poll reports budget-exhausted and `cluster add` fails with the wrong cause",
			op.OpState, op.Terminal)
	}
	if !strings.Contains(op.Timeline, "WITHOUT proving JetStream placement") {
		t.Fatalf("advancing unproven must leave a DURABLE record — it is the only evidence that this "+
			"grow shipped a SERVING we could not prove, and the only way the added latency ever gets "+
			"measured. timeline=%q", op.Timeline)
	}
}

// TestJoinServesOncePlacementIsProven is P3, the happy path: the gate must actually open.
func TestJoinServesOncePlacementIsProven(t *testing.T) {
	now := time.Now()
	admin, op := jsGateAdmin(t, "op-js-happy", now.Add(2*time.Minute), &now)

	calls := 0
	admin.jsPlaceableFn = func() (bool, string) {
		calls++
		if calls < 3 {
			return false, "the JS meta has assigned 1/2 peers to the events stream"
		}
		return true, ""
	}

	for range 3 {
		admin.driveJoin(op, joinSub())
		op = jsGateReload(t, admin, "op-js-happy")
		now = now.Add(5 * time.Second)
	}
	if op.OpState != cluster.OpStateServing || !op.Terminal {
		t.Fatalf("once placement is proven the join must reach terminal SERVING, got %s (terminal=%v) "+
			"— a gate that never opens is worse than no gate", op.OpState, op.Terminal)
	}
	// G-14 (internal review): plan §6 mandated both of these and the first version dropped them. Without
	// them a regression that carries the stale "waiting for the JetStream meta …" into a terminal SERVING
	// — a scary last_error on a SUCCESSFUL grow — goes unnoticed.
	if op.LastError != "" {
		t.Fatalf("a successful grow must not carry a last_error; got %q", op.LastError)
	}
	var growMarker string
	_ = admin.node.RODB().QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`,
		cluster.MetaKeyGrowActive).Scan(&growMarker)
	if growMarker != "" {
		t.Fatalf("the cluster-add grow marker must be released by the terminal tick, still held by %q — "+
			"leaving it serializes the whole membership plane behind a finished grow", growMarker)
	}
	before := calls
	admin.driveJoin(op, joinSub())
	if calls != before {
		t.Fatal("the probe must not be consulted after the op is terminal")
	}
}

// TestJoinZeroDeadlineAdvances is P4: legacy / hand-seeded rows carry CatchupDeadline == 0. Treating
// that as "no bound" would be an unbounded hold — the #45 shape — so it must advance.
func TestJoinZeroDeadlineAdvances(t *testing.T) {
	now := time.Now()
	admin, op := jsGateAdmin(t, "op-js-zero", time.Unix(0, 0), &now)
	if op.CatchupDeadline != 0 {
		t.Skipf("fixture did not produce a zero deadline (got %d)", op.CatchupDeadline)
	}
	admin.jsPlaceableFn = func() (bool, string) { return false, "never placeable" }

	admin.driveJoin(op, joinSub())
	op = jsGateReload(t, admin, "op-js-zero")
	if !op.Terminal {
		t.Fatal("a zero CatchupDeadline must be treated as EXPIRED and advance, never as an unbounded wait")
	}
}

// TestRetireNeverConsultsThePlacementGate is P6: the conjunct is join-only. A panicking probe proves
// the retire ladder cannot reach it — retire's own gate (topoAdvance, including the deliberate N=1
// de-cluster carve-out drill 41 pins) must be untouched.
func TestRetireNeverConsultsThePlacementGate(t *testing.T) {
	now := time.Now()
	admin, _ := jsGateAdmin(t, "op-js-unused", now.Add(time.Minute), &now)
	admin.jsPlaceableFn = func() (bool, string) {
		panic("the JS placement gate must never be consulted on the retire path")
	}

	in := cluster.OpStartInput{
		OpID: "op-retire-js", Kind: cluster.OpKindRetire, TargetNode: "leaver-1",
		InitState: cluster.OpStateNatsRolledOut, Confirmed: true,
		// G-5 (internal review): this was seeded 9999, which made topoAdvance return false and driveRetire
		// exit ONE LINE before any code where a mis-placed conjunct could live — the panic was unreachable
		// and the pin was vacuous. 0 short-circuits the topology gate so the ladder actually ENTERS the
		// terminal arm, and the terminal assertion below stops it silently re-becoming vacuous.
		TopoTargetGen: 0, CatchupDeadline: now.Add(time.Minute).UnixNano(),
		Timeline: `[{"s":"NATS_ROLLED_OUT"}]`,
	}
	if err := admin.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, now)
	}); err != nil {
		t.Fatalf("seed retire op: %v", err)
	}
	rop := jsGateReload(t, admin, "op-retire-js")
	admin.driveRetire(rop, substrate{phase: "VOTER", inRaft: true, isVoter: true, numVoters: 2})

	rop = jsGateReload(t, admin, "op-retire-js")
	if rop.OpState == cluster.OpStateNatsRolledOut && !rop.Terminal {
		t.Fatalf("the retire ladder never left NATS_ROLLED_OUT, so it did not reach the terminal arm and "+
			"this pin proves nothing about where the join conjunct lives (state=%s) — the vacuity G-5 "+
			"found", rop.OpState)
	}
}

// TestJSPlaceableFromPredicate is P8: the pure predicate, no broker and no JetStream. Every row names
// the mutation it kills.
func TestJSPlaceableFromPredicate(t *testing.T) {
	st := func(assigned, configured, actual int) jsstream.StreamReplicaState {
		return jsstream.StreamReplicaState{
			Name: "events", Target: 2, Assigned: assigned, Configured: configured,
			Actual: actual, Ready: actual >= 2,
		}
	}
	for _, tc := range []struct {
		name   string
		target int
		st     jsstream.StreamReplicaState
		obsErr error
		want   bool
		kills  string
	}{
		{"N=1 needs no placement at all", 1, jsstream.StreamReplicaState{}, nil, true,
			"the target<=1 short-circuit — without it a force-single/N=1 broker waits for a placement that is not a thing"},
		{"unobservable events stream is NOT placeable", 2, jsstream.StreamReplicaState{}, errors.New("lookup events: not found"), false,
			"folding an observation error into 'placeable' (fail-OPEN), which would make the gate decorative"},
		{"the replica raise has not landed yet", 2, st(2, 1, 2), nil, false,
			"the Configured check — a stream still configured at R=1 proves nothing about R=2 placement"},
		{"assigned below target is NOT placeable", 2, st(1, 2, 1), nil, false,
			"the Assigned check, i.e. the entire gate"},
		{"ASSIGNED BUT NOT CAUGHT UP *IS* placeable", 2, st(2, 2, 1), nil, true,
			"the whole design decision: gating on catch-up instead of assignment would make every grow " +
				"wait for a full byte copy (events alone is capped at 1 GiB) — a false-block of a healthy grow"},
		{"fully converged is placeable", 2, st(2, 2, 2), nil, true, "an inverted predicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, detail := jsPlaceableFrom(tc.target, tc.st, tc.obsErr)
			if got != tc.want {
				t.Fatalf("jsPlaceableFrom = %v (%s), want %v — this row kills: %s", got, detail, tc.want, tc.kills)
			}
			if !got && detail == "" {
				t.Fatal("a refusal must carry a reason an operator can read in `cluster ops show`")
			}
		})
	}
}

// TestAssignedReplicasIgnoresCatchUp is P10: AssignedReplicas must count peers the META placed,
// regardless of whether they have caught up. Collapsing it onto ActualReplicas reddens this.
func TestAssignedReplicasIgnoresCatchUp(t *testing.T) {
	info := &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{
		Replicas: []*jetstream.PeerInfo{
			{Name: "b", Current: false, Offline: false}, // assigned, still copying
			{Name: "c", Current: true, Offline: false},
		},
	}}
	if got := jsstream.AssignedReplicas(info); got != 3 {
		t.Fatalf("AssignedReplicas = %d, want 3 (1 self + 2 assigned peers) — a peer that is assigned "+
			"but not yet Current ALREADY proves the meta could place the group, which is the whole "+
			"reason this is not ActualReplicas", got)
	}
	if got := jsstream.ActualReplicas(info); got != 2 {
		t.Fatalf("ActualReplicas = %d, want 2 — it must stay Current-gated; retire depends on it", got)
	}
	if got := jsstream.AssignedReplicas(nil); got != 0 {
		t.Fatalf("nil info must be 0, got %d", got)
	}
	if got := jsstream.AssignedReplicas(&jetstream.StreamInfo{}); got != 1 {
		t.Fatalf("a non-clustered stream is its own whole group, want 1, got %d", got)
	}
}

// TestJSPlacementGateIsWired is P9, the source pin. wireClusterLate needs raft and cannot run
// hermetically, and the seam is fail-OPEN when nil — so a half-wiring would silently disable the whole
// gate with every test green. This repo has been bitten by exactly that twice.
func TestJSPlacementGateIsWired(t *testing.T) {
	src, err := os.ReadFile("clusterwrite.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "admin.jsPlaceableFn = b.clusterJSPlaceable") {
		t.Fatal("wireClusterLate must assign jsPlaceableFn — the seam is fail-open when nil, so an " +
			"unwired build ships the pre-G69 behaviour with every hermetic pin still green")
	}
}

// TestJoinDegradesBeforeTopoAdvanceCanBlock is the G-1 pin — the internal review's BLOCKER, which the
// main process shipped and did not see.
//
// The conjunct in isolation always advances at its deadline. The LADDER did not: holding the op alive
// at NATS_ROLLED_OUT keeps topoAdvance (fail-CLOSED, expiry branch = OpStateBlocked) re-evaluated every
// tick, and it runs FIRST. Pre-G69 a join went terminal on the first converged tick and was immune.
// The exposure is correlated, not exotic — topoConvergedForOp needs each voter's TopoReported, set only
// when that node answered THAT tick's health scatter-gather, and the saturated host that makes the JS
// meta slow to place is the same host that drops a health reply. Landing in BLOCKED means
// blockedConfirmDecision(0,0,false) reports budget-exhausted on the FIRST poll: `cluster add` exits
// non-zero with the WRONG cause and the membership plane is fenced.
//
// So: with the JS gate still refusing AND topology convergence flipping false late in the window, the
// op must reach terminal SERVING — never BLOCKED.
func TestJoinDegradesBeforeTopoAdvanceCanBlock(t *testing.T) {
	now := time.Now()
	deadline := now.Add(2 * time.Minute)
	admin, op := jsGateAdmin(t, "op-js-reserve", deadline, &now)
	admin.jsPlaceableFn = func() (bool, string) { return false, "the JS meta has assigned 1/2 peers to the events stream" }

	// Drive to just inside the reserve window: the JS conjunct must degrade here, BEFORE topoAdvance's
	// own expiry branch (which fires only after the full deadline) can ever be reached.
	now = deadline.Add(-jsGateExpiryReserve + time.Second)
	admin.driveJoin(op, joinSub())
	op = jsGateReload(t, admin, "op-js-reserve")

	if op.OpState == cluster.OpStateBlocked {
		t.Fatal("the composed gate landed in BLOCKED — `cluster add` would then exit non-zero with the " +
			"wrong causal string and assertNoActiveOp would fence the membership plane, which is exactly " +
			"the outcome the plan's OQ-1 rejected")
	}
	if op.OpState != cluster.OpStateServing || !op.Terminal {
		t.Fatalf("the JS conjunct must degrade to terminal SERVING inside the reserve window (%v before "+
			"the deadline), so the degrade tick always precedes any tick on which the fail-closed "+
			"topology gate could expire; got %s (terminal=%v)", jsGateExpiryReserve, op.OpState, op.Terminal)
	}
	if !strings.Contains(op.Timeline, "WITHOUT proving JetStream placement") {
		t.Fatalf("the degrade must still be recorded; timeline=%q", op.Timeline)
	}
}

// TestJSGateReserveExceedsTwoObserveTicks pins the constant relation the G-1 fix rests on: the reserve
// must cover at least two observe ticks, or the degrade tick is not guaranteed to precede the expiry
// tick and the BLOCKER is reachable again.
func TestJSGateReserveExceedsTwoObserveTicks(t *testing.T) {
	if jsGateExpiryReserve < 2*observeTickInterval {
		t.Fatalf("jsGateExpiryReserve (%v) must be >= 2 observe ticks (%v): the controller only evaluates "+
			"this ladder once per tick, so a smaller reserve can let topoAdvance's fail-closed expiry "+
			"branch fire on the same tick the JS conjunct meant to degrade",
			jsGateExpiryReserve, 2*observeTickInterval)
	}
	if jsGateExpiryReserve >= opTopoConvergeTimeout {
		t.Fatalf("the reserve (%v) must be a fraction of the window it reserves from (%v), or the JS gate "+
			"never gets to wait at all", jsGateExpiryReserve, opTopoConvergeTimeout)
	}
}

// TestClusterJSPlaceableProbe closes G-6: P8 tested the pure predicate GIVEN a state and P10 tested
// AssignedReplicas GIVEN an info, but nothing tested the production code that decides WHICH stream and
// WHICH target to ask about, nor that CollectStreamState actually populates the two new fields. All of
// that could be deleted or mis-wired with every other pin green.
func TestClusterJSPlaceableProbe(t *testing.T) {
	t.Run("N=1 issues no JS call at all", func(t *testing.T) {
		n, _ := d7SingleNode(t, "solo")
		b := &Broker{cfg: Config{Logger: silentLogger(), Now: time.Now}}
		b.cl = &clusterRuntime{node: n}
		setBrokerJS(b, nil) // a JS call would nil-panic; reaching one is the failure this asserts against
		ok, detail := b.clusterJSPlaceable()
		if !ok || detail != "" {
			t.Fatalf("a single-voter cluster has nothing to place and must short-circuit BEFORE touching "+
				"JetStream; got ok=%v detail=%q", ok, detail)
		}
	})
}

// TestCollectStreamStatePopulatesPlacementFields closes the other half of G-6: the population layer.
// Deleting the two assignments in CollectStreamState leaves every predicate test green while the gate
// silently sees Assigned=0 and blocks every grow until its deadline.
func TestCollectStreamStatePopulatesPlacementFields(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "g69probe", Subjects: []string{"g69.probe"}, Replicas: 1,
	}); err != nil {
		t.Skipf("embedded server cannot host the probe stream: %v", err)
	}

	st, err := jsstream.CollectStreamState(ctx, js, "g69probe", 1)
	if err != nil {
		t.Fatalf("CollectStreamState: %v", err)
	}
	if st.Configured != 1 {
		t.Fatalf("Configured must come from the live StreamConfig.Replicas, got %d — if this is 0 the "+
			"gate's Configured<target rule blocks every grow until its deadline", st.Configured)
	}
	if st.Assigned < 1 {
		t.Fatalf("Assigned must be populated from the live cluster info, got %d — if this is 0 the gate "+
			"never opens on its own and every grow degrades", st.Assigned)
	}
	if !st.Ready || st.Actual != 1 {
		t.Fatalf("the PRE-EXISTING fields must be byte-unchanged (retire depends on them): actual=%d ready=%v",
			st.Actual, st.Ready)
	}
}

// TestJSPlacementSeamIsFailOpenWhenUnwired is G-15(ii): the nil-seam contract is load-bearing (dozens
// of unit paths build a bare ClusterAdmin, and a half-wired production build would otherwise hold every
// join until its deadline) — and flipping it to fail-CLOSED survived the entire suite.
func TestJSPlacementSeamIsFailOpenWhenUnwired(t *testing.T) {
	now := time.Now()
	admin, op := jsGateAdmin(t, "op-js-nilseam", now.Add(2*time.Minute), &now)
	admin.jsPlaceableFn = nil // explicitly unwired

	admin.driveJoin(op, joinSub())
	op = jsGateReload(t, admin, "op-js-nilseam")
	if op.OpState != cluster.OpStateServing || !op.Terminal {
		t.Fatalf("an UNWIRED seam must be fail-OPEN (today's behaviour), got %s (terminal=%v). "+
			"Fail-closed here would hold every join in every bare-admin unit path — and a half-wired "+
			"production build would hold every real grow until its deadline", op.OpState, op.Terminal)
	}
}

// TestJSPlacementRunsAfterTopologyConvergence is G-15(iii): the conjunct's position RELATIVE to
// topoAdvance was unpinned — moving it earlier survived. Order matters for which reason the operator
// sees: asking about JetStream placement before the conf has even rolled out would mask the real
// blocker ("topology convergence: voter x at gen N < M") behind a JS message.
func TestJSPlacementRunsAfterTopologyConvergence(t *testing.T) {
	now := time.Now()
	admin := g3AdminWithSelf(t, "host.example")
	admin.now = func() time.Time { return now }
	called := false
	admin.jsPlaceableFn = func() (bool, string) { called = true; return false, "js not ready" }

	in := cluster.OpStartInput{
		OpID: "op-js-order", Kind: cluster.OpKindJoin, TargetNode: "joiner-1",
		InitState: cluster.OpStateNatsRolledOut, Confirmed: true,
		TopoTargetGen:   9999, // NOT converged: topoAdvance must refuse first
		CatchupDeadline: now.Add(2 * time.Minute).UnixNano(),
		Timeline:        `[{"s":"NATS_ROLLED_OUT"}]`,
	}
	if err := admin.node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.PlanClusterOpStart(in, now)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	op := jsGateReload(t, admin, "op-js-order")
	admin.driveJoin(op, joinSub())

	if called {
		t.Fatal("the JS placement probe ran while TOPOLOGY had not converged — the operator would then " +
			"be told about JetStream while the real blocker is that the rendered nats.conf has not " +
			"rolled out to every voter")
	}
	op = jsGateReload(t, admin, "op-js-order")
	if !strings.Contains(op.LastError, "topology") {
		t.Fatalf("the recorded reason must be the topology one, got %q", op.LastError)
	}
}

// TestPlacementCanaryMeasuresRatherThanInfers is external review F3's direct-measurement pin, on a REAL
// embedded JetStream rather than a stub.
//
// The gate's cheap half (events Configured/Assigned) is a PROXY: the review showed it can be satisfied
// by an assignment that says nothing about now, because tether never issues a JS peer-remove. The
// canary asks the meta the exact question the CLI contract promises — "can a NEW R=N asset be created
// right now" — and must leave nothing behind.
func TestPlacementCanaryMeasuresRatherThanInfers(t *testing.T) {
	url := testharness.StartJSNATS(t) // a JetStream-ENABLED embedded server; startNATS has JS off
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// R=1 is placeable on a single embedded server: the probe must SUCCEED and clean up after itself.
	if err := jsstream.ProbeMetaCanPlace(ctx, js, 1); err != nil {
		t.Fatalf("a single-node meta must be able to place an R=1 asset: %v", err)
	}
	if _, serr := js.Stream(ctx, jsstream.PlacementCanaryStreamName); serr == nil {
		t.Fatal("the canary must be DELETED after the probe — a stream that lingers would accumulate one " +
			"per gate evaluation and pollute the operator's stream list")
	}

	// R=3 is NOT placeable on a single server: the probe must REPORT that rather than infer success.
	// This is the half a proxy cannot do — the events stream's assignment counts would be unchanged.
	if err := jsstream.ProbeMetaCanPlace(ctx, js, 3); err == nil {
		t.Fatal("a single-node meta cannot place an R=3 asset, so the probe must fail — if it passes, the " +
			"gate is inferring rather than measuring and `cluster add` can still return 0 while the first " +
			"CreateObjectStore(Replicas:3) fails")
	}
	// A failed probe must not leave a partial canary behind either.
	if _, serr := js.Stream(ctx, jsstream.PlacementCanaryStreamName); serr == nil {
		t.Fatal("a FAILED probe must leave no canary behind")
	}
}
