package main

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/natsconf"
)

// cluster_topo_render_test.go — the ctl-side topology surfaces: the TOPO column (topoCell), the
// status card headline (cardTopReason), `cluster doctor`, and `reconcile nats --wait`.
//
// Before batch C, topoCell was the ONLY one of the four that classified at all, it did so with its
// own substring set, and that set was missing the render-failure marker the broker side had. The
// other three had no topology branch whatsoever. These tests pin all four onto the shared classifier.

const (
	tcReasonRenderFailed = "render (nats.conf could not be assembled — fix the conf/secrets, or `cluster reconcile nats --manual`): boom"
	tcReasonApplyFailed  = "apply: no space left on device"
	tcReasonUnknownDir   = "nats.conf has an unrecognized directive — fix it, or `cluster reconcile nats --manual`: boom"
	tcReasonCutover      = "standalone→clustered cutover rendered + validated but WITHHELD — a reconciler SIGHUP cannot safely cross it; run `tether cluster add <this-broker>` to perform the coordinated restart"
)

func tcVoter(id string, observed uint64, action, reason string) adminsock.ClusterNodeStatus {
	return adminsock.ClusterNodeStatus{
		NodeID: id, Phase: "VOTER", Role: "voter", Reachable: true, ReachSource: "nats-health",
		TopoReported: true, TopoApplied: observed, TopoObserved: observed,
		TopoAction: action, TopoReconcileReason: reason,
	}
}

// TestTopoCellRendersEveryAction pins the TOPO column token for each reconcile action.
//
// Mutation: revert topoCell to its own substring switch — the "render failure" and "apply failure"
// rows redden, because those are exactly the two markers the old ctl-side set was missing.
func TestTopoCellRendersEveryAction(t *testing.T) {
	const desired = uint64(7)
	rows := []struct {
		name     string
		observed uint64
		action   string
		reason   string
		want     string
	}{
		{"converged", 7, natsconf.ActionNoop, "", "7/7→7 ✓"},
		{"reloaded and converged", 7, natsconf.ActionReloaded, "", "7/7→7 ✓"},
		{"behind", 4, natsconf.ActionSwappedReloadPending, "awaiting reload", "4/4→7 …"},
		{"unresolvable is still catching up", 0, natsconf.ActionUnresolvable, "no mesh peers known yet (converging)", "0/0→7 …"},
		{"render failure", 4, natsconf.ActionRejected, tcReasonRenderFailed, "4/4→7 STUCK"},
		{"apply failure", 4, natsconf.ActionRejected, tcReasonApplyFailed, "4/4→7 STUCK"},
		{"unknown directive", 4, natsconf.ActionUnknownDirective, tcReasonUnknownDir, "4/4→7 STUCK"},
		{"withheld cutover", 4, natsconf.ActionAwaitingClusteredCutover, tcReasonCutover, "4/4→7 HOLD"},
		{"action from a newer release", 4, "some_future_action", "", "4/4→7 ?"},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			got := topoCell(tcVoter("b", r.observed, r.action, r.reason), desired)
			if got != r.want {
				t.Fatalf("topoCell = %q, want %q", got, r.want)
			}
		})
	}
	if got := topoCell(adminsock.ClusterNodeStatus{NodeID: "old"}, desired); got != "-" {
		t.Errorf("a node that does not report topology must render %q, got %q", "-", got)
	}
	if got := topoCell(tcVoter("b", 0, natsconf.ActionNoop, ""), 0); got != "-" {
		t.Errorf("desired==0 (nothing managed) must render %q, got %q", "-", got)
	}
}

// TestTopoCellNeverGreensAWedgedReconcile is the polarity guard: no state that needs an operator may
// render the converged tick, at ANY generation relation. The "observed == desired" rows are the
// wedged-after-converging shape that the broker side used to miss entirely.
//
// Mutation: gate the Stuck/Held classification on observed < desired — the observed==desired rows
// start rendering "✓" and this reddens.
func TestTopoCellNeverGreensAWedgedReconcile(t *testing.T) {
	const desired = uint64(5)
	for _, tc := range []struct {
		name   string
		action string
		reason string
	}{
		{"render failure", natsconf.ActionRejected, tcReasonRenderFailed},
		{"apply failure", natsconf.ActionRejected, tcReasonApplyFailed},
		{"unknown directive", natsconf.ActionUnknownDirective, tcReasonUnknownDir},
		{"withheld cutover", natsconf.ActionAwaitingClusteredCutover, tcReasonCutover},
	} {
		for _, observed := range []uint64{0, desired - 1, desired, desired + 1} {
			cell := topoCell(tcVoter("b", observed, tc.action, tc.reason), desired)
			if strings.Contains(cell, "✓") || strings.HasSuffix(cell, "…") {
				t.Errorf("%s at observed=%d rendered %q — a reconcile that needs an operator must "+
					"never read as converged or as still-catching-up", tc.name, observed, cell)
			}
		}
	}
}

// TestTopoCellLegacyBrokerClassifiesRenderFailure is THE original defect, at the surface where it
// shipped: a broker that sends no TopoAction (pre-batch-C) reporting a render failure. The ctl-side
// substring set had "unrecognized directive" and "nats-server -t" but NOT the render marker, so this
// rendered "…" — telling the operator to wait for a self-heal that was never coming, while the
// broker's own verdict correctly said DEGRADED/STUCK.
//
// Mutation: drop the render marker from classifyLegacyReason — reddens.
func TestTopoCellLegacyBrokerClassifiesRenderFailure(t *testing.T) {
	const desired = uint64(7)
	for _, tc := range []struct {
		name   string
		reason string
		want   string
	}{
		{"render failure", tcReasonRenderFailed, "STUCK"},
		{"apply failure", tcReasonApplyFailed, "STUCK"},
		{"unknown directive", tcReasonUnknownDir, "STUCK"},
		{"withheld cutover", tcReasonCutover, "HOLD"},
	} {
		// TopoAction deliberately empty: an older broker on the other end of the mixed-version window.
		got := topoCell(tcVoter("b", 4, "", tc.reason), desired)
		if !strings.HasSuffix(got, tc.want) {
			t.Errorf("legacy %s: topoCell = %q, want it to end in %q", tc.name, got, tc.want)
		}
	}
}

// TestCardTopReasonNamesTopology pins the status-card headline. It had NO topology branch, so a
// cluster whose only degraded trigger was a wedged reconcile fell through to the generic "fault
// tolerance reduced — see the table" fallback: not merely vague but FALSE (fault tolerance was
// fine), and it points at `cluster add` instead of at that broker's nats.conf.
//
// Mutation: delete the topology loop from cardTopReason — reddens.
func TestCardTopReasonNamesTopology(t *testing.T) {
	rep := &adminsock.ClusterStatusReport{
		TopoDesired: 7,
		Nodes: []adminsock.ClusterNodeStatus{
			tcVoter("a", 7, natsconf.ActionNoop, ""),
			tcVoter("b", 4, natsconf.ActionRejected, tcReasonRenderFailed),
			tcVoter("c", 7, natsconf.ActionNoop, ""),
		},
	}
	got := cardTopReason(rep)
	if !strings.Contains(got, "b") || !strings.Contains(got, "STUCK") {
		t.Fatalf("card headline = %q, want it to name broker b and STUCK", got)
	}
	if strings.Contains(got, "fault tolerance") {
		t.Fatalf("card headline fell through to the fault-tolerance fallback (%q) — fault tolerance "+
			"is fine; the topology reconcile is wedged, and that fallback sends the operator to "+
			"`cluster add` instead of to the conf", got)
	}

	// A withheld cutover must name `cluster add`, because THAT one really is the remedy.
	rep.Nodes[1] = tcVoter("b", 4, natsconf.ActionAwaitingClusteredCutover, tcReasonCutover)
	if got := cardTopReason(rep); !strings.Contains(got, "cluster add") {
		t.Fatalf("a withheld cutover's headline = %q, want it to name `cluster add`", got)
	}

	// No topology problem → the topology branch must not steal the headline from a real one.
	rep.Nodes[1] = tcVoter("b", 7, natsconf.ActionNoop, "")
	rep.Nodes[2].Inconsistent = true
	if got := cardTopReason(rep); !strings.Contains(got, "INCONSISTENT") {
		t.Fatalf("with topology healthy the headline must be the INCONSISTENT node, got %q", got)
	}
}

// TestClusterDoctorTopologyCheck: `cluster doctor` had no topology item at all, so it PASSed every
// check on a cluster with a permanently fail-closed reconciler. STUCK is FATAL (no further topology
// change on that broker can land, so the next grow/retire will hang); HELD is ADVISORY and must
// route to `cluster add`, never to the `--manual` remedy.
//
// Mutation: delete the topology switch from clusterDoctorOnline — reddens.
func TestClusterDoctorTopologyCheck(t *testing.T) {
	find := func(checks []clusteroffline.DoctorCheck, name string) (clusteroffline.DoctorCheck, bool) {
		for _, c := range checks {
			if c.Name == name {
				return c, true
			}
		}
		return clusteroffline.DoctorCheck{}, false
	}
	base := func(n adminsock.ClusterNodeStatus) *adminsock.ClusterStatusReport {
		return &adminsock.ClusterStatusReport{
			LeaderID: "a", Health: "DEGRADED", TopoDesired: 7,
			Nodes: []adminsock.ClusterNodeStatus{tcVoter("a", 7, natsconf.ActionNoop, ""), n},
		}
	}

	stuck, ok := find(clusterDoctorOnline(base(tcVoter("b", 4, natsconf.ActionRejected, tcReasonApplyFailed))), "topology")
	if !ok {
		t.Fatal("`cluster doctor` has no `topology` check — it PASSes every item on a cluster whose reconciler is wedged")
	}
	if stuck.Status != clusteroffline.DoctorFatal {
		t.Errorf("a STUCK reconcile must be FATAL, got %v (%s)", stuck.Status, stuck.Detail)
	}
	if !strings.Contains(stuck.Detail, "b") {
		t.Errorf("the topology check must name the wedged broker, got %q", stuck.Detail)
	}

	held, _ := find(clusterDoctorOnline(base(tcVoter("b", 4, natsconf.ActionAwaitingClusteredCutover, tcReasonCutover))), "topology")
	if held.Status != clusteroffline.DoctorAdvisory {
		t.Errorf("a WITHHELD cutover is not a fault: want ADVISORY, got %v (%s)", held.Status, held.Detail)
	}
	if !strings.Contains(held.Detail, "cluster add") || !strings.Contains(held.Detail, "do NOT run") {
		t.Errorf("a WITHHELD cutover must route to `cluster add` AND warn off `--manual`, got %q", held.Detail)
	}

	okCheck, _ := find(clusterDoctorOnline(base(tcVoter("b", 7, natsconf.ActionNoop, ""))), "topology")
	if okCheck.Status != clusteroffline.DoctorPass {
		t.Errorf("a converged cluster must PASS the topology check, got %v (%s)", okCheck.Status, okCheck.Detail)
	}

	// origin: batch-c external review D11. ClassifyTopo explicitly says both states degrade:
	// Behind is not converged yet, and UnknownAction is fail-closed because this ctl cannot know
	// whether the peer can self-heal. Calling either one PASS makes `cluster doctor` contradict the
	// authoritative health banner and its own detail ("every ... is converging").
	for _, tc := range []struct {
		name string
		node adminsock.ClusterNodeStatus
	}{
		{"behind", tcVoter("b", 4, natsconf.ActionSwappedReloadPending, "awaiting reload")},
		{"unknown action", tcVoter("b", 4, "some_future_action", "")},
	} {
		t.Run(tc.name+" is not pass", func(t *testing.T) {
			got, found := find(clusterDoctorOnline(base(tc.node)), "topology")
			if !found {
				t.Fatal("`cluster doctor` omitted the topology check")
			}
			if got.Status == clusteroffline.DoctorPass {
				t.Fatalf("%s topology was reported PASS (%q), but ClassifyTopo marks it degrading and "+
					"the cluster health report is DEGRADED", tc.name, got.Detail)
			}
		})
	}
}

// TestTopoWedgedStopsTheWait: `reconcile nats --wait` polls until its deadline and then exits 75
// (EX_TEMPFAIL). For a conf that will not validate, or a cutover awaiting `cluster add`, that is the
// same misdirection C3 removes at the status layer — one layer down, and it tells a retry loop to
// keep waiting. topoWedged is what lets the poll bail out with the remedy.
//
// Mutation: make topoWedged ignore TopoHeld (or return nil) — reddens.
func TestTopoWedgedStopsTheWait(t *testing.T) {
	rep := &adminsock.ClusterStatusReport{
		TopoDesired: 7,
		Nodes: []adminsock.ClusterNodeStatus{
			tcVoter("a", 7, natsconf.ActionNoop, ""),
			tcVoter("b", 4, natsconf.ActionSwappedReloadPending, "awaiting reload"),
		},
	}
	// A merely-behind cluster must NOT bail — waiting is exactly right there.
	if nodes, _ := topoWedged(rep); len(nodes) != 0 {
		t.Fatalf("a behind-but-converging cluster must keep waiting, got wedged=%v", nodes)
	}
	if got := topoLaggards(rep); len(got) != 1 || !strings.Contains(got[0], "behind") {
		t.Fatalf("laggards = %v, want one entry naming the behind state", got)
	}

	for _, tc := range []struct {
		name   string
		action string
		reason string
		want   natsconf.TopoState
		remedy string
	}{
		{"stuck", natsconf.ActionRejected, tcReasonApplyFailed, natsconf.TopoStuck, "nats.conf"},
		{"held", natsconf.ActionAwaitingClusteredCutover, tcReasonCutover, natsconf.TopoHeld, "cluster add"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep.Nodes[1] = tcVoter("b", 4, tc.action, tc.reason)
			nodes, worst := topoWedged(rep)
			if len(nodes) != 1 || !strings.Contains(nodes[0], "b") {
				t.Fatalf("wedged = %v, want one entry naming broker b", nodes)
			}
			if worst != tc.want {
				t.Fatalf("worst = %s, want %s", worst, tc.want)
			}
			if !strings.Contains(worst.NextStep(), tc.remedy) {
				t.Fatalf("the bail-out remedy %q does not name %q", worst.NextStep(), tc.remedy)
			}
		})
	}

	// An UNREACHABLE voter must not be called wedged — its report is stale, and the unreachable
	// branch of topoLaggards already owns that row.
	rep.Nodes[1] = tcVoter("b", 4, natsconf.ActionRejected, tcReasonApplyFailed)
	rep.Nodes[1].Reachable = false
	if nodes, _ := topoWedged(rep); len(nodes) != 0 {
		t.Fatalf("an unreachable voter's stale report must not bail the wait, got %v", nodes)
	}
}

// TestDeclusterNoteDropsItsCausalClaimWhenThePruneFailed (batch C / C1).
//
// The force-single success message used to end with "(the abandoned peers are already pruned, so the
// N=1 proof now passes)". That is not a description, it is a CAUSAL INSTRUCTION: it tells the
// operator the next command will work and names the reason. When the prune failed it was simply
// false — a ghost roster row carries no raft role, and `reconcile nats --to-standalone` refuses on an
// unrecognized role — so an operator following it got an error about a raft role and no path onward.
//
// Mutation: return the same note on both branches — reddens.
func TestDeclusterNoteDropsItsCausalClaimWhenThePruneFailed(t *testing.T) {
	// origin: batch-c internal review C1-F2 — the note used to key on "is there a retry op", but there
	// are THREE outcomes and an op id separates only two. The reassuring clause printed on the worst
	// branch (prune failed AND no retry op), which is where an operator following it walks straight
	// into `--to-standalone` refusing. It keys on prune completion now.
	success := declusterPruneNote(false)
	if !strings.Contains(success, "already pruned") {
		t.Errorf("on the clean path the note should still explain WHY the N=1 proof now passes; got %q", success)
	}
	failed := declusterPruneNote(true)
	if strings.Contains(failed, "already pruned") {
		t.Errorf("the note still claims the peers are pruned while a finalize op is retrying them (%q); "+
			"an operator following it hits `unrecognized raft role \"\"` from --to-standalone", failed)
	}
	if !strings.Contains(failed, "abandoned roster row is gone") {
		t.Errorf("the failure-branch note must name the real precondition (the rows being gone), not the "+
			"existence of an operation — there is a branch with no operation at all; got %q", failed)
	}
	if success == failed {
		t.Error("both branches print the same note — the whole point is that only one of them is true")
	}
}

// TestCardTopReasonFoldsBySeverityNotNodeOrder.
//
// origin: batch-c internal review tests-F9. cardTopReason returned the FIRST degraded node in table
// order, while computeHealth — the banner this card sits directly under — folds with WorstTopoState.
// With one HELD broker before one STUCK broker the card headlined "run `cluster add`" beneath a banner
// saying that broker's nats.conf cannot be rendered. The single-degraded-node fixture in
// TestCardTopReasonNamesTopology could not see it.
//
// Mutation: return inside the node loop instead of folding first — reddens.
func TestCardTopReasonFoldsBySeverityNotNodeOrder(t *testing.T) {
	rep := &adminsock.ClusterStatusReport{
		TopoDesired: 7,
		Nodes: []adminsock.ClusterNodeStatus{
			tcVoter("a", 7, natsconf.ActionNoop, ""),
			// HELD comes FIRST in table order; STUCK must still win the headline.
			tcVoter("b", 4, natsconf.ActionAwaitingClusteredCutover, tcReasonCutover),
			tcVoter("c", 4, natsconf.ActionRejected, tcReasonApplyFailed),
		},
	}
	got := cardTopReason(rep)
	if !strings.Contains(got, "STUCK") || !strings.Contains(got, "c") {
		t.Fatalf("headline = %q, want the STUCK broker c — the banner under which this card renders "+
			"folds with WorstTopoState, so a headline chosen by table order can contradict it", got)
	}
	// And the reverse order must give the same answer: severity, not position.
	rep.Nodes[1], rep.Nodes[2] = rep.Nodes[2], rep.Nodes[1]
	if got2 := cardTopReason(rep); got2 != got {
		t.Fatalf("headline changed when the two degraded rows swapped places (%q vs %q) — it is still "+
			"order-dependent", got, got2)
	}
	// UnknownAction outranks Behind but not Stuck/Held.
	rep.Nodes = []adminsock.ClusterNodeStatus{
		tcVoter("a", 7, natsconf.ActionNoop, ""),
		tcVoter("b", 4, natsconf.ActionSwappedReloadPending, "awaiting reload"),
		tcVoter("c", 4, "some_future_action", ""),
	}
	if got := cardTopReason(rep); !strings.Contains(got, "unrecognized topology action") {
		t.Fatalf("an unreadable action must outrank a merely-behind peer; got %q", got)
	}
}

// TestEveryTopoStateHasAConsumerVerdict is the exhaustiveness table the external review asked for:
// for EVERY state the classifier can produce, both ctl consumers must agree with Degrades().
//
// origin: batch-c EXTERNAL review M2 + its "recommendations" section. The two consumers were written
// as `if state == X || state == Y`, which is one keystroke from silently dropping a state — and both
// had in fact dropped one. A table over AllTopoStates() means adding a seventh state fails here
// rather than defaulting to a wrong polarity in production.
func TestEveryTopoStateHasAConsumerVerdict(t *testing.T) {
	// A representative (action, reason, observed) triple per state, so the table drives the REAL
	// classifier rather than fabricating states directly.
	const desired = uint64(7)
	byState := map[natsconf.TopoState]adminsock.ClusterNodeStatus{
		natsconf.TopoConverged:     tcVoter("b", 7, natsconf.ActionNoop, ""),
		natsconf.TopoBehind:        tcVoter("b", 4, natsconf.ActionSwappedReloadPending, "awaiting reload"),
		natsconf.TopoHeld:          tcVoter("b", 4, natsconf.ActionAwaitingClusteredCutover, tcReasonCutover),
		natsconf.TopoStuck:         tcVoter("b", 4, natsconf.ActionRejected, tcReasonApplyFailed),
		natsconf.TopoUnknownAction: tcVoter("b", 4, "some_future_action", ""),
	}
	for _, st := range natsconf.AllTopoStates() {
		if st == natsconf.TopoUnreported {
			continue // by construction it has no reporting node to build a row from
		}
		node, ok := byState[st]
		if !ok {
			t.Fatalf("no fixture row for %s — a new TopoState was added without giving this table a "+
				"representative, so its consumer behaviour is untested", st)
		}
		// The fixture must actually classify to the state it is filed under, or the rest is meaningless.
		if got := natsconf.ClassifyTopo(node.TopoAction, node.TopoReconcileReason, node.TopoObserved, desired, node.TopoReported); got != st {
			t.Fatalf("fixture for %s classifies as %s", st, got)
		}
		rep := &adminsock.ClusterStatusReport{
			LeaderID: "a", Health: "DEGRADED", TopoDesired: desired,
			Nodes: []adminsock.ClusterNodeStatus{tcVoter("a", 7, natsconf.ActionNoop, ""), node},
		}

		// doctor: a degrading state must never be reported PASS.
		var topo clusteroffline.DoctorCheck
		for _, c := range clusterDoctorOnline(rep) {
			if c.Name == "topology" {
				topo = c
			}
		}
		if topo.Name == "" {
			t.Fatalf("%s: `cluster doctor` emitted no topology check", st)
		}
		if st.Degrades() && topo.Status == clusteroffline.DoctorPass {
			t.Errorf("%s degrades but `cluster doctor` reported PASS (%q) — the check contradicts the "+
				"health banner it sits beside", st, topo.Detail)
		}
		if !st.Degrades() && topo.Status != clusteroffline.DoctorPass {
			t.Errorf("%s does not degrade but `cluster doctor` reported %v (%q)", st, topo.Status, topo.Detail)
		}

		// --wait: a state that cannot self-heal must bail out instead of polling to the deadline.
		wedged, _ := topoWedged(rep)
		selfHealing := st == natsconf.TopoBehind || st == natsconf.TopoConverged
		if selfHealing && len(wedged) != 0 {
			t.Errorf("%s can self-heal, but `reconcile nats --wait` bails out on it (%v) — waiting is "+
				"the correct behaviour there", st, wedged)
		}
		if !selfHealing && st.Degrades() && len(wedged) == 0 {
			t.Errorf("%s cannot self-heal, but `reconcile nats --wait` keeps polling it to the deadline "+
				"and then exits EX_TEMPFAIL, telling automation to retry something retrying cannot fix", st)
		}
	}
}

// TestInconsistentReasonDrivesCauseAndRemedy pins the m1 fix.
//
// origin: batch-c EXTERNAL review m1. batch C gave the Inconsistent bool a SECOND meaning (a DRAINING
// node whose drain marker is gone and which no operation is driving), and every consumer kept
// printing the roster/raft cause. For the drain case that misstated the cause AND handed back a
// remedy that recursed the operator into the command they were already running.
//
// Mutation: make the consumers ignore InconsistentReason again — reddens.
func TestInconsistentReasonDrivesCauseAndRemedy(t *testing.T) {
	node := func(reason string) adminsock.ClusterNodeStatus {
		n := tcVoter("b", 7, natsconf.ActionNoop, "")
		n.Inconsistent, n.InconsistentReason = true, reason
		return n
	}
	rep := func(reason string) *adminsock.ClusterStatusReport {
		return &adminsock.ClusterStatusReport{
			LeaderID: "a", Health: "DEGRADED", TopoDesired: 7,
			Nodes: []adminsock.ClusterNodeStatus{tcVoter("a", 7, natsconf.ActionNoop, ""), node(reason)},
		}
	}

	// The card headline names the drain, and its remedy is the drain remedy.
	drain := cardTopReason(rep(adminsock.InconsistentDrainingWithoutMarker))
	if !strings.Contains(drain, "DRAINING") || !strings.Contains(drain, "drain --abort") {
		t.Errorf("drain-class headline = %q, want it to name the drain and `cluster drain --abort`", drain)
	}
	if strings.Contains(drain, "roster/raft") {
		t.Errorf("drain-class headline still blames roster/raft: %q", drain)
	}
	// The historical class is unchanged, including for a pre-batch-C broker that sends no reason.
	for _, r := range []string{adminsock.InconsistentRosterRaft, ""} {
		if got := cardTopReason(rep(r)); !strings.Contains(got, "roster/raft") {
			t.Errorf("reason=%q headline = %q, want the roster/raft wording", r, got)
		}
	}

	// doctor: the detail must carry the right remedy, and must NOT tell the operator to run the
	// command they are already running.
	find := func(reason string) string {
		for _, c := range clusterDoctorOnline(rep(reason)) {
			if c.Name == "roster_consistency" {
				return c.Detail
			}
		}
		return ""
	}
	d := find(adminsock.InconsistentDrainingWithoutMarker)
	if !strings.Contains(d, "drain --abort") {
		t.Errorf("doctor detail for the drain class = %q, want `cluster drain --abort`", d)
	}
	if strings.Contains(d, "run `cluster doctor`") {
		t.Errorf("doctor detail recurses the operator back into cluster doctor: %q", d)
	}
	if rr := find(adminsock.InconsistentRosterRaft); !strings.Contains(rr, "raft configuration") {
		t.Errorf("doctor detail for the roster/raft class = %q, want it to name the raft configuration", rr)
	}
}
