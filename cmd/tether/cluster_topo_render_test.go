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
