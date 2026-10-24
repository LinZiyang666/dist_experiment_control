package cli

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

	stuck, ok := find(ClusterDoctorOnline(base(tcVoter("b", 4, natsconf.ActionRejected, tcReasonApplyFailed))), "topology")
	if !ok {
		t.Fatal("`cluster doctor` has no `topology` check — it PASSes every item on a cluster whose reconciler is wedged")
	}
	if stuck.Status != clusteroffline.DoctorFatal {
		t.Errorf("a STUCK reconcile must be FATAL, got %v (%s)", stuck.Status, stuck.Detail)
	}
	if !strings.Contains(stuck.Detail, "b") {
		t.Errorf("the topology check must name the wedged broker, got %q", stuck.Detail)
	}

	held, _ := find(ClusterDoctorOnline(base(tcVoter("b", 4, natsconf.ActionAwaitingClusteredCutover, tcReasonCutover))), "topology")
	if held.Status != clusteroffline.DoctorAdvisory {
		t.Errorf("a WITHHELD cutover is not a fault: want ADVISORY, got %v (%s)", held.Status, held.Detail)
	}
	if !strings.Contains(held.Detail, "cluster add") || !strings.Contains(held.Detail, "do NOT run") {
		t.Errorf("a WITHHELD cutover must route to `cluster add` AND warn off `--manual`, got %q", held.Detail)
	}

	okCheck, _ := find(ClusterDoctorOnline(base(tcVoter("b", 7, natsconf.ActionNoop, ""))), "topology")
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
			got, found := find(ClusterDoctorOnline(base(tc.node)), "topology")
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
		for _, c := range ClusterDoctorOnline(rep(reason)) {
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
