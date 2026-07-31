package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/natsconf"
)

// cluster_health_monotonicity_test.go — the exit code must not flash green during a recovery.
//
// origin: C7 staging drill, resurrected by line-2 A3. The assertion below was carried for months as
// test/c7/drill_test.go: a 27-line file whose only test body was t.Skip("tracked follow-up"), behind
// the build tag `c7_integration`, which nothing in the repo -- not the Makefile, not CI, not
// all_phases_test.go -- ever enabled. It was doubly invisible: the tag meant it never compiled, and
// the Skip meant that even if it had, it would have asserted nothing. It described itself as
// "tracked NOT silently closed", but no `go test` invocation in the tree ever printed so much as a
// SKIP line for it. That is what an untracked item looks like when it is wearing tracking's clothes.
//
// WHAT IT CLAIMED TO NEED, AND WHY THIS IS THE HONEST FORM
// -------------------------------------------------------
// The old file specified a multi-node drill: startD7Cluster(3) -> kill -> ForceSingle -> regrow
// N1->N2->N3, sampling StatusReport().ExitCode at each waypoint. That shape scripts ONE path through
// the state space and asserts the invariant at the handful of points that path happens to visit.
//
// computeHealth is a pure function of (forceSingle, leaderID, voters, topoDesired, nodes), and
// healthExitCode is a pure function of its verdict. So the invariant it wanted -- "never exit 0 until
// N>=3 AND streams are at target" -- is decidable directly, rather than only at the points one scripted
// regrow walks past. TestClusterExitZeroImpliesRealHA below is that stronger statement.
//
// THE SCOPE OF THAT SWEEP, EXACTLY
// -------------------------------
// origin: line-2 external review A3. This comment used to say the sweep covered "EVERY reachable
// combination", which was false and not by a small margin: the sweep varied four things and computeHealth
// reads nine. `nodes` is an unbounded slice of a struct with a dozen live fields -- "every combination"
// is not a thing any test can claim about that input.
//
// What it actually is: a base grid crossed with a table of DEGRADATION PATHS.
//
//	base grid    forceSingle{2} x leaderID{"", "a"} x voters{0..7} x topoDesired{0, 5}  = 64
//	x spoilers   one per distinct `degraded = true` assignment in computeHealth          = 11
//
// A spoiler is applied to ONE node in an otherwise clean roster, which is the interesting case: the
// premature green this file exists to forbid is one where everything looks fine except the single thing
// nobody checked. The claim is therefore precise and checkable -- "no (base state, single degradation)
// pair yields exit 0 unless the degradation is absent and HA is genuinely present" -- and the spoiler
// table is reconciled against the source in TestHealthSpoilersEachActuallyDegrade, so a spoiler that
// silently stops biting fails rather than quietly shrinking the sweep.
//
// WHAT THIS DOES NOT COVER, STATED PLAINLY
// ----------------------------------------
// These tests pin the health->exit decision. They do not pin StatusReport's POPULATION of voters /
// forceSingle / stream counts at each stage of a real recovery -- that wiring is exercised by the
// gated d7 drills (TestD7Matrix "ForceSingleRecoverRestart", "FollowerStatusViewSource"). The split is
// deliberate: the decision is where a premature green would be MINTED, and it is the half that was
// never asserted anywhere.
//
// Nor does it cover COMBINATIONS of spoilers, or spoilers on more than one node at a time. Both are
// strictly harder to stay green under than the single-spoiler case being asserted, so the omission
// cannot hide a premature green -- but it is an omission, and saying so is the whole point of this
// section.

// healthInputs is one sample of computeHealth's arguments.
type healthInputs struct {
	forceSingle bool
	leaderID    string
	voters      int
	topoDesired uint64
	nodes       []adminsock.ClusterNodeStatus
}

func (h healthInputs) exitCode() int {
	health, _, _ := computeHealth(h.forceSingle, h.leaderID, h.voters, h.topoDesired, h.nodes)
	return healthExitCode(health)
}

// voterRow is a clean, reachable, fully-converged VOTER row: the shape that must NOT by itself be
// enough to turn the cluster green when the voter COUNT is short.
func voterRow(id string) adminsock.ClusterNodeStatus {
	return adminsock.ClusterNodeStatus{
		NodeID: id, Phase: phaseVoter, ReachSource: "self", Reachable: true, TopoReported: true,
	}
}

// TestClusterExitCodeStaysNonZeroThroughRecovery walks the waypoints a quorum-loss recovery actually
// passes through and asserts the exit code is continuously non-zero until the last one.
//
// The N=2 rows are the trap this drill was written for (the old file called it "the manual:credrot
// clears-at-N=2 trap"): once force_single_active has been cleared and no severe alert is present, a
// two-voter cluster looks entirely healthy field-by-field -- leader elected, every voter reachable,
// topology converged, nothing draining. It is still not HA, and reporting exit 0 there would tell an
// operator the recovery finished one node early.
func TestClusterExitCodeStaysNonZeroThroughRecovery(t *testing.T) {
	tests := []struct {
		waypoint string
		in       healthInputs
		wantExit int
	}{
		{
			waypoint: "quorum lost — majority dead, this broker sees no leader",
			in:       healthInputs{leaderID: "", voters: 3, nodes: []adminsock.ClusterNodeStatus{voterRow("a")}},
			wantExit: 2,
		},
		{
			waypoint: "force-single active — survivor rewrote raft config to {self}",
			in: healthInputs{
				forceSingle: true, leaderID: "a", voters: 1,
				nodes: []adminsock.ClusterNodeStatus{voterRow("a")},
			},
			wantExit: 3,
		},
		{
			waypoint: "N=1 after the force-single marker cleared — single voter, no fault tolerance",
			in:       healthInputs{leaderID: "a", voters: 1, nodes: []adminsock.ClusterNodeStatus{voterRow("a")}},
			wantExit: 1,
		},
		{
			waypoint: "N=2 STABLE — marker cleared, no severe alert, every field clean (the credrot trap)",
			in: healthInputs{
				leaderID: "a", voters: 2,
				nodes: []adminsock.ClusterNodeStatus{voterRow("a"), voterRow("b")},
			},
			wantExit: 1,
		},
		{
			waypoint: "N=3 but the third voter is still catching up",
			in: healthInputs{
				leaderID: "a", voters: 3,
				nodes: []adminsock.ClusterNodeStatus{
					voterRow("a"), voterRow("b"),
					{NodeID: "c", Phase: phaseCatchingUp, ReachSource: "self", Reachable: true, TopoReported: true},
				},
			},
			wantExit: 1,
		},
		{
			waypoint: "N=3, all voters caught up, but a stream is BELOW its target replica count",
			in: healthInputs{
				leaderID: "a", voters: 3,
				nodes: []adminsock.ClusterNodeStatus{
					voterRow("a"), voterRow("b"),
					{
						NodeID: "c", Phase: phaseVoter, ReachSource: "self", Reachable: true, TopoReported: true,
						StreamTarget: 3, StreamActual: 2,
					},
				},
			},
			wantExit: 1,
		},
		{
			waypoint: "N=3, all voters caught up, all streams AT target — the FIRST legitimate green",
			in: healthInputs{
				leaderID: "a", voters: 3,
				nodes: []adminsock.ClusterNodeStatus{
					voterRow("a"), voterRow("b"),
					{
						NodeID: "c", Phase: phaseVoter, ReachSource: "self", Reachable: true, TopoReported: true,
						StreamTarget: 3, StreamActual: 3,
					},
				},
			},
			wantExit: 0,
		},
	}

	sawGreen := false
	for _, tc := range tests {
		got := tc.in.exitCode()
		if got != tc.wantExit {
			t.Errorf("waypoint %q: exit = %d, want %d", tc.waypoint, got, tc.wantExit)
		}
		if got == 0 {
			sawGreen = true
		} else if sawGreen {
			t.Errorf("waypoint %q reported exit %d AFTER an earlier waypoint already reported 0 — "+
				"the table is out of chronological order, which would make the monotonicity claim vacuous",
				tc.waypoint, got)
		}
	}
	if !sawGreen {
		t.Error("no waypoint in the table reached exit 0 — the table cannot show WHEN green becomes " +
			"legitimate, so it proves nothing about prematurity")
	}
}

// healthSpoiler is one way a single node can make a cluster not-HA. There is one entry per distinct
// `degraded = true` assignment reachable in computeHealth, so the table is a transcription of the
// source rather than a hand-picked sample of it.
//
// degradesAt reports whether this spoiler should withhold green at a given topoDesired. Only the topo
// spoilers depend on it: ClassifyTopo returns TopoUnreported when desired == 0, so "this broker's live
// topology trails" is not even a question until a generation has been desired.
type healthSpoiler struct {
	name       string
	apply      func(*adminsock.ClusterNodeStatus)
	degradesAt func(topoDesired uint64) bool
}

func always(uint64) bool            { return true }
func onlyWhenDesired(d uint64) bool { return d > 0 }

var healthSpoilers = []healthSpoiler{
	{"none", func(*adminsock.ClusterNodeStatus) {}, func(uint64) bool { return false }},
	{"stream below target replicas", func(n *adminsock.ClusterNodeStatus) {
		n.StreamTarget, n.StreamActual = 3, 2
	}, always},
	{"voter a real health poll found unreachable", func(n *adminsock.ClusterNodeStatus) {
		n.ReachSource, n.Reachable = "nats-health", false
	}, always},
	{"voter trailing the leader past the lag threshold", func(n *adminsock.ClusterNodeStatus) {
		n.AppliedLag = observeLagThreshold + 1
	}, always},
	{"phase disagrees with the raft config", func(n *adminsock.ClusterNodeStatus) {
		n.Inconsistent = true
	}, always},
	{"phase CATCHING_UP", func(n *adminsock.ClusterNodeStatus) { n.Phase = phaseCatchingUp }, always},
	{"phase ADD_FAILED", func(n *adminsock.ClusterNodeStatus) { n.Phase = phaseAddFailed }, always},
	{"phase DRAINING", func(n *adminsock.ClusterNodeStatus) { n.Phase = phaseDraining }, always},
	{"phase RETIRING", func(n *adminsock.ClusterNodeStatus) { n.Phase = phaseRetiring }, always},
	{"live topology behind the desired generation", func(n *adminsock.ClusterNodeStatus) {
		n.TopoReported, n.TopoObserved, n.TopoReconcileReason = true, 0, "still converging"
	}, onlyWhenDesired},
	{"reconciler wedged: render rejected", func(n *adminsock.ClusterNodeStatus) {
		n.TopoReported, n.TopoAction = true, natsconf.ActionRejected
	}, onlyWhenDesired},
	{"self row almost out of disk", func(n *adminsock.ClusterNodeStatus) { n.DiskFreePct = 5 }, always},
	{"self row almost out of ports", func(n *adminsock.ClusterNodeStatus) {
		n.PortsUsed, n.PortsTotal = 90, 100
	}, always},
}

// healthSweepCase builds one roster and returns its exit code. The spoiler lands on node 0 only.
func healthSweepCase(forceSingle bool, leaderID string, voters int, topoDesired uint64, sp healthSpoiler) int {
	nodes := make([]adminsock.ClusterNodeStatus, 0, voters)
	for i := 0; i < voters; i++ {
		n := voterRow(string(rune('a' + i)))
		n.TopoReported, n.TopoObserved = true, topoDesired
		if i == 0 {
			sp.apply(&n)
		}
		nodes = append(nodes, n)
	}
	in := healthInputs{forceSingle: forceSingle, leaderID: leaderID, voters: voters,
		topoDesired: topoDesired, nodes: nodes}
	return in.exitCode()
}

// TestClusterExitZeroImpliesRealHA is the invariant in its strongest form: sweep the base grid crossed
// with the degradation table and assert that a green exit is only ever reachable when the cluster
// genuinely has HA. This is what makes the waypoint table above non-vacuous -- the table shows the
// intended sequence, this shows no other input in scope escapes. The scope is stated exactly in the
// file header ("THE SCOPE OF THAT SWEEP, EXACTLY"); it is a large grid, not "everything".
func TestClusterExitZeroImpliesRealHA(t *testing.T) {
	greens := 0
	for _, forceSingle := range []bool{false, true} {
		for _, leaderID := range []string{"", "a"} {
			for voters := 0; voters <= 7; voters++ {
				for _, topoDesired := range []uint64{0, 5} {
					for _, sp := range healthSpoilers {
						if healthSweepCase(forceSingle, leaderID, voters, topoDesired, sp) != 0 {
							continue
						}
						greens++
						switch {
						case forceSingle:
							t.Errorf("exit 0 with force-single ACTIVE (voters=%d, spoiler=%q)", voters, sp.name)
						case leaderID == "":
							t.Errorf("exit 0 with NO leader visible (voters=%d, spoiler=%q)", voters, sp.name)
						case voters < 3:
							t.Errorf("exit 0 at %d voter(s) — a cluster that cannot lose a node is not HA "+
								"(spoiler=%q)", voters, sp.name)
						case sp.degradesAt(topoDesired):
							t.Errorf("exit 0 with a degraded node: %s (voters=%d, topoDesired=%d)",
								sp.name, voters, topoDesired)
						}
					}
				}
			}
		}
	}
	if greens == 0 {
		t.Fatal("the sweep never produced a single exit 0 — every branch is refusing, so the implication " +
			"`exit 0 => real HA` is vacuously true and this test proves nothing")
	}
}

// TestHealthSpoilersEachActuallyDegrade is the reverse assertion on the table above, and it is the part
// that keeps the sweep from shrinking silently.
//
// A spoiler that has stopped biting -- a renamed phase constant, a threshold moved past the value the
// table pokes, a guard deleted -- does not make TestClusterExitZeroImpliesRealHA fail. It makes it pass
// MORE EASILY, by contributing cases that are green and legitimately so. The sweep would keep reporting
// success over a table that had quietly become a list of no-ops. So each spoiler must be shown to flip
// an otherwise-green cluster to non-green on its own.
//
// Verified by deleting each guard in computeHealth and confirming the corresponding spoiler is reported
// NO-OP. That exercise also mapped where the guards are REDUNDANT: the two topo spoilers survive the
// removal of `if st.Degrades() { degraded = true }` and, separately, the removal of the
// `topoWorst.Banner() != ""` early return -- either path alone still withholds green, and only removing
// BOTH makes them no-ops. Recorded because it is the kind of thing a future single-guard mutation test
// will misread as "my probe does not work".
func TestHealthSpoilersEachActuallyDegrade(t *testing.T) {
	for _, topoDesired := range []uint64{0, 5} {
		if base := healthSweepCase(false, "a", 3, topoDesired, healthSpoilers[0]); base != 0 {
			t.Fatalf("the clean 3-voter baseline at topoDesired=%d exits %d, not 0 — every spoiler below "+
				"would then 'pass' for the wrong reason", topoDesired, base)
		}
		for _, sp := range healthSpoilers[1:] {
			got := healthSweepCase(false, "a", 3, topoDesired, sp)
			want := sp.degradesAt(topoDesired)
			if want && got == 0 {
				t.Errorf("spoiler %q at topoDesired=%d is a NO-OP: a clean 3-voter cluster stays green "+
					"with it applied. Either computeHealth stopped checking this, or the spoiler no longer "+
					"constructs the state it names.", sp.name, topoDesired)
			}
			if !want && got != 0 {
				t.Errorf("spoiler %q claims not to degrade at topoDesired=%d but exit is %d — the table's "+
					"degradesAt is wrong, which makes the main sweep accept a green it should reject",
					sp.name, topoDesired, got)
			}
		}
	}
}
