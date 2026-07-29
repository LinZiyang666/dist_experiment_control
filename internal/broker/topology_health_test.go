package broker

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/natsconf"
)

// topology_health_test.go (C3 acceptance) — "任一 broker 未完成 topology apply 时不显 HEALTHY-HA".
// The gate degrades a REACHED, REPORTING voter whose live topology generation trails the desired one,
// gated on presence (TopoReported) NOT a TopoObserved>0 magnitude guard (so observed=0 still degrades),
// and never trips on a non-reporting (older) broker.
func TestComputeHealthTopologyGate(t *testing.T) {
	const desired = uint64(7)
	voter := func(id string, observed uint64, reported bool) adminsock.ClusterNodeStatus {
		return adminsock.ClusterNodeStatus{
			NodeID: id, Phase: phaseVoter, Role: "voter", Reachable: true, ReachSource: "nats-health",
			TopoReported: reported, TopoObserved: observed, TopoApplied: observed,
		}
	}
	leader := adminsock.ClusterNodeStatus{
		NodeID: "a", Phase: phaseVoter, Role: "leader", Reachable: true, ReachSource: "self",
		TopoReported: true, TopoObserved: 7, TopoApplied: 7,
	}

	// All three reachable + reporting voters at the desired generation → HEALTHY_HA.
	converged := []adminsock.ClusterNodeStatus{leader, voter("b", 7, true), voter("c", 7, true)}
	if h, _, _ := computeHealth(false, "a", 3, desired, converged); h != healthHealthyHA {
		t.Fatalf("all voters at the desired topology gen must be HEALTHY_HA, got %s", h)
	}

	// One voter behind (observed 6 < 7) → DEGRADED.
	behind := []adminsock.ClusterNodeStatus{leader, voter("b", 6, true), voter("c", 7, true)}
	if h, _, _ := computeHealth(false, "a", 3, desired, behind); h != healthDegraded {
		t.Fatalf("a reachable voter behind the desired topology gen must be DEGRADED, got %s", h)
	}

	// A just-promoted voter at observed=0 with desired>0 → DEGRADED (presence-gated, NOT a >0 guard).
	zero := []adminsock.ClusterNodeStatus{leader, voter("b", 0, true), voter("c", 7, true)}
	if h, _, _ := computeHealth(false, "a", 3, desired, zero); h != healthDegraded {
		t.Fatalf("observed=0 with desired>0 must be DEGRADED (presence-gated), got %s", h)
	}

	// A NON-reporting voter (older broker, TopoReported=false) must NOT trip the topo gate.
	older := []adminsock.ClusterNodeStatus{leader, voter("b", 7, true), voter("c", 0, false)}
	if h, _, _ := computeHealth(false, "a", 3, desired, older); h != healthHealthyHA {
		t.Fatalf("a non-reporting voter must NOT trip the topo gate (only reporting voters), got %s", h)
	}

	// topoDesired==0 (no topology managed yet) → the gate is inert.
	if h, _, _ := computeHealth(false, "a", 3, 0, []adminsock.ClusterNodeStatus{leader, voter("b", 0, true), voter("c", 0, true)}); h != healthHealthyHA {
		t.Fatalf("topoDesired==0 must leave the gate inert, got %s", h)
	}
}

// topoHealthVoter builds a reached, reporting VOTER carrying a topology self-report.
func topoHealthVoter(id string, observed uint64, action, reason string) adminsock.ClusterNodeStatus {
	return adminsock.ClusterNodeStatus{
		NodeID: id, Phase: phaseVoter, Role: "voter", Reachable: true, ReachSource: "nats-health",
		TopoReported: true, TopoObserved: observed, TopoApplied: observed,
		TopoAction: action, TopoReconcileReason: reason,
	}
}

// TestComputeHealthTopologyVerdicts drives computeHealth through every reconcile action and asserts
// the verdict AND the operator text. Three rows are regressions of shipped defects:
//
//   - "wedged at the converged generation": the STUCK test used to be NESTED inside
//     `observed < topoDesired`, and ActionUnknownDirective returns applied/observed UNCHANGED. A
//     broker wedged AFTER converging therefore reported HEALTHY_HA with a permanently fail-closed
//     reconciler on it. Mutation: re-nest the ClassifyTopo call under `n.TopoObserved < topoDesired`.
//
//   - "apply failure": ActionRejected's third producer has Reason "apply: …", which matched NONE of
//     the three old substrings, so a full disk / unwritable .bak rendered as "still catching up".
//     Mutation: classify only the render and -t producers as stuck.
//
//   - "withheld cutover": its Reason contains "rendeRED", so the old "render" substring classified a
//     DELIBERATE hold as STUCK — and STUCK's next-step recommends `reconcile nats --manual`, the one
//     command that applies a clustered conf under a running standalone nats-server (G4 #10/#4).
//     Mutation: map ActionAwaitingClusteredCutover to TopoStuck.
//
// voters is 3 throughout: at voters <= 2 the FaultTolerance==0 branch returns BEFORE any topology
// banner, so a 2-voter fixture would assert nothing about topology (see plan N13).
func TestComputeHealthTopologyVerdicts(t *testing.T) {
	const desired = uint64(7)
	leader := adminsock.ClusterNodeStatus{
		NodeID: "a", Phase: phaseVoter, Role: "leader", Reachable: true, ReachSource: "self",
		TopoReported: true, TopoObserved: 7, TopoApplied: 7, TopoAction: natsconf.ActionNoop,
	}
	rows := []struct {
		name       string
		node       adminsock.ClusterNodeStatus
		wantHealth string
		wantBanner string // substring
		wantNext   string // substring
		// negateManual: the next-step must WARN AGAINST `reconcile nats --manual` rather than
		// recommend it. A naive "must not contain --manual" would be wrong (and would have passed
		// while the text said the opposite): the correct text NAMES the command inside a negation.
		negateManual bool
		wantHealthy  bool
	}{
		{
			name:        "converged",
			node:        topoHealthVoter("b", 7, natsconf.ActionNoop, ""),
			wantHealthy: true,
		},
		{
			name:       "behind",
			node:       topoHealthVoter("b", 5, natsconf.ActionSwappedReloadPending, "conf is current but the live server has not loaded it — a restart will pick it up"),
			wantHealth: healthDegraded,
			wantBanner: "has not caught the desired generation",
			wantNext:   "reconcile nats --all --wait",
		},
		{
			name:       "wedged at the converged generation (regression)",
			node:       topoHealthVoter("b", 7, natsconf.ActionUnknownDirective, "nats.conf has an unrecognized directive — fix it, or `cluster reconcile nats --manual`: boom"),
			wantHealth: healthDegraded,
			wantBanner: "STUCK",
			wantNext:   "fix that broker's nats.conf",
		},
		{
			name:       "apply failure (regression)",
			node:       topoHealthVoter("b", 5, natsconf.ActionRejected, "apply: no space left on device"),
			wantHealth: healthDegraded,
			wantBanner: "STUCK",
			wantNext:   "fix that broker's nats.conf",
		},
		{
			name:         "withheld cutover is HELD, never STUCK (regression)",
			node:         topoHealthVoter("b", 5, natsconf.ActionAwaitingClusteredCutover, "standalone→clustered cutover rendered + validated but WITHHELD — a reconciler SIGHUP cannot safely cross it; run `tether cluster add <this-broker>` to perform the coordinated restart"),
			wantHealth:   healthDegraded,
			wantBanner:   "WITHHELD",
			wantNext:     "cluster add",
			negateManual: true,
		},
		{
			name:       "an action this binary predates is not silently healthy",
			node:       topoHealthVoter("b", 7, "some_future_action", ""),
			wantHealth: healthDegraded,
			wantBanner: "does not recognize",
			wantNext:   "upgrade this ctl/broker",
		},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			nodes := []adminsock.ClusterNodeStatus{leader, r.node, topoHealthVoter("c", 7, natsconf.ActionNoop, "")}
			h, banner, next := computeHealth(false, "a", 3, desired, nodes)
			if r.wantHealthy {
				if h != healthHealthyHA {
					t.Fatalf("want HEALTHY_HA, got %s (banner=%q)", h, banner)
				}
				return
			}
			if h != r.wantHealth {
				t.Fatalf("health = %s, want %s (banner=%q)", h, r.wantHealth, banner)
			}
			if !strings.Contains(banner, r.wantBanner) {
				t.Errorf("banner %q does not contain %q", banner, r.wantBanner)
			}
			if !strings.Contains(next, r.wantNext) {
				t.Errorf("next-step %q does not contain %q", next, r.wantNext)
			}
			if r.negateManual {
				if next == natsconf.TopoStuck.NextStep() {
					t.Errorf("a withheld cutover was given the STUCK next-step verbatim (%q) — that "+
						"recommends `reconcile nats --manual`, which applies a clustered conf under a "+
						"running standalone nats-server (G4 #10/#4)", next)
				}
				if !strings.Contains(next, "do NOT run") || !strings.Contains(next, "--manual") {
					t.Errorf("a withheld cutover's next-step must NAME `reconcile nats --manual` inside "+
						"an explicit warning, so a reader cannot arrive at it from the generic STUCK "+
						"advice; got %q", next)
				}
			}
		})
	}
}

// TestComputeHealthTopologyStuckOutranksBehind: the node needing hands must own the banner, or an
// operator reading a mixed cluster is told to wait while one broker will never converge.
func TestComputeHealthTopologyStuckOutranksBehind(t *testing.T) {
	const desired = uint64(9)
	leader := adminsock.ClusterNodeStatus{
		NodeID: "a", Phase: phaseVoter, Role: "leader", Reachable: true, ReachSource: "self",
		TopoReported: true, TopoObserved: 9, TopoApplied: 9, TopoAction: natsconf.ActionNoop,
	}
	nodes := []adminsock.ClusterNodeStatus{
		leader,
		topoHealthVoter("b", 4, natsconf.ActionSwappedReloadPending, "awaiting reload"),
		topoHealthVoter("c", 9, natsconf.ActionRejected, "apply: disk full"),
	}
	_, banner, next := computeHealth(false, "a", 3, desired, nodes)
	if !strings.Contains(banner, "STUCK") {
		t.Fatalf("a STUCK broker must own the banner over a merely-behind one; got %q / %q", banner, next)
	}
}

// TestComputeHealthIgnoresUnreachedVoterTopology: an unreachable voter's last-known topology report
// is stale, and computeHealth deliberately excludes it (topoConvergedForOp, which gates IRREVERSIBLE
// membership steps, deliberately does the opposite — see its doc comment). Pinning this stops a
// future "unify the four predicates" pass from quietly flipping the status polarity.
func TestComputeHealthIgnoresUnreachedVoterTopology(t *testing.T) {
	const desired = uint64(4)
	leader := adminsock.ClusterNodeStatus{
		NodeID: "a", Phase: phaseVoter, Role: "leader", Reachable: true, ReachSource: "self",
		TopoReported: true, TopoObserved: 4, TopoApplied: 4, TopoAction: natsconf.ActionNoop,
	}
	stale := topoHealthVoter("b", 1, natsconf.ActionRejected, "apply: disk full")
	stale.Reachable = false
	nodes := []adminsock.ClusterNodeStatus{leader, stale, topoHealthVoter("c", 4, natsconf.ActionNoop, "")}
	_, banner, _ := computeHealth(false, "a", 3, desired, nodes)
	if strings.Contains(banner, "STUCK") {
		t.Fatalf("an UNREACHED voter's stale topology report must not drive the topology banner "+
			"(the unreachable-voter degrade owns that row instead); got %q", banner)
	}
}
