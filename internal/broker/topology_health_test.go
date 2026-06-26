package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
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
