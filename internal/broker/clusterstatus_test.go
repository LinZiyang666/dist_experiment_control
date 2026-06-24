package broker

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// clusterstatus_test.go — D7 §8.3/§17 cheap units (make test): quorum projection,
// health→exit-code SSOT, drain F==0 confirm gate, single-node status render. The
// multi-node drain/retire/leader-switch + status double-view drills are gated
// d7_integration (test/d7).

func TestD7ProjectQuorum(t *testing.T) {
	cases := []struct {
		voters int
		retire bool
		wantV  int
		wantQ  int
		wantF  int
	}{
		{1, false, 1, 1, 0}, // N=1 never HA
		{2, false, 2, 2, 0}, // plain drain at N=2 -> F==0 (OQ-6 / §8.3 parenthetical)
		{3, false, 3, 2, 1}, // plain drain at N=3 -> still tolerates 1
		{3, true, 2, 2, 0},  // retire N=3 -> N=2 -> F==0
		{5, true, 4, 3, 1},  // retire N=5 -> N=4 -> F==1
	}
	for _, c := range cases {
		got := ProjectQuorum(c.voters, c.retire)
		if got.Voters != c.wantV || got.Quorum != c.wantQ || got.FaultTolerance != c.wantF {
			t.Errorf("ProjectQuorum(%d, retire=%v) = %+v, want voters=%d quorum=%d F=%d",
				c.voters, c.retire, got, c.wantV, c.wantQ, c.wantF)
		}
	}
}

func TestD7HealthExitCodeSSOT(t *testing.T) {
	cases := map[string]int{
		healthHealthyHA: 0, healthDegraded: 1, healthQuorumLost: 2, healthForceSingle: 3,
	}
	for h, want := range cases {
		if got := healthExitCode(h); got != want {
			t.Errorf("healthExitCode(%q) = %d, want %d", h, got, want)
		}
	}
}

func TestD7ComputeHealth(t *testing.T) {
	healthy := []adminsock.ClusterNodeStatus{
		{NodeID: "a", Phase: phaseVoter, Role: "leader"},
		{NodeID: "b", Phase: phaseVoter, Role: "voter"},
		{NodeID: "c", Phase: phaseVoter, Role: "voter"},
	}
	// force-single dominates everything.
	if h, _, next := computeHealth(true, "a", 3, healthy); h != healthForceSingle {
		t.Errorf("force-single: got %s", h)
	} else if !strings.Contains(next, "--self-id") {
		t.Errorf("force-single next step missing --self-id: %q", next)
	}
	// no leader => quorum-lost (exit 2), NOT from absence of reports but a positive empty-leader.
	if h, _, next := computeHealth(false, "", 3, healthy); h != healthQuorumLost {
		t.Errorf("no-leader: got %s", h)
	} else if !strings.Contains(next, "--self-id") || !strings.Contains(next, "--self-addr") {
		t.Errorf("quorum-lost next step missing force-single identity flags: %q", next)
	}
	// N=3 all VOTER, leader present => HEALTHY_HA.
	if h, _, _ := computeHealth(false, "a", 3, healthy); h != healthHealthyHA {
		t.Errorf("healthy: got %s", h)
	}
	// N=2 => F==0 => DEGRADED even with a leader + all VOTER.
	twoVoters := healthy[:2]
	if h, _, next := computeHealth(false, "a", 2, twoVoters); h != healthDegraded {
		t.Errorf("N=2: got %s", h)
	} else if !strings.Contains(next, "<node-id>") || !strings.Contains(next, "--tunnel-addr") {
		t.Errorf("F==0 next step missing full add workflow: %q", next)
	}
	// A CATCHING_UP node => DEGRADED.
	joining := []adminsock.ClusterNodeStatus{
		{NodeID: "a", Phase: phaseVoter, Role: "leader"},
		{NodeID: "b", Phase: phaseVoter, Role: "voter"},
		{NodeID: "c", Phase: phaseCatchingUp, Role: "voter"},
	}
	if h, _, _ := computeHealth(false, "a", 3, joining); h != healthDegraded {
		t.Errorf("catching-up: got %s", h)
	}
}

func TestD7DrainConfirmGateF0(t *testing.T) {
	n, _ := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	// Plain drain at N=1 projects F==0 -> confirm required. (retire=false so the
	// last-voter hard-refuse does not pre-empt the confirm gate — review m4.)
	err := admin.DrainNode("single-1", false, false, time.Now(), nil)
	var qc *ErrQuorumConfirmRequired
	if !errors.As(err, &qc) {
		t.Fatalf("want ErrQuorumConfirmRequired, got %v", err)
	}
	if qc.Proj.FaultTolerance != 0 {
		t.Fatalf("projection F = %d, want 0", qc.Proj.FaultTolerance)
	}
}

func TestD7RetireLastVoterHardRefused(t *testing.T) {
	n, _ := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	// Retiring the only voter is hard-refused even WITH confirmation (review m4).
	err := admin.DrainNode("single-1", true, true, time.Now(), nil)
	if err == nil || !strings.Contains(err.Error(), "cannot retire the last voter") {
		t.Fatalf("retire of the last voter must be hard-refused, got %v", err)
	}
}

func TestD7StatusReportRendersRoleAndHealth(t *testing.T) {
	n, addr := d7SingleNode(t, "single-1")
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, "single-1", addr)
	caughtUp := func(barrier uint64) (bool, error) {
		cur, err := n.AppliedIndex()
		return cur >= barrier, err
	}
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport: %v", err)
	}
	if len(rep.Nodes) != 1 {
		t.Fatalf("want 1 node, got %d", len(rep.Nodes))
	}
	node := rep.Nodes[0]
	if node.NodeID != "single-1" || node.Phase != phaseVoter || node.Role != "leader" {
		t.Fatalf("node render wrong: %+v", node)
	}
	if node.Inconsistent {
		t.Fatal("a VOTER that is a raft leader must not be INCONSISTENT")
	}
	// N=1 is never HA.
	if rep.Health != healthDegraded || rep.ExitCode != 1 {
		t.Fatalf("N=1 health = %s exit=%d, want DEGRADED/1", rep.Health, rep.ExitCode)
	}
}
