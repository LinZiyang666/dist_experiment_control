package clusterupgrade

import "testing"

// plan_test.go — G5 #13/#14: adversarial table tests for the pure roll planner. Ordering
// (followers-first/leader-last), the #19 half-upgrade skip guard, N=2 honesty, and the
// no-transfer-target refusal are all pinned here without any raft/systemd harness.

func ids(steps []Step, kind StepKind) []string {
	var out []string
	for _, s := range steps {
		if s.Kind == kind {
			out = append(out, s.NodeID)
		}
	}
	return out
}

func upgradeStep(steps []Step, id string) *Step {
	for i := range steps {
		if steps[i].Kind == StepUpgrade && steps[i].NodeID == id {
			return &steps[i]
		}
	}
	return nil
}

func TestComputeFollowersFirstLeaderLast(t *testing.T) {
	// b is leader; a, c are caught-up voter followers. None at target → all upgrade, leader LAST.
	nodes := []Node{
		{ID: "a", Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
		{ID: "b", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
		{ID: "c", Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
	}
	p := Compute(nodes, "v2")
	if len(p.Refused) != 0 {
		t.Fatalf("unexpected refusal: %v", p.Refused)
	}
	order := ids(p.Steps, StepUpgrade)
	if len(order) != 3 || order[len(order)-1] != "b" {
		t.Fatalf("leader b must be upgraded LAST; order=%v", order)
	}
	// the leader's step must carry a transfer target that is another voter.
	ls := upgradeStep(p.Steps, "b")
	if ls == nil || ls.TransferTo == "" || ls.TransferTo == "b" {
		t.Fatalf("leader step must transfer to another voter, got %+v", ls)
	}
}

func TestComputeAllAtTargetSkips(t *testing.T) {
	nodes := []Node{
		{ID: "a", Voter: true, BrokerVer: "v2", AgentVer: "v2"},
		{ID: "b", IsLeader: true, Voter: true, BrokerVer: "v2", AgentVer: "v2"},
	}
	p := Compute(nodes, "v2")
	if p.Upgrades() != 0 {
		t.Fatalf("all-at-target must produce zero upgrades, got %d (%v)", p.Upgrades(), p.Steps)
	}
	if len(ids(p.Steps, StepSkip)) != 2 {
		t.Fatalf("both hosts must be SKIP, got %v", p.Steps)
	}
}

func TestComputeHalfUpgradedNotSkipped(t *testing.T) {
	// #19: broker at target but its co-located agent still stale → NOT done, must upgrade.
	nodes := []Node{
		{ID: "a", Voter: true, CaughtUp: true, BrokerVer: "v2", AgentVer: "v1"}, // half-upgraded
		{ID: "b", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v2", AgentVer: "v2"},
	}
	p := Compute(nodes, "v2")
	if upgradeStep(p.Steps, "a") == nil {
		t.Fatal("a broker at target with a stale co-located agent must NOT be skipped (#19 half-upgrade)")
	}
	if len(ids(p.Steps, StepSkip)) != 1 || ids(p.Steps, StepSkip)[0] != "b" {
		t.Fatalf("only the fully-upgraded b should skip, got skips=%v", ids(p.Steps, StepSkip))
	}
}

func TestComputeLeaderOnlyNotAtTargetTransfers(t *testing.T) {
	// followers already upgraded; only the leader remains → transfer to an at-target caught-up voter.
	nodes := []Node{
		{ID: "a", Voter: true, CaughtUp: true, BrokerVer: "v2", AgentVer: "v2"},
		{ID: "b", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
	}
	p := Compute(nodes, "v2")
	if len(p.Refused) != 0 {
		t.Fatalf("a caught-up survivor exists; must not refuse: %v", p.Refused)
	}
	ls := upgradeStep(p.Steps, "b")
	if ls == nil || ls.TransferTo != "a" {
		t.Fatalf("leader b must transfer to the caught-up survivor a, got %+v", ls)
	}
}

func TestComputeRefusesWhenNoCaughtUpTransferTarget(t *testing.T) {
	// leader must upgrade, the only other voter is NOT caught-up → cannot safely transfer → REFUSE.
	nodes := []Node{
		{ID: "a", Voter: true, CaughtUp: false, BrokerVer: "v2", AgentVer: "v2"},
		{ID: "b", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
	}
	p := Compute(nodes, "v2")
	if len(p.Refused) == 0 {
		t.Fatal("must refuse: the leader needs upgrade but no other caught-up voter can take leadership")
	}
}

func TestComputeN1NoTransfer(t *testing.T) {
	nodes := []Node{{ID: "solo", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"}}
	p := Compute(nodes, "v2")
	if len(p.Refused) != 0 {
		t.Fatalf("N=1 leader must not refuse (no quorum to lose): %v", p.Refused)
	}
	ls := upgradeStep(p.Steps, "solo")
	if ls == nil || ls.TransferTo != "" {
		t.Fatalf("N=1 leader must upgrade with NO transfer, got %+v", ls)
	}
	if p.N2WriteFence {
		t.Fatal("N=1 is not the N=2 write-fence case")
	}
}

func TestComputeN2WriteFenceFlag(t *testing.T) {
	nodes := []Node{
		{ID: "a", Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
		{ID: "b", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
	}
	p := Compute(nodes, "v2")
	if !p.N2WriteFence {
		t.Fatal("exactly 2 voters must set the N2WriteFence honesty flag")
	}
	// non-voters must not count toward the fence.
	nodes = append(nodes, Node{ID: "c", Voter: false, BrokerVer: "v1", AgentVer: "v1"})
	if !Compute(nodes, "v2").N2WriteFence {
		t.Fatal("a non-voter must not change the 2-voter fence verdict")
	}
}

func TestComputeDeterministicOrder(t *testing.T) {
	// same input in a different slice order → identical plan (sort.SliceStable by ID).
	a := []Node{
		{ID: "c", Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
		{ID: "a", Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
		{ID: "b", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
	}
	b := []Node{a[1], a[2], a[0]}
	if got, want := ids(Compute(a, "v2").Steps, StepUpgrade), ids(Compute(b, "v2").Steps, StepUpgrade); len(got) != len(want) {
		t.Fatalf("nondeterministic length")
	} else {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("nondeterministic order: %v vs %v", got, want)
			}
		}
	}
}
