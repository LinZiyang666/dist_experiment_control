package clusterupgrade

import "testing"

// External review regression: a cluster-health broadcast can collect replies across a
// leadership transition. If more than one broker claims IsLeader in the same folded
// snapshot, the planner must fail closed; silently dropping one leader can skip a
// broker and transfer leadership to an un-upgraded target.
func TestExternalReviewComputeRefusesMultipleWritableLeaders(t *testing.T) {
	nodes := []Node{
		{ID: "a", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
		{ID: "b", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
		{ID: "c", Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1"},
	}
	p := Compute(nodes, "v2")
	if len(p.Refused) == 0 {
		t.Fatalf("ambiguous multiple-leader snapshot must refuse, got steps=%+v", p.Steps)
	}
	if p.Upgrades() != 0 {
		t.Fatalf("refused multiple-leader snapshot must not schedule upgrades, got steps=%+v", p.Steps)
	}
}
