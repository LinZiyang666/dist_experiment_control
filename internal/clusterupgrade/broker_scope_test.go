package clusterupgrade

import (
	"reflect"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §5 / T45
func TestBrokerPlanScopePreservesAgentPresence(t *testing.T) {
	nodes := []Node{
		{ID: "paired", Voter: true, BrokerVer: "v2", AgentVer: "v1", Agent: AgentPresent},
		{ID: "unknown", BrokerVer: "v2", Agent: AgentUnknown},
		{ID: "absent", BrokerVer: "v2", Agent: AgentAbsent},
	}
	original := append([]Node(nil), nodes...)
	whole := Compute(nodes, "v2")
	broker := ComputeWithScope(nodes, "v2", ScopeBrokerDaemon)
	if whole.Scope != ScopeWholeHost || whole.Upgrades() != 2 || broker.Scope != ScopeBrokerDaemon || broker.Upgrades() != 0 {
		t.Fatalf("scope verdicts: whole=%+v broker=%+v", whole, broker)
	}
	if !reflect.DeepEqual(nodes, original) {
		t.Fatal("scope rewrote agent presence in the input")
	}
	nodes[0].BrokerVer = "v1"
	broker = ComputeWithScope(nodes, "v2", ScopeBrokerDaemon)
	if broker.Upgrades() != 1 || broker.Steps[2].NodeID != "unknown" {
		t.Fatalf("broker plan: %+v", broker)
	}
	for _, s := range broker.Steps {
		if s.NodeID == "paired" && s.BrokerOnly {
			t.Fatal("product scope was forged as AgentAbsent")
		}
	}
	if p := ComputeWithScope(nodes, "v2", "broker"); len(p.Refused) == 0 || len(p.Steps) != 0 {
		t.Fatalf("unknown scope admitted: %+v", p)
	}
}

// origin: hot-update S6 plan §5 / T37 / T45
func TestBrokerPlanSoleVoterWriteStall(t *testing.T) {
	for _, scope := range []PlanScope{ScopeWholeHost, ScopeBrokerDaemon} {
		p := ComputeWithScope([]Node{{ID: "solo", IsLeader: true, Voter: true, BrokerVer: "v1", Agent: AgentPresent}}, "v2", scope)
		if !p.N1WriteStall || p.N2WriteFence || len(p.Refused) != 0 || len(p.Steps) != 1 || p.Steps[0].TransferTo != "" {
			t.Fatalf("sole-voter plan: %+v", p)
		}
		p = ComputeWithScope([]Node{{ID: "a", Voter: true}, {ID: "b", Voter: true}}, "v2", scope)
		if p.N1WriteStall || !p.N2WriteFence {
			t.Fatalf("two-voter flags: %+v", p)
		}
	}
}

// The planning owner is deliberately IO-free. Measure its production entry
// points too: a future implementation must not hide work in an unjoined helper
// simply because its caller already has a measured roll cycle.
// origin: hot-update S6 plan §9.3 / T54
func TestBrokerPlanLeakCycles(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerPlan(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker scope plans", g)
	testharness.AssertNoFDLeak(t, "broker scope plans", fd, 0)
}

func exerciseBrokerPlan(t *testing.T) {
	t.Helper()
	nodes := []Node{{ID: "solo", IsLeader: true, Voter: true, BrokerVer: "v1", Agent: AgentPresent, AgentVer: "v1"}}
	before := append([]Node(nil), nodes...)
	for _, scope := range []PlanScope{ScopeWholeHost, ScopeBrokerDaemon} {
		p := ComputeWithScope(nodes, "v2", scope)
		if len(p.Steps) != 1 || !p.N1WriteStall || p.Scope != scope || p.Steps[0].BrokerOnly {
			t.Fatalf("scope lost voter or agent facts: %+v", p)
		}
		if p := ComputeWithScope(nodes, "v2", "unknown"); len(p.Refused) == 0 || len(p.Steps) != 0 {
			t.Fatalf("unknown scope admitted: %+v", p)
		}
	}
	if !reflect.DeepEqual(nodes, before) {
		t.Fatal("planner changed the caller's live snapshot")
	}
}
