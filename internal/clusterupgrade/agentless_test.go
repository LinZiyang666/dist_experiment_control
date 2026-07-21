package clusterupgrade

import "testing"

// agentless_test.go — P3. A broker host with NO co-located agent could never be "at target", so every
// `cluster upgrade` on such a cluster planned an agent re-exec for a nid nobody answers, took
// agent_no_responders back, and HALTed — in the field this left three brokers carrying the new binary on
// disk while only ONE self-reported the new version, plus a roll lock nobody released.
//
// `serve --colocated-agent-nid` defaults to empty and is documented optional, so "dedicated broker host"
// is a supported production shape, not a misconfiguration.

// TestAtTargetDistinguishesTheThreeAgentStates is the core of P3: (a) agent present and at target,
// (b) agent present but NOT at target, (c) no agent at all. (b) and (c) must never be conflated —
// collapsing them into "not at target" is the entire defect.
func TestAtTargetDistinguishesTheThreeAgentStates(t *testing.T) {
	const target = "v2"
	cases := []struct {
		name string
		node Node
		want bool
		why  string
	}{
		{
			name: "(a) agent present and at target",
			node: Node{ID: "h", BrokerVer: "v2", AgentVer: "v2", Agent: AgentPresent},
			want: true,
		},
		{
			name: "(b) agent present but stale",
			node: Node{ID: "h", BrokerVer: "v2", AgentVer: "v1", Agent: AgentPresent},
			want: false,
			why:  "the #19 half-upgraded host must stay not-at-target",
		},
		{
			name: "(b') agent present but its release is unknown (down / not in the node list)",
			node: Node{ID: "h", BrokerVer: "v2", AgentVer: "", Agent: AgentPresent},
			want: false,
			why:  "a DECLARED agent we cannot see is a fault to surface, never a licence to skip the agent leg",
		},
		{
			name: "(c) no co-located agent at all",
			node: Node{ID: "h", BrokerVer: "v2", AgentVer: "", Agent: AgentAbsent},
			want: true,
			why:  "P3: on a dedicated broker host the daemon IS the whole host; anything else is unreachable-by-construction",
		},
		{
			name: "(c) but the broker itself is stale",
			node: Node{ID: "h", BrokerVer: "v1", AgentVer: "", Agent: AgentAbsent},
			want: false,
			why:  "broker-only must still mean the BROKER is checked",
		},
		{
			name: "unknown presence fails CLOSED (the zero value)",
			node: Node{ID: "h", BrokerVer: "v2", AgentVer: "v1"},
			want: false,
			why:  "a caller that forgets to classify must get the conservative pre-P3 verdict, never the broker-only shortcut",
		},
		{
			name: "empty target is never at target",
			node: Node{ID: "h", BrokerVer: "", AgentVer: "", Agent: AgentAbsent},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.node.AtTarget(target); got != tc.want {
				t.Fatalf("AtTarget(%q) = %v, want %v — %s", target, got, tc.want, tc.why)
			}
		})
	}
}

// TestAgentlessClusterCanActuallyFinishARoll is the P3 exit assertion at plan level: three agentless
// hosts already carrying the target must plan to ZERO work. Before P3 this planned three UPGRADEs, every
// one of which tried to re-exec a non-existent agent.
func TestAgentlessClusterCanActuallyFinishARoll(t *testing.T) {
	nodes := []Node{
		{ID: "brk1", Voter: true, CaughtUp: true, BrokerVer: "v2", Agent: AgentAbsent},
		{ID: "brk2", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v2", Agent: AgentAbsent},
		{ID: "brk3", Voter: true, CaughtUp: true, BrokerVer: "v2", Agent: AgentAbsent},
	}
	p := Compute(nodes, "v2")
	if len(p.Refused) != 0 {
		t.Fatalf("refused: %v", p.Refused)
	}
	if p.Upgrades() != 0 {
		t.Fatalf("an already-at-target agentless cluster planned %d upgrade(s) — this is P3: AtTarget is "+
			"structurally false without an agent, so the roll can never converge and re-runs forever", p.Upgrades())
	}
	for _, s := range p.Steps {
		if s.Kind != StepSkip {
			t.Fatalf("%s: kind %q, want skip", s.NodeID, s.Kind)
		}
	}
}

// TestAgentlessStepsAreMarkedBrokerOnly pins the decision the EXECUTOR consumes. Getting AtTarget right
// is not enough: a stale agentless host still needs an upgrade step, and that step must tell the driver
// not to send reexec-agent. If the flag is dropped the roll HALTs on agent_no_responders exactly as
// before, with a plan that looks correct.
func TestAgentlessStepsAreMarkedBrokerOnly(t *testing.T) {
	nodes := []Node{
		{ID: "brk1", Voter: true, CaughtUp: true, BrokerVer: "v1", Agent: AgentAbsent},
		{ID: "brk2", Voter: true, CaughtUp: true, BrokerVer: "v1", AgentVer: "v1", Agent: AgentPresent},
		{ID: "brk3", IsLeader: true, Voter: true, CaughtUp: true, BrokerVer: "v1", Agent: AgentAbsent},
	}
	p := Compute(nodes, "v2")
	if len(p.Refused) != 0 {
		t.Fatalf("refused: %v", p.Refused)
	}
	got := map[string]bool{}
	for _, s := range p.Steps {
		if s.Kind == StepUpgrade {
			got[s.NodeID] = s.BrokerOnly
		}
	}
	want := map[string]bool{"brk1": true, "brk2": false, "brk3": true}
	if len(got) != len(want) {
		t.Fatalf("upgrade steps = %v, want one per host %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("%s BrokerOnly = %v, want %v — a mis-marked step either re-execs an agent that does not "+
				"exist (HALT) or silently skips a real agent (half-upgraded host)", id, got[id], w)
		}
	}
	// The LEADER's step is built on a separate code path (leader-last + TransferTo); make sure the flag
	// survives it — that path is easy to forget.
	for _, s := range p.Steps {
		if s.NodeID == "brk3" {
			if s.TransferTo == "" {
				t.Fatal("fixture precondition: the leader step should carry a transfer target")
			}
			if !s.BrokerOnly {
				t.Fatal("the leader-last step dropped BrokerOnly — the last host of every agentless roll would HALT")
			}
		}
	}
}
