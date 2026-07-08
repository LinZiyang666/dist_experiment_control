package main

import "testing"

// node_versions_test.go — G5 #19: the pure broker/agent version correlation. Whole-host verdict, skew
// detection, the (assumed) fallback for a pre-G5 broker, and the "?" for missing reports are pinned here.

func rowByID(rows []brokerVersionRow, id string) *brokerVersionRow {
	for i := range rows {
		if rows[i].NodeID == id {
			return &rows[i]
		}
	}
	return nil
}

func TestCorrelateWholeHostAtTarget(t *testing.T) {
	brokers := map[string]brokerDaemon{
		"brk-a": {BrokerVer: "v2", ColocatedAgentNID: "agent-a"},
	}
	agents := map[string]string{"agent-a": "v2"}
	r := rowByID(correlateBrokerVersions(brokers, agents, "v2"), "brk-a")
	if r == nil || !r.WholeHostAt || r.Skew || r.Assumed {
		t.Fatalf("both at v2 with a self-declared agent → WholeHostAt, no skew, not assumed; got %+v", r)
	}
	if r.AgentNID != "agent-a" {
		t.Fatalf("agent nid should be the self-declared one, got %q", r.AgentNID)
	}
}

func TestCorrelateHalfUpgradedSkew(t *testing.T) {
	// broker upgraded, co-located agent still stale → SKEW, NOT whole-host-done (#19's core catch).
	brokers := map[string]brokerDaemon{"brk-a": {BrokerVer: "v2", ColocatedAgentNID: "agent-a"}}
	agents := map[string]string{"agent-a": "v1"}
	r := rowByID(correlateBrokerVersions(brokers, agents, "v2"), "brk-a")
	if r == nil || !r.Skew || r.WholeHostAt {
		t.Fatalf("broker v2 + agent v1 → skew, not whole-host; got %+v", r)
	}
}

func TestCorrelateAssumedFallbackForPreG5Broker(t *testing.T) {
	// a pre-G5 broker omits ColocatedAgentNID → fall back to node_id==nid, flagged Assumed.
	brokers := map[string]brokerDaemon{"brk-a": {BrokerVer: "v2", ColocatedAgentNID: ""}}
	agents := map[string]string{"brk-a": "v2"}
	r := rowByID(correlateBrokerVersions(brokers, agents, "v2"), "brk-a")
	if r == nil || !r.Assumed || r.AgentNID != "brk-a" {
		t.Fatalf("missing agent nid must fall back to node_id==nid, flagged Assumed; got %+v", r)
	}
	if !r.WholeHostAt {
		t.Fatal("the assumed pairing still resolves both versions at target")
	}
}

func TestCorrelateMissingReportsRenderQuestion(t *testing.T) {
	// broker did not report a version; its agent is absent from the node list → both "?", no skew claim.
	brokers := map[string]brokerDaemon{"brk-a": {BrokerVer: "", ColocatedAgentNID: "agent-a"}}
	r := rowByID(correlateBrokerVersions(brokers, map[string]string{}, "v2"), "brk-a")
	if r == nil || r.BrokerVer != "?" || r.AgentVer != "?" {
		t.Fatalf("missing reports must render '?', got %+v", r)
	}
	if r.Skew {
		t.Fatal("two unknowns must not be claimed as a skew")
	}
	if r.WholeHostAt {
		t.Fatal("unknown versions cannot be whole-host-at-target")
	}
}

func TestCorrelateNoTargetNoVerdict(t *testing.T) {
	brokers := map[string]brokerDaemon{"brk-a": {BrokerVer: "v2", ColocatedAgentNID: "agent-a"}}
	agents := map[string]string{"agent-a": "v2"}
	r := rowByID(correlateBrokerVersions(brokers, agents, ""), "brk-a")
	if r == nil || r.WholeHostAt {
		t.Fatalf("no target → no WholeHostAt verdict; got %+v", r)
	}
}

func TestCorrelateDeterministicSort(t *testing.T) {
	brokers := map[string]brokerDaemon{
		"brk-c": {BrokerVer: "v2", ColocatedAgentNID: "c"},
		"brk-a": {BrokerVer: "v2", ColocatedAgentNID: "a"},
		"brk-b": {BrokerVer: "v2", ColocatedAgentNID: "b"},
	}
	rows := correlateBrokerVersions(brokers, map[string]string{}, "v2")
	if rows[0].NodeID != "brk-a" || rows[1].NodeID != "brk-b" || rows[2].NodeID != "brk-c" {
		t.Fatalf("rows must be sorted by NodeID, got %v", rows)
	}
}
