package clusterspec

import (
	"strings"
	"testing"
)

func TestParseDefaultsAndValidation(t *testing.T) {
	s, err := Parse([]byte("nodes:\n  - node_id: brk-a\n  - node_id: brk-b\n    desired: absent\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Nodes[0].Desired != "voter" {
		t.Fatalf("empty desired must default to voter, got %q", s.Nodes[0].Desired)
	}
	if _, err := Parse([]byte("nodes:\n  - node_id: x\n    desired: bogus\n")); err == nil {
		t.Fatal("bogus desired must error")
	}
	if _, err := Parse([]byte("nodes:\n  - raft_addr: x\n")); err == nil {
		t.Fatal("missing node_id must error")
	}
}

// TestParseRejectsDuplicateNodeID — Stage-C m10.
func TestParseRejectsDuplicateNodeID(t *testing.T) {
	if _, err := Parse([]byte("nodes:\n  - node_id: brk-a\n  - node_id: brk-a\n    desired: absent\n")); err == nil {
		t.Fatal("a duplicate node_id must be rejected")
	}
}

func TestDiffAddAndRetireOrdered(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: brk-a\n  - node_id: brk-b\n  - node_id: brk-c\n  - node_id: brk-old\n    desired: absent\n"))
	live := []LiveNode{
		{NodeID: "brk-a", Phase: "VOTER", Role: "leader"},
		{NodeID: "brk-b", Phase: "VOTER", Role: "voter"},
		{NodeID: "brk-old", Phase: "VOTER", Role: "voter"},
	}
	steps := Diff(spec, live, "brk-a")
	if len(steps) == 0 {
		t.Fatal("expected steps")
	}
	if steps[0].Order != 0 || steps[0].NodeID != "brk-c" {
		t.Fatalf("first step should be add brk-c: %+v", steps[0])
	}
	last := steps[len(steps)-1]
	if last.Order != 9 || last.NodeID != "brk-old" {
		t.Fatalf("last step should be retire brk-old: %+v", last)
	}
}

func TestDiffRefusesLastVoterRetire(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: brk-a\n    desired: absent\n"))
	live := []LiveNode{{NodeID: "brk-a", Phase: "VOTER", Role: "leader"}}
	steps := Diff(spec, live, "brk-a")
	if len(steps) != 1 || !strings.HasPrefix(steps[0].Reason, "REFUSED") {
		t.Fatalf("retiring the last voter must be a single REFUSED step: %+v", steps)
	}
}

// TestDiffRetireLearnerNotRefused — Stage-C M2 + External-review F12: a non-voter retire is never
// REFUSED for a quorum reason and never decrements the voter floor; AND it never emits a
// `cluster remove` the backend would reject (only RETIRING / VOTER_ADD_FAILED get a real remove;
// CATCHING_UP / INCONSISTENT get a `cluster doctor` diagnostic instead).
func TestDiffRetireLearnerNotRefused(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: a\n  - node_id: b\n  - node_id: c\n    desired: absent\n  - node_id: d\n    desired: absent\n  - node_id: e\n    desired: absent\n"))
	live := []LiveNode{
		{NodeID: "a", Phase: "VOTER", Role: "leader"},
		{NodeID: "b", Phase: "VOTER", Role: "voter"},
		{NodeID: "c", Phase: "CATCHING_UP", Role: "learner"}, // mid-join non-voter
		{NodeID: "d", Phase: "VOTER", Role: ""},              // INCONSISTENT (roster voter, not in raft config)
		{NodeID: "e", Phase: "VOTER_ADD_FAILED", Role: ""},   // a finishable non-voter
	}
	byID := map[string]Step{}
	for _, s := range Diff(spec, live, "a") {
		byID[s.NodeID] = s
	}
	for _, id := range []string{"c", "d", "e"} {
		if strings.HasPrefix(byID[id].Reason, "REFUSED") {
			t.Fatalf("non-voter %s must NOT be REFUSED: %+v", id, byID[id])
		}
	}
	if strings.Contains(byID["c"].Verb, "recovery node remove") || !strings.Contains(byID["c"].Verb, "cluster doctor") {
		t.Fatalf("a CATCHING_UP non-voter must get a diagnostic, not a backend-rejected remove: %+v", byID["c"])
	}
	if strings.Contains(byID["d"].Verb, "recovery node remove") || !strings.Contains(byID["d"].Verb, "cluster doctor") {
		t.Fatalf("an INCONSISTENT non-voter must get a diagnostic, not a backend-rejected remove: %+v", byID["d"])
	}
	if !strings.Contains(byID["e"].Verb, "recovery node remove") {
		t.Fatalf("a VOTER_ADD_FAILED non-voter SHOULD get a real `recovery node remove --manual`: %+v", byID["e"])
	}
}

// TestDiffLeaderSoleVoterRetireNoTransferStep — Stage-C M3: a refused last-voter-leader retire emits
// ONLY the REFUSED step (no contradictory transfer-leader step).
func TestDiffLeaderSoleVoterRetireNoTransferStep(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: a\n    desired: absent\n  - node_id: c\n    desired: absent\n"))
	live := []LiveNode{
		{NodeID: "a", Phase: "VOTER", Role: "leader"},
		{NodeID: "c", Phase: "CATCHING_UP", Role: "learner"},
	}
	steps := Diff(spec, live, "a")
	for _, s := range steps {
		if s.Order == 5 {
			t.Fatalf("a refused leader-retire must NOT emit a transfer-leader step: %+v", s)
		}
	}
}

// TestDiffTransferLeaderNamesRealTarget — Stage-C M3: a valid leader retire names a concrete voter.
func TestDiffTransferLeaderNamesRealTarget(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: b\n  - node_id: c\n  - node_id: a\n    desired: absent\n"))
	live := []LiveNode{
		{NodeID: "a", Phase: "VOTER", Role: "leader"},
		{NodeID: "b", Phase: "VOTER", Role: "voter"},
		{NodeID: "c", Phase: "VOTER", Role: "voter"},
	}
	steps := Diff(spec, live, "a")
	found := false
	for _, s := range steps {
		if s.Order == 5 {
			found = true
			if strings.Contains(s.Verb, "<another-voter>") {
				t.Fatalf("transfer-leader must name a real voter, not the placeholder: %+v", s)
			}
		}
	}
	if !found {
		t.Fatalf("a valid leader retire must emit a transfer-leader step: %+v", steps)
	}
}

// TestDiffMultiRetireDoesNotDropBelowFloorBeforeAddVerified — Stage-C M4: with a pending add, a
// multi-retire that would cross below 2 voters is REFUSED (the add isn't a voter yet).
func TestDiffMultiRetireDoesNotDropBelowFloorBeforeAddVerified(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: d\n  - node_id: a\n    desired: absent\n  - node_id: b\n    desired: absent\n  - node_id: c\n    desired: absent\n"))
	live := []LiveNode{
		{NodeID: "a", Phase: "VOTER", Role: "voter"},
		{NodeID: "b", Phase: "VOTER", Role: "voter"},
		{NodeID: "c", Phase: "VOTER", Role: "leader"},
	}
	steps := Diff(spec, live, "c")
	drains := 0
	refused := 0
	for _, s := range steps {
		if strings.Contains(s.Verb, "cluster retire") {
			drains++
		}
		if strings.HasPrefix(s.Reason, "REFUSED") {
			refused++
		}
	}
	// With 3 voters all-absent + 1 pending add: only the FIRST voter-retire (3→2) is allowed; the
	// next (2→1) is REFUSED until d is a verified voter.
	if drains != 1 || refused < 1 {
		t.Fatalf("multi-retire must stop at 2 voters while an add is pending (drains=%d refused=%d): %+v", drains, refused, steps)
	}
}

// TestDiffTwoVotersBothRetireNoPlaceholder — Audit QS-MAJOR-1: leader+only-other-voter both absent
// → a single REFUSED step, NO placeholder transfer, NO drain.
func TestDiffTwoVotersBothRetireNoPlaceholder(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: a\n    desired: absent\n  - node_id: b\n    desired: absent\n"))
	live := []LiveNode{
		{NodeID: "a", Phase: "VOTER", Role: "leader"},
		{NodeID: "b", Phase: "VOTER", Role: "voter"},
	}
	steps := Diff(spec, live, "a")
	for _, s := range steps {
		if strings.Contains(s.Verb, "<another-voter>") {
			t.Fatalf("must not emit a placeholder transfer: %+v", s)
		}
		if s.NodeID == "a" && strings.Contains(s.Verb, "cluster retire") {
			t.Fatalf("leader-with-no-surviving-voter must be REFUSED, not drained: %+v", s)
		}
	}
}

// TestApplyPlanEmitsRunnableVerbs (C8 review M1) — every Step.Verb the reconcile plan prints must be a
// command that EXISTS post-C8: the join step uses `join prepare --node-id` (NOT the non-existent
// --self-id flag), retire uses `cluster retire`, a VOTER_ADD_FAILED non-voter uses `recovery node
// remove … --manual`, and NO verb names a deleted spelling (cluster add / sign-join / drain --retire /
// bare `cluster remove`).
func TestApplyPlanEmitsRunnableVerbs(t *testing.T) {
	spec, _ := Parse([]byte("nodes:\n  - node_id: a\n  - node_id: b\n  - node_id: nu\n  - node_id: e\n    desired: absent\n  - node_id: old\n    desired: absent\n"))
	live := []LiveNode{
		{NodeID: "a", Phase: "VOTER", Role: "leader"},
		{NodeID: "b", Phase: "VOTER", Role: "voter"},
		{NodeID: "e", Phase: "VOTER_ADD_FAILED", Role: ""}, // gets a real recovery node remove
		{NodeID: "old", Phase: "VOTER", Role: "voter"},     // gets cluster retire
		// "nu" is desired=voter but absent → the join step.
	}
	steps := Diff(spec, live, "a")
	byID := map[string]Step{}
	for _, s := range steps {
		byID[s.NodeID] = s
		for _, bad := range []string{"cluster add", "sign-join", "drain --retire", "--self-id", "tether cluster remove "} {
			if strings.Contains(s.Verb, bad) {
				t.Errorf("plan emits a deleted/non-existent verb token %q: %q", bad, s.Verb)
			}
		}
	}
	if !strings.Contains(byID["nu"].Verb, "join prepare --node-id") {
		t.Errorf("join step must use `join prepare --node-id` (the real flag): %q", byID["nu"].Verb)
	}
	if !strings.Contains(byID["old"].Verb, "cluster retire") {
		t.Errorf("a live retire must use `cluster retire`: %q", byID["old"].Verb)
	}
	if !strings.Contains(byID["e"].Verb, "recovery node remove") || !strings.Contains(byID["e"].Verb, "--manual") {
		t.Errorf("a VOTER_ADD_FAILED remove must use `recovery node remove … --manual`: %q", byID["e"].Verb)
	}
}

// TestParseRejectsBadNodeID — Audit SEC-MAJOR-1 / External-review F13: an injection-shaped node_id
// (shell metachars, newline, path separators, whitespace) is rejected by ValidateClusterNodeID.
// Case IS allowed (a broker node_id is a deployment-chosen server name, not a per-session leaf nid).
func TestParseRejectsBadNodeID(t *testing.T) {
	for _, bad := range []string{"x; rm -rf /", "a\nb", "../etc", "has space", "a/b", "a.b", "a|b", "a$b"} {
		if _, err := Parse([]byte("nodes:\n  - node_id: \"" + bad + "\"\n")); err == nil {
			t.Fatalf("node_id %q must be rejected", bad)
		}
	}
	// valid: lowercase, uppercase, digits, hyphen, underscore.
	for _, ok := range []string{"brk-a1", "node-A", "Broker_01"} {
		if _, err := Parse([]byte("nodes:\n  - node_id: " + ok + "\n")); err != nil {
			t.Fatalf("a valid node_id %q must pass: %v", ok, err)
		}
	}
}
