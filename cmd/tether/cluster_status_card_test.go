package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// cluster_status_card_test.go — B7 DOC#6(a): the --card glance render reformats the SAME report
// (no new verdict) + the DOC#6(c) incident hint on an un-healthy cluster.

func TestStatusCardHealthy(t *testing.T) {
	var buf bytes.Buffer
	renderClusterStatusCard(&buf, &adminsock.ClusterStatusReport{
		Health: "HEALTHY_HA", ExitCode: 0, LeaderID: "brk-a", Verdict: "3 voters, tolerates 1 failure",
		Nodes: []adminsock.ClusterNodeStatus{{NodeID: "brk-a", Phase: "VOTER", Role: "leader", ReleaseVersion: "v1.0.0", Reachable: true, ReachSource: "self"}},
	})
	out := buf.String()
	if !strings.Contains(out, "HEALTHY (HA)") {
		t.Fatalf("healthy headline missing:\n%s", out)
	}
	if strings.Contains(out, "export-incident") {
		t.Fatalf("healthy cluster must NOT show an incident hint:\n%s", out)
	}
}

func TestStatusCardDegradedShowsTopReasonAndIncidentHint(t *testing.T) {
	var buf bytes.Buffer
	renderClusterStatusCard(&buf, &adminsock.ClusterStatusReport{
		Health: "DEGRADED", ExitCode: 1, LeaderID: "brk-a",
		Banner: "a node is mid-join / draining or roster/raft INCONSISTENT", NextStep: "cluster status",
		Nodes: []adminsock.ClusterNodeStatus{
			{NodeID: "brk-a", Phase: "VOTER", Role: "leader", Reachable: true, ReachSource: "self"},
			{NodeID: "brk-b", Phase: "VOTER", Inconsistent: true, Reachable: true, ReachSource: "nats-health"},
		},
	})
	out := buf.String()
	if !strings.Contains(out, "DEGRADED: node brk-b roster/raft INCONSISTENT") {
		t.Fatalf("degraded headline must carry the top reason:\n%s", out)
	}
	if !strings.Contains(out, "export-incident") {
		t.Fatalf("a degraded cluster must show the incident-export hint:\n%s", out)
	}
	if !strings.Contains(out, "what to do:") {
		t.Fatalf("card must show the next step:\n%s", out)
	}
}

func TestStatusCardForceSingle(t *testing.T) {
	var buf bytes.Buffer
	renderClusterStatusCard(&buf, &adminsock.ClusterStatusReport{Health: "FORCE_SINGLE", ExitCode: 3, LeaderID: "brk-a"})
	out := buf.String()
	if !strings.Contains(out, "FORCE-SINGLE") || !strings.Contains(out, "export-incident") {
		t.Fatalf("force-single card wrong:\n%s", out)
	}
}

// TestClusterDoctorOnline — B7 DOC#6(b): the online diagnostic maps health conditions to
// PASS/ADVISORY/FATAL checks (reusing the DoctorCheck types).
func TestClusterDoctorOnline(t *testing.T) {
	checks := clusterDoctorOnline(&adminsock.ClusterStatusReport{
		Health: "DEGRADED", LeaderID: "brk-a",
		Nodes: []adminsock.ClusterNodeStatus{
			{NodeID: "brk-a", Phase: "VOTER", Reachable: true, ReachSource: "self", ReleaseVersion: "v1.0.0"},
			{NodeID: "brk-b", Phase: "VOTER", Inconsistent: true, Reachable: true, ReachSource: "nats-health", ReleaseVersion: "v1.1.0"},
		},
	})
	byName := map[string]string{}
	for _, c := range checks {
		byName[c.Name] = string(c.Status)
	}
	if byName["leader"] != "PASS" {
		t.Fatalf("leader present must PASS: %v", byName)
	}
	if byName["roster_consistency"] != "FATAL" {
		t.Fatalf("an INCONSISTENT node must be FATAL: %v", byName)
	}
	if byName["version_skew"] != "ADVISORY" {
		t.Fatalf("two releases must be a version_skew ADVISORY: %v", byName)
	}
}

// TestClusterDoctorOnlineNoLeader — no leader is FATAL.
func TestClusterDoctorOnlineNoLeader(t *testing.T) {
	checks := clusterDoctorOnline(&adminsock.ClusterStatusReport{Health: "QUORUM_LOST", LeaderID: ""})
	for _, c := range checks {
		if c.Name == "leader" && string(c.Status) != "FATAL" {
			t.Fatalf("no leader must be FATAL, got %s", c.Status)
		}
	}
}
