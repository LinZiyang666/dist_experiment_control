package d7_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// fixtures_test.go — D7 §13.13 cheap status contract (make test): the 5 render states
// each carry the contracted exit code, and the --json schema is stable (field names
// present). The behavioral no-:7400 + multi-broker aggregation drills are gated
// d7_integration. These fixtures double as the Stage-C acceptance samples (§17).

type fixture struct {
	name     string
	report   adminsock.ClusterStatusReport
	wantExit int
}

func d7Fixtures() []fixture {
	return []fixture{
		{"healthy-ha", adminsock.ClusterStatusReport{
			SchemaVersion: 1, View: "ctl-nats", Health: "HEALTHY_HA", ExitCode: 0, LeaderID: "br-1",
			Nodes: []adminsock.ClusterNodeStatus{
				{NodeID: "br-1", Name: "alpha", Phase: "VOTER", Role: "leader", StreamActual: 3, StreamTarget: 3, Reachable: true, ReachSource: "self-report", AccountNkMatch: true},
				{NodeID: "br-2", Name: "beta", Phase: "VOTER", Role: "voter", StreamActual: 3, StreamTarget: 3, Reachable: true, ReachSource: "self-report", AccountNkMatch: true},
				{NodeID: "br-3", Name: "gamma", Phase: "VOTER", Role: "voter", StreamActual: 3, StreamTarget: 3, Reachable: true, ReachSource: "self-report", AccountNkMatch: true},
			},
		}, 0},
		{"degraded-catching-up", adminsock.ClusterStatusReport{
			SchemaVersion: 1, View: "ctl-nats", Health: "DEGRADED", ExitCode: 1, LeaderID: "br-1", Banner: "a node is mid-join",
			Nodes: []adminsock.ClusterNodeStatus{
				{NodeID: "br-3", Name: "gamma", Phase: "CATCHING_UP", Role: "voter", Reachable: true, ReachSource: "self-report"},
			},
		}, 1},
		{"quorum-lost", adminsock.ClusterStatusReport{
			SchemaVersion: 1, View: "ctl-nats", Health: "QUORUM_LOST", ExitCode: 2, LeaderID: "",
			Banner: "READ-ONLY; after confirming peers dead, force-single on a survivor (go to broker host br-1)",
			Nodes: []adminsock.ClusterNodeStatus{
				{NodeID: "br-2", Name: "beta", Phase: "VOTER", Role: "voter", Reachable: false, ReachSource: "self-report"},
			},
		}, 2},
		{"force-single", adminsock.ClusterStatusReport{
			SchemaVersion: 1, View: "offline", Health: "FORCE_SINGLE", ExitCode: 3, LeaderID: "br-1",
			Nodes: []adminsock.ClusterNodeStatus{
				{NodeID: "br-1", Name: "alpha", Phase: "VOTER", Role: "leader", Reachable: true, ReachSource: "raft-ping"},
			},
		}, 3},
		{"joining-add-failed", adminsock.ClusterStatusReport{
			SchemaVersion: 1, View: "ctl-nats", Health: "DEGRADED", ExitCode: 1, LeaderID: "br-1", NextStep: "cluster doctor",
			Nodes: []adminsock.ClusterNodeStatus{
				{NodeID: "br-4", Name: "delta", Phase: "VOTER_ADD_FAILED", Role: "", Reachable: true, ReachSource: "self-report", Inconsistent: true},
			},
		}, 1},
	}
}

// exitForHealth mirrors the §17 health→exit-code contract (the broker-side SSOT
// healthExitCode is unexported; the non-vacuous mapping test lives in
// internal/broker as TestD7HealthExitCodeSSOT). Here we assert each fixture is
// SELF-CONSISTENT (its health implies its exit code) — catching a fixture/contract
// drift rather than comparing a literal to itself (review M10).
func exitForHealth(h string) int {
	switch h {
	case "HEALTHY_HA":
		return 0
	case "DEGRADED":
		return 1
	case "QUORUM_LOST":
		return 2
	case "FORCE_SINGLE":
		return 3
	default:
		return -1
	}
}

func TestD7StatusFixturesExitCodes(t *testing.T) {
	for _, f := range d7Fixtures() {
		derived := exitForHealth(f.report.Health)
		if derived != f.wantExit {
			t.Errorf("%s: health %q maps to exit %d, want %d", f.name, f.report.Health, derived, f.wantExit)
		}
		if f.report.ExitCode != derived {
			t.Errorf("%s: fixture exit_code %d disagrees with its health %q (=> %d)", f.name, f.report.ExitCode, f.report.Health, derived)
		}
	}
}

func TestD7StatusJSONSchemaStable(t *testing.T) {
	for _, f := range d7Fixtures() {
		b, err := json.Marshal(f.report)
		if err != nil {
			t.Fatalf("%s: marshal: %v", f.name, err)
		}
		s := string(b)
		for _, field := range []string{`"view"`, `"health"`, `"exit_code"`, `"leader_id"`, `"nodes"`} {
			if !strings.Contains(s, field) {
				t.Errorf("%s: --json schema missing %s", f.name, field)
			}
		}
		// reach_source must travel on every node (the no-false-quorum-lost discriminator).
		if len(f.report.Nodes) > 0 && !strings.Contains(s, `"reach_source"`) {
			t.Errorf("%s: --json node schema missing reach_source", f.name)
		}
		// Round-trip stability.
		var back adminsock.ClusterStatusReport
		if err := json.Unmarshal(b, &back); err != nil {
			t.Errorf("%s: round-trip: %v", f.name, err)
		}
		if back.Health != f.report.Health || back.ExitCode != f.report.ExitCode {
			t.Errorf("%s: round-trip drift", f.name)
		}
	}
}
