package broker

import (
	"encoding/json"
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
	if h, _, next := computeHealth(true, "a", 3, 0, healthy); h != healthForceSingle {
		t.Errorf("force-single: got %s", h)
	} else if !strings.Contains(next, "--self-id") {
		t.Errorf("force-single next step missing --self-id: %q", next)
	}
	// no leader => quorum-lost (exit 2), NOT from absence of reports but a positive empty-leader.
	if h, _, next := computeHealth(false, "", 3, 0, healthy); h != healthQuorumLost {
		t.Errorf("no-leader: got %s", h)
	} else if !strings.Contains(next, "--self-id") || !strings.Contains(next, "--self-addr") {
		t.Errorf("quorum-lost next step missing force-single identity flags: %q", next)
	}
	// N=3 all VOTER, leader present => HEALTHY_HA.
	if h, _, _ := computeHealth(false, "a", 3, 0, healthy); h != healthHealthyHA {
		t.Errorf("healthy: got %s", h)
	}
	// N=2 => F==0 => DEGRADED even with a leader + all VOTER.
	twoVoters := healthy[:2]
	if h, _, next := computeHealth(false, "a", 2, 0, twoVoters); h != healthDegraded {
		t.Errorf("N=2: got %s", h)
	} else if !strings.Contains(next, "join prepare") || !strings.Contains(next, "join approve") || !strings.Contains(next, "--tunnel-addr") {
		t.Errorf("F==0 next step must name the C8 join workflow (prepare/approve), not the deleted `cluster add`: %q", next)
	}
	// A CATCHING_UP node => DEGRADED.
	joining := []adminsock.ClusterNodeStatus{
		{NodeID: "a", Phase: phaseVoter, Role: "leader"},
		{NodeID: "b", Phase: phaseVoter, Role: "voter"},
		{NodeID: "c", Phase: phaseCatchingUp, Role: "voter"},
	}
	if h, _, _ := computeHealth(false, "a", 3, 0, joining); h != healthDegraded {
		t.Errorf("catching-up: got %s", h)
	}
}

func TestExternalReviewRereviewForceSingleNextStepUsesRecoveryRejoinPrepare(t *testing.T) {
	healthy := []adminsock.ClusterNodeStatus{
		{NodeID: "a", Phase: phaseVoter, Role: "leader"},
		{NodeID: "b", Phase: phaseVoter, Role: "voter"},
		{NodeID: "c", Phase: phaseVoter, Role: "voter"},
	}
	h, _, next := computeHealth(true, "a", 3, 0, healthy)
	if h != healthForceSingle {
		t.Fatalf("force-single health = %s, want %s", h, healthForceSingle)
	}
	// Forbid the BARE deleted command `cluster recover <args>` — matched as "cluster recover " with a
	// trailing space so the valid C8 spelling `cluster recovery rejoin prepare` (which contains "cluster
	// recover" as a substring, but "cluster recover y", never "cluster recover ") is not a false positive.
	if strings.Contains(next, "cluster recover ") {
		t.Fatalf("force-single next step names deleted C8 command `cluster recover`: %q", next)
	}
	if !strings.Contains(next, "cluster recovery rejoin prepare") {
		t.Fatalf("force-single next step must name C8 primary recovery rejoin flow: %q", next)
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
	// Batch-A A13: the synchronous retire path is gone; DrainNode refuses the
	// flag and names the recoverable operation. The last-voter guard itself now
	// lives in StartRetireOperation, which is the only way to retire at all.
	err := admin.DrainNode("single-1", true, true, time.Now(), nil)
	if err == nil || !strings.Contains(err.Error(), "cluster retire") {
		t.Fatalf("drain --retire must be refused and must name `cluster retire`, got %v", err)
	}
}

// TestB1ClusterVerdict pins the plain-language voter-count verdict (the authoritative socket view).
// TestB2ClusterCodeFor (B2 item 4): the broker recognizes its OWN (D7-pinned) cluster error
// substrings into a stable machine Code; an unrecognized error yields "" (→ CLI exit 70).
func TestB2ClusterCodeFor(t *testing.T) {
	cases := []struct{ in, want string }{
		{"cluster add x: catch_up_stalled after 5s (barrier=3)", adminsock.CodeCatchUpStalled},
		{"transfer-leader: ghost is not in the raft configuration", adminsock.CodeNotAVoter},
		{"transfer-leader: x is not a voter (cannot be leader)", adminsock.CodeNotAVoter},
		{"cannot retire the last voter", adminsock.CodeNotAVoter},
		{(&ErrRemoveOwnsResources{NodeID: "x", Exposes: 2}).Error(), adminsock.CodeRemoveOwnsResources}, // B3 item 7
		{"some random store failure", ""},
	}
	for _, c := range cases {
		if got := clusterCodeFor(errors.New(c.in)); got != c.want {
			t.Errorf("clusterCodeFor(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if clusterCodeFor(nil) != "" {
		t.Error("clusterCodeFor(nil) must be \"\"")
	}
	// The authored literal "still HOMES" must be in the error (clusterCodeFor + D7 pin key on it).
	if !strings.Contains((&ErrRemoveOwnsResources{NodeID: "x", Exposes: 1}).Error(), "still HOMES") {
		t.Error("ErrRemoveOwnsResources must contain the authored 'still HOMES' literal")
	}
}

func TestB1ClusterVerdict(t *testing.T) {
	cases := []struct {
		voters   int
		streamOK bool
		health   string
		wantSub  string // "" = empty verdict
	}{
		{0, true, healthHealthyHA, ""},
		{1, true, healthDegraded, "NO redundancy"},
		{2, true, healthDegraded, "NO fault-tolerant writes"},
		{2, true, healthHealthyHA, "NO fault-tolerant writes"}, // voters==2 branch wins over health (precedence)
		{3, true, healthHealthyHA, "survives 1 broker failure"},
		{4, true, healthHealthyHA, "survives 1 broker failure"}, // even voter: quorum 3, F=1
		{5, true, healthHealthyHA, "survives 2 broker failure"},
		{3, false, healthHealthyHA, "DEGRADED right now"}, // streams below target
		{3, true, healthDegraded, "DEGRADED right now"},   // health not HEALTHY_HA
	}
	for _, c := range cases {
		got := clusterVerdict(c.voters, c.streamOK, c.health)
		if c.wantSub == "" {
			if got != "" {
				t.Errorf("clusterVerdict(%d,%v,%s) = %q, want empty", c.voters, c.streamOK, c.health, got)
			}
			continue
		}
		if !strings.Contains(got, c.wantSub) {
			t.Errorf("clusterVerdict(%d,%v,%s) = %q, want substring %q", c.voters, c.streamOK, c.health, got, c.wantSub)
		}
	}
}

func TestB1StreamsAtTarget(t *testing.T) {
	cases := []struct {
		name  string
		nodes []adminsock.ClusterNodeStatus
		want  bool
	}{
		{"empty", nil, true},
		{"at target", []adminsock.ClusterNodeStatus{{StreamTarget: 3, StreamActual: 3}}, true},
		{"below target", []adminsock.ClusterNodeStatus{{StreamTarget: 3, StreamActual: 1}}, false},
		{"target 0 ignored", []adminsock.ClusterNodeStatus{{StreamTarget: 0, StreamActual: 0}}, true},
		{"one below among many", []adminsock.ClusterNodeStatus{{StreamTarget: 3, StreamActual: 3}, {StreamTarget: 3, StreamActual: 2}}, false},
	}
	for _, c := range cases {
		if got := streamsAtTarget(c.nodes); got != c.want {
			t.Errorf("%s: streamsAtTarget = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestB1IsLeaderViewSerializes: is_leader_view=false MUST serialize (a monitor needs the
// non-leader signal); view_host/verdict use omitempty; schema_version stays 1; and a v1-shaped
// payload (no new keys) still unmarshals cleanly (forward-compat).
func TestB1IsLeaderViewSerializes(t *testing.T) {
	b, err := json.Marshal(adminsock.ClusterStatusReport{SchemaVersion: 1, IsLeaderView: false})
	if err != nil {
		t.Fatal(err)
	}
	js := string(b)
	if !strings.Contains(js, `"is_leader_view":false`) {
		t.Errorf("is_leader_view=false must serialize (no omitempty): %s", js)
	}
	if strings.Contains(js, "view_host") || strings.Contains(js, "verdict") {
		t.Errorf("empty view_host/verdict must be omitted: %s", js)
	}
	if !strings.Contains(js, `"schema_version":1`) {
		t.Errorf("schema_version must stay 1: %s", js)
	}
	// Forward-compat: a v1 payload without the new keys unmarshals with zero values, no error.
	var rep adminsock.ClusterStatusReport
	if err := json.Unmarshal([]byte(`{"schema_version":1,"view":"ctl-nats","health":"HEALTHY_HA","nodes":[]}`), &rep); err != nil {
		t.Fatalf("v1-shaped payload must unmarshal: %v", err)
	}
	if rep.IsLeaderView || rep.ViewHost != "" || rep.Verdict != "" {
		t.Errorf("missing new keys must zero-value, got %+v", rep)
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
	// B1: the report is stamped as this node's (leader) view, with the N=1 plain-language verdict.
	if rep.ViewHost != "single-1" {
		t.Errorf("ViewHost = %q, want single-1", rep.ViewHost)
	}
	if !rep.IsLeaderView {
		t.Error("a single-node leader must report IsLeaderView=true")
	}
	if !strings.Contains(rep.Verdict, "NO redundancy") {
		t.Errorf("N=1 verdict = %q, want 'NO redundancy'", rep.Verdict)
	}
}

// origin: batch B2 independent external review F2
func TestClusterStatusSchemaBumpsWhenStreamActualGainsAnUnobservedSentinel(t *testing.T) {
	rep := adminsock.ClusterStatusReport{
		SchemaVersion: statusSchemaVersion,
		Nodes: []adminsock.ClusterNodeStatus{{
			StreamActual: StreamActualUnobserved,
			StreamTarget: 3,
		}},
	}
	payload, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	if StreamActualUnobserved < 0 && statusSchemaVersion < 2 {
		t.Fatalf("cluster status emits a new negative stream_actual value under schema_version=%d: %s; "+
			"docs/usage.md requires a schema bump when a field's value domain or semantics changes",
			statusSchemaVersion, payload)
	}
}
