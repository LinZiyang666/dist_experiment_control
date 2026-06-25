package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
)

// B1 item 1 — the ctl-over-NATS cluster-status pure cores. These run in `make test` with no
// nats-server (the os.Exit-free logic is fully decoupled from the NATS dial).

func TestSummarizeClusterHealth(t *testing.T) {
	writable := func(id string) proto.ClusterHealthResp {
		return proto.ClusterHealthResp{NodeID: id, WritableLeaderConfirmed: true, LeaderID: id, LeaderContactStale: false}
	}
	staleFollower := func(id string) proto.ClusterHealthResp {
		return proto.ClusterHealthResp{NodeID: id, WritableLeaderConfirmed: false, LeaderContactStale: true}
	}
	cases := []struct {
		name     string
		replies  []proto.ClusterHealthResp
		wantSeen int
		wantAny  bool
		wantWr   bool
		wantStal bool
		wantFS   bool
	}{
		{"zero replies", nil, 0, false, false, false, false},
		{"n=1 writable", []proto.ClusterHealthResp{writable("a")}, 1, true, true, false, false},
		// A FLAPPING broker answering 3x must not fake a quorum-sized count.
		{"duplicate NodeID", []proto.ClusterHealthResp{writable("a"), writable("a"), writable("a")}, 1, true, true, false, false},
		// Empty NodeID is not counted as distinct (BrokersSeen stays 0 though AnyReply is true).
		{"empty NodeID", []proto.ClusterHealthResp{{WritableLeaderConfirmed: true}}, 0, true, true, false, false},
		{"mixed a,a,b", []proto.ClusterHealthResp{writable("a"), {NodeID: "a"}, {NodeID: "b"}}, 2, true, true, false, false},
		{"3 stale no-leader", []proto.ClusterHealthResp{staleFollower("a"), staleFollower("b"), staleFollower("c")}, 3, true, false, true, false},
		// The case ALL drafts missed: some non-stale, none writable = electing, NOT quorum-lost.
		{"not-stale-mixed", []proto.ClusterHealthResp{staleFollower("a"), {NodeID: "b", LeaderContactStale: false}}, 2, true, false, false, false},
		{"force-single", []proto.ClusterHealthResp{{NodeID: "a", ForceSingleActive: true, WritableLeaderConfirmed: true}}, 1, true, true, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := summarizeClusterHealth(c.replies)
			if s.BrokersSeen != c.wantSeen {
				t.Errorf("BrokersSeen = %d, want %d", s.BrokersSeen, c.wantSeen)
			}
			if s.AnyReply != c.wantAny {
				t.Errorf("AnyReply = %v, want %v", s.AnyReply, c.wantAny)
			}
			if s.WritableLeaderSeen != c.wantWr {
				t.Errorf("WritableLeaderSeen = %v, want %v", s.WritableLeaderSeen, c.wantWr)
			}
			if s.AllStale != c.wantStal {
				t.Errorf("AllStale = %v, want %v", s.AllStale, c.wantStal)
			}
			if s.ForceSingleActive != c.wantFS {
				t.Errorf("ForceSingleActive = %v, want %v", s.ForceSingleActive, c.wantFS)
			}
		})
	}
}

func TestCtlExitCode(t *testing.T) {
	cases := []struct {
		name string
		s    ctlClusterSummary
		want int
	}{
		// Headline: a single-broker laptop user (zero replies) must NOT get a nonzero alarm.
		{"zero replies", ctlClusterSummary{}, 0},
		{"writable", ctlClusterSummary{AnyReply: true, WritableLeaderSeen: true, BrokersSeen: 3}, 0},
		// force-single takes precedence over writable (it IS writable, but it is an emergency).
		{"force-single+writable", ctlClusterSummary{AnyReply: true, WritableLeaderSeen: true, ForceSingleActive: true}, 3},
		{"read-only all-stale", ctlClusterSummary{AnyReply: true, AllStale: true, BrokersSeen: 3}, 2},
		{"electing transient", ctlClusterSummary{AnyReply: true, BrokersSeen: 2}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ctlExitCode(c.s); got != c.want {
				t.Errorf("ctlExitCode(%+v) = %d, want %d", c.s, got, c.want)
			}
		})
	}
}

func TestCtlVerdictLine(t *testing.T) {
	cases := []struct {
		name    string
		s       ctlClusterSummary
		wantSub string
	}{
		{"zero", ctlClusterSummary{}, "no cluster detected"},
		{"force-single", ctlClusterSummary{AnyReply: true, ForceSingleActive: true}, "at least one broker reports emergency"},
		{"writable >=3", ctlClusterSummary{AnyReply: true, WritableLeaderSeen: true, BrokersSeen: 3}, "looks healthy"},
		{"writable <3", ctlClusterSummary{AnyReply: true, WritableLeaderSeen: true, BrokersSeen: 1}, "fewer than 3 answered"},
		{"read-only", ctlClusterSummary{AnyReply: true, AllStale: true, BrokersSeen: 3}, "READ-ONLY"},
		{"electing", ctlClusterSummary{AnyReply: true, BrokersSeen: 2}, "electing a leader (transient)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ctlVerdictLine(c.s); !strings.Contains(got, c.wantSub) {
				t.Errorf("ctlVerdictLine(%+v) = %q, want substring %q", c.s, got, c.wantSub)
			}
		})
	}
}

// TestRenderCtlStatus: text mode points at the broker-host command for the full table; --json
// mode emits a parseable summary; the no-cluster case stays exit 0 / friendly.
func TestRenderCtlStatus(t *testing.T) {
	// Text, writable cluster.
	s := summarizeClusterHealth([]proto.ClusterHealthResp{
		{NodeID: "a", WritableLeaderConfirmed: true, LeaderID: "a"},
		{NodeID: "b"}, {NodeID: "c"},
	})
	var sb strings.Builder
	renderCtlStatus(&sb, s, false)
	out := sb.String()
	if !strings.Contains(out, "broker(s) answered over NATS") || !strings.Contains(out, "tether cluster status` on a broker host") {
		t.Fatalf("text render missing aggregate/leader pointer: %q", out)
	}

	// --json must be valid JSON of the summary.
	var sj strings.Builder
	renderCtlStatus(&sj, s, true)
	var decoded ctlClusterSummary
	if err := json.Unmarshal([]byte(sj.String()), &decoded); err != nil {
		t.Fatalf("--json not parseable: %v (%q)", err, sj.String())
	}
	if decoded.BrokersSeen != 3 || !decoded.WritableLeaderSeen {
		t.Fatalf("--json round-trip wrong: %+v", decoded)
	}

	// No cluster (zero replies): friendly single-broker note, exit 0.
	var sn strings.Builder
	zero := summarizeClusterHealth(nil)
	renderCtlStatus(&sn, zero, false)
	if !strings.Contains(sn.String(), "single-broker is fully supported") {
		t.Fatalf("no-cluster render must reassure single-broker users: %q", sn.String())
	}
	if ctlExitCode(zero) != 0 {
		t.Fatalf("no-cluster exit must be 0, got %d", ctlExitCode(zero))
	}
}

// TestRenderCtlStatusJSONDiscriminator (B1 review F6): the ctl --remote --json shape carries a
// "view":"ctl-remote" discriminator and deliberately NO schema_version (it is NOT the socket
// ClusterStatusReport monitor contract).
func TestRenderCtlStatusJSONDiscriminator(t *testing.T) {
	var sj strings.Builder
	s := summarizeClusterHealth([]proto.ClusterHealthResp{{NodeID: "a", WritableLeaderConfirmed: true}})
	renderCtlStatus(&sj, s, true)
	out := sj.String()
	if !strings.Contains(out, `"view": "ctl-remote"`) {
		t.Errorf("ctl --json must carry the view discriminator: %q", out)
	}
	// External-review Q3: the ctl summary now ALSO carries the (schema, schema_version) pair the rest
	// of the B2 machine-JSON contract uses (superseding the earlier view-only B1 F6 decision).
	if !strings.Contains(out, `"schema": "ctl_cluster_summary"`) || !strings.Contains(out, `"schema_version": 1`) {
		t.Errorf("ctl --json must carry the (schema, schema_version) discriminator: %q", out)
	}
}

// TestSummarizeLeaderIDWritableOnly (B1 review F4): a follower stamps LeaderID on its reply, but
// the ctl fold deliberately trusts a leader id ONLY from a writable reply (a stale follower-named
// leader is not propagated).
func TestSummarizeLeaderIDWritableOnly(t *testing.T) {
	s := summarizeClusterHealth([]proto.ClusterHealthResp{{NodeID: "x", LeaderID: "x", WritableLeaderConfirmed: false}})
	if s.LeaderID != "" {
		t.Errorf("LeaderID from a non-writable reply must NOT propagate, got %q", s.LeaderID)
	}
}

// TestCtlVerdictForceSingleStraggler (B1 review F5): a single straggler still reporting
// force_single_active among >=2 writable replies must NOT make the verdict falsely claim the
// cluster is "running on one broker".
func TestCtlVerdictForceSingleStraggler(t *testing.T) {
	s := summarizeClusterHealth([]proto.ClusterHealthResp{
		{NodeID: "a", WritableLeaderConfirmed: true, LeaderID: "a"},
		{NodeID: "b", WritableLeaderConfirmed: true},
		{NodeID: "c", ForceSingleActive: true, WritableLeaderConfirmed: true},
	})
	v := ctlVerdictLine(s)
	if strings.Contains(v, "running on one broker") {
		t.Errorf("verdict must not falsely claim 'running on one broker' with 3 writable replies: %q", v)
	}
	if !strings.Contains(v, "at least one broker reports emergency") {
		t.Errorf("force-single straggler verdict should be hedged: %q", v)
	}
}

// TestCtlVerdictNoNuclearLeak (B1 review F9): NO ctl verdict line nudges an imperative operator
// escape-hatch command (cluster force-single / cluster recover). The descriptive force-single
// state is fine, but it must never read as a command to run.
func TestCtlVerdictNoNuclearLeak(t *testing.T) {
	states := []ctlClusterSummary{
		{},
		{AnyReply: true, ForceSingleActive: true, WritableLeaderSeen: true, BrokersSeen: 3},
		{AnyReply: true, WritableLeaderSeen: true, BrokersSeen: 3},
		{AnyReply: true, WritableLeaderSeen: true, BrokersSeen: 1},
		{AnyReply: true, AllStale: true, BrokersSeen: 3},
		{AnyReply: true, BrokersSeen: 2},
	}
	for _, s := range states {
		v := ctlVerdictLine(s)
		for _, banned := range []string{"cluster force-single", "cluster recover"} {
			if strings.Contains(v, banned) {
				t.Errorf("ctl verdict nudges the operator-only command %q: %q", banned, v)
			}
		}
	}
}

// TestCtlVerdictEmptyNodeIDFloorsToOne (B1 review proposed #9): a writable reply with an empty
// NodeID still counts as "1 broker(s)" through the verdict (the <3 writable branch).
func TestCtlVerdictEmptyNodeIDFloorsToOne(t *testing.T) {
	s := summarizeClusterHealth([]proto.ClusterHealthResp{{WritableLeaderConfirmed: true}})
	v := ctlVerdictLine(s)
	if !strings.Contains(v, "1 broker(s) answered with a writable leader") {
		t.Errorf("empty-NodeID writable reply should floor to '1 broker(s)': %q", v)
	}
}

// TestRenderClusterStatusFooter (B1 review F2): the operator-socket render footer (legend +
// verdict + view-source) carries B1's three new blocks and the leader/follower wording.
func TestRenderClusterStatusFooter(t *testing.T) {
	render := func(rep *adminsock.ClusterStatusReport) string {
		cmd := &cobra.Command{}
		var buf bytes.Buffer
		cmd.SetOut(&buf)
		renderClusterStatus(cmd, rep)
		return buf.String()
	}

	// Leader view + verdict + legend present.
	leader := render(&adminsock.ClusterStatusReport{
		Health: "HEALTHY_HA", ExitCode: 0, ViewHost: "n1", IsLeaderView: true,
		Verdict: "HA active — writes are replicated; the cluster survives 1 broker failure(s).",
	})
	if !strings.Contains(leader, "view: authoritative (leader n1).") {
		t.Errorf("leader footer wrong: %q", leader)
	}
	if !strings.Contains(leader, "verdict: HA active") {
		t.Errorf("verdict line missing: %q", leader)
	}
	if !strings.Contains(leader, "columns: LAG=") {
		t.Errorf("legend block missing: %q", leader)
	}

	// Follower view → "re-run on the leader (n1)".
	follower := render(&adminsock.ClusterStatusReport{
		Health: "DEGRADED", ExitCode: 1, ViewHost: "n2", LeaderID: "n1", IsLeaderView: false,
	})
	if !strings.Contains(follower, "this is n2's local view") || !strings.Contains(follower, "re-run on the leader (n1)") {
		t.Errorf("follower footer wrong: %q", follower)
	}

	// Follower with unknown leader → "(unknown)".
	noLeader := render(&adminsock.ClusterStatusReport{ViewHost: "n2", LeaderID: "", IsLeaderView: false})
	if !strings.Contains(noLeader, "re-run on the leader (unknown)") {
		t.Errorf("empty-leader footer should say unknown: %q", noLeader)
	}

	// Empty ViewHost → no view: line; empty Verdict → no verdict: line.
	bare := render(&adminsock.ClusterStatusReport{Health: "HEALTHY_HA"})
	if strings.Contains(bare, "view:") {
		t.Errorf("empty ViewHost must not print a view: line: %q", bare)
	}
	if strings.Contains(bare, "verdict:") {
		t.Errorf("empty Verdict must not print a verdict: line: %q", bare)
	}
}

// TestClusterStatusRemoteOfflineMutuallyExclusive (B1 review F7): --remote and --offline cannot be
// combined (a typo must loud-fail, not silently fall through to the offline disk path).
func TestClusterStatusRemoteOfflineMutuallyExclusive(t *testing.T) {
	sp := "/nonexistent.sock"
	cmd := newClusterStatusCmd(&sp)
	cmd.SetArgs([]string{"--offline", "--remote"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected an error when both --offline and --remote are set")
	}
}
