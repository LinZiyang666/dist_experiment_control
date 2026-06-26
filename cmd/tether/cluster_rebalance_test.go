package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// cluster_rebalance_test.go (C-rebalance) — CLI-side adversarial tests for `cluster rebalance proxy`:
// the dry-run flag is transmitted, the report renders correctly, an already-balanced cluster says so,
// a failed move exits transient (75, not internal/70), and a non-leader bounce redirects.

// TestRebalanceProxyDryRunTransmitsFlagAndRenders: --dry-run sets Request.DryRun and the output reads
// "would move" with each move tagged (dry-run) — never executes anything on the CLI side.
func TestRebalanceProxyDryRunTransmitsFlagAndRenders(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		if req.Op != adminsock.OpClusterRebalanceProxy {
			t.Errorf("unexpected op %q", req.Op)
		}
		if !req.DryRun {
			t.Error("--dry-run must set Request.DryRun=true")
		}
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: &adminsock.ProxyRebalanceReport{
			DryRun: true, Voters: 3, Proxies: 4, Planned: 1, Moves: []adminsock.ProxyRebalanceMove{
				{SID: "s", NID: "n0", Port: 20001, From: "a", To: "c"},
			},
		}}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy", "--dry-run"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("dry-run must not error: %v\n%s", err, out.String())
	}
	o := out.String()
	if !strings.Contains(o, "would move") || !strings.Contains(o, "(dry-run)") {
		t.Errorf("dry-run output must read 'would move' + '(dry-run)':\n%s", o)
	}
	if !strings.Contains(o, "a -> c") {
		t.Errorf("output must render the move a -> c:\n%s", o)
	}
}

// TestRebalanceProxyExecutedRenders: without --dry-run the request carries DryRun=false and the output
// reports the executed moves.
func TestRebalanceProxyExecutedRenders(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		if req.DryRun {
			t.Error("no --dry-run ⇒ Request.DryRun must be false")
		}
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: &adminsock.ProxyRebalanceReport{
			Voters: 2, Proxies: 4, Planned: 1, Moves: []adminsock.ProxyRebalanceMove{
				{SID: "s", NID: "n0", Port: 20001, From: "a", To: "b", Done: true},
			},
		}}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute must not error: %v\n%s", err, out.String())
	}
	if o := out.String(); !strings.Contains(o, "moving") || !strings.Contains(o, "[ok]") {
		t.Errorf("executed output must read 'moving' + '[ok]':\n%s", o)
	}
}

// TestRebalanceProxyAlreadyBalanced: an empty move set renders the friendly "already balanced" line and
// exits 0 (no moves is success, not an error).
func TestRebalanceProxyAlreadyBalanced(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: &adminsock.ProxyRebalanceReport{
			Voters: 3, Proxies: 6, Moves: []adminsock.ProxyRebalanceMove{},
		}}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("balanced must exit 0: %v", err)
	}
	if o := out.String(); !strings.Contains(o, "already balanced") {
		t.Errorf("output must say 'already balanced':\n%s", o)
	}
}

// TestRebalanceProxyFailedMoveIsTransient: a per-move failure (lost-leadership/no-quorum mid-pass) must
// surface as exitTransient(75) — a converge script retries once HA is restored — NOT exitInternal(70).
func TestRebalanceProxyFailedMoveIsTransient(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: &adminsock.ProxyRebalanceReport{
			Voters: 2, Proxies: 4, Planned: 1, Moves: []adminsock.ProxyRebalanceMove{
				{SID: "s", NID: "n0", Port: 20001, From: "a", To: "b", Error: "not the leader"},
			},
		}}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	err := cmd.Execute()
	if err == nil {
		t.Fatalf("a failed move must error:\n%s", out.String())
	}
	ee, ok := err.(*ExitError)
	if !ok || ee.Class != exitTransient {
		t.Errorf("failed move must be exitTransient(75), got %#v", err)
	}
	if o := out.String(); !strings.Contains(o, "FAILED") {
		t.Errorf("output must surface the FAILED move:\n%s", o)
	}
}

// TestRebalanceProxyNotLeaderRedirects: a follower bounce (NotLeader + LeaderHost) redirects (errNonLeader)
// and names the leader — the operator re-runs there.
func TestRebalanceProxyNotLeaderRedirects(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, NotLeader: true, LeaderHost: "brk-a:7400", Code: adminsock.CodeNotLeader}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	if err := cmd.Execute(); err != errNonLeader {
		t.Fatalf("a follower bounce must return errNonLeader, got %v", err)
	}
	if o := out.String(); !strings.Contains(o, "brk-a:7400") {
		t.Errorf("redirect must name the leader host:\n%s", o)
	}
}

// TestRebalanceCmdStructure: `cluster rebalance` exposes exactly the `proxy` subcommand with a --dry-run
// flag (the command surface the proposal names). Guards against an accidental rename/regression.
func TestRebalanceCmdStructure(t *testing.T) {
	sock := "unused"
	root := newClusterRebalanceCmd(&sock)
	if root.Name() != "rebalance" {
		t.Fatalf("root name = %q, want rebalance", root.Name())
	}
	proxy := childByName(root, "proxy")
	if proxy == nil {
		t.Fatal("`cluster rebalance proxy` subcommand missing")
	}
	if proxy.Flags().Lookup("dry-run") == nil {
		t.Error("`cluster rebalance proxy` must have a --dry-run flag")
	}
}

func childByName(parent *cobra.Command, name string) *cobra.Command {
	for _, c := range parent.Commands() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

// TestRebalanceProxyClusterNotEnabled: a single-broker (non-cluster) reply exits exitUsage(64) — running
// rebalance on a broker that isn't in cluster mode is an operator usage error, not a tether fault.
func TestRebalanceProxyClusterNotEnabled(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, Error: "proxy rebalance not available (cluster mode not enabled)", Code: adminsock.CodeClusterNotEnabled}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	err := cmd.Execute()
	ee, ok := err.(*ExitError)
	if !ok || ee.Class != exitUsage {
		t.Fatalf("cluster_not_enabled must be exitUsage(64), got %#v", err)
	}
}

// TestRebalanceProxyElectionInProgressTransient: a NotLeader bounce with NO LeaderHost (election in
// progress) is a routine failover → exitTransient(75), NOT a terminal permission error.
func TestRebalanceProxyElectionInProgressTransient(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, NotLeader: true, LeaderHost: "", Code: adminsock.CodeNotLeader}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	err := cmd.Execute()
	ee, ok := err.(*ExitError)
	if !ok || ee.Class != exitTransient {
		t.Fatalf("election-in-progress bounce must be exitTransient(75), got %#v", err)
	}
}

// TestRebalanceProxyPartialFailureStoppedEarly: a pass that PLANNED 3 but only ATTEMPTED 2 (one ok, one
// FAILED, then leadership lost) must render both the ok + FAILED lines AND a loud "stopped early: N not
// attempted" disclosure, and exit transient — never silently look balanced (R1/R4).
func TestRebalanceProxyPartialFailureStoppedEarly(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: &adminsock.ProxyRebalanceReport{
			Voters: 3, Proxies: 9, Planned: 3, Moves: []adminsock.ProxyRebalanceMove{
				{SID: "s", NID: "n0", Port: 20001, From: "a", To: "b", Done: true},
				{SID: "s", NID: "n1", Port: 20002, From: "a", To: "c", Error: "no longer the leader"},
			}, // 3 planned, only 2 attempted ⇒ 1 not attempted
		}}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	err := cmd.Execute()
	o := out.String()
	if !strings.Contains(o, "[ok]") || !strings.Contains(o, "FAILED") {
		t.Errorf("must render both the ok and FAILED move lines:\n%s", o)
	}
	if !strings.Contains(o, "stopped early") || !strings.Contains(o, "1 planned move") {
		t.Errorf("must loudly disclose the 1 un-attempted planned move:\n%s", o)
	}
	if ee, ok := err.(*ExitError); !ok || ee.Class != exitTransient {
		t.Errorf("a partial/stopped pass must exit transient(75), got %#v", err)
	}
}

// TestRebalanceProxySingleVoter: with <2 eligible voters there is nowhere to spread — say so distinctly
// (not "already balanced: 0 proxies", which understates a real proxy population) and exit 0.
func TestRebalanceProxySingleVoter(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: &adminsock.ProxyRebalanceReport{
			Voters: 1, Proxies: 0, Planned: 0, Moves: []adminsock.ProxyRebalanceMove{},
		}}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRebalanceCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"proxy"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("single-voter must exit 0: %v", err)
	}
	if o := out.String(); !strings.Contains(o, "only 1 eligible voter") {
		t.Errorf("must distinctly report the single-voter case:\n%s", o)
	}
}

// TestRebalanceRegisteredInClusterTree: `cluster rebalance` is actually wired into `tether cluster` under
// the online group (TestRebalanceCmdStructure only checks the standalone constructor).
func TestRebalanceRegisteredInClusterTree(t *testing.T) {
	rebal := childByName(newClusterCmd(), "rebalance")
	if rebal == nil {
		t.Fatal("`cluster rebalance` is not registered under `tether cluster`")
	}
	if rebal.GroupID != "online" {
		t.Errorf("`cluster rebalance` group = %q, want online", rebal.GroupID)
	}
	if strings.TrimSpace(rebal.Example) == "" {
		t.Error("`cluster rebalance` must have an Example (TestClusterSubcommandsHaveExamplesAndGroups)")
	}
}
