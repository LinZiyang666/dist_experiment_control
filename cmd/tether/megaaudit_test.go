package main

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// megaaudit_test.go — regression pins for the C1–C8 mega-audit MAJOR fixes that live in cmd/tether.

// TestMegaAuditMAJ7ProxyQuorumCodesTransient: C5 proxy quorum-loss codes are self-healing transients
// (exit 75), not unclassified tether bugs (exit 70); ha_policy_invalid is operator usage (64).
func TestMegaAuditMAJ7ProxyQuorumCodesTransient(t *testing.T) {
	for _, code := range []string{"proxy_disabled_no_quorum", "proxy_frozen_readonly"} {
		if got := brokerCodeExitClass(code); got != exitTransient {
			t.Errorf("%s must map to exitTransient(75), got %d", code, got)
		}
	}
	if got := brokerCodeExitClass("ha_policy_invalid"); got != exitUsage {
		t.Errorf("ha_policy_invalid must map to exitUsage(64), got %d", got)
	}
}

// TestMegaAuditMAJ8LeaderElectionTransient: a non-leader bounce with NO LeaderHost is an
// election-in-progress (routine failover) → transient (75), NOT a terminal permission error (77).
func TestMegaAuditMAJ8LeaderElectionTransient(t *testing.T) {
	cmd := newClusterCmd()
	cmd.SetErr(&strings.Builder{})
	// Election in progress: NotLeader + empty LeaderHost.
	err := leaderRedirect(cmd, &adminsock.Response{NotLeader: true, LeaderHost: ""})
	if err == nil {
		t.Fatal("an election bounce must return an error")
	}
	if ee, ok := err.(*ExitError); !ok || ee.Class != exitTransient {
		t.Errorf("election bounce must be exitTransient(75), got %#v", err)
	}
	// Named leader host: terminal-for-this-host (77).
	err = leaderRedirect(cmd, &adminsock.Response{NotLeader: true, LeaderHost: "brk-b:7400"})
	if ee, ok := err.(*ExitError); !ok || ee.Class != exitNoPerm {
		t.Errorf("named-leader bounce must be exitNoPerm(77), got %#v", err)
	}
	// Not a bounce: nil.
	if err := leaderRedirect(cmd, &adminsock.Response{NotLeader: false}); err != nil {
		t.Errorf("a non-bounce must return nil, got %v", err)
	}
}

// TestMegaAuditMAJ5RaiseFailureNonZeroExitAndGuideRaisedFalse: when the persistent NOT-SAFE alert
// CANNOT be raised, `cluster retire --compromised --require-credential-rotation` must exit NON-ZERO
// (rejection #5: never default to "safe restored") and the guide must NOT claim a live alert.
func TestMegaAuditMAJ5RaiseFailureNonZeroExit(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		switch req.Op {
		case adminsock.OpClusterStatus:
			return adminsock.Response{Op: req.Op, OK: true, Cluster: &adminsock.ClusterStatusReport{Health: "HEALTHY_HA"}}
		case adminsock.OpClusterRetire:
			return adminsock.Response{Op: req.Op, OK: true, OpID: "op-1"}
		case adminsock.OpClusterAlertRaise:
			return adminsock.Response{Op: req.Op, Error: "no quorum to raise the alert"} // raise FAILS
		}
		return adminsock.Response{Op: req.Op, OK: true}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterRetireCmd(&sock)
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader("brk-b\n"))
	cmd.SetArgs([]string{"brk-b", "--compromised", "--require-credential-rotation"})
	err := cmd.Execute()
	if err == nil {
		t.Fatalf("a failed alert raise must NOT exit 0 (rejection #5 false-safe):\n%s", out.String())
	}
	// The op WAS created (the node is being retired) but the durable not-safe surface is missing —
	// the output must say so loudly, and the guide must NOT claim a live `alert clear`.
	o := out.String()
	if !strings.Contains(o, "NOT raised") && !strings.Contains(o, "not raised") {
		t.Errorf("output must loudly report the alert was not raised:\n%s", o)
	}
	if strings.Contains(o, "alert clear manual:credrot:brk-b") {
		t.Errorf("the guide must NOT instruct `alert clear` for an alert that was never raised:\n%s", o)
	}
}
