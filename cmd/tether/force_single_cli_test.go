package main

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// force_single_cli_test.go — external-review F4 (CLI behavior, complementing the source-grep regression):
// the online force-single CLI transmits the operator-confirmed --self-id + --dry-run to the broker, and
// a real (non-dry-run) online recover REFUSES at the typed confirm on a non-interactive terminal (never
// committing). Both run against the shared stub admin socket harness (startStubAdmin / stubClusterBackend).

// TestOnlineForceSingleCLIDryRunTransmitsSelfIDAndDryRun: `--online --dry-run` sends NodeID + DryRun on
// the arm and NEVER calls commit (a drill needs no TTY confirm).
func TestOnlineForceSingleCLIDryRunTransmitsSelfIDAndDryRun(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		if req.Op == adminsock.OpClusterForceSingleCommit {
			t.Errorf("dry-run must NOT call commit")
		}
		if req.Op != adminsock.OpClusterForceSingleArm {
			t.Errorf("unexpected op %q", req.Op)
		}
		return adminsock.Response{Op: req.Op, OK: true, ForceSingle: &adminsock.ForceSingleReport{
			BrokerSelfID: "brk-a", WouldProceed: false, Reason: "node still has leader contact",
		}}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterForceSingleCmd()
	cmd.Flags().String("socket", sock, "")
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--online", "--dry-run", "--self-id", "brk-a", "--confirm-peers-dead", "brk-b"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("dry-run must not error: %v\n%s", err, out.String())
	}
	arm, ok := b.find(adminsock.OpClusterForceSingleArm)
	if !ok {
		t.Fatal("the arm step never ran")
	}
	if arm.NodeID != "brk-a" {
		t.Fatalf("CLI must transmit --self-id as Request.NodeID for broker validation, got %q", arm.NodeID)
	}
	if !arm.DryRun {
		t.Fatal("CLI must set Request.DryRun on a --dry-run arm")
	}
	if b.count(adminsock.OpClusterForceSingleCommit) != 0 {
		t.Fatal("a dry-run must never reach commit")
	}
}

// TestOnlineForceSingleCLINonTTYRefusesCommit: a real (non-dry-run) online recover on a non-interactive
// terminal must REFUSE at the typed node-id confirm — the arm runs, but commit is never sent.
func TestOnlineForceSingleCLINonTTYRefusesCommit(t *testing.T) {
	b := &stubClusterBackend{resp: func(req adminsock.Request, _ int) adminsock.Response {
		switch req.Op {
		case adminsock.OpClusterForceSingleArm:
			return adminsock.Response{Op: req.Op, OK: true, ForceSingle: &adminsock.ForceSingleReport{
				BrokerSelfID: "brk-a", WouldProceed: true, ArmToken: "tok-123",
			}}
		case adminsock.OpClusterForceSingleCommit:
			return adminsock.Response{Op: req.Op, OK: true}
		}
		return adminsock.Response{Op: req.Op}
	}}
	sock := startStubAdmin(t, b)
	cmd := newClusterForceSingleCmd()
	cmd.Flags().String("socket", sock, "")
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	// No SetIn => cmd.InOrStdin() is os.Stdin, which under `go test` is not a TTY → confirmTypedNodeID
	// HARD-REFUSES (the brain-split-capable op has no unattended path).
	cmd.SetArgs([]string{"--online", "--self-id", "brk-a", "--confirm-peers-dead", "brk-b"})
	if err := cmd.Execute(); err == nil {
		t.Fatalf("a non-TTY real online force-single must refuse at the typed confirm; out=%s", out.String())
	}
	if b.count(adminsock.OpClusterForceSingleArm) == 0 {
		t.Fatal("the arm step should have run before the confirm")
	}
	if b.count(adminsock.OpClusterForceSingleCommit) != 0 {
		t.Fatal("commit must NOT run without a successful TTY confirm")
	}
}
