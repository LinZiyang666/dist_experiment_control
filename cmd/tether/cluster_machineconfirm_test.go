package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// cluster_machineconfirm_test.go — B6 A4 units (make test): the per-call-site machine-confirm
// escape. The load-bearing assertion is that a CORRECT flag+env STILL refuses on a
// never-escapable call site (the param default is no-escape), so the escape can never leak into
// force-single / recover / F==0-drain / init / restore.

// nonTTY builds a command whose InOrStdin() is the real os.Stdin (a non-TTY under `go test`),
// so the interactive path HARD-REFUSES — isolating the escape as the ONLY way through.
func nonTTY(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.SetErr(&bytes.Buffer{})
	return cmd
}

func TestMachineEscapeAllowedSiteProceeds(t *testing.T) {
	t.Setenv(machineConfirmEnv, "brk-b")
	// allowMachineEscape=true + flag==want + env==want → proceeds without a TTY.
	if !confirmTypedNodeID(nonTTY(t), "brk-b", "", true, "brk-b") {
		t.Fatal("correct flag+env on an escapable site must proceed unattended")
	}
}

func TestMachineEscapeNeverEscapableSiteStillRefuses(t *testing.T) {
	// The load-bearing test: a CORRECT flag+env on a never-escapable site (allowMachineEscape
	// FALSE) must STILL fall through to the TTY refuse. This proves the escape is a per-call-site
	// capability, never a global property of confirmTypedNodeID.
	t.Setenv(machineConfirmEnv, "brk-b")
	if confirmTypedNodeID(nonTTY(t), "brk-b", "", false, "brk-b") {
		t.Fatal("a never-escapable site must refuse even with a correct flag+env")
	}
}

func TestMachineEscapeRequiresBothFlagAndEnv(t *testing.T) {
	t.Run("env only (no flag) refuses", func(t *testing.T) {
		t.Setenv(machineConfirmEnv, "brk-b")
		if confirmTypedNodeID(nonTTY(t), "brk-b", "", true, "") {
			t.Fatal("env alone (a stray exported var) must not be enough")
		}
	})
	t.Run("flag only (no env) refuses", func(t *testing.T) {
		t.Setenv(machineConfirmEnv, "")
		if confirmTypedNodeID(nonTTY(t), "brk-b", "", true, "brk-b") {
			t.Fatal("flag alone (a stray history flag) must not be enough")
		}
	})
	t.Run("wrong env id refuses", func(t *testing.T) {
		t.Setenv(machineConfirmEnv, "brk-OTHER")
		if confirmTypedNodeID(nonTTY(t), "brk-b", "", true, "brk-b") {
			t.Fatal("an env naming a DIFFERENT node must not confirm")
		}
	})
	t.Run("wrong flag id refuses", func(t *testing.T) {
		t.Setenv(machineConfirmEnv, "brk-b")
		if confirmTypedNodeID(nonTTY(t), "brk-b", "", true, "brk-OTHER") {
			t.Fatal("a flag naming a DIFFERENT node must not confirm")
		}
	})
}

// TestRemoveMachineEscapeEndToEnd drives the actual `cluster remove` command unattended: BOTH
// --confirm-node-id and the env equal the node-id, so it passes the confirm gate and proceeds to
// the (failing, no socket) admin call — proving the gate did not block it. Without the env it must
// abort at the confirm gate instead.
func TestRemoveMachineEscapeEndToEnd(t *testing.T) {
	sp := "/nonexistent.sock"
	run := func(args ...string) error {
		cmd := newClusterRemoveCmd(&sp)
		cmd.SetArgs(args)
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		return cmd.Execute()
	}

	t.Run("flag+env passes the confirm gate", func(t *testing.T) {
		t.Setenv(machineConfirmEnv, "brk-b")
		err := run("brk-b", "--confirm-node-id", "brk-b")
		// It gets PAST the confirm gate; the only remaining failure is the admin dial (no socket).
		if err == nil {
			t.Fatal("expected a downstream admin error (no socket), not nil")
		}
		if strings.Contains(err.Error(), "aborted") {
			t.Fatalf("must NOT abort at the confirm gate with flag+env: %v", err)
		}
	})

	t.Run("flag without env aborts at the confirm gate", func(t *testing.T) {
		t.Setenv(machineConfirmEnv, "")
		err := run("brk-b", "--confirm-node-id", "brk-b")
		if err == nil || !strings.Contains(err.Error(), "aborted") {
			t.Fatalf("flag alone (no env) must abort at the confirm gate, got %v", err)
		}
	})
}

// TestRestoreNeverEscapableEndToEnd — Stage-C MAJOR-6: `cluster restore` is in the never-escapable
// set, so even a correct $TETHER_CONFIRM_NODE_ID + --confirm-node-id (the escape that WORKS for
// `cluster remove`) must STILL refuse on a non-TTY — restore is irreversible.
func TestRestoreNeverEscapableEndToEnd(t *testing.T) {
	t.Setenv(machineConfirmEnv, "brk-a")
	cmd := newClusterRestoreCmd()
	cmd.SetArgs([]string{"/tmp/nonexistent-bundle", "--confirm-node-id", "brk-a"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	// Must abort at the typed-confirm (non-TTY refuse) — NEVER proceed to the restore itself.
	if err == nil {
		t.Fatal("restore must refuse on a non-TTY even with a matching flag+env (never-escapable)")
	}
	if !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("restore must abort at the confirm gate, got %v", err)
	}
}
