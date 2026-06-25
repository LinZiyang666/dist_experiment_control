package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestTier2RejectsUnattendedYes (B3 item 4): the irreversible / quorum-affecting commands reject
// --yes with a helpful usage error (exit 64), BEFORE reaching the typed-confirm prompt.
func TestTier2RejectsUnattendedYes(t *testing.T) {
	sp := "/nonexistent.sock"
	cases := []struct {
		name string
		cmd  *cobra.Command
		args []string
	}{
		{"remove", newClusterRemoveCmd(&sp), []string{"brk-x", "--yes"}},
		{"force-single", newClusterForceSingleCmd(), []string{"--yes"}},
		{"recover", newClusterRecoverCmd(), []string{"--yes"}},
		{"init", newClusterInitCmd(), []string{"--yes"}},
		{"restore", newClusterRestoreCmd(), []string{"/tmp/b", "--confirm-node-id", "brk-a", "--yes"}}, // B6: restore is Tier-2
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.cmd.SetArgs(c.args)
			c.cmd.SetOut(&bytes.Buffer{})
			c.cmd.SetErr(&bytes.Buffer{})
			err := c.cmd.Execute()
			if err == nil {
				t.Fatalf("%s --yes must error", c.name)
			}
			var ee *ExitError
			if !errors.As(err, &ee) || ee.Class != exitUsage {
				t.Errorf("%s --yes: want usage error (64), got %v", c.name, err)
			}
			if !strings.Contains(err.Error(), "cannot run unattended") {
				t.Errorf("%s --yes: message should explain no unattended path: %q", c.name, err.Error())
			}
		})
	}
}

// TestDrainRetireHasNoYesRejector (B3 review m3): plain drain is Tier-1-ish and registers NO --yes
// flag, so `drain --retire --yes` is cobra's bare "unknown flag" (NOT the helpful rejector message
// — that would contradict the plan's two-tier decision).
func TestDrainRetireHasNoYesRejector(t *testing.T) {
	sp := "/nonexistent.sock"
	cmd := newClusterDrainCmd(&sp)
	cmd.SetArgs([]string{"brk-b", "--retire", "--yes"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("drain --retire --yes must be an unknown-flag error, got %v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "cannot run unattended") {
		t.Error("drain must NOT carry the Tier-2 --yes rejector")
	}
}

// TestConfirmTypedNodeIDNonTTYRefuses (B3 review n1): with stdin (the production path; under
// `go test` stdin is not a TTY) confirmTypedNodeID HARD-REFUSES — the most dangerous gate's
// safety must not silently pass unattended.
func TestConfirmTypedNodeIDNonTTYRefuses(t *testing.T) {
	cmd := &cobra.Command{} // no SetIn → InOrStdin() == os.Stdin → TTY check fires
	var errBuf bytes.Buffer
	cmd.SetErr(&errBuf)
	if confirmTypedNodeID(cmd, "brk-a", "", false, "") {
		t.Error("a non-interactive stdin must refuse (no unattended destructive op)")
	}
	if !strings.Contains(errBuf.String(), "interactive terminal") {
		t.Errorf("refusal message should mention an interactive terminal: %q", errBuf.String())
	}
}

// TestConfirmTypedNodeID (B3 item 4): the refactored confirm reads cmd.InOrStdin(), so the positive
// path is unit-testable; a wrong id returns false.
func TestConfirmTypedNodeID(t *testing.T) {
	mk := func(in string) *cobra.Command {
		cmd := &cobra.Command{}
		cmd.SetIn(strings.NewReader(in))
		cmd.SetErr(&bytes.Buffer{})
		return cmd
	}
	if !confirmTypedNodeID(mk("brk-a\n"), "brk-a", "", false, "") {
		t.Error("matching typed id must confirm")
	}
	if confirmTypedNodeID(mk("wrong\n"), "brk-a", "", false, "") {
		t.Error("a wrong id must NOT confirm")
	}
	// consequence is printed before the prompt.
	var errBuf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetIn(strings.NewReader("brk-a\n"))
	cmd.SetErr(&errBuf)
	confirmTypedNodeID(cmd, "brk-a", "CONSEQUENCE: danger", false, "")
	if !strings.Contains(errBuf.String(), "CONSEQUENCE: danger") {
		t.Errorf("consequence must be printed: %q", errBuf.String())
	}
}

// TestAlertAckSyntheticGuard (B3 item 4): `alert ack quorum_lost`/`force_single_active` is
// explained + refused BEFORE any session read / NATS dial (so it works on a partitioned box).
func TestAlertAckSyntheticGuard(t *testing.T) {
	for _, key := range []string{"quorum_lost", "force_single_active"} {
		cmd := newAlertAckCmd()
		cmd.SetArgs([]string{key})
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "LIVE cluster-health condition") {
			t.Errorf("%s: want synthetic-gate explanation, got %v", key, err)
		}
		var ee *ExitError
		if !errors.As(err, &ee) || ee.Class != exitUsage {
			t.Errorf("%s: want usage error (64), got %v", key, err)
		}
	}
}
