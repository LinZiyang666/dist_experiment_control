package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// r14_force_single_yes_test.go — #36 (docs/reviews/r6-findings.md): the OFFLINE force-single --yes
// is rejected by the Tier-2 rejector, but the ONLINE arm diverged — it returned before reaching the
// rejector, so `force-single --online --yes` skipped the unattended-forbidden gate. The fix routes
// the online arm through the SAME rejector; the TTY-typed node_id confirm inside is untouched.

// TestOnlineForceSingleRejectsUnattendedYes: the online arm must reject --yes at Tier-2, BEFORE it
// touches the admin socket, with the same usage error (64) the offline path produces.
func TestOnlineForceSingleRejectsUnattendedYes(t *testing.T) {
	cmd := newClusterForceSingleCmd()
	cmd.SetArgs([]string{"--online", "--self-id", "brk-a", "--yes"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("online force-single --yes must error (Tier-2 rejector), got nil")
	}
	var ee *ExitError
	if !errors.As(err, &ee) || ee.Class != exitUsage {
		t.Fatalf("want usage error (64), got %v", err)
	}
	if !strings.Contains(err.Error(), "cannot run unattended") {
		t.Errorf("message should explain no unattended path: %q", err.Error())
	}
}

// TestOnlineForceSingleWithoutYesReachesSocket proves the rejector is gated on --yes only — without
// it, the online arm proceeds to the admin socket (an unreachable socket yields a dial error, NOT
// the Tier-2 "cannot run unattended" message). Guards against the fix over-rejecting online mode.
func TestOnlineForceSingleWithoutYesReachesSocket(t *testing.T) {
	cmd := newClusterForceSingleCmd()
	cmd.SetArgs([]string{"--online", "--self-id", "brk-a"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	// err may be nil in the rare case a live admin socket answers; the only thing we assert is that
	// the Tier-2 rejector did NOT fire without --yes.
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "cannot run unattended") {
		t.Errorf("online without --yes must NOT hit the Tier-2 rejector: %q", err.Error())
	}
}
