package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// Every command that operates "in the active session" reads the
// sid from cli.ReadCurrentSession(home) BEFORE it ever talks to
// NATS, and short-circuits with the same error string if there
// is no active session. This file pins that contract so a future
// refactor that moves the check inside the NATS path (and thus
// blocks on a connect attempt instead of failing fast) breaks
// loudly.
//
// The check is the FIRST guard in each RunE — so we can drive
// every command by giving it --home <fresh-tempdir> (which has
// no current_session file). No NATS server is required because
// the command never gets that far.

func runWithFreshHome(t *testing.T, cmd *cobra.Command, args ...string) error {
	t.Helper()
	home := t.TempDir()
	full := append([]string{"--home", home}, args...)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(full)
	return cmd.Execute()
}

// TestPsRequiresActiveSession: bare `tether ps` against a fresh
// home returns the "no active session — run `tether login -s
// <sid>` first" error. Otherwise scripting `tether ps || tether
// login -s lab` would block on a NATS dial that takes seconds
// to time out.
func TestPsRequiresActiveSession(t *testing.T) {
	err := runWithFreshHome(t, newPsCmd())
	mustNoActiveSession(t, "ps", err)
}

// TestExecRequiresActiveSession: same contract. We pass a
// dummy nid + argv so cobra's positional-args validation
// passes and we reach the active-session guard.
func TestExecRequiresActiveSession(t *testing.T) {
	err := runWithFreshHome(t, newExecCmd(), "lab-1", "echo", "hi")
	mustNoActiveSession(t, "exec", err)
}

// TestRunRequiresActiveSession: same.
func TestRunRequiresActiveSession(t *testing.T) {
	err := runWithFreshHome(t, newRunCmd(), "lab-1", "bash")
	mustNoActiveSession(t, "run", err)
}

// TestExposeRequiresActiveSession: same. --local + --name make
// it past the cobra-level required-flag check (we don't care
// about validation here, just want the FIRST guard to fire).
func TestExposeRequiresActiveSession(t *testing.T) {
	err := runWithFreshHome(t, newExposeCmd(), "lab-1", "--local", "8888", "--name", "j")
	mustNoActiveSession(t, "expose", err)
}

// TestExposeRmRequiresActiveSession: expose's rm subcommand has
// its own RunE; pin it explicitly. Use the parent expose cmd's
// AddCommand wiring so subcommand routing is exercised too.
func TestExposeRmRequiresActiveSession(t *testing.T) {
	parent := newExposeCmd() // already has expose-rm attached as a child
	home := t.TempDir()
	parent.SetOut(&bytes.Buffer{})
	parent.SetErr(&bytes.Buffer{})
	parent.SetArgs([]string{"rm", "lab-1", "--name", "j", "--home", home})
	err := parent.Execute()
	mustNoActiveSession(t, "expose rm", err)
}

// TestHistoryRequiresActiveSession: history reads sid from
// current_session even before opening JetStream. Pin the
// fail-fast.
func TestHistoryRequiresActiveSession(t *testing.T) {
	err := runWithFreshHome(t, newHistoryCmd())
	mustNoActiveSession(t, "history", err)
}

// TestNodeUpgradeRequiresActiveSession: upgrade --url + --sha256
// pass cobra validation; the active-session guard must still
// fire before we ever try to dial NATS.
func TestNodeUpgradeRequiresActiveSession(t *testing.T) {
	parent := newNodeCmd() // attaches upgrade as a child
	home := t.TempDir()
	parent.SetOut(&bytes.Buffer{})
	parent.SetErr(&bytes.Buffer{})
	parent.SetArgs([]string{
		"upgrade", "lab-1",
		"--url", "https://x/",
		"--sha256", "deadbeef",
		"--home", home,
	})
	err := parent.Execute()
	mustNoActiveSession(t, "upgrade", err)
}

func TestNodeLsRequiresActiveSession(t *testing.T) {
	parent := newNodeCmd()
	home := t.TempDir()
	parent.SetOut(&bytes.Buffer{})
	parent.SetErr(&bytes.Buffer{})
	parent.SetArgs([]string{"ls", "--home", home})
	err := parent.Execute()
	mustNoActiveSession(t, "node ls", err)
}

func TestAlertLsRequiresActiveSession(t *testing.T) {
	err := runWithFreshHome(t, newAlertLsCmd())
	mustNoActiveSession(t, "alert ls", err)
}

func TestAlertAckRequiresActiveSession(t *testing.T) {
	// Use a STORE-BACKED key, not a synthetic gate name — B3 item 4 short-circuits
	// quorum_lost/force_single_active BEFORE the session check, so they no longer reach this path.
	err := runWithFreshHome(t, newAlertAckCmd(), "disk_pressure:n1")
	mustNoActiveSession(t, "alert ack", err)
}

func mustNoActiveSession(t *testing.T, verb string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s without active session: expected error, got nil", verb)
	}
	// The exact wording is part of the user-visible contract
	// (documented in docs/usage.md §9). If you change it, update
	// the doc + this assertion together.
	want := "no active session"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("%s: error should mention %q; got %q", verb, want, err.Error())
	}
	if !strings.Contains(err.Error(), "tether login") {
		t.Errorf("%s: error should suggest `tether login`; got %q", verb, err.Error())
	}
}
