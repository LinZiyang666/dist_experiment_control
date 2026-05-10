package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cli"
)

// TestLogoutClearsExistingSession pins the documented behavior:
// after `tether logout`, the current_session file is empty and a
// follow-up `tether ctx` prints nothing. Reasonably the only
// reason this command exists; if it stops clearing, every CLI
// test that relies on "no active session" returns the wrong sid.
func TestLogoutClearsExistingSession(t *testing.T) {
	home := t.TempDir()
	if err := cli.WriteCurrentSession(home, "lab"); err != nil {
		t.Fatal(err)
	}

	cmd := newLogoutCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--home", home})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("logout: unexpected error: %v", err)
	}

	if got := cli.ReadCurrentSession(home); got != "" {
		t.Errorf("after logout: current_session should be empty, got %q", got)
	}
	if !strings.Contains(out.String(), "current session cleared") {
		t.Errorf("logout should print confirmation; got %q", out.String())
	}
}

// TestLogoutIsIdempotent: calling logout twice (or on a fresh home
// with no current_session at all) MUST NOT error. Operators script
// `tether logout` into shell rc files and expect a no-op when
// already logged out.
func TestLogoutIsIdempotent(t *testing.T) {
	home := t.TempDir()
	cmd := newLogoutCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--home", home})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("logout (no prior session): unexpected error: %v", err)
	}

	out.Reset()
	cmd2 := newLogoutCmd()
	cmd2.SetOut(&out)
	cmd2.SetErr(&out)
	cmd2.SetArgs([]string{"--home", home})
	if err := cmd2.Execute(); err != nil {
		t.Fatalf("logout (second time): unexpected error: %v", err)
	}
}

// TestLogoutRespectsHomeIsolation guards against the kind of
// sloppy bug where logout would walk up to $HOME/.tether and
// clobber the operator's real session because the test forgot
// to pass --home. Two parallel temp homes; clearing one must
// leave the other untouched.
func TestLogoutRespectsHomeIsolation(t *testing.T) {
	homeA := t.TempDir()
	homeB := t.TempDir()
	_ = cli.WriteCurrentSession(homeA, "lab-a")
	_ = cli.WriteCurrentSession(homeB, "lab-b")

	cmd := newLogoutCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"--home", homeA})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}

	if got := cli.ReadCurrentSession(homeA); got != "" {
		t.Errorf("homeA should be cleared; got %q", got)
	}
	if got := cli.ReadCurrentSession(homeB); got != "lab-b" {
		t.Errorf("homeB must NOT be touched; got %q want lab-b", got)
	}
}

// TestCtxPrintsActiveSession is the happy path: ctx prints the
// active sid plus a trailing newline. Scripts pipe `tether ctx`
// into env exports so the trailing-newline contract is part of
// the command's surface.
func TestCtxPrintsActiveSession(t *testing.T) {
	home := t.TempDir()
	if err := cli.WriteCurrentSession(home, "prod"); err != nil {
		t.Fatal(err)
	}

	cmd := newCtxCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--home", home})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("ctx: unexpected error: %v", err)
	}
	if got := out.String(); got != "prod\n" {
		t.Errorf("ctx should print %q; got %q", "prod\n", got)
	}
}

// TestCtxSilentWhenNoActiveSession: with nothing logged in, ctx
// must print nothing AND exit 0. Scripts use `[ -n "$(tether ctx)" ]`
// as the "are we logged in" check; an error or noisy output would
// break that idiom.
func TestCtxSilentWhenNoActiveSession(t *testing.T) {
	home := t.TempDir()
	cmd := newCtxCmd()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"--home", home})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("ctx (no session): unexpected error: %v", err)
	}
	if got := out.String(); got != "" {
		t.Errorf("ctx (no session) should print nothing; got %q", got)
	}
	if got := errOut.String(); got != "" {
		t.Errorf("ctx (no session) stderr should be empty; got %q", got)
	}
}

// TestCtxRespectsHomeIsolation: same isolation invariant as
// logout — ctx --home <X> must read only <X>/current_session.
func TestCtxRespectsHomeIsolation(t *testing.T) {
	homeA := t.TempDir()
	homeB := t.TempDir()
	_ = cli.WriteCurrentSession(homeA, "lab-a")
	// homeB intentionally has no current_session

	cmd := newCtxCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"--home", homeB})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "" {
		t.Errorf("ctx --home homeB should be silent; got %q", got)
	}

	// Sanity: pointing at homeA returns lab-a, proving the read
	// did consult the --home flag, not just any default path.
	out.Reset()
	cmd2 := newCtxCmd()
	cmd2.SetOut(&out)
	cmd2.SetErr(&bytes.Buffer{})
	cmd2.SetArgs([]string{"--home", homeA})
	if err := cmd2.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "lab-a\n" {
		t.Errorf("ctx --home homeA: got %q want %q", got, "lab-a\n")
	}
}

// TestLogoutOnFreshHomeIsNoError: a brand-new ctl install whose
// ~/.tether doesn't yet exist must succeed on `tether logout`
// (Remove of a non-existent file is swallowed). Operators script
// logout into install-time hooks; an ENOENT bubble-up would
// surprise them on freshly-installed boxes.
func TestLogoutOnFreshHomeIsNoError(t *testing.T) {
	parent := t.TempDir()
	home := filepath.Join(parent, "fresh-tether")
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("precondition: %s should not exist; err=%v", home, err)
	}

	cmd := newLogoutCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"--home", home})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("logout (fresh home): unexpected error: %v", err)
	}
	if got := cli.ReadCurrentSession(home); got != "" {
		t.Errorf("fresh home should still report no active session; got %q", got)
	}
}
