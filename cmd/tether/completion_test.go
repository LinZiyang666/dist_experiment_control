package main

// Cobra-level end-to-end tests for dynamic completion. These verify
// that ValidArgsFunction / RegisterFlagCompletionFunc are correctly
// wired into each command and that the `tether __complete ...` hidden
// command (which cobra auto-adds for `completion` script consumption)
// invokes them.
//
// Tests deliberately avoid spinning up a real NATS broker — the
// completion path is engineered so that on a host with no identity /
// no broker URL it returns the noop transport and yields zero
// candidates silently. That is exactly what we want to verify here:
//   * the hook is registered (verb has ValidArgsFunction / flag
//     completion func) and reachable through __complete;
//   * the hook produces NoFileComp (preventing file fallback) instead
//     of hanging or panicking;
//   * flags at various positions (--nats-url before verb, persistent
//     flags on subcommand roots, --broker alias) are honored without
//     errors.

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// runComplete invokes the hidden `__complete` command cobra auto-adds
// for completion-script consumption. The output format is one
// candidate per line, followed by a `:N` directive line and a
// terminating blank line. We return the candidate slice + the
// directive marker string ("Completion ended with directive: ...").
func runComplete(t *testing.T, args ...string) (candidates []string, directiveLine string, stdout, stderr string) {
	t.Helper()
	rootCmd := newRootCmd()
	outBuf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	rootCmd.SetOut(outBuf)
	rootCmd.SetErr(errBuf)
	rootCmd.SetArgs(append([]string{"__complete"}, args...))
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("__complete %v: %v\nstdout: %s\nstderr: %s", args, err, outBuf.String(), errBuf.String())
	}
	stdout = outBuf.String()
	stderr = errBuf.String()
	for _, line := range strings.Split(strings.TrimRight(stdout, "\n"), "\n") {
		if strings.HasPrefix(line, ":") {
			// Numeric directive emitted by cobra's __complete glue.
			directiveLine = line
			continue
		}
		if strings.HasPrefix(line, "Completion ended with directive:") {
			directiveLine = line
			continue
		}
		if line == "" {
			continue
		}
		candidates = append(candidates, line)
	}
	return
}

// freshHomeFlag returns "--home" + a temp dir so __complete sees no
// identity on disk → noop transport → empty candidates + NoFileComp.
func freshHomeFlag(t *testing.T) []string {
	t.Helper()
	home := filepath.Join(t.TempDir(), "no-such-dir-for-tether")
	return []string{"--home", home}
}

// 1. `tether __complete exec ""` on a fresh home returns NoFileComp
// and zero candidates (no identity → silent).
func TestComplete_Exec_NoIdentitySilent(t *testing.T) {
	cands, dir, _, _ := runComplete(t, "exec", "--home", filepath.Join(t.TempDir(), "no-home"), "")
	if len(cands) != 0 {
		t.Errorf("expected zero candidates with no identity, got %v", cands)
	}
	// directive 4 = ShellCompDirectiveNoFileComp (verified against cobra source).
	if !strings.Contains(dir, "directive: ShellCompDirectiveNoFileComp") &&
		!strings.Contains(dir, ":4") {
		t.Errorf("directive mismatch: %q (expected NoFileComp / :4)", dir)
	}
}

// 2. `tether __complete exec <nid> arg` — positional past the first
// returns nothing (remote argv not locally completable).
func TestComplete_Exec_PastFirstPositionalNoCandidates(t *testing.T) {
	home := freshHomeFlag(t)
	cands, dir, _, _ := runComplete(t, append([]string{"exec"}, append(home, "a100", "")...)...)
	if len(cands) != 0 {
		t.Errorf("expected zero candidates for argv position, got %v", cands)
	}
	if !strings.Contains(dir, "directive: ShellCompDirectiveNoFileComp") &&
		!strings.Contains(dir, ":4") {
		t.Errorf("directive mismatch: %q", dir)
	}
}

// 3. expose rm has both positional and --name flag completion.
func TestComplete_ExposeRm_FlagAndPositionalHooked(t *testing.T) {
	home := freshHomeFlag(t)

	// Positional (nid) — no identity → empty silent.
	cands, dir, _, _ := runComplete(t, append([]string{"expose", "rm"}, append(home, "")...)...)
	if len(cands) != 0 {
		t.Errorf("expose rm positional: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("expose rm positional directive: %q", dir)
	}

	// --name flag value. Use attached form so cobra's __complete sees
	// the empty string as the --name value, not a positional after it.
	args := append([]string{"expose", "rm", "a100"}, home...)
	args = append(args, "--name=")
	cands, dir, _, _ = runComplete(t, args...)
	if len(cands) != 0 {
		t.Errorf("expose rm --name: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("expose rm --name directive: %q", dir)
	}
}

// 4. session rm (positional) is owner-filtered.
func TestComplete_SessionRm_OwnerFilteredHookExists(t *testing.T) {
	home := freshHomeFlag(t)
	cands, dir, _, _ := runComplete(t, append([]string{"session", "rm"}, append(home, "")...)...)
	if len(cands) != 0 {
		t.Errorf("session rm: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("session rm directive: %q", dir)
	}
}

// 5. login --session/-s flag value uses VisibleSessions.
func TestComplete_LoginSession_FlagValueHooked(t *testing.T) {
	home := freshHomeFlag(t)
	cands, dir, _, _ := runComplete(t, append([]string{"login"}, append(home, "-s", "")...)...)
	if len(cands) != 0 {
		t.Errorf("login -s: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("login -s directive: %q", dir)
	}
}

// 6. node upgrade positional — hook present.
func TestComplete_NodeUpgrade_PositionalHooked(t *testing.T) {
	home := freshHomeFlag(t)
	cands, dir, _, _ := runComplete(t, append([]string{"node", "upgrade"}, append(home, "")...)...)
	if len(cands) != 0 {
		t.Errorf("node upgrade: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("node upgrade directive: %q", dir)
	}
}

// 7. admin evict 2-positional dispatch.
// First positional → CompleteAdminSessions (broker socket path).
// On a fresh tmpdir with no socket → silent empty + NoFileComp.
func TestComplete_AdminEvict_TwoPositionals(t *testing.T) {
	bogusSocket := filepath.Join(t.TempDir(), "no-such-socket")

	// First positional (sid).
	cands, dir, _, _ := runComplete(t, "admin", "evict", "--socket", bogusSocket, "")
	if len(cands) != 0 {
		t.Errorf("admin evict pos[0]: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("admin evict pos[0] directive: %q", dir)
	}

	// Second positional (nid).
	cands, dir, _, _ = runComplete(t, "admin", "evict", "--socket", bogusSocket, "lab", "")
	if len(cands) != 0 {
		t.Errorf("admin evict pos[1]: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("admin evict pos[1] directive: %q", dir)
	}

	// Third positional → NoFileComp explicit.
	cands, dir, _, _ = runComplete(t, "admin", "evict", "--socket", bogusSocket, "lab", "n1", "")
	if len(cands) != 0 {
		t.Errorf("admin evict pos[2]: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("admin evict pos[2] directive: %q", dir)
	}
}

// 8. admin audit positional → CompleteAdminSessions.
func TestComplete_AdminAudit_PositionalHooked(t *testing.T) {
	bogusSocket := filepath.Join(t.TempDir(), "no-such-socket")
	cands, dir, _, _ := runComplete(t, "admin", "audit", "--socket", bogusSocket, "")
	if len(cands) != 0 {
		t.Errorf("admin audit: got %v, want []", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("admin audit directive: %q", dir)
	}
}

// 9. Flag-position: --nats-url BEFORE the verb still works (cobra global
// flag handling). Verifies flagsAtCompletionTime reads correctly.
func TestComplete_FlagPosition_NatsURLBeforeVerb(t *testing.T) {
	home := freshHomeFlag(t)
	// __complete --nats-url X exec ""
	args := append([]string{"--nats-url", "wss://nowhere.example:443", "exec"}, append(home, "")...)
	cands, dir, _, _ := runComplete(t, args...)
	if len(cands) != 0 {
		t.Errorf("expected empty (no identity), got %v", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("directive: %q", dir)
	}
}

// 10. login --broker (alias of --nats-url) doesn't break hook resolution.
// Use --broker=... attached form so cobra's __complete unambiguously sees
// the final "" as the -s value being completed (not a positional).
func TestComplete_LoginBrokerAlias(t *testing.T) {
	home := freshHomeFlag(t)
	// Order: [home flags...] [--broker=URL] [--session ""]
	args := []string{"login"}
	args = append(args, home...)
	args = append(args, "--broker=wss://nowhere:443", "--session", "")
	cands, dir, _, _ := runComplete(t, args...)
	if len(cands) != 0 {
		t.Errorf("expected empty, got %v", cands)
	}
	if !strings.Contains(dir, ":4") && !strings.Contains(dir, "NoFileComp") {
		t.Errorf("directive: %q", dir)
	}
}
