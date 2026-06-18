package main

// CLI / UX surface tests for the tether cobra entrypoint.
//
// Coverage targets (per the CLI/UX test brief, 12 dimensions):
//   1. --help on every subcommand and subcommand-group leaf.
//   2. Unknown --flag returns an error.
//   3. Unknown subcommand under each subcommand group.
//   4. Required-flag contracts (--pin, --name, --local, --url + --sha256,
//      mutual exclusion of --all + <nid>, missing target).
//   5. Numeric edge cases for --local / --timeout / --lines.
//   6. String edge cases for sid / name (empty, long, unicode, shell metachars).
//   7. stdout/stderr separation contract.
//   8. --kind enum validation (history).
//   9. SetInterspersed(false) for exec/run.
//  10. TETHER_SESSION env wins over current_session file.
//  11. Empty-string flag values vs unset (--session "").
//  12. Short-flag aliases (-a, -s, -n, -f).
//
// All tests drive `newRootCmd()` end-to-end so they exercise the same
// silence-errors / silence-usage settings real users see; they pin the
// command tree to a fresh `t.TempDir()` home and (where it would
// otherwise dial NATS) to `nats://127.0.0.1:1` so connect failures are
// instantaneous (~1ms refused) instead of waiting for the default
// 2s connect timeout. NO real broker / NATS server is started.

import (
	"bytes"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/spf13/cobra"
)

// fastBrokerURL is a port no broker will ever bind. ConnectNATS errors
// out within milliseconds with "no servers available", instead of
// timing out, keeping the unit test suite fast even when the active-
// session guard has been satisfied and we want to exercise downstream
// CLI parsing without standing up a NATS server.
const fastBrokerURL = "nats://127.0.0.1:1"

// runRoot constructs a fresh root command, wires capture buffers, and
// runs args. Returns (stdout, stderr, err). The root carries
// SilenceErrors=true / SilenceUsage=true (see main.go) which matches
// what real users see; the helper preserves that.
func runRoot(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)
	err = root.Execute()
	return out.String(), errOut.String(), err
}

// ---------------------------------------------------------------------
// Dimension 1: --help on every command + subcommand.
// ---------------------------------------------------------------------

// TestHelpExitsZeroWithUsage walks every cobra command in the tree and
// asserts `... --help` succeeds AND prints the canonical Usage block
// to stdout. Cobra's --help intercept runs BEFORE RunE, so we can
// exercise this without any active session, NATS, or broker.
//
// The Use field of each command appears in the Usage block ("Usage:\n
// tether <cmd> ..."), so we check both "Usage:" and the leaf token.
func TestHelpExitsZeroWithUsage(t *testing.T) {
	cases := []struct {
		name string
		args []string
		// useToken is the leaf word of the command's Use field that
		// the help output's Usage line MUST contain (verifies we
		// reached the right command, not a sibling).
		useToken string
	}{
		{"root", []string{"--help"}, "tether"},
		{"version", []string{"version", "--help"}, "version"},
		{"serve", []string{"serve", "--help"}, "serve"},
		{"agent", []string{"agent", "--help"}, "agent"},
		{"login", []string{"login", "--help"}, "login"},
		{"logout", []string{"logout", "--help"}, "logout"},
		{"ctx", []string{"ctx", "--help"}, "ctx"},
		{"session", []string{"session", "--help"}, "session"},
		{"session create", []string{"session", "create", "--help"}, "create"},
		{"session ls", []string{"session", "ls", "--help"}, "ls"},
		{"session rm", []string{"session", "rm", "--help"}, "rm"},
		{"ps", []string{"ps", "--help"}, "ps"},
		{"exec", []string{"exec", "--help"}, "exec"},
		{"run", []string{"run", "--help"}, "run"},
		{"expose", []string{"expose", "--help"}, "expose"},
		{"expose rm", []string{"expose", "rm", "--help"}, "rm"},
		{"history", []string{"history", "--help"}, "history"},
		{"node", []string{"node", "--help"}, "node"},
		{"node upgrade", []string{"node", "upgrade", "--help"}, "upgrade"},
		{"admin", []string{"admin", "--help"}, "admin"},
		{"admin sessions", []string{"admin", "sessions", "--help"}, "sessions"},
		{"admin nodes", []string{"admin", "nodes", "--help"}, "nodes"},
		{"admin audit", []string{"admin", "audit", "--help"}, "audit"},
		{"admin evict", []string{"admin", "evict", "--help"}, "evict"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, err := runRoot(t, tc.args...)
			if err != nil {
				t.Fatalf("--help should exit 0; got err=%v stderr=%q", err, errOut)
			}
			if !strings.Contains(out, "Usage:") {
				t.Errorf("--help stdout missing 'Usage:' block:\n%s", out)
			}
			if !strings.Contains(out, tc.useToken) {
				t.Errorf("--help stdout missing token %q:\n%s", tc.useToken, out)
			}
			// Help must not write anything to stderr (cobra's
			// --help is a "successful" path; errOut should stay
			// empty even with SilenceErrors=true).
			if errOut != "" {
				t.Errorf("--help stderr should be empty; got %q", errOut)
			}
		})
	}
}

// TestShortHelpFlagAlsoWorks confirms cobra's built-in -h short form
// is wired (not shadowed by another short). Three representative
// commands; the rest inherit the same registry.
func TestShortHelpFlagAlsoWorks(t *testing.T) {
	for _, leaf := range []string{"version", "ctx", "logout"} {
		t.Run(leaf, func(t *testing.T) {
			out, _, err := runRoot(t, leaf, "-h")
			if err != nil {
				t.Fatalf("-h should exit 0; got %v", err)
			}
			if !strings.Contains(out, "Usage:") {
				t.Errorf("-h stdout missing Usage block:\n%s", out)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 2: unknown flags.
// ---------------------------------------------------------------------

// TestUnknownFlagReturnsError walks every leaf command and asserts a
// nonsense flag is rejected by pflag. The exact wording ("unknown
// flag: ...") is pflag's contract — if it changes upstream we'll
// notice. With SilenceErrors=true, the error is RETURNED but not
// re-printed to stderr by cobra, which is the user-visible contract.
func TestUnknownFlagReturnsError(t *testing.T) {
	bogus := "--this-flag-does-not-exist"
	leafCommands := [][]string{
		{"version"},
		{"serve"},
		{"agent"},
		{"login"},
		{"logout"},
		{"ctx"},
		{"session", "create", "x"},
		{"session", "ls"},
		{"session", "rm", "x"},
		{"ps"},
		{"exec", "lab-1", "echo", "hi"},
		{"run", "lab-1", "bash"},
		{"expose", "lab-1"},
		{"expose", "rm", "lab-1"},
		{"history"},
		{"node", "upgrade", "lab-1"},
		{"admin", "sessions"},
		{"admin", "nodes"},
		{"admin", "audit", "sid"},
		{"admin", "evict", "sid", "nid"},
	}
	for _, args := range leafCommands {
		name := strings.Join(args, " ")
		t.Run(name, func(t *testing.T) {
			// Put the bogus flag FIRST. exec/run set
			// SetInterspersed(false) and would treat any flag
			// after the first positional as an argv token.
			full := append([]string{args[0], bogus}, args[1:]...)
			_, _, err := runRoot(t, full...)
			if err == nil {
				t.Fatalf("expected error for unknown flag in %q; got nil", name)
			}
			if !strings.Contains(err.Error(), "unknown flag") {
				t.Errorf("error should mention 'unknown flag'; got %q", err.Error())
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 3: unknown subcommand under groups.
//
// REAL UX BUG documented inline: cobra's default for a group command
// (one with subcommands but no Args validator) is "no error, dump
// help to stdout, exit 0". So `tether session foo` SILENTLY shows
// help and exits 0 — scripts that pipe `tether session $arg || ...`
// can't detect a typo. The tests below pin the CURRENT behavior so
// the suite is green, but flag the gap for the bug-report (final
// step). A future fix would set Args:cobra.NoArgs or add a custom
// PersistentPreRun to reject unknown positionals on groups.
// ---------------------------------------------------------------------

// TestUnknownSubcommandUnderGroup documents the actual cobra default:
// unknown sub of a group exits 0 and dumps help to stdout. If the
// project later adds Args validation to fix the UX gap, this test
// (and its sibling assertion direction) needs flipping.
func TestUnknownSubcommandUnderGroup(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"session foo", []string{"session", "foo"}},
		{"admin bar", []string{"admin", "bar"}},
		{"node baz", []string{"node", "baz"}},
		{"expose rm has subcommand-less path so n/a", nil}, // skipped below
	}
	for _, tc := range cases {
		if tc.args == nil {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			out, _, err := runRoot(t, tc.args...)
			// CURRENT cobra default behavior: nil error +
			// help on stdout. Documented as a UX gap.
			if err != nil {
				t.Fatalf("CURRENT contract: cobra returns nil for unknown sub of a group; got %v",
					err)
			}
			if !strings.Contains(out, "Available Commands") &&
				!strings.Contains(out, "Usage:") {
				t.Errorf("expected help dump on stdout for unknown sub; got %q", out)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 4: required-flag contracts.
// ---------------------------------------------------------------------

func TestRequiredFlagContracts(t *testing.T) {
	cases := []struct {
		name        string
		args        []string
		wantSubstr  string
		wantNonZero bool // err must be non-nil
	}{
		{
			name:        "session create no --pin",
			args:        []string{"session", "create", "labname", "--nats-url", fastBrokerURL},
			wantSubstr:  "--pin is required",
			wantNonZero: true,
		},
		{
			name: "session create empty --pin",
			args: []string{
				"session", "create", "labname",
				"--pin", "", "--nats-url", fastBrokerURL,
			},
			wantSubstr:  "--pin is required",
			wantNonZero: true,
		},
		{
			name:        "expose missing --name",
			args:        []string{"expose", "--local", "8888", "lab-1"},
			wantSubstr:  "--name is required",
			wantNonZero: true,
		},
		{
			name:        "expose empty --name",
			args:        []string{"expose", "--local", "8888", "--name", "", "lab-1"},
			wantSubstr:  "--name is required",
			wantNonZero: true,
		},
		{
			name:        "expose missing --local (defaults to 0)",
			args:        []string{"expose", "--name", "x", "lab-1"},
			wantSubstr:  "--local must be 1..65535",
			wantNonZero: true,
		},
		{
			name:        "node upgrade missing --url and --sha256",
			args:        []string{"node", "upgrade", "lab-1"},
			wantSubstr:  "--url and --sha256 are required",
			wantNonZero: true,
		},
		{
			name: "node upgrade missing --sha256 only",
			args: []string{
				"node", "upgrade", "lab-1",
				"--url", "https://x",
			},
			wantSubstr:  "--url and --sha256 are required",
			wantNonZero: true,
		},
		{
			name: "node upgrade --all + <nid> mutually exclusive",
			args: []string{
				"node", "upgrade", "--all", "lab-1",
				"--url", "https://x", "--sha256", "deadbeef",
			},
			wantSubstr:  "mutually exclusive",
			wantNonZero: true,
		},
		{
			name: "node upgrade neither --all nor <nid>",
			args: []string{
				"node", "upgrade",
				"--url", "https://x", "--sha256", "deadbeef",
			},
			wantSubstr:  "either <nid> or --all is required",
			wantNonZero: true,
		},
		{
			name:        "agent --session is required",
			args:        []string{"agent"},
			wantSubstr:  "--session is required",
			wantNonZero: true,
		},
		{
			name:        "agent --session empty == missing",
			args:        []string{"agent", "--session", ""},
			wantSubstr:  "--session is required",
			wantNonZero: true,
		},
	}
	// Every case needs an active session for the upgrade / expose
	// pre-checks to RUN before short-circuiting on "no active
	// session". The required-flag check ordering varies command by
	// command — node upgrade checks --url BEFORE the active-session
	// guard, expose checks --name AFTER it. To keep the table flat,
	// we set TETHER_SESSION for every case; commands that don't
	// read it are unaffected.
	t.Setenv("TETHER_SESSION", "labx")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			args := append([]string{}, tc.args...)
			// inject --home where the command supports it (every
			// command above does); positional comes after, so prepend.
			args = injectHomeFlag(args, home)
			_, _, err := runRoot(t, args...)
			if tc.wantNonZero && err == nil {
				t.Fatalf("expected non-nil error mentioning %q; got nil", tc.wantSubstr)
			}
			if err != nil && !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("error should contain %q; got %q",
					tc.wantSubstr, err.Error())
			}
		})
	}
}

// injectHomeFlag inserts "--home <dir>" right after the leaf command
// path so it's parsed before the first positional even when the
// command sets SetInterspersed(false). For a verb chain like
// {"session", "create", "name"} we insert at position 2; for a single
// verb {"agent"} we insert at position 1; for groups + verb like
// {"node", "upgrade", "lab-1"} the first positional is at index 2,
// so we insert at 2.
//
// Special case: `tether agent` does NOT register a --home flag (it
// always uses cli.DefaultHome internally; agent.go line ~78). For
// that command, return args unchanged so callers don't accidentally
// trigger an "unknown flag: --home" error.
func injectHomeFlag(args []string, home string) []string {
	if len(args) > 0 && args[0] == "agent" {
		return args
	}
	insertAt := commandPathLen(args)
	out := make([]string, 0, len(args)+2)
	out = append(out, args[:insertAt]...)
	out = append(out, "--home", home)
	out = append(out, args[insertAt:]...)
	return out
}

// commandPathLen returns the prefix length covering the cobra command
// path (no flags, no positionals). It walks until the first arg
// starting with "--" or "-", or the first arg that's a positional.
// Heuristic: tether's command paths are always single tokens that
// match a known set; we hardcode the depth per first verb to avoid
// false positives.
func commandPathLen(args []string) int {
	if len(args) == 0 {
		return 0
	}
	switch args[0] {
	case "session", "expose", "node", "admin":
		// {group, sub, ...}; if args[1] doesn't look like a flag,
		// treat it as part of the path.
		if len(args) >= 2 && !strings.HasPrefix(args[1], "-") {
			return 2
		}
		return 1
	default:
		return 1
	}
}

// ---------------------------------------------------------------------
// Dimension 5: numeric edge cases.
// ---------------------------------------------------------------------

func TestExposeLocalNumericBounds(t *testing.T) {
	t.Setenv("TETHER_SESSION", "lab")
	cases := []struct {
		port string
		// All four values must be REJECTED by the guard in expose.go.
	}{
		{"0"}, {"-1"}, {"65536"}, {"99999"},
	}
	for _, tc := range cases {
		t.Run("local="+tc.port, func(t *testing.T) {
			home := t.TempDir()
			_, _, err := runRoot(t, "expose",
				"--home", home, "--local", tc.port, "--name", "x",
				"--nats-url", fastBrokerURL, "lab-1")
			if err == nil {
				t.Fatalf("expected error for --local %s; got nil", tc.port)
			}
			if !strings.Contains(err.Error(), "--local must be 1..65535") {
				t.Errorf("error should mention bounds message; got %q", err.Error())
			}
		})
	}
}

// TestNodeUpgradeTimeoutZeroParses ensures --timeout 0 is at least
// PARSED (no panic, no parse error). The downstream NATS request
// fails because we point at a refused port — we don't assert the
// failure message, only that we got past flag parsing.
func TestNodeUpgradeTimeoutZeroParses(t *testing.T) {
	t.Setenv("TETHER_SESSION", "lab")
	home := t.TempDir()
	_, _, err := runRoot(t, "node", "upgrade",
		"--home", home, "--url", "https://x", "--sha256", "deadbeef",
		"--timeout", "0", "--nats-url", fastBrokerURL, "lab-1")
	if err == nil {
		// We expected a downstream NATS error; nil here means the
		// command somehow succeeded against a refused port.
		t.Fatal("expected NATS-side error after parse; got nil")
	}
	if strings.Contains(err.Error(), "invalid argument") ||
		strings.Contains(err.Error(), "unknown flag") {
		t.Errorf("--timeout 0 should parse cleanly; got %q", err.Error())
	}
}

// TestHistoryLinesBoundsParse: -n -5 / -n 0 / -n very-large all parse
// without panic. Negative is silently mapped to 0 by the IntVarP +
// downstream `if lastN > 0` check, so behavior is "show all from
// oldest". The contract here is "no panic, no parse error" — we
// don't assert the resulting query shape (covered by history_paths_test.go).
func TestHistoryLinesBoundsParse(t *testing.T) {
	t.Setenv("TETHER_SESSION", "lab")
	for _, n := range []string{"-5", "0", "9999999"} {
		t.Run("n="+n, func(t *testing.T) {
			home := t.TempDir()
			_, _, err := runRoot(t, "history",
				"--home", home, "-n", n,
				"--nats-url", fastBrokerURL)
			// Parse must succeed — only NATS dial should fail.
			if err == nil {
				t.Fatalf("expected NATS-side error after parse; got nil")
			}
			if strings.Contains(err.Error(), "invalid argument") ||
				strings.Contains(err.Error(), "unknown flag") {
				t.Errorf("-n %s should parse cleanly; got %q", n, err.Error())
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 6: string edge cases.
// ---------------------------------------------------------------------

// TestLoginAcceptsExoticSidStrings: the CLI layer must NOT panic, NOT
// truncate, NOT reject on its own — those decisions belong to the
// broker (which checks PIN + membership server-side). An overly
// strict CLI-side regex would lock out legitimate sids in non-ASCII
// locales (实验室 is a real lab name).
func TestLoginAcceptsExoticSidStrings(t *testing.T) {
	cases := []string{
		strings.Repeat("a", 512),
		"../etc/passwd",
		"with spaces",
		"实验室",
		"foo;rm -rf /",
	}
	for _, sid := range cases {
		t.Run(sid, func(t *testing.T) {
			home := t.TempDir()
			_, _, err := runRoot(t, "login",
				"--home", home, "-s", sid,
				"--nats-url", fastBrokerURL)
			// We expect a NATS connect error (broker refused) or
			// "activation refused" — what we MUST NOT see is a
			// panic, an "unknown flag", or a CLI-side rejection.
			if err == nil {
				t.Fatal("expected NATS-side error; got nil")
			}
			es := err.Error()
			if strings.Contains(es, "panic") ||
				strings.Contains(es, "unknown flag") ||
				strings.Contains(es, "invalid argument") {
				t.Errorf("CLI must accept arbitrary sid bytes; got %q", es)
			}
			// Sanity: the error path should mention either NATS
			// or activation, confirming we made it past parse.
			if !strings.Contains(es, "no servers") &&
				!strings.Contains(es, "activation") &&
				!strings.Contains(es, "login:") {
				t.Errorf("expected NATS / activation error; got %q", es)
			}
		})
	}
}

// TestExposeAcceptsExoticNameStrings: same intent for --name. Names
// with shell metachars are someone else's problem (the broker's, on
// the wire); the CLI parser can't know whether downstream code shells
// out. CLI must just plumb the bytes through.
func TestExposeAcceptsExoticNameStrings(t *testing.T) {
	t.Setenv("TETHER_SESSION", "lab")
	cases := []string{"foo bar", "foo;rm -rf /", "n=ame:weird", "实验"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			_, _, err := runRoot(t, "expose",
				"--home", home, "--local", "8888",
				"--name", name, "--nats-url", fastBrokerURL,
				"lab-1")
			if err == nil {
				t.Fatal("expected NATS-side error; got nil")
			}
			es := err.Error()
			if strings.Contains(es, "panic") ||
				strings.Contains(es, "--name is required") {
				t.Errorf("CLI rejected exotic --name (%q); got %q", name, es)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 7: stdout vs stderr separation.
//
// With root.SilenceErrors=true (main.go), cobra does NOT echo the
// returned error to stderr — `main()` does that one level up. The
// test contract for v1 therefore is:
//
//   - successful commands: write user-facing output to stdout, NOT stderr
//   - failed commands:     return a non-nil error AND keep stderr empty
//                          inside cobra (the parent main() prints it)
//
// We exercise both happy and sad paths.
// ---------------------------------------------------------------------

func TestStdoutStderrSeparation(t *testing.T) {
	t.Run("logout success → stdout has confirmation, stderr empty", func(t *testing.T) {
		home := t.TempDir()
		// Pre-populate so the message "current session cleared"
		// is informative.
		if err := cli.WriteCurrentSession(home, "lab"); err != nil {
			t.Fatal(err)
		}
		out, errOut, err := runRoot(t, "logout", "--home", home)
		if err != nil {
			t.Fatalf("logout should succeed; got %v", err)
		}
		if !strings.Contains(out, "current session cleared") {
			t.Errorf("logout: confirmation should be on stdout; got stdout=%q", out)
		}
		if errOut != "" {
			t.Errorf("logout: stderr must stay empty on success; got %q", errOut)
		}
	})

	t.Run("ps no-active-session → err returned, stdout empty, stderr empty", func(t *testing.T) {
		home := t.TempDir()
		// Defensive: make sure no env-level session pollutes the test.
		t.Setenv("TETHER_SESSION", "")
		out, errOut, err := runRoot(t, "ps", "--home", home)
		if err == nil {
			t.Fatal("expected no-active-session error")
		}
		if !strings.Contains(err.Error(), "no active session") {
			t.Errorf("error must mention 'no active session'; got %q",
				err.Error())
		}
		if out != "" {
			t.Errorf("error path must NOT write to stdout; got %q", out)
		}
		if errOut != "" {
			t.Errorf("error path must NOT write to stderr (root has SilenceErrors); got %q",
				errOut)
		}
	})

	t.Run("expose --local 0 → err returned, no output channel pollution", func(t *testing.T) {
		t.Setenv("TETHER_SESSION", "lab")
		home := t.TempDir()
		out, errOut, err := runRoot(t, "expose",
			"--home", home, "--local", "0", "--name", "x",
			"--nats-url", fastBrokerURL, "lab-1")
		if err == nil {
			t.Fatal("expected --local bounds error")
		}
		if !strings.Contains(err.Error(), "--local must be 1..65535") {
			t.Errorf("error should mention bounds; got %q", err.Error())
		}
		if out != "" {
			t.Errorf("validation error must NOT print to stdout; got %q", out)
		}
		if errOut != "" {
			t.Errorf("validation error must NOT print to stderr (silenced); got %q",
				errOut)
		}
	})

	t.Run("ctx active → exact 'sid\\n' on stdout, nothing on stderr", func(t *testing.T) {
		home := t.TempDir()
		if err := cli.WriteCurrentSession(home, "lab"); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TETHER_SESSION", "")
		out, errOut, err := runRoot(t, "ctx", "--home", home)
		if err != nil {
			t.Fatalf("ctx should succeed; got %v", err)
		}
		if out != "lab\n" {
			t.Errorf("ctx stdout must be exact 'lab\\n'; got %q", out)
		}
		if errOut != "" {
			t.Errorf("ctx stderr must be empty; got %q", errOut)
		}
	})
}

// ---------------------------------------------------------------------
// Dimension 8: --kind enum validation (history).
//
// The order in history.go is: ReadCurrentSession FIRST, then --kind
// validation. So the kind tests need an active session.
// ---------------------------------------------------------------------

func TestHistoryKindEnum(t *testing.T) {
	t.Setenv("TETHER_SESSION", "lab")
	// Valid values: must NOT trigger the "must be one of" message.
	// We expect a downstream NATS error (refused port) instead.
	for _, k := range []string{"call", "proc", "port", "transfer"} {
		t.Run("valid="+k, func(t *testing.T) {
			home := t.TempDir()
			_, _, err := runRoot(t, "history",
				"--home", home, "--kind", k,
				"--nats-url", fastBrokerURL)
			if err == nil {
				t.Fatalf("expected NATS-side error past validation; got nil")
			}
			if strings.Contains(err.Error(), "must be one of") {
				t.Errorf("valid --kind %q should not be rejected; got %q", k, err.Error())
			}
		})
	}
	// Invalid values: must trip the explicit guard. Assert the FULL
	// enum message (including transfer) — the old prefix-only substring
	// `... call | proc | port` passed even when transfer was missing.
	for _, k := range []string{"invalid", "calls", "proc!", "PORT"} {
		t.Run("invalid="+k, func(t *testing.T) {
			home := t.TempDir()
			_, _, err := runRoot(t, "history",
				"--home", home, "--kind", k,
				"--nats-url", fastBrokerURL)
			if err == nil {
				t.Fatalf("expected --kind rejection for %q; got nil", k)
			}
			if !strings.Contains(err.Error(), "must be one of: call | proc | port | transfer") {
				t.Errorf("expected full enum-validation message; got %q", err.Error())
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 9: SetInterspersed(false) for exec / run.
//
// Without this, `tether exec lab-1 ls -la` blows up because cobra
// parses `-la` as our flag (-l, -a... unknown shorthand). With
// SetInterspersed(false), pflag stops parsing flags at the first
// positional and treats the rest as argv. The proof: error mentions
// "no active session" (exec's own RunE) — NOT "unknown shorthand".
// ---------------------------------------------------------------------

func TestExecRunInterspersedFalse(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{
			name: "exec passes -la to remote argv",
			args: []string{"exec", "lab-1", "ls", "-la"},
		},
		{
			name: "exec passes a long-form remote flag through",
			args: []string{"exec", "lab-1", "grep", "--color=auto", "foo", "/etc/hosts"},
		},
		{
			name: "run passes -- and remote flags through",
			args: []string{"run", "lab-1", "--", "vim", "-O", "a", "b"},
		},
		{
			name: "run passes a single-dash short flag through",
			args: []string{"run", "lab-1", "bash", "-c", "echo hi"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("TETHER_SESSION", "")
			// Inject --home BEFORE the first positional (the leaf
			// verb is at index 0). With interspersed=false, --home
			// after the first positional would be eaten as argv.
			full := append([]string{tc.args[0], "--home", home}, tc.args[1:]...)
			_, _, err := runRoot(t, full...)
			if err == nil {
				t.Fatal("expected no-active-session error")
			}
			es := err.Error()
			if strings.Contains(es, "unknown shorthand flag") ||
				strings.Contains(es, "unknown flag") {
				t.Errorf("SetInterspersed(false) failed — flag parsed as ours: %q", es)
			}
			if !strings.Contains(es, "no active session") {
				t.Errorf("expected 'no active session' (proves we reached RunE past parse); got %q", es)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 10: TETHER_SESSION env wins over current_session file.
// ---------------------------------------------------------------------

func TestEnvSessionWinsOverFile(t *testing.T) {
	home := t.TempDir()
	if err := cli.WriteCurrentSession(home, "filelab"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TETHER_SESSION", "envlab")
	out, _, err := runRoot(t, "ctx", "--home", home)
	if err != nil {
		t.Fatalf("ctx: %v", err)
	}
	if out != "envlab\n" {
		t.Errorf("env should win over file; got %q want %q", out, "envlab\n")
	}
}

// TestEnvSessionEmptyFallsBackToFile: the precedence is "env IF
// non-empty"; an env set to "" must fall through to the file. This
// is the contract documented in internal/cli/identity.go.
func TestEnvSessionEmptyFallsBackToFile(t *testing.T) {
	home := t.TempDir()
	if err := cli.WriteCurrentSession(home, "filelab"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TETHER_SESSION", "")
	out, _, err := runRoot(t, "ctx", "--home", home)
	if err != nil {
		t.Fatalf("ctx: %v", err)
	}
	if out != "filelab\n" {
		t.Errorf("empty env should fall through to file; got %q want %q",
			out, "filelab\n")
	}
}

// ---------------------------------------------------------------------
// Dimension 11: empty-string flag values vs unset.
//
// In cobra/pflag, `--flag ""` and "no --flag" both leave the bound
// variable as the registered default (typically ""). The tests below
// pin that "explicit empty == missing" parity for the flags whose
// validation messages we've already pinned in dimension 4.
// ---------------------------------------------------------------------

func TestEmptyFlagValueEqualsMissing(t *testing.T) {
	t.Setenv("TETHER_SESSION", "lab")
	cases := []struct {
		name        string
		argsEmpty   []string // --flag "" form
		argsMissing []string // flag omitted form
		wantSubstr  string
	}{
		{
			name: "session create: --pin ''",
			argsEmpty: []string{"session", "create", "labname",
				"--pin", "", "--nats-url", fastBrokerURL},
			argsMissing: []string{"session", "create", "labname",
				"--nats-url", fastBrokerURL},
			wantSubstr: "--pin is required",
		},
		{
			name: "expose: --name ''",
			argsEmpty: []string{"expose", "--local", "8888",
				"--name", "", "--nats-url", fastBrokerURL, "lab-1"},
			argsMissing: []string{"expose", "--local", "8888",
				"--nats-url", fastBrokerURL, "lab-1"},
			wantSubstr: "--name is required",
		},
		{
			name:        "agent: --session ''",
			argsEmpty:   []string{"agent", "--session", ""},
			argsMissing: []string{"agent"},
			wantSubstr:  "--session is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/empty", func(t *testing.T) {
			home := t.TempDir()
			args := injectHomeFlag(tc.argsEmpty, home)
			_, _, err := runRoot(t, args...)
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("empty form: want %q; got %v", tc.wantSubstr, err)
			}
		})
		t.Run(tc.name+"/missing", func(t *testing.T) {
			home := t.TempDir()
			args := injectHomeFlag(tc.argsMissing, home)
			_, _, err := runRoot(t, args...)
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("missing form: want %q; got %v", tc.wantSubstr, err)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Dimension 12: short-flag aliases.
//
// The aliases pinned here are part of the CLI's surface; users have
// scripted them. If a refactor changes pflag binding (e.g. drops the
// "n" short for --lines), these tests catch it.
// ---------------------------------------------------------------------

func TestShortFlagAliases(t *testing.T) {
	t.Run("ps -a == --all (parses; reaches active-session guard)", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("TETHER_SESSION", "")
		_, _, err := runRoot(t, "ps", "-a", "--home", home)
		if err == nil {
			t.Fatal("expected no-active-session")
		}
		if strings.Contains(err.Error(), "unknown shorthand") ||
			strings.Contains(err.Error(), "unknown flag") {
			t.Errorf("-a should be wired as --all; got %q", err.Error())
		}
		// Sanity: same with explicit --all.
		_, _, err2 := runRoot(t, "ps", "--all", "--home", home)
		if err2 == nil {
			t.Fatal("expected no-active-session for --all path too")
		}
	})

	t.Run("login -s lab == --session lab", func(t *testing.T) {
		home := t.TempDir()
		_, _, err := runRoot(t, "login", "-s", "lab",
			"--home", home, "--nats-url", fastBrokerURL)
		if err == nil {
			t.Fatal("expected NATS error past parse")
		}
		if strings.Contains(err.Error(), "unknown shorthand") {
			t.Errorf("-s should be wired as --session; got %q", err.Error())
		}
	})

	t.Run("history -n 5 == --lines 5", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("TETHER_SESSION", "lab")
		_, _, err := runRoot(t, "history", "-n", "5",
			"--home", home, "--nats-url", fastBrokerURL)
		if err == nil {
			t.Fatal("expected NATS error past parse")
		}
		if strings.Contains(err.Error(), "unknown shorthand") {
			t.Errorf("-n should be wired as --lines; got %q", err.Error())
		}
	})

	t.Run("history -f == --follow", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("TETHER_SESSION", "lab")
		_, _, err := runRoot(t, "history", "-f",
			"--home", home, "--nats-url", fastBrokerURL)
		if err == nil {
			t.Fatal("expected NATS error past parse")
		}
		if strings.Contains(err.Error(), "unknown shorthand") {
			t.Errorf("-f should be wired as --follow; got %q", err.Error())
		}
	})

	t.Run("history --kind call -n 10 (mixed long + short)", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("TETHER_SESSION", "lab")
		_, _, err := runRoot(t, "history",
			"--kind", "call", "-n", "10",
			"--home", home, "--nats-url", fastBrokerURL)
		if err == nil {
			t.Fatal("expected NATS error past parse")
		}
		if strings.Contains(err.Error(), "must be one of") ||
			strings.Contains(err.Error(), "unknown shorthand") {
			t.Errorf("mixed short/long should parse cleanly; got %q", err.Error())
		}
	})
}

// ---------------------------------------------------------------------
// Sanity: keep the reference to cobra explicit so future cleanups don't
// drop the import while the file still uses *cobra.Command via runRoot.
// ---------------------------------------------------------------------
var _ = cobra.Command{}
