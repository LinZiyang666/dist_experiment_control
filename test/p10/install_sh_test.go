// Package p10_test exercises P10 deliverables — install.sh and the
// node-upgrade verb. install.sh is invoked as a real shell script
// against a t.TempDir() $HOME so the assertions run on actual
// filesystem state without polluting the developer machine.
package p10_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// scriptPath returns the absolute path to scripts/install.sh in
// the source tree. Computed off this test file's location instead
// of os.Getwd() because go test sets the working directory to the
// package, but the script lives two levels up.
func scriptPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtimeCaller()
	repo := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	return filepath.Join(repo, "scripts", "install.sh")
}

// runtimeCaller wraps runtime.Caller(0) so callers don't need to
// import runtime themselves.
func runtimeCaller() (uintptr, string, int, bool) {
	return runtime.Caller(1)
}

// skipIfAgentOrBrokerUnsupported short-circuits tests that exercise
// `install.sh --role agent` or `--role broker` on hosts where the
// script intentionally refuses to install them. install.sh only
// supports --role ctl on darwin (the agent/broker code paths assume
// systemd / useradd / launchd-less startup). Without this guard the
// agent + broker test cases would fail with "not supported on macOS"
// on a developer laptop while still passing in Linux CI.
func skipIfAgentOrBrokerUnsupported(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("install.sh agent/broker roles are rejected on macOS by design; only --role ctl is supported there")
	}
}

// runInstall execs the install.sh under the given fake HOME. extra
// args go on the command line. Returns combined output for log
// inspection; non-zero exit returns t.Fatal.
func runInstall(t *testing.T, home string, extra ...string) string {
	t.Helper()
	cmd := exec.Command("bash", append([]string{scriptPath(t)}, extra...)...)
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, string(out))
	}
	return string(out)
}

// runInstallExpectFail is the negative-path counterpart — exit
// non-zero is expected.
func runInstallExpectFail(t *testing.T, home string, extra ...string) string {
	t.Helper()
	cmd := exec.Command("bash", append([]string{scriptPath(t)}, extra...)...)
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("install.sh unexpectedly succeeded\n%s", string(out))
	}
	return string(out)
}

// TestInstallShBareInvocationDefaultsToCtl: per architecture K.2,
// `curl install.sh | sh` (no flags) installs the ctl. Smaller-K
// roles always pass --role explicitly. The behavior was tightened
// from the original "--role required" to match the documented
// invocation.
func TestInstallShBareInvocationDefaultsToCtl(t *testing.T) {
	out := runInstall(t, t.TempDir(), "--dry-run", "--skip-download")
	if !strings.Contains(out, "role=ctl") {
		t.Errorf("expected ctl default; got:\n%s", out)
	}
}

// TestInstallShAgentDryRunNoFiles: --dry-run + --role agent must
// touch zero files outside $HOME and ZERO files inside $HOME (the
// dry-run wraps every mutating action). Architecture K.0 §2:
// install.sh never starts anything; here we verify it can also
// "do nothing" cleanly.
func TestInstallShAgentDryRunNoFiles(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	out := runInstall(t, home,
		"--role", "agent",
		"--broker", "wss://broker.example.com:443",
		"--session", "lab",
		"--pin", "123456",
		"--nid", "lab-1",
		"--dry-run",
		"--skip-download",
	)
	if !strings.Contains(out, "(dry-run)") {
		t.Errorf("expected dry-run markers in output; got:\n%s", out)
	}
	// $HOME stays empty after dry-run.
	entries, err := os.ReadDir(home)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("dry-run wrote files into HOME: %v", names)
	}
}

// TestInstallShAgentSkipDownloadWritesYAML: with --skip-download
// install.sh skips the binary fetch but still writes agent.yaml +
// hardens directory perms. Verifies the per-session layout from
// architecture K.1.
func TestInstallShAgentSkipDownloadWritesYAML(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	out := runInstall(t, home,
		"--role", "agent",
		"--broker", "wss://broker.example.com:443",
		"--session", "lab",
		"--pin", "123456",
		"--nid", "lab-1",
		"--skip-download",
	)
	if !strings.Contains(out, "agent config written to") {
		t.Errorf("missing next-step banner; got:\n%s", out)
	}
	yaml := filepath.Join(home, ".tether", "agent", "lab", "agent.yaml")
	body, err := os.ReadFile(yaml)
	if err != nil {
		t.Fatalf("agent.yaml not written: %v", err)
	}
	for _, want := range []string{
		"broker_url: wss://broker.example.com:443",
		"session: lab",
		"nid: lab-1",
		// tunnel_addr is derived from --broker by install.sh so the
		// agent.yaml is self-sufficient (architecture A.3 split-ports
		// puts frps on host:7000). Pin the derivation so a regression
		// in the sed pipeline doesn't silently leak operator-supplied
		// flags as an undocumented dependency.
		"tunnel_addr: broker.example.com:7000",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("agent.yaml missing %q; got:\n%s", want, string(body))
		}
	}

	// Per-session dir must be 0700, agent.yaml 0600.
	if fi, err := os.Stat(filepath.Dir(yaml)); err != nil {
		t.Fatal(err)
	} else if mode := fi.Mode().Perm(); mode != 0o700 {
		t.Errorf("session dir mode: got %o want 0700", mode)
	}
	if fi, err := os.Stat(yaml); err != nil {
		t.Fatal(err)
	} else if mode := fi.Mode().Perm(); mode != 0o600 {
		t.Errorf("agent.yaml mode: got %o want 0600", mode)
	}
}

// TestInstallShTunnelAddrDerivation pins the install.sh sed
// pipeline that turns --broker into agent.yaml's tunnel_addr.
// Architecture A.3 nails frps to port 7000, so we ALWAYS want
// "<broker host>:7000" regardless of the input scheme/port/path
// shape. A regression here would force operators to hand-edit
// tunnel_addr after install.sh runs.
func TestInstallShTunnelAddrDerivation(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	cases := []struct {
		name      string
		brokerURL string
		want      string
	}{
		{"wss with port", "wss://broker.example.com:443", "broker.example.com:7000"},
		{"ws no port", "ws://broker.example.com", "broker.example.com:7000"},
		{"bare host", "broker.example.com", "broker.example.com:7000"},
		{"with path", "wss://broker.example.com:443/nats", "broker.example.com:7000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			runInstall(t, home,
				"--role", "agent",
				"--broker", tc.brokerURL,
				"--session", "lab",
				"--pin", "x",
				"--nid", "lab-1",
				"--skip-download",
			)
			body, err := os.ReadFile(filepath.Join(home, ".tether", "agent", "lab", "agent.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			want := "tunnel_addr: " + tc.want
			if !strings.Contains(string(body), want) {
				t.Errorf("input %q: agent.yaml missing %q; got:\n%s",
					tc.brokerURL, want, string(body))
			}
		})
	}
}

// TestInstallShAgentRequiresMandatoryFlags: missing --broker /
// --session / --nid each surface a clean error.
func TestInstallShAgentRequiresMandatoryFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no broker", []string{"--role", "agent", "--session", "lab", "--nid", "lab-1"}, "--broker required"},
		{"no session", []string{"--role", "agent", "--broker", "wss://x", "--nid", "lab-1"}, "--session required"},
		{"no nid", []string{"--role", "agent", "--broker", "wss://x", "--session", "lab"}, "--nid required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := runInstallExpectFail(t, t.TempDir(), append(tc.args, "--skip-download", "--dry-run")...)
			if !strings.Contains(out, tc.want) {
				t.Errorf("expected %q; got: %s", tc.want, out)
			}
		})
	}
}

// TestInstallShCtlDryRun: ctl install dry-run should mention the
// PATH note when defaulting to ~/.local/bin (only triggered when
// /usr/local/bin isn't writable, which holds for a non-root test).
func TestInstallShCtlDryRun(t *testing.T) {
	home := t.TempDir()
	out := runInstall(t, home,
		"--role", "ctl",
		"--dry-run",
		"--skip-download",
	)
	if !strings.Contains(out, "tether installed to") {
		t.Errorf("missing install banner; got:\n%s", out)
	}
}

// TestInstallShBrokerDryRunIsNonRoot: broker install path requires
// root for useradd / /etc / /var; in dry-run we still verify the
// argument validation runs cleanly without trying to su.
func TestInstallShBrokerDryRun(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	out := runInstall(t, home,
		"--role", "broker",
		"--domain", "tether.example.com",
		"--acme-email", "admin@example.com",
		"--dry-run",
		"--skip-download",
	)
	for _, want := range []string{
		"broker files installed",
		"systemd units created",
		"sudo systemctl daemon-reload",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in broker dry-run output; got:\n%s", want, out)
		}
	}
}

// TestInstallShUnknownRoleRejected: --role foo dies cleanly.
func TestInstallShUnknownRoleRejected(t *testing.T) {
	out := runInstallExpectFail(t, t.TempDir(),
		"--role", "foobar",
		"--dry-run",
	)
	if !strings.Contains(out, "unknown --role foobar") {
		t.Errorf("expected role-validation error; got: %s", out)
	}
}

// TestInstallShDoesNotForkTether: assert install.sh leaves no
// tether process behind by counting children before/after. Even
// without --dry-run, install.sh MUST not invoke `tether` itself
// (architecture K.0 §2 + K.5 强制语义).
func TestInstallShDoesNotForkTether(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	// Create a fake tether binary so install.sh's `tether version`
	// path (if any future regression added one) would actually do
	// something visible.
	bin := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	fakePath := filepath.Join(bin, "tether")
	if err := os.WriteFile(fakePath, []byte("#!/bin/sh\necho 'INVOKED' > "+filepath.Join(home, "marker")+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	runInstall(t, home,
		"--role", "agent",
		"--broker", "wss://x",
		"--session", "lab",
		"--pin", "x",
		"--nid", "lab-1",
		"--skip-download",
	)
	if _, err := os.Stat(filepath.Join(home, "marker")); err == nil {
		t.Errorf("install.sh invoked the tether binary; K.0 §2 forbids this")
	}
}
