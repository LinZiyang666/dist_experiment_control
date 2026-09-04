// Package p10_test exercises P10 deliverables — install.sh and the
// node-upgrade verb. install.sh is invoked as a real shell script
// against a t.TempDir() $HOME so the assertions run on actual
// filesystem state without polluting the developer machine.
package p10_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	// The banner no longer says "written to" unconditionally: on a re-run that KEEPS an
	// existing file nothing was written, and claiming otherwise was round 2's K-F7.
	if !strings.Contains(out, "agent config:") {
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

	// Fresh-install contract (transfer-unrestrict v0.4.0): the generated
	// agent.yaml must contain NO ACTIVE (non-commented) file_transfer /
	// allow_roots key, so a fresh install resolves to OPEN (whole-FS) mode.
	// The template ships a COMMENTED example block; a future edit that
	// uncomments `allow_roots: []` would silently DISABLE transfer on every
	// new install (the inverse footgun the change exists to prevent), so
	// pin that only comment lines may mention these keys.
	for _, ln := range strings.Split(string(body), "\n") {
		trimmed := strings.TrimSpace(ln)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.Contains(trimmed, "allow_roots") || trimmed == "file_transfer:" {
			t.Errorf("fresh install must ship OPEN (no active file_transfer/allow_roots); got active line %q", ln)
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
	// #76: units are ENABLED for boot by default (symlink only, never
	// started); the dry-run must preview both the daemon-reload and the
	// enable, and the banner tells the operator to START (enable already
	// happened) rather than enable --now.
	for _, want := range []string{
		"broker files installed",
		"systemd units created and ENABLED for boot",
		"(dry-run) systemctl daemon-reload",
		"(dry-run) systemctl enable nats-server tether-broker caddy",
		"sudo systemctl start nats-server tether-broker caddy",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in broker dry-run output; got:\n%s", want, out)
		}
	}
	// The enable is symlink-only: the new banner must NOT resurrect an
	// `enable --now` in the default path (--now would start processes).
	if strings.Contains(out, "enable --now") {
		t.Errorf("default banner must not suggest enable --now (units are already enabled; --now would start); got:\n%s", out)
	}
}

// TestInstallShBrokerNoEnable pins the #76 opt-out: --no-enable skips the
// enable entirely and the banner falls back to the full manual command.
// origin: docs/deploy-tier-gotchas.md #76
func TestInstallShBrokerNoEnable(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	out := runInstall(t, t.TempDir(),
		"--role", "broker",
		"--domain", "tether.example.com",
		"--acme-email", "admin@example.com",
		"--dry-run",
		"--skip-download",
		"--no-enable",
	)
	if strings.Contains(out, "(dry-run) systemctl enable") {
		t.Errorf("--no-enable must skip the enable, got:\n%s", out)
	}
	// The opt-out banner must hand the operator the complete command,
	// enable --now included — they chose to own that step.
	if !strings.Contains(out, "sudo systemctl enable --now nats-server tether-broker caddy") {
		t.Errorf("--no-enable banner must carry the full enable --now command; got:\n%s", out)
	}
}

// TestInstallShJournaldDropin pins the #77 conditional journald cap.
// TETHER_JOURNALD_ROOT is the test-only seam; the tier VALUE is asserted by
// shape only ([0-9]+M) — the exact tier depends on the build host's /var/log
// filesystem and is independently recomputed by drill 32 on a real container.
// origin: docs/deploy-tier-gotchas.md #77
func TestInstallShJournaldDropin(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	brokerDry := func(t *testing.T, jroot string) string {
		t.Helper()
		home := t.TempDir()
		cmd := exec.Command("bash", scriptPath(t),
			"--role", "broker",
			"--domain", "tether.example.com",
			"--acme-email", "admin@example.com",
			"--dry-run",
			"--skip-download",
		)
		cmd.Env = append(os.Environ(), "HOME="+home, "TETHER_JOURNALD_ROOT="+jroot)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("install.sh failed: %v\n%s", err, out)
		}
		return string(out)
	}
	dropinRe := regexp.MustCompile(`\(dry-run\) write .*journald\.conf\.d/60-tether\.conf \(SystemMaxUse=[0-9]+M\)`)

	t.Run("no explicit setting: drop-in previewed with a derived cap", func(t *testing.T) {
		out := brokerDry(t, t.TempDir())
		if !dropinRe.MatchString(out) {
			t.Errorf("expected a journald drop-in preview with a derived SystemMaxUse; got:\n%s", out)
		}
	})

	t.Run("explicit operator setting is respected", func(t *testing.T) {
		jroot := t.TempDir()
		if err := os.WriteFile(filepath.Join(jroot, "journald.conf"),
			[]byte("[Journal]\nSystemMaxUse=800M\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		out := brokerDry(t, jroot)
		if dropinRe.MatchString(out) {
			t.Errorf("an explicit SystemMaxUse must suppress the drop-in; got:\n%s", out)
		}
		if !strings.Contains(out, "operator setting respected") {
			t.Errorf("the skip must be announced; got:\n%s", out)
		}
	})

	t.Run("commented-out stub is NOT a setting", func(t *testing.T) {
		// The live incident host had exactly a commented `#SystemMaxUse=`
		// stub — treating that as configured would be the #77 failure mode
		// all over again.
		jroot := t.TempDir()
		if err := os.WriteFile(filepath.Join(jroot, "journald.conf"),
			[]byte("[Journal]\n#SystemMaxUse=\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		out := brokerDry(t, jroot)
		if !dropinRe.MatchString(out) {
			t.Errorf("a commented-out SystemMaxUse is not a setting; the drop-in must still be previewed; got:\n%s", out)
		}
	})

	t.Run("our own MARKED drop-in does not suppress a rewrite", func(t *testing.T) {
		// Idempotence (review F3): a re-install must overwrite OUR file — proven
		// by its ownership MARKER, not by its path — to pick up a grown disk. An
		// UNMARKED same-name file is a foreign operator file (covered below).
		jroot := t.TempDir()
		dir := filepath.Join(jroot, "journald.conf.d")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "60-tether.conf"),
			[]byte("# managed-by: tether-install.sh (#77 journald cap)\n[Journal]\nSystemMaxUse=200M\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		out := brokerDry(t, jroot)
		if !dropinRe.MatchString(out) {
			t.Errorf("our own MARKED prior drop-in must be overwritten on re-install; got:\n%s", out)
		}
	})

	t.Run("foreign same-name drop-in is not claimed as ours", func(t *testing.T) {
		// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F3
		// A filename is not an ownership marker. An operator or configuration
		// manager may already own 60-tether.conf; install.sh must not overwrite it
		// merely because that is also tether's preferred basename.
		jroot := t.TempDir()
		dir := filepath.Join(jroot, "journald.conf.d")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "60-tether.conf"),
			[]byte("# managed by site policy\n[Journal]\nSystemMaxUse=350M\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		out := brokerDry(t, jroot)
		if dropinRe.MatchString(out) {
			t.Errorf("install.sh claimed an unmarked operator-owned 60-tether.conf as its own and would overwrite it:\n%s", out)
		}
	})

	t.Run("foreign same-name drop-in WITHOUT SystemMaxUse is still respected", func(t *testing.T) {
		// The load-bearing case for the ownership marker (review F3): a foreign
		// file that does NOT set SystemMaxUse would slip past the operator-scan
		// (which only looks for SystemMaxUse) and be overwritten — unless the
		// path-collision is caught by the marker check first.
		jroot := t.TempDir()
		dir := filepath.Join(jroot, "journald.conf.d")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "60-tether.conf"),
			[]byte("# managed by site policy\n[Journal]\nRateLimitBurst=1000\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		out := brokerDry(t, jroot)
		if dropinRe.MatchString(out) {
			t.Errorf("install.sh would overwrite an unmarked foreign drop-in that sets no SystemMaxUse (F3):\n%s", out)
		}
		if !strings.Contains(out, "NOT tether-owned") {
			t.Errorf("install.sh should announce it is leaving the foreign file; got:\n%s", out)
		}
	})

	t.Run("another conf.d file with an explicit setting suppresses", func(t *testing.T) {
		jroot := t.TempDir()
		dir := filepath.Join(jroot, "journald.conf.d")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "50-site.conf"),
			[]byte("[Journal]\nSystemMaxUse=2G\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		out := brokerDry(t, jroot)
		if dropinRe.MatchString(out) {
			t.Errorf("an operator conf.d setting must suppress the drop-in; got:\n%s", out)
		}
	})
}

// TestInstallShBrokerUninstallSymmetry (#76/#77): --uninstall previews the
// disable and the journald drop-in removal.
// origin: docs/deploy-tier-gotchas.md #76
func TestInstallShBrokerUninstallSymmetry(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	out := runInstall(t, t.TempDir(),
		"--role", "broker",
		"--dry-run",
		"--uninstall",
	)
	if !strings.Contains(out, "journald.conf.d/60-tether.conf") {
		t.Errorf("uninstall must remove the journald drop-in; got:\n%s", out)
	}
	// The path is now QUOTED and carries its `.new` sidecar — round 2, K-F9 (clean up
	// the orphan) and G2 (resolve through $SYSTEMD_DIR so a redirected uninstall cannot
	// delete a real broker's files). Assert both halves rather than a prefix, so the
	// sidecar removal cannot be dropped without this noticing.
	if !strings.Contains(out, "rm -f '/etc/systemd/system/tether-broker.service'") {
		t.Errorf("uninstall must remove the unit files; got:\n%s", out)
	}
	if !strings.Contains(out, "'/etc/systemd/system/tether-broker.service.new'") {
		t.Errorf("uninstall must also remove the unit's .new sidecar, or a host that was ever "+
			"re-installed keeps an orphan no later run mentions; got:\n%s", out)
	}
	// review Mi7: dry-run uninstall previews the disable host-INDEPENDENTLY
	// (the systemd probe is skipped under --dry-run).
	if !strings.Contains(out, "systemctl disable nats-server tether-broker caddy") {
		t.Errorf("dry-run uninstall must preview the disable regardless of host systemd; got:\n%s", out)
	}
}

// TestInstallShBrokerBannerSelfConsistent (review M4): the closing banner must
// never claim "ENABLED for boot" and "NOT enabled" simultaneously — claiming
// enabled where enable did not run is how #76 came back on the env-guard path.
// origin: docs/deploy-tier-gotchas.md #76
func TestInstallShBrokerBannerSelfConsistent(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	for _, extra := range [][]string{{}, {"--no-enable"}} {
		args := append([]string{"--role", "broker", "--domain", "brkx",
			"--acme-email", "x@x.test", "--dry-run", "--skip-download"}, extra...)
		out := runInstall(t, t.TempDir(), args...)
		enabled := strings.Contains(out, "ENABLED for boot")
		notEnabled := strings.Contains(out, "NOT enabled")
		if enabled && notEnabled {
			t.Errorf("banner contradicts itself (both ENABLED and NOT enabled) for args %v:\n%s", extra, out)
		}
		if len(extra) == 0 && !enabled {
			t.Errorf("default dry-run banner must state ENABLED for boot; got:\n%s", out)
		}
		if len(extra) == 1 && !notEnabled {
			t.Errorf("--no-enable banner must state NOT enabled; got:\n%s", out)
		}
	}
}

// TestInstallShJournaldStaleDropinRemoved (review M5): if an operator sets
// SystemMaxUse AFTER a prior install left our 60-tether.conf, a reinstall must
// REMOVE our stale drop-in (journald merges by filename order, so a leftover
// would keep overriding the operator's setting).
// origin: docs/deploy-tier-gotchas.md #77
func TestInstallShJournaldStaleDropinRemoved(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	jroot := t.TempDir()
	// Operator's explicit setting in the main config (spaced form, review Mi6).
	if err := os.WriteFile(filepath.Join(jroot, "journald.conf"),
		[]byte("[Journal]\nSystemMaxUse = 2G\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A stale prior-install drop-in — MARKED as ours (review F3: only a
	// marker-owned file may be removed; a foreign same-name file must survive).
	dir := filepath.Join(jroot, "journald.conf.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "60-tether.conf"),
		[]byte("# managed-by: tether-install.sh (#77 journald cap)\n[Journal]\nSystemMaxUse=200M\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	cmd := exec.Command("bash", scriptPath(t),
		"--role", "broker", "--domain", "brkx", "--acme-email", "x@x.test",
		"--dry-run", "--skip-download")
	cmd.Env = append(os.Environ(), "HOME="+home, "TETHER_JOURNALD_ROOT="+jroot)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, out)
	}
	s := string(out)
	// Mi6: the spaced operator setting must be RECOGNIZED (respected)…
	if !strings.Contains(s, "operator setting respected") {
		t.Errorf("spaced `SystemMaxUse = 2G` must be recognized as an operator setting; got:\n%s", s)
	}
	// M5: …and our stale drop-in removed so it stops shadowing the operator.
	if !strings.Contains(s, "removed our stale journald drop-in") {
		t.Errorf("a stale 60-tether.conf must be removed when an operator setting exists; got:\n%s", s)
	}
}

// TestInstallShJournaldForeignDropinSurvivesUninstall (review F3): a same-name
// operator/site-policy file WITHOUT the tether ownership marker must NOT be
// removed by --uninstall — a root uninstaller deleting operator config is the
// mirror defect of the install-side overwrite.
// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F3
func TestInstallShJournaldForeignDropinSurvivesUninstall(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	jroot := t.TempDir()
	dir := filepath.Join(jroot, "journald.conf.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(dir, "60-tether.conf")
	if err := os.WriteFile(foreign, []byte("# managed by site policy\n[Journal]\nSystemMaxUse=350M\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	cmd := exec.Command("bash", scriptPath(t), "--role", "broker", "--uninstall")
	cmd.Env = append(os.Environ(), "HOME="+home, "TETHER_JOURNALD_ROOT="+jroot)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("uninstall failed: %v\n%s", err, out)
	}
	if _, statErr := os.Stat(foreign); statErr != nil {
		t.Fatalf("uninstall deleted an UNMARKED operator drop-in (F3): %v\noutput:\n%s", statErr, out)
	}
	if !strings.Contains(string(out), "NOT tether-owned") {
		t.Errorf("uninstall should announce it is leaving the foreign file; got:\n%s", out)
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
