package p10_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

// Each invocation gets a private HOME, system root and executable tripwires.
// Even an accidental live start or download is observable without reaching a
// service manager, the network, or the installed production binary.
type retrofitFixture struct {
	home, root, bin, marker string
	env                     []string
}

func newRetrofitFixture(t *testing.T) retrofitFixture {
	t.Helper()
	f := retrofitFixture{home: t.TempDir(), root: t.TempDir(), bin: t.TempDir()}
	f.marker = filepath.Join(t.TempDir(), "invocations")
	tripwire := "#!/bin/sh\nprintf '%s\\n' \"$0 $*\" >> \"$INSTALL_TEST_MARKER\"\nexit 99\n"
	for _, name := range []string{"tether", "systemctl", "curl", "nats-server", "caddy", "useradd", "tether-infra-helper", "tether-infra-launch", "nft"} {
		writeInstallFixture(t, filepath.Join(f.bin, name), tripwire, 0o755)
		if name == "tether" || name == "nats-server" || name == "caddy" {
			writeInstallFixture(t, filepath.Join(f.root, "usr/local/bin", name), tripwire, 0o755)
		}
	}
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "HOME=") && !strings.HasPrefix(env, "PATH=") && !strings.HasPrefix(env, "TETHER_") && !strings.HasPrefix(env, "INSTALL_TEST_MARKER=") {
			f.env = append(f.env, env)
		}
	}
	f.env = append(f.env, "HOME="+f.home, "PATH="+f.bin+string(os.PathListSeparator)+os.Getenv("PATH"), "TETHER_INSTALL_ROOT="+f.root, "INSTALL_TEST_MARKER="+f.marker)
	return f
}

func writeInstallFixture(t *testing.T, path, body string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}

func (f retrofitFixture) run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return f.runShell(t, []string{"sh"}, args...)
}

func (f retrofitFixture) runShell(t *testing.T, shell []string, args ...string) (string, error) {
	t.Helper()
	argv := append(append([]string{}, shell[1:]...), scriptPath(t))
	cmd := exec.CommandContext(t.Context(), shell[0], append(argv, args...)...)
	cmd.Env = f.env
	out, err := testsupervisor.CombinedOutput(cmd)
	// An installer could swallow an error from a started process. Its return
	// code alone therefore cannot prove K.0 or the offline supply contract.
	if data, readErr := os.ReadFile(f.marker); readErr == nil {
		t.Fatalf("installer invoked a forbidden executable:\n%s\n%s", data, out)
	} else if !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	return string(out), err
}

// origin: hot-update S8 plan §5.2 / §11.2 P20 — retrofit stays executable by POSIX callers.
func TestInstallShRetrofitPOSIXShells(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	for _, shell := range [][]string{{"bash"}, {"dash"}, {"busybox", "sh"}} {
		t.Run(shell[0], func(t *testing.T) {
			if _, err := exec.LookPath(shell[0]); err != nil {
				t.Skipf("shell unavailable: %v", err)
			}
			for _, role := range []string{"broker", "agent"} {
				f := newRetrofitFixture(t)
				out, err := f.runShell(t, shell, "--hot-update-retrofit", "--role", role, "--session", "lab")
				if err != nil || !strings.Contains(out, "canonical startup templates are prepared") {
					t.Fatalf("%s retrofit: %v\n%s", role, err, out)
				}
			}
		})
	}
}

func assertInstallContents(t *testing.T, path, want string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil || string(body) != want {
		t.Fatalf("%s: got %q, want %q: %v", path, body, want, err)
	}
}

func assertInstallMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != want {
		t.Fatalf("%s: mode %04o, want %04o", path, info.Mode().Perm(), want)
	}
}

// origin: hot-update S8 plan §5.2 / §11.2 P20 / LQ1
func TestHotUpdateRetrofitPreservesInstallation(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	for _, role := range []string{"broker", "agent"} {
		t.Run(role, func(t *testing.T) {
			f := newRetrofitFixture(t)
			preserved := map[string]string{
				"etc/tether/broker.yaml":                      "# operator cluster config\n",
				"etc/tether/nats.d/nats.conf":                 "# operator ping configuration\n",
				"etc/tether/Caddyfile":                        "# operator TLS configuration\n",
				"etc/systemd/system/tether-broker.service":    "# operator canonical unit\n",
				"etc/systemd/system/nats-server.service":      "# operator NATS unit\n",
				"etc/systemd/system/caddy.service":            "# operator Caddy unit\n",
				"var/lib/tether/upgrade-state.json":           "existing pending transaction\n",
				"var/lib/tether-upgrade/artifacts/source-pin": "retained immutable source\n",
			}
			for _, name := range []string{"tether", "nats-server", "caddy"} {
				path := "usr/local/bin/" + name
				body, err := os.ReadFile(filepath.Join(f.root, path))
				if err != nil {
					t.Fatal(err)
				}
				preserved[path] = string(body)
			}
			for path, body := range preserved {
				writeInstallFixture(t, filepath.Join(f.root, path), body, 0o600)
			}
			writeInstallFixture(t, filepath.Join(f.home, ".tether/agent/lab/agent.yaml"), "# operator agent config\n", 0o600)
			business := filepath.Join(f.root, "var/lib/tether")
			if err := os.Chmod(business, 0o751); err != nil {
				t.Fatal(err)
			}
			args := []string{"--hot-update-retrofit"} // The documented bare retrofit defaults to broker.
			if role == "agent" {
				args = append(args, "--role", role, "--session", "lab")
			}
			for repeat := 0; repeat < 2; repeat++ {
				out, err := f.run(t, args...)
				if err != nil {
					t.Fatalf("retrofit: %v\n%s", err, out)
				}
				for path, want := range preserved {
					assertInstallContents(t, filepath.Join(f.root, path), want)
				}
				assertInstallContents(t, filepath.Join(f.home, ".tether/agent/lab/agent.yaml"), "# operator agent config\n")
				assertInstallMode(t, business, 0o751)
				for _, path := range []string{"var/lib/tether-upgrade", "var/lib/tether-upgrade/control", "var/lib/tether-upgrade/artifacts"} {
					assertInstallMode(t, filepath.Join(f.root, path), 0o755)
				}
				if role == "broker" {
					assertInstallMode(t, filepath.Join(business, "upgrade-control"), 0o700)
					unit, err := os.ReadFile(filepath.Join(f.root, "etc/systemd/system/tether-broker.service.new"))
					if err != nil || !strings.Contains(string(unit), "ExecStart="+filepath.Join(f.root, "usr/local/bin/tether")+" serve --config "+filepath.Join(f.root, "etc/tether/broker.yaml")) {
						t.Fatalf("canonical startup template missing: %v\n%s", err, unit)
					}
					if !strings.Contains(out, "EXISTING CONFIG KEPT") {
						t.Fatal("retrofit concealed the kept startup template")
					}
				} else {
					assertInstallMode(t, filepath.Join(f.home, ".tether/agents/lab/upgrades"), 0o700)
				}
			}
		})
	}
}

// origin: hot-update S8 plan §5.2 / §11.2 P20 — preview has no filesystem effects.
func TestHotUpdateRetrofitDryRunWritesNothing(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	for _, role := range []string{"broker", "agent"} {
		f := newRetrofitFixture(t)
		out, err := f.run(t, "--hot-update-retrofit", "--role", role, "--session", "lab", "--dry-run")
		if err != nil || !strings.Contains(out, "(dry-run)") {
			t.Fatalf("preview: %v\n%s", err, out)
		}
		for _, path := range []string{filepath.Join(f.root, "var"), filepath.Join(f.root, "etc"), filepath.Join(f.home, ".tether")} {
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatalf("dry-run wrote %s: %v", path, err)
			}
		}
	}
}

// origin: hot-update S8 plan §5.2 / §11.2 P20 — reject ambiguous retrofit effects and aliases.
func TestHotUpdateRetrofitRejectsUnsafeInputs(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	for _, tc := range []struct {
		extra []string
		want  string
	}{
		{[]string{"--uninstall"}, "excludes --uninstall"},
		{[]string{"--force-config"}, "excludes --force-config"},
		{[]string{"--bin", "unused"}, "excludes --bin"},
		{[]string{"--role", "ctl"}, "requires --role broker or agent"},
		{[]string{"--role", "agent", "--session", "../escape"}, "safe nonempty session ID"},
	} {
		f := newRetrofitFixture(t)
		out, err := f.run(t, append([]string{"--hot-update-retrofit"}, tc.extra...)...)
		if err == nil || !strings.Contains(out, tc.want) {
			t.Fatalf("unsafe args %v: want %q refusal, got %v: %s", tc.extra, tc.want, err, out)
		}
		if _, err := os.Stat(filepath.Join(f.root, "var")); !os.IsNotExist(err) {
			t.Fatalf("invalid retrofit mutated shared paths: %v", err)
		}
	}
	f := newRetrofitFixture(t)
	target := t.TempDir()
	if err := os.Chmod(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.root, "var/lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(f.root, "var/lib/tether-upgrade")); err != nil {
		t.Fatal(err)
	}
	out, err := f.run(t, "--hot-update-retrofit")
	if err == nil || !strings.Contains(out, "symlink") {
		t.Fatalf("alias accepted: %v\n%s", err, out)
	}
	assertInstallMode(t, target, 0o700)
}

func exerciseRetrofitNeverStarts(t *testing.T) {
	t.Helper()
	for _, role := range []string{"broker", "agent"} {
		f := newRetrofitFixture(t)
		out, err := f.run(t, "--hot-update-retrofit", "--role", role, "--session", "lab")
		if err != nil {
			t.Fatalf("retrofit: %v\n%s", err, out)
		}
	}
}

// origin: hot-update S8 plan §5.2 / §7.2 / §11.2 P20 — local supply remains offline.
// origin: S9 internal review FIX-INST F4
func TestInstallShOfflineBinaryDoesNotDownloadOrExecute(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	for _, role := range []string{"ctl", "agent", "broker"} {
		t.Run(role, func(t *testing.T) {
			f := newRetrofitFixture(t)
			preserved := map[string]string{}
			for _, name := range []string{"nats-server", "caddy"} {
				path := filepath.Join(f.bin, name)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				preserved[path] = string(data)
			}
			local := filepath.Join(t.TempDir(), "verified candidate")
			payload := "#!/bin/sh\nprintf 'local binary executed\\n' >> \"$INSTALL_TEST_MARKER\"\nexit 99\n"
			writeInstallFixture(t, local, payload, 0o600)
			args := []string{"--role", role, "--bin", local, "--prefix", f.bin}
			switch role {
			case "agent":
				args = append(args, "--broker", "wss://example.test", "--session", "lab", "--nid", "lab-1")
			case "broker":
				// origin: hot-update S9 plan §6.4 / §6.5
				// Broker --bin now requires an executable ELF and the full verified
				// offline release. A real tripwire keeps execution detectable after
				// replacing the old shell-script fixture with that required format.
				program := filepath.Join(t.TempDir(), "candidate.go")
				writeInstallFixture(t, program, `package main
import "os"
func main() {
    file, err := os.OpenFile(os.Getenv("INSTALL_TEST_MARKER"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
    if err != nil { os.Exit(98) }
    _, _ = file.WriteString(os.Args[0] + " candidate executed\n")
    _ = file.Close()
    os.Exit(99)
}
`, 0o600)
				if err := os.Remove(local); err != nil {
					t.Fatal(err)
				}
				build := exec.CommandContext(t.Context(), "go", "build", "-o", local, program)
				build.Env = append(os.Environ(), "CGO_ENABLED=0")
				if output, err := testsupervisor.CombinedOutput(build); err != nil {
					t.Fatalf("build offline execution tripwire: %v\n%s", err, output)
				}
				data, err := os.ReadFile(local)
				if err != nil {
					t.Fatal(err)
				}
				payload = string(data)
				supply := brokerOfflineSupply(t, data)
				f.env = append(f.env, "TETHER_INFRA_SUPPORT_DIR="+supply)
				t.Cleanup(func() {
					_ = filepath.WalkDir(f.root, func(path string, entry os.DirEntry, err error) error {
						if err == nil && entry.IsDir() {
							return os.Chmod(path, 0o700)
						}
						return err
					})
				})
				args = append(args, "--domain", "example.test", "--acme-email", "ops@example.test", "--version", "v0.0.0-installer-fixture")
			}
			if role == "broker" {
				// Missing nats.d is the actual partial-install counterexample.
				// Its preview must not create the publication parent or authority.
				out, err := f.run(t, append(append([]string{}, args...), "--dry-run")...)
				if err != nil || !strings.Contains(out, "(dry-run)") {
					t.Fatalf("offline preview: %v\n%s", err, out)
				}
				for _, path := range []string{"etc", "var", "run"} {
					if _, err := os.Lstat(filepath.Join(f.root, path)); !os.IsNotExist(err) {
						t.Fatalf("offline dry-run created %s: %v", path, err)
					}
				}
			}
			out, err := f.run(t, args...)
			if err != nil {
				t.Fatalf("offline install: %v\n%s", err, out)
			}
			assertInstallContents(t, filepath.Join(f.bin, "tether"), payload)
			assertInstallMode(t, filepath.Join(f.bin, "tether"), 0o755)
			for path, want := range preserved {
				assertInstallContents(t, path, want)
			}
			if role == "broker" {
				parent := filepath.Join(f.root, "etc/tether/nats.d")
				assertInstallMode(t, parent, 0o750)
				if _, err := os.Stat(filepath.Join(parent, "nats.conf.new")); err != nil {
					t.Fatalf("partial installation has no staged NATS config: %v", err)
				}
				if _, err := os.Stat(filepath.Join(parent, "nats.conf")); !os.IsNotExist(err) {
					t.Fatalf("stage-only installed a live NATS config: %v", err)
				}
				if err := os.Chmod(parent, 0o711); err != nil {
					t.Fatal(err)
				}
				before, err := os.Stat(parent)
				if err != nil {
					t.Fatal(err)
				}
				if out, err := f.run(t, args...); err != nil {
					t.Fatalf("offline re-run: %v\n%s", err, out)
				}
				after, err := os.Stat(parent)
				if err != nil || !os.SameFile(before, after) || after.Mode().Perm() != 0o711 {
					t.Fatalf("re-run changed existing NATS directory inode or mode: %v", err)
				}
			}
			if _, err := os.Stat(filepath.Join(f.bin, ".tether.new")); !os.IsNotExist(err) {
				t.Fatalf("offline install retained staging file: %v", err)
			}
		})
	}
}

// origin: hot-update S8 plan §5.2 / LQ1 / §11.2 P20 — the shell only renders the handoff.
func TestUpgradeStageIsExplicitInstallerHandoff(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	f := newRetrofitFixture(t)
	out, err := f.run(t, "--hot-update-retrofit")
	if err != nil {
		t.Fatalf("retrofit: %v\n%s", err, out)
	}
	command := "tether upgrade stage --grant grant.json --source source-image --candidate candidate-image --receipt-out receipt.json"
	if strings.Count(out, command) != 1 {
		t.Fatalf("missing explicit artifact entry: %s", out)
	}
	opts, err := upgradecli.ParseStage(strings.Fields(strings.TrimPrefix(command, "tether upgrade stage ")))
	if err != nil || opts != (upgradecli.StageOptions{Grant: "grant.json", Source: "source-image", Candidate: "candidate-image", ReceiptOut: "receipt.json"}) {
		t.Fatalf("rendered stage command no longer matches the real parser: %+v: %v", opts, err)
	}
	for _, want := range []string{"only upgrade journal writer", "node upgrade NID --tx-id TXID --artifact-receipt receipt.json", "broker upgrade reload --tx-id TXID --artifact-receipt receipt.json"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing handoff %q: %s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(f.root, "var/lib/tether/upgrade-state.json")); !os.IsNotExist(err) {
		t.Fatalf("installer became a journal writer: %v", err)
	}
}

// origin: hot-update S8 plan §5.2 / §11.2 P20 — lint must reject shell-active generated prose.
// origin: S9 internal review FIX-INST F5
func TestInstallHeredocLintControls(t *testing.T) {
	source, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	lint := filepath.Join(filepath.Dir(filepath.Dir(scriptPath(t))), "test/simcluster/tests/lint-install.sh")
	for _, tc := range []struct {
		name, append, literal string
		unquote               bool
		wantFail              bool
	}{
		{"original", "", "", false, false},
		{"unquoted backtick", "\ncat <<INSTALL_CONTROL\n# `forbidden-command`\nINSTALL_CONTROL\n", "", false, true},
		{"unquoted dollar paren", "\ncat << INSTALL_CONTROL\n# $(forbidden-command)\nINSTALL_CONTROL\n", "", false, true},
		{"existing banner backtick", "", "# `forbidden-command`\n", true, true},
		{"existing banner dollar paren", "", "# $(forbidden-command)\n", true, true},
		// origin: hot-update S9 plan §1 / §12 P37 / T88
		// Exact banner inventory now rejects an extra document even when inert.
		// Put the positive quoting control inside an existing quoted banner so
		// it continues to prove literal substitutions without changing the set.
		{"quoted literal", "", "# `literal` $(literal)\n", false, false},
		{"extra quoted banner", "\ncat <<'INSTALL_CONTROL'\n# `literal` $(literal)\nINSTALL_CONTROL\n", "", false, true},
		{"empty scan", "#!/bin/sh\nexit 0\n", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "install-control.sh")
			body := string(source) + tc.append
			if tc.literal != "" {
				const opener = "    cat <<'EOF'\n"
				if !strings.Contains(body, opener) {
					t.Fatal("quoted banner control has no actual installer target")
				}
				replacement := opener
				if tc.unquote {
					replacement = "    cat <<EOF\n"
				}
				body = strings.Replace(body, opener, replacement+tc.literal, 1)
			}
			if tc.name == "empty scan" {
				body = tc.append
			}
			writeInstallFixture(t, path, body, 0o600)
			cmd := exec.CommandContext(t.Context(), "sh", lint, path)
			out, err := testsupervisor.CombinedOutput(cmd)
			if (err != nil) != tc.wantFail {
				t.Fatalf("lint failure=%v, want %v:\n%s", err, tc.wantFail, out)
			}
			if tc.unquote && !strings.Contains(string(out), "command substitution") {
				t.Fatalf("active-body control failed for another reason:\n%s", out)
			}
		})
	}
}

// origin: S8 internal review remaining-orchestration F5 (R5) / hot-update S8 plan §5.2 LQ1
func TestRetrofitRendersCompleteDeploymentDeclaration(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	for _, roleName := range []string{"agent", "broker"} {
		t.Run(roleName, func(t *testing.T) {
			f := newRetrofitFixture(t)
			role := upgradecli.AgentBootRole(filepath.Join(f.home, ".tether"), "lab", "node", uint32(os.Geteuid()))
			if roleName == "broker" {
				role = upgradecli.BrokerBootRole(filepath.Join(f.root, "var/lib/tether"), filepath.Join(f.root, "etc/tether/broker.yaml"), "", uint32(os.Geteuid()))
			}
			// The supplied roster contains a sibling under another UID. The shell
			// preserves the entire declaration instead of inferring a singleton.
			sibling := role.Want
			sibling.Component = upgradetx.ComponentAgent
			sibling.SID = "sibling"
			sibling.ConfiguredNID = "other"
			sibling.UID++
			reg := upgradelock.Registration{CanonicalPath: filepath.Join(f.root, "usr/local/bin/tether"), ControlDir: role.ControlDir, DeploymentID: "fixture", Aliases: []string{}, Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "primary"}, {Key: sibling, ServiceID: "sibling"}}}
			body, _ := json.Marshal(reg)
			source := filepath.Join(t.TempDir(), "registration.json")
			writeInstallFixture(t, source, string(body), 0600)
			args := []string{"--hot-update-retrofit", "--role", roleName, "--session", "lab", "--hot-update-registration", source}
			if out, err := f.run(t, append(args, "--dry-run")...); err != nil || !strings.Contains(out, "installation.json") {
				t.Fatalf("registration preview: %v %s", err, out)
			}
			if _, err := os.Stat(role.ControlDir); !os.IsNotExist(err) {
				t.Fatalf("preview created registry: %v", err)
			}
			for repeat := 0; repeat < 2; repeat++ {
				if out, err := f.run(t, args...); err != nil {
					t.Fatalf("registration render: %v %s", err, out)
				}
				assertInstallContents(t, filepath.Join(role.ControlDir, "installation.json"), string(body))
				assertInstallMode(t, filepath.Join(role.ControlDir, "installation.json"), 0600)
			}
			plan, err := upgradecli.RegisteredJournal(role)
			if err != nil || !plan.Registered {
				t.Fatalf("rendered declaration cannot register the canonical entry: %+v %v", plan, err)
			}
			registration, err := upgradelock.RegistrationFor(plan.Key)
			if err != nil || len(registration.Participants) != 2 || registration.Participants[1].Key.UID != sibling.UID {
				t.Fatalf("sibling declaration lost: %+v %v", registration, err)
			}
			if _, err := os.Stat(plan.Journal); !os.IsNotExist(err) {
				t.Fatalf("retrofit became a journal writer: %v", err)
			}
			writeInstallFixture(t, source, string(body)+" ", 0600)
			if out, err := f.run(t, args...); err == nil || !strings.Contains(out, "declaration differs") {
				t.Fatalf("changed declaration overwrote installation: %v %s", err, out)
			}
			assertInstallContents(t, filepath.Join(role.ControlDir, "installation.json"), string(body))
		})
	}
}

// origin: S8 internal review remaining-orchestration F5 (R5)
func TestRetrofitRegistrationRejectsDeclarationAliases(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	f := newRetrofitFixture(t)
	source := filepath.Join(t.TempDir(), "declaration.json")
	writeInstallFixture(t, source, "{}", 0600)
	alias := source + ".alias"
	if err := os.Symlink(source, alias); err != nil {
		t.Fatal(err)
	}
	out, err := f.run(t, "--hot-update-retrofit", "--hot-update-registration", alias)
	if err == nil || !strings.Contains(out, "regular declaration file") {
		t.Fatalf("declaration alias accepted: %v %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(f.root, "var/lib/tether/upgrade-control/installation.json")); !os.IsNotExist(err) {
		t.Fatalf("alias published a declaration: %v", err)
	}
}
