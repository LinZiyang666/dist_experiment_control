package p10_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

// config_preservation_test.go — a re-run of install.sh must not silently revert local edits.
//
// origin: prerelease audit deploy-release-docs/DRD-F1 + DRD-F5.
//
// A bare re-run rewrote broker.yaml, the Caddyfile, nats.d/nats.conf and the unit files
// unconditionally, and the docs suggested exactly that as the easy way to upgrade. So a
// re-run reverted the `broker.cluster` block that makes a node a cluster member, any
// auth_callout edits, and a hand-tuned Caddy site — and the node came back up looking
// perfectly healthy while no longer part of its cluster.
//
// Preserving unconditionally would be the opposite mistake: a release that has to
// correct a unit file (G1 #23 shipped `Restart=always` that way) relies on the rewrite
// to reach existing machines. So the file is KEPT, the new content is written beside it
// as `<file>.new`, and the operator is told.

func TestARerunKeepsAnEditedAgentConfigAndWritesTheNewOneBeside(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	args := []string{
		"--role", "agent",
		"--broker", "wss://broker.example.com:443",
		"--session", "lab",
		"--pin", "123456",
		"--nid", "lab-1",
		"--skip-download",
	}
	runInstall(t, home, args...)

	yaml := filepath.Join(home, ".tether", "agent", "lab", "agent.yaml")
	edited := "# an operator's local edit\nnid: lab-1\n"
	if err := os.WriteFile(yaml, []byte(edited), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runInstall(t, home, args...)

	body, err := os.ReadFile(yaml)
	if err != nil {
		t.Fatalf("agent.yaml disappeared: %v", err)
	}
	if string(body) != edited {
		t.Fatalf("a re-run OVERWROTE the operator's agent.yaml.\n\ngot:\n%s\nwant it untouched:\n%s\n\n"+
			"The docs point operators at a re-run as the way to upgrade. Overwriting means every "+
			"local setting is reverted by the act of upgrading, and nothing says so.",
			string(body), edited)
	}

	fresh, err := os.ReadFile(yaml + ".new")
	if err != nil {
		t.Fatalf("no agent.yaml.new written beside the kept file: %v.\n\n"+
			"Keeping the old file is only half of it: without the new content beside it the operator "+
			"has no way to see what this release would have changed.", err)
	}
	if !strings.Contains(string(fresh), "broker_url:") {
		t.Errorf("agent.yaml.new does not look like a generated config:\n%s", string(fresh))
	}
	if st, serr := os.Stat(yaml + ".new"); serr == nil && st.Mode().Perm() != 0o600 {
		t.Errorf("agent.yaml.new is mode %o; it can carry the same secrets as the real file and must "+
			"be 600", st.Mode().Perm())
	}

	if !strings.Contains(out, "EXISTING CONFIG KEPT") || !strings.Contains(out, yaml) {
		t.Errorf("the run did not TELL the operator it kept a file.\n\n"+
			"A `.new` file nobody is told about is a `.new` file nobody reads. Output was:\n%s", out)
	}
}

func TestForceConfigOverwritesDeliberately(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	args := []string{
		"--role", "agent",
		"--broker", "wss://broker.example.com:443",
		"--session", "lab",
		"--pin", "123456",
		"--nid", "lab-1",
		"--skip-download",
	}
	runInstall(t, home, args...)

	yaml := filepath.Join(home, ".tether", "agent", "lab", "agent.yaml")
	if err := os.WriteFile(yaml, []byte("# local\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runInstall(t, home, append(args, "--force-config")...)

	body, err := os.ReadFile(yaml)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "# local") {
		t.Fatal("--force-config did not overwrite.\n\n" +
			"The escape hatch has to work, or a release that must correct a config file cannot " +
			"reach machines that already have one — which is the failure the unconditional " +
			"rewrite was there to prevent.")
	}
	if !strings.Contains(string(body), "broker_url:") {
		t.Errorf("agent.yaml is not the generated content:\n%s", string(body))
	}
	if strings.Contains(out, "EXISTING CONFIG KEPT") {
		t.Error("--force-config still reported keeping files")
	}
}

// TestDryRunStatesTheConfigPolicy: a --dry-run that does not mention the policy is
// describing a different program than the one that runs.
func TestDryRunStatesTheConfigPolicy(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	base := []string{
		"--role", "agent",
		"--broker", "wss://broker.example.com:443",
		"--session", "lab", "--pin", "123456", "--nid", "lab-1",
	}

	// A CLEAN host has nothing to keep, and must not be told otherwise.
	// origin: prerelease audit round 2, K-F11 — the line used to be unconditional, so a
	// first install announced that files "would be KEPT" when there were none.
	clean := runInstall(t, home, append(append([]string{}, base...), "--dry-run")...)
	if strings.Contains(clean, "would be KEPT") {
		t.Errorf("a dry-run on a host with no config claimed one would be kept; got:\n%s", clean)
	}

	// Now give it something to keep.
	if err := os.MkdirAll(filepath.Join(home, ".tether", "agent", "lab"), 0o700); err != nil {
		t.Fatal(err)
	}
	yaml := filepath.Join(home, ".tether", "agent", "lab", "agent.yaml")
	if err := os.WriteFile(yaml, []byte("# local\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runInstall(t, home, append(append([]string{}, base...), "--dry-run")...)
	if !strings.Contains(out, "would be KEPT") {
		t.Errorf("--dry-run does not state what happens to an EXISTING config; got:\n%s", out)
	}

	forced := runInstall(t, home, append(append([]string{}, base...), "--dry-run", "--force-config")...)
	if !strings.Contains(forced, "would be OVERWRITTEN") {
		t.Errorf("--dry-run --force-config does not state that it would overwrite; got:\n%s", forced)
	}
}

// TestUninstallingOneAgentKeepsTheCtlIdentity is #48.
//
// ~/.tether is not the agent's directory: it holds keys/default.nk, this user's ctl
// private key, which cannot be regenerated and is the owner fingerprint on every
// session they created. uninstall_ctl deliberately does not touch it; uninstall_agent
// deleted the whole tree.
func TestUninstallingOneAgentKeepsTheCtlIdentity(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	runInstall(t, home,
		"--role", "agent",
		"--broker", "wss://broker.example.com:443",
		"--session", "lab", "--pin", "123456", "--nid", "lab-1",
		"--skip-download",
	)
	keys := filepath.Join(home, ".tether", "keys")
	if err := os.MkdirAll(keys, 0o700); err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(keys, "default.nk")
	if err := os.WriteFile(key, []byte("SU-not-a-real-seed\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	runInstall(t, home, "--role", "agent", "--uninstall", "--session", "lab")

	if _, err := os.Stat(key); err != nil {
		t.Fatalf("uninstalling ONE agent removed the ctl private key: %v.\n\n"+
			"That key cannot be regenerated. It is the owner fingerprint on every session this user "+
			"created, so losing it orphans all of them — not just this agent's. uninstall_ctl "+
			"deliberately does not touch this directory.", err)
	}
	if _, err := os.Stat(filepath.Join(home, ".tether", "agent", "lab")); err == nil {
		t.Error("the session directory that WAS asked for is still there")
	}
}

// origin: prerelease audit round 2, C7.
//
// THE ✔ BANNER MUST NOT CLAIM A FILE THIS RUN DID NOT WRITE.
//
// The kept-config report and the success banner were added by the same change and
// contradicted each other: the report said "this run did NOT overwrite the
// following" and, a few lines later, "✔ agent config: <path>" said it had. An
// operator who reads the ✔ — which is what a ✔ is for — walks away believing the
// release's new config is on disk when their previous one still is.
func TestTheSuccessBannerDoesNotClaimAKeptFileWasWritten(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	args := []string{
		"--role", "agent",
		"--broker", "wss://broker.example.com:443",
		"--session", "lab",
		"--pin", "123456",
		"--nid", "lab-1",
		"--skip-download",
	}
	// A FIRST run really does write it, and must say so with no caveat — a warning
	// printed on every install is a warning nobody reads.
	first := runInstall(t, home, args...)
	if strings.Contains(first, "NOT written by this run") {
		t.Errorf("a clean install printed the kept-config caveat.\n\nOutput was:\n%s", first)
	}

	yaml := filepath.Join(home, ".tether", "agent", "lab", "agent.yaml")
	if err := os.WriteFile(yaml, []byte("# an operator's local edit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := runInstall(t, home, args...)

	if !strings.Contains(out, "NOT written by this run") {
		t.Fatalf("a re-run that KEPT the operator's config still printed an unqualified success "+
			"banner.\n\nOutput was:\n%s\n\n"+
			"The banner and the kept-config report are in the same output contradicting each "+
			"other, and the ✔ is the half an operator trusts.", out)
	}
	// The caveat has to come AFTER the ✔ it qualifies — before it, it reads as being
	// about something else and the ✔ is still the last word.
	tick := strings.Index(out, "✔ agent config:")
	caveat := strings.Index(out, "NOT written by this run")
	if tick < 0 {
		t.Fatalf("the agent success banner is gone entirely:\n%s", out)
	}
	if caveat < tick {
		t.Errorf("the kept-config caveat is printed BEFORE the ✔ it qualifies (at %d vs %d).\n\n"+
			"That is the ordering K-F7 already fixed once for the report itself: an operator who "+
			"has reached a ✔ has stopped reading.", caveat, tick)
	}
}

// runBrokerInstall runs a REAL `--role broker` install into a redirected root.
//
// origin: prerelease audit round 2, G2 / K-F10 / CC-5. Everything above this line
// exercises `--role agent`, whose files land under $HOME. B4 — the BLOCKER these
// guards exist for — is the BROKER path: broker.yaml, the Caddyfile, nats.d/nats.conf
// and the three unit files, all at absolute system paths a test cannot write. So the
// blocker's own role had no coverage, and reverting the preservation on the two files
// B4 names left the entire suite green.
func runBrokerInstall(t *testing.T, root string, extra ...string) string {
	t.Helper()
	out, err := runBrokerInstallResult(t, root, extra...)
	if err != nil {
		t.Fatalf("broker install.sh failed: %v\n%s", err, out)
	}
	return out
}

func runBrokerInstallResult(t *testing.T, root string, extra ...string) (string, error) {
	t.Helper()
	// origin: hot-update S9 plan §6.4 / S9 internal review FIX-INST F1
	// Skip-download now validates the complete offline release. Supplying it
	// keeps these tests on the real config writer instead of bypassing admission.
	supply := brokerOfflineSupply(t, nil)
	t.Cleanup(func() {
		// Immutable slots remain read-only during the test, then become removable.
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				return os.Chmod(path, 0o700)
			}
			return err
		})
	})
	args := append([]string{
		"--role", "broker",
		"--domain", "b.example.com",
		"--acme-email", "ops@example.com",
		"--skip-download",
		"--no-enable",
		"--version", "v0.0.0-installer-fixture",
	}, extra...)
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{scriptPath(t)}, args...)...)
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "HOME=") && !strings.HasPrefix(env, "TETHER_") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, "HOME="+t.TempDir(), "TETHER_INSTALL_ROOT="+root, "TETHER_INFRA_SUPPORT_DIR="+supply)
	out, err := testsupervisor.CombinedOutput(cmd)
	return string(out), err
}

// brokerOfflineSupply provides byte-parser fixtures, never runtime acceptance.
// A real executable supplied by the offline test also makes candidate execution
// observable. Each name has different bytes so immutable content slots are distinct.
// origin: hot-update S9 plan §6.4 / §6.5 / S9 internal review FIX-INST F1–F4
func brokerOfflineSupply(t *testing.T, candidate []byte) string {
	t.Helper()
	dir := t.TempDir()
	if candidate == nil {
		candidate = make([]byte, 64)
		copy(candidate, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
		switch runtime.GOARCH {
		case "amd64":
			candidate[18] = 0x3e
		case "arm64":
			candidate[18] = 0xb7
		default:
			t.Fatalf("unsupported offline fixture architecture: %s", runtime.GOARCH)
		}
	}
	manifest := "format 1\nrelease v0.0.0-installer-fixture\ngoos linux\ngoarch " + runtime.GOARCH +
		"\nprotocol 1\nprevious-reader 1\nsource-sha256 " + strings.Repeat("1", 64) + "\n"
	sums := ""
	for _, name := range []string{"tether", "nats-server", "caddy", "tether-infra-helper", "tether-infra-launch", "infra-catalog.json"} {
		data := append(append([]byte{}, candidate...), []byte("\n"+name)...)
		switch name {
		case "tether":
			data = candidate // --bin must match this exact checksum.
		case "infra-catalog.json":
			data = []byte("{\"fixture\":true}\n")
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o755); err != nil {
			t.Fatal(err)
		}
		if name == "tether" || name == "nats-server" || name == "caddy" {
			sums += fmt.Sprintf("%x %s\n", sha256.Sum256(data), name)
		} else {
			manifest += fmt.Sprintf("sha256 %x %s\n", sha256.Sum256(data), name)
		}
	}
	writeInstallFixture(t, filepath.Join(dir, "infra-support.manifest"), manifest, 0o644)
	writeInstallFixture(t, filepath.Join(dir, "broker-binaries.sha256"), sums, 0o644)
	return dir
}

// origin: external review 2026-09-03, redirected-install host-systemd finding.
//
// TETHER_INSTALL_ROOT is the real broker-install test seam: its banner says the
// broker is NOT installed on this host, and it already suppresses useradd because
// that would mutate host state. systemctl is the same class of side effect. A dry
// run is used here as a safe trace of the commands the corresponding real run would
// execute; on a live-systemd machine those commands otherwise target the host, not
// the redirected root.
func TestRedirectedBrokerLifecycleDoesNotTargetHostSystemd(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	root := t.TempDir()
	for _, tc := range []struct {
		name string
		args []string
	}{
		{
			name: "install",
			args: []string{"--role", "broker", "--domain", "b.example.com",
				"--acme-email", "ops@example.com", "--skip-download", "--dry-run"},
		},
		{
			name: "uninstall",
			args: []string{"--role", "broker", "--uninstall", "--dry-run"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("bash", append([]string{scriptPath(t)}, tc.args...)...)
			cmd.Env = append(os.Environ(), "HOME="+t.TempDir(), "TETHER_INSTALL_ROOT="+root)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("redirected broker %s dry-run failed: %v\n%s", tc.name, err, out)
			}
			if strings.Contains(string(out), "(dry-run) systemctl") {
				t.Fatalf("redirected broker %s still targets host systemctl; TETHER_INSTALL_ROOT "+
					"redirects files but not service-manager state:\n%s", tc.name, out)
			}
		})
	}
}

// brokerConfigs are the files B4 is actually about. Every one of them carries
// operator state that a bare re-run used to revert: broker.yaml holds the
// `broker.cluster` block that makes a node a cluster member, nats.d/nats.conf holds
// auth_callout edits, the Caddyfile holds a hand-tuned site, and the units hold
// whatever an operator has had to add to keep the host alive.
func brokerConfigs(root string) map[string]string {
	return map[string]string{
		"broker.yaml": filepath.Join(root, "etc", "tether", "broker.yaml"),
		"Caddyfile":   filepath.Join(root, "etc", "tether", "Caddyfile"),
		"nats.conf":   filepath.Join(root, "etc", "tether", "nats.d", "nats.conf"),
		"unit":        filepath.Join(root, "etc", "systemd", "system", "tether-broker.service"),
		"nats-unit":   filepath.Join(root, "etc", "systemd", "system", "nats-server.service"),
		"caddy-unit":  filepath.Join(root, "etc", "systemd", "system", "caddy.service"),
	}
}

// origin: prerelease audit round 2, G2 / K-F10.
//
// THE BLOCKER'S OWN ROLE. A re-run must keep every edited broker file and write the
// release's version beside it, exactly as it does for the agent — and the reason
// this is worth its own test rather than trusting the shared helper is that
// `config_dest` has to be reached at FOUR separate write sites on this path, and a
// site that simply does not call it is invisible to any test of the helper itself.
// origin: S9 internal review FIX-INST F1
func TestARerunKeepsEveryEditedBrokerFile(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	root := t.TempDir()
	runBrokerInstall(t, root)

	edits := map[string]string{}
	for name, path := range brokerConfigs(root) {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("the first install did not write %s (%s): %v", name, path, err)
		}
		edit := "# an operator's local edit to " + name + "\n"
		if err := os.WriteFile(path, []byte(edit), 0o600); err != nil {
			t.Fatal(err)
		}
		edits[name] = edit
	}

	out := runBrokerInstall(t, root)

	for name, path := range brokerConfigs(root) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("%s disappeared on the re-run: %v", name, err)
			continue
		}
		if string(body) != edits[name] {
			t.Errorf("a re-run OVERWROTE the operator's %s.\n\ngot:\n%s\nwant it untouched:\n%s\n\n"+
				"This is B4: the docs point operators at a re-run as the way to upgrade, so every "+
				"local setting — including the broker.cluster block that makes this node a cluster "+
				"member — is reverted by the act of upgrading, and the node comes back looking "+
				"healthy while no longer part of its cluster.", name, string(body), edits[name])
		}
		if _, err := os.Stat(path + ".new"); err != nil {
			t.Errorf("no %s.new written beside the kept file: %v.\n\n"+
				"Keeping the old file is only half of it — without the new content beside it the "+
				"operator cannot see what this release would have changed.", name, err)
		}
	}
	if !strings.Contains(out, "EXISTING CONFIG KEPT") {
		t.Errorf("the broker re-run did not TELL the operator it kept files.\n\nOutput was:\n%s", out)
	}
	if !strings.Contains(out, "NOT written by this run") {
		t.Errorf("the broker ✔ banner claimed files were installed that this run kept (round 2, C7)."+
			"\n\nOutput was:\n%s", out)
	}
}

// origin: prerelease audit round 2, G2 / K-F10.
//
// The other half, on the broker path: --force-config is the deliberate override, and
// it has to actually override — a preservation that cannot be switched off is the
// opposite mistake, since a release that must correct a unit file (G1 #23 shipped
// `Restart=always` that way) reaches existing machines only through the rewrite.
// origin: S9 internal review FIX-INST F2
func TestForceConfigOverwritesEveryBrokerFile(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	root := t.TempDir()
	runBrokerInstall(t, root)
	generated := map[string]string{}
	for name, path := range brokerConfigs(root) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		generated[name] = string(body)
		if err := os.WriteFile(path, []byte("# local "+name+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	runBrokerInstall(t, root, "--force-config")

	for name, path := range brokerConfigs(root) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.HasPrefix(string(body), "# local ") {
			t.Errorf("--force-config did not overwrite %s.\n\n"+
				"Preservation that cannot be switched off means a release which has to CORRECT a "+
				"config or unit file can never reach an existing machine.", name)
		}
		if string(body) != generated[name] {
			t.Errorf("--force-config did not publish the generated %s content", name)
		}
	}
	// origin: hot-update S9 plan §6.4 / S9 internal review FIX-INST F2
	// The cold override still works, but an unknown registered authority is
	// never permission to overwrite the active host's files.
	policy := filepath.Join(root, "var/lib/tether-infra/policy.json")
	writeInstallFixture(t, policy, "unknown authority\n", 0o600)
	out, err := runBrokerInstallResult(t, root, "--force-config")
	if err == nil || !strings.Contains(out, "infra_busy") {
		t.Fatalf("--force-config bypassed installed authority: %v\n%s", err, out)
	}
	assertInstallContents(t, policy, "unknown authority\n")
	for name, path := range brokerConfigs(root) {
		assertInstallContents(t, path, generated[name])
	}
}

// origin: prerelease audit round 2, K-F9.
//
// A `.new` SIDECAR MUST NOT OUTLIVE ITS PURPOSE.
//
// Its whole job is to hold what this release WOULD have written, so an operator can
// diff it against the config they kept. Two events end that job and neither used to
// clean up:
//
//   - --force-config takes the new content as-is, so the sidecar now duplicates the
//     live file. The report's closing line — "delete the ones you have decided
//     against, or the next run's report is the only thing telling them apart from the
//     current config" — is then false in both halves: they are no longer different,
//     and the next run keeps nothing so it reports nothing.
//   - --uninstall removes the config; a sidecar for a file that is gone is an orphan
//     no later run will ever mention.
//
// origin: S9 internal review FIX-INST F3
func TestSupersededAndOrphanedNewFilesAreCleanedUp(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	root := t.TempDir()
	runBrokerInstall(t, root)

	// A kept re-run, which is what mints the sidecars.
	for _, path := range brokerConfigs(root) {
		if err := os.WriteFile(path, []byte("# local\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runBrokerInstall(t, root)
	for name, path := range brokerConfigs(root) {
		if _, err := os.Stat(path + ".new"); err != nil {
			t.Fatalf("premise broken: the kept re-run wrote no %s.new: %v", name, err)
		}
	}

	// --force-config: the sidecar's content becomes the live file.
	runBrokerInstall(t, root, "--force-config")
	for name, path := range brokerConfigs(root) {
		if _, err := os.Stat(path + ".new"); err == nil {
			t.Errorf("%s.new survived --force-config.\n\n"+
				"It now holds a byte-identical copy of the live config, so it is no longer a diff "+
				"against anything — and because this run kept nothing, the kept-config report that "+
				"is supposed to be the only thing distinguishing them is not printed at all.", name)
		}
	}

	// And an uninstall must not leave sidecars behind. Re-mint them first.
	for _, path := range brokerConfigs(root) {
		if err := os.WriteFile(path, []byte("# local again\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runBrokerInstall(t, root)
	lock := filepath.Join(root, "var/lib/tether-infra/journal.lock")
	beforeLock, err := os.Stat(lock)
	if err != nil {
		t.Fatal(err)
	}
	runBrokerInstall(t, root, "--uninstall")
	// origin: hot-update S9 plan §6.4 / S9 internal review FIX-INST F3
	// Uninstall now requires content ownership: edited configs survive while
	// generated sidecars are retired. The old orphan check still covers every .new.
	for _, path := range brokerConfigs(root) {
		assertInstallContents(t, path, "# local again\n")
	}
	afterLock, err := os.Stat(lock)
	if err != nil || !os.SameFile(beforeLock, afterLock) {
		t.Fatalf("uninstall replaced or removed the permanent journal lock: %v", err)
	}

	var leftovers []string
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if !d.IsDir() && strings.HasSuffix(p, ".new") {
			rel, _ := filepath.Rel(root, p)
			leftovers = append(leftovers, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) > 0 {
		t.Errorf("uninstall left %d orphaned .new file(s) behind: %v.\n\n"+
			"The config they were a diff against is gone, so nothing will ever mention them again.",
			len(leftovers), leftovers)
	}
}

// origin: prerelease audit round 2, K-F9 / G2.
//
// THE UNINSTALL PATH MUST RESOLVE ITS PATHS THE SAME WAY THE INSTALL PATH DOES.
//
// uninstall_broker hardcoded four absolute paths in one `rm -rf`, which was merely
// redundant until TETHER_INSTALL_ROOT existed — and then it was a live hazard: a
// redirected uninstall would have deleted the REAL /var/lib/tether, i.e. the SQLite
// state, the raft data dir and the secrets that cannot be regenerated, on a host
// whose matching install had written nothing there.
//
// The assertion is on the SCRIPT, because the only way to observe the bug by running
// it is to actually destroy a real installation.
func TestUninstallResolvesPathsThroughTheSameVariablesAsInstall(t *testing.T) {
	body, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	src := string(body)
	uninstall := src[strings.Index(src, "uninstall_broker() {"):]
	if i := strings.Index(uninstall, "\n}\n"); i > 0 {
		uninstall = uninstall[:i]
	}
	for _, literal := range []string{
		"rm -rf /etc/tether",
		"/var/lib/tether /var/log/tether",
		"rm -f /etc/systemd/system/",
	} {
		if strings.Contains(uninstall, literal) {
			t.Errorf("uninstall_broker still removes a HARDCODED system path (%q).\n\n"+
				"install_broker resolves the same paths through $ETC_DIR / $LIB_DIR / $SYSTEMD_DIR, "+
				"which honour TETHER_INSTALL_ROOT. An uninstall that does not is one env var away "+
				"from deleting a real broker's unregenerable secrets.", literal)
		}
	}
}
