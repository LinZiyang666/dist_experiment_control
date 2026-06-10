package p10_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func repoRootFromInstallScript(t *testing.T) string {
	t.Helper()
	return filepath.Dir(filepath.Dir(scriptPath(t)))
}

func TestReviewCtlInstallDefaultsToCtl(t *testing.T) {
	out := runInstall(t, t.TempDir(),
		"--dry-run",
		"--skip-download",
	)
	if !strings.Contains(out, "role=ctl") {
		t.Fatalf("install.sh without --role should take the K.2 ctl default; got:\n%s", out)
	}
}

func TestReviewAgentInstallStartPathUsesConfiguredBroker(t *testing.T) {
	skipIfAgentOrBrokerUnsupported(t)
	home := t.TempDir()
	brokerURL := "wss://broker.example.com:443"
	out := runInstall(t, home,
		"--role", "agent",
		"--broker", brokerURL,
		"--session", "lab",
		"--pin", "123456",
		"--nid", "lab-1",
		"--skip-download",
	)

	commandCarriesBroker := strings.Contains(out, "--nats-url "+brokerURL) ||
		strings.Contains(out, "--nats-url="+brokerURL)
	if commandCarriesBroker {
		return
	}

	agentSource, err := os.ReadFile(filepath.Join(repoRootFromInstallScript(t), "cmd", "tether", "agent.go"))
	if err != nil {
		t.Fatal(err)
	}
	agentLoadsInstalledConfig := strings.Contains(string(agentSource), "agent.yaml") &&
		strings.Contains(string(agentSource), "broker_url")
	if agentLoadsInstalledConfig {
		return
	}

	t.Fatalf("agent install records %q in agent.yaml, but neither the printed start command carries --nats-url nor cmd/tether agent loads agent.yaml; output:\n%s",
		brokerURL, out)
}

func TestReviewBrokerInstallPreparesSidecarsAndWritableRuntimeDirs(t *testing.T) {
	body, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	installsNATS := strings.Contains(script, "github.com/nats-io") ||
		strings.Contains(script, "nats-server-v") ||
		strings.Contains(script, "nats-server_")
	if !installsNATS {
		t.Error("broker install must download/install nats-server, not only write a unit that points at /usr/local/bin/nats-server")
	}

	installsCaddy := strings.Contains(script, "github.com/caddyserver") ||
		strings.Contains(script, "caddy_") ||
		strings.Contains(script, "caddy-v")
	if !installsCaddy {
		t.Error("broker install must download/install caddy, not only write a unit that points at /usr/local/bin/caddy")
	}

	setsTetherOwnership := strings.Contains(script, "chown") ||
		strings.Contains(script, "install -d -o tether") ||
		strings.Contains(script, "install -o tether")
	if !setsTetherOwnership {
		t.Error("broker install must make /var/lib/tether, /var/log/tether, and /var/run/tether writable by the tether service user")
	}
}

func TestReviewUpgradeRestartsAfterSuccessfulReplacement(t *testing.T) {
	root := repoRootFromInstallScript(t)
	agentCmd, err := os.ReadFile(filepath.Join(root, "cmd", "tether", "agent.go"))
	if err != nil {
		t.Fatal(err)
	}
	upgrade, err := os.ReadFile(filepath.Join(root, "internal", "agent", "upgrade.go"))
	if err != nil {
		t.Fatal(err)
	}

	unitRestartsOnCleanExit := strings.Contains(string(agentCmd), "Restart=always") ||
		strings.Contains(string(agentCmd), "Restart=on-success")
	upgradeExitsAsFailure := strings.Contains(string(upgrade), "os.Exit(1)") ||
		strings.Contains(string(upgrade), "os.Exit(2)")
	upgradeLaunchesReplacement := strings.Contains(string(upgrade), `exec.Command("systemd-run"`) ||
		strings.Contains(string(upgrade), `exec.Command("nohup"`) ||
		strings.Contains(string(upgrade), "syscall.Exec") ||
		strings.Contains(string(upgrade), "exec.Command(exePath")

	if !unitRestartsOnCleanExit && !upgradeExitsAsFailure && !upgradeLaunchesReplacement {
		t.Fatal("successful node upgrade replaces the binary then exits 0, but the generated user service only restarts on failure and the upgrade path does not launch the replacement")
	}
}

func TestReviewBrokerSidecarUnitsUseInstallPrefix(t *testing.T) {
	body, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)

	if !strings.Contains(script, "install_nats_server \"$BIN_DIR\"") ||
		!strings.Contains(script, "install_caddy \"$BIN_DIR\"") {
		t.Fatal("test assumption failed: broker sidecars are not installed through BIN_DIR")
	}
	if strings.Contains(script, "ExecStart=/usr/local/bin/nats-server") {
		t.Error("nats-server unit is hard-coded to /usr/local/bin even though --prefix installs the sidecar into BIN_DIR")
	}
	if strings.Contains(script, "ExecStart=/usr/local/bin/caddy") {
		t.Error("caddy unit is hard-coded to /usr/local/bin even though --prefix installs the sidecar into BIN_DIR")
	}
}
