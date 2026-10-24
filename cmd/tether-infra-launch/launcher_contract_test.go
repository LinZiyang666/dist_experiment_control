//go:build linux && amd64

// Copy into cmd/tether-infra-launch. No subprocess or host effect is required.
package main

import (
	"github.com/LinZiyang666/tether/internal/testharness"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infraunit"
)

// origin: S9 internal review rv-infra F02
func TestManagedUnitLauncherArgumentsRoundTrip(t *testing.T) {
	for _, component := range []c.Component{c.NATS, c.Caddy} {
		body, err := infraunit.ManagedUnitDropIn(component)
		if err != nil {
			t.Fatal(err)
		}
		var args []string
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "ExecStart=/") {
				args = strings.Fields(strings.TrimPrefix(line, "ExecStart="))[1:]
			}
		}
		got, err := parseComponent(args)
		if err != nil || got != component {
			t.Errorf("managed unit component %d produces rejected argv %q: component=%d err=%v", component, args, got, err)
		}
	}
}

// origin: S9 internal review rv-infra F02
func TestInstallerLauncherArgumentsMatchManagedUnits(t *testing.T) {
	source, err := os.ReadFile("../../scripts/install.sh")
	if err != nil {
		t.Fatal(err)
	}
	_, body, ok := strings.Cut(string(source), "write_infra_files() {\n")
	if !ok {
		t.Fatal("installer renderer missing")
	}
	body, _, ok = strings.Cut(body, "\n}\n\ninstall_broker()")
	if !ok {
		t.Fatal("installer renderer boundary missing")
	}
	root := t.TempDir()
	script := `set -eu
DRY_RUN=0
INFRA_ETC_DIR="$1"
INFRA_RUN_DIR="$1/run"
LIBEXEC_DIR=/usr/local/libexec
INFRA_MAX_CONNECTIONS=4
INFRA_WORKER_LIFETIME_S=15
INFRA_WORKER_STOP_S=2
infra_safe_path() { :; }
config_dest() { CONFIG_DEST="$1"; }
record_owned_file() { :; }
publish_config() { chmod "$1" "$CONFIG_DEST"; }
` + body
	cmd := exec.Command("sh", "-c", script, "installer-render", root)
	if output, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("render: %v %s", err, output)
	}
	for _, item := range []struct {
		name      string
		component c.Component
	}{{"nats-server", c.NATS}, {"caddy", c.Caddy}} {
		actual, err := os.ReadFile(filepath.Join(root, "templates", item.name+".service.d", "90-tether-infra.conf"))
		if err != nil {
			t.Fatal(err)
		}
		want, err := infraunit.ManagedUnitDropIn(item.component)
		if err != nil || string(actual) != string(want) {
			t.Fatalf("installer and managed unit differ for %s: %v", item.name, err)
		}
		for _, line := range strings.Split(string(actual), "\n") {
			if strings.HasPrefix(line, "ExecStart=/") {
				got, err := parseComponent(strings.Fields(strings.TrimPrefix(line, "ExecStart="))[1:])
				if err != nil || got != item.component {
					t.Fatalf("installed argv rejected: %s %v", line, err)
				}
			}
		}
	}
}
