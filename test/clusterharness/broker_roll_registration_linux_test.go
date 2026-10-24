//go:build linux

package clusterharness

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
)

// All installation prerequisites precede the first daemon and business
// socket. The test never retrofits a running fixture to make a cut admissible.
func registerN3Roll(t *testing.T, p *n3MaintenanceProcess) {
	t.Helper()
	config := filepath.Join(p.dir, "broker.yaml")
	if err := os.WriteFile(config, []byte("broker:\n  storage:\n    db: "+filepath.Join(p.dir, "tether.db")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	role := upgradecli.BrokerBootRole(p.dir, config, "", uint32(os.Geteuid()))
	if err := os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(role.ControlDir, 0700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(p.dir, "n3-product")
	if err := os.Chmod(canonical, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: role.ControlDir, DeploymentID: "n3-transaction-roll", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "broker-" + p.id}}}); err != nil {
		t.Fatal(err)
	}
}
func n3RollPIN(t *testing.T) string {
	t.Helper()
	pin, err := productPINHash()
	if err != nil {
		t.Fatal(err)
	}
	return pin
}
