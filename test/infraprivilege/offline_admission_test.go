//go:build linux && amd64 && infraprivilege_integration

package infraprivilege

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/test/infraharness"
)

// Invalid offline inputs must fail before any installation directory or lock
// exists, even when admission gets past the complete supporting release.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F3 / hot-update S9 plan §6.4 / T54
func TestInfraOfflineAdmissionRejectsBeforeEffects(t *testing.T) {
	supply, root := installerSupply(t)
	release := supply.Releases[len(supply.Releases)-1]
	members := archiveMembers(t, safeSupplyPath(t, root, release.Archive.Path))
	inputs := make(map[string][]byte)
	for _, name := range []string{"tether", "tether-infra-helper", "tether-infra-launch", "infra-catalog.json", "infra-support.manifest"} {
		inputs[name] = members[name]
		if len(inputs[name]) == 0 {
			t.Fatalf("SETUP-RED: genuine current archive missing %s", name)
		}
	}
	for _, row := range []struct {
		kind    c.ArtifactKind
		name    string
		version string
	}{{c.NATSBinary, "nats-server", "2.14.6"}, {c.CaddyBinary, "caddy", "2.7.6"}} {
		d, err := controlbudget.After(30 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		artifact, err := infraharness.PrepareStockArtifact(t.TempDir(), infraharness.StockSource(repository(t), ""), row.kind, row.version, d, nil)
		if err != nil {
			t.Fatal(err)
		}
		inputs[row.name] = readFile(t, artifact.Binary)
	}
	var sums strings.Builder
	for _, name := range []string{"tether", "nats-server", "caddy"} {
		fmt.Fprintf(&sums, "%x  %s\n", sha256.Sum256(inputs[name]), name)
	}
	inputs["broker-binaries.sha256"] = []byte(sums.String())
	for _, defect := range []string{"missing-support", "wrong-release", "missing-checksums", "duplicate-checksum", "missing-binary", "wrong-elf", "wrong-hash"} {
		t.Run(defect, func(t *testing.T) {
			s := newInstallerSandbox(t)
			source := filepath.Join(s.downloads, "offline")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			for name, original := range inputs {
				data := original
				switch {
				case defect == "missing-support" && name == "infra-support.manifest",
					defect == "missing-checksums" && name == "broker-binaries.sha256",
					defect == "missing-binary" && name == "caddy":
					continue
				case defect == "wrong-release" && name == "infra-support.manifest":
					data = []byte(strings.ReplaceAll(string(data), release.Version, "v0.0.0-foreign"))
				case defect == "duplicate-checksum" && name == "broker-binaries.sha256":
					data = []byte(string(data) + fmt.Sprintf("%x  tether\n", sha256.Sum256(inputs["tether"])))
				case defect == "wrong-elf" && name == "caddy":
					data = []byte("invalid platform")
				case defect == "wrong-hash" && name == "caddy":
					data = append(append([]byte(nil), original...), 1)
				}
				if err := os.WriteFile(filepath.Join(source, name), data, 0755); err != nil {
					t.Fatal(err)
				}
			}
			roots := []string{s.root, s.home, s.tmp, s.downloads}
			before := make([]map[string]installedFile, len(roots))
			for i, path := range roots {
				before[i] = treeManifest(t, path)
			}
			cmd := s.command(t, "broker", "--version", release.Version, "--skip-download")
			cmd.Env = append(cmd.Env, "TETHER_INFRA_SUPPORT_DIR="+source)
			output, rc := runCommand(t, cmd)
			if rc == 0 {
				t.Fatalf("invalid offline supply was accepted: %s", output)
			}
			for i, path := range roots {
				assertManifestEqual(t, path, before[i], treeManifest(t, path))
			}
			if len(readFile(t, s.audit)) != 0 {
				t.Fatal("offline rejection attempted a process/kernel/download effect")
			}
		})
	}
}
