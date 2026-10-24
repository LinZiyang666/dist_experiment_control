//go:build linux && amd64 && infraprivilege_integration

package infraprivilege

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/test/infraharness"
)

// The installer consumes only the current archive. Its read-only/never-start
// oracle does not need a live privileged runner or a historical helper. Keep
// those requirements on the runtime tests, where their receipts are observed.
// Explicit complete manifests still take the original strict admission path.
// origin: hot-update S9 plan §6.4 / §9.1 / T54; S9 INFRA-SUITE-SUPPLIES
func installerSupply(t *testing.T) (privilegeSupply, string) {
	t.Helper()
	if os.Getenv("TETHER_S9_PRIVILEGE_SUPPLY") != "" {
		return loadSupply(t)
	}
	root, repo := t.TempDir(), repository(t)
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	source := infraharness.StockSource(repo, "")
	for _, row := range []struct {
		kind    c.ArtifactKind
		version string
	}{{c.NATSBinary, "2.14.6"}, {c.CaddyBinary, "2.7.6"}} {
		d, err := controlbudget.After(30 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := infraharness.PrepareStockArtifact(root, source, row.kind, row.version, d, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Resolve the already-selected toolchain once, then build offline. A new HOME
	// isolates all installer/product paths; cached Go inputs remain read-only
	// dependencies and no installed product executable is ever invoked.
	cmd := exec.CommandContext(t.Context(), "go", "env", "-json", "GOROOT", "GOPATH", "GOCACHE")
	cmd.Dir = repo
	var toolchain struct{ GOROOT, GOPATH, GOCACHE string }
	output, rc := runCommand(t, cmd)
	if rc != 0 || json.Unmarshal(output, &toolchain) != nil {
		t.Fatalf("SETUP-RED: local Go toolchain unavailable: %s", output)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	cmd = exec.CommandContext(ctx, "/usr/bin/python3", "-I", "-B", "-c", currentInstallerArchive, repo, root, source)
	cmd.Dir = repo
	cmd.Env = []string{"PATH=" + filepath.Join(toolchain.GOROOT, "bin") + ":/usr/bin:/bin", "HOME=" + root,
		"GOPATH=" + toolchain.GOPATH, "GOCACHE=" + toolchain.GOCACHE, "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off",
		"GOFLAGS=" + os.Getenv("GOFLAGS"), "SIM_VERSION=v0.0.0-privilege", "PYTHONDONTWRITEBYTECODE=1"}
	output, rc = runCommand(t, cmd)
	if rc != 0 {
		t.Fatalf("SETUP-RED: current workspace installer archive build failed: %s", output)
	}
	var supply privilegeSupply
	if err := strictJSON(readFile(t, filepath.Join(root, "installer-supply.json")), &supply); err != nil {
		t.Fatal(err)
	}
	if supply.Version != 1 || len(supply.Releases) != 1 {
		t.Fatal("installer supply must contain exactly the current release")
	}
	for _, row := range []suppliedFile{supply.Releases[0].Archive, supply.Releases[0].Checksums, supply.Releases[0].Source, supply.Installer} {
		verifySuppliedFile(t, root, row)
	}
	if !bytes.Equal(readFile(t, safeSupplyPath(t, root, supply.Installer.Path)), readFile(t, filepath.Join(repo, "scripts/install.sh"))) {
		t.Fatal("installer supply lost current source binding")
	}
	return supply, root
}

// Immutable installed slots have read-only directories. Restore traversal/write
// access only during teardown of the private fixture, after every metadata and
// never-start assertion has run; TempDir's ordinary unlink cannot remove them.
func makeInstallerTreeRemovable(t *testing.T, root string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0700)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// origin: S9 internal review INFRA-SUITE-SUPPLIES F4
func TestInfraInstallerCleanupRemovesImmutableSlots(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "immutable-slot")
	if err := os.Mkdir(slot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(slot, "manifest"), []byte("private fixture"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(slot, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(slot, 0700) })
	makeInstallerTreeRemovable(t, root)
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("immutable fixture slot prevented teardown: %v", err)
	}
}

// Use P26's actual packaging and the release manifest producer shared with
// GoReleaser. Do not fabricate a previous generation or a successful runner.
const currentInstallerArchive = `
import hashlib, importlib.util, json, shutil, sys
from pathlib import Path
repo, root, source = map(Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("offline_release_supply", repo / "test/infraharness/release_supply.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
release = module.current(repo, root)
catalog = repo / "internal/infraartifact/testdata/catalog.json"
def pin(path):
    return {"Path": str(path.relative_to(root)), "SHA256": hashlib.sha256(path.read_bytes()).hexdigest()}
installer = root / "install.sh"
shutil.copyfile(repo / "scripts/install.sh", installer)
downloads = []
for entry in json.loads(catalog.read_bytes()):
    if (entry["Kind"], entry["Version"]) not in ((1,"2.14.6"),(2,"2.7.6")):
        continue
    archive_in, sums_in = source / entry["ArchiveName"], None
    component = "nats-server" if entry["Kind"] == 1 else "caddy"
    for name in (component+"-"+entry["Version"]+"-checksums.txt", str(entry["Kind"])+"-"+entry["Version"]+".checksums", Path(entry["ChecksumURL"]).name):
        if (source / name).exists():
            sums_in = source / name
            break
    if sums_in is None:
        raise ValueError("pre-supplied checksum list missing")
    for url, incoming, expected in ((entry["ArchiveURL"], archive_in, entry["ArchiveSHA256"]), (entry["ChecksumURL"], sums_in, entry["ChecksumSHA256"])):
        destination = root / (component+"-"+incoming.name)
        shutil.copyfile(incoming, destination)
        if pin(destination)["SHA256"] != expected:
            raise ValueError("official installer input changed after verification")
        downloads.append({"URL": url, "File": pin(destination)})
empty = {"Path":"", "SHA256":""}
value = {"Version":1,"Downloads":downloads,"Releases":[release],"Runner":empty,"RunnerSource":empty,"Installer":pin(installer),
    "ImageContext":"","ImageRoot":"","ImageReceipt":empty,"SupportCatalog":empty}
(root / "installer-supply.json").write_text(json.dumps(value,separators=(",",":")))
`
