//go:build linux && amd64 && infraprivilege_integration

package infraprivilege

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The scanner and installed-target verifier are separate commands. Both must
// pass: a correct source snapshot alone cannot certify the staged target bytes.
// Bind the excluded self-copy too, without introducing a recursive self-hash.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F4
func runImageSupplyGuard(t *testing.T, context, receipt, installed string) ([]byte, int) {
	t.Helper()
	guard := filepath.Join(repository(t), "test/simcluster/lib/infra-image-manifest.py")
	copyReceipt, copyErr := os.ReadFile(filepath.Join(context, "vendor/infra-image-inputs.json"))
	pinnedReceipt, receiptErr := os.ReadFile(receipt)
	if copyErr != nil || receiptErr != nil || !bytes.Equal(copyReceipt, pinnedReceipt) {
		return []byte("image receipt COPY differs from the supplied receipt"), 1
	}
	output, rc := runCommand(t, cleanCommand(t, "python3", guard, "check", context, receipt))
	if rc != 0 {
		return output, 1
	}
	output, rc = runCommand(t, cleanCommand(t, "python3", guard, "targets", receipt,
		"--root", installed, "--dockerfile", filepath.Join(installed, "opt/sim/Dockerfile")))
	if rc != 0 {
		return output, 1
	}
	return output, 0
}

// This is a real invocation of the source/target guard on a private filesystem,
// without building an image or pretending that the payload is a product binary.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F4
func TestInfraImageSupplyUsesGuardProtocol(t *testing.T) {
	root := t.TempDir()
	cmd := cleanCommand(t, "/usr/bin/python3", "-I", "-B", "-c", imageGuardFixture, repository(t), root)
	if output, rc := runCommand(t, cmd); rc != 0 {
		t.Fatalf("image guard fixture failed rc=%d: %s", rc, output)
	}
	context, installed := filepath.Join(root, "context"), filepath.Join(root, "installed")
	receipt := filepath.Join(root, "receipt.json")
	check := func(want int) {
		t.Helper()
		if output, rc := runImageSupplyGuard(t, context, receipt, installed); rc != want {
			t.Fatalf("image guard rc=%d want=%d: %s", rc, want, output)
		}
	}
	check(0)
	for _, path := range []string{filepath.Join(installed, "opt/sim/payload"), filepath.Join(context, "vendor/payload"), filepath.Join(context, "vendor/infra-image-inputs.json")} {
		before := readFile(t, path)
		if err := os.WriteFile(path, append(bytes.Clone(before), 'x'), 0600); err != nil {
			t.Fatal(err)
		}
		check(1)
		if err := os.WriteFile(path, before, 0600); err != nil {
			t.Fatal(err)
		}
		check(0)
	}
}

const imageGuardFixture = `
import importlib.util, json, shutil, sys
from pathlib import Path
repo, root = map(Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("image_guard", repo / "test/simcluster/lib/infra-image-manifest.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
context, installed = root / "context", root / "installed"
(context / "vendor/infra").mkdir(parents=True)
for name in guard.BUILD_INPUTS:
    target = context / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(repo / "test/simcluster" / name, target)
(context / "vendor/infra/build-source.json").write_text(json.dumps({"schema_version":1,"inputs":[{"path":"go.mod"}]}))
(context / "vendor/payload").write_bytes(b"guard protocol payload")
(context / "Dockerfile").write_text("COPY vendor/payload /opt/sim/payload\nCOPY vendor/infra /opt/sim/infra\nCOPY Dockerfile /opt/sim/Dockerfile\nCOPY vendor/infra-image-inputs.json /opt/sim/infra-image-inputs.json\n")
receipt = guard.snapshot(context)
guard.write(context / "vendor/infra-image-inputs.json", receipt)
shutil.copyfile(context / "vendor/infra-image-inputs.json", root / "receipt.json")
for row in receipt["copies"]:
    target = installed / row["target"].lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    if row["type"] == "directory":
        target.mkdir(exist_ok=True)
    else:
        shutil.copyfile(context / row["source"], target)
    target.chmod(row["mode"])
spec = importlib.util.spec_from_file_location("release_supply", repo / "test/infraharness/release_supply.py")
producer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(producer)
produced = root / "produced"
produced.mkdir()
producer.image(repo, context, produced)
`
