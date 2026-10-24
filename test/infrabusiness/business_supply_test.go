//go:build linux && amd64 && infrabusiness_integration

package infrabusiness_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/test/infraharness"
)

// The four official artifacts can be staged without root. This proves input
// custody only; the live mixed cohort must still provide physical witnesses.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F6
func TestInfraBusinessSupplyStagesOfficialArtifacts(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	root, source := t.TempDir(), infraharness.StockSource(repo, "")
	cmd := exec.CommandContext(t.Context(), "/usr/bin/python3", "-I", "-B", "-c", stageBusinessStock, repo, source, root)
	runSupplyControl(t, cmd, 0)
	data, err := os.ReadFile(filepath.Join(root, "stock.json"))
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []struct {
		Kind, Path, SHA256, Version string
		Race                        bool
	}
	if json.Unmarshal(data, &artifacts) != nil || len(artifacts) != 4 {
		t.Fatal("business supply did not stage four official artifacts")
	}
	catalog, err := infraartifact.New(infraartifact.Config{})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, artifact := range artifacts {
		kind := c.NATSBinary
		if artifact.Kind == "caddy-"+artifact.Version {
			kind = c.CaddyBinary
		} else if artifact.Kind != "nats-"+artifact.Version {
			t.Fatal("unexpected stock artifact")
		}
		allowed, err := catalog.Resolve(kind, artifact.Version, "linux", "amd64")
		if err != nil || seen[artifact.Kind] || artifact.Race || artifact.SHA256 != hex.EncodeToString(allowed.BinarySHA256[:]) {
			t.Fatal("business stock lost its frozen official identity")
		}
		seen[artifact.Kind] = true
		if err := infraharness.VerifyArtifact(root, artifact.Path, allowed.BinarySHA256, false); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("changed_official_archive", func(t *testing.T) {
		var entries []infraartifact.Entry
		data, err := os.ReadFile(filepath.Join(repo, "internal/infraartifact/testdata/catalog.json"))
		if err != nil || json.Unmarshal(data, &entries) != nil || len(entries) == 0 {
			t.Fatal("frozen catalog unavailable")
		}
		entry := entries[0]
		incoming := t.TempDir()
		data, err = os.ReadFile(filepath.Join(source, entry.ArchiveName))
		if err != nil {
			t.Fatal(err)
		}
		data[0] ^= 1
		if err := os.WriteFile(filepath.Join(incoming, entry.ArchiveName), data, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), "/usr/bin/python3", "-I", "-B", "-c", stageBusinessStock, repo, incoming, t.TempDir())
		output := runSupplyControl(t, cmd, 1)
		if !bytes.Contains(output, []byte("P26: official stock archive/checksum digest mismatch")) {
			t.Fatalf("modified official input crossed archive admission: %s", output)
		}
	})
}

// A deployment recipe is not the executable adapter consumed by runLive. Do
// not build expensive current inputs or emit a usable manifest without P32.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F6
func TestInfraBusinessSupplyRequiresExecutableOwner(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	root := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "/usr/bin/python3", "-I", "-B", filepath.Join(repo, "test/infraharness/business_supply.py"),
		"--repo", repo, "--output", root, "--stock", filepath.Join(root, "not-read-before-admission"))
	output := runSupplyControl(t, cmd, 2)
	if !bytes.Contains(output, []byte("P32: live --scenario/--supply/--evidence workload and source closure required")) {
		t.Fatal("missing workload refusal lost its actual owner")
	}
	if _, err := os.Stat(filepath.Join(root, "business-supply.json")); !os.IsNotExist(err) {
		t.Fatal("missing live owner published a usable supply")
	}
}

func runSupplyControl(t *testing.T, cmd *exec.Cmd, expected int) []byte {
	t.Helper()
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	process, err := infraharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = process.Stop() }()
	err = process.Wait()
	rc := 0
	if err != nil {
		exit, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatal(err)
		}
		rc = exit.ExitCode()
	}
	if rc != expected {
		t.Fatalf("supply control rc=%d want=%d: %s", rc, expected, output.Bytes())
	}
	return output.Bytes()
}

const stageBusinessStock = `
import importlib.util, json, sys
from pathlib import Path
repo, source, root = map(Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("business_supply", repo / "test/infraharness/business_supply.py")
supply = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supply)
rows = supply.stock(repo, source, root / "stock")
(root / "stock.json").write_text(json.dumps(rows))
`
