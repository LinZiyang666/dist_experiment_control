//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// A serialization control is deliberately distinct from runLive. Its input is
// the oracle fixture, not a process/socket/S7 witness or a successful live cut.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F6
func TestInfraBusinessReceiptProducerCanonicalBinding(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	r := reviewBusinessReceipt(t)
	r.Scenario = "mixed"
	// Go escapes these characters even with UTF-8 output. Python's default
	// encoder differs, as it also does for promoted fields and uint64 values.
	r.Required[0].Nonce += "<&>\u2028\u2029é"
	r.Required[0].Cookie = ^uint64(0)
	data, err := json.Marshal(r)
	if err != nil || os.WriteFile(filepath.Join(root, "input.json"), data, 0600) != nil {
		t.Fatal("could not write canonical encoder control")
	}
	cmd := exec.CommandContext(t.Context(), "/usr/bin/python3", "-I", "-B", filepath.Join(repo, "test/infraharness/business_receipt_controls.py"), repo, root)
	if output, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("business receipt producer control: %v: %s", err, output)
	}
	produced, err := os.ReadFile(filepath.Join(root, "canonical-control.json"))
	if err != nil || !bytes.Equal(produced, data) {
		t.Fatal("producer output differs from the exact Go receipt codec", err)
	}
	var decoded businessReceipt
	if err := readJSON(filepath.Join(root, "canonical-control.json"), &decoded); err != nil {
		t.Fatal("actual runLive decoder rejected producer bytes", err)
	}
}
