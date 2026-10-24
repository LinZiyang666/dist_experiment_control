package infraharness

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Source controls run independently of the expensive genuine binary producer.
// They cannot make a live acceptance cell pass or generate runtime receipts.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F4 / F6
func TestInfraSupplySourceCustody(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupplySourceCustody(t)
	}
	testharness.AssertNoGoroutineLeak(t, "offline supply source custody", g)
	testharness.AssertNoFDLeak(t, "offline supply source custody", fd, 0)
}

func exerciseSupplySourceCustody(t *testing.T) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	cmd := exec.CommandContext(t.Context(), "/usr/bin/python3", "-I", "-B", filepath.Join(filepath.Dir(file), "supply_controls.py"))
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	process, err := StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = process.Stop() }()
	if err := process.Wait(); err != nil {
		t.Fatalf("source custody controls: %v: %s", err, output.Bytes())
	}
}
