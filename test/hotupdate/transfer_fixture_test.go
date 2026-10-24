//go:build linux && hotupdate_integration

package hotupdate_test

// Real: the unmodified Python fixture self-test and its local file/socket/NATS
// protocol readers. Simulated: controlled custody/history producers only.
// Not covered: deploy continuity or S7 product producers in this fixture test.
import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/test/transferharness"
)

// origin: hot-update S7 plan §10.1 / T42. Both pre-judge and post-judge late
// failed and a half-written-then-restored target run through the original judge.
// origin: S7 internal review rv-lifecycle F7
func TestTransferFixtureRejectsPartialDestinationAndLateTerminal(t *testing.T) {
	if transferharness.SharedSupervisor == nil {
		t.Fatal("required Python fixture has no shared process supervisor")
	}
	h := transferharness.New(t, transferharness.SharedSupervisor(t))
	// Copy a known interpreter artifact into this fixture; Start never resolves
	// an installed tether or accepts an artifact in another engineer's worktree.
	interpreter := "/usr/bin/python3"
	data, err := os.ReadFile(interpreter)
	if err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(h.Root, "python3")
	if err := os.WriteFile(private, data, 0o700); err != nil {
		t.Fatal(err)
	}
	sha, err := transferharness.FileSHA(private)
	if err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs("../simcluster/tests/transfer-fixture-selftest.py")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // ctx-root: supervised hermetic collector test.
	defer cancel()
	p, err := h.Start(ctx, transferharness.Spec{Binary: private, ArtifactSHA: sha,
		Args: []string{"-I", "-B", script, "FixtureOracleTests"}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := p.Wait(ctx)
	if err != nil || receipt.NativeRC != 0 {
		raw, _ := os.ReadFile(receipt.StderrPath)
		t.Fatalf("fixture oracle controls failed: native_rc=%d err=%v\n%s", receipt.NativeRC, err, raw)
	}
}
