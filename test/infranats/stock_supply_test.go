//go:build linux && amd64 && infranats_integration

package infranats_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/infraharness"
)

// P26 supplies immutable official bytes before this job. Tests never download,
// build a historical release, search PATH or use an installed tether. The
// embedded catalog is the allowlist; the running image is independently hashed.
// origin: hot-update S9 plan §3.1 / §9.1 / T60–T63
func stockArtifact(t *testing.T, version string) (string, c.Artifact) {
	t.Helper()
	root := os.Getenv("TETHER_INFRA_NATS_SUPPLY")
	if root == "" {
		source := infraharness.StockSource("", "")
		staged, err := infraharness.PrepareStockArtifact(t.TempDir(), source, c.NATSBinary, version, budget(t, 30*time.Second), nil)
		if err != nil {
			t.Fatal(err)
		}
		return staged.Binary, staged.Artifact
	}
	catalog, err := infraartifact.New(infraartifact.Config{})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.Resolve(c.NATSBinary, version, "linux", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "nats-server-"+version)
	if err := infraharness.VerifyArtifact(root, path, artifact.BinarySHA256, false); err != nil {
		t.Fatalf("SETUP-RED pending P26: official %s supply: %v", version, err)
	}
	return path, artifact
}

func errorsAsKilled(err error, exit **exec.ExitError) bool {
	// The supervisor preserves the native signalled result. A successful wait
	// without an observed signal must not substitute for old-incarnation exit.
	if !errors.As(err, exit) {
		return false
	}
	status, ok := (*exit).Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

// origin: S9 internal review INFRA-SUITE-SUPPLIES F1
func TestNATSWaitRecognizesNativeKillStatus(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNativeWait(t)
	}
	testharness.AssertNoGoroutineLeak(t, "native NATS wait", g)
	testharness.AssertNoFDLeak(t, "native NATS wait", fd, 0)
}

func exerciseNativeWait(t *testing.T) {
	t.Helper()
	for _, row := range []struct {
		script string
		killed bool
	}{{"kill -KILL $$", true}, {"exit 1", false}, {"exit 0", false}} {
		cmd := exec.CommandContext(t.Context(), "/bin/sh", "-c", row.script)
		process, err := infraharness.StartProcess(cmd)
		if err != nil {
			t.Fatal(err)
		}
		var exit *exec.ExitError
		if err := process.Wait(); errorsAsKilled(err, &exit) != row.killed {
			t.Fatalf("native wait classification lost signal identity: %v", err)
		}
	}
}

// Cleanup follows CommandContext's cancellation, which testing performs before
// t.Cleanup. It joins fixture custody; product retirement is asserted in stop.
func cleanupStockNode(t *testing.T, n *stockNode) {
	t.Helper()
	if n.proc == nil {
		return
	}
	var exit *exec.ExitError
	if err := n.proc.Stop(); err != nil && !errorsAsKilled(err, &exit) {
		t.Error(err)
	}
	if exists, err := infraharness.ProcessExists(n.proc.Identity()); err != nil || exists {
		t.Error("stock fixture cleanup left an incarnation", err)
	}
	n.proc = nil
}
