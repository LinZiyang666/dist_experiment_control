//go:build linux

package clusterharness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The replacement is the compiled product command. Its only local evidence is
// the original grant cache; it must obtain the plan and progress from the
// daemon through the signed route, with no access to the departed ctl's model.
func resumeN3RollCLI(ctx context.Context, t *testing.T, binary, home, url, seedPath, parent string, identity []byte) error {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(home, "keys"), 0700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(home, "keys", "default.nk"), identity, 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(home, "current_session"), []byte("n3"), 0600); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, binary, "cluster", "upgrade", "--resume", parent, "--home", home, "--nats-url", url, "--account-seed", seedPath)
	command.Dir = home
	command.Env = append(handoff.CleanEnvironment(os.Environ()), "HOME="+home, "TETHER_HOME="+home, "TETHER_SESSION=n3", "GORACE=atexit_sleep_ms=0")
	output, err := testharness.CombinedOutput(command)
	if err != nil {
		return fmt.Errorf("replacement cluster --resume: %w\n%s", err, output)
	}
	return nil
}
