// process.go owns the fullstack adapters to the shared FIX-ORPHAN supervisor.
package fullstackharness

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// StartProcess exposes the target PID, including across same-PID exec. The
// returned waiter must be joined; Cmd.Wait does not own the supervisor's lease.
// origin: hot-update S10 plan §6 / P05; lead L15.
func StartProcess(cmd *exec.Cmd) (func() error, error) {
	return testharness.StartProcess(cmd)
}

// RunProcess joins the command and the shared supervisor on every return path.
func RunProcess(cmd *exec.Cmd) error { return testharness.RunProcess(cmd) }

// Output and CombinedOutput also supervise build tools and short-lived probes.
func Output(cmd *exec.Cmd) ([]byte, error)         { return testharness.Output(cmd) }
func CombinedOutput(cmd *exec.Cmd) ([]byte, error) { return testharness.CombinedOutput(cmd) }

// ProcessIdentity is a census key, rather than a reusable PID. Cgroup records the
// kernel's complete membership, including namespace-relative paths.
type ProcessIdentity struct {
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	PIDNS      string `json:"pid_namespace"`
	Cgroup     string `json:"cgroup"`
}

// ProcessReady is emitted only after the owner watch and shared reaper are live.
// A launcher receipt does not certify product or deployment readiness.
type ProcessReady struct {
	Launcher ProcessIdentity `json:"launcher"`
	Target   ProcessIdentity `json:"target"`
}

// LauncherOptions applies to the test-only Python/shell launcher. Lease is
// consumed and closed even on partial startup. Remote callers send '.' at less
// than LeaseTimeout intervals; EOF or silence retires the entire target tree.
// Cgroup must name an already registered membership before a remote target starts.
// Ready is borrowed; the caller owns its close and must drain it before joining.
type LauncherOptions struct {
	Lease        *os.File
	LeaseTimeout time.Duration
	Cgroup       string
	Ready        io.Writer
}

var launcherImplementation = func(_ context.Context, _ *exec.Cmd, options LauncherOptions) error {
	if options.Lease != nil {
		_ = options.Lease.Close()
	}
	return errors.New("fullstack supervisor: Linux owner supervision is unsupported")
}

// RunLauncher treats cmd as an unstarted template. It joins the shared waiter
// before returning, including after parent death, control cancellation, broken
// READY output or remote lease expiry. The template retains actual target status.
// origin: hot-update S10 plan §6 / P05; lead L15.
func RunLauncher(ctx context.Context, cmd *exec.Cmd, options LauncherOptions) error {
	return launcherImplementation(ctx, cmd, options)
}

// ProcessExitCode preserves child status and never promotes cleanup's 125 to
// success. Setup failures are 126; neither value is a native drill verdict.
func ProcessExitCode(err error) int {
	if err == nil {
		return 0
	}
	// A joined target ExitError must never win over failed supervision, even
	// when another wrapper changes the order of errors.Join's children.
	// origin: S10 internal review rv-harness F5
	var supervision interface{ SupervisorExitCode() int }
	if errors.As(err, &supervision) {
		return supervision.SupervisorExitCode()
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return 128 + int(status.Signal())
		}
		return exit.ExitCode()
	}
	return 126
}
