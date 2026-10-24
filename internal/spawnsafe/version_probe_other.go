//go:build !linux

package spawnsafe

import (
	"os/exec"
	"syscall"
)

// RunVersionProbe protects the probe group on platforms without Linux
// subreaping. Orphan reaping belongs to the OS there; image handoff is unsupported.
func RunVersionProbe(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }()
	return cmd.Wait()
}
