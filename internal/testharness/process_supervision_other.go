//go:build !linux

package testharness

import "os/exec"

// StartProcess retains ordinary process ownership where Linux subreaping and
// pidfds are unavailable. The fatal-exit supervision regressions are Linux-only.
func StartProcess(cmd *exec.Cmd) (func() error, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd.Wait, nil
}
