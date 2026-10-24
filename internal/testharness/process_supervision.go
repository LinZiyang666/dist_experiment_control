package testharness

import (
	"bytes"
	"errors"
	"os/exec"
)

// RunProcess joins both the target and its private supervisor. StartProcess
// exposes the target PID; callers must use its returned waiter, not Cmd.Wait.
func RunProcess(cmd *exec.Cmd) error {
	wait, err := StartProcess(cmd)
	if err != nil {
		return err
	}
	return wait()
}

// CombinedOutput supervises short commands as well: compilers and smoke probes
// can outlive a fatal package timeout through their own descendants.
func CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
	if cmd.Stdout != nil || cmd.Stderr != nil {
		return nil, errors.New("testharness: output already configured")
	}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	err := RunProcess(cmd)
	return output.Bytes(), err
}

// Output captures stdout while retaining the command's configured stderr.
func Output(cmd *exec.Cmd) ([]byte, error) {
	if cmd.Stdout != nil {
		return nil, errors.New("testharness: stdout already configured")
	}
	var output bytes.Buffer
	cmd.Stdout = &output
	err := RunProcess(cmd)
	return output.Bytes(), err
}
