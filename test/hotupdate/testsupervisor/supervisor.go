// Package testsupervisor owns fixture process trees outside the test runner.
// A returned waiter joins the target and its supervisor; Cmd.Wait must not be
// used because the target is a child of the external supervisor.
// origin: hot-update S8 plan §9.1 / P21 / L21
package testsupervisor

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StartProcess preserves argv, environment, directory, stdio and inherited
// descriptors. Use exec.CommandContext with the caller's context for bounded
// work. Cancellation closes the supervisor lifeline instead of killing the
// supervisor before it can collect escaped descendants. Fatal runner exits also
// close that lifeline, independently of goroutine cleanup and Pdeathsig.
func StartProcess(cmd *exec.Cmd) (func() error, error) {
	// origin: hot-update S8 plan §9.1 / P21
	if cmd == nil || cmd.Path == "" || len(cmd.Args) == 0 {
		return nil, errors.New("testsupervisor: incomplete command")
	}
	if cmd.Err != nil {
		return nil, cmd.Err
	}
	if cmd.Process != nil {
		return nil, errors.New("testsupervisor: command already started")
	}
	return startChild(cmd)
}

// RunProcess retains supervision for short commands too. A compiler, ctl or
// collector can leave a descendant even when the immediate command succeeds.
func RunProcess(cmd *exec.Cmd) error {
	wait, err := StartProcess(cmd)
	if err != nil {
		return err
	}
	return wait()
}

// CombinedOutput captures both streams only when the caller has not assigned
// them. It returns the actual command error after the entire tree has joined.
func CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
	if cmd == nil || cmd.Stdout != nil || cmd.Stderr != nil {
		return nil, errors.New("testsupervisor: output already configured or command missing")
	}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	err := RunProcess(cmd)
	return output.Bytes(), err
}

// Output retains the caller's stderr writer while capturing stdout.
func Output(cmd *exec.Cmd) ([]byte, error) {
	if cmd == nil || cmd.Stdout != nil {
		return nil, errors.New("testsupervisor: stdout already configured or command missing")
	}
	var output bytes.Buffer
	cmd.Stdout = &output
	err := RunProcess(cmd)
	return output.Bytes(), err
}

// identity binds all survivor observations and emergency signals to the same
// process incarnation. A recycled PID never establishes fixture ownership.
type identity struct {
	PID   int
	Start uint64
}

const retainedDispatch = "_TETHER_S8_RETAINED_RUNNER"

type retainedSpec struct {
	Path      string
	Args, Env []string
	Files     int
}

type retainedReceipt struct {
	Identity identity
	Code     int
	Error    string
}

func init() {
	if value := os.Getenv(retainedDispatch); value != "" {
		fd, err := strconv.Atoi(value)
		if err != nil || fd < 3 {
			os.Exit(126)
		}
		os.Exit(runRetainedRunner(fd))
	}
}

// Retain an independently supervised guard while a victim runner dies. Its
// waiter exposes the victim's real terminal receipt BEFORE the guard retires.
// Otherwise an outer reaper would kill the leaked inner tree before the oracle
// can distinguish a broken supervisor from a working one. The second waiter
// releases the guard and joins every remaining descendant, even on mutation
// failures. Runner death/context cancellation also closes its CLOEXEC lease.
// origin: hot-update S8 plan §9.1 / P21 / L21
func startRetainedRunner(ctx context.Context, cmd *exec.Cmd) (func() error, func() error, error) {
	if cmd == nil || cmd.Path == "" || len(cmd.Args) == 0 || cmd.SysProcAttr != nil || cmd.Process != nil {
		return nil, nil, errors.New("testsupervisor: invalid retained runner")
	}
	path, err := os.Executable()
	if err != nil {
		return nil, nil, err
	}
	var files []*os.File
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	pipe := func() (*os.File, *os.File, error) {
		r, w, err := os.Pipe()
		if err == nil {
			files = append(files, r, w)
		}
		return r, w, err
	}
	specR, specW, err := pipe()
	if err != nil {
		return nil, nil, err
	}
	leaseR, leaseW, err := pipe()
	if err != nil {
		return nil, nil, err
	}
	readyR, readyW, err := pipe()
	if err != nil {
		return nil, nil, err
	}
	exitR, exitW, err := pipe()
	if err != nil {
		return nil, nil, err
	}
	guard := exec.CommandContext(ctx, path)
	guard.Env = append(retainedEnvironment(cmd.Environ()), retainedDispatch+"="+strconv.Itoa(3+len(cmd.ExtraFiles)))
	guard.Dir, guard.Stdin, guard.Stdout, guard.Stderr = cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr
	guard.ExtraFiles = append(append([]*os.File(nil), cmd.ExtraFiles...), specR, leaseR, readyW, exitW)
	join, err := startRetainedGuardian(guard)
	if err != nil {
		return nil, nil, err
	}
	_ = specR.Close()
	_ = leaseR.Close()
	_ = readyW.Close()
	_ = exitW.Close()
	err = gob.NewEncoder(specW).Encode(retainedSpec{Path: cmd.Path, Args: cmd.Args, Env: retainedEnvironment(cmd.Environ()), Files: len(cmd.ExtraFiles)})
	_ = specW.Close()
	var ready retainedReceipt
	if err == nil {
		err = gob.NewDecoder(readyR).Decode(&ready)
	}
	if err == nil && (ready.Error != "" || ready.Identity.PID <= 0 || ready.Identity.Start == 0) {
		err = errors.New("testsupervisor: retained runner failed to publish identity")
	}
	if err == nil {
		cmd.Process, err = os.FindProcess(ready.Identity.PID)
	}
	if err != nil {
		_ = leaseW.Close()
		_ = join()
		return nil, nil, err
	}
	_ = readyR.Close()
	files = nil // The two idempotent waiters now own the remaining descriptors.
	var waitOnce, closeOnce sync.Once
	var waitErr, closeErr error
	wait := func() error {
		waitOnce.Do(func() {
			defer func() { _ = exitR.Close() }()
			var receipt retainedReceipt
			waitErr = gob.NewDecoder(exitR).Decode(&receipt)
			if waitErr == nil && receipt.Identity != ready.Identity {
				waitErr = errors.New("testsupervisor: retained runner terminal identity changed")
			}
			if waitErr == nil && (receipt.Code != 0 || receipt.Error != "") {
				waitErr = fmt.Errorf("testsupervisor: retained runner exit=%d: %s", receipt.Code, receipt.Error)
			}
		})
		return waitErr
	}
	closeGuard := func() error {
		closeOnce.Do(func() {
			_ = leaseW.Close()
			closeErr = join()
			_ = exitR.Close()
			_ = cmd.Process.Release()
		})
		return closeErr
	}
	return wait, closeGuard, nil
}

func retainedEnvironment(env []string) []string {
	var clean []string
	for _, entry := range env {
		if !strings.HasPrefix(entry, retainedDispatch+"=") {
			clean = append(clean, entry)
		}
	}
	return clean
}

func runRetainedRunner(fd int) int {
	specFile := os.NewFile(uintptr(fd), "retained-spec")
	lease := os.NewFile(uintptr(fd+1), "retained-lease")
	ready := os.NewFile(uintptr(fd+2), "retained-ready")
	exit := os.NewFile(uintptr(fd+3), "retained-exit")
	defer func() { _ = specFile.Close(); _ = lease.Close(); _ = ready.Close(); _ = exit.Close() }()
	var spec retainedSpec
	if err := gob.NewDecoder(specFile).Decode(&spec); err != nil || spec.Files != fd-3 || len(spec.Args) == 0 {
		return 126
	}
	_ = specFile.Close()
	if err := prepareRetainedRunner(fd); err != nil {
		return 126
	}
	cmd := exec.Command(spec.Path, spec.Args[1:]...)
	cmd.Args, cmd.Env = spec.Args, retainedRunnerEnvironment(spec.Env)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	for index := 3; index < fd; index++ {
		cmd.ExtraFiles = append(cmd.ExtraFiles, os.NewFile(uintptr(index), "retained-inherited"))
	}
	if err := cmd.Start(); err != nil {
		_ = gob.NewEncoder(ready).Encode(retainedReceipt{Error: "runner launch failed"})
		return 126
	}
	for _, file := range cmd.ExtraFiles {
		_ = file.Close()
	}
	id, _, err := processIdentity(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return 126
	}
	if err := gob.NewEncoder(ready).Encode(retainedReceipt{Identity: id}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return 126
	}
	_ = ready.Close()
	leaseDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, lease); close(leaseDone) }()
	terminal := make(chan error, 1)
	go func() { terminal <- cmd.Wait() }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		// Only terminal adoptees are reaped here. Live descendants remain
		// untouched until the caller's oracle has inspected the inner tree.
		if err := reapRetainedTerminals(id.PID); err != nil {
			_ = cmd.Process.Kill()
			return 125
		}
		select {
		case err := <-terminal:
			receipt := retainedReceipt{Identity: id, Code: -1}
			if cmd.ProcessState != nil {
				receipt.Code = cmd.ProcessState.ExitCode()
			}
			if err != nil {
				receipt.Error = err.Error()
			}
			_ = gob.NewEncoder(exit).Encode(receipt)
			_ = exit.Close()
			terminal = nil
		case <-leaseDone:
			if terminal != nil {
				_ = cmd.Process.Kill()
				<-terminal
			}
			// The surrounding external supervisor collects live descendants
			// and proves emptiness when this retained guard retires.
			return 0
		case <-ticker.C:
		}
	}
}
