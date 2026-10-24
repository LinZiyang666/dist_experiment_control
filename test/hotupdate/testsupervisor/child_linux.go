//go:build linux

package testsupervisor

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// Keep one Linux implementation of the subreaper, clone-time pidfd, CLOEXEC
// lifeline and terminal-status handoff. The existing helper already handles
// worker-thread same-PID exec and cancellation with a short WaitDelay; copying
// it here would create two independently evolving cleanup protocols.
// origin: hot-update S8 plan §9.1 / P21 / M21
func startChild(cmd *exec.Cmd) (func() error, error) {
	return testharness.StartProcess(cmd)
}

// The retained guard's outer cleanup is trusted independently of the launcher
// under mutation. Reaping only exited adoptees cannot conceal a live orphan.
func startRetainedGuardian(cmd *exec.Cmd) (func() error, error) {
	return testharness.StartProcess(cmd)
}

func prepareRetainedRunner(fd int) error {
	// Protocol writers must not escape into the victim's descendants and keep
	// the terminal or lifeline EOF barrier open after its owner has died.
	for index := 3; index <= fd+3; index++ {
		unix.CloseOnExec(index)
	}
	return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
}

func retainedRunnerEnvironment(env []string) []string { return env }

func reapRetainedTerminals(target int) error {
	threads, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return err
	}
	for _, thread := range threads {
		body, err := os.ReadFile("/proc/self/task/" + thread.Name() + "/children")
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return err
		}
		for _, text := range strings.Fields(string(body)) {
			pid, err := strconv.Atoi(text)
			if err != nil {
				return err
			}
			if pid != target {
				_, err = unix.Wait4(pid, nil, unix.WNOHANG, nil)
				if err != nil && !errors.Is(err, unix.ECHILD) && !errors.Is(err, unix.EINTR) {
					return err
				}
			}
		}
	}
	return nil
}

func processIdentity(pid int) (identity, int, error) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return identity{}, 0, err
	}
	end := strings.LastIndexByte(string(body), ')')
	if end < 0 {
		return identity{}, 0, errors.New("testsupervisor: malformed process identity")
	}
	fields := strings.Fields(string(body)[end+1:])
	if len(fields) < 20 {
		return identity{}, 0, errors.New("testsupervisor: incomplete process identity")
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return identity{}, 0, err
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	return identity{PID: pid, Start: start}, parent, err
}

func identityExists(id identity) bool {
	current, _, err := processIdentity(id.PID)
	return err == nil && current == id
}

// Pin first, then recheck start ticks. An emergency cleanup in a mutation run
// must not turn a PID reuse into a signal against another engineer's fixture.
func killIdentity(id identity) error {
	fd, err := unix.PidfdOpen(id.PID, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	if !identityExists(id) {
		return nil
	}
	err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

func reapIdentity(id identity) {
	if identityExists(id) {
		_, _ = unix.Wait4(id.PID, nil, unix.WNOHANG, nil)
	}
}

func adoptTestOrphans() (func(), error) {
	var previous int32
	if err := unix.Prctl(unix.PR_GET_CHILD_SUBREAPER, uintptr(unsafe.Pointer(&previous)), 0, 0, 0); err != nil {
		return nil, err
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return nil, err
	}
	return func() { _ = unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, uintptr(previous), 0, 0, 0) }, nil
}

func fixtureAttributes(escaped bool) *syscall.SysProcAttr {
	if escaped {
		return &syscall.SysProcAttr{Setsid: true}
	}
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}
