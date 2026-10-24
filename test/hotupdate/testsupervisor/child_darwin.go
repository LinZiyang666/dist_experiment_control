//go:build darwin

package testsupervisor

import (
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const darwinDispatch = "_TETHER_S8_DARWIN_SUPERVISOR"
const darwinCustody = "TETHER_TEST_TREE_CUSTODY"

type darwinSpec struct {
	Path       string
	Args, Env  []string
	ExtraFiles int
	Attributes *syscall.SysProcAttr
	Custody    string
}

type darwinReceipt struct {
	Target   identity
	Code     int
	Signal   syscall.Signal
	Complete bool
	Error    string
}

// Darwin has neither Linux subreaping nor pidfds. Keep an external owner and
// track each child's kernel start time; inherited custody also discovers a
// double-forked child after its original parent disappears. Explicit fixture
// exec environments must retain the TETHER_TEST_TREE_CUSTODY_* entries. Each
// nested owner keeps its ancestors' entries as well. The value is random
// fixture identity, never business authority or secret material.
// origin: hot-update S8 plan §9.1 / P21 / L21
func init() {
	if value := os.Getenv(darwinDispatch); value != "" {
		fd, err := strconv.Atoi(value)
		if err != nil || fd < 3 {
			os.Exit(126)
		}
		os.Exit(runDarwinSupervisor(fd))
	}
}

func startChild(cmd *exec.Cmd) (func() error, error) {
	if a := cmd.SysProcAttr; a != nil && !reflect.DeepEqual(*a, syscall.SysProcAttr{
		Setpgid: a.Setpgid, Pgid: a.Pgid, Setsid: a.Setsid,
		Setctty: a.Setctty, Ctty: a.Ctty, Noctty: a.Noctty,
	}) {
		return nil, errors.New("testsupervisor: unsupported Darwin process attributes")
	}
	path, err := os.Executable()
	if err != nil {
		return nil, err
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
		return nil, err
	}
	leaseR, leaseW, err := pipe()
	if err != nil {
		return nil, err
	}
	readyR, readyW, err := pipe()
	if err != nil {
		return nil, err
	}
	resultR, resultW, err := pipe()
	if err != nil {
		return nil, err
	}
	var closeOnce sync.Once
	closeLease := func() { closeOnce.Do(func() { _ = leaseW.Close() }) }
	helper := *cmd // Preserve CommandContext's private context and stdio pumps.
	helper.Path, helper.Args = path, []string{path}
	helper.Env = darwinEnvironment(cmd.Environ())
	helper.Env = append(helper.Env, darwinDispatch+"="+strconv.Itoa(3+len(cmd.ExtraFiles)), "GORACE=atexit_sleep_ms=0")
	helper.ExtraFiles = append(append([]*os.File(nil), cmd.ExtraFiles...), specR, leaseR, readyW, resultW)
	helper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if helper.Cancel != nil {
		helper.Cancel = func() error {
			closeLease()
			// Joining through the kernel exit state keeps WaitDelay from killing
			// a supervisor while it still owns escaped descendants.
			for {
				info, err := unix.SysctlKinfoProc("kern.proc.pid", helper.Process.Pid)
				if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EIO) {
					return nil
				}
				if err == nil && info.Proc.P_stat == 5 { // SZOMB: terminal, ready for the caller's Wait.
					return nil
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	if err := helper.Start(); err != nil {
		return nil, err
	}
	_ = specR.Close()
	_ = leaseR.Close()
	_ = readyW.Close()
	_ = resultW.Close()
	var token [16]byte
	_, err = rand.Read(token[:])
	if err == nil {
		err = gob.NewEncoder(specW).Encode(darwinSpec{Path: cmd.Path, Args: cmd.Args, Env: darwinEnvironment(cmd.Environ()),
			ExtraFiles: len(cmd.ExtraFiles), Attributes: cmd.SysProcAttr, Custody: fmt.Sprintf("%x", token)})
	}
	_ = specW.Close()
	var ready darwinReceipt
	if err == nil {
		err = gob.NewDecoder(readyR).Decode(&ready)
	}
	if err == nil && (ready.Error != "" || ready.Target.PID <= 0 || ready.Target.Start == 0) {
		err = fmt.Errorf("testsupervisor: Darwin target start refused: %s", ready.Error)
	}
	if err != nil {
		closeLease()
		_ = helper.Wait()
		return nil, err
	}
	cmd.Process, err = os.FindProcess(ready.Target.PID)
	if err != nil {
		closeLease()
		_ = helper.Wait()
		return nil, err
	}
	// Transfer the lease and result reader out of the startup cleanup. They have
	// exactly one owner until the idempotent waiter finishes.
	files = nil
	_ = readyR.Close()
	var once sync.Once
	var waitErr error
	return func() error {
		once.Do(func() {
			defer closeLease()
			defer func() { _ = resultR.Close() }()
			var result darwinReceipt
			decodeErr := gob.NewDecoder(resultR).Decode(&result)
			waitErr = errors.Join(decodeErr, helper.Wait())
			if !result.Complete || result.Target != ready.Target || result.Error != "" {
				waitErr = errors.Join(waitErr, errors.New("testsupervisor: Darwin tree has no complete exit receipt"))
			} else if result.Code != 0 || result.Signal != 0 {
				waitErr = errors.Join(waitErr, &darwinExitError{PID: result.Target.PID, Code: result.Code, Signal: result.Signal})
			}
		})
		return waitErr
	}, nil
}

// The actual target is not waitable by the runner on Darwin. Return its explicit
// terminal receipt rather than fabricating ProcessState from the helper's PID.
type darwinExitError struct {
	PID, Code int
	Signal    syscall.Signal
}

func (e *darwinExitError) Error() string {
	return fmt.Sprintf("testsupervisor: target pid=%d exit=%d signal=%d", e.PID, e.Code, e.Signal)
}

func (e *darwinExitError) ExitCode() int { return e.Code }

func darwinEnvironment(env []string) []string {
	var out []string
	for _, entry := range env {
		if !strings.HasPrefix(entry, darwinDispatch+"=") {
			out = append(out, entry)
		}
	}
	return out
}

func runDarwinSupervisor(fd int) int {
	specFile := os.NewFile(uintptr(fd), "supervisor-spec")
	lease := os.NewFile(uintptr(fd+1), "supervisor-lifeline")
	ready := os.NewFile(uintptr(fd+2), "supervisor-ready")
	result := os.NewFile(uintptr(fd+3), "supervisor-result")
	for index := 3; index <= fd+3; index++ {
		unix.CloseOnExec(index)
	}
	defer func() { _ = specFile.Close(); _ = lease.Close(); _ = ready.Close(); _ = result.Close() }()
	var spec darwinSpec
	if err := gob.NewDecoder(specFile).Decode(&spec); err != nil || spec.ExtraFiles != fd-3 || len(spec.Args) == 0 || spec.Custody == "" {
		return 126
	}
	_ = specFile.Close()
	// Refuse before launching if this account cannot enumerate custody. An
	// inaccessible process table must never become an empty-tree proof.
	if err := darwinCensus(make(map[int]identity), spec.Custody); err != nil {
		_ = gob.NewEncoder(ready).Encode(darwinReceipt{Error: "custody preflight unavailable"})
		return 126
	}
	cmd := exec.Command(spec.Path, spec.Args[1:]...)
	cmd.Args, cmd.Env, cmd.SysProcAttr = spec.Args, append(spec.Env, darwinCustody+"_"+spec.Custody+"=1"), spec.Attributes
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	for index := 3; index < fd; index++ {
		cmd.ExtraFiles = append(cmd.ExtraFiles, os.NewFile(uintptr(index), "fixture-inherited"))
	}
	if err := cmd.Start(); err != nil {
		_ = gob.NewEncoder(ready).Encode(darwinReceipt{Error: "target launch failed"})
		return 126
	}
	for _, file := range cmd.ExtraFiles {
		_ = file.Close()
	}
	id, _, err := processIdentity(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = gob.NewEncoder(ready).Encode(darwinReceipt{Error: "target identity unavailable"})
		return 126
	}
	owned := map[int]identity{id.PID: id}
	if err := gob.NewEncoder(ready).Encode(darwinReceipt{Target: id}); err != nil {
		_ = cmd.Process.Kill()
	}
	_ = ready.Close()
	leaseDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, lease); close(leaseDone) }()
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	finished, stopping := false, false
	var terminal error
	for {
		if err := darwinCensus(owned, spec.Custody); err != nil {
			if terminal == nil {
				terminal = err
			}
			stopping = true
		}
		if finished && !stopping && len(owned) != 0 {
			terminal = errors.New("testsupervisor: target exited with owned descendants")
			stopping = true
		}
		if stopping || finished {
			for _, child := range owned {
				if err := killIdentity(child); err != nil {
					if terminal == nil {
						terminal = err
					}
				}
			}
		}
		if finished && len(owned) == 0 {
			// Two independent empty reads include marker discovery as well as
			// the original PID set. A single PID disappearance is not custody.
			<-ticker.C
			if err := darwinCensus(owned, spec.Custody); err != nil {
				if terminal == nil {
					terminal = err
				}
			} else if len(owned) == 0 {
				break
			}
		}
		select {
		case <-leaseDone:
			stopping, leaseDone = true, nil
		case waitErr := <-waited:
			if cmd.ProcessState == nil && waitErr != nil {
				terminal = errors.Join(terminal, waitErr)
			}
			finished, waited = true, nil
		case <-ticker.C:
		}
	}
	receipt := darwinReceipt{Target: id, Complete: terminal == nil, Code: -1}
	if cmd.ProcessState != nil {
		receipt.Code = cmd.ProcessState.ExitCode()
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			receipt.Signal = status.Signal()
		}
	}
	if terminal != nil {
		receipt.Error = "tree census or cleanup failed"
	}
	if err := gob.NewEncoder(result).Encode(receipt); err != nil && !stopping {
		return 126
	}
	if terminal != nil {
		return 125
	}
	return 0
}

func darwinCensus(owned map[int]identity, token string) error {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return err
	}
	live := make(map[int]unix.KinfoProc)
	for _, process := range processes {
		live[int(process.Proc.P_pid)] = process
	}
	for pid, previous := range owned {
		current, ok := live[pid]
		if !ok || darwinIdentity(current) != previous {
			delete(owned, pid)
		}
	}
	for changed := true; changed; {
		changed = false
		for pid, process := range live {
			if pid == os.Getpid() || owned[pid].PID != 0 || process.Eproc.Ucred.Uid != uint32(os.Getuid()) {
				continue
			}
			parent := owned[int(process.Eproc.Ppid)].PID != 0
			if !parent {
				body, err := unix.SysctlRaw("kern.procargs2", pid)
				if err != nil {
					if process.Proc.P_stat == 5 || errors.Is(err, unix.ESRCH) || errors.Is(err, unix.EINVAL) {
						continue // A terminal process no longer has argv/environment memory.
					}
					return errors.New("testsupervisor: Darwin custody unreadable")
				}
				// Inspect only exact NUL-delimited identity entries; never emit
				// argv/environment content into a receipt, log or error.
				for _, entry := range strings.Split(string(body), "\x00") {
					parent = parent || entry == darwinCustody+"_"+token+"=1"
				}
			}
			if parent {
				owned[pid], changed = darwinIdentity(process), true
			}
		}
	}
	return nil
}

func darwinIdentity(info unix.KinfoProc) identity {
	return identity{PID: int(info.Proc.P_pid), Start: uint64(info.Proc.P_starttime.Sec)*1_000_000 + uint64(info.Proc.P_starttime.Usec)}
}

func processIdentity(pid int) (identity, int, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return identity{}, 0, err
	}
	return darwinIdentity(*info), int(info.Eproc.Ppid), nil
}

func identityExists(id identity) bool {
	current, _, err := processIdentity(id.PID)
	return err == nil && current == id
}

func killIdentity(id identity) error {
	if !identityExists(id) {
		return nil
	}
	err := unix.Kill(id.PID, unix.SIGKILL)
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

func adoptTestOrphans() (func(), error) { return func() {}, nil }

func fixtureAttributes(escaped bool) *syscall.SysProcAttr {
	if escaped {
		return &syscall.SysProcAttr{Setsid: true}
	}
	return &syscall.SysProcAttr{Setpgid: true}
}

func startRetainedGuardian(cmd *exec.Cmd) (func() error, error) { return startChild(cmd) }
func prepareRetainedRunner(fd int) error {
	for index := 3; index <= fd+3; index++ {
		unix.CloseOnExec(index)
	}
	return nil
}
func reapRetainedTerminals(_ int) error { return nil }

func retainedRunnerEnvironment(env []string) []string {
	// The guard's own marker must survive the explicitly supplied runner env.
	// Its outer supervisor then owns the runner even if the inner one is broken.
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, darwinCustody+"_") {
			env = append(env, entry)
		}
	}
	return env
}
