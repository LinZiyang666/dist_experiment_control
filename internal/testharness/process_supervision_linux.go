//go:build linux

package testharness

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const processSupervisorEnv = "_TETHER_TEST_PROCESS_SUPERVISOR"

type processSpec struct {
	Path           string
	Args           []string
	ExtraFiles     int
	Attributes     *syscall.SysProcAttr
	RaceOptions    string
	RaceOptionsSet bool
}

type processReady struct {
	PID   int
	Error string
}

// Cleanup has its own verdict; a target assertion failure cannot demote it to
// a native mutation kill. The target status remains in Cmd.ProcessState.
// origin: S10 internal review rv-harness F5
type processSupervisionError struct {
	detail string
	cause  error
}

func (e *processSupervisionError) Error() string           { return e.detail + ": " + e.cause.Error() }
func (e *processSupervisionError) Unwrap() error           { return e.cause }
func (e *processSupervisionError) SupervisorExitCode() int { return 125 }

// The helper must dispatch before the importing package's init hooks or test
// flag parser. It runs the requested executable, never another test selection.
func init() {
	if value := os.Getenv(processSupervisorEnv); value != "" {
		fd, err := strconv.Atoi(value)
		if err != nil || fd < 3 {
			os.Exit(126)
		}
		os.Exit(runProcessSupervisor(fd))
	}
}

// StartProcess keeps a separate subreaper alive until the target tree is empty.
// The returned waiter owns the CLOEXEC lease writer and must be joined on every
// normal path. Cmd.Process is the actual target (including after same-PID exec),
// so existing identity, signal and process-group oracles keep their meaning.
//
// Pdeathsig is per task: Go worker threads start with zero, and a worker's exec
// replaces the leader without copying its death signal. Locking the parent's
// fork thread alone cannot repair that. The helper instead watches a pipe whose
// only writer belongs to the test process; timeout, panic and SIGKILL close it.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN request 5.
func StartProcess(cmd *exec.Cmd) (func() error, error) {
	if err := validateProcessCommand(cmd); err != nil {
		return nil, err
	}
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	specR, specW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer func() { _ = specR.Close(); _ = specW.Close() }()
	leaseR, leaseW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		_ = leaseR.Close()
		_ = leaseW.Close()
		return nil, err
	}
	defer func() { _ = leaseR.Close(); _ = readyW.Close() }()
	var closeOnce sync.Once
	closeLease := func() { closeOnce.Do(func() { _ = leaseW.Close() }) }
	// Copy before either command has started: this preserves CommandContext's
	// private context while letting the public command retain its target identity.
	helper := *cmd
	helper.Path, helper.Args = path, []string{path}
	helper.Env = processEnvironment(cmd.Environ())
	var raceOptions string
	var raceOptionsSet bool
	var helperEnv []string
	for _, entry := range helper.Env {
		if strings.HasPrefix(entry, "GORACE=") {
			raceOptions, raceOptionsSet = strings.TrimPrefix(entry, "GORACE="), true
		} else {
			helperEnv = append(helperEnv, entry)
		}
	}
	// A private reaper has already joined the tree before exit. Race's default
	// one-second exit sleep per helper would consume aggregate matrix budgets;
	// the target retains its original race options and diagnostic behavior.
	helperEnv = append(helperEnv, "GORACE="+raceOptions+" atexit_sleep_ms=0")
	helper.Env = helperEnv
	helper.Env = append(helper.Env, processSupervisorEnv+"="+strconv.Itoa(3+len(cmd.ExtraFiles)))
	helper.ExtraFiles = append(append([]*os.File(nil), cmd.ExtraFiles...), specR, leaseR, readyW)
	helperFD := -1
	helper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, PidFD: &helperFD}
	if helper.Cancel != nil {
		// Do not SIGKILL the reaper: it must collect escaped grandchildren before
		// Wait returns, including when the caller's context has already expired.
		// Cancel joins through the clone-time pidfd before os/exec starts its
		// WaitDelay escalation timer. That timer keeps its original value but
		// cannot kill a reaper that still owns live or unreaped descendants.
		// origin: hot-update S6 plan §9.3 / FIX-ORPHAN cancellation seam.
		helper.Cancel = func() error {
			closeLease()
			if helperFD < 0 {
				return errors.New("testharness: supervisor pidfd unavailable")
			}
			poll := []unix.PollFd{{Fd: int32(helperFD), Events: unix.POLLIN}}
			for {
				_, err := unix.Poll(poll, -1)
				if errors.Is(err, unix.EINTR) {
					continue
				}
				return err
			}
		}
	}
	if err := helper.Start(); err != nil {
		closeLease()
		_ = readyR.Close()
		return nil, err
	}
	_ = readyW.Close() // Only the helper may keep the census pipe alive.
	startup, joinHelper := processSupervisorCensus(cmd, &helper, helperFD, readyR)
	if helperFD < 0 {
		closeLease()
		_ = specW.Close()
		_ = joinHelper()
		return nil, errors.New("testharness: supervisor pidfd unavailable")
	}
	_ = specR.Close()
	_ = leaseR.Close()
	err = gob.NewEncoder(specW).Encode(processSpec{Path: cmd.Path, Args: cmd.Args,
		ExtraFiles: len(cmd.ExtraFiles), Attributes: cmd.SysProcAttr,
		RaceOptions: raceOptions, RaceOptionsSet: raceOptionsSet})
	_ = specW.Close()
	var ready processReady
	if err == nil {
		response := <-startup
		ready, err = response.value, response.err
	}
	if err == nil && (ready.Error != "" || ready.PID <= 0) {
		err = fmt.Errorf("testharness: target start failed: %s", ready.Error)
	}
	if err != nil {
		closeLease()
		_ = joinHelper()
		return nil, err
	}
	cmd.Process, err = os.FindProcess(ready.PID)
	if err != nil {
		closeLease()
		_ = joinHelper()
		return nil, err
	}
	return processTargetWaiter(cmd, closeLease, joinHelper), nil
}

// validateProcessCommand refuses attributes that cannot be transported to the
// actual target, before any helper or lease is created.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN; S10 LINT-L4 responsibility split
func validateProcessCommand(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Path == "" || len(cmd.Args) == 0 {
		return errors.New("testharness: incomplete process command")
	}
	if cmd.Err != nil {
		return cmd.Err
	}
	if cmd.Process != nil {
		return errors.New("testharness: process already started")
	}
	// The helper leaves the target's terminal status for its original caller.
	// Adoption lets Process.Wait release the pidfd without Release's historical
	// Pid=-1 write racing existing identity and process-group observations.
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("testharness: caller subreaper: %w", err)
	}
	// FD/credential/namespace-dependent attributes need an explicit transport;
	// reject them rather than applying them to the reaper or silently dropping them.
	if a := cmd.SysProcAttr; a != nil && !reflect.DeepEqual(*a, syscall.SysProcAttr{
		Setpgid: a.Setpgid, Pgid: a.Pgid, Pdeathsig: a.Pdeathsig,
		Setsid: a.Setsid, Setctty: a.Setctty, Ctty: a.Ctty, Noctty: a.Noctty,
	}) {
		return errors.New("testharness: unsupported supervisor process attributes")
	}
	return nil
}

type processResponse struct {
	value processReady
	err   error
}

// processSupervisorCensus drains private evidence independently of target stdio
// and returns a joiner that preserves cleanup failure as a separate verdict.
// origin: S10 internal review rv-harness F5; S10 LINT-L4 responsibility split
func processSupervisorCensus(cmd, helper *exec.Cmd, helperFD int, readyR *os.File) (<-chan processResponse, func() error) {
	// Drain both startup and terminal census on the private pipe. Retaining
	// target stderr would prevent EOF; waiting until helper.Wait to drain a
	// large census could instead deadlock the reaper's final write.
	// origin: S10 internal review rv-harness F5
	startup, terminal := make(chan processResponse, 1), make(chan processResponse, 1)
	go func() {
		decoder := gob.NewDecoder(readyR)
		var first, last processReady
		firstErr := decoder.Decode(&first)
		startup <- processResponse{first, firstErr}
		lastErr := firstErr
		if firstErr == nil {
			lastErr = decoder.Decode(&last)
		}
		_ = readyR.Close()
		terminal <- processResponse{last, lastErr}
	}()
	joinHelper := func() error {
		err := helper.Wait()
		report := <-terminal
		_ = unix.Close(helperFD)
		// The reaper cannot retain the target's stderr merely to report its
		// terminal diagnosis: that alias would hold StderrPipe EOF open.
		// Its private exit code carries this diagnosis back to the caller.
		// origin: hot-update S6 plan §9.3 / T56 / TESTFIX
		if helper.ProcessState != nil && !helper.ProcessState.Success() {
			detail := "testharness: process supervisor failed"
			if helper.ProcessState.ExitCode() == 125 {
				detail = "testharness: process exited with surviving descendants"
			}
			if report.value.Error != "" {
				detail += ": " + report.value.Error
			}
			if cmd.Stderr != nil {
				_, _ = fmt.Fprintln(cmd.Stderr, detail)
			}
			err = &processSupervisionError{detail: detail, cause: errors.Join(err, report.err)}
		} else if report.err != nil {
			err = &processSupervisionError{detail: "testharness: missing supervisor census", cause: errors.Join(err, report.err)}
		}
		return err
	}
	return startup, joinHelper
}

// processTargetWaiter collects the adopted target exactly once after its reaper
// has joined every descendant; native status cannot erase supervision failure.
// origin: S10 internal review rv-harness F5; S10 LINT-L4 responsibility split
func processTargetWaiter(cmd *exec.Cmd, closeLease func(), joinHelper func() error) func() error {
	var waitOnce sync.Once
	var waitErr error
	return func() error {
		waitOnce.Do(func() {
			waitErr = joinHelper()
			state, targetErr := cmd.Process.Wait()
			closeLease()
			cmd.ProcessState = state
			if targetErr != nil {
				waitErr = errors.Join(waitErr, targetErr)
			} else if !state.Success() {
				// Preserve supervisor failures even when the target also failed.
				// Ordinary cancellation still exposes the target's native signal.
				// origin: S10 internal review rv-harness F5
				targetErr = &exec.ExitError{ProcessState: state}
				var supervision *processSupervisionError
				if errors.As(waitErr, &supervision) {
					waitErr = errors.Join(waitErr, targetErr)
				} else {
					waitErr = targetErr
				}
			}
		})
		return waitErr
	}
}

func processEnvironment(env []string) []string {
	if env == nil {
		env = os.Environ()
	}
	clean := make([]string, 0, len(env))
	for _, entry := range env {
		if !strings.HasPrefix(entry, processSupervisorEnv+"=") {
			clean = append(clean, entry)
		}
	}
	return clean
}

func runProcessSupervisor(specFD int) int {
	// Keep the fork thread alive for the optional secondary Pdeathsig guard.
	// The lease remains authoritative across worker exec and credential changes.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return 126
	}
	callerFD, err := unix.PidfdOpen(os.Getppid(), 0)
	if err != nil {
		return 126
	}
	defer func() { _ = unix.Close(callerFD) }()
	specFile := os.NewFile(uintptr(specFD), "process-spec")
	lease := os.NewFile(uintptr(specFD+1), "process-lease")
	readyFile := os.NewFile(uintptr(specFD+2), "process-ready")
	for fd := 3; fd <= specFD+2; fd++ {
		unix.CloseOnExec(fd)
	}
	defer func() { _ = specFile.Close(); _ = lease.Close(); _ = readyFile.Close() }()
	var spec processSpec
	if err := gob.NewDecoder(specFile).Decode(&spec); err != nil || spec.ExtraFiles != specFD-3 {
		return 126
	}
	_ = specFile.Close()
	// The lease and caller pidfd own cancellation; a caller context must not
	// kill the target before the subreaper has joined its descendants.
	// origin: hot-update S6 plan §9.3
	cmd := exec.CommandContext(context.Background(), spec.Path, spec.Args[1:]...) // ctx-root: supervised target lifetime is owned by the lease.
	cmd.Args, cmd.Env = spec.Args, processEnvironment(os.Environ())
	var targetEnv []string
	for _, entry := range cmd.Env {
		if !strings.HasPrefix(entry, "GORACE=") {
			targetEnv = append(targetEnv, entry)
		}
	}
	cmd.Env = targetEnv
	if spec.RaceOptionsSet {
		cmd.Env = append(cmd.Env, "GORACE="+spec.RaceOptions)
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.SysProcAttr = spec.Attributes
	for fd := 3; fd < specFD; fd++ {
		cmd.ExtraFiles = append(cmd.ExtraFiles, os.NewFile(uintptr(fd), "process-inherited"))
	}
	if err := cmd.Start(); err != nil {
		_ = gob.NewEncoder(readyFile).Encode(processReady{Error: err.Error()})
		return 126
	}
	defer func() { _ = cmd.Process.Release() }()
	for _, file := range cmd.ExtraFiles {
		_ = file.Close()
	}
	// Only the target owns its stdio endpoints. In particular a child may
	// close stdout, then remain alive while the caller verifies its identity.
	// Keeping a supervisor alias prevents EOF and deadlocks that protocol.
	// Reserve 0–2 with /dev/null so later runtime opens cannot reuse them.
	// origin: hot-update S6 plan §6.3 / T48 / TESTFIX
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err == nil {
		for fd := 0; fd < 3; fd++ {
			err = errors.Join(err, unix.Dup3(int(null.Fd()), fd, unix.O_CLOEXEC))
		}
		_ = null.Close()
	}
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = reapSupervisorDescendants(cmd.Process.Pid)
		return 126
	}
	encoder := gob.NewEncoder(readyFile)
	if err := encoder.Encode(processReady{PID: cmd.Process.Pid}); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = reapSupervisorDescendants(cmd.Process.Pid)
		return 126
	}
	leaseDone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, lease); close(leaseDone) }()
	waited := make(chan error, 1)
	go func() {
		var info unix.Siginfo
		var err error
		for {
			err = unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
			if !errors.Is(err, unix.EINTR) {
				break
			}
		}
		waited <- err
	}()
	// A terminal leader can orphan a revoked PTY job while the target keeps
	// running. Adoption makes that job our child: the target cannot wait for it.
	// Reap this ordinary duty during the target's lifetime rather than leaving
	// our own zombies for the final missed-duty census.
	// origin: S7 internal review FIX-ZOMBIE F1
	reapTick := time.NewTicker(10 * time.Millisecond)
waiting:
	for {
		select {
		case err := <-waited:
			if err != nil {
				panic(fmt.Sprintf("testharness: target wait: %v", err))
			}
			break waiting
		case <-leaseDone:
			_ = cmd.Process.Kill()
			if err := <-waited; err != nil {
				panic(fmt.Sprintf("testharness: target wait: %v", err))
			}
			break waiting
		case <-reapTick.C:
			if err := reapExitedSupervisorDescendants(cmd.Process.Pid); err != nil {
				panic(fmt.Sprintf("testharness: adopted child wait: %v", err))
			}
		}
	}
	reapTick.Stop()
	// Descendants that setsid, drop cwd/argv references, or lose an intermediate
	// parent are adopted here. Never wait4(-1) in the caller: that would steal
	// another fixture's business-child terminal status.
	leaked := reapSupervisorDescendants(cmd.Process.Pid)
	report := processReady{}
	if leaked != nil {
		report.Error = leaked.Error()
	}
	// The caller drains this independently of target stdio and joins the read.
	// Owner death may break the pipe, but must never prevent descendant reaping.
	reportErr := encoder.Encode(report)
	_ = readyFile.Close()
	caller := []unix.PollFd{{Fd: int32(callerFD), Events: unix.POLLIN}}
	for {
		_, err := unix.Poll(caller, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			panic(fmt.Sprintf("testharness: caller pidfd: %v", err))
		}
		break
	}
	if caller[0].Revents != 0 {
		// The caller cannot reap after a fatal exit. In the ordinary path the
		// target stays a zombie until this helper exits and its caller adopts it;
		// that also prevents PID recycling throughout the final handshake. Lease
		// EOF from context cancellation still leaves a live caller to take status.
		_, _ = cmd.Process.Wait()
		return 0
	}
	if leaked != nil || reportErr != nil {
		return 125
	}
	return 0
}

// Only a zombie observed before target exit belongs to ordinary subreaper
// maintenance. Check the target AFTER reading that child's terminal evidence;
// checking once before the scan can erase a child adopted by target exit itself.
// WNOWAIT pins the target and preserves its original caller's terminal status.
// Live orphans and every descendant first observed after exit still go through
// reapSupervisorDescendants, which records PID/start ticks before cleanup.
// origin: S7 internal review FIX-ZOMBIE F1
func reapExitedSupervisorDescendants(targetPID int) error {
	children, err := supervisorChildren()
	if err != nil {
		return err
	}
	for pid := range children {
		if pid == targetPID {
			continue
		}
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return err
		}
		fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
		if fields[0] != "Z" {
			continue
		}
		var info unix.Siginfo
		for {
			err = unix.Waitid(unix.P_PID, targetPID, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil)
			if !errors.Is(err, unix.EINTR) {
				break
			}
		}
		if err != nil {
			return err
		}
		if info.Signo != 0 {
			return nil // Target exited; retain all remaining evidence for its census.
		}
		if _, err := unix.Wait4(pid, nil, unix.WNOHANG, nil); err != nil && !errors.Is(err, unix.ECHILD) && !errors.Is(err, unix.EINTR) {
			return err
		}
	}
	return nil
}

func reapSupervisorDescendants(targetPID int) error {
	var leaked error
	recorded := make(map[fixtureProcess]bool)
	for {
		children, err := supervisorChildren()
		if err != nil {
			panic(fmt.Sprintf("testharness: supervisor children: %v", err))
		}
		remaining := false
		for pid := range children {
			if pid == targetPID {
				continue
			}
			remaining = true
			stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
				continue
			}
			if err != nil {
				panic(fmt.Sprintf("testharness: supervisor census: %v", err))
			}
			fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
			process := fixtureProcess{pid: pid, ticks: fields[19]}
			// Record before Wait4 consumes a zombie's sole remaining evidence.
			// Successful emergency cleanup cannot erase the missed wait duty.
			// origin: S10 internal review rv-harness F5
			if !recorded[process] {
				leaked = errors.Join(leaked, fmt.Errorf("descendant pid=%d start_ticks=%s state=%s", pid, process.ticks, fields[0]))
				recorded[process] = true
			}
			reaped, err := unix.Wait4(pid, nil, unix.WNOHANG, nil)
			if reaped > 0 || errors.Is(err, unix.ECHILD) || errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil {
				panic(fmt.Sprintf("testharness: supervisor wait: %v", err))
			}
			if err := signalFixtureProcess(process); err != nil {
				panic(fmt.Sprintf("testharness: supervisor signal: %v", err))
			}
		}
		if !remaining {
			return leaked
		}
		time.Sleep(10 * time.Millisecond) // Poll killed/adopted children until only the terminal target remains.
	}
}

func supervisorChildren() (map[int]bool, error) {
	threads, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return nil, err
	}
	children := make(map[int]bool)
	for _, thread := range threads {
		body, err := os.ReadFile("/proc/self/task/" + thread.Name() + "/children")
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, value := range strings.Fields(string(body)) {
			pid, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			children[pid] = true
		}
	}
	return children, nil
}
