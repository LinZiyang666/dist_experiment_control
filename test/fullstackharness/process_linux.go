//go:build linux

// process_linux.go adds owner deathwatch and remote leases to FIX-ORPHAN.
package fullstackharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func init() { launcherImplementation = runLauncherLinux }

func processIdentity(pid int) (ProcessIdentity, error) {
	base := "/proc/" + strconv.Itoa(pid)
	stat, err := os.ReadFile(base + "/stat")
	if err != nil {
		return ProcessIdentity{}, err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return ProcessIdentity{}, errors.New("fullstack supervisor: malformed process census")
	}
	fields := strings.Fields(string(stat)[end+1:])
	if len(fields) < 20 {
		return ProcessIdentity{}, errors.New("fullstack supervisor: incomplete process census")
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 {
		return ProcessIdentity{}, errors.New("fullstack supervisor: invalid process start time")
	}
	namespace, err := os.Readlink(base + "/ns/pid")
	if err != nil {
		return ProcessIdentity{}, err
	}
	cgroup, err := os.ReadFile(base + "/cgroup")
	if err != nil || len(cgroup) == 0 {
		return ProcessIdentity{}, errors.New("fullstack supervisor: missing process cgroup")
	}
	return ProcessIdentity{PID: pid, StartTicks: ticks, PIDNS: namespace, Cgroup: string(cgroup)}, nil
}

func registeredCgroup(identity ProcessIdentity, expected string) bool {
	if expected == "" || !strings.HasPrefix(expected, "/") {
		return false
	}
	for _, line := range strings.Split(identity.Cgroup, "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) == 3 && fields[2] == expected {
			return true
		}
	}
	return false
}

func runLauncherLinux(ctx context.Context, template *exec.Cmd, options LauncherOptions) error {
	if options.Lease != nil {
		lease, err := takeRemoteLease(options.Lease)
		if err != nil {
			return err
		}
		options.Lease = lease
		defer func() { _ = options.Lease.Close() }()
	}
	if ready, ok := options.Ready.(*os.File); ok {
		connection, err := ready.SyscallConn()
		if err != nil {
			return err
		}
		if err := connection.Control(func(fd uintptr) { unix.CloseOnExec(int(fd)) }); err != nil {
			return err
		}
	}
	if ctx == nil || template == nil || template.Path == "" || len(template.Args) == 0 || template.Process != nil {
		return errors.New("fullstack supervisor: incomplete launcher command")
	}
	if template.Err != nil {
		return template.Err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	identity, err := processIdentity(os.Getpid())
	if err != nil {
		return err
	}
	if options.Cgroup != "" && !registeredCgroup(identity, options.Cgroup) {
		return errors.New("fullstack supervisor: launcher cgroup does not match registered owner")
	}
	// An open remote stdin can outlive a dead docker client. A renewable deadline,
	// checked before spawning, covers that case as well as explicit lease EOF.
	// origin: hot-update S10 plan §6 / P05 remote child control.
	if options.Lease != nil {
		if options.LeaseTimeout <= 0 || !registeredCgroup(identity, options.Cgroup) {
			return errors.New("fullstack supervisor: remote lease requires deadline and registered cgroup")
		}
		if err := readLeasePulse(options.Lease, options.LeaseTimeout); err != nil {
			return err
		}
	}
	parent := os.Getppid()
	if parent <= 1 {
		return errors.New("fullstack supervisor: launcher has no live owner")
	}
	parentFD, err := unix.PidfdOpen(parent, 0)
	if err != nil {
		return fmt.Errorf("fullstack supervisor: owner pidfd: %w", err)
	}
	defer func() { _ = unix.Close(parentFD) }()
	// Reparenting between observation and pidfd_open must not bind the watch to
	// an unrelated owner. pidfds prevent reuse after this acquisition.
	if os.Getppid() != parent {
		return errors.New("fullstack supervisor: owner changed before deathwatch")
	}
	// An explicit signal needs a target handler window; owner/lease loss still
	// cancels the shared reaper immediately. Keeping that distinction here
	// avoids treating intentional TERM as an unannounced SIGKILL.
	// origin: S10 internal review FIX-SUPERVISE-2 F2
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	joinWatch := watchLauncherOwner(runCtx, cancel, parentFD, options)
	defer cancel()
	cmd := exec.CommandContext(runCtx, template.Path, template.Args[1:]...)
	cmd.Args, cmd.Env, cmd.Dir = template.Args, template.Env, template.Dir
	cmd.Stdin, cmd.Stdout, cmd.Stderr = template.Stdin, template.Stdout, template.Stderr
	cmd.ExtraFiles, cmd.SysProcAttr, cmd.WaitDelay = template.ExtraFiles, template.SysProcAttr, template.WaitDelay
	wait, err := StartProcess(cmd)
	if err != nil {
		return errors.Join(err, joinWatch())
	}
	template.Process = cmd.Process
	joinCancellation := watchLauncherCancellation(ctx, cancel, cmd.Process)
	if options.Ready != nil {
		target, censusErr := processIdentity(cmd.Process.Pid)
		if censusErr == nil && options.Cgroup != "" && !registeredCgroup(target, options.Cgroup) {
			censusErr = errors.New("fullstack supervisor: target escaped registered cgroup before READY")
		}
		if censusErr == nil {
			censusErr = json.NewEncoder(options.Ready).Encode(ProcessReady{Launcher: identity, Target: target})
		}
		if censusErr != nil {
			err = censusErr
			cancel()
		}
	}
	// Join is unconditional, including READY failure. Emergency reaping's 125
	// remains a failure even when no process survives the final census.
	// origin: hot-update S10 plan §6 / P05 lost waiter and swallowed 125 controls.
	waitErr := wait()
	template.ProcessState = cmd.ProcessState
	return errors.Join(waitErr, err, joinCancellation(), joinWatch())
}

// Forward only an explicit signal cause. Generic context cancellation, remote
// lease expiry and owner death retain their original immediate kill/reap duty.
// A target that ignores the signal loses its lease after the bounded grace;
// any resulting survivor still goes through the shared native125 census.
// origin: S10 internal review FIX-SUPERVISE-2 F2 / F3
func watchLauncherCancellation(ctx context.Context, cancel context.CancelFunc, target *os.Process) func() error {
	stop, done := make(chan struct{}), make(chan struct{})
	var fault error
	go func() {
		defer close(done)
		select {
		case <-stop:
			return
		case <-ctx.Done():
		}
		var source interface{ Signal() syscall.Signal }
		if !errors.As(context.Cause(ctx), &source) {
			cancel()
			return
		}
		// os.Process uses its pidfd, so target exit/reuse cannot redirect TERM
		// into another fixture. Never signal the helper's process group.
		if err := target.Signal(source.Signal()); err != nil && !errors.Is(err, os.ErrProcessDone) {
			fault = err
			cancel()
			return
		}
		grace := time.NewTimer(5 * time.Second)
		defer grace.Stop()
		select {
		case <-stop:
		case <-grace.C:
			cancel()
		}
	}()
	return func() error {
		close(stop)
		<-done
		return fault
	}
}

// Inherited descriptors arrive in blocking mode. Re-wrap a CLOEXEC duplicate
// after setting nonblocking mode so Go's poller can enforce heartbeat deadlines;
// File.Fd would put it back into blocking mode and silently disable that fence.
// origin: hot-update S10 plan §6 / P05 remote lease deadline.
func takeRemoteLease(original *os.File) (*os.File, error) {
	defer func() { _ = original.Close() }()
	connection, err := original.SyscallConn()
	if err != nil {
		return nil, err
	}
	fd := -1
	var duplicateErr error
	err = connection.Control(func(value uintptr) {
		fd, duplicateErr = unix.FcntlInt(value, unix.F_DUPFD_CLOEXEC, 3)
	})
	if err != nil || duplicateErr != nil {
		return nil, errors.Join(err, duplicateErr)
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return os.NewFile(uintptr(fd), "fullstack-remote-lease"), nil
}

func readLeasePulse(lease *os.File, timeout time.Duration) error {
	if err := lease.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return errors.New("fullstack supervisor: remote lease must support read deadlines")
	}
	var pulse [1]byte
	if _, err := io.ReadFull(lease, pulse[:]); err != nil {
		return fmt.Errorf("fullstack supervisor: remote lease ended: %w", err)
	}
	if pulse[0] != '.' {
		return errors.New("fullstack supervisor: invalid remote lease pulse")
	}
	return nil
}

func watchLauncherOwner(ctx context.Context, cancel context.CancelFunc, parentFD int, options LauncherOptions) func() error {
	var workers sync.WaitGroup
	var fault error
	var faultOnce sync.Once
	fail := func(err error) { faultOnce.Do(func() { fault = err; cancel() }) }
	workers.Go(func() {
		poll := []unix.PollFd{{Fd: int32(parentFD), Events: unix.POLLIN}}
		for ctx.Err() == nil {
			_, err := unix.Poll(poll, 20)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if err != nil || poll[0].Revents != 0 {
				fail(errors.New("fullstack supervisor: launcher owner died"))
				return
			}
		}
	})
	if options.Lease != nil {
		workers.Go(func() {
			for ctx.Err() == nil {
				if err := readLeasePulse(options.Lease, options.LeaseTimeout); err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
			}
		})
	}
	return func() error {
		cancel()
		if options.Lease != nil {
			_ = options.Lease.Close() // Wake a remote read immediately, then join it.
		}
		workers.Wait()
		return fault
	}
}
