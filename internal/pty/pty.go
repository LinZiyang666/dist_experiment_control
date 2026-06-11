// Package pty wraps github.com/creack/pty for tether's `run` (interactive
// PTY) flow. The interesting parts (vs a one-shot `pty.Start`):
//
//   - Two-phase startup. We allocate the PTY pair separately so the
//     agent can pub `pty.<pid>.ready{cols, rows}` BEFORE fork+exec'ing
//     the child. Fork+exec only happens after ctl signals "I have
//     subscribed and I am ready to receive your bytes" — see C.5.1.
//
//   - SIGWINCH propagation. Resize events arrive on a NATS subject from
//     ctl's local terminal; the agent ioctl(TIOCSWINSZ)s the master so
//     the child's foreground process gets a real SIGWINCH from the
//     kernel.
//
//   - Process-group kill. Children are started in their own session
//     (Setsid: true), so a single `kill -PGID` on the agent side
//     delivers SIGINT to the entire interactive job (shell + everything
//     it spawned) — that's the Ctrl-C semantics ctl users expect.
//
// We deliberately do NOT use `pty.Start` because it both allocates AND
// execs in one call; we need the gap between those two events.
package pty

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"

	cpty "github.com/creack/pty"
)

// Session is one allocated-but-not-yet-running PTY pair. Returned by
// Allocate; the caller exec's the child via Start.
//
// Field access is serialized by mu so a hung-fs spawn-timeout abandon (the run
// watchdog leaves an in-flight Start goroutine parked at cmd.Start while the
// handler calls Close) cannot race Start's post-exec writes against Close's
// teardown, nor double-close the slave fd (review M1). cmd.Start itself runs
// WITHOUT mu held, so a D-state execve cannot wedge Close.
type Session struct {
	Master *os.File // agent reads/writes raw bytes here

	mu      sync.Mutex
	slave   *os.File  // bound to the child's stdio at exec time, then closed in parent
	cmd     *exec.Cmd // populated by Start
	started bool      // set when Start begins: it now OWNS the slave fd lifecycle
	closed  bool      // set by Close; Start that resumes after Close cleans up + bails
}

// Allocate creates a fresh PTY pair sized to (cols, rows) but does NOT
// fork/exec anything. The returned Session.Master is ready for read/
// write; the slave is held internally until Start.
//
// Callers MUST eventually invoke Close to free both fds, regardless of
// whether Start was ever called.
func Allocate(cols, rows int) (*Session, error) {
	master, slave, err := cpty.Open()
	if err != nil {
		return nil, fmt.Errorf("pty: open: %w", err)
	}
	s := &Session{Master: master, slave: slave}
	if err := s.SetSize(cols, rows); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// Start forks + execs argv with the PTY slave bound to its stdin/stdout/
// stderr, in its own session (Setsid + Setctty so the child becomes the
// PTY's session leader and gets real SIGWINCH from the kernel on resize).
//
// On success the parent's copy of the slave fd is closed (so EOF on the
// master happens cleanly when the child exits), and Wait() drives child
// reaping.
//
// env is in os/exec format ("KEY=VALUE"); pass nil to inherit the agent's
// environment.
//
// resolvedPath, when non-empty, is an absolute path for argv[0] pre-resolved by
// the hung-fs-safe spawn policy; the child is built as &exec.Cmd{Path, Args}
// DIRECTLY, bypassing exec.Command's LookPath (which would D-hang stat'ing a
// wedged $PATH dir). When empty the legacy exec.Command path is used unchanged.
func (s *Session) Start(argv []string, env []string, cwd string, resolvedPath string) error {
	if len(argv) == 0 {
		return fmt.Errorf("pty: argv required")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("pty: session closed")
	}
	if s.started || s.slave == nil {
		s.mu.Unlock()
		return fmt.Errorf("pty: slave already consumed (Start called twice?)")
	}
	// Take ownership of the slave fd: from here Close will NOT touch it (so its
	// close can't race cmd.Start's use of the same fd, review M1); Start always
	// closes it after cmd.Start returns.
	s.started = true
	slave := s.slave
	s.mu.Unlock()

	var cmd *exec.Cmd
	if resolvedPath != "" {
		// Args[0] keeps the ORIGINAL argv[0]; only Path is the absolute.
		cmd = &exec.Cmd{Path: resolvedPath, Args: argv}
	} else {
		cmd = exec.Command(argv[0], argv[1:]...)
	}
	if cwd != "" {
		cmd.Dir = cwd
	}
	if env != nil {
		cmd.Env = env
	}
	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid:  true,
		Setctty: true,
	}
	// cmd.Start runs WITHOUT the lock so a D-state execve cannot wedge Close.
	if err := cmd.Start(); err != nil {
		// Drop our owned slave fd (Close skipped it because started==true).
		s.mu.Lock()
		_ = slave.Close()
		s.slave = nil
		s.mu.Unlock()
		return fmt.Errorf("pty: start child: %w", err)
	}
	// Drop the parent-side slave fd now that cmd.Start has dup'd it into the
	// child (so EOF on the master surfaces when the child exits). We own it, so
	// no concurrent close from Close (review M1).
	s.mu.Lock()
	_ = slave.Close()
	s.slave = nil
	s.cmd = cmd
	closedDuringStart := s.closed
	s.mu.Unlock()
	if closedDuringStart {
		// Close ran while we were (possibly) wedged in cmd.Start; we own the
		// just-started child. Kill the whole process GROUP (Setsid ⇒ pgid==pid)
		// so any descendants it forked in its brief life die too, then Wait
		// exactly once to reap the zombie + close the cmd's fds (review F5 —
		// previously it only Killed the leader and never Waited).
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
		}
		return fmt.Errorf("pty: session closed during start")
	}
	return nil
}

// Wait blocks until the child exits and returns its exit code. A non-nil
// error is returned only for setup failures (Wait without a successful
// Start, wait syscall failure). Normal non-zero exits — including
// signal-kills, which surface as ExitCode()==-1 — return the code with
// a nil error.
func (s *Session) Wait() (int, error) {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil {
		return -1, fmt.Errorf("pty: Wait without Start")
	}
	err := cmd.Wait()
	if err == nil {
		return 0, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode(), nil
	}
	return -1, err
}

// SetSize ioctl(TIOCSWINSZ)'s the PTY master so the child sees a real
// SIGWINCH (kernel delivers it because the child is the PTY's session
// leader, see Start's SysProcAttr).
func (s *Session) SetSize(cols, rows int) error {
	s.mu.Lock()
	master := s.Master
	s.mu.Unlock()
	if master == nil {
		return fmt.Errorf("pty: master closed")
	}
	if cols <= 0 || rows <= 0 {
		// 80x24 is the conventional dumb-terminal default — used when
		// ctl forgets to send size or sends nonsense; never fail
		// silently with a 0x0 PTY.
		cols, rows = 80, 24
	}
	return cpty.Setsize(master, &cpty.Winsize{
		Cols: uint16(cols),
		Rows: uint16(rows),
	})
}

// Signal forwards sig to the child's process GROUP (negative pid). With
// Setsid above, the group ID equals the child's pid, so this delivers
// the signal to the entire interactive job (shell + everything it
// spawned). v1 sends only SIGINT this way (Ctrl-C semantics).
func (s *Session) Signal(sig syscall.Signal) error {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("pty: process not started")
	}
	pgid := cmd.Process.Pid
	if err := syscall.Kill(-pgid, sig); err != nil {
		return fmt.Errorf("pty: kill -%d -%d: %w", int(sig), pgid, err)
	}
	return nil
}

// OSPID returns the OS pid of the child after Start. Returns 0 when
// Start has not run yet (or the child is gone). Caller uses this to
// pair the agent's tether-ULID with /proc/<pid>/stat field 22 for
// the architecture G.1 PID-reuse triple.
func (s *Session) OSPID() int {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	return cmd.Process.Pid
}

// KillAndWaitAfterAbandonedStart owns the narrow timeout race where cmd.Start
// completed successfully just as the watchdog selected its timer. In that case
// Start may already have returned nil before Close marked the Session closed, so
// its closedDuringStart branch could not reap the child. The timeout reaper calls
// this method exactly once after observing that nil Start result.
func (s *Session) KillAndWaitAfterAbandonedStart() {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	_ = cmd.Wait()
}

// Close releases the master + slave fds. Idempotent; safe to call after the
// child has exited AND concurrently with an abandoned, still-wedged Start (it
// sets closed so that Start, if it later resumes, kills its child and skips the
// fd teardown Close already did — no race, no double-close; review M1).
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	var firstErr error
	if s.Master != nil {
		if err := s.Master.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.Master = nil
	}
	// Only close the slave if Start never took ownership of it. Once started,
	// Start owns the slave fd and closes it after cmd.Start returns — closing it
	// here would race cmd.Start's use of the same fd (review M1).
	if !s.started && s.slave != nil {
		if err := s.slave.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.slave = nil
	}
	return firstErr
}
