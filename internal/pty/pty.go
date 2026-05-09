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
	"syscall"

	cpty "github.com/creack/pty"
)

// Session is one allocated-but-not-yet-running PTY pair. Returned by
// Allocate; the caller exec's the child via Start.
type Session struct {
	Master *os.File // agent reads/writes raw bytes here
	slave  *os.File // bound to the child's stdio at exec time, then closed in parent

	cmd *exec.Cmd // populated by Start
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
func (s *Session) Start(argv []string, env []string, cwd string) error {
	if len(argv) == 0 {
		return fmt.Errorf("pty: argv required")
	}
	if s.slave == nil {
		return fmt.Errorf("pty: slave already consumed (Start called twice?)")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	if env != nil {
		cmd.Env = env
	}
	cmd.Stdin = s.slave
	cmd.Stdout = s.slave
	cmd.Stderr = s.slave
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid:  true,
		Setctty: true,
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("pty: start child: %w", err)
	}
	// Drop the parent-side slave fd so EOF on the master surfaces when
	// the child closes its end. (creack/pty docs / pty.Start does the
	// same.) We zero the field too so a programming-error second Start
	// returns a clear "already consumed" rather than dup-fd-leak.
	_ = s.slave.Close()
	s.slave = nil
	s.cmd = cmd
	return nil
}

// Wait blocks until the child exits and returns its exit code. A non-nil
// error is returned only for setup failures (Wait was called without a
// successful Start, child was killed by a signal, etc.). Normal non-zero
// exits return the exit code with a nil error.
func (s *Session) Wait() (int, error) {
	if s.cmd == nil {
		return -1, fmt.Errorf("pty: Wait without Start")
	}
	err := s.cmd.Wait()
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
	if s.Master == nil {
		return fmt.Errorf("pty: master closed")
	}
	if cols <= 0 || rows <= 0 {
		// 80x24 is the conventional dumb-terminal default — used when
		// ctl forgets to send size or sends nonsense; never fail
		// silently with a 0x0 PTY.
		cols, rows = 80, 24
	}
	return cpty.Setsize(s.Master, &cpty.Winsize{
		Cols: uint16(cols),
		Rows: uint16(rows),
	})
}

// Signal forwards sig to the child's process GROUP (negative pid). With
// Setsid above, the group ID equals the child's pid, so this delivers
// the signal to the entire interactive job (shell + everything it
// spawned). v1 sends only SIGINT this way (Ctrl-C semantics).
func (s *Session) Signal(sig syscall.Signal) error {
	if s.cmd == nil || s.cmd.Process == nil {
		return fmt.Errorf("pty: process not started")
	}
	pgid := s.cmd.Process.Pid
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
	if s.cmd == nil || s.cmd.Process == nil {
		return 0
	}
	return s.cmd.Process.Pid
}

// Close releases the master + slave fds. Idempotent; safe to call after
// the child has exited.
func (s *Session) Close() error {
	var firstErr error
	if s.Master != nil {
		if err := s.Master.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.Master = nil
	}
	if s.slave != nil {
		if err := s.slave.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.slave = nil
	}
	return firstErr
}
