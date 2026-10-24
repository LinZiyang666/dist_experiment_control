package pty

import (
	"os"
	"time"
)

// TerminalIdentity proves the original master, separately from mutable tty
// observations. Started means successful Start and adoption, not an attempt.
// origin: hot-update S4 plan §2.3 / §3.1
type TerminalIdentity struct {
	Version        int    `json:"version"`
	BootID         string `json:"boot_id"`
	LeaderPID      int    `json:"leader_pid"`
	StartTimeTicks int64  `json:"start_time_ticks"`
	Dev            uint64 `json:"dev"`
	Ino            uint64 `json:"ino"`
	Rdev           uint64 `json:"rdev"`
	MountID        uint64 `json:"mount_id"`
	PTYIndex       uint32 `json:"pty_index"`
	Started        bool   `json:"started"`
}

// TerminalObservation is diagnostic state, never instructions to restore tty
// settings. A draining leader can be a zombie with no observable controlling tty.
type TerminalObservation struct {
	Cols            uint16 `json:"cols"`
	Rows            uint16 `json:"rows"`
	SessionID       int    `json:"session_id,omitempty"`
	ForegroundPGID  int    `json:"foreground_pgid,omitempty"`
	ControlTTYKnown bool   `json:"control_tty_known"`
	Termios         []byte `json:"termios,omitempty"`
}

// Master has one close/deadline owner and two I/O borrowers. Borrow is only
// legal while frozen; its caller must not perform I/O, Close, or call File.Fd.
// Independent deadlines prevent resuming one pump from unblocking the other.
type Master interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	Resize(cols, rows uint16) error
	Observe() (TerminalObservation, error)
	Borrow() *os.File
	Close() error
}
