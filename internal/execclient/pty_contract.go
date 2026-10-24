package execclient

import "github.com/LinZiyang666/tether/internal/proto"

// Source and Sink must wake blocked I/O on Cancel. The protocol owner joins
// its bounded workers before restoring the terminal or returning to the CLI.
// origin: hot-update S4 plan §2.3 / §7.2
type Source interface {
	Read([]byte) (int, error)
	Cancel() error
}

type Sink interface {
	Write([]byte) (int, error)
	Cancel() error
}

// ControlEvent has no sequence supplied by the caller; the single protocol
// loop assigns it and preserves the payload until its receipt is confirmed.
type ControlEvent struct {
	Op         string
	Cols, Rows uint16
	Signal     int
}

// The create boundary is irreversible: only probing may choose legacy run.
// origin: hot-update S4 plan §7.2 / §10
type ptyPhase uint8

const (
	ptyProbing ptyPhase = iota
	ptyCreating
	ptyAttached
	ptySettling
	ptyDone
)

type Result struct {
	Handle   proto.ProcessHandle
	PID      string
	ExitCode int
	Signal   int
	// TerminalError is the agent-recorded setup or wait failure of the task
	// itself. StdoutAbort/StderrAbort say why a stream ended without a true
	// EOF after StdoutBytes/StderrBytes were delivered and acknowledged. All
	// three empty means a clean run.
	TerminalError string
	StdoutAbort   string
	StderrAbort   string
	StdoutBytes   uint64
	StderrBytes   uint64
	// CreateSent is true once the create request was handed to the
	// transport. From then on the remote task may exist even if no reply
	// ever came back (a lost create reply followed by a timeout leaves PID
	// empty), so a caller must never treat the command as "never ran".
	// origin: docs/reviews/hot-update-reliable-exec-plan.md §1.1.
	CreateSent bool
	// Executed is true once there is evidence the child ran: a running
	// phase, any delivered byte, or a terminal with a signal or a real exit
	// code. A failStart shape (exit -1, signal 0, two empty EOFs, never
	// running) keeps it false. It is client-derived, not a wire fact.
	Executed bool
	// InputAbort retains accepted input that never reached the master. Only
	// InputACK permits releasing bytes; InputAccepted alone is not delivery.
	// origin: hot-update S4 plan §2.3 / §3.2
	InputAbort              string
	InputACK, InputAccepted uint64
}
