// Package ptysession owns reliable terminal bytes, control receipts, and the
// original master independently of the control connection's lifetime.
package ptysession

import (
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/workload"
)

// InputStatus separates applied bytes from accepted credit. Only Applied
// permits a client to release its copy; Ended may retain unapplied bytes.
// origin: hot-update S4 plan §2.3 / §3.2
type InputStatus struct {
	Applied, Accepted, Window uint64
	FIN, Ended                bool
	Error                     string
}

// InputAbortError describes an already committed input termination, not a
// refused identity, sequence, payload or capacity request. Callers must return
// its final receipt as status so the original client can drain and settle.
// origin: hot-update S4 internal review rv-eof rv-eof-2
type InputAbortError struct {
	Cause error
}

func (e *InputAbortError) Error() string { return e.Cause.Error() }
func (e *InputAbortError) Unwrap() error { return e.Cause }

// ControlRequest has a sequence independent of input, so blocked input cannot
// prevent an urgent control. A retry preserves both sequence and payload.
type ControlRequest struct {
	Seq        uint64
	Op         string
	Cols, Rows uint16
	Signal     int
}

// ControlReceipt retains failed controls too: a lost reply never repeats a
// signal or ioctl that has already consumed this sequence.
type ControlReceipt struct {
	Seq           uint64
	Digest, Error string
}

// HealthProof belongs to one connection generation and is never serialized as
// valid authority for a new generation to expire a terminal's keepalive.
type HealthProof struct {
	Generation uint64
	AtBootNS   int64
	Healthy    bool
	Verifier   HealthProofVerifier
}

// HealthProofVerifier checks the connection that produced a proof at its point
// of consumption. The owner supplies this interface without coupling terminal
// actors to the agent or its network transport. It must not perform a probe.
// origin: hot-update S4 internal review rv-auth RV-AUTH-3
type HealthProofVerifier interface {
	Current(generation uint64) bool
}

// Batch describes the merged output stream; FIN/Error apply at its final end.
type Batch struct {
	Offset uint64
	Data   []byte
	FIN    bool
	Error  string
}

// Config supplies existing owners, not a caller-controlled lifetime. Input and
// output replay capacity must be reserved before the child is started.
type Config struct {
	Manager       *workload.Manager
	PID           string
	Master        pty.Master
	Identity      pty.TerminalIdentity
	Input, Output *workload.Replay
	Clock         func() (int64, error)
	KAIntervalMS  int
}

// These phases deliberately separate ownership from source lifetime: logical
// output abort can still require discard-drain before releasing the reap hold.
// origin: hot-update S4 plan §3.3 / §4.1 / §10
type actorPhase uint8

const (
	phaseActive actorPhase = iota
	phaseFreezing
	phaseFrozen
	phaseClosed
)

type sourcePhase uint8

const (
	sourceReading sourcePhase = iota
	sourceDiscarding
	sourceEnded
)
