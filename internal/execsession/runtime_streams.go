// origin: hot-update S6 plan §9.3 / T53; split by responsibility, preserving ownership.
package execsession

import (
	"errors"
	"io"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/workload"
)

// outputsEnded is evaluated only for a task whose pumps were all built: a
// setup failure marks its unbuilt streams aborted and makes observe ready at
// once, so no abort check belongs here (S0 internal review G4 removed one
// that could never be reached).
//
// A DISCARDING stream (D11: truncated for its consumer, pump still draining
// the live pipe) is not ended: its ReadError names the truncation, but the
// child and the descendants holding the pipe are alive, and treating it as
// ended would release the AdoptDraining reap hold while they live — the
// leader would be reaped under a descendant that still writes, and a later
// Revoke could not reach that descendant. The pump clears Discarding at the
// true EOF. origin: S2 internal review F1.
func outputsEnded(t *task) bool {
	for _, output := range t.output {
		if output == nil {
			return false
		}
		state := output.Snapshot()
		if !state.Replay.FIN && (state.ReadError == "" || state.Discarding) {
			return false
		}
	}
	return true
}

// streamAbort reports why output stream i ended without a true EOF, or ""
// while it is live or ended by EOF. A pump-recorded read failure and a runtime
// abort are the same protocol fact. Callers read it BEFORE the log snapshot:
// once non-empty the pump has exited (a read failure is recorded in the same
// critical section as its last Append), so End cannot move afterwards.
func streamAbort(t *task, i int) string {
	if t.abort[i] != "" {
		return t.abort[i]
	}
	if t.output[i] == nil {
		return ""
	}
	state := t.output[i].Snapshot()
	if state.Replay.FIN {
		return "" // a true EOF is never reported as an abort
	}
	return state.ReadError
}

// streamSettled: after this request's ACK was applied, stream i is collectable
// only when the client holds every byte up to its final offset — a true EOF
// the client echoed as FIN, or a retained abort acknowledged where the source
// stopped (fin is then false: acknowledge refused a FIN claim on a non-FIN log).
func streamSettled(t *task, i int, fin bool) bool {
	log := t.logs[i+1]
	if log == nil {
		return fin // allocate never built this log: an empty true EOF (partial-allocation path)
	}
	abort := streamAbort(t, i) // before the snapshot; see streamAbort
	state := log.Snapshot()
	if state.Base != state.End {
		return false
	}
	if fin {
		return state.FIN
	}
	return abort != ""
}

func acknowledge(t *task, req proto.ProcessResumeReq) error {
	acks := [2]uint64{req.StdoutACK, req.StderrACK}
	fins := [2]bool{req.StdoutFIN, req.StderrFIN}
	// Validate both streams before releasing either prefix.
	for i := range 2 {
		state := workload.ReplayState{FIN: true}
		if t.logs[i+1] != nil {
			state = t.logs[i+1].Snapshot()
		}
		if acks[i] < state.Base || acks[i] > state.End || (fins[i] && (!state.FIN || acks[i] != state.End)) {
			return workload.ErrReplayOffset
		}
	}
	for i := range 2 {
		if t.logs[i+1] != nil {
			if err := t.logs[i+1].Ack(acks[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func outputBatch(t *task, i int) (*proto.ProcessOutput, error) {
	log := t.logs[i+1]
	if log == nil {
		return &proto.ProcessOutput{FIN: true}, nil // partial-allocation path: an empty true EOF
	}
	abort := streamAbort(t, i) // before the snapshot; see streamAbort
	state := log.Snapshot()
	buf := make([]byte, min(uint64(32<<10), state.End-state.Base))
	n, err := log.Read(state.Base, buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	batch := &proto.ProcessOutput{Offset: state.Base, Data: buf[:n]}
	// Both end markers apply exactly at Offset+len(Data) and only in the batch
	// that reaches End: a client that sees either already holds every retained
	// byte. They are mutually exclusive by construction — an abort never sets
	// Replay.FIN and a true EOF is never reported as an abort. An Error on a
	// non-final batch would make the client treat 32 KiB as the stream's end
	// and refuse the rest as "bytes after abort".
	if state.Base+uint64(n) == state.End {
		batch.FIN = state.FIN
		if !state.FIN {
			batch.Error = abort
		}
	}
	return batch, nil
}

func closeTask(t *task) {
	if t.pty != nil {
		_ = t.pty.Close()
	}
	if t.input != nil {
		_ = t.input.Close()
	}
	for _, output := range t.output {
		if output != nil {
			_ = output.Close()
		}
	}
	for _, log := range t.logs {
		if log != nil {
			_ = log.Close()
		}
	}
}
