package execsession

import (
	"log/slog"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/workload"
)

// LogPTYImage reads only the final Prepare re-export; the caller holds the cut
// latch until exec or thaw. The registry (and its secret) never enters a record.
// origin: hot-update S4 plan §4.2 / §9.1; P18.
func LogPTYImage(logger *slog.Logger, phase string, generation uint64, img Image) {
	for _, t := range img.Tasks {
		if t.PTY != nil {
			st := *t.PTY
			if t.Revoked != "" {
				st.Revoked = t.Revoked
			}
			ptysession.ImageRecord(phase, generation, t.PID, st).Log(logger)
		}
	}
}

// LogPTYActivation observes imported owners after successful activation, never
// the bundle retained at boot. Reconnect/import cannot renew a logged deadline.
func (r *Runtime) LogPTYActivation(generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.sortedTasks() {
		if t.pty != nil {
			ptysession.ActorImageRecord(t.pty, "activated", generation, t.pid).Log(r.cfg.Logger)
		} else if t.ptyState != nil {
			ptysession.ImageRecord("activated", generation, t.pid, *t.ptyState).Log(r.cfg.Logger)
		}
	}
}

// finish holds Runtime.mu and has already committed the terminal result. Failed
// pre-spawn tasks have no child identity and must not claim a lifecycle exit.
func logPTYExit(r *Runtime, t *task, exit workload.Exit) {
	if t.pty != nil {
		ptysession.ActorExitedRecord(t.pty, t.pid, exit.Code, exit.Signal).Log(r.cfg.Logger)
	} else if t.ptyState != nil && t.ptyState.Identity.StartTimeTicks != 0 {
		ptysession.ExitedRecord(t.pid, exit.Code, exit.Signal, *t.ptyState).Log(r.cfg.Logger)
	}
}

// LogPTYCarrier observes a real session teardown or successful register, never
// a dial/reconnect attempt. A failed setup can tear down another connection;
// the pending loss remains one responsibility until a session is established.
// The flag is local transport state, not authority or part of the image ABI.
// origin: hot-update S4 plan §6.1 / §9.1; P18 carrier receipts.
func (r *Runtime) LogPTYCarrier(phase string, generation uint64) {
	// Non-Linux agents keep the legacy path when the reliable runtime is
	// unavailable. An observability call must not change that admission result.
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch phase {
	case "carrier-lost":
		if r.ptyCarrierLost {
			return
		}
		r.ptyCarrierLost = true
	case "carrier-restored":
		if !r.ptyCarrierLost {
			return
		}
		r.ptyCarrierLost = false
	default:
		return
	}
	for _, t := range r.sortedTasks() {
		if t.pty != nil {
			ptysession.ActorImageRecord(t.pty, phase, generation, t.pid).Log(r.cfg.Logger)
		} else if t.ptyState != nil {
			ptysession.ImageRecord(phase, generation, t.pid, *t.ptyState).Log(r.cfg.Logger)
		}
	}
}

// Called under Runtime.mu only AFTER Settle succeeds and before retirement
// closes replay files. OutputBatch has applied the real final ACK already.
// origin: hot-update S4 plan §3 / §9.1; P18 settlement receipts.
func logPTYSettlement(r *Runtime, t *task, terminal procrequest.Terminal) {
	input := t.ptyState
	var applied, accepted uint64
	if t.pty != nil {
		status := t.pty.InputStatus()
		applied, accepted = status.Applied, status.Accepted
	} else if input != nil {
		applied, accepted = input.Input.Base, input.Input.End
	}
	output := t.logs[1].Snapshot()
	ptysession.SettledRecord(t.pid, terminal.ExitCode, terminal.Signal, applied, accepted, output.Base, output.End).Log(r.cfg.Logger)
}

// A tombstone has no replay owner. Its successful ACK must exactly match the
// retained final End; record that validated ACK rather than fabricating a Base.
func logPTYDiscardedSettlement(r *Runtime, pid string, req proto.ProcessResumeReq, ts *tombstone) {
	if ts.kind == proto.ProcessKindPTY && ts.pty != nil {
		ptysession.SettledRecord(pid, ts.terminal.ExitCode, ts.terminal.Signal,
			ts.pty.InputACK, ts.pty.InputAccepted, req.StdoutACK, ts.finalEnd[0]).Log(r.cfg.Logger)
	}
}
