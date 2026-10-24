package ptysession

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
)

// Record deliberately lists safe scalar fields instead of serializing an image,
// request or error. Those owners also carry bytes, secrets and user arguments.
// origin: hot-update S4 plan §9.1; P18 passive runtime receipts.
type Record struct {
	Message string
	Fields  []any
}

// Log uses the daemon's existing handler; nil permits runtimes without an audit
// sink. Logging never changes a sequence, deadline or replay responsibility.
func (r Record) Log(logger *slog.Logger) {
	if logger != nil {
		logger.Info(r.Message, r.Fields...)
	}
}

// ImageRecord reports a final exported cut, with A/Q and Base/End in their
// original byte domains. Neither revoked reasons nor control digests are data
// safe for an unconstrained log vocabulary.
func ImageRecord(phase string, generation uint64, pid string, s Image) Record {
	return Record{"agent: pty image state", []any{
		"phase", phase, "generation", generation, "pid", pid,
		"input_applied", s.Input.Base, "input_accepted", s.Input.End,
		"input_fin", s.InputFIN, "input_ended", s.InputEnded,
		"output_base", s.Output.Base, "output_end", s.Output.End, "output_fin", s.Output.FIN,
		"control_seq", s.ControlSeq, "ka_seq", s.LastKASeq,
		"ka_deadline_boot_ns", s.KADeadlineBootNS, "revoked", s.Revoked != "", "master_closed", s.MasterClosed,
	}}
}

// ActorImageRecord reads the imported owner, including changes at activation,
// rather than the initial bundle. The actor lock makes the counters agree with
// the same accounting boundary used by its image exporter.
func ActorImageRecord(a *Actor, phase string, generation uint64, pid string) Record {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ImageRecord(phase, generation, pid, auditImageLocked(a))
}

func auditImageLocked(a *Actor) Image {
	s := a.state
	s.Input, s.Output = a.cfg.Input.Snapshot(), a.cfg.Output.Snapshot()
	return s
}

// ControlRecord is called only for a newly committed non-KA sequence. Unknown
// error text is collapsed to a token; it must never become a user-data sink.
func ControlRecord(pid, op string, receipt ControlReceipt) Record {
	outcome := "ok"
	if receipt.Error != "" {
		outcome = "error"
		for _, token := range []string{"ioctl_error", "signal_error", "close_error", "authority_expired"} {
			if receipt.Error == "pty control failed: "+token {
				outcome = token
				break
			}
		}
	}
	return Record{"agent: pty control committed", []any{"pid", pid, "seq", receipt.Seq, "op", op, "outcome", outcome}}
}

// StartedRecord hashes the request identity so correlation does not disclose a
// caller-supplied value. The original child identity remains available to drills.
func StartedRecord(pid, requestID string, osPID int, ticks int64) Record {
	return Record{"agent: run reliable=true started", []any{"pid", pid,
		"request_id_hash", fmt.Sprintf("%x", sha256.Sum256([]byte(requestID))),
		"os_pid", osPID, "start_time_ticks", ticks}}
}

// ExitedRecord reports the terminal byte responsibility after the pumps ended.
// Accepted can exceed applied on an abort, and FIN must never be inferred.
func ExitedRecord(pid string, code, signal int, s Image) Record {
	return Record{"agent: run reliable=true exited", []any{"pid", pid, "code", code, "signal", signal,
		"input_applied", s.Input.Base, "input_accepted", s.Input.End, "input_fin", s.InputFIN, "input_ended", s.InputEnded,
		"output_base", s.Output.Base, "output_end", s.Output.End, "output_fin", s.Output.FIN}}
}

// ActorExitedRecord observes the terminal owner without taking or closing any
// resource. It shares the actor accounting lock with freeze and input writes.
func ActorExitedRecord(a *Actor, pid string, code, signal int) Record {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ExitedRecord(pid, code, signal, auditImageLocked(a))
}

// SettledRecord carries the terminal status and actual byte responsibility at
// the successful terminal-ack commit. Base is the acknowledged offset, not an
// inferred copy of End; an abort may still leave input Applied < Accepted.
// origin: hot-update S4 plan §3 / §9.1; P18 settlement receipts.
func SettledRecord(pid string, code, signal int, applied, accepted, base, end uint64) Record {
	return Record{"agent: run reliable=true settled", []any{"pid", pid, "code", code, "signal", signal,
		"input_applied", applied, "input_accepted", accepted, "output_base", base, "output_end", end}}
}
