package ptysession

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

func (a *Actor) inputStatusLocked() InputStatus {
	s := a.cfg.Input.Snapshot()
	return InputStatus{Applied: s.Base, Accepted: s.End, Window: uint64(s.Capacity) - (s.End - s.Base), FIN: a.state.InputFIN, Ended: a.state.InputEnded, Error: a.state.InputError}
}

func (a *Actor) InputStatus() InputStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inputStatusLocked()
}

// AcceptInput compares retained overlap, trims already-applied bytes, then
// accepts the entire new suffix or none. Credit and ACK are different facts.
// origin: hot-update S4 plan §3.2 / T11
func (a *Actor) AcceptInput(seq uint64, data []byte, fin bool) (InputStatus, error) {
	a.mu.Lock()
	err := a.acceptInputLocked(seq, data, fin)
	var aborted *InputAbortError
	join := errors.As(err, &aborted)
	done, closed := a.write.done, a.state.MasterClosed
	a.mu.Unlock()
	if join {
		// The request owns this join; no background recovery or Manager guard
		// can publish an abort with counters that its writer will later change.
		// origin: hot-update S4 internal review rv-eof rv-eof-2
		if !closed {
			_ = a.cfg.Master.SetWriteDeadline(time.Now())
		}
		<-done
	}
	return a.InputStatus(), err
}

func (a *Actor) acceptInputLocked(seq uint64, data []byte, fin bool) error {
	if a.phase == phaseClosed {
		return os.ErrClosed
	}
	if a.state.Revoked != "" {
		return ErrRevoked
	}
	if a.phase != phaseActive {
		return workload.ErrFrozen
	}
	if len(data) > upgradebudget.PTYBatchBytes || uint64(len(data)) > math.MaxUint64-seq {
		return ErrConflict
	}
	s := a.cfg.Input.Snapshot()
	end := seq + uint64(len(data))
	if seq > s.End || (a.state.InputFIN && (end > s.End || (fin && end != s.End))) || (fin && end < s.End) {
		return ErrConflict
	}
	// Prefixes below A have left the ring and must never be compared with
	// subsequently reused physical bytes. Every still-retained overlap is exact.
	from, to := max(seq, s.Base), min(end, s.End)
	if from < to {
		retained := make([]byte, int(to-from))
		n, err := a.cfg.Input.Read(from, retained)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n != len(retained) || !bytes.Equal(retained, data[from-seq:to-seq]) {
			return ErrConflict
		}
	}
	if (a.state.InputEnded || a.inputClosing != "") && end > s.End {
		return &InputAbortError{Cause: errors.New("ptysession: input ended")}
	}
	if end > s.End {
		suffix := data[s.End-seq:]
		if uint64(len(suffix)) > uint64(s.Capacity)-(s.End-s.Base) {
			return workload.ErrBackpressure
		}
		n, err := a.cfg.Input.Append(suffix)
		if err != nil || n != len(suffix) {
			a.abortInputLocked("replay_error")
			return &InputAbortError{Cause: fmt.Errorf("ptysession: retain input: %w", errors.Join(err, io.ErrShortWrite))}
		}
	}
	if fin && !a.state.InputFIN {
		if a.state.InputEnded || a.inputClosing != "" {
			return &InputAbortError{Cause: errors.New("ptysession: input ended")}
		}
		if err := a.cfg.Input.Finish(); err != nil {
			a.abortInputLocked("replay_error")
			return &InputAbortError{Cause: err}
		}
		a.state.InputFIN = true
		s = a.cfg.Input.Snapshot()
		if s.Base == s.End {
			a.state.InputEnded = true
		}
	}
	return nil
}

func (a *Actor) writeLoop(stop <-chan struct{}) {
	defer a.pumpDone(&a.write, false)
	buf := make([]byte, upgradebudget.PTYBatchBytes)
	for {
		a.mu.Lock()
		s, changed := a.cfg.Input.Observe()
		if stopped(stop) || a.inputClosing != "" || a.state.InputEnded || a.state.Revoked != "" || a.state.MasterClosed {
			a.mu.Unlock()
			return
		}
		if a.state.InputFIN && s.Base == s.End {
			// Logical FIN is neither VEOF nor a terminal close. The read side lives on.
			// origin: hot-update S4 plan §3.2 / T05
			a.state.InputEnded = true
			a.mu.Unlock()
			return
		}
		n, err := a.cfg.Input.Read(s.Base, buf)
		a.mu.Unlock()
		if err != nil && !errors.Is(err, io.EOF) {
			a.mu.Lock()
			a.abortInputLocked("replay_error")
			a.mu.Unlock()
			return
		}
		if n == 0 {
			select {
			case <-changed:
			case <-stop:
			}
			continue
		}
		if !a.inputAuthorized() {
			return
		}
		written, writeErr := a.cfg.Master.Write(buf[:n])
		a.mu.Lock()
		// The byte receipt precedes every error and pause decision. In particular a
		// deadline can return a real short write whose suffix belongs to the image.
		// origin: hot-update S4 plan §4.1 / T04
		if written > 0 {
			if err := a.cfg.Input.Ack(s.Base + uint64(written)); err != nil {
				a.abortInputLocked("replay_error")
			}
		}
		if writeErr != nil && !errors.Is(writeErr, os.ErrDeadlineExceeded) {
			a.abortInputLocked("write_error")
		}
		if written == 0 && writeErr == nil {
			a.abortInputLocked("write_error")
		}

		// Publishing the final applied byte and logical Ended is one accounting
		// step; a concurrent source end must not invent an unapplied-input error.
		applied := a.cfg.Input.Snapshot()
		if a.state.InputFIN && applied.Base == applied.End && a.state.InputError == "" && a.inputClosing == "" {
			a.state.InputEnded = true
		}
		a.mu.Unlock()
	}
}

// Authority is checked immediately before admitting another kernel write.
// Revocation is recorded before cleanup, including when the clock is broken.
func (a *Actor) inputAuthorized() bool {
	a.mu.Lock()
	deadline := a.state.AuthorityDeadlineBootNS
	revoked := a.state.Revoked != ""
	a.mu.Unlock()
	if revoked {
		return false
	}
	if deadline == 0 {
		return true
	}
	now, err := a.cfg.Clock()
	if err != nil || now >= deadline {
		a.mu.Lock()
		if a.state.Revoked == "" {
			a.state.Revoked = "authority expired"
			a.state.CleanupPending = true
		}
		a.abortInputLocked("authority_expired")
		a.mu.Unlock()
		return false
	}
	return true
}
