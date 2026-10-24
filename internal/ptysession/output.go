package ptysession

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

// OutputBatch carries merged stdout/stderr. A final error belongs only to the
// final retained prefix, never to an earlier batch or the diagnostic Pending.
func (a *Actor) OutputBatch(ack uint64, fin bool, limit int) (Batch, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.phase == phaseClosed {
		return Batch{}, os.ErrClosed
	}
	if a.phase != phaseActive {
		return Batch{}, workload.ErrFrozen
	}
	s := a.cfg.Output.Snapshot()
	if ack > s.End || (fin && (ack != s.End || (!s.FIN && a.state.OutputError == ""))) {
		return Batch{}, workload.ErrReplayOffset
	}
	if err := a.cfg.Output.Ack(ack); err != nil {
		return Batch{}, err
	}
	offset := max(ack, s.Base)
	if limit <= 0 || limit > upgradebudget.PTYBatchBytes {
		limit = upgradebudget.PTYBatchBytes
	}
	buf := make([]byte, min(uint64(limit), s.End-offset))
	n, err := a.cfg.Output.Read(offset, buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return Batch{}, err
	}
	b := Batch{Offset: offset, Data: buf[:n]}
	if offset+uint64(n) == s.End {
		b.FIN = s.FIN
		b.Error = a.state.OutputError
	}
	return b, nil
}

func (a *Actor) readLoop(stop <-chan struct{}) {
	defer a.pumpDone(&a.read, true)
	buf := make([]byte, upgradebudget.PTYBatchBytes)
	for {
		a.mu.Lock()
		s, changed := a.cfg.Output.Observe()
		if stopped(stop) || a.source == sourceEnded || a.state.MasterClosed {
			a.mu.Unlock()
			return
		}
		discard := a.source == sourceDiscarding
		free := int(s.Capacity) - int(s.End-s.Base)
		a.mu.Unlock()
		if !discard && free == 0 {
			select {
			case <-changed:
			case <-stop:
			}
			continue
		}
		size := len(buf)
		if !discard {
			size = min(size, free)
		}
		n, err := a.cfg.Master.Read(buf[:size])
		a.mu.Lock()
		// This sole reader is also the finalizer. It records n before publishing
		// Error, so no other in-flight read can extend End after logical abort.
		// Pending is diagnostic responsibility and is never appended on resume.
		// origin: hot-update S4 plan §3.3 / T13
		if n > 0 && a.source == sourceReading {
			written, retainErr := a.cfg.Output.Append(buf[:n])
			if retainErr != nil || written != n {
				a.state.Pending = append([]byte(nil), buf[written:n]...)
				a.state.OutputError = fmt.Sprintf("retain output: wrote %d of %d: %v", written, n, retainErr)
				a.source = sourceDiscarding
			}
		}
		ended := false
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			ended = true
			clean := errors.Is(err, io.EOF) || errors.Is(err, syscall.EIO)
			// Only a successfully adopted, still-owned master may interpret read EIO
			// as last-slave EOF. Write EIO and an explicit close never enter this arm.
			if clean && !a.state.MasterClosed && a.cfg.Identity.Started {
				if a.state.OutputError == "" {
					if e := a.cfg.Output.Finish(); e != nil {
						a.state.OutputError = e.Error()
					}
				}
				a.source = sourceEnded
				a.state.SourceEnded = true
			} else {
				if a.state.OutputError == "" {
					a.state.OutputError = "pty output aborted: " + err.Error()
				}
				a.source = sourceEnded
			}
			a.abortInputLocked("source_ended")
		}
		a.mu.Unlock()
		if ended {
			a.finishSource()
			return
		}
	}
}

// A zombie leader is deliberately retained until the last slave ends; there
// is no timer here and no dependency on when the leader itself exited.
// origin: hot-update S4 plan §3.3 / T02
func (a *Actor) finishSource() {
	a.mu.Lock()
	done, closed := a.write.done, a.state.MasterClosed
	a.mu.Unlock()
	// Stop intake first, then wake and join the exact writer generation outside
	// both accounting and Manager locks. Its last n must precede close and reap.
	// origin: hot-update S4 internal review rv-eof rv-eof-1 / rv-fd rv-fd-1
	if !closed {
		_ = a.cfg.Master.SetWriteDeadline(time.Now())
	}
	<-done
	a.mu.Lock()
	closeMaster := !a.state.MasterClosed
	a.state.MasterClosed = true
	a.mu.Unlock()
	if closeMaster {
		_ = a.cfg.Master.Close()
	}
	_ = a.releaseReap()
}

// Output EOF does not discharge administrative cleanup. Keep the leader PID
// reserved until the census has settled, or the next tick would scan a reaped
// (possibly recycled) leader and lose its authority to finish the session.
// origin: hot-update S4 plan §6.2 / T16
func (a *Actor) releaseReap() error {
	a.mu.Lock()
	release := (a.state.MasterClosed || a.state.SourceEnded) && !a.state.CleanupPending && !a.write.running && a.state.InputEnded
	a.mu.Unlock()
	if release {
		return a.cfg.Manager.AllowReap(a.cfg.PID)
	}
	return nil
}
