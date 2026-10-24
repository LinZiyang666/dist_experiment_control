package workload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// OutputState keeps source termination separate from receiver acknowledgement.
// Replay.FIN means a true source EOF; ReadError retains an exceptional source
// failure instead of fabricating EOF or a child exit code. Discarding marks a
// stream the session authority truncated (Discard): ReadError names the
// reason, the retained prefix stays readable, and the pump keeps draining the
// source so the child is never blocked on a pipe nobody will ever ACK.
type OutputState struct {
	Version    int         `json:"version"`
	Replay     ReplayState `json:"replay"`
	ReadError  string      `json:"read_error,omitempty"`
	Pending    []byte      `json:"pending,omitempty"`
	Discarding bool        `json:"discarding,omitempty"`
}

// Output is the sole reader of a pollable source file and the sole appender to
// its replay log. Its consumer may Read/Ack the log independently. Carrier
// lifetime is deliberately absent: lack of ACK fills the bounded log and
// backpressures the original pipe without closing it or dropping bytes.
//
// Lifecycle methods serialize. Freeze interrupts only this owner's temporary
// read deadline, then waits for every n>0 result to enter the log before it
// exports either file. No source FD is closed to obtain a handoff safe point.
//
// paused and frozen are distinct: paused says the pump goroutine has been
// told to stop (Freeze, Abort and Discard all set it), frozen says the actor
// and its log are currently handed over and refuse a second Freeze. An
// aborted stream is paused but not frozen, so Freeze still accepts it and
// exports its retained bytes and End (hot-update S2 plan §4 item 1); a frozen
// stream is both, so Freeze after Freeze is refused.
type Output struct {
	control    sync.Mutex
	mu         sync.Mutex
	source     *os.File
	log        *Replay
	done       chan struct{}
	pause      chan struct{}
	running    bool
	paused     bool
	frozen     bool
	closed     bool
	discarding bool
	readErr    string
	pending    []byte
}

// NewOutput takes ownership of source and log only on success. The caller
// supplies an unused log and a pollable file with no external deadline owner.
// Ordinary os.Pipe readers satisfy this; a raw blocking descriptor must first
// be registered with Go's poller by the domain that creates it.
func NewOutput(source *os.File, log *Replay) (*Output, error) {
	return NewOutputWithPrefix(source, log, nil)
}

// NewOutputWithPrefix retains an optional bounded startup diagnostic before
// the first source byte. It has the same ownership contract as NewOutput.
func NewOutputWithPrefix(source *os.File, log *Replay, prefix []byte) (*Output, error) {
	if source == nil || log == nil {
		return nil, fmt.Errorf("workload: missing output resource")
	}
	log.mu.Lock()
	valid := !log.closed && !log.frozen && log.state.End == 0 && !log.state.FIN
	log.mu.Unlock()
	if !valid || len(prefix) > 32<<10 || int64(len(prefix)) > log.Snapshot().Capacity {
		return nil, fmt.Errorf("workload: output requires unused replay log")
	}
	if err := validateStreamFile(source, false); err != nil {
		return nil, err
	}
	if err := source.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("workload: output source is not pollable: %w", err)
	}
	if _, err := log.Append(prefix); err != nil {
		return nil, err
	}
	p := &Output{source: source, log: log, pause: make(chan struct{})}
	p.startLocked()
	return p, nil
}

// RestoreOutput adopts validated resources but starts paused, with no I/O.
// The replay must already have been restored, and its state must agree with
// the metadata. Failure leaves both resources owned by the caller.
func RestoreOutput(state OutputState, source *os.File, log *Replay) (*Output, error) {
	if source == nil || log == nil || state.Version != 1 ||
		len(state.ReadError) > 4096 || len(state.Pending) > 32<<10 ||
		(len(state.Pending) != 0 && state.ReadError == "") || (state.ReadError != "" && state.Replay.FIN) ||
		(state.Discarding && state.ReadError == "") {
		return nil, fmt.Errorf("workload: invalid output state")
	}
	log.mu.Lock()
	valid := !log.closed && log.frozen && log.state == state.Replay
	log.mu.Unlock()
	if !valid {
		return nil, fmt.Errorf("workload: output replay state mismatch")
	}
	if err := validateStreamFile(source, false); err != nil {
		return nil, err
	}
	if err := source.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("workload: restored output source is not pollable: %w", err)
	}
	p := &Output{source: source, log: log, paused: true, frozen: true, readErr: state.ReadError,
		discarding: state.Discarding, pending: append([]byte(nil), state.Pending...),
		pause: make(chan struct{}), done: make(chan struct{})}
	close(p.pause)
	close(p.done)
	return p, nil
}

func (p *Output) startLocked() {
	p.running = true
	p.done = make(chan struct{})
	go p.run()
}

func (p *Output) run() {
	buffer := make([]byte, 32<<10)
	for {
		p.mu.Lock()
		state, changed := p.log.Observe()
		if p.paused || p.closed || state.FIN || (p.readErr != "" && !p.discarding) {
			p.running = false
			close(p.done)
			p.mu.Unlock()
			return
		}
		if p.discarding {
			// Discard: the stream already ended for its consumer (at End, with
			// ReadError naming the reason); this actor now only keeps the
			// child's pipe drained. Nothing is retained. A deadline error is
			// Freeze interrupting the read; EOF or any other error means the
			// source is exhausted and the actor may retire.
			p.mu.Unlock()
			_, err := p.source.Read(buffer)
			p.mu.Lock()
			switch {
			case err == nil:
			case errors.Is(err, os.ErrDeadlineExceeded):
				if !p.paused {
					// Discard's own wake-up of a read in flight, not a
					// freeze (a freeze sets paused first and clears the
					// deadline itself on thaw).
					_ = p.source.SetReadDeadline(time.Time{})
				}
			default:
				p.discarding = false // EOF or a real error: the source is exhausted
			}
			p.mu.Unlock()
			continue
		}
		pause := p.pause
		free := int(state.Capacity - int64(state.End-state.Base))
		p.mu.Unlock()
		if free == 0 {
			select {
			case <-changed:
			case <-pause:
			}
			continue
		}
		n, err := p.source.Read(buffer[:min(free, len(buffer))])
		p.mu.Lock()
		// The read may return both bytes and an error, including a freeze
		// deadline. Every byte is recorded before the next pause check.
		if n > 0 {
			written, writeErr := p.log.Append(buffer[:n])
			if writeErr != nil || written != n {
				// This can only be an I/O failure or an ownership violation:
				// this actor is the only appender and read at most free bytes.
				p.readErr = fmt.Sprintf("retain output: wrote %d of %d: %v", written, n, writeErr)
				// Preserve even an exceptional storage-failure suffix. It is
				// bounded by one read buffer and belongs to the domain's error
				// recovery decision; a failed memfd write must not erase bytes
				// already removed from the original business pipe.
				p.pending = append([]byte(nil), buffer[written:n]...)
			}
		}
		if err != nil && p.readErr == "" {
			switch {
			case errors.Is(err, io.EOF):
				if finishErr := p.log.Finish(); finishErr != nil {
					p.readErr = finishErr.Error()
				}
			case errors.Is(err, os.ErrDeadlineExceeded):
				// Freeze owns this deadline. A canceled freeze may already
				// have cleared it; the following iteration rechecks state.
			default:
				p.readErr = err.Error()
			}
		}
		p.mu.Unlock()
	}
}

// Freeze returns borrowed source/log files for the explicit handoff resource.
// A canceled freeze resumes the original pump before returning an error.
// Successful return leaves both the pump and its log frozen until Thaw.
//
// An aborted stream (Abort ran: pump gone, ReadError set, bytes retained) is
// accepted: its retained prefix and End are exactly what the next image must
// serve, and refusing it would make every revoked-and-full task a permanent
// "cannot upgrade" reason. Only a closed actor or one already handed over
// (frozen) is refused. origin: hot-update S2 plan §4 item 1.
func (p *Output) Freeze(ctx context.Context) (OutputState, map[string]*os.File, error) {
	p.control.Lock()
	defer p.control.Unlock()
	if err := ctx.Err(); err != nil {
		return OutputState{}, nil, err
	}
	p.mu.Lock()
	if p.closed || p.frozen {
		p.mu.Unlock()
		return OutputState{}, nil, fmt.Errorf("workload: output unavailable for freeze")
	}
	if !p.paused {
		p.paused = true
		close(p.pause) // Abort may already have closed it; closing twice panics
	}
	done := p.done
	err := p.source.SetReadDeadline(time.Now())
	p.mu.Unlock()
	if err != nil {
		p.thaw()
		return OutputState{}, nil, err
	}
	select {
	case <-done:
	case <-ctx.Done():
		p.thaw()
		return OutputState{}, nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		p.thaw()
		return OutputState{}, nil, err
	}
	state, file, err := p.log.Freeze()
	if err != nil {
		p.thaw()
		return OutputState{}, nil, err
	}
	p.mu.Lock()
	p.frozen = true
	snapshot := p.snapshotLocked(state)
	p.mu.Unlock()
	return snapshot, map[string]*os.File{"source": p.source, "replay": file}, nil
}

func (p *Output) Thaw() {
	p.control.Lock()
	defer p.control.Unlock()
	p.thaw()
}

func (p *Output) thaw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.paused {
		return
	}
	p.frozen = false
	p.log.Thaw()
	if err := p.source.SetReadDeadline(time.Time{}); err != nil {
		p.readErr = err.Error()
	}
	p.paused = false
	p.pause = make(chan struct{})
	// An ended stream (true EOF, or an abort that is not draining) has no
	// pump to restart: run() would exit on its first iteration, and a Thaw
	// of an image with many ended streams would spawn that many transient
	// goroutines for nothing (S2 internal review R7).
	if !p.running && !p.log.Snapshot().FIN && (p.readErr == "" || p.discarding) {
		p.startLocked()
	}
}

// Detach dismantles a paused actor WITHOUT closing its files: the caller
// receives the original source and the still-frozen log to hand elsewhere
// (a failed import salvaging its descriptors for a re-dispatch). Only an
// actor that is paused with no goroutine — restored and never thawed, or
// frozen — qualifies; a live pump would race the new owner on the shared
// open-file description. The actor is closed afterwards. ok=false leaves the
// actor untouched. origin: hot-update S2 plan §4 item 2.
func (p *Output) Detach() (source *os.File, log *Replay, ok bool) {
	p.control.Lock()
	defer p.control.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.paused || p.running {
		return nil, nil, false
	}
	p.closed = true
	source, log = p.source, p.log
	p.source, p.log = nil, nil
	return source, log, true
}

func (p *Output) Snapshot() OutputState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked(p.log.Snapshot())
}

func (p *Output) snapshotLocked(replay ReplayState) OutputState {
	return OutputState{Version: 1, Replay: replay, ReadError: p.readErr,
		Pending: append([]byte(nil), p.pending...), Discarding: p.discarding}
}

// Discard truncates a live stream for its consumer the way Abort does —
// ended by reason at its current End, retained bytes still readable, never a
// fabricated FIN — but keeps this actor draining the source so the child can
// go on writing. It is the running-abandoned bound (hot-update S2 plan D11 /
// §22.4 item 8): a task nobody has ACKed for AbandonedRunningGrace with a
// full log would otherwise hold its child on a blocked write forever, and the
// only other exits (kill the child, or fabricate ACKs so the ring wraps) are
// either a user-visible regression or a lie to a client that may still come
// back. A stream that already ended, a closed actor and a stream already
// discarding are left as they are.
func (p *Output) Discard(reason string) {
	p.control.Lock()
	defer p.control.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.readErr != "" || p.log.Snapshot().FIN {
		return
	}
	p.readErr, p.discarding = reason, true
	if p.paused {
		// Frozen: the pump is not running and the marker alone is the
		// change; the next thaw starts a draining pump.
		return
	}
	// A pump parked on replay backpressure waits on the log's change channel
	// or on pause: closing pause (and replacing it, so the actor stays live)
	// wakes it. A pump blocked in Read wakes on the deadline; the discarding
	// branch clears that deadline itself when no freeze owns it.
	close(p.pause)
	p.pause = make(chan struct{})
	_ = p.source.SetReadDeadline(time.Now())
	if !p.running {
		p.startLocked()
	}
}

// Abort ends a live stream without a source EOF: the stream is recorded as
// ended by reason at its current End (never a fabricated FIN), every byte
// already retained stays readable, a pump stalled on replay backpressure is
// woken and a pump blocked in Read is released, and the actor exits. It is
// the session authority's tool (hot-update S1 internal review P2): a revoked
// task whose log is full has no client left to ACK, so without this the pump
// would wait for capacity forever and the task never reach a terminal. A
// stream that already ended (EOF or an earlier error) is left as it is; a
// closed pump is a no-op. Freeze after Abort SUCCEEDS: an aborted stream
// still hands over its retained bytes and End, and restores as an ended,
// pumpless stream (this sentence once read "reports the pump unavailable",
// which was true of the paused-based refusal and would have turned every
// revoked-and-full task into a permanent upgrade blocker — S2 plan §4 item 1).
func (p *Output) Abort(reason string) {
	p.control.Lock()
	defer p.control.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.readErr != "" || p.log.Snapshot().FIN {
		return
	}
	p.readErr = reason
	if !p.paused {
		p.paused = true
		close(p.pause)
	}
	// A Read in flight returns with a deadline error; run() rechecks readErr
	// before it would clear or record that error, so reason wins.
	_ = p.source.SetReadDeadline(time.Now())
}

// Close is task teardown, never a pause mechanism. It closes the source to
// release a blocked read and waits for this actor before closing its log.
func (p *Output) Close() error {
	p.control.Lock()
	defer p.control.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	if !p.paused {
		close(p.pause)
	}
	done := p.done
	err := p.source.Close()
	p.mu.Unlock()
	<-done
	return errors.Join(err, p.log.Close())
}
