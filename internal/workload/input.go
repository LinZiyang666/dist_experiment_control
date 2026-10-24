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

// InputState separates accepted bytes (Replay.End), bytes actually written to
// the original destination (Replay.Base), and delivered EOF. FIN is accepted
// EOF; the write end closes only after every preceding byte was written.
type InputState struct {
	Version    int         `json:"version"`
	Replay     ReplayState `json:"replay"`
	EOF        bool        `json:"eof"`
	WriteError string      `json:"write_error,omitempty"`
}

// Input owns a process pipe writer and is the sole ACK owner of its replay log.
// EOF closes that pipe end. TCP half-close and PTY terminal-input semantics
// belong to their respective domain adapters, not this pipe actor. The upstream
// adapter may Append/Finish the log; those operations transfer responsibility
// to this owner, independently of a particular control connection. It must not
// call Ack: only completed destination writes advance the applied offset.
type Input struct {
	control sync.Mutex
	mu      sync.Mutex
	target  *os.File
	log     *Replay
	done    chan struct{}
	pause   chan struct{}
	running bool
	paused  bool
	closed  bool
	eof     bool
	err     string
}

func NewInput(target *os.File, log *Replay) (*Input, error) {
	if target == nil || log == nil {
		return nil, fmt.Errorf("workload: missing input resource")
	}
	log.mu.Lock()
	valid := !log.closed && !log.frozen && log.state.Base == 0
	log.mu.Unlock()
	if !valid {
		return nil, fmt.Errorf("workload: input requires unapplied replay log")
	}
	if err := validateStreamFile(target, true); err != nil {
		return nil, err
	}
	if err := target.SetWriteDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("workload: input target is not pollable: %w", err)
	}
	p := &Input{target: target, log: log, pause: make(chan struct{})}
	p.startLocked()
	return p, nil
}

// RestoreInput takes ownership on success and remains paused. An already
// delivered EOF has no remaining destination descriptor: restoring it must not
// manufacture a fresh writer that would delay EOF in the original child.
func RestoreInput(state InputState, target *os.File, log *Replay) (*Input, error) {
	if log == nil || state.Version != 1 || len(state.WriteError) > 4096 ||
		(state.EOF && (!state.Replay.FIN || state.Replay.Base != state.Replay.End)) ||
		(state.EOF != (target == nil)) {
		return nil, fmt.Errorf("workload: invalid input state")
	}
	log.mu.Lock()
	valid := !log.closed && log.frozen && log.state == state.Replay
	log.mu.Unlock()
	if !valid {
		return nil, fmt.Errorf("workload: input replay state mismatch")
	}
	if target != nil {
		if err := validateStreamFile(target, true); err != nil {
			return nil, err
		}
		if err := target.SetWriteDeadline(time.Time{}); err != nil {
			return nil, fmt.Errorf("workload: restored input target is not pollable: %w", err)
		}
	}
	p := &Input{target: target, log: log, paused: true, eof: state.EOF, err: state.WriteError,
		pause: make(chan struct{}), done: make(chan struct{})}
	close(p.pause)
	close(p.done)
	return p, nil
}

func (p *Input) startLocked() {
	p.running = true
	p.done = make(chan struct{})
	go p.run()
}

func (p *Input) run() {
	buffer := make([]byte, 32<<10)
	for {
		p.mu.Lock()
		state, changed := p.log.Observe()
		if p.paused || p.closed || p.err != "" || p.eof {
			p.running = false
			close(p.done)
			p.mu.Unlock()
			return
		}
		if state.FIN && state.Base == state.End {
			// Keep close and its receipt under the same freeze barrier.
			if err := p.target.Close(); err != nil {
				p.err = err.Error()
			}
			p.target, p.eof = nil, true
			p.mu.Unlock()
			continue
		}
		pause := p.pause
		p.mu.Unlock()
		if state.Base == state.End {
			select {
			case <-changed:
			case <-pause:
			}
			continue
		}
		n, err := p.log.Read(state.Base, buffer)
		if err != nil && !errors.Is(err, io.EOF) {
			p.mu.Lock()
			p.err = err.Error()
			p.mu.Unlock()
			continue
		}
		written, writeErr := p.target.Write(buffer[:n])
		p.mu.Lock()
		// A partial write followed by a freeze deadline still applied its
		// prefix. The unconsumed suffix stays in the log for the new image.
		if written > 0 {
			if ackErr := p.log.Ack(state.Base + uint64(written)); ackErr != nil {
				p.err = ackErr.Error()
			}
		}
		if writeErr != nil && !errors.Is(writeErr, os.ErrDeadlineExceeded) {
			p.err = writeErr.Error()
		} else if writeErr == nil && written != n {
			p.err = io.ErrShortWrite.Error()
		}
		p.mu.Unlock()
	}
}

func (p *Input) Freeze(ctx context.Context) (InputState, map[string]*os.File, error) {
	p.control.Lock()
	defer p.control.Unlock()
	if err := ctx.Err(); err != nil {
		return InputState{}, nil, err
	}
	p.mu.Lock()
	if p.closed || p.paused {
		p.mu.Unlock()
		return InputState{}, nil, fmt.Errorf("workload: input unavailable for freeze")
	}
	p.paused = true
	close(p.pause)
	done := p.done
	var err error
	if p.target != nil {
		err = p.target.SetWriteDeadline(time.Now())
	}
	p.mu.Unlock()
	if err != nil {
		p.thaw()
		return InputState{}, nil, err
	}
	select {
	case <-done:
	case <-ctx.Done():
		p.thaw()
		return InputState{}, nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		p.thaw()
		return InputState{}, nil, err
	}
	state, file, err := p.log.Freeze()
	if err != nil {
		p.thaw()
		return InputState{}, nil, err
	}
	p.mu.Lock()
	snapshot := InputState{Version: 1, Replay: state, EOF: p.eof, WriteError: p.err}
	files := map[string]*os.File{"replay": file}
	if p.target != nil {
		files["target"] = p.target
	}
	p.mu.Unlock()
	return snapshot, files, nil
}

func (p *Input) Thaw() {
	p.control.Lock()
	defer p.control.Unlock()
	p.thaw()
}

func (p *Input) thaw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.paused {
		return
	}
	p.log.Thaw()
	if p.target != nil {
		if err := p.target.SetWriteDeadline(time.Time{}); err != nil {
			p.err = err.Error()
		}
	}
	p.paused = false
	p.pause = make(chan struct{})
	// A delivered EOF or a recorded write failure leaves nothing to pump.
	if !p.running && !p.eof && p.err == "" {
		p.startLocked()
	}
}

// Detach dismantles a paused actor WITHOUT closing its files (see
// Output.Detach): the caller receives the original write end (nil once EOF
// was delivered) and the still-frozen log. Only a restored-and-never-thawed
// or frozen actor qualifies; ok=false leaves it untouched.
func (p *Input) Detach() (target *os.File, log *Replay, ok bool) {
	p.control.Lock()
	defer p.control.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.paused || p.running {
		return nil, nil, false
	}
	p.closed = true
	target, log = p.target, p.log
	p.target, p.log = nil, nil
	return target, log, true
}

func (p *Input) Snapshot() InputState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return InputState{Version: 1, Replay: p.log.Snapshot(), EOF: p.eof, WriteError: p.err}
}

func (p *Input) Close() error {
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
	var err error
	if p.target != nil {
		err = p.target.Close()
	}
	p.mu.Unlock()
	<-done
	return errors.Join(err, p.log.Close())
}
