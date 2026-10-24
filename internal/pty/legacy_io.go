package pty

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// ErrLegacyInputOverflow is a terminal input failure. The agent reports one
// RunChunk with reason pty_input_overflow; it must not retry this batch or claim
// that the already-started child never ran. Subject deduplication stays there.
var ErrLegacyInputOverflow = errors.New("pty: legacy input buffer full")

// LegacyIO gives each legacy PTY one bounded writer, independent of the shared
// subscription callbacks. The ring includes the in-flight write until its
// actual n is accounted; tiny messages cannot grow an unbounded slice queue.
// The failure watcher only closes/hangs up this session, waking blocked I/O on
// platforms where image/replay ownership is unsupported too.
// origin: hot-update S4 plan §7.1 / §7.3
//
// Construct after successful Start/adoption and before starting terminal I/O.
// Read has one caller; Close joins the internal writer and failure watcher.
// The caller joins its own Read pump after Close. Normal tail draining must
// reach Read EOF before AllowReap/Wait; Close is explicit abandonment only.
type LegacyIO struct {
	session      *Session
	master       *os.File
	verified     bool
	mu           sync.Mutex
	buffer       [upgradebudget.PTYInputBytes]byte
	base, used   int
	stopped      bool
	failure      error
	lastKA       time.Time
	wake         chan struct{}
	stop         chan struct{}
	failures     chan error
	failureReady chan struct{}
	writerDone   chan struct{}
	watchDone    chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

func NewLegacyIO(session *Session) (*LegacyIO, error) {
	if session == nil {
		return nil, fmt.Errorf("pty: missing legacy session")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if !session.ready || session.closed || session.transferred || session.Master == nil || session.borrowers != 0 {
		return nil, fmt.Errorf("pty: legacy terminal ownership unavailable")
	}
	l := &LegacyIO{session: session, master: session.Master,
		verified: legacyTerminalVerified(session), lastKA: time.Now(),
		wake: make(chan struct{}, 1), stop: make(chan struct{}), failures: make(chan error, 1),
		failureReady: make(chan struct{}),
		writerDone:   make(chan struct{}), watchDone: make(chan struct{})}
	// A permanent borrower excludes both TakeMaster and a second legacy owner.
	session.borrowers++
	go l.writeLoop()
	go l.watchFailure()
	return l, nil
}

// EnqueueInput copies before returning; NATS may reuse its message storage.
// Admission does no kernel I/O and never waits for another session's writer.
func (l *LegacyIO) EnqueueInput(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		if l.failure != nil {
			return l.failure
		}
		return os.ErrClosed
	}
	if len(data) > len(l.buffer)-l.used {
		l.failLocked(ErrLegacyInputOverflow)
		return ErrLegacyInputOverflow
	}
	end := (l.base + l.used) % len(l.buffer)
	n := copy(l.buffer[end:], data)
	copy(l.buffer[:], data[n:])
	l.used += len(data)
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return nil
}

func (l *LegacyIO) Read(p []byte) (int, error) {
	n, err := l.master.Read(p)
	l.mu.Lock()
	verified := l.verified && !l.stopped
	l.mu.Unlock()
	return terminalReadResult(n, err, verified)
}

// Resize and keepalive never queue behind blocked input. The agent owns the
// liveness policy; the timestamp here is merely its last accepted observation.
func (l *LegacyIO) Resize(cols, rows int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return os.ErrClosed
	}
	return l.session.SetSize(cols, rows)
}
func (l *LegacyIO) Keepalive() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return os.ErrClosed
	}
	l.lastKA = time.Now()
	return nil
}
func (l *LegacyIO) LastKeepalive() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastKA
}

// Failure returns the first stable failure. The agent emits its single failure
// chunk from this value (or EnqueueInput's result), not from every rejected retry.
func (l *LegacyIO) Failure() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.failure
}

// FailureReady closes once, including for an asynchronous writer error. The
// adapter can publish one failure chunk without polling or stealing the
// internal watcher's notification. Close alone does not manufacture a failure.
func (l *LegacyIO) FailureReady() <-chan struct{} { return l.failureReady }

func (l *LegacyIO) Hangup() {
	l.mu.Lock()
	l.failLocked(fmt.Errorf("pty: legacy terminal hangup"))
	l.mu.Unlock()
}

func (l *LegacyIO) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.stopped = true
		close(l.stop)
		l.mu.Unlock()
		l.closeErr = l.session.Close()
		<-l.writerDone
		<-l.watchDone
		l.session.releaseMaster()
	})
	return l.closeErr
}

func (l *LegacyIO) failLocked(err error) {
	if l.stopped {
		return
	}
	l.stopped, l.failure = true, err
	close(l.failureReady)
	l.failures <- err // exactly one notification; never waits for the watcher
}

func (l *LegacyIO) watchFailure() {
	defer close(l.watchDone)
	select {
	case <-l.stop:
	case <-l.failures:
		// No queue lock across process ownership or Close: a blocked writer
		// must be able to account its partial write while teardown wakes it.
		l.session.Hangup()
		select {
		case l.wake <- struct{}{}:
		default:
		}
	}
}

func (l *LegacyIO) writeLoop() {
	defer close(l.writerDone)
	var batch [upgradebudget.PTYBatchBytes]byte
	for {
		l.mu.Lock()
		if l.stopped {
			l.mu.Unlock()
			return
		}
		n := min(l.used, len(batch), len(l.buffer)-l.base)
		copy(batch[:n], l.buffer[l.base:l.base+n])
		l.mu.Unlock()
		if n == 0 {
			select {
			case <-l.stop:
				return
			case <-l.wake:
			}
			continue
		}
		written, err := l.master.Write(batch[:n])
		l.mu.Lock()
		l.base = (l.base + written) % len(l.buffer)
		l.used -= written
		if err == nil && written == 0 {
			err = io.ErrNoProgress
		}
		if err != nil {
			l.failLocked(fmt.Errorf("pty: legacy input write: %w", err))
		}
		l.mu.Unlock()
	}
}
