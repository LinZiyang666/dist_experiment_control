package clusterio

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
)

// TransportTracker counts owners, not just open sockets. A fence closes the
// underlying socket to interrupt TLS and io.Copy; the accepted handler's own
// deferred Close is the receipt which removes it from the census.
// origin: hot-update S6 plan §3.2 / T16
type TransportTracker struct {
	mu          sync.Mutex
	fenced      bool
	connections map[*trackedConn]struct{}
	readers     map[*trackedReader]struct{}
	changed     chan struct{}
	closeErr    error
}

var ErrFenced = errors.New("clusterio: transport admission fenced")

func NewTracker() *TransportTracker {
	return &TransportTracker{connections: make(map[*trackedConn]struct{}), readers: make(map[*trackedReader]struct{}), changed: make(chan struct{})}
}

func (t *TransportTracker) signalLocked() { close(t.changed); t.changed = make(chan struct{}) }

func (t *TransportTracker) Track(c net.Conn) (net.Conn, error) {
	if c == nil {
		return nil, errors.New("clusterio: nil connection")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fenced {
		return nil, ErrFenced
	}
	w := &trackedConn{Conn: c, tracker: t}
	t.connections[w] = struct{}{}
	t.signalLocked()
	return w, nil
}

type trackedConn struct {
	net.Conn
	tracker   *TransportTracker
	stopOnce  sync.Once
	stopErr   error
	ownerOnce sync.Once
}

func (c *trackedConn) stop() error {
	c.stopOnce.Do(func() { c.stopErr = c.Conn.Close() })
	return c.stopErr
}

func (c *trackedConn) Close() error {
	err := c.stop()
	c.ownerOnce.Do(func() {
		t := c.tracker
		t.mu.Lock()
		defer t.mu.Unlock()
		// An unsuccessful actual close retains an unknown owner. Removing it
		// would allow the next store to open while a payload can still run.
		if err == nil {
			delete(t.connections, c)
		} else {
			t.closeErr = errors.Join(t.closeErr, err)
		}
		t.signalLocked()
	})
	return err
}

// TrackReader retains a snapshot reader until the RPC response acknowledges
// that its consumer has returned. Close also waits for an outstanding Read to
// return before removing the reader; a socket close alone is not a join.
// origin: hot-update S6 plan §3.4 / T16
func (t *TransportTracker) TrackReader(r io.Reader) (io.ReadCloser, error) {
	if r == nil {
		return nil, errors.New("clusterio: nil reader")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fenced {
		return nil, ErrFenced
	}
	w := &trackedReader{source: r, tracker: t}
	t.readers[w] = struct{}{}
	t.signalLocked()
	return w, nil
}

type trackedReader struct {
	source   io.Reader
	tracker  *TransportTracker
	closed   bool
	active   uint32
	stopOnce sync.Once
	stopErr  error
}

// An outgoing snapshot may be blocked reading its source rather than writing
// the transport socket. Closing both sides wakes io.Copy without pretending
// that the sender/consumer has already joined.
// origin: hot-update S6 plan §3.2 / T16
func (r *trackedReader) stop() error {
	r.stopOnce.Do(func() {
		if connection, ok := r.source.(*trackedConn); ok {
			// This reader borrows an accepted connection. Cancelling the raw
			// socket must preserve the handler's separate deferred Close receipt.
			r.stopErr = connection.stop()
		} else if source, ok := r.source.(io.Closer); ok {
			r.stopErr = source.Close()
		}
	})
	return r.stopErr
}

func (r *trackedReader) Read(p []byte) (int, error) {
	t := r.tracker
	t.mu.Lock()
	if r.closed {
		t.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	r.active++
	t.mu.Unlock()
	n, err := r.source.Read(p)
	t.mu.Lock()
	r.active--
	if r.closed && r.active == 0 && r.stopErr == nil {
		delete(t.readers, r)
	}
	t.signalLocked()
	t.mu.Unlock()
	return n, err
}

func (r *trackedReader) Close() error {
	err := r.stop()
	t := r.tracker
	t.mu.Lock()
	defer t.mu.Unlock()
	r.closed = true
	if err != nil {
		t.closeErr = errors.Join(t.closeErr, err)
	}
	if r.active == 0 && err == nil {
		delete(t.readers, r)
	}
	t.signalLocked()
	return err
}

func (t *TransportTracker) Counts() (uint32, uint32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return uint32(len(t.connections)), uint32(len(t.readers))
}

// NoteCloseError preserves an unknown staging owner even after its socket has
// closed. Counts reaching zero must not turn a failed file close into a seal.
// origin: hot-update S6 plan §3.2 / T10
func (t *TransportTracker) NoteCloseError(err error) {
	if err == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeErr = errors.Join(t.closeErr, err)
	t.signalLocked()
}

var _ Tracker = (*TransportTracker)(nil)

var ErrDeadline = errors.New("clusterio: cut deadline expired or invalid")

// Remaining uses the original absolute BOOTTIME deadline at every wait; joining
// one object must not grant the next object a new budget.
func Remaining(deadline controlbudget.Deadline) (time.Duration, error) {
	now, err := bootNow()
	if err != nil {
		return 0, err
	}
	if deadline <= 0 || int64(deadline) <= now {
		return 0, ErrDeadline
	}
	return time.Duration(int64(deadline) - now), nil
}

func (t *TransportTracker) Fence(deadline controlbudget.Deadline) error {
	if _, err := Remaining(deadline); err != nil {
		return err
	}
	return t.Stop()
}

// Stop is the terminal, unbounded counterpart of Fence. Owners still join
// synchronously; no timed-out shutdown goroutine is discarded.
func (t *TransportTracker) Stop() error {
	t.mu.Lock()
	t.fenced = true
	connections := make([]*trackedConn, 0, len(t.connections))
	for c := range t.connections {
		connections = append(connections, c)
	}
	readers := make([]*trackedReader, 0, len(t.readers))
	for r := range t.readers {
		readers = append(readers, r)
	}
	t.signalLocked()
	t.mu.Unlock()
	var errs []error
	for _, c := range connections {
		errs = append(errs, c.stop())
	}
	for _, r := range readers {
		errs = append(errs, r.stop())
	}
	t.mu.Lock()
	t.closeErr = errors.Join(t.closeErr, errors.Join(errs...))
	err := t.closeErr
	t.mu.Unlock()
	return err
}

func (t *TransportTracker) Join(deadline controlbudget.Deadline) error {
	for {
		remaining, err := Remaining(deadline)
		if err != nil {
			return err
		}
		t.mu.Lock()
		if !t.fenced {
			t.mu.Unlock()
			return errors.New("clusterio: join requires fence")
		}
		if t.closeErr != nil {
			err := t.closeErr
			t.mu.Unlock()
			return err
		}
		if len(t.connections) == 0 && len(t.readers) == 0 {
			t.mu.Unlock()
			return nil
		}
		changed := t.changed
		t.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-changed:
			timer.Stop()
		case <-timer.C:
			return ErrDeadline
		}
	}
}

// Wait is used after terminal transport shutdown. Waiting here keeps ownership
// with the caller even if a foreign consumer fails to return.
func (t *TransportTracker) Wait() error {
	for {
		t.mu.Lock()
		if t.closeErr != nil {
			err := t.closeErr
			t.mu.Unlock()
			return err
		}
		if len(t.connections) == 0 && len(t.readers) == 0 {
			t.mu.Unlock()
			return nil
		}
		changed := t.changed
		t.mu.Unlock()
		<-changed
	}
}
