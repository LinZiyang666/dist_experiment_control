package rawsocket

import (
	"os"
	"sync"
	"syscall"
	"time"
)

// socketState owns one pollable file. The descriptor number is only accessed
// inside RawConn callbacks: File.Fd would silently put the shared OFD into
// blocking mode and invalidate both the old and the imported pump.
// origin: hot-update S5 plan §3.2 / T08
// connectMu serializes the sole SO_ERROR consumer independently of data IO.
type socketState struct {
	mu              sync.Mutex
	connectMu       sync.Mutex
	file            *os.File
	raw             syscall.RawConn
	identity        Fingerprint
	tuple           Tuple
	connectState    ConnectState
	connectError    error
	connectConsumed bool
	connectDeadline time.Time
	closed          bool
	frozen          bool
	active          int
}

func (s *Socket) Fingerprint() Fingerprint {
	if s == nil || s.state == nil {
		return Fingerprint{}
	}
	return s.state.identity
}

func (s *Socket) beginIO() (*socketState, error) {
	if s == nil || s.state == nil {
		return nil, ErrClosed
	}
	st := s.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return nil, ErrClosed
	}
	if st.frozen {
		return nil, ErrBusy
	}
	st.active++
	return st, nil
}
func (s *Socket) endIO() { s.state.mu.Lock(); s.state.active--; s.state.mu.Unlock() }

// Read and Write perform one successful syscall, preserving short progress.
// The flow actor, rather than a buffering net.Conn, owns every unwritten suffix.
func (s *Socket) Read(p []byte) (int, error) {
	st, err := s.beginIO()
	if err != nil {
		return 0, err
	}
	defer s.endIO()
	return readSocket(st.raw, p)
}
func (s *Socket) Write(p []byte) (int, error) {
	st, err := s.beginIO()
	if err != nil {
		return 0, err
	}
	defer s.endIO()
	return writeSocket(st.raw, p)
}
func (s *Socket) SetReadDeadline(t time.Time) error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	return s.state.file.SetReadDeadline(t)
}
func (s *Socket) SetWriteDeadline(t time.Time) error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	return s.state.file.SetWriteDeadline(t)
}
func (s *Socket) CloseWrite() error {
	st, err := s.beginIO()
	if err != nil {
		return err
	}
	defer s.endIO()
	return shutdownSocket(st.raw, false)
}
func (s *Socket) Shutdown() error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	// Revocation is stronger than a cut; it must reach all duplicates even frozen.
	return shutdownSocket(s.state.raw, true)
}
func (s *Socket) Close() error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	st := s.state
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return nil
	}
	st.closed = true
	st.mu.Unlock()
	return st.file.Close()
}

// Hold is the raw safe point used by image owners after joining their pumps.
// It never cancels IO by closing the business socket.
func Hold(s *Socket) error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	st := s.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return ErrClosed
	}
	if st.active != 0 {
		return ErrBusy
	}
	st.frozen = true
	return nil
}
func Release(s *Socket) error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	st := s.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return ErrClosed
	}
	st.frozen = false
	return nil
}

// BorrowHeldFile is for the image adapter. No ownership or descriptor flags
// change; the wrapper must remain held until Prepare has duplicated the file.
func BorrowHeldFile(s *Socket) (*os.File, error) {
	if s == nil || s.state == nil {
		return nil, ErrClosed
	}
	f, _, err := s.state.imageFile()
	return f, err
}

// The image adapter borrows only after all IO has joined at the raw safe point.
// Returning the original and its identity under one guard keeps the live and
// candidate paths consistent without creating another netpoll wrapper.
// origin: hot-update S5 plan §3.2 / §5.1 / T08
func (st *socketState) imageFile() (*os.File, Fingerprint, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return nil, Fingerprint{}, ErrClosed
	}
	if !st.frozen || st.active != 0 {
		return nil, Fingerprint{}, ErrBusy
	}
	return st.file, st.identity, nil
}

// ConnectReceipt persists the destructive SO_ERROR observation, including
// failure. Validation and subsequent CompleteConnect calls never consume it.
func ConnectReceipt(s *Socket) (ConnectState, bool, Tuple, time.Time, error) {
	if s == nil || s.state == nil {
		return 0, false, Tuple{}, time.Time{}, ErrClosed
	}
	st := s.state
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.connectState, st.connectConsumed, st.tuple, st.connectDeadline, st.connectError
}

// RestoreConnectReceipt installs a validated private decision on a held wrapper.
// It does not observe SO_ERROR again, including for a previously failed connect.
func RestoreConnectReceipt(s *Socket, phase ConnectState, consumed bool, code uint64, tuple Tuple, deadline time.Time) error {
	if s == nil || s.state == nil {
		return ErrClosed
	}
	st := s.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.active != 0 || st.closed {
		return ErrBusy
	}
	if phase < ConnectConnecting || phase > ConnectFailed || code > uint64(^uint32(0)) || phase == ConnectFailed && (!consumed || code == 0) || phase == ConnectConnecting && (consumed || code != 0) || phase == ConnectConnected && code != 0 {
		return ErrIdentity
	}
	if st.tuple.Local.IsValid() && st.tuple.Local != tuple.Local {
		return ErrIdentity
	}
	if st.tuple.Remote.IsValid() && st.tuple.Remote != tuple.Remote {
		return ErrIdentity
	}
	st.connectState = phase
	st.connectConsumed = consumed
	st.tuple = tuple
	st.connectDeadline = deadline
	if code != 0 {
		st.connectError = syscall.Errno(code)
	}
	st.frozen = true
	return nil
}
