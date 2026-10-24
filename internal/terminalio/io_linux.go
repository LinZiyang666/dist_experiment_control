//go:build linux

package terminalio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// Each direction has one worker and no queued batch. A request borrows the
// caller's buffer until its reply, including cancellation; returning early on
// cancellation would let the caller reuse memory still touched by a syscall.
// origin: hot-update S4 plan §7.3 / T43
const syscallBytes = 32 << 10

type ioRequest struct {
	data []byte
	done chan ioResult
}

type ioResult struct {
	n   int
	err error
}

type stream struct {
	fd, wake  int
	reading   bool
	tty       bool
	requests  chan ioRequest
	cancelled chan struct{}
	done      chan struct{}
	cancel    sync.Once
	cancelErr error
}

type platformIO struct {
	mu       sync.Mutex // Serializes raw/restore/close, never held by a worker.
	in, out  *stream
	raw      *term.State
	closed   bool
	closeErr error
}

func open(in, out *os.File) (*IO, error) {
	input, err := openStream(in, "stdin", true)
	if err != nil {
		return nil, err
	}
	output, err := openStream(out, "stdout", false)
	if err != nil {
		return nil, errors.Join(err, input.release())
	}
	result := &IO{local: platformIO{in: input, out: output}}
	go input.run()
	go output.run()
	return result, nil
}

func openStream(file *os.File, name string, reading bool) (*stream, error) {
	if file == nil {
		return nil, fmt.Errorf("terminalio: %s: %w", name, os.ErrInvalid)
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("terminalio: %s: %w", name, err)
	}
	s := &stream{fd: -1, wake: -1, reading: reading}
	var inner error
	// RawConn pins the original against Close and avoids File.Fd's implicit
	// change to blocking mode. No syscall here changes the caller's flags.
	err = raw.Control(func(fd uintptr) { inner = s.acquire(int(fd), name) })
	if err != nil || inner != nil {
		return nil, errors.Join(err, inner, s.release())
	}
	s.wake, err = unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("terminalio: %s wake: %w", name, err), s.release())
	}
	s.requests = make(chan ioRequest)
	s.cancelled = make(chan struct{})
	s.done = make(chan struct{})
	return s, nil
}

func (s *stream) acquire(original int, name string) error {
	var before unix.Stat_t
	if err := unix.Fstat(original, &before); err != nil {
		return fmt.Errorf("terminalio: %s stat: %w", name, err)
	}
	kind := before.Mode & unix.S_IFMT
	independent := kind == unix.S_IFIFO
	switch kind {
	case unix.S_IFREG:
		// Regular files cannot wait for a peer. Duplicate without changing any
		// OFD flags, preserving inherited offsets and O_APPEND across the CLI.
	case unix.S_IFCHR:
		if before.Rdev == unix.Mkdev(1, 3) { // Linux /dev/null, not arbitrary devices.
			break
		}
		if !term.IsTerminal(original) {
			return &ErrUnsupportedStdio{Stream: name, Kind: "character device"}
		}
		// Reopening ptmx allocates a different terminal even when fstat matches.
		// Stdio must be a slave/TTY whose proc-fd reopen names the same device.
		if _, err := unix.IoctlGetInt(original, unix.TIOCGPTN); err == nil {
			return &ErrUnsupportedStdio{Stream: name, Kind: "PTY master"}
		}
		s.tty, independent = true, true
	case unix.S_IFIFO:
	case unix.S_IFSOCK:
		return &ErrUnsupportedStdio{Stream: name, Kind: "socket"}
	case unix.S_IFDIR:
		return &ErrUnsupportedStdio{Stream: name, Kind: "directory"}
	case unix.S_IFBLK:
		return &ErrUnsupportedStdio{Stream: name, Kind: "block device"}
	default:
		return &ErrUnsupportedStdio{Stream: name, Kind: "unknown file type"}
	}
	flags, err := unix.FcntlInt(uintptr(original), unix.F_GETFL, 0)
	if err != nil {
		return fmt.Errorf("terminalio: %s flags: %w", name, err)
	}
	access := flags & unix.O_ACCMODE
	if flags&unix.O_PATH != 0 || (s.reading && access == unix.O_WRONLY) || (!s.reading && access == unix.O_RDONLY) {
		return fmt.Errorf("terminalio: %s access: %w", name, unix.EBADF)
	}
	if independent {
		// origin: hot-update S4 plan §7.3 / T43
		// dup+SetNonblock would change the shell's shared open file description.
		// O_NOCTTY also prevents opening a terminal from acquiring it as ctty.
		s.fd, err = unix.Open(fmt.Sprintf("/proc/self/fd/%d", original), access|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOCTTY, 0)
	} else {
		s.fd, err = unix.FcntlInt(uintptr(original), unix.F_DUPFD_CLOEXEC, 3)
	}
	if err != nil {
		if independent && (errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENXIO) || errors.Is(err, unix.ENODEV) || errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM)) {
			return fmt.Errorf("terminalio: reopen %s: %w: %w", name, &ErrUnsupportedStdio{Stream: name, Kind: "unreopenable terminal or FIFO"}, err)
		}
		return fmt.Errorf("terminalio: duplicate %s: %w", name, err)
	}
	// Verify the reopened object while the original is still pinned. This is
	// stronger than trusting a proc-fd pathname (or a descriptor integer).
	var after unix.Stat_t
	if err := unix.Fstat(s.fd, &after); err != nil {
		return fmt.Errorf("terminalio: %s reopened stat: %w", name, err)
	}
	if before.Dev != after.Dev || before.Ino != after.Ino || kind != after.Mode&unix.S_IFMT || before.Rdev != after.Rdev {
		return &ErrUnsupportedStdio{Stream: name, Kind: "reopened descriptor with changed identity"}
	}
	return nil
}

func (s *stream) Read(p []byte) (int, error)  { return s.perform(p) }
func (s *stream) Write(p []byte) (int, error) { return s.perform(p) }

func (s *stream) perform(p []byte) (int, error) {
	req := ioRequest{data: p, done: make(chan ioResult, 1)}
	select {
	case <-s.cancelled:
		return 0, os.ErrClosed
	case s.requests <- req:
		// Only the worker can release a submitted buffer, even after Cancel.
		result := <-req.done
		return result.n, result.err
	}
}

func (s *stream) run() {
	defer close(s.done)
	for {
		if s.stopped() {
			return
		}
		select {
		case <-s.cancelled:
			return
		case req := <-s.requests:
			var result ioResult
			if s.reading {
				result.n, result.err = s.read(req.data)
			} else {
				result.n, result.err = s.write(req.data)
			}
			req.done <- result
		}
	}
}

func (s *stream) stopped() bool {
	select {
	case <-s.cancelled:
		return true
	default:
		return false
	}
}

func (s *stream) read(p []byte) (int, error) {
	for {
		if s.stopped() {
			return 0, os.ErrClosed
		}
		if len(p) == 0 {
			return 0, nil
		}
		n, err := unix.Read(s.fd, p[:min(len(p), syscallBytes)])
		if n > 0 {
			return n, err
		}
		if err == nil {
			return 0, io.EOF
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if !errors.Is(err, unix.EAGAIN) {
			return 0, err
		}
		if err := s.wait(unix.POLLIN); err != nil {
			return 0, err
		}
	}
}

func (s *stream) write(p []byte) (int, error) {
	written := 0
	for {
		if s.stopped() {
			return written, os.ErrClosed
		}
		if written == len(p) {
			return written, nil
		}
		n, err := unix.Write(s.fd, p[written:written+min(len(p)-written, syscallBytes)])
		if n > 0 {
			written += n // Cancellation/error never erases a committed prefix.
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EAGAIN) {
			if err := s.wait(unix.POLLOUT); err != nil {
				return written, err
			}
			continue
		}
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
}

func (s *stream) wait(events int16) error {
	// The wake descriptor stays readable forever after Cancel. A single worker
	// per direction and no draining prevents a lost wake between EAGAIN/poll.
	fds := []unix.PollFd{{Fd: int32(s.fd), Events: events}, {Fd: int32(s.wake), Events: unix.POLLIN}}
	for {
		if s.stopped() {
			return os.ErrClosed
		}
		_, err := unix.Poll(fds, -1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if s.stopped() || fds[1].Revents != 0 {
			return os.ErrClosed
		}
		if fds[0].Revents&unix.POLLNVAL != 0 {
			return unix.EBADF
		}
		// HUP/ERR must go through the real syscall: there may be unread tail
		// bytes, or a write failure instead of an input EOF.
		if fds[0].Revents != 0 {
			return nil
		}
	}
}

func (s *stream) Cancel() error {
	s.cancel.Do(func() {
		close(s.cancelled)
		// origin: hot-update S4 plan §7.3 / T43
		// A channel/context alone cannot wake a worker already inside poll.
		var wake [8]byte
		binary.NativeEndian.PutUint64(wake[:], 1)
		for {
			_, err := unix.Write(s.wake, wake[:])
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if !errors.Is(err, unix.EAGAIN) {
				s.cancelErr = err
			}
			break
		}
	})
	return s.cancelErr
}

// release is called only before worker startup or after both worker joins.
// Neither the original File nor its FD is retained as a close target.
func (s *stream) release() error {
	var errs []error
	for _, fd := range []int{s.fd, s.wake} {
		if fd >= 0 {
			errs = append(errs, unix.Close(fd))
		}
	}
	return errors.Join(errs...)
}

func (p *platformIO) source() Source { return p.in }
func (p *platformIO) sink() Sink     { return p.out }

func (p *platformIO) enterRaw() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.in == nil {
		return os.ErrClosed
	}
	if p.raw != nil || !p.in.tty {
		return nil
	}
	var err error
	p.raw, err = term.MakeRaw(p.in.fd)
	return err
}

func (p *platformIO) restore() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return p.closeErr
	}
	return p.restoreLocked()
}

func (p *platformIO) restoreLocked() error {
	if p.raw == nil {
		return nil
	}
	if err := term.Restore(p.in.fd, p.raw); err != nil {
		return err // Retain the saved state for a later explicit retry.
	}
	p.raw = nil
	return nil
}

func (p *platformIO) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return p.closeErr
	}
	p.closed = true
	if p.in == nil { // A zero IO never acquired resources or changed termios.
		return nil
	}
	// Wake BOTH directions before waiting for either. Closing a polled fd from
	// another goroutine is not a wake guarantee and risks descriptor reuse.
	inErr, outErr := p.in.Cancel(), p.out.Cancel()
	<-p.in.done
	<-p.out.done
	p.closeErr = errors.Join(inErr, outErr, p.restoreLocked(), p.in.release(), p.out.release())
	return p.closeErr
}
