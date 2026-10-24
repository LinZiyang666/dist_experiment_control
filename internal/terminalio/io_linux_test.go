//go:build linux

package terminalio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	cpty "github.com/creack/pty"
	"golang.org/x/sys/unix"
)

func exerciseTerminalIO(t *testing.T) {
	t.Helper()
	for _, kind := range []string{"tty", "fifo", "pipe", "tty_close", "fifo_close"} {
		t.Run(kind, func(t *testing.T) { exerciseTerminalCancellation(t, kind) })
	}
	t.Run("regular_file", func(t *testing.T) { exerciseTerminalFile(t, false) })
	t.Run("append_file", func(t *testing.T) { exerciseTerminalFile(t, true) })
	t.Run("dev_null", exerciseTerminalNull)
	t.Run("unsupported", exerciseTerminalUnsupported)
	t.Run("fifo_eof", exerciseTerminalEOF)
}

type terminalFixture struct {
	in, out            *os.File
	inputPeer, outPeer *os.File
}

func newTerminalFixture(t *testing.T, kind string) terminalFixture {
	t.Helper()
	if kind == "tty" {
		master, slave, err := cpty.Open()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = master.Close(); _ = slave.Close() })
		terminalSetNonblock(t, master, true)
		terminalSetNonblock(t, slave, false)
		return terminalFixture{slave, slave, master, master}
	}
	var input, inputPeer, output, outPeer *os.File
	if kind == "fifo" {
		dir := t.TempDir()
		for _, name := range []string{"input", "output"} {
			if err := unix.Mkfifo(filepath.Join(dir, name), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		input = terminalOpenFile(t, filepath.Join(dir, "input"), unix.O_RDONLY|unix.O_NONBLOCK)
		inputPeer = terminalOpenFile(t, filepath.Join(dir, "input"), unix.O_WRONLY|unix.O_NONBLOCK)
		outPeer = terminalOpenFile(t, filepath.Join(dir, "output"), unix.O_RDONLY|unix.O_NONBLOCK)
		output = terminalOpenFile(t, filepath.Join(dir, "output"), unix.O_WRONLY|unix.O_NONBLOCK)
	} else {
		var fds [2]int
		if err := unix.Pipe2(fds[:], unix.O_CLOEXEC); err != nil {
			t.Fatal(err)
		}
		input, inputPeer = terminalOwnFD(t, fds[0]), terminalOwnFD(t, fds[1])
		if err := unix.Pipe2(fds[:], unix.O_CLOEXEC); err != nil {
			t.Fatal(err)
		}
		outPeer, output = terminalOwnFD(t, fds[0]), terminalOwnFD(t, fds[1])
	}
	// Blocking originals make dup+SetNonblock observably corrupt the caller.
	terminalSetNonblock(t, input, false)
	terminalSetNonblock(t, output, false)
	terminalSetNonblock(t, inputPeer, true)
	terminalSetNonblock(t, outPeer, true)
	return terminalFixture{input, output, inputPeer, outPeer}
}

func exerciseTerminalCancellation(t *testing.T, kind string) {
	t.Helper()
	closeDriven := strings.HasSuffix(kind, "_close")
	kind = strings.TrimSuffix(kind, "_close")
	f := newTerminalFixture(t, kind)
	inputFlags, outputFlags := terminalFlags(t, f.in), terminalFlags(t, f.out)
	var originalTermios *unix.Termios
	if kind == "tty" {
		originalTermios = terminalTermios(t, f.in)
	}
	adapter := terminalOpenAdapter(t, f.in, f.out)
	terminalRequireOriginalFlags(t, f.in, inputFlags)
	terminalRequireOriginalFlags(t, f.out, outputFlags)
	for _, s := range []*stream{adapter.local.in, adapter.local.out} {
		flags, err := unix.FcntlInt(uintptr(s.fd), unix.F_GETFL, 0)
		if err != nil || flags&unix.O_NONBLOCK == 0 {
			t.Fatalf("owned descriptor must be nonblocking: flags=%#x err=%v", flags, err)
		}
		cloexec, err := unix.FcntlInt(uintptr(s.fd), unix.F_GETFD, 0)
		if err != nil || cloexec&unix.FD_CLOEXEC == 0 {
			t.Fatalf("owned descriptor must be CLOEXEC: flags=%#x err=%v", cloexec, err)
		}
		// Independent OFDs also isolate later changes to the adapter's flags.
		if err := unix.SetNonblock(s.fd, false); err != nil {
			t.Fatal(err)
		}
		terminalRequireOriginalFlags(t, f.in, inputFlags)
		terminalRequireOriginalFlags(t, f.out, outputFlags)
		if err := unix.SetNonblock(s.fd, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := adapter.EnterRaw(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.EnterRaw(); err != nil {
		t.Fatal(err)
	}
	if kind == "tty" {
		if got := terminalTermios(t, f.in); got.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG) != 0 {
			t.Fatalf("input did not enter raw mode: %+v", got)
		}
	}
	for i := 0; i < 2; i++ {
		if err := adapter.Restore(); err != nil {
			t.Fatal(err)
		}
	}
	if kind == "tty" && !reflect.DeepEqual(originalTermios, terminalTermios(t, f.in)) {
		t.Fatal("repeated EnterRaw/Restore lost the original terminal state")
	}
	if err := adapter.EnterRaw(); err != nil {
		t.Fatal(err)
	}
	pattern := []byte("raw\x03\x04\x1b[31m\r\n\xff\xe2\x82\xac")
	terminalPeerWrite(t, f.inputPeer, pattern)
	got := make([]byte, len(pattern))
	if _, err := io.ReadFull(adapter.Source(), got); err != nil || !bytes.Equal(got, pattern) {
		t.Fatalf("raw input bytes=%x err=%v want=%x", got, err, pattern)
	}
	if n, err := adapter.Sink().Write(pattern); err != nil || n != len(pattern) {
		t.Fatalf("raw output write=%d %v", n, err)
	}
	if got := terminalPeerRead(t, f.outPeer, len(pattern)); !bytes.Equal(got, pattern) {
		t.Fatalf("raw output bytes=%x want=%x", got, pattern)
	}
	readDone, writeDone := make(chan ioResult, 1), make(chan ioResult, 1)
	go func() { n, err := adapter.Source().Read(make([]byte, 1)); readDone <- ioResult{n, err} }()
	terminalAwaitPoll(t, "read")
	payload := bytes.Repeat([]byte("blocked-output-"), 128<<10)
	go func() { n, err := adapter.Sink().Write(payload); writeDone <- ioResult{n, err} }()
	terminalAwaitPoll(t, "write")
	// Both calls reached the kernel poll with a live peer and cannot finish by
	// EOF or capacity. Cancellation must wake them without closing originals.
	select {
	case result := <-readDone:
		t.Fatalf("idle read completed before cancellation: %+v", result)
	default:
	}
	select {
	case result := <-writeDone:
		t.Fatalf("saturated write completed before cancellation: %+v", result)
	default:
	}
	var closeDone chan error
	if closeDriven {
		closeDone = make(chan error, 1)
		go func() { closeDone <- adapter.Close() }()
	} else {
		if err := adapter.Source().Cancel(); err != nil {
			t.Fatal(err)
		}
		if err := adapter.Sink().Cancel(); err != nil {
			t.Fatal(err)
		}
	}
	r := terminalAwaitResult(t, readDone)
	w := terminalAwaitResult(t, writeDone)
	if r.n != 0 || !errors.Is(r.err, os.ErrClosed) {
		t.Fatalf("cancelled read=%+v", r)
	}
	if w.n <= 0 || w.n >= len(payload) || !errors.Is(w.err, os.ErrClosed) {
		t.Fatalf("cancelled write must retain a real partial prefix: %+v total=%d", w, len(payload))
	}
	if got := terminalPeerRead(t, f.outPeer, w.n); !bytes.Equal(got, payload[:w.n]) {
		t.Fatal("cancelled output did not deliver exactly its reported prefix")
	}
	terminalAwaitJoined(t, adapter.local.in.done)
	terminalAwaitJoined(t, adapter.local.out.done)
	if closeDriven {
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close woke I/O but did not join and restore")
		}
	}
	terminalCloseConcurrently(t, adapter)
	terminalRequireOriginalFlags(t, f.in, inputFlags)
	terminalRequireOriginalFlags(t, f.out, outputFlags)
	if kind == "tty" && !reflect.DeepEqual(originalTermios, terminalTermios(t, f.in)) {
		t.Fatal("Close did not restore original termios")
	}
	if kind != "tty" {
		terminalPeerWrite(t, f.inputPeer, []byte("i"))
		var b [1]byte
		if n, err := unix.Read(terminalFD(t, f.in), b[:]); err != nil || n != 1 || b[0] != 'i' {
			t.Fatalf("original stdin unusable after Close: %d %v %q", n, err, b)
		}
		if n, err := unix.Write(terminalFD(t, f.out), []byte("o")); err != nil || n != 1 {
			t.Fatalf("original stdout unusable after Close: %d %v", n, err)
		}
		if got := terminalPeerRead(t, f.outPeer, 1); string(got) != "o" {
			t.Fatalf("original stdout bytes=%q", got)
		}
	}
}

func exerciseTerminalFile(t *testing.T, appendMode bool) {
	t.Helper()
	dir := t.TempDir()
	payload := bytes.Repeat([]byte("file-byte-"), 10<<10)
	inPath, outPath := filepath.Join(dir, "input"), filepath.Join(dir, "output")
	if err := os.WriteFile(inPath, append([]byte("skip"), payload...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outPath, []byte("prefix:"), 0o600); err != nil {
		t.Fatal(err)
	}
	in := terminalOpenFile(t, inPath, unix.O_RDONLY)
	flags := unix.O_WRONLY
	if appendMode {
		flags |= unix.O_APPEND
	}
	out := terminalOpenFile(t, outPath, flags)
	if _, err := in.Seek(4, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	outOffset := int64(len("prefix:"))
	if appendMode {
		outOffset = 0 // O_APPEND must survive rather than overwrite this prefix.
	}
	if _, err := out.Seek(outOffset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	inFlags, outFlags := terminalFlags(t, in), terminalFlags(t, out)
	adapter := terminalOpenAdapter(t, in, out)
	if err := adapter.EnterRaw(); err != nil {
		t.Fatal(err)
	}
	first := make([]byte, len(payload))
	n, err := adapter.Source().Read(first)
	if err != nil || n != syscallBytes || !bytes.Equal(first[:n], payload[:n]) {
		t.Fatalf("regular file bounded read=%d err=%v", n, err)
	}
	rest, err := io.ReadAll(adapter.Source())
	if err != nil || !bytes.Equal(append(first[:n], rest...), payload) {
		t.Fatalf("regular-file offset/content mismatch: err=%v", err)
	}
	if n, err := adapter.Sink().Write(payload); err != nil || n != len(payload) {
		t.Fatalf("regular-file write=%d %v", n, err)
	}
	if err := adapter.Source().Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Sink().Cancel(); err != nil {
		t.Fatal(err)
	}
	if n, err := adapter.Source().Read(first); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cancelled regular-file read=%d %v", n, err)
	}
	if n, err := adapter.Sink().Write([]byte("must-not-write")); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cancelled regular-file write=%d %v", n, err)
	}
	terminalCloseConcurrently(t, adapter)
	terminalRequireOriginalFlags(t, in, inFlags)
	terminalRequireOriginalFlags(t, out, outFlags)
	if offset, err := in.Seek(0, io.SeekCurrent); err != nil || offset != int64(4+len(payload)) {
		t.Fatalf("original input offset=%d err=%v", offset, err)
	}
	actual, err := os.ReadFile(outPath)
	if err != nil || !bytes.Equal(actual, append([]byte("prefix:"), payload...)) {
		t.Fatalf("regular-file output/append mismatch: err=%v", err)
	}
}

func exerciseTerminalNull(t *testing.T) {
	file := terminalOpenFile(t, "/dev/null", unix.O_RDWR)
	wantFlags := terminalFlags(t, file)
	adapter := terminalOpenAdapter(t, file, file)
	if err := adapter.EnterRaw(); err != nil {
		t.Fatal(err)
	}
	if n, err := adapter.Source().Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("/dev/null read=%d %v", n, err)
	}
	payload := make([]byte, 3*syscallBytes+1)
	if n, err := adapter.Sink().Write(payload); n != len(payload) || err != nil {
		t.Fatalf("/dev/null write=%d %v", n, err)
	}
	terminalCloseConcurrently(t, adapter)
	terminalRequireOriginalFlags(t, file, wantFlags)
}

func exerciseTerminalUnsupported(t *testing.T) {
	good := newTerminalFixture(t, "tty")
	flags, state := terminalFlags(t, good.in), terminalTermios(t, good.in)
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	socket := terminalOwnFD(t, fds[0])
	_ = terminalOwnFD(t, fds[1])
	directory, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = directory.Close() })
	unsafeDevice := terminalOpenFile(t, "/dev/zero", unix.O_RDONLY)
	for _, row := range []struct {
		name, stream, kind string
		in, out            *os.File
	}{
		{"socket_input", "stdin", "socket", socket, good.out},
		{"socket_output", "stdout", "socket", good.in, socket},
		{"directory", "stdin", "directory", directory, good.out},
		{"unsafe_device", "stdin", "character device", unsafeDevice, good.out},
		{"pty_master", "stdin", "PTY master", good.inputPeer, good.out},
	} {
		t.Run(row.name, func(t *testing.T) {
			adapter, err := Open(row.in, row.out)
			if adapter != nil {
				_ = adapter.Close()
				t.Fatal("unsupported descriptor acquired an adapter")
			}
			var unsupported *ErrUnsupportedStdio
			if !errors.As(err, &unsupported) || unsupported.Stream != row.stream || unsupported.Kind != row.kind {
				t.Fatalf("unsupported error=%v typed=%+v want=%s/%s", err, unsupported, row.stream, row.kind)
			}
			terminalRequireOriginalFlags(t, good.in, flags)
			if !reflect.DeepEqual(state, terminalTermios(t, good.in)) {
				t.Fatal("Open failure changed termios before create")
			}
			if _, err := row.in.Stat(); err != nil {
				t.Fatalf("Open failure closed original stdin: %v", err)
			}
			if _, err := row.out.Stat(); err != nil {
				t.Fatalf("Open failure closed original stdout: %v", err)
			}
		})
	}
	if adapter, err := Open(nil, good.out); adapter != nil || !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("nil input=%v %v", adapter, err)
	}
}

func exerciseTerminalEOF(t *testing.T) {
	f := newTerminalFixture(t, "fifo")
	adapter := terminalOpenAdapter(t, f.in, f.out)
	const tail = "fifo-tail-before-HUP"
	terminalPeerWrite(t, f.inputPeer, []byte(tail))
	if err := f.inputPeer.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(adapter.Source())
	if err != nil || string(got) != tail {
		t.Fatalf("FIFO HUP lost unread tail: bytes=%q err=%v", got, err)
	}
	// EOF in one direction leaves output usable until explicit cancellation.
	if n, err := adapter.Sink().Write([]byte("still-output")); n != len("still-output") || err != nil {
		t.Fatalf("input EOF closed output: %d %v", n, err)
	}
	if got := terminalPeerRead(t, f.outPeer, len("still-output")); string(got) != "still-output" {
		t.Fatalf("output after input EOF=%q", got)
	}
	terminalCloseConcurrently(t, adapter)
}

func terminalOpenAdapter(t *testing.T, in, out *os.File) *IO {
	t.Helper()
	adapter, err := Open(in, out)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// A mutation removing the production wake must fail promptly, and the
		// fixture must still recover its workers before releasing kernel peers.
		for _, s := range []*stream{adapter.local.in, adapter.local.out} {
			select {
			case <-s.done:
			default:
				_ = s.Cancel()
				var wake [8]byte
				binary.NativeEndian.PutUint64(wake[:], 1)
				_, _ = unix.Write(s.wake, wake[:])
			}
		}
		if err := adapter.Close(); err != nil {
			t.Errorf("adapter cleanup: %v", err)
		}
	})
	return adapter
}

func terminalCloseConcurrently(t *testing.T, adapter *IO) {
	t.Helper()
	done := make(chan error, 8)
	for i := 0; i < cap(done); i++ {
		go func() { done <- adapter.Close() }()
	}
	for i := 0; i < cap(done); i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not join workers")
		}
	}
	if err := adapter.Restore(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Source().Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Sink().Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.EnterRaw(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("EnterRaw after Close=%v", err)
	}
}

func terminalAwaitPoll(t *testing.T, direction string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	stack := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "terminalio.(*stream).wait(") &&
				strings.Contains(goroutine, "terminalio.(*stream)."+direction+"(") &&
				strings.Contains(goroutine, "unix.Ppoll(") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("%s never reached a real blocked poll", direction)
}

func terminalAwaitResult(t *testing.T, done <-chan ioResult) ioResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("Cancel did not wake blocked I/O")
		return ioResult{}
	}
}

func terminalAwaitJoined(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled worker did not exit")
	}
}

func terminalOwnFD(t *testing.T, fd int) *os.File {
	t.Helper()
	file := os.NewFile(uintptr(fd), fmt.Sprintf("fixture-fd-%d", fd))
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func terminalOpenFile(t *testing.T, path string, flags int) *os.File {
	t.Helper()
	fd, err := unix.Open(path, flags|unix.O_CLOEXEC|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	return terminalOwnFD(t, fd)
}

func terminalFD(t *testing.T, file *os.File) int {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	if err := raw.Control(func(value uintptr) { fd = int(value) }); err != nil {
		t.Fatal(err)
	}
	return fd
}

func terminalSetNonblock(t *testing.T, file *os.File, enabled bool) {
	t.Helper()
	if err := unix.SetNonblock(terminalFD(t, file), enabled); err != nil {
		t.Fatal(err)
	}
}

func terminalFlags(t *testing.T, file *os.File) [2]int {
	t.Helper()
	fd := terminalFD(t, file)
	status, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	return [2]int{status, descriptor}
}

func terminalRequireOriginalFlags(t *testing.T, file *os.File, want [2]int) {
	t.Helper()
	if got := terminalFlags(t, file); got != want {
		t.Fatalf("original flags changed: got=%#x want=%#x", got, want)
	}
	if _, err := file.Stat(); err != nil {
		t.Fatalf("original descriptor closed: %v", err)
	}
}

func terminalTermios(t *testing.T, file *os.File) *unix.Termios {
	t.Helper()
	state, err := unix.IoctlGetTermios(terminalFD(t, file), unix.TCGETS)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func terminalPeerWrite(t *testing.T, file *os.File, data []byte) {
	t.Helper()
	fd := terminalFD(t, file)
	deadline := time.Now().Add(5 * time.Second)
	for len(data) > 0 && time.Now().Before(deadline) {
		n, err := unix.Write(fd, data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	if len(data) != 0 {
		t.Fatal("peer write did not complete")
	}
}

func terminalPeerRead(t *testing.T, file *os.File, size int) []byte {
	t.Helper()
	fd := terminalFD(t, file)
	data := make([]byte, size)
	read := 0
	deadline := time.Now().Add(5 * time.Second)
	for read < size && time.Now().Before(deadline) {
		n, err := unix.Read(fd, data[read:])
		if n > 0 {
			read += n
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	if read != size {
		t.Fatalf("peer read=%d want=%d", read, size)
	}
	return data
}
