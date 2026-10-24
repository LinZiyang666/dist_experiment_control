//go:build linux

// Real: original kernel sockets, nonblocking/CLOEXEC flags, identity and salvage.
// Stubbed: FileSource transfer failure only. Frozen-owner integration needs P1.
// No carrier or same-PID exec is exercised here.
package rawsocket

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S5 plan §5.1 / §12.2 P6
func TestRawSocketImageIdentityAndSalvage(t *testing.T) {
	before, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseRawSocketImage(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "raw socket image", before)
	testharness.AssertNoFDLeak(t, "raw socket image", fds, 0)
}
func exerciseRawSocketImage(t *testing.T) {
	t.Helper()
	f, peer, want := rawImagePair(t)
	defer func(closeFunc func() error) { _ = closeFunc() }(f.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	other, otherPeer, _ := rawImagePair(t)
	defer func(closeFunc func() error) { _ = closeFunc() }(other.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(otherPeer.Close)
	src := &rawImageSource{file: f}
	got, err := TakeImageFile(src, "flow.01", "raw", want)
	if err != nil || got != f || src.calls != 1 {
		t.Fatalf("original transfer: %v %v calls=%d", got, err, src.calls)
	}
	for _, wrong := range []Fingerprint{{Dev: want.Dev, Ino: want.Ino + 1, Type: want.Type}, {Dev: want.Dev + 1, Ino: want.Ino, Type: want.Type}, {Dev: want.Dev, Ino: want.Ino, Type: unix.S_IFREG}, {}} {
		src = &rawImageSource{file: f}
		got, err = TakeImageFile(src, "flow.01", "raw", wrong)
		if got != f || !errors.Is(err, ErrIdentity) {
			t.Fatalf("wrong fingerprint accepted or salvage lost: got=%v err=%v", got, err)
		}
	}
	src = &rawImageSource{file: other}
	got, err = TakeImageFile(src, "flow.01", "raw", want)
	if got != other || !errors.Is(err, ErrIdentity) {
		t.Fatal("swapped same-shape descriptor accepted or lost")
	}
	src = &rawImageSource{file: f, err: errors.New("transfer failed after take")}
	got, err = TakeImageFile(src, "flow.01", "raw", want)
	if got != f || err == nil || src.calls != 1 {
		t.Fatal("failed Take discarded transferred descriptor")
	}
	if err := validateImageFile(f, want); err != nil {
		t.Fatal("failed checks mutated original socket")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(r.Close)
	defer func(closeFunc func() error) { _ = closeFunc() }(w.Close)
	if _, err := TakeImageFile(&rawImageSource{file: r}, "flow.01", "raw", want); !errors.Is(err, ErrIdentity) {
		t.Fatal("accepted non-socket descriptor")
	}
}
func rawImagePair(t *testing.T) (*os.File, *os.File, Fingerprint) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	f, p := os.NewFile(uintptr(pair[0]), "image-raw"), os.NewFile(uintptr(pair[1]), "image-peer")
	var stat unix.Stat_t
	if err := unix.Fstat(pair[0], &stat); err != nil {
		_ = f.Close()
		_ = p.Close()
		t.Fatal(err)
	}
	return f, p, Fingerprint{Dev: uint64(stat.Dev), Ino: stat.Ino, Type: stat.Mode & unix.S_IFMT}
}

// origin: hot-update S5 plan §5.1 / §3.2
func TestRawSocketImageRejectsMutableFlags(t *testing.T) {
	for _, cmd := range []int{unix.F_GETFL, unix.F_GETFD} {
		t.Run(fmt.Sprintf("flag-%d", cmd), func(t *testing.T) {
			f, p, want := rawImagePair(t)
			defer func(closeFunc func() error) { _ = closeFunc() }(f.Close)
			defer func(closeFunc func() error) { _ = closeFunc() }(p.Close)
			raw, err := f.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			bit, set := unix.O_NONBLOCK, unix.F_SETFL
			if cmd == unix.F_GETFD {
				bit, set = unix.FD_CLOEXEC, unix.F_SETFD
			}
			var original int
			if err := raw.Control(func(fd uintptr) {
				original, err = unix.FcntlInt(fd, cmd, 0)
				if err == nil {
					_, err = unix.FcntlInt(fd, set, original&^bit)
				}
			}); err != nil {
				t.Fatal(err)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := validateImageFile(f, want); !errors.Is(err, ErrIdentity) {
				t.Fatalf("accepted descriptor with missing required flag: %v", err)
			}
			var flags int
			if err := raw.Control(func(fd uintptr) { flags, err = unix.FcntlInt(fd, cmd, 0); _, _ = unix.FcntlInt(fd, set, original) }); err != nil {
				t.Fatal(err)
			}
			if err != nil || flags&bit != 0 {
				t.Fatal("validation repaired descriptor flags")
			}
		})
	}
}

// origin: hot-update S5 plan §5.3
func TestRawSocketImageExportRequiresFrozenOwner(t *testing.T) {
	for _, s := range []*Socket{nil, {}} {
		if f, err := s.ExportFile(); f != nil || !errors.Is(err, ErrClosed) {
			t.Fatalf("unowned export returned %v %v", f, err)
		}
	}
}

type rawImageSource struct {
	file  *os.File
	err   error
	calls int
}

func (s *rawImageSource) TakeFile(id, name string) (*os.File, error) {
	s.calls++
	f := s.file
	s.file = nil
	return f, s.err
}
