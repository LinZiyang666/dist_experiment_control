// These tests compare original Linux open-file descriptions and real sockets.
// Kernel fixtures exercise reopen rejection; no product cut is certified here.
package fullstackharness

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S10 plan §4.2 / P04
func TestFullstackOFDWitnessRejectsReopen(t *testing.T) {
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseOFDWitness(t)
	}
	testharness.AssertNoGoroutineLeak(t, "OFD witness lifecycle", g)
	testharness.AssertNoFDLeak(t, "OFD witness lifecycle", f, 0)
}

func exerciseOFDWitness(t *testing.T) {
	t.Helper()
	// ctx-root: one hermetic kernel lifecycle, with no product ownership.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	owner := fixtureStamp(t)
	file, err := os.CreateTemp(t.TempDir(), "same-inode-*")
	if err != nil {
		t.Fatal(err)
	}
	fileClosed := false
	defer func() {
		// The original is explicitly closed to test a surviving surrogate dup.
		// Its fallback must not close the same descriptor a second time.
		if !fileClosed {
			closeIdentityFixture(t, file.Close)
		}
	}()
	if _, err := file.WriteString("nonempty OFD exercise"); err != nil {
		t.Fatal(err)
	}
	original := int(file.Fd())
	flags, err := ReadDescriptorFlags(os.Getpid(), original)
	if err != nil || flags&unix.O_CLOEXEC == 0 {
		t.Fatalf("original CLOEXEC: %x %v", flags, err)
	}
	w, err := NewOFDWitness(owner, original)
	if err != nil {
		t.Fatalf("SETUP: real KCMP_FILE witness required: %v", err)
	}
	defer closeIdentityFixture(t, w.Close)
	proof, err := w.Check(ctx, owner, original)
	if err != nil || proof.Flags != flags {
		t.Fatalf("original OFD not witnessed: %+v %v", proof, err)
	}
	if flags, err := unix.FcntlInt(uintptr(proof.WitnessFD), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("OFD witness duplicate lacks CLOEXEC: %v", err)
	}
	dup, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIdentityFixture(t, func() error { return unix.Close(dup) })
	if _, err := w.Check(ctx, owner, dup); err != nil {
		t.Fatalf("real duplicate rejected: %v", err)
	}
	reopen, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIdentityFixture(t, reopen.Close)
	a, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	b, err := reopen.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("reopen control must have exactly the same inode")
	}
	if _, err := w.Check(ctx, owner, int(reopen.Fd())); !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("same-inode reopen signed as original OFD: %v", err)
	}
	bad := owner
	bad.Process.StartTicks++
	if _, err := w.Check(ctx, bad, original); err == nil {
		t.Fatal("wrong process incarnation accepted")
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := w.Check(cancelled, owner, original); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel ignored: %v", err)
	}
	// Failed duplicate construction must close its already-created pidfd.
	if _, err := NewOFDWitness(owner, -1); err == nil {
		t.Fatal("negative FD accepted")
	}
	if _, err := NewOFDWitness(owner, 1<<28); err == nil {
		t.Fatal("missing FD accepted")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(proof.WitnessFD), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("witness duplicate remained open after Close: %v", err)
	}
	if _, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0); err != nil {
		t.Fatalf("Close destroyed borrowed original: %v", err)
	}
	if _, err := w.Check(ctx, owner, original); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed witness accepted: %v", err)
	}
	// Closing the original while a surrogate dup survives is a different defect
	// from reopening the inode, and needs its own adversarial assertion.
	lost, err := NewOFDWitness(owner, original)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIdentityFixture(t, lost.Close)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	fileClosed = true
	if _, err := lost.Check(ctx, owner, dup); err == nil {
		t.Fatal("closed original hidden by surviving duplicate")
	}
	if err := lost.Close(); err != nil {
		t.Fatal(err)
	}
	// Socket cookies corroborate the real kcmp comparison without mutating the
	// original O_NONBLOCK flags or introducing net.Conn.File's side effects.
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIdentityFixture(t, func() error { return unix.Close(pair[0]) })
	defer closeIdentityFixture(t, func() error { return unix.Close(pair[1]) })
	socketFlags, err := ReadDescriptorFlags(os.Getpid(), pair[0])
	if err != nil {
		t.Fatal(err)
	}
	socket, err := NewOFDWitness(owner, pair[0])
	if err != nil {
		t.Fatal(err)
	}
	defer closeIdentityFixture(t, socket.Close)
	socketProof, err := socket.Check(ctx, owner, pair[0])
	if err != nil || socketProof.SocketCookie == 0 || socketProof.Flags != socketFlags {
		t.Fatalf("socket witness changed flags or lacked cookie: %+v %v", socketProof, err)
	}
	if _, err := unix.Write(pair[0], []byte("socket I/O")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, err := unix.Read(pair[1], buffer)
	if err != nil || string(buffer[:n]) != "socket I/O" {
		t.Fatalf("socket I/O: %q %v", buffer[:n], err)
	}
	t.Logf("OFD kernel witness: original=%+v socket=%+v", proof, socketProof)
}
