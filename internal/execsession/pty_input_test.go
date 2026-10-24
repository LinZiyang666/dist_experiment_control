//go:build linux

package execsession

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S4 internal review rv-eof rv-eof-2 (independent verifier)
// A real child emits a retained tail and keeps its slave open. A read-only
// duplicate of its real input replay causes an actual failed pwrite, not a
// fabricated status. The original descriptor is restored before teardown.
func TestPTYCommittedReplayAbortIsAStatusReply(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseCommittedReplayAbortStatus(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "committed replay abort status", beforeG)
	testharness.AssertNoFDLeak(t, "committed replay abort status", beforeFD, 0)
}

func exerciseCommittedReplayAbortStatus(t *testing.T) {
	t.Helper()
	f := newPTYRuntime(t, 0)
	q, owner := ptyCreate(t, f, "replay-status", []string{"/bin/sh", "-c", "stty raw -echo; printf TAIL; read line"})
	ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
		return p.Stdout != nil && bytes.Contains(p.Stdout.Data, []byte("TAIL"))
	})
	imageFreeze(t, f.r)
	_, files, err := owner.pty.Freeze(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	file := files["input-replay"]
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	target, backup := -1, -1
	var inner error
	if err := raw.Control(func(fd uintptr) { target = int(fd); backup, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || inner != nil {
		t.Fatalf("backup: %v %v", err, inner)
	}
	defer func() { _ = unix.Close(backup) }()
	ro, err := os.Open("/proc/self/fd/" + strconv.Itoa(target))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	roRaw, err := ro.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := roRaw.Control(func(fd uintptr) { inner = unix.Dup3(int(fd), target, unix.O_CLOEXEC) }); err != nil || inner != nil {
		t.Fatalf("install read-only replay: %v %v", err, inner)
	}
	defer func() {
		if err := unix.Dup3(backup, target, unix.O_CLOEXEC); err != nil {
			t.Error(err)
		}
	}()
	q.Op = proto.ProcessOpInput
	q.Input = []byte("DATA")
	reply, callErr := f.r.Handle(f.scope, q, "pty-inbox")
	t.Logf("runtime error=%v; status A=%d Q=%d ended=%v inputError=%q tail=%+v", callErr, reply.InputACK, reply.InputAccepted, reply.InputEnded, reply.InputError, reply.Stdout)
	if !reply.InputEnded || reply.InputError != "pty input aborted: replay_error" || reply.InputACK != 0 || reply.InputAccepted != 0 || reply.Stdout == nil || !bytes.Equal(reply.Stdout.Data, []byte("TAIL")) {
		t.Fatalf("fault premise not established: %+v %v", reply, callErr)
	}
	if reply.Kind != "status" || reply.Stderr == nil || !reply.Stderr.FIN || len(reply.Stderr.Data) != 0 || reply.Stderr.Offset != 0 {
		t.Fatalf("abort lost merged status envelope: %+v", reply)
	}
	if callErr != nil {
		t.Errorf("committed abort escaped as top-level refusal: %v", callErr)
	}
	// A committed abort must not turn subsequent invalid sequences into status.
	// The wire envelope still refuses requests that own no committed mutation.
	q.InputSeq = 1
	if _, err := f.r.Handle(f.scope, q, "pty-inbox"); !errors.Is(err, ptysession.ErrConflict) {
		t.Fatalf("input abort swallowed a sequence refusal: %v", err)
	}
}
