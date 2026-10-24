//go:build linux

package ptysession

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// Real: zombie leader, sole descendant-held slave, output after the former
// 200ms cutoff, actual same-PID exec and original leader exit status 23.
// origin: hot-update S4 plan §3.3 / T02
func TestPtyTailOutlivesLeaderExit(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if mode := os.Getenv(actorImageMode); mode != "" {
		runActorImage(t, mode, "tail")
		return
	}
	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("tail", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			runActorExecParent(t, "tail")
		})
	}
	testharness.AssertNoGoroutineLeak(t, "descendant tail", g0)
	testharness.AssertNoFDLeak(t, "descendant tail", fd0, 0)
}

// Real: kernel Read n>0 and replay write failure; the still-live slave writes
// again after Error. Controlled: read-return barrier and a read-only replay FD.
// origin: hot-update S4 plan §3.3 / T13
func TestPtyOutputAbortFreezesFinalEnd(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("abort", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseOutputAbort(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "output abort finalizer", g0)
	testharness.AssertNoFDLeak(t, "output abort finalizer", fd0, 0)
}
func exerciseOutputAbort(t *testing.T) {
	f := startActorFixture(t, `tty.setraw(0)
mark('ready')
wait('prefix')
os.write(1,b'RETAINED_PREFIX')
mark('prefix-written')
wait('fail')
os.write(1,b'PENDING_SUFFIX')
mark('failed-read-written')
wait('discard')
os.write(1,b'MUST_NOT_EXTEND_END')
mark('discard-written')
wait('exit')
`)
	waitMark(t, f.dir, "ready")
	markActor(t, f.dir, "prefix")
	waitOutput(t, f.a, "RETAINED_PREFIX")
	image, files := freezeActor(t, f.a)
	restore := makeReplayReadOnly(t, files["output-replay"])
	defer restore()
	m := &observedMaster{Master: f.a.cfg.Master, readReturned: make(chan struct{}, 1)}
	release := make(chan struct{})
	defer func() {
		if !stopped(release) {
			close(release)
		}
	}()
	m.releaseRead = release
	f.a.cfg.Master = m
	activateActor(t, f.a)
	markActor(t, f.dir, "fail")
	select {
	case <-m.readReturned:
	case <-time.After(actorTestBudget):
		t.Fatal("no real read before append fault")
	}
	frozen := make(chan Image, 1)
	failure := make(chan error, 1)
	go func() {
		s, _, err := f.a.Freeze(time.Now().Add(actorTestBudget))
		if err != nil {
			failure <- err
		} else {
			frozen <- s
		}
	}()
	select {
	case <-frozen:
		t.Fatal("freeze passed before Read n accounting")
	case err := <-failure:
		t.Fatal(err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	var aborted Image
	select {
	case aborted = <-frozen:
	case err := <-failure:
		t.Fatal(err)
	case <-time.After(actorTestBudget):
		t.Fatal("freeze did not join reader")
	}
	if aborted.Output.End != image.Output.End || len(aborted.Pending) == 0 || !bytes.Equal(aborted.Pending, []byte("PENDING_SUFFIX")) || aborted.OutputError == "" || !aborted.Discarding || aborted.SourceEnded {
		t.Fatalf("abort lost responsibility: %+v", aborted)
	}
	row, _ := f.manager.Lookup("terminal")
	if !row.ReapHeld || row.Exit != nil {
		t.Fatal("logical abort released leader")
	}
	restore()
	activateActor(t, f.a)
	batch, err := f.a.OutputBatch(0, false, 3)
	if err != nil || batch.Error != "" || batch.FIN {
		t.Fatalf("error before final batch %+v %v", batch, err)
	}
	batch, err = f.a.OutputBatch(3, false, 32768)
	if err != nil || batch.Error == "" || batch.FIN {
		t.Fatalf("missing final error %+v %v", batch, err)
	}
	markActor(t, f.dir, "discard")
	waitMark(t, f.dir, "discard-written")
	// A subsequent cut joins the discard read; Error and End must be stable even
	// when storage is writable again, so Pending cannot silently rejoin delivery.
	again, _ := freezeActor(t, f.a)
	if again.Output.End != aborted.Output.End || again.OutputError != aborted.OutputError || !bytes.Equal(again.Pending, aborted.Pending) || again.SourceEnded {
		t.Fatal("abort end grew or forged source end")
	}
	activateActor(t, f.a)
	markActor(t, f.dir, "exit")
	awaitActor(t, "discard source actually ended", func() bool { f.a.mu.Lock(); defer f.a.mu.Unlock(); return f.a.source == sourceEnded })
	if _, err := f.a.OutputBatch(aborted.Output.End, true, 1); err != nil {
		t.Fatal("aborted terminal ACK failed", err)
	}
}

func makeReplayReadOnly(t *testing.T, file *os.File) func() {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	target, backup := -1, -1
	var inner error
	if err := raw.Control(func(fd uintptr) { target = int(fd); backup, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || inner != nil {
		t.Fatalf("duplicate replay: %v %v", err, inner)
	}
	readOnly, err := os.Open("/proc/self/fd/" + strconv.Itoa(target))
	if err != nil {
		_ = unix.Close(backup)
		t.Fatal(err)
	}
	roRaw, err := readOnly.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := roRaw.Control(func(fd uintptr) { inner = unix.Dup3(int(fd), target, unix.O_CLOEXEC) }); err != nil || inner != nil {
		t.Fatalf("inject read-only replay: %v %v", err, inner)
	}
	_ = readOnly.Close()
	restored := false
	return func() {
		if restored {
			return
		}
		restored = true
		if err := unix.Dup3(backup, target, unix.O_CLOEXEC); err != nil {
			t.Error(err)
		}
		_ = unix.Close(backup)
	}
}
