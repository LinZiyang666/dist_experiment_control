//go:build linux

package ptysession

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Real: independently blocked master read/write syscalls and the original
// slave's byte echo. Controlled: direction admission and a delayed write return.
// origin: hot-update S4 plan §4.1 / T06
func TestPtyDuplexDeadlineIsolation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("directions", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseDeadlineIsolation(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "duplex freeze", g0)
	testharness.AssertNoFDLeak(t, "duplex freeze", fd0, 0)
}
func exerciseDeadlineIsolation(t *testing.T) {
	f := startActorFixture(t, `tty.setraw(0)
import select
mark('ready')
while not os.path.exists(root+'/exit'):
 if os.path.exists(root+'/output') and not os.path.exists(root+'/sent'):
  os.write(1,b'INDEPENDENT_OUTPUT')
  mark('sent')
 if select.select([0],[],[],.01)[0]:
  data=os.read(0,4096)
  os.write(1,data)
`)
	waitMark(t, f.dir, "ready")
	m := instrumentActor(t, f.a)
	pauseDirection(t, f.a, true)
	if _, err := f.a.AcceptInput(0, []byte("write-with-read-stopped"), false); err != nil {
		t.Fatal(err)
	}
	awaitActor(t, "write advanced while read stopped", func() bool { return f.a.InputStatus().Applied == uint64(len("write-with-read-stopped")) })
	pauseDirection(t, f.a, false)
	resumeDirection(t, f.a, true)
	waitOutput(t, f.a, "write-with-read-stopped")
	if _, err := f.a.AcceptInput(uint64(len("write-with-read-stopped")), []byte("held"), false); err != nil {
		t.Fatal(err)
	}
	markActor(t, f.dir, "output")
	waitOutput(t, f.a, "INDEPENDENT_OUTPUT")
	if s := f.a.InputStatus(); s.Applied != uint64(len("write-with-read-stopped")) {
		t.Fatal("clearing read resumed write", s)
	}
	// Write remains expired after clearing read; then the reverse is checked.
	if n, err := m.Master.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write deadline cleared by read: n=%d err=%v", n, err)
	}
	pauseDirection(t, f.a, true)
	resumeDirection(t, f.a, false)
	awaitActor(t, "write resumed", func() bool { return f.a.InputStatus().Applied == uint64(len("write-with-read-stoppedheld")) })
	if n, err := m.Master.Read(make([]byte, 1)); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read deadline cleared by write: n=%d err=%v", n, err)
	}
	resumeDirection(t, f.a, true)
	waitOutput(t, f.a, "held")
	// The syscall really returns n>0, but accounting is held past Freeze's
	// deadline. Recovery must join that generation before admitting its successor.
	freezeActor(t, f.a)
	release := make(chan struct{})
	defer func() {
		if !stopped(release) {
			close(release)
		}
	}()
	m.writeReturned = make(chan struct{}, 1)
	m.releaseWrite = release
	activateActor(t, f.a)
	if _, err := f.a.AcceptInput(uint64(len("write-with-read-stoppedheld")), []byte("after-timeout"), false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.writeReturned:
	case <-time.After(actorTestBudget):
		t.Fatal("write did not return")
	}
	if _, _, err := f.a.Freeze(time.Now().Add(20 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("freeze did not include accounting: %v", err)
	}
	close(release)
	awaitActor(t, "write recovery after timeout", func() bool {
		return f.a.InputStatus().Applied == uint64(len("write-with-read-stoppedheldafter-timeout"))
	})
	waitOutput(t, f.a, "after-timeout")
	m.mu.Lock()
	maxRead, maxWrite := m.maxRead, m.maxWrite
	m.mu.Unlock()
	if maxRead != 1 || maxWrite != 1 {
		t.Fatalf("duplicate borrowers read=%d write=%d", maxRead, maxWrite)
	}
}
func pauseDirection(t *testing.T, a *Actor, read bool) {
	t.Helper()
	<-a.control
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	p := &a.write
	if read {
		p = &a.read
	}
	a.pauseLocked(p)
	done := p.done
	a.mu.Unlock()
	if err := a.directionDeadline(read, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := waitPump(done, time.Now().Add(actorTestBudget)); err != nil {
		t.Fatal(err)
	}
}
func resumeDirection(t *testing.T, a *Actor, read bool) {
	t.Helper()
	<-a.control
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	defer a.mu.Unlock()
	p := &a.write
	if read {
		p = &a.read
	}
	if err := a.directionDeadline(read, time.Time{}); err != nil {
		t.Fatal(err)
	}
	p.paused = false
	a.startPumpLocked(p, read)
}

// Real: an admitted Manager control holds the actor token. Freeze's absolute
// deadline includes admission and the short state-lock wait, without Close.
// origin: hot-update S4 plan §4.1 / T07
func TestPtyFreezeDeadlineIncludesControlAdmission(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("admission", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeAdmission(t)
		})
		t.Run("blocked-write", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeBlockedWrite(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "freeze admission", g0)
	testharness.AssertNoFDLeak(t, "freeze admission", fd0, 0)
}
func exerciseFreezeAdmission(t *testing.T) {
	f := startActorFixture(t, "tty.setraw(0)\nmark('ready')\ndata=os.read(0,100)\nos.write(1,data)\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	entered, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		<-f.a.control
		defer func() { f.a.control <- struct{}{} }()
		finished <- f.manager.WithControl("terminal", func(_ workload.Control) error { close(entered); <-release; return nil })
	}()
	select {
	case <-entered:
	case err := <-finished:
		requireDependency(t, err)
		t.Fatal("control was not admitted")
	case <-time.After(actorTestBudget):
		t.Fatal("control admission hung")
	}
	start := time.Now()
	freezeDone := make(chan error, 1)
	go func() { _, _, err := f.a.Freeze(start.Add(25 * time.Millisecond)); freezeDone <- err }()
	var err error
	select {
	case err = <-freezeDone:
	case <-time.After(250 * time.Millisecond):
		t.Error("freeze waited past control admission deadline")
	}
	close(release)
	if controlErr := <-finished; controlErr != nil {
		t.Fatal(controlErr)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) || time.Since(start) > 250*time.Millisecond {
		t.Fatalf("deadline began after token admission: %v elapsed=%v", err, time.Since(start))
	}
	f.a.mu.Lock()
	closed := f.a.state.MasterClosed
	f.a.mu.Unlock()
	if closed {
		t.Fatal("freeze closed master to wake borrowers")
	}
	if _, err := f.a.AcceptInput(0, []byte("still-live"), false); err != nil {
		t.Fatal(err)
	}
	waitOutput(t, f.a, "still-live")
	f.a.mu.Lock()
	result := make(chan error, 1)
	go func() { _, _, err := f.a.Freeze(time.Now().Add(25 * time.Millisecond)); result <- err }()
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Error(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("state admission ignored deadline")
	}
	f.a.mu.Unlock()
}

// A genuinely blocked raw-slave write cannot own the actor accounting mutex.
// The freeze must wake and join it, preserve n, and retain the original master.
// origin: hot-update S4 plan §4.1 / T07
func exerciseFreezeBlockedWrite(t *testing.T) {
	f := startActorFixture(t, blockedChild())
	waitMark(t, f.dir, "ready")
	m := instrumentActor(t, f.a)
	if _, err := f.a.AcceptInput(0, inputPayload(32768), false); err != nil {
		t.Fatal(err)
	}
	waitMark(t, f.dir, "blocked")
	done := make(chan error, 1)
	start := time.Now()
	go func() { _, _, err := f.a.Freeze(start.Add(100 * time.Millisecond)); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			_ = m.SetWriteDeadline(time.Now())
			t.Fatal("blocked write prevented bounded freeze", err)
		}
	case <-time.After(time.Second):
		// Release even a mutant holding mu across Write before fixture cleanup.
		_ = m.SetWriteDeadline(time.Now())
		<-done
		t.Fatal("blocked write held actor accounting lock")
	}
	if time.Since(start) > time.Second {
		t.Fatal("freeze exceeded deadline")
	}
	if _, err := m.Master.Borrow().Stat(); err != nil {
		t.Fatal("freeze closed original master", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.writes) != 1 || m.writes[0].N <= 0 || m.writes[0].N >= m.writes[0].Attempt {
		t.Fatalf("missing actual blocked write receipt: %+v", m.writes)
	}
}
