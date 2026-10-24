// Real: portable PTY sessions, workload-owned children, bounded input and Close
// wakeup. Controlled: no broker callbacks; subject deduplication belongs to agent.
// Linux identity checks run here; Darwin execution requires its own runner.
package pty

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 plan §7.1 / §12 P2
func TestLegacyPTYOwnerRepeatedLifecycle(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyPTYLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "portable legacy PTY owner", beforeG)
	testharness.AssertNoFDLeak(t, "portable legacy PTY owner", beforeFD, 0)
}

func exerciseLegacyPTYLifecycle(t *testing.T) {
	t.Helper()
	f := newLegacyPTYFixture(t, "stty -echo; printf READY; IFS= read -r token; printf 'TAIL:%s:' \"$token\"; stty size; exit 23")
	if _, err := NewLegacyIO(f.session); err == nil {
		t.Fatal("second legacy owner accepted")
	}
	if _, _, err := f.session.TakeMaster(); err == nil {
		t.Fatal("legacy owner allowed transfer while borrowing master")
	}
	f.ready(t)
	payload := []byte("original-input\n")
	if err := f.io.Resize(137, 43); err != nil {
		t.Fatal(err)
	}
	beforeKA := f.io.LastKeepalive()
	if err := f.io.Keepalive(); err != nil || f.io.LastKeepalive().Before(beforeKA) {
		t.Fatalf("keepalive failed: %v", err)
	}
	if err := f.io.EnqueueInput(payload); err != nil {
		t.Fatal(err)
	}
	// Mutation of caller storage may not change the already accepted batch.
	for i := range payload {
		payload[i] = 'x'
	}
	tail, err := io.ReadAll(f.io)
	if err != nil || string(tail) != "TAIL:original-input:43 137\r\n" {
		t.Fatalf("legacy terminal bytes=%q err=%v", tail, err)
	}
	if err := f.manager.AllowReap("legacy"); err != nil {
		t.Fatal(err)
	}
	exit, err := f.manager.Wait("legacy")
	if err != nil || exit.Code != 23 {
		t.Fatalf("legacy exit=%+v err=%v", exit, err)
	}
	f.finish(t)

	blocked := newLegacyPTYFixture(t, "stty raw -echo; printf READY; exec sleep 30")
	blocked.ready(t)
	// A real short write and timeout establish a full kernel input queue;
	// the queue exercise cannot pass simply because its slave kept reading.
	if err := blocked.io.master.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	attempt := bytes.Repeat([]byte{'a'}, 1<<20)
	n, err := blocked.io.master.Write(attempt)
	if n <= 0 || n >= len(attempt) || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("blocked-slave witness n=%d/%d err=%v", n, len(attempt), err)
	}
	if err := blocked.io.master.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := blocked.io.EnqueueInput(bytes.Repeat([]byte{'b'}, upgradebudget.PTYInputBytes)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("legacy enqueue waited for blocked kernel write")
	}
	if err := blocked.io.Resize(91, 32); err != nil {
		t.Fatal(err)
	}
	if err := blocked.io.Keepalive(); err != nil {
		t.Fatal(err)
	}
	for retry := 0; retry < 3; retry++ {
		if err := blocked.io.EnqueueInput(make([]byte, upgradebudget.PTYInputBytes+1)); !errors.Is(err, ErrLegacyInputOverflow) {
			t.Fatalf("overflow was not terminal: %v", err)
		}
	}
	if !errors.Is(blocked.io.Failure(), ErrLegacyInputOverflow) {
		t.Fatal("overflow failure not retained")
	}
	select {
	case <-blocked.io.FailureReady():
	default:
		t.Fatal("overflow did not notify the agent adapter")
	}
	blocked.io.mu.Lock()
	used := blocked.io.used
	blocked.io.mu.Unlock()
	if used < 0 || used > upgradebudget.PTYInputBytes {
		t.Fatalf("unbounded queue used=%d", used)
	}
	select {
	case <-blocked.io.writerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("overflow failed to wake blocked writer")
	}
	blocked.finish(t)
}

// origin: hot-update S4 plan §3.1 / §7.3
func TestLegacyPTYOwnerRefusesUnreadySessions(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if _, err := NewLegacyIO(nil); err == nil {
		t.Fatal("nil session accepted")
	}
	for _, failed := range []bool{false, true} {
		s, err := Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		if failed {
			_ = s.Start([]string{"/no/such/legacy-pty-command"}, nil, "", "")
		}
		if _, err := NewLegacyIO(s); err == nil {
			t.Fatal("unready Session became legacy I/O owner")
		}
		_ = s.Close()
	}
}

type legacyPTYFixture struct {
	session  *Session
	manager  *workload.Manager
	io       *LegacyIO
	finished bool
}

func newLegacyPTYFixture(t *testing.T, script string) *legacyPTYFixture {
	t.Helper()
	s, err := Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start([]string{"/bin/sh", "-c", script}, nil, "", ""); err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	f := &legacyPTYFixture{session: s, manager: &workload.Manager{}}
	id := ptyChildIdentity(t, s, "legacy")
	if err := s.AdoptDraining(f.manager, id); err != nil {
		_ = s.Signal(syscall.SIGKILL)
		_, _ = s.Wait()
		_ = s.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { f.finish(t) })
	f.io, err = NewLegacyIO(s)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *legacyPTYFixture) ready(t *testing.T) {
	t.Helper()
	if err := f.io.master.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ready := make([]byte, 5)
	if _, err := io.ReadFull(f.io, ready); err != nil || string(ready) != "READY" {
		t.Fatalf("legacy readiness=%q %v", ready, err)
	}
	if err := f.io.master.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}
func (f *legacyPTYFixture) finish(t *testing.T) {
	t.Helper()
	if f.finished {
		return
	}
	f.finished = true
	if f.io != nil {
		_ = f.io.Close()
		_ = f.io.Close()
		if _, err := f.io.master.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("legacy Close did not release its master: %v", err)
		}
		select {
		case <-f.io.writerDone:
		default:
			t.Error("legacy Close did not join writer")
		}
		select {
		case <-f.io.watchDone:
		default:
			t.Error("legacy Close did not join failure watcher")
		}
	} else {
		_ = f.session.Close()
	}
	f.manager.Thaw()
	_ = f.manager.Signal("legacy", syscall.SIGKILL)
	_ = f.manager.AllowReap("legacy")
	if _, err := f.manager.Wait("legacy"); err != nil {
		t.Error(err)
	}
	if err := f.manager.Forget("legacy"); err != nil {
		t.Error(err)
	}
}

func ptyChildIdentity(t *testing.T, session *Session, name string) workload.Identity {
	t.Helper()
	id := workload.Identity{ID: name, OSPID: session.OSPID(), StartedAt: time.Now().UTC()}
	if runtime.GOOS != "linux" {
		return id
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	id.BootID = strings.TrimSpace(string(boot))
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(id.OSPID) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) < 20 {
		t.Fatal("child stat incomplete")
	}
	id.StartTimeTicks, err = strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
