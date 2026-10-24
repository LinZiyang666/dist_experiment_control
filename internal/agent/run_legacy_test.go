//go:build linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

// origin: hot-update S4 plan §7.1 / T42. The first slave deliberately does
// not read in raw mode: its master Write must block on real kernel pressure.
// A second terminal sharing the wildcard still receives input, resize and KA.
func TestLegacyPTYInputDoesNotBlockOtherSessions(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseLegacyPTYIntake(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "legacy PTY intake x5", beforeG)
	testharness.AssertNoFDLeak(t, "legacy PTY intake x5", beforeFD, 0)
}

func exerciseLegacyPTYIntake(t *testing.T) {
	url := startNATS(t)
	a := newBootAgent(t, url, "node")
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	a.ncBox.Store(nc)
	fin := &sessionFinalizer{}
	t.Cleanup(func() {
		for _, closeSub := range fin.takeCleanups() {
			closeSub()
		}
	})
	if err := startPtyIntake(a, nc, fin); err != nil {
		t.Fatal(err)
	}
	// Only the subscription is under test here; the legacy/reliable reapers
	// have independent tests. A canceled loop avoids an artificial clock tick.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := a.startCtlLiveness(ctx, nc, fin); err != nil {
		t.Fatal(err)
	}
	failed, err := nc.SubscribeSync("_TINBOX.legacy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failed.Unsubscribe() })

	makeRun := func(pid, command string) *pty.Session {
		t.Helper()
		ready := filepath.Join(t.TempDir(), "ready")
		s, err := pty.Allocate(80, 24)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start([]string{"/bin/sh", "-c", "stty raw -echo; printf ready > \"$1\"; " + command, "fixture", ready}, nil, filepath.Dir(ready), ""); err != nil {
			_ = s.Close()
			t.Fatal(err)
		}
		rec := &procRec{sess: s, kaGrace: time.Minute, lastKA: time.Now().Add(-time.Hour)}
		rec.input = newLegacyRunInput(a, pid, s, nc, "_TINBOX.legacy")
		a.registerProc(pid, rec)
		t.Cleanup(func() {
			_ = s.Signal(syscall.SIGKILL)
			rec.input.Stop()
			_, _ = s.Wait()
			a.unregisterProc(pid)
		})
		waitFor(t, bootTestBudget, func() bool { b, _ := os.ReadFile(ready); return string(b) == "ready" })
		return s
	}
	makeRun("blocked", "exec sleep 60")
	healthy := makeRun("healthy", "exec cat")
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	publish := func(subject string, data []byte) {
		t.Helper()
		if err := nc.Publish(subject, data); err != nil {
			t.Fatal(err)
		}
	}
	legacy := func(pid, op string) string { return proto.SubjectPrefix + ".s.lab.pty." + pid + "." + op }
	node := func(pid, op string) string { return proto.SubjectPrefix + ".s.lab.node.node.pty." + pid + "." + op }
	// Larger than the kernel PTY buffer, below our memory budget. This
	// blocks the direct-WriteInput mutation inside the shared wildcard.
	publish(legacy("blocked", "in"), bytes.Repeat([]byte{'x'}, 192<<10))
	publish(legacy("healthy", "in"), []byte("free"))
	read := func(want string) {
		t.Helper()
		if err := healthy.Master.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, len(want))
		if _, err := io.ReadFull(healthy.Master, buf); err != nil || string(buf) != want {
			t.Fatalf("other terminal input stalled or duplicated: got %q, want %q: %v", buf, want, err)
		}
	}
	read("free")
	q := legacyPTYInputFor(a, "blocked")
	charged, _, _ := q.Snapshot()
	if charged == 0 {
		t.Fatal("fixture did not retain a real blocked master write")
	}
	publish(legacy("healthy", "resize"), []byte(`{"cols":111,"rows":37}`))
	publish(legacy("healthy", "ka"), nil)
	waitFor(t, time.Second, func() bool {
		var size *unix.Winsize
		raw, err := healthy.Master.SyscallConn()
		if err != nil {
			return false
		}
		_ = raw.Control(func(fd uintptr) { size, _ = unix.IoctlGetWinsize(int(fd), unix.TIOCGWINSZ) })
		a.procsMu.Lock()
		fresh := time.Since(a.procs["healthy"].lastKA) < time.Minute
		a.procsMu.Unlock()
		return size != nil && size.Col == 111 && size.Row == 37 && fresh
	})
	// N-1 dual publication must inject exactly one copy. Barrier joins all
	// subscription callbacks before the final delimiter is sent.
	publish(node("healthy", "in"), []byte("abc"))
	m := &nats.Msg{Subject: legacy("healthy", "in"), Data: []byte("abc"), Header: nats.Header{}}
	m.Header.Set(proto.PtyNodeHeader, "node")
	if err := nc.PublishMsg(m); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	barrier := make(chan struct{})
	if err := nc.Barrier(func() { close(barrier) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-barrier:
	case <-time.After(time.Second):
		t.Fatal("wildcard callback blocked")
	}
	publish(node("healthy", "in"), []byte("!"))
	read("abc!")

	// Overflow closes acceptance and publishes one truthful failed chunk.
	publish(legacy("blocked", "in"), bytes.Repeat([]byte{'y'}, upgradebudget.PTYInputBytes))
	msg, err := failed.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("overflow not reported: %v", err)
	}
	var chunk proto.RunChunk
	if err := json.Unmarshal(msg.Data, &chunk); err != nil {
		t.Fatal(err)
	}
	if chunk.Kind != "failed" || chunk.Reason != "pty_input_overflow" || chunk.PID != "blocked" {
		t.Fatalf("overflow misreported as startup failure: %+v", chunk)
	}
	publish(legacy("blocked", "in"), []byte("late"))
	_, queued, stopped := q.Snapshot()
	select {
	case <-q.Done():
	case <-time.After(time.Second):
		t.Fatal("overflow did not wake and retire the blocked input writer")
	}
	if !stopped || queued != 0 {
		t.Fatal("overflow left input admission open")
	}
	if _, err := failed.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("overflow was published more than once: %v", err)
	}
	// Zero-byte controls need their own finite count bound while the writer
	// is blocked; a byte-only budget would permit an unbounded resize flood.
	makeRun("controls", "exec sleep 60")
	publish(legacy("controls", "in"), bytes.Repeat([]byte{'x'}, 192<<10))
	// Input and resize use different NATS subscriptions: their callbacks may
	// run out of order. Prove that input is in flight before flooding controls.
	controls := legacyPTYInputFor(a, "controls")
	waitFor(t, time.Second, func() bool {
		charged, queued, _ := controls.Snapshot()
		return charged > 0 && queued == 0
	})
	for i := 0; i <= legacyPTYQueueEvents; i++ {
		publish(legacy("controls", "resize"), []byte(`{"cols":88,"rows":33}`))
	}
	msg, err = failed.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("control queue overflow not reported: %v", err)
	}
	if err := json.Unmarshal(msg.Data, &chunk); err != nil {
		t.Fatal(err)
	}
	if chunk.Kind != "failed" || chunk.Reason != "pty_input_overflow" || chunk.PID != "controls" {
		t.Fatalf("control queue overflow lost its running-task identity: %+v", chunk)
	}
}
