//go:build linux

// Real runtime admission, registry and freeze barriers; no transport or agent.
// The held lock/reference is the adversarial scheduling fixture, not an actor.
package execsession

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 plan §4.1 / T50
func TestPTYRuntimeAdmissionLockDeadline(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePTYAdmissionDeadline(t)
	}
	testharness.AssertNoGoroutineLeak(t, "PTY runtime admission", beforeG)
	testharness.AssertNoFDLeak(t, "PTY runtime admission", beforeFD, 0)
	t.Run("real-actor-control", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		exercisePTYRuntimeControlDeadline(t)
	})
}

func exercisePTYAdmissionDeadline(t *testing.T) {
	t.Helper()
	f := newRuntimeFixture(t, 64)
	r := f.r
	r.mu.Lock()
	result := make(chan error, 1)
	go func() { result <- r.Freeze(time.Now().Add(30 * time.Millisecond)) }()
	// This timer releases a deliberately held entry mutex even under the
	// naked-Lock mutation, so a red test does not strand the test process.
	select {
	case err := <-result:
		r.mu.Unlock()
		if !errors.Is(err, procrequest.ErrBusy) {
			t.Fatalf("held admission lock: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		r.mu.Unlock()
		<-result
		t.Fatal("Freeze waited for the entry mutex past its deadline")
	}
	// A pinned Handle/control call must consume the SAME budget, with mu
	// available to a session-destroyed event throughout that wait.
	r.mu.Lock()
	r.ioActive++
	r.mu.Unlock()
	go func() { result <- r.Freeze(time.Now().Add(100 * time.Millisecond)) }()
	deadline := time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		entered := r.freezing
		r.mu.Unlock()
		if entered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("freeze never closed intake")
		}
		time.Sleep(time.Millisecond)
	}
	revoked := make(chan error, 1)
	go func() { _, err := r.Revoke("deadlineSession", "session_destroyed"); revoked <- err }()
	select {
	case err := <-revoked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("Runtime.mu was held while waiting for a control reference")
	}
	if err := <-result; !errors.Is(err, procrequest.ErrBusy) {
		t.Fatalf("pinned control freeze: %v", err)
	}
	r.mu.Lock()
	_, marked := r.revocations["deadlineSession"]
	r.ioActive--
	r.mu.Unlock()
	if !marked || r.Frozen() {
		t.Fatal("failed freeze lost revocation or published a false barrier")
	}
	if err := r.Freeze(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.Thaw(); err != nil {
		t.Fatal(err)
	}
	f.close(t)
}

// Resize pauses at the real master call, so Runtime.Handle holds an actual
// actor control reference while Freeze and a revocation race it.
type ptyResizeBarrier struct {
	pty.Master
	entered, release chan struct{}
}

func (m *ptyResizeBarrier) Resize(cols, rows uint16) error {
	close(m.entered)
	<-m.release
	return m.Master.Resize(cols, rows)
}

func exercisePTYRuntimeControlDeadline(t *testing.T) {
	t.Helper()
	f := newPTYRuntime(t, 0)
	q, owner := ptyCreate(t, f, "control-deadline", []string{"/bin/sh", "-c", "stty raw -echo; printf ready; read line"})
	ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
		return p.Stdout != nil && strings.Contains(string(p.Stdout.Data), "ready")
	})
	imageFreeze(t, f.r)
	st := *owner.frozen.pty
	files, err := owner.pty.Detach()
	requirePTYDependency(t, err)
	row, _ := f.manager.Lookup(owner.pid)
	master, err := pty.RestoreMaster(files[filePTYMaster], st.Identity, row)
	requirePTYDependency(t, err)
	barrier := &ptyResizeBarrier{Master: master, entered: make(chan struct{}), release: make(chan struct{})}
	input, err := workload.RestoreReplay(&f.r.budget, st.Input, files[filePTYInput])
	if err != nil {
		t.Fatal(err)
	}
	output, err := workload.RestoreReplay(&f.r.budget, st.Output, files[filePTYOutput])
	if err != nil {
		t.Fatal(err)
	}
	actor, err := ptysession.Import(ptysession.Config{Manager: &f.manager, PID: owner.pid, Master: barrier, Identity: st.Identity, Input: input, Output: output, Clock: f.cfg.Clock}, st)
	requirePTYDependency(t, err)
	f.r.mu.Lock()
	owner.pty, owner.logs[0], owner.logs[1] = actor, input, output
	f.r.mu.Unlock()
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	control := q
	control.Op = "resize"
	control.ControlSeq = 1
	control.Resize = &proto.PtyResizeEvent{Cols: 97, Rows: 37}
	completed := make(chan error, 1)
	go func() { _, err := f.r.Handle(f.scope, control, "pty-inbox"); completed <- err }()
	failureAwait(t, barrier.entered, "real actor resize transaction")
	t.Cleanup(func() {
		select {
		case <-barrier.release:
		default:
			close(barrier.release)
		}
	})
	freezeResult := make(chan error, 1)
	go func() { freezeResult <- f.r.Freeze(time.Now().Add(40 * time.Millisecond)) }()
	revoked := make(chan error, 1)
	go func() { _, err := f.r.Revoke(f.scope.SID, "session_destroyed"); revoked <- err }()
	select {
	case err := <-freezeResult:
		if !errors.Is(err, procrequest.ErrBusy) {
			t.Fatalf("blocked actor control cut: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		close(barrier.release)
		<-completed
		<-revoked
		<-freezeResult
		t.Fatal("real actor control exceeded Runtime.Freeze deadline")
	}
	if !lockBefore(&f.r.mu, time.Now().Add(300*time.Millisecond)) {
		close(barrier.release)
		<-completed
		<-revoked
		t.Fatal("control waited under the runtime admission lock")
	}
	mark, registered := f.r.revocations[f.scope.SID]
	f.r.mu.Unlock()
	if !registered || mark.reason != "session_destroyed" {
		t.Fatal("control wait blocked revocation registration")
	}
	close(barrier.release)
	if err := <-completed; err != nil && !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool { return p.Phase == "terminal" })
	f.close(t)
}
