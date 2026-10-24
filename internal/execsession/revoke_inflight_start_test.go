//go:build linux

// revoke_inflight_start_test.go — Revoke against a task whose startup owner is
// still inside the adapter's Start, and against a task whose output pump is
// stalled on replay backpressure. Both are shapes the session-authority
// contract (architecture §22.4 item 7: "SIGKILL every task's owned process
// group … terminal … tombstone") must hold for, and neither is covered by
// revoke_test.go, which revokes only tasks already observed running.
package execsession

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S1 internal review (protocol lane) — Revoke marks the
// task and SIGKILLs via Manager while the startup owner may still be inside
// cfg.Start (the child not yet adopted, so the Signal is ErrUnknown and
// nothing dies). When Start then returns and adopts the child, nothing in
// start() re-checks t.revoked, so the child of a revoked session runs to its
// own completion under a terminal that claims "revoked:".
func TestRevokeReachesChildAdoptedAfterRevocation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	entered, release := make(chan struct{}), make(chan struct{})
	var clock atomic.Int64
	clock.Store(1_000_000_000)
	manager := &workload.Manager{}
	r, err := New(Config{Instance: "inflightInstance", Manager: manager, ReplayCapacity: 64 << 10,
		Clock: func() (int64, error) { return clock.Load(), nil },
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			close(entered)
			<-release // Revoke runs while the startup owner is in here
			stdio, err := failureStartChild(manager, pid, req)
			return stdio, "", err
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.SignalAll(syscall.SIGKILL) })

	scope := clockedScope("inflightSession")
	probe, err := r.Handle(scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1, Op: "probe", ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}, "failure-inbox")
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe: %+v, %v", probe, err)
	}
	req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 2, Op: "create", ActorFP: scope.ActorFP,
		Handle: &proto.ProcessHandle{Lease: *probe.Lease, RequestID: "inflight"}, AttachEpoch: 1,
		Exec: &proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}}}
	reply, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || reply.PID == "" {
		t.Fatalf("create: %+v, %v", reply, err)
	}
	failureAwait(t, entered, "startup owner entering Start")

	clock.Add(int64(time.Second))
	if killed, err := r.Revoke(scope.SID, "session_destroyed"); killed != 1 || err != nil {
		t.Fatalf("Revoke reported %d killed (%v), want 1 (the task is in the table)", killed, err)
	}
	close(release) // the child is spawned and adopted AFTER the revocation

	deadline := time.Now().Add(3 * time.Second)
	var child []workload.Record
	for {
		child = manager.Snapshot()
		if len(child) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child never adopted: %+v", child)
		}
		time.Sleep(time.Millisecond)
	}
	// The contract: a revoked session's child does not run. Give the runtime
	// a generous window to deliver the kill it owes.
	deadline = time.Now().Add(2 * time.Second)
	for {
		if err := syscall.Kill(child[0].OSPID, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d of revoked session %s is still alive 2s after Revoke returned (adopted after the revocation, never killed)", child[0].OSPID, scope.SID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// And its terminal must name the revocation with a signal death.
	r.mu.Lock()
	owner := r.tasks[reply.PID]
	r.mu.Unlock()
	failureAwait(t, owner.done, "revoked child reap")
	req.CallID++
	req.Op, req.Exec = "resume", nil
	_, _, _, terminal := failureCollect(t, r, scope, req)
	if terminal.Signal != 9 || !strings.HasPrefix(terminal.Error, "revoked: session_destroyed") {
		t.Fatalf("terminal of a revoked task: %+v", terminal)
	}
}

// origin: hot-update S1 internal review (protocol lane) — a task whose pump
// is stalled on replay backpressure (log full, nobody ACKing) is exactly the
// task a destroyed session leaves behind. Revoke SIGKILLs it, but the pump
// never reaches EOF (it is waiting for an ACK no client can ever send after
// session_destroyed: the broker refuses every op for the SID and Handle
// refuses with ErrRevoked before acknowledge), so observe never finishes:
// no terminal, no Exited event, the child stays an unreaped zombie under
// ReapHeld and the replay budget stays reserved until Close.
func TestRevokeOfBackpressuredTaskStillReachesTerminal(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 64) // 64-byte stdout replay: the child fills it at once
	scope := clockedScope("backpressureSession")
	marker := filepath.Join(t.TempDir(), "filled")
	req, owner := clockedCreate(t, r, scope, "backpressured", []string{"/bin/sh", "-c",
		"printf 0123456789012345678901234567890123456789012345678901234567890123456789; : > " + marker + "; exec sleep 30"})
	failureAwaitRunning(t, r, scope, req)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child never wrote its output")
		}
		time.Sleep(time.Millisecond)
	}
	// Let the pump reach the full log (64 of 80 bytes retained, 16 pending in the pipe).
	deadline = time.Now().Add(5 * time.Second)
	for {
		if st := owner.output[0].Snapshot(); st.Replay.End-st.Replay.Base == 64 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pump never filled the log: %+v", owner.output[0].Snapshot())
		}
		time.Sleep(time.Millisecond)
	}

	clock.Add(int64(time.Second))
	if killed, err := r.Revoke(scope.SID, "session_destroyed"); killed != 1 || err != nil {
		t.Fatalf("Revoke killed %d (%v), want 1", killed, err)
	}
	// After session_destroyed nobody can ACK: every op is ErrRevoked.
	req.CallID++
	req.StdoutACK = 64
	if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("ack after revoke: %v", err)
	}
	// The contract still owes a terminal (and the broker its exit event).
	select {
	case <-owner.done:
	case <-time.After(3 * time.Second):
		rows := r.cfg.Manager.Snapshot()
		t.Fatalf("revoked backpressured task never reached terminal: owner still running 3s after SIGKILL; process table=%+v (Exit==nil means the killed child is an unreaped zombie held by ReapHeld)", rows)
	}
}
