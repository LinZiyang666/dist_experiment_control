//go:build linux

// revoke_test.go — session revocation and the abandoned-terminal sweep, on
// real children and real replay files with an injected boot clock.
package execsession

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// clockedRuntime builds a runtime whose registry and sweep share one fake
// boot clock, starting real /bin/sh children through failureStartChild.
func clockedRuntime(t *testing.T, clock *atomic.Int64, capacity int64) *Runtime {
	t.Helper()
	manager := &workload.Manager{}
	r, err := New(Config{Instance: "clockedInstance", Manager: manager, ReplayCapacity: capacity,
		Clock: func() (int64, error) { return clock.Load(), nil },
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := failureStartChild(manager, pid, req)
			return stdio, "", err
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Errorf("runtime cleanup: %v", err)
		}
	})
	return r
}

func clockedScope(sid string) procrequest.Scope {
	digest := sha256.Sum256([]byte("clocked actor " + sid))
	return procrequest.Scope{SID: sid, NID: "clockedNode", ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "clockedClient"}
}

// clockedCreate probes and creates one task; the returned request is ready
// for resume (CallID advanced by the caller).
func clockedCreate(t *testing.T, r *Runtime, scope procrequest.Scope, requestID string, argv []string) (proto.ProcessResumeReq, *task) {
	t.Helper()
	probe, err := r.Handle(scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1, Op: "probe", ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}, "failure-inbox")
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe: %+v, %v", probe, err)
	}
	req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 2, Op: "create", ActorFP: scope.ActorFP,
		Handle: &proto.ProcessHandle{Lease: *probe.Lease, RequestID: requestID}, AttachEpoch: 1, Exec: &proto.ExecReq{Argv: argv}}
	reply, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || reply.PID == "" {
		t.Fatalf("create: %+v, %v", reply, err)
	}
	r.mu.Lock()
	owner := r.tasks[reply.PID]
	r.mu.Unlock()
	if owner == nil {
		t.Fatal("create retained no owner")
	}
	req.CallID++
	req.Op, req.Exec = "resume", nil
	return req, owner
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.5 — a session
// authority's revocation kills the live task, its terminal says so, nothing
// under a pre-revocation lease can be created, and a lease issued afterwards
// (a later session of the same name) is unaffected.
func TestRevokeKillsLiveTasksAndRefusesPreRevocationLeases(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 64<<10)
	scope := clockedScope("revokedSession")
	req, owner := clockedCreate(t, r, scope, "liveRequest", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, r, scope, req)
	child := r.cfg.Manager.Snapshot()
	if len(child) != 1 {
		t.Fatalf("expected one live child: %+v", child)
	}
	preLease := *req.Handle
	other := clockedScope("otherSession")
	otherReq, _ := clockedCreate(t, r, other, "otherRequest", []string{"/bin/sh", "-c", "exec sleep 30"})

	clock.Add(int64(time.Second))
	if killed, err := r.Revoke(scope.SID, "session_destroyed"); killed != 1 || err != nil {
		t.Fatalf("Revoke killed %d, err %v, want 1/nil", killed, err)
	}
	failureAwait(t, owner.done, "revoked child reap")
	if err := syscall.Kill(child[0].OSPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("revoked child still alive: %v", err)
	}
	// The result is collectable and names the revocation.
	got, fin, abort, terminal := failureCollect(t, r, scope, req)
	if terminal.Signal != 9 || !strings.HasPrefix(terminal.Error, "revoked: session_destroyed") || len(got[0]) != 0 || !fin[0] || abort[0] != "" {
		t.Fatalf("revoked terminal: %+v got=%v fin=%v abort=%v", terminal, got, fin, abort)
	}
	// Nothing can be created under the pre-revocation lease, including a
	// re-create of the same handle after the terminal was collected.
	req.CallID++
	req.Op, req.Exec = "create", &proto.ExecReq{Argv: []string{"/bin/true"}}
	req.StdoutFIN, req.StderrFIN = false, false
	if _, err := r.Handle(scope, req, "failure-inbox"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("re-create under a revoked lease: %v", err)
	}
	fresh := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 99, Op: "create", ActorFP: scope.ActorFP,
		Handle: &proto.ProcessHandle{Lease: preLease.Lease, RequestID: "afterRevoke"}, AttachEpoch: 1, Exec: &proto.ExecReq{Argv: []string{"/bin/true"}}}
	if _, err := r.Handle(scope, fresh, "failure-inbox"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("new request under a pre-revocation lease: %v", err)
	}
	// The other session is untouched, and a lease issued after the
	// revocation for the same session name creates normally.
	otherReq.CallID++
	if reply, err := r.Handle(other, otherReq, "failure-inbox"); err != nil || reply.Phase == "terminal" {
		t.Fatalf("other session affected by the revocation: %+v, %v", reply, err)
	}
	clock.Add(int64(time.Second))
	laterReq, later := clockedCreate(t, r, scope, "laterSession", []string{"/bin/true"})
	failureAwait(t, later.done, "post-revocation task")
	if _, _, _, term := failureCollect(t, r, scope, laterReq); term.ExitCode != 0 || strings.Contains(term.Error, "revoked") {
		t.Fatalf("post-revocation lease was treated as revoked: %+v", term)
	}
}

// failureAwaitRunning polls resume until the record reports running.
func failureAwaitRunning(t *testing.T, r *Runtime, scope procrequest.Scope, req proto.ProcessResumeReq) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		req.CallID++
		reply, err := r.Handle(scope, req, "failure-inbox")
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		if reply.Phase == "running" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task never reached running: %+v", reply)
		}
		time.Sleep(time.Millisecond)
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.6 — a terminal
// nobody collects keeps its replay budget only until its lease expired AND
// AbandonedTerminalGrace passed since the last touch; then the bytes go, a
// tombstone keeps the terminal queryable and still settleable by a client
// that acknowledged every byte, and after DiscardTombstoneTTL the record is
// collected so a late retry gets ErrExpired.
func TestSweepDiscardsAbandonedTerminalAndKeepsTombstone(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 64)
	r.budget.Limit = 3 * (64 << 10) // three tasks' worth: proves each discard returns a reservation
	scope := clockedScope("sweepSession")
	abandoned, owner := clockedCreate(t, r, scope, "abandoned", []string{"/bin/sh", "-c", "printf out; exit 23"})
	failureAwait(t, owner.done, "abandoned task exit")
	touched, touchedOwner := clockedCreate(t, r, scope, "touched", []string{"/bin/sh", "-c", "printf out; exit 23"})
	failureAwait(t, touchedOwner.done, "touched task exit")
	live, _ := clockedCreate(t, r, scope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, r, scope, live)

	grace := AbandonedTerminalGrace.Nanoseconds()
	// Neither an unexpired lease nor an expired lease inside the grace
	// discards anything.
	clock.Add(int64(10 * time.Second))
	if res, err := r.Sweep(); res != (SweepResult{}) || err != nil {
		t.Fatalf("swept before lease expiry: %+v err=%v", res, err)
	}
	clock.Store(abandoned.Handle.Lease.ExpiresBootNS + int64(time.Second))
	if res, err := r.Sweep(); res != (SweepResult{}) || err != nil {
		t.Fatalf("swept inside the grace: %+v err=%v", res, err)
	}
	// Past lease and grace: the touched task is touched now, so only the
	// abandoned one goes; the live task is never a candidate.
	clock.Store(abandoned.Handle.Lease.ExpiresBootNS + grace + int64(time.Second))
	touched.CallID++
	if _, err := r.Handle(scope, touched, "failure-inbox"); err != nil {
		t.Fatalf("touch: %v", err)
	}
	if res, err := r.Sweep(); res != (SweepResult{Discarded: 1}) || err != nil {
		t.Fatalf("sweep: %+v err=%v, want discarded=1 only", res, err)
	}
	r.mu.Lock()
	_, stillTouched := r.tasks[touchedOwner.pid]
	tasks := len(r.tasks)
	r.mu.Unlock()
	if !stillTouched || tasks != 2 {
		t.Fatalf("sweep discarded the wrong task: touched=%v tasks=%d", stillTouched, tasks)
	}
	// The abandoned task's terminal is still queryable, its bytes are gone.
	abandoned.CallID++
	reply, err := r.Handle(scope, abandoned, "failure-inbox")
	if !errors.Is(err, ErrOutputDiscarded) || reply.TerminalID == "" || reply.ExitCode != 23 {
		t.Fatalf("discarded task query: %+v, %v", reply, err)
	}
	// A client holding every byte (3 bytes of stdout, FIN; empty stderr,
	// FIN) settles exactly once; a client that does not is refused.
	abandoned.CallID++
	abandoned.Op, abandoned.TerminalID = "terminal-ack", reply.TerminalID
	abandoned.StdoutACK, abandoned.StdoutFIN, abandoned.StderrACK, abandoned.StderrFIN = 1, false, 0, true
	if _, err := r.Handle(scope, abandoned, "failure-inbox"); !errors.Is(err, ErrOutputDiscarded) {
		t.Fatalf("partial holder settled a discarded task: %v", err)
	}
	abandoned.CallID++
	abandoned.StdoutACK, abandoned.StdoutFIN = 3, true
	if settled, err := r.Handle(scope, abandoned, "failure-inbox"); err != nil || settled.Phase != "settled" {
		t.Fatalf("full holder could not settle: %+v, %v", settled, err)
	}
	// Discarding returned the replay reservation: with two tasks left, one
	// more full task's worth of budget must be free.
	probe, err := workload.NewReplay(&r.budget, 64<<10)
	if err != nil {
		t.Fatalf("discard did not release the replay budget: %v", err)
	}
	_ = probe.Close()
	// The touched task is abandoned in turn; its tombstone expires after the
	// TTL, and a late retry then sees ErrExpired from the registry.
	clock.Add(grace + int64(time.Second))
	if res, err := r.Sweep(); res.Discarded != 1 || err != nil {
		t.Fatalf("touched task not discarded after its own grace: %+v (%v)", res, err)
	}
	clock.Add(DiscardTombstoneTTL.Nanoseconds() + int64(time.Second))
	if res, err := r.Sweep(); res.Expired != 1 || err != nil {
		t.Fatalf("tombstone not expired: %+v (%v)", res, err)
	}
	touched.CallID++
	if _, err := r.Handle(scope, touched, "failure-inbox"); !errors.Is(err, procrequest.ErrExpired) {
		t.Fatalf("late retry after tombstone expiry: %v", err)
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.3 — an adapter
// whose Start returns a child it did not adopt violates the contract; the
// runtime records that as a setup failure instead of polling AllowReap
// forever, and the lease of a different runtime instance is refused with the
// one identity error a client may act on.
func TestUnadoptedStartAndForeignInstanceAreRefusedDeterministically(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	manager := &workload.Manager{}
	r, err := New(Config{Instance: "contractInstance", Manager: manager, ReplayCapacity: 64,
		Clock: func() (int64, error) { return clock.Load(), nil },
		Start: func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := workload.NewStdio()
			return stdio, "", err // pipes but no child, no adoption
		},
		Started: func(string, proto.ExecReq, workload.Identity) { t.Error("Started fired for an unadopted child") },
		Exited:  func(string, int) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Errorf("runtime cleanup: %v", err)
		}
	})
	scope := clockedScope("contractSession")
	req, owner := clockedCreate(t, r, scope, "unadopted", []string{"/bin/true"})
	failureAwait(t, owner.done, "unadopted start failure")
	req.CallID++
	reply, err := r.Handle(scope, req, "failure-inbox")
	if err != nil || reply.Phase != "terminal" || !strings.Contains(reply.Error, "not adopted") {
		t.Fatalf("unadopted start not recorded as a terminal: %+v, %v", reply, err)
	}
	// A lease minted by another runtime instance is refused as ErrInstance
	// before anything else is examined.
	foreignLease := req.Handle.Lease
	foreignLease.TargetInstance = "someOtherInstance"
	foreign := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 7, Op: "resume", ActorFP: scope.ActorFP, AttachEpoch: 1,
		Handle: &proto.ProcessHandle{Lease: foreignLease, RequestID: "x"}}
	if _, err := r.Handle(scope, foreign, "failure-inbox"); !errors.Is(err, procrequest.ErrInstance) {
		t.Fatalf("foreign instance lease: %v", err)
	}
}

// origin: hot-update S2 plan D11 / §22.4 item 8 — the running-abandoned
// bound. A live task nobody acknowledges holds its child on a blocked write
// once its log is full; after the lease expired AND AbandonedRunningGrace
// passed since the last touch, the stalled stream is truncated: the child
// writes again, the retained prefix stays readable, a returning client sees
// the stream end as aborted (Error, never FIN) and the task still reaches
// its real terminal. Negative arms: a touch inside the grace, or a stream
// that is not actually stalled, changes nothing. The truncation marker
// survives an image switch.
func TestSweepDiscardsAbandonedRunningOutput(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 64)
	scope := clockedScope("abandonedRunning")
	// The child writes 80 bytes (64 retained, 16 in the pipe), then waits on
	// a marker file before writing 4 KiB more and exiting 23: the second
	// write is what a truncated stream must let through.
	marker := t.TempDir() + "/go"
	stalled, owner := clockedCreate(t, r, scope, "stalled", []string{"/bin/sh", "-c",
		"head -c 80 /dev/zero; while [ ! -e " + marker + " ]; do sleep 0.01; done; head -c 4096 /dev/zero; exit 23"})
	failureAwaitRunning(t, r, scope, stalled)
	full := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return owner.output[0] != nil && owner.output[0].Snapshot().Replay.End == 64
	}
	deadline := time.Now().Add(5 * time.Second)
	for !full() {
		if time.Now().After(deadline) {
			t.Fatal("stdout log never filled")
		}
		time.Sleep(time.Millisecond)
	}
	quiet, quietOwner := clockedCreate(t, r, scope, "quiet", []string{"/bin/sh", "-c", "printf ten-bytes-; exec sleep 30"})
	failureAwaitRunning(t, r, scope, quiet)

	runningGrace := AbandonedRunningGrace.Nanoseconds()
	// Inside the lease: nothing.
	if res, err := r.Sweep(); res != (SweepResult{}) || err != nil {
		t.Fatalf("swept inside the lease: %+v %v", res, err)
	}
	// Lease expired, grace not passed since the last touch: nothing.
	clock.Store(stalled.Handle.Lease.ExpiresBootNS + runningGrace/2)
	if res, err := r.Sweep(); res != (SweepResult{}) || err != nil {
		t.Fatalf("swept inside the running grace: %+v %v", res, err)
	}
	// A touch renews the grace.
	clock.Add(runningGrace/2 + int64(time.Second))
	stalled.CallID = 300
	if _, err := r.Handle(scope, stalled, "failure-inbox"); err != nil {
		t.Fatal(err)
	}
	if res, err := r.Sweep(); res != (SweepResult{}) || err != nil {
		t.Fatalf("swept right after a touch: %+v %v", res, err)
	}
	// Grace passed: only the stalled stream goes; the quiet task's streams
	// (10 bytes in a 64-byte log) are not stalled and are left alone.
	clock.Add(runningGrace + int64(time.Second))
	res, err := r.Sweep()
	if err != nil || res != (SweepResult{Truncated: 1}) {
		t.Fatalf("sweep past the running grace: %+v %v, want exactly one truncated stream", res, err)
	}
	r.mu.Lock()
	quietState := quietOwner.output[0].Snapshot()
	stalledState := owner.output[0].Snapshot()
	r.mu.Unlock()
	if quietState.Discarding || quietState.ReadError != "" {
		t.Fatalf("a non-stalled stream was truncated: %+v", quietState)
	}
	if !stalledState.Discarding || !strings.HasPrefix(stalledState.ReadError, "abandoned:") || stalledState.Replay.FIN || stalledState.Replay.End != 64 {
		t.Fatalf("stalled stream after the sweep: %+v", stalledState)
	}
	// The child is no longer blocked: it sees the marker, writes 4 KiB and
	// exits 23 — bytes that are drained, not retained.
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	failureAwait(t, owner.done, "truncated task exit")
	got, fin, abort, terminal := failureCollect(t, r, scope, stalled)
	if len(got[0]) != 64 || fin[0] || !strings.HasPrefix(abort[0], "abandoned:") || !fin[1] || terminal.ExitCode != 23 {
		t.Fatalf("truncated task: bytes=%d fin=%v abort=%q terminal=%+v", len(got[0]), fin, abort, terminal)
	}
	// The truncation is settleable like any aborted stream (ack==End, no FIN).
	ack := stalled
	ack.CallID, ack.Op, ack.TerminalID = 400, "terminal-ack", terminal.TerminalID
	ack.StdoutACK, ack.StdoutFIN, ack.StderrACK, ack.StderrFIN = 64, false, uint64(len(got[1])), true
	if settled, err := r.Handle(scope, ack, "failure-inbox"); err != nil || settled.Phase != "settled" {
		t.Fatalf("settle truncated task: %+v %v", settled, err)
	}
	// Frozen in the middle of a discard, the marker rides the image and the
	// restored pump keeps draining.
	r.mu.Lock()
	quietOwner.output[0].Discard("abandoned: fixture")
	r.mu.Unlock()
	if err := r.Freeze(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	img, _, err := r.Export()
	if err != nil {
		t.Fatal(err)
	}
	var exported *StreamImage
	for _, ti := range img.Tasks {
		if ti.PID == quietOwner.pid {
			exported = ti.Stdout
		}
	}
	if exported == nil || exported.Output == nil || !exported.Output.Discarding || exported.Output.ReadError != "abandoned: fixture" {
		t.Fatalf("discard marker not exported: %+v", exported)
	}
	if err := r.Thaw(); err != nil {
		t.Fatal(err)
	}
	_ = r.cfg.Manager.Signal(quietOwner.pid, syscall.SIGKILL)
	failureAwait(t, quietOwner.done, "quiet task exit")
}
