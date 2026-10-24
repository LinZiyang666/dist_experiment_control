//go:build linux

// process_admission_test.go — the S1 admission barrier around a spawned exec
// child (started before adoption, the StartedUnacked snapshot flag), the
// session_destroyed revocation of managed workloads, and the refusal-code
// mapping the reliable runtime answers with. Real children and real NATS.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.1/§3.2 (C-5) —
// the courier owns the proc.started BEFORE the child is visible in the
// process table, so a register snapshot taken at any instant after the spawn
// either carries the pending started (row flagged StartedUnacked) or sees a
// table row whose started the broker already acknowledged. A spawn that
// never happened leaves neither.
func TestExecStartedEnqueuedBeforeAdoptAndSnapshotFlagsIt(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	url := startNATS(t)
	a := courierAgent(t, url)
	// The order itself, observed at the seam between the two calls (S1
	// review E3): when the hook fires, the started must already be pending
	// and the child must NOT be in the process table yet. Swapping
	// enqueueStarted and AdoptDraining fails here, not at the end-state
	// checks below (which both orders satisfy).
	type seamObservation struct {
		pending bool
		adopted bool
	}
	seam := make(chan seamObservation, 1)
	hook := func(a *Agent, pid string) {
		if pid != "admission-exec" {
			return
		}
		_, adopted := a.workloads.Lookup(pid)
		seam <- seamObservation{pending: receiptPending(t, a.courier, pid, procEventStarted) != nil, adopted: adopted}
	}
	afterEnqueueStartedHook.Store(&hook)
	t.Cleanup(func() { afterEnqueueStartedHook.Store(nil) })
	// No responder: the started stays pending, which is the window C-5 closes.
	stdio, warning, err := startExecPipes(a, "admission-exec", &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "exec sleep 30"}})
	if err != nil || warning != "" {
		t.Fatalf("startExecPipes: %v %q", err, warning)
	}
	select {
	case obs := <-seam:
		if !obs.pending || obs.adopted {
			t.Fatalf("at the seam between enqueueStarted and AdoptDraining: started pending=%v, child adopted=%v — want pending and not yet adopted (C-5 order)", obs.pending, obs.adopted)
		}
	default:
		t.Fatal("the seam between enqueueStarted and AdoptDraining never fired")
	}
	t.Cleanup(func() {
		stdio.Close()
		a.reapManagedChildren()
		_ = a.workloads.AllowReap("admission-exec")
		_, _ = a.workloads.Wait("admission-exec")
		_ = a.workloads.Forget("admission-exec")
	})
	pending := receiptPending(t, a.courier, "admission-exec", procEventStarted)
	if pending == nil || pending.startTimeTicks == 0 || pending.startedAt.IsZero() {
		t.Fatalf("started not enqueued with the child's identity: %+v", pending)
	}
	row, ok := a.workloads.Lookup("admission-exec")
	if !ok || row.StartTimeTicks != pending.startTimeTicks || row.OSPID <= 0 {
		t.Fatalf("table row does not share the started identity: %+v vs %+v", row, pending)
	}
	var snap proto.LocalProcess
	for _, p := range localProcessSnapshot(a) {
		if p.PID == "admission-exec" {
			snap = p
		}
	}
	if snap.PID == "" || snap.State != "running" || !snap.StartedUnacked || snap.StartTimeTicks != row.StartTimeTicks {
		t.Fatalf("snapshot must flag the unacked started: %+v", snap)
	}
	// A failed spawn enqueues nothing: no started, no table row, no snapshot row.
	if _, _, err := startExecPipes(a, "never-spawned", &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/nonexistent/binary"}}); err == nil {
		t.Fatal("spawn of a missing binary succeeded")
	}
	if receiptPending(t, a.courier, "never-spawned", procEventStarted) != nil {
		t.Fatal("a failed spawn enqueued a started")
	}
	for _, p := range localProcessSnapshot(a) {
		if p.PID == "never-spawned" {
			t.Fatalf("a failed spawn appears in the snapshot: %+v", p)
		}
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.5 (C-10, exec
// half) — session_destroyed kills the session's reliable task AND the legacy
// exec child with their descendants, records a revoked terminal, refuses a
// late create under the pre-revocation lease, and leaves the daemon running.
// origin: S8 internal review FIX-AGENTRACE-A F1
func TestSessionDestroyedRevokesReliableAndLegacyChildren(t *testing.T) {
	// Another fixture's supervisor can make the shared test process a
	// subreaper. Give this tree its own supervisor so killed grandchildren
	// are joined by their wait owner before the unchanged ESRCH census.
	if runIsolatedAgentTest(t, "TestSessionDestroyedRevokesReliableAndLegacyChildren") {
		return
	}
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	url := startNATS(t)
	a := courierAgent(t, url)
	nc := a.ncBox.Load()
	if a.processRuntime == nil {
		t.Fatal("agent has no reliable runtime")
	}
	marker := filepath.Join(t.TempDir(), "grandchild")
	// Reliable task: a shell whose grandchild writes a marker when it dies
	// on SIGKILL of the group... a grandchild that survives would keep the
	// marker absent, so the marker is "the subtree died" evidence via the
	// exit of the leader; the grandchild's death is checked by pid.
	scope := procrequest.Scope{SID: a.cfg.SID, NID: nidOf(a), ActorFP: "SHA256:" + strings.Repeat("A", 43), ClientLineage: "revokeClient"}
	probe, err := a.processRuntime.Handle(scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1, Op: "probe", ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}, "_TINBOX.revoke")
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe: %+v %v", probe, err)
	}
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	create := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 2, Op: "create", ActorFP: scope.ActorFP, AttachEpoch: 1,
		Handle: &proto.ProcessHandle{Lease: *probe.Lease, RequestID: "reliableTask"},
		Exec:   &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "sleep 30 & echo $! > " + pidFile + "; wait"}}}
	reply, err := a.processRuntime.Handle(scope, create, "_TINBOX.revoke")
	if err != nil || reply.PID == "" {
		t.Fatalf("create: %+v %v", reply, err)
	}
	// Legacy exec child with its own grandchild.
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	legacyDone := make(chan struct{})
	go func() {
		defer close(legacyDone)
		_, _ = a.runChild(nc, inbox, "legacy-exec", &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "sleep 30 & echo $! > " + marker + "; wait"}})
	}()
	waitFile := func(path string) int {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if body, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(body)) != "" {
				if pid, err := strconv.Atoi(strings.TrimSpace(string(body))); err == nil && pid > 0 {
					return pid
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("grandchild pid never appeared in %s", path)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	reliableGrandchild := waitFile(pidFile)
	legacyGrandchild := waitFile(marker)
	t.Cleanup(func() {
		_ = syscall.Kill(reliableGrandchild, syscall.SIGKILL)
		_ = syscall.Kill(legacyGrandchild, syscall.SIGKILL)
		a.reapManagedChildren()
		<-legacyDone
	})
	// The event arrives on a real NATS subscription, but this test's OWN
	// handler routes it — the agent's session() is not running here. What is
	// under test is revokeSessionWorkloads (kill + tombstone + legacy reap);
	// the dispatch (handleSysEvent's session_destroyed branch) is pinned by
	// TestHandleSysEventRoutesSessionDestroyedToRevocationAndKeepsTheRun and
	// end to end by TestExecReliableSessionRmMidStreamKillsChild (S1 review E7).
	evictDone := make(chan struct{})
	subEv, err := nc.Subscribe(proto.SubjSysEvents, func(msg *nats.Msg) {
		var ev struct {
			Type string `json:"type"`
			SID  string `json:"sid"`
		}
		_ = json.Unmarshal(msg.Data, &ev)
		if ev.Type == "session_destroyed" && ev.SID == a.cfg.SID {
			revokeSessionWorkloads(a, "session_destroyed")
			close(evictDone)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subEv.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]string{"type": "session_destroyed", "sid": a.cfg.SID})
	if err := nc.Publish(proto.SubjSysEvents, body); err != nil {
		t.Fatal(err)
	}
	select {
	case <-evictDone:
	case <-time.After(5 * time.Second):
		t.Fatal("session_destroyed never reached the handler")
	}
	dead := func(pid int) bool {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
				return true
			}
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !dead(reliableGrandchild) || !dead(legacyGrandchild) {
		// Kernel state distinguishes a missed kill from an unjoined adopted
		// zombie without relaxing the final ESRCH census.
		for _, pid := range []int{reliableGrandchild, legacyGrandchild} {
			stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
			t.Logf("descendant %d kernel state: %s (%v)", pid, stat, err)
		}
		t.Fatalf("descendants survived the revocation: reliable=%d legacy=%d", reliableGrandchild, legacyGrandchild)
	}
	select {
	case <-legacyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy exec handler did not finish after the revocation")
	}
	// The reliable task's terminal names the revocation; a late create under
	// the pre-revocation lease is refused; the daemon's run context is intact.
	resume := create
	resume.Op, resume.Exec, resume.CallID = "resume", nil, 3
	deadline := time.Now().Add(5 * time.Second)
	for {
		reply, err = a.processRuntime.Handle(scope, resume, "_TINBOX.revoke")
		if err == nil && reply.Phase == "terminal" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("revoked task never reached terminal: %+v %v", reply, err)
		}
		resume.CallID++
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.HasPrefix(reply.Error, "revoked: session_destroyed") || reply.Signal != 9 {
		t.Fatalf("terminal does not record the revocation: %+v", reply)
	}
	late := create
	late.CallID, late.Handle = 50, &proto.ProcessHandle{Lease: *probe.Lease, RequestID: "afterRevocation"}
	if _, err := a.processRuntime.Handle(scope, late, "_TINBOX.revoke"); !errors.Is(err, execsession.ErrRevoked) {
		t.Fatalf("pre-revocation lease created a task after session_destroyed: %v", err)
	}
	if rc := a.loadRunCtx(); rc != nil && rc.Err() != nil {
		t.Fatal("session_destroyed cancelled the daemon's run context")
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §3.4 — every runtime
// refusal the client can act on maps to its stable code; unknown errors carry
// none so the client falls back to the prose prefix.
func TestProcessRefusalCodeTable(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{procrequest.ErrInstance, proto.CodeProcessInstanceLost},
		{procrequest.ErrCapacity, proto.CodeProcessCapacity},
		{procrequest.ErrExpired, proto.CodeProcessExpired},
		{procrequest.ErrConflict, proto.CodeProcessConflict},
		{workload.ErrReplayOffset, proto.CodeProcessConflict},
		{procrequest.ErrStale, proto.CodeProcessStale},
		{procrequest.ErrBusy, proto.CodeProcessBusy},
		{procrequest.ErrFrozen, proto.CodeProcessBusy},
		{execsession.ErrRevoked, proto.CodeProcessRevoked},
		{execsession.ErrOutputDiscarded, proto.CodeProcessOutputDiscarded},
		{execsession.ErrClosed, proto.CodeProcessRuntimeClosed},
		{procrequest.ErrIdentity, proto.CodeProcessIdentity},
		{procrequest.ErrUnknown, proto.CodeProcessIdentity},
		{execsession.ErrTerminalUnrecorded, proto.CodeProcessIdentity},
		{errors.New("exec session: unsupported operation"), ""},
		{nil, ""},
	} {
		if got := processRefusalCode(tc.err); got != tc.want {
			t.Errorf("processRefusalCode(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
	// The forwarded handler stamps the code onto the wire reply.
	url := startNATS(t)
	a := courierAgent(t, url)
	nc := a.ncBox.Load()
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	foreign := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 9, Op: "resume", AttachEpoch: 1,
		ActorFP: "SHA256:" + strings.Repeat("B", 43), ClientLineage: "x",
		Handle: &proto.ProcessHandle{Lease: proto.ProcessLease{Version: 1, TargetInstance: "not-this-runtime", BootID: "b", SID: a.cfg.SID, NID: nidOf(a),
			ActorFP: "SHA256:" + strings.Repeat("B", 43), ClientLineage: "x", LeaseID: strings.Repeat("0", 32), ExpiresBootNS: 1}, RequestID: "r"}}
	body, _ := json.Marshal(foreign)
	handleProcessResumeForwarded(a, nc, &nats.Msg{Subject: proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), proto.ProcessResumeVerb), Reply: inbox, Data: body})
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var reply proto.ProcessResumeReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil || reply.Kind != "error" || reply.Code != proto.CodeProcessInstanceLost || reply.CallID != 9 {
		t.Fatalf("foreign-instance refusal on the wire: %+v err=%v", reply, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopProcessRuntime(ctx, a)
}

// origin: hot-update S1 internal review protocol-V1 / e2e-V1 — the C-5 flag
// on the WIRE. A real agent whose started is still pending (nobody answers
// proc.started) registers through its real register path; the register.req
// the broker receives must carry `local_processes[pid].started_unacked=true`
// for the running child, and a register reply that does NOT accept the pid
// must leave the started pending (the courier keeps owing it). The broker
// half — a fresh broker neither drops nor accepts such a pid, and accepts it
// once its row exists — is TestReconcileNeverAcceptsStartedUnackedPidWithoutARow,
// which reads the same wire key (frozen by TestForwardPayloadKeysAreWireFrozen).
func TestRegisterRequestCarriesStartedUnackedWhileStartedIsPending(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	url := startNATS(t)
	a := courierAgent(t, url)
	a.cfg.RegisterTimeout = 2 * time.Second
	a.cfg.RegisterRetryInitial = 50 * time.Millisecond
	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	captured := make(chan proto.NodeRegisterReq, 4)
	if _, err := stub.Subscribe(proto.SubjNodeRegister("lab", "n1"), func(m *nats.Msg) {
		var req proto.NodeRegisterReq
		if err := json.Unmarshal(m.Data, &req); err != nil {
			return
		}
		if !strings.Contains(string(m.Data), `"started_unacked":true`) {
			// The key spelling is the contract with the broker's decoder.
			req.LocalProcesses = nil
		}
		captured <- req
		// A broker that never saw the started: no accepted, no drop.
		b, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = m.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()
	// No proc.started responder anywhere: the started stays pending.
	stdio, _, err := startExecPipes(a, "wire-exec", &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "exec sleep 30"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdio.Close()
		a.reapManagedChildren()
		_ = a.workloads.AllowReap("wire-exec")
		_, _ = a.workloads.Wait("wire-exec")
		_ = a.workloads.Forget("wire-exec")
	})
	if receiptPending(t, a.courier, "wire-exec", procEventStarted) == nil {
		t.Fatal("precondition: the started is not pending")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.register(ctx, a.ncBox.Load()); err != nil {
		t.Fatalf("register: %v", err)
	}
	var req proto.NodeRegisterReq
	select {
	case req = <-captured:
	case <-time.After(3 * time.Second):
		t.Fatal("the broker stub never received the register.req")
	}
	var row *proto.LocalProcess
	for i := range req.LocalProcesses {
		if req.LocalProcesses[i].PID == "wire-exec" {
			row = &req.LocalProcesses[i]
		}
	}
	if row == nil {
		t.Fatalf("register.req did not carry the running child with the wire key started_unacked: %+v", req.LocalProcesses)
	}
	if row.State != "running" || !row.StartedUnacked || row.StartTimeTicks == 0 {
		t.Fatalf("register.req row for the child: %+v (want running, started_unacked, real identity)", *row)
	}
	// A register that did not accept the pid leaves the started pending: the
	// courier still owes it, the broker will grow the row from the retry.
	if receiptPending(t, a.courier, "wire-exec", procEventStarted) == nil {
		t.Fatal("the register reply (no accepted pid) made the courier drop the pending started")
	}
	if _, adopted := a.workloads.Lookup("wire-exec"); !adopted {
		t.Fatal("the child was dropped from the process table by the register")
	}
}
