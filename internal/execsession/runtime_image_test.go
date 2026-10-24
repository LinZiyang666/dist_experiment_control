//go:build linux

// runtime_image_test.go — the image fence and the in-process image round
// trip: Freeze/Export/Import/Thaw/Discard/Detach over real children, real
// pipes and real memfd replays, WITHOUT the exec. The same-PID exec proof
// lives in runtime_image_exec_test.go; everything about the ABI that does
// not need a new process image is proved here so the exec test can stay a
// single, expensive, literal receipt.
// Real: exec.Cmd children, pipes, memfd replays, descriptor duplication.
// Stubbed: handoff's sealed snapshot (a dup-backed FileSource stands in).
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §5.1.
package execsession

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

// imageDupSource stands in for handoff.Restored inside one process: every
// exported original is duplicated (F_DUPFD_CLOEXEC, exactly what Prepare
// does) and handed out once by TakeFile. Close releases what was not taken.
type imageDupSource struct {
	files map[string]map[string]*os.File
}

func newImageDupSource(t *testing.T, resources []handoff.Resource) *imageDupSource {
	t.Helper()
	src := &imageDupSource{files: make(map[string]map[string]*os.File, len(resources))}
	for _, res := range resources {
		files := make(map[string]*os.File, len(res.Files))
		for name, file := range res.Files {
			files[name] = imageDup(t, file)
		}
		src.files[res.ID] = files
	}
	t.Cleanup(src.Close)
	return src
}

func (s *imageDupSource) TakeFile(id, name string) (*os.File, error) {
	f := s.files[id][name]
	if f == nil {
		return nil, fmt.Errorf("fake bundle: %s/%s unavailable", id, name)
	}
	delete(s.files[id], name)
	return f, nil
}

func (s *imageDupSource) Close() {
	for _, files := range s.files {
		for name, f := range files {
			_ = f.Close()
			delete(files, name)
		}
	}
}

func imageDup(t *testing.T, file *os.File) *os.File {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	dup, dupErr := -1, error(nil)
	if err := raw.Control(func(fd uintptr) { dup, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || dupErr != nil {
		t.Fatalf("dup: %v %v", err, dupErr)
	}
	return os.NewFile(uintptr(dup), "image-dup")
}

func imageFileOpen(t *testing.T, name string, f *os.File) {
	t.Helper()
	if _, err := f.Stat(); err != nil {
		t.Fatalf("%s: closed by the importer: %v", name, err)
	}
	raw, err := f.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	flags, flagErr := 0, error(nil)
	if err := raw.Control(func(fd uintptr) { flags, flagErr = unix.FcntlInt(fd, unix.F_GETFD, 0) }); err != nil || flagErr != nil {
		t.Fatal(err, flagErr)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("%s: lost CLOEXEC", name)
	}
}

// imageChildAlive proves the kernel still holds the child for us.
func imageChildAlive(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("child %d gone: %v", pid, err)
	}
}

func imageProcState(t *testing.T, pid int) string {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(data)[strings.LastIndexByte(string(data), ')')+1:])
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func imageFreeze(t *testing.T, r *Runtime) {
	t.Helper()
	if err := r.Freeze(time.Now().Add(runtimeTestBudget)); err != nil {
		t.Fatalf("freeze: %v", err)
	}
}

// imageCut freezes runtime and its (shared) manager and exports both halves.
func imageCut(t *testing.T, f *runtimeFixture) (Image, []handoff.Resource, []workload.Record) {
	t.Helper()
	imageFreeze(t, f.r)
	rows, err := f.manager.Freeze()
	if err != nil {
		t.Fatalf("manager freeze: %v", err)
	}
	img, resources, err := f.r.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	return img, resources, rows
}

// imageSwitch is the in-process switch: the imported runtime takes over the
// fixture (sharing its frozen Manager), the old one is discarded, and both
// halves are thawed in the mandated order (Manager first).
func imageSwitch(t *testing.T, f *runtimeFixture, cfg Config, img Image, resources []handoff.Resource, rows []workload.Record) *Runtime {
	t.Helper()
	src := newImageDupSource(t, resources)
	imported, salvage, err := Import(cfg, img, rows, src)
	if err != nil {
		t.Fatalf("import: %v (salvage %d)", err, len(salvage))
	}
	if err := f.r.Discard(); err != nil {
		t.Fatalf("discard old runtime: %v", err)
	}
	f.r = imported
	f.manager.Thaw()
	if err := imported.Thaw(); err != nil {
		t.Fatalf("thaw imported runtime: %v", err)
	}
	return imported
}

func imageControlPipe(t *testing.T, f *runtimeFixture) *os.File {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	f.control = reader
	return writer
}

func imageExitedCount(f *runtimeFixture, pid string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.exited[pid])
}

// origin: hot-update S2 plan exit #3 / §5.1 — the only "no spawn in flight"
// evidence is the registry's own starting record. While a creation is in
// flight Freeze must return procrequest.ErrBusy at the deadline with nothing
// frozen (attach ops still served, no pump paused), further creates must
// wait rather than race the fence, and the in-flight child must survive:
// the mutation that drops the starting check lets Freeze succeed, and the
// startup owner's registry.Running then meets ErrFrozen and SIGKILLs a
// perfectly good child.
func TestRuntimeImageFreezeRefusesWhileStarting(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	manager := &workload.Manager{}
	var spawned atomic.Int32
	var warnMu sync.Mutex
	var warns []string
	r, err := New(Config{Instance: "startingInstance", Manager: manager, ReplayCapacity: 64 << 10,
		Warn: func(msg string, err error) {
			warnMu.Lock()
			warns = append(warns, msg+": "+err.Error())
			warnMu.Unlock()
		},
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-release // Freeze runs while this startup owner is inside Start
			}
			stdio, err := failureStartChild(manager, pid, req)
			if err == nil {
				spawned.Add(1)
			}
			return stdio, "", err
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), runtimeTestBudget)
		defer cancel()
		_ = r.Close(ctx)
	})
	scope := clockedScope("startingSession")
	inflight, _ := clockedCreate(t, r, scope, "inflight", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwait(t, entered, "startup owner entering Start")

	began := time.Now()
	err = r.Freeze(began.Add(200 * time.Millisecond))
	waited := time.Since(began)
	if !errors.Is(err, procrequest.ErrBusy) || waited < 200*time.Millisecond || waited > 2*time.Second {
		t.Fatalf("Freeze with a creation in flight: err=%v after %v, want ErrBusy at the deadline", err, waited)
	}
	if r.Frozen() {
		t.Fatal("a refused Freeze left the fence up")
	}
	// The refusal names the in-flight pid for the operator (a creation whose
	// terminal was refused would keep the node deferred until restart).
	warnMu.Lock()
	warned := strings.Join(warns, "\n")
	warnMu.Unlock()
	if !strings.Contains(err.Error(), inflight.Handle.RequestID) && !strings.Contains(err.Error(), "1 starting record") {
		t.Fatalf("ErrBusy does not name the starting record: %v", err)
	}
	if !strings.Contains(warned, "starting record") {
		t.Fatalf("no Warn named the in-flight creation: %q", warned)
	}
	// Nothing was frozen: the registry issues and the in-flight task answers.
	if _, err := r.Handle(scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 90, Op: "probe",
		ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}, "failure-inbox"); err != nil {
		t.Fatalf("probe after a refused Freeze: %v", err)
	}
	inflight.CallID = 91
	if reply, err := r.Handle(scope, inflight, "failure-inbox"); err != nil || reply.Phase != "starting" {
		t.Fatalf("resume after a refused Freeze: %+v %v", reply, err)
	}
	r.mu.Lock()
	quiescing, registryFrozen := r.quiescing, r.registry.Frozen()
	r.mu.Unlock()
	if quiescing || registryFrozen {
		t.Fatalf("refused Freeze left quiescing=%t registryFrozen=%t", quiescing, registryFrozen)
	}

	// A create arriving while Freeze waits is parked, not raced: it returns
	// only after Freeze gives up.
	freezeDone := make(chan error, 1)
	go func() { freezeDone <- r.Freeze(time.Now().Add(300 * time.Millisecond)) }()
	time.Sleep(50 * time.Millisecond)
	createDone := make(chan time.Time, 1)
	go func() {
		clockedCreate(t, r, clockedScope("secondSession"), "second", []string{"/bin/sh", "-c", "exec sleep 30"})
		createDone <- time.Now()
	}()
	select {
	case <-createDone:
		t.Fatal("create was admitted while Freeze was quiescing")
	case <-time.After(100 * time.Millisecond):
	}
	if err := <-freezeDone; !errors.Is(err, procrequest.ErrBusy) {
		t.Fatalf("second Freeze: %v", err)
	}
	select {
	case <-createDone:
	case <-time.After(runtimeTestBudget):
		t.Fatal("parked create never returned after Freeze gave up")
	}

	close(release)
	failureAwaitRunning(t, r, scope, inflight)
	imageFreeze(t, r)
	if spawned.Load() != 2 {
		t.Fatalf("spawned %d children, want 2", spawned.Load())
	}
	for _, row := range manager.Snapshot() {
		if row.Exit != nil {
			t.Fatalf("a child died around the fence: %+v", row)
		}
		imageChildAlive(t, row.OSPID)
	}
	if err := r.Thaw(); err != nil {
		t.Fatal(err)
	}
	_ = manager.SignalAll(syscall.SIGKILL)
	got, _, _, reply := failureCollect(t, r, scope, inflight)
	if len(got[0]) != 0 || reply.Signal != int(syscall.SIGKILL) {
		t.Fatalf("in-flight task after the fence: %+v", reply)
	}
}

// origin: hot-update S2 plan D1 / §1.5 — a frozen runtime answers nothing
// and refuses nothing: every op parks until Thaw (or Close: ErrClosed); the
// sweep is inert; a revocation under the fence is recorded and carried out
// by Thaw, which refuses to run while the process table is still frozen
// (the kill would otherwise be lost).
func TestRuntimeFrozenFenceBlocksUntilThaw(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(1_000_000_000)
	r := clockedRuntime(t, &clock, 256)
	manager := r.cfg.Manager
	scope := clockedScope("fenceSession")
	live, owner := clockedCreate(t, r, scope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, r, scope, live)
	imageFreeze(t, r)

	type outcome struct {
		op    string
		reply proto.ProcessResumeReply
		err   error
	}
	results := make(chan outcome, 3)
	probe := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, Op: "probe", ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}
	resume := live
	create := live
	create.Op, create.Exec = "create", &proto.ExecReq{Argv: []string{"/nonexistent/binary"}}
	create.Handle = &proto.ProcessHandle{Lease: live.Handle.Lease, RequestID: "fenced-create"}
	for i, req := range []proto.ProcessResumeReq{probe, resume, create} {
		req.CallID = uint64(100 + i)
		go func(req proto.ProcessResumeReq) {
			reply, err := r.Handle(scope, req, "failure-inbox")
			results <- outcome{op: req.Op, reply: reply, err: err}
		}(req)
	}
	select {
	case got := <-results:
		t.Fatalf("%s answered under the fence: %+v %v", got.op, got.reply, got.err)
	case <-time.After(150 * time.Millisecond):
	}
	// Sweep is inert under the fence even past every grace.
	clock.Add(int64(procrequest.MaxTTL) + int64(AbandonedTerminalGrace) + int64(AbandonedRunningGrace))
	if res, err := r.Sweep(); res != (SweepResult{}) || err != nil {
		t.Fatalf("sweep under the fence: %+v %v", res, err)
	}
	// Revoke only marks; the child is alive until the image that thaws
	// carries the kill out — and it must not thaw before the table does.
	if killed, err := r.Revoke(scope.SID, "session_destroyed"); killed != 1 || err != nil {
		t.Fatalf("revoke under the fence: %d %v", killed, err)
	}
	rows := manager.Snapshot()
	if len(rows) != 1 || rows[0].Exit != nil {
		t.Fatalf("process table under the fence: %+v", rows)
	}
	// kill(pid, 0) succeeds on a zombie too: the child must still be RUNNING
	// (not Z) a moment after the revocation was recorded.
	time.Sleep(50 * time.Millisecond)
	if state := imageProcState(t, rows[0].OSPID); state == "" || state == "Z" {
		t.Fatalf("revocation under the fence killed the child (state %q); the kill belongs to Thaw", state)
	}
	if _, err := manager.Freeze(); err != nil {
		t.Fatal(err)
	}
	if err := r.Thaw(); !errors.Is(err, workload.ErrFrozen) || !r.Frozen() {
		t.Fatalf("Thaw before the table thawed: err=%v frozen=%t, want ErrFrozen and still frozen", err, r.Frozen())
	}
	select {
	case got := <-results:
		t.Fatalf("%s answered after a refused Thaw: %+v %v", got.op, got.reply, got.err)
	case <-time.After(50 * time.Millisecond):
	}
	r.mu.Lock()
	stillPaused := owner.output[0].Snapshot().Replay.End == 0 && owner.frozen != nil
	r.mu.Unlock()
	if !stillPaused {
		t.Fatal("a refused Thaw changed the task's frozen state")
	}
	manager.Thaw()
	if err := r.Thaw(); err != nil {
		t.Fatal(err)
	}
	seen := map[string]outcome{}
	for len(seen) < 3 {
		select {
		case got := <-results:
			seen[got.op] = got
		case <-time.After(runtimeTestBudget):
			t.Fatalf("ops never returned after Thaw: %v", seen)
		}
	}
	if seen["probe"].err != nil || seen["probe"].reply.Lease == nil {
		t.Fatalf("probe after Thaw: %+v", seen["probe"])
	}
	if !errors.Is(seen["resume"].err, ErrRevoked) {
		t.Fatalf("resume of the revoked task after Thaw: %+v", seen["resume"])
	}
	if !errors.Is(seen["create"].err, ErrRevoked) {
		t.Fatalf("create under the pre-revocation lease after Thaw: %+v", seen["create"])
	}
	deadline := time.Now().Add(runtimeTestBudget)
	for {
		if err := syscall.Kill(rows[0].OSPID, 0); err != nil {
			break
		}
		r.mu.Lock()
		terminal := owner.terminalAt != 0
		r.mu.Unlock()
		if terminal || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := syscall.Kill(rows[0].OSPID, 0); err == nil {
		r.mu.Lock()
		terminal := owner.terminalAt
		r.mu.Unlock()
		if terminal == 0 {
			t.Fatal("the revocation recorded under the fence was never carried out by Thaw")
		}
	}

	// Close under the fence releases every waiter with ErrClosed.
	other := clockedRuntime(t, &clock, 256)
	otherScope := clockedScope("closedSession")
	otherLive, _ := clockedCreate(t, other, otherScope, "live", []string{"/bin/sh", "-c", "exec sleep 30"})
	failureAwaitRunning(t, other, otherScope, otherLive)
	imageFreeze(t, other)
	closed := make(chan error, 1)
	go func() {
		otherLive.CallID = 200
		_, err := other.Handle(otherScope, otherLive, "failure-inbox")
		closed <- err
	}()
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestBudget)
	defer cancel()
	if err := other.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter after Close: %v", err)
		}
	case <-time.After(runtimeTestBudget):
		t.Fatal("Close did not release the parked op")
	}
}

// origin: hot-update S2 plan exit #2 (in-process half) / §5.1 — the bytes a
// client has not acknowledged, the stdin it has not finished, an aborted
// stream's retained prefix and a terminal awaiting its ACK all survive
// Freeze → Export → Import → Thaw with their offsets intact, and every
// exit is delivered exactly once.
func TestRuntimeImageRestoresPartiallyAckedOutputs(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f, cfg := runtimeFixtureConfig(t, 256)
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { f.close(t) })
	control := imageControlPipe(t, f)

	// A: live, partial ACK on stdout, stderr unacknowledged, stdin unfinished.
	payload := append([]byte("first\n"), runtimePayload(200<<10)...)
	hA := f.lease(t, "partial")
	createdA := f.create(t, hA, proto.ExecReq{Argv: []string{"/bin/sh", "-c",
		"printf 0123456789; printf err >&2; read -r go <&3; IFS= read -r line; printf 'tail:%s' \"$line\"; exit 23"}, Stdin: payload})
	clientA := &runtimeClient{handle: hA, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.waitFor(t, "A's first bytes", func() bool {
		reply, err := f.request(clientA.req("resume"), clientA.inbox)
		if err != nil {
			t.Fatalf("resume A: %v", err)
		}
		return reply.Phase == "running" && reply.Stdout != nil && len(reply.Stdout.Data) == 10 && reply.Stderr != nil && len(reply.Stderr.Data) == 3
	})
	clientA.stdout = []byte("0123") // ACK four of the ten bytes, none of stderr
	if _, err := f.request(clientA.req("ack"), clientA.inbox); err != nil {
		t.Fatal(err)
	}
	// B: terminal awaiting its ACK with stdout aborted by a read error (the
	// PTY master reports EIO once the child is gone: a pumped, aborted stream).
	hB := f.lease(t, "aborted")
	createdB := f.create(t, hB, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf held; exit 37"},
		Env: map[string]string{runtimeFixtureShape: runtimeFixturePTYStdout}})
	f.waitFor(t, "B terminal", func() bool {
		reply, err := f.request(proto.ProcessResumeReq{Op: "resume", Handle: &hB, AttachEpoch: 1}, "_TINBOX.actor.original")
		return err == nil && reply.Phase == "terminal"
	})
	// D: revoked while its 256-byte log was full — an aborted, pumped stream.
	scopeD := f.scope
	scopeD.SID = "revokedSession"
	probeD, err := f.r.Handle(scopeD, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: f.calls.Add(1), Op: "probe",
		ActorFP: scopeD.ActorFP, ClientLineage: scopeD.ClientLineage}, "_TINBOX.actor.d")
	if err != nil {
		t.Fatal(err)
	}
	hD := proto.ProcessHandle{Lease: *probeD.Lease, RequestID: "revoked"}
	specD := proto.ExecReq{Argv: []string{"/bin/sh", "-c", "head -c 400 /dev/zero; exec sleep 30"}}
	createdD, err := f.r.Handle(scopeD, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: f.calls.Add(1), Op: "create",
		ActorFP: scopeD.ActorFP, Handle: &hD, AttachEpoch: 1, Exec: &specD}, "_TINBOX.actor.d")
	if err != nil {
		t.Fatal(err)
	}
	f.waitFor(t, "D's log full", func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		td := f.r.tasks[createdD.PID]
		return td != nil && td.output[0] != nil && td.output[0].Snapshot().Replay.End == 256
	})
	if killed, err := f.r.Revoke(scopeD.SID, "session_destroyed"); killed != 1 || err != nil {
		t.Fatalf("revoke D: %d %v", killed, err)
	}
	f.waitFor(t, "D terminal", func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		return f.r.tasks[createdD.PID].terminalAt != 0
	})

	img, resources, rows := imageCut(t, f)
	var taskA TaskImage
	for _, ti := range img.Tasks {
		if ti.PID == createdA.PID {
			taskA = ti
		}
	}
	if taskA.Stdin == nil || taskA.Stdin.Input == nil || taskA.Stdin.Replay.Base >= taskA.Stdin.Replay.End ||
		taskA.Stdout == nil || taskA.Stdout.Replay.Base != 4 || taskA.Stdout.Replay.End != 10 ||
		taskA.Stderr == nil || taskA.Stderr.Replay.Base != 0 || taskA.Stderr.Replay.End != 3 {
		t.Fatalf("exported A does not carry the partial ACK / unfinished stdin: %+v", taskA)
	}
	if len(img.Tasks) != 3 || img.BudgetUsed != f.r.ReplayBudgetUsed() {
		t.Fatalf("exported %d tasks, budget %d vs %d", len(img.Tasks), img.BudgetUsed, f.r.ReplayBudgetUsed())
	}
	usedBefore := f.r.ReplayBudgetUsed()
	imported := imageSwitch(t, f, cfg, img, resources, rows)
	if got := imported.ReplayBudgetUsed(); got != usedBefore {
		t.Fatalf("budget after import %d, before %d", got, usedBefore)
	}

	// A: the first batch after the switch starts exactly at the ACK.
	reply, err := f.request(clientA.req("resume"), clientA.inbox)
	if err != nil || reply.Stdout == nil || reply.Stdout.Offset != 4 || string(reply.Stdout.Data) != "456789" ||
		reply.Stderr == nil || reply.Stderr.Offset != 0 || string(reply.Stderr.Data) != "err" || reply.TargetInstance != "original-runtime" {
		t.Fatalf("A after the switch: %+v %v", reply, err)
	}
	clientA.accept(t, reply)
	if _, err := control.WriteString("go\n"); err != nil {
		t.Fatal(err)
	}
	f.drain(t, clientA)
	if !bytes.Equal(clientA.stdout, []byte("0123456789tail:first")) || !bytes.Equal(clientA.stderr, []byte("err")) || clientA.terminal.ExitCode != 23 {
		t.Fatalf("A's bytes/rc after the switch: stdout=%q stderr=%q rc=%d", clientA.stdout, clientA.stderr, clientA.terminal.ExitCode)
	}
	f.settle(t, clientA)
	// B: terminal-ack per stream — stdout aborted (the PTY read error) at
	// its End with its bytes retained, stderr a true FIN.
	replyB, err := f.request(proto.ProcessResumeReq{Op: "resume", Handle: &hB, AttachEpoch: 1}, "_TINBOX.actor.original")
	if err != nil || replyB.Phase != "terminal" || replyB.ExitCode != 37 || replyB.Stdout == nil || replyB.Stdout.Error == "" ||
		replyB.Stdout.FIN || string(replyB.Stdout.Data) != "held" || replyB.Stderr == nil || !replyB.Stderr.FIN {
		t.Fatalf("B after the switch: %+v stdout=%+v stderr=%+v %v", replyB, replyB.Stdout, replyB.Stderr, err)
	}
	ackB := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &hB, AttachEpoch: 1, TerminalID: replyB.TerminalID,
		StdoutACK: replyB.Stdout.Offset + uint64(len(replyB.Stdout.Data)),
		StderrACK: replyB.Stderr.Offset + uint64(len(replyB.Stderr.Data)), StderrFIN: true}
	if settled, err := f.request(ackB, "_TINBOX.actor.original"); err != nil || settled.Phase != "settled" {
		t.Fatalf("settle B after the switch: %+v %v", settled, err)
	}
	// D: the revoked task's retained 256 bytes and its abort marker.
	replyD, err := f.r.Handle(scopeD, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: f.calls.Add(1), Op: "resume",
		ActorFP: scopeD.ActorFP, Handle: &hD, AttachEpoch: 1}, "_TINBOX.actor.d")
	if err != nil || replyD.Phase != "terminal" || replyD.Stdout == nil || len(replyD.Stdout.Data) != 256 ||
		!strings.HasPrefix(replyD.Stdout.Error, "revoked:") || !strings.HasPrefix(replyD.Error, "revoked:") {
		t.Fatalf("D after the switch: %+v %v", replyD, err)
	}
	ackD := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: f.calls.Add(1), Op: "terminal-ack", ActorFP: scopeD.ActorFP,
		Handle: &hD, AttachEpoch: 1, TerminalID: replyD.TerminalID, StdoutACK: 256, StderrACK: replyD.Stderr.Offset + uint64(len(replyD.Stderr.Data)),
		StderrFIN: replyD.Stderr.FIN}
	if settled, err := f.r.Handle(scopeD, ackD, "_TINBOX.actor.d"); err != nil || settled.Phase != "settled" {
		t.Fatalf("settle D after the switch: %+v %v", settled, err)
	}
	for _, pid := range []string{createdA.PID, createdB.PID, createdD.PID} {
		if n := imageExitedCount(f, pid); n != 1 {
			t.Fatalf("task %s exited %d times, want exactly once", pid, n)
		}
	}
	if rows := f.manager.Snapshot(); len(rows) != 0 {
		t.Fatalf("process table not empty after settlement: %+v", rows)
	}
}

// origin: hot-update S2 plan D5 — a task's phase is derived from its
// registry record, never from its done channel: finish (record terminal)
// and conclude (done closed) are separated by an unlocked window, and an
// image cut in that window must still import the task as terminal.
func TestRuntimeImageExportsTaskBetweenFinishAndConclude(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f, cfg := runtimeFixtureConfig(t, 256)
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { f.close(t) })
	h := f.lease(t, "window")
	created := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf done; exit 5"}})
	client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.drain(t, client)
	// Re-open the window: the record is terminal, the owner has not published.
	r.mu.Lock()
	owner := r.tasks[created.PID]
	owner.done = make(chan struct{})
	r.mu.Unlock()
	img, resources, rows := imageCut(t, f)
	imported := imageSwitch(t, f, cfg, img, resources, rows)
	imported.mu.Lock()
	restored := imported.tasks[created.PID]
	imported.mu.Unlock()
	select {
	case <-restored.done:
	default:
		t.Fatal("imported terminal task is not published")
	}
	f.settle(t, client)
	if n := imageExitedCount(f, created.PID); n != 1 {
		t.Fatalf("exited %d times", n)
	}
}

// origin: hot-update S2 plan §1.6 / S1 review E5 (frozen half) — a child
// that exits DURING the cut is accounted exactly once by the next image:
// as a zombie (the table was frozen too: reaped by the restored table's
// reaper) or as a retained exit row (the table was live: the reaper
// recorded it while observe was gated). The direct oracle of the gate is
// that observe touched nothing while frozen: stdin still owned, exit row
// not forgotten, no exit delivered.
func TestRuntimeImageAccountsExitDuringFreeze(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	for _, arm := range []struct {
		name  string
		shape string
	}{{"zombie", ""}, {"reaped", runtimeFixtureAdopt}} {
		t.Run(arm.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f, cfg := runtimeFixtureConfig(t, 256)
			r, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			f.r = r
			t.Cleanup(func() { f.close(t) })
			h := f.lease(t, "exiting")
			env := map[string]string{}
			if arm.shape != "" {
				env[runtimeFixtureShape] = arm.shape
			}
			// The child closes its stdio before sleeping: both streams reach a
			// true EOF while it is alive, so observe is already "ready" (polling
			// the table every tick) when the fence goes up — the shape in which
			// an un-gated observe would release stdin and try to finish under
			// the fence.
			created := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf alive; exec >/dev/null 2>&1; exec sleep 30"}, Env: env})
			client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
			f.waitFor(t, "running with both streams at EOF", func() bool {
				reply, err := f.request(client.req("resume"), client.inbox)
				if err != nil || reply.Phase != "running" || reply.Stdout == nil || reply.Stderr == nil {
					return false
				}
				client.accept(t, reply)
				return client.stdoutFIN && client.stderrFIN
			})
			imageFreeze(t, r)
			var rows []workload.Record
			if arm.name == "zombie" {
				rows, err = f.manager.Freeze()
				if err != nil {
					t.Fatal(err)
				}
			} else {
				rows = f.manager.Snapshot()
			}
			osPID := rows[0].OSPID
			if err := syscall.Kill(osPID, syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if arm.name == "zombie" {
				f.waitFor(t, "zombie", func() bool { return imageProcState(t, osPID) == "Z" })
				time.Sleep(50 * time.Millisecond) // give a mis-gated observer its chance to act
			} else {
				f.waitFor(t, "reaped row", func() bool {
					rows := f.manager.Snapshot()
					return len(rows) == 1 && rows[0].Exit != nil
				})
				time.Sleep(50 * time.Millisecond)
			}
			// The gate's direct oracle.
			r.mu.Lock()
			owner := r.tasks[created.PID]
			inputOwned, terminalAt := owner.input != nil, owner.terminalAt
			r.mu.Unlock()
			if !inputOwned || terminalAt != 0 || imageExitedCount(f, created.PID) != 0 {
				t.Fatalf("observe acted under the fence: input=%t terminalAt=%d exited=%d", inputOwned, terminalAt, imageExitedCount(f, created.PID))
			}
			if arm.name == "reaped" {
				if rows = f.manager.Snapshot(); len(rows) != 1 || rows[0].Exit == nil {
					t.Fatalf("exit row forgotten under the fence: %+v", rows)
				}
				if _, err := f.manager.Freeze(); err != nil {
					t.Fatal(err)
				}
			}
			img, resources, err := r.Export()
			if err != nil {
				t.Fatal(err)
			}
			imported := imageSwitch(t, f, cfg, img, resources, rows)
			f.drain(t, client)
			if client.terminal.Signal != int(syscall.SIGTERM) || client.terminal.ExitCode != -1 || string(client.stdout) != "alive" {
				t.Fatalf("terminal after the switch: %+v stdout=%q", client.terminal, client.stdout)
			}
			f.settle(t, client)
			if n := imageExitedCount(f, created.PID); n != 1 {
				t.Fatalf("exited %d times, want exactly once", n)
			}
			_ = imported
		})
	}
}

// imageRejectFixture builds the reference cut every rejection row mutates:
// a live task, a terminal task, a tombstone and a revocation.
type imageRejectFixture struct {
	f         *runtimeFixture
	cfg       Config
	img       Image
	resources []handoff.Resource
	rows      []workload.Record
	live      string
	terminal  string
	tombstone string
	osPID     int
	clock     atomic.Int64
}

func newImageRejectFixture(t *testing.T) *imageRejectFixture {
	t.Helper()
	rf := &imageRejectFixture{}
	rf.clock.Store(int64(time.Hour))
	f, cfg := runtimeFixtureConfig(t, 256)
	cfg.Clock = func() (int64, error) { return rf.clock.Load(), nil }
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { f.close(t) })
	rf.f, rf.cfg = f, cfg
	h := f.lease(t, "live")
	rf.live = f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}}).PID
	liveClient := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.waitFor(t, "live running", func() bool {
		reply, err := f.request(liveClient.req("resume"), liveClient.inbox)
		return err == nil && reply.Phase == "running"
	})
	hT := f.lease(t, "terminal")
	rf.terminal = f.create(t, hT, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exit 7"}}).PID
	f.drain(t, &runtimeClient{handle: hT, epoch: 1, inbox: "_TINBOX.actor.original"})
	hE := f.lease(t, "tombstone")
	rf.tombstone = f.create(t, hE, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exit 9"}}).PID
	f.drain(t, &runtimeClient{handle: hE, epoch: 1, inbox: "_TINBOX.actor.original"})
	// A terminal reply precedes the observer's done publication. Sweep must
	// retain an unpublished owner, so join both completions before advancing
	// the synthetic clock and expecting exactly one abandoned tombstone.
	// origin: hot-update S2 plan §1.5 / §1.6; GATES-FINAL-2 fixture ordering.
	for _, pid := range []string{rf.terminal, rf.tombstone} {
		r.mu.Lock()
		done := r.tasks[pid].done
		r.mu.Unlock()
		failureAwait(t, done, "terminal observer completion before sweep")
	}
	rf.clock.Store(hE.Lease.ExpiresBootNS + int64(AbandonedTerminalGrace) + int64(time.Second))
	// Keep the terminal task alive through the sweep by touching it.
	if _, err := f.request(proto.ProcessResumeReq{Op: "resume", Handle: &hT, AttachEpoch: 1}, "_TINBOX.actor.original"); err != nil {
		t.Fatal(err)
	}
	if res, err := r.Sweep(); res.Discarded != 1 || err != nil {
		t.Fatalf("sweep: %+v %v", res, err)
	}
	if _, err := r.Revoke("someOtherSession", "session_destroyed"); err != nil {
		t.Fatal(err)
	}
	rf.img, rf.resources, rf.rows = imageCut(t, f)
	rf.osPID = rf.rows[0].OSPID
	return rf
}

func (rf *imageRejectFixture) task(pid string) *TaskImage {
	for i := range rf.img.Tasks {
		if rf.img.Tasks[i].PID == pid {
			return &rf.img.Tasks[i]
		}
	}
	return nil
}

func imageClone(t *testing.T, img Image) Image {
	t.Helper()
	data, err := json.Marshal(img)
	if err != nil {
		t.Fatal(err)
	}
	var out Image
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// origin: hot-update S2 plan exit #3 / §2.3 — every rejection leaves
// nothing installed: the files already taken come back open (Prepare-ready
// salvage), the child is untouched, the process table is unchanged, no
// goroutine was started, and the error text never carries the registry key.
func TestRuntimeImageImportRejectsInvalidBundle(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	rf := newImageRejectFixture(t)
	live, terminal := rf.task(rf.live), rf.task(rf.terminal)
	if live == nil || terminal == nil || len(rf.img.Tombstones) != 1 || len(rf.img.Revocations) != 1 {
		t.Fatalf("fixture cut: %+v", rf.img)
	}
	secretHex := hex.EncodeToString(rf.img.Registry.Secret)
	secretB64 := base64.StdEncoding.EncodeToString(rf.img.Registry.Secret)
	type mutation struct {
		name    string
		img     func(*Image)
		rows    func([]workload.Record) []workload.Record
		files   func(*imageDupSource)
		cfg     func(*Config)
		foreign bool
		// pure rows are refused by stage ① (data alone): no descriptor may
		// have been taken. Without this the budget rows would pass on the
		// stage-③ reservation failure even if stage ① adopted the image's
		// own limit (mutation M36 survived exactly that way).
		pure bool
	}
	setTask := func(img *Image, pid string, fn func(*TaskImage)) {
		for i := range img.Tasks {
			if img.Tasks[i].PID == pid {
				fn(&img.Tasks[i])
			}
		}
	}
	plain, err := os.CreateTemp(t.TempDir(), "plain")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	if err := plain.Truncate(256); err != nil {
		t.Fatal(err)
	}
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = readEnd.Close(); _ = writeEnd.Close() })
	rows := []mutation{
		{name: "schema-version", img: func(img *Image) { img.Version = 2 }},
		{name: "instance-mismatch", img: func(img *Image) { img.Instance = "someone-else" }},
		{name: "registry-instance-mismatch", img: func(img *Image) { img.Registry.Instance = "someone-else" }},
		{name: "boot-id", img: func(img *Image) { img.Registry.BootID = "0000-not-this-boot" }},
		{name: "secret-length", img: func(img *Image) { img.Registry.Secret = img.Registry.Secret[:8] }},
		{name: "duplicate-task", img: func(img *Image) { img.Tasks = append(img.Tasks, img.Tasks[0]) }},
		{name: "task-without-record", img: func(img *Image) { img.Tasks[0].PID = "01ORPHANTASKWITHOUTRECORD0" }},
		{name: "handle-mismatch", img: func(img *Image) { setTask(img, rf.live, func(ti *TaskImage) { ti.Handle.RequestID = "other" }) }},
		{name: "duplicate-registry-identity", img: func(img *Image) {
			img.Registry.Records = append(img.Registry.Records, img.Registry.Records[0])
		}},
		{name: "budget-sum", img: func(img *Image) { img.BudgetUsed++ }, pure: true},
		// The limit is THIS binary's (Config.ReplayBudget), never the image's
		// figure (S2 internal review I2/R6): an image that fits the exporter
		// but not the importer is refused before any descriptor is taken.
		{name: "budget-over-importer-limit", cfg: func(cfg *Config) { cfg.ReplayBudget = 1024 }, pure: true},
		{name: "budget-limit-inflated-is-ignored", img: func(img *Image) { img.BudgetLimit = 1 << 40 },
			cfg: func(cfg *Config) { cfg.ReplayBudget = 1024 }, pure: true},
		{name: "capacity-over-max", img: func(img *Image) {
			setTask(img, rf.live, func(ti *TaskImage) {
				ti.Stdout.Replay.Capacity = workload.DefaultReplayCapacity + 1
				ti.Stdout.Output.Replay.Capacity = workload.DefaultReplayCapacity + 1
			})
			img.BudgetUsed += workload.DefaultReplayCapacity + 1 - 256
		}},
		{name: "terminal-with-stdin", img: func(img *Image) {
			setTask(img, rf.terminal, func(ti *TaskImage) {
				ti.Stdin = live.Stdin
				ti.Files[fileStdinReplay] = live.Files[fileStdinReplay]
				ti.Files[fileStdinTarget] = live.Files[fileStdinTarget]
			})
			img.BudgetUsed += live.Stdin.Replay.Capacity
		}},
		{name: "running-without-stdin", img: func(img *Image) {
			setTask(img, rf.live, func(ti *TaskImage) {
				img.BudgetUsed -= ti.Stdin.Replay.Capacity
				ti.Stdin = nil
				delete(ti.Files, fileStdinReplay)
				delete(ti.Files, fileStdinTarget)
			})
		}},
		{name: "running-without-pump", img: func(img *Image) {
			setTask(img, rf.live, func(ti *TaskImage) { ti.Stdout.Output = nil; delete(ti.Files, fileStdoutSource) })
		}},
		{name: "abort-with-pump", img: func(img *Image) { setTask(img, rf.live, func(ti *TaskImage) { ti.Abort[0] = "fabricated" }) }},
		{name: "lost-terminal-on-terminal", img: func(img *Image) { setTask(img, rf.terminal, func(ti *TaskImage) { ti.LostTerminal = true }) }},
		{name: "record-without-owner", img: func(img *Image) { img.Tasks = img.Tasks[:0] }},
		{name: "settled-record-with-task", img: func(img *Image) {
			for i := range img.Registry.Records {
				if img.Registry.Records[i].PID == rf.terminal {
					img.Registry.Records[i].Phase = "settled"
				}
			}
		}},
		{name: "tombstone-without-record", img: func(img *Image) { img.Tombstones[0].PID = "01ORPHANTOMBSTONE000000000" }},
		{name: "tombstone-is-task", img: func(img *Image) { img.Tombstones[0].PID = rf.terminal }},
		{name: "tombstone-terminal-mismatch", img: func(img *Image) { img.Tombstones[0].Terminal.ExitCode++ }},
		{name: "last-touch-after-freeze", img: func(img *Image) { setTask(img, rf.live, func(ti *TaskImage) { ti.LastTouch = img.FrozenAtBootNS + 1 }) }},
		{name: "freeze-in-future", img: func(img *Image) { img.FrozenAtBootNS = rf.clock.Load() + 1 }},
		{name: "revocation-duplicate", img: func(img *Image) { img.Revocations = append(img.Revocations, img.Revocations[0]) }},
		{name: "pending-without-error", img: func(img *Image) {
			setTask(img, rf.live, func(ti *TaskImage) { ti.Stdout.Output.Pending = []byte("x") })
		}},
		{name: "input-not-eof-without-target", img: func(img *Image) {
			// The live task delivered its (empty) stdin EOF, so the bundle
			// has no write end; claiming the EOF is still pending demands one.
			setTask(img, rf.live, func(ti *TaskImage) { ti.Stdin.Input.EOF = false })
		}},
		{name: "readerror-with-fin", img: func(img *Image) {
			setTask(img, rf.live, func(ti *TaskImage) {
				ti.Stdout.Output.ReadError, ti.Stdout.Output.Replay.FIN, ti.Stdout.Replay.FIN = "boom", true, true
			})
		}},
		{name: "extra-file", img: func(img *Image) {
			setTask(img, rf.live, func(ti *TaskImage) { ti.Files["stdin-extra"] = FileIdentity{} })
		}},
		{name: "missing-file-entry", img: func(img *Image) { setTask(img, rf.live, func(ti *TaskImage) { delete(ti.Files, fileStderrSource) }) }},
		{name: "declared-file-absent", files: func(src *imageDupSource) {
			f := src.files[resourceID(rf.live)][fileStderrReplay]
			_ = f.Close()
			delete(src.files[resourceID(rf.live)], fileStderrReplay)
		}},
		{name: "replay-is-plain-file", img: func(img *Image) {
			// Identity is patched so the shape predicate is the one that fires.
			id, err := fileIdentity(plain)
			if err != nil {
				t.Fatal(err)
			}
			setTask(img, rf.live, func(ti *TaskImage) { ti.Files[fileStdoutReplay] = id })
		}, files: func(src *imageDupSource) {
			old := src.files[resourceID(rf.live)][fileStdoutReplay]
			_ = old.Close()
			src.files[resourceID(rf.live)][fileStdoutReplay] = imageDup(t, plain)
		}},
		{name: "source-and-replay-swapped", files: func(src *imageDupSource) {
			files := src.files[resourceID(rf.live)]
			files[fileStdoutReplay], files[fileStdoutSource] = files[fileStdoutSource], files[fileStdoutReplay]
		}},
		{name: "same-shape-memfd-swapped", files: func(src *imageDupSource) {
			files := src.files[resourceID(rf.live)]
			files[fileStdoutReplay], files[fileStderrReplay] = files[fileStderrReplay], files[fileStdoutReplay]
		}},
		{name: "write-end-in-source-slot", img: func(img *Image) {
			id, err := fileIdentity(writeEnd)
			if err != nil {
				t.Fatal(err)
			}
			setTask(img, rf.live, func(ti *TaskImage) { ti.Files[fileStdoutSource] = id })
		}, files: func(src *imageDupSource) {
			files := src.files[resourceID(rf.live)]
			_ = files[fileStdoutSource].Close()
			files[fileStdoutSource] = imageDup(t, writeEnd)
		}},
		{name: "foreign-process-row", foreign: true, rows: func(rows []workload.Record) []workload.Record {
			return append(rows, workload.Record{Identity: workload.Identity{ID: "legacy-exec-child", OSPID: rf.osPID, StartTimeTicks: 1, StartedAt: time.Now()}})
		}},
		{name: "row-for-terminal-task", rows: func(rows []workload.Record) []workload.Record {
			return append(rows, workload.Record{Identity: workload.Identity{ID: rf.terminal, OSPID: rf.osPID, StartTimeTicks: 1, StartedAt: time.Now()}})
		}},
		{name: "running-task-without-row", rows: func([]workload.Record) []workload.Record { return nil }},
		{name: "duplicate-row", rows: func(rows []workload.Record) []workload.Record { return append(rows, rows[0]) }},
	}
	baselineG := runtime.NumGoroutine()
	tableBefore := fmt.Sprintf("%+v", rf.f.manager.Snapshot())
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			img := imageClone(t, rf.img)
			if tc.img != nil {
				tc.img(&img)
			}
			procRows := append([]workload.Record(nil), rf.rows...)
			if tc.rows != nil {
				procRows = tc.rows(procRows)
			}
			cfg := rf.cfg
			if tc.cfg != nil {
				tc.cfg(&cfg)
			}
			fdBefore := testharness.FDCount()
			src := newImageDupSource(t, rf.resources)
			if tc.files != nil {
				tc.files(src)
			}
			imported, salvage, err := Import(cfg, img, procRows, src)
			if err == nil || imported != nil {
				if imported != nil {
					_, _ = imported.Detach()
				}
				t.Fatalf("mutation %s was imported", tc.name)
			}
			if !errors.Is(err, ErrImage) {
				t.Fatalf("mutation %s: error is not ErrImage: %v", tc.name, err)
			}
			if tc.foreign != errors.Is(err, ErrForeignProcess) {
				t.Fatalf("mutation %s: ErrForeignProcess=%t: %v", tc.name, errors.Is(err, ErrForeignProcess), err)
			}
			text := err.Error()
			if strings.Contains(text, secretHex) || strings.Contains(text, secretB64) || strings.Contains(strings.ToUpper(text), strings.ToUpper(secretHex)) {
				t.Fatalf("mutation %s: error carries the registry secret: %q", tc.name, text)
			}
			// Salvage: every taken file open, CLOEXEC, and nothing was closed.
			taken := 0
			for _, res := range salvage {
				for name, f := range res.Files {
					imageFileOpen(t, res.ID+"/"+name, f)
					taken++
					_ = f.Close()
				}
			}
			src.Close()
			if tc.pure && taken != 0 {
				t.Fatalf("mutation %s: refused only after %d descriptors were taken; stage ① must refuse it from the data", tc.name, taken)
			}
			if got := testharness.FDCount(); got != fdBefore {
				t.Fatalf("mutation %s: importer leaked or closed descriptors: fd=%d before=%d (taken %d)", tc.name, got, fdBefore, taken)
			}
			imageChildAlive(t, rf.osPID)
			if got := fmt.Sprintf("%+v", rf.f.manager.Snapshot()); got != tableBefore {
				t.Fatalf("mutation %s: process table changed:\n%s\n%s", tc.name, tableBefore, got)
			}
			if g := runtime.NumGoroutine(); g > baselineG+2 {
				t.Fatalf("mutation %s: goroutines %d > baseline %d", tc.name, g, baselineG)
			}
		})
	}
	// The intact bundle still imports, and the old image can still be
	// thawed instead: nothing above consumed anything.
	rf.f.manager.Thaw()
	if err := rf.f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
	h := live.Handle
	_ = rf.f.manager.Signal(rf.live, syscall.SIGKILL)
	client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
	rf.f.drain(t, client)
	if client.terminal.Signal != int(syscall.SIGKILL) {
		t.Fatalf("live task after the refused imports: %+v", client.terminal)
	}
	rf.f.settle(t, client)
}

// origin: hot-update S2 plan §5.1 — the registry key travels only inside
// the sealed snapshot: no rendering of the image and no error text may
// carry it, and the image stays a metadata-sized document no matter how
// many megabytes its logs retain.
func TestRuntimeImportErrorsNeverCarryRegistrySecret(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newRuntimeFixture(t, workload.DefaultReplayCapacity)
	for i := 0; i < 3; i++ {
		h := f.lease(t, fmt.Sprintf("big%d", i))
		f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}, Stdin: runtimePayload(64 << 10)})
		client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
		f.waitFor(t, "running", func() bool {
			reply, err := f.request(client.req("resume"), client.inbox)
			return err == nil && reply.Phase == "running"
		})
	}
	img, _, _ := imageCut(t, f)
	secretHex := hex.EncodeToString(img.Registry.Secret)
	secretB64 := base64.StdEncoding.EncodeToString(img.Registry.Secret)
	for verb, text := range map[string]string{"%v": fmt.Sprintf("%v", img), "%+v": fmt.Sprintf("%+v", img),
		"%x": fmt.Sprintf("%x", img.Registry.Secret), "%s": fmt.Sprint(img.Registry)} {
		if strings.Contains(text, secretHex) || strings.Contains(text, secretB64) || strings.Contains(strings.ToUpper(text), strings.ToUpper(secretHex)) {
			t.Fatalf("%s rendering of the image carries the registry secret", verb)
		}
	}
	data, err := json.Marshal(img)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), secretB64) {
		t.Fatal("the JSON image must carry the key for the sealed snapshot")
	}
	if len(data) > 64<<10 {
		t.Fatalf("image JSON is %d bytes for 3 tasks × 8 MiB logs: bytes leaked into the metadata", len(data))
	}
	if img.BudgetUsed != 3*(workload.DefaultReplayCapacity*2+64<<10) {
		t.Fatalf("budget %d", img.BudgetUsed)
	}
	f.manager.Thaw()
	if err := f.r.Thaw(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S2 plan D-carried "tombstone/revocations 随 image 导出"
// (S1 §9-11) — both ownership decisions survive the switch: a tombstone
// still settles exactly once for a client holding every byte and answers
// ErrOutputDiscarded to anyone else; a lease issued before a revocation is
// still refused, one issued afterwards still works. The mutation that drops
// tombstones from Export is caught at import (a terminal record with no
// owner); dropping revocations lets the old lease create.
func TestRuntimeImagePreservesTombstonesAndRevocations(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	rf := newImageRejectFixture(t)
	var tombstone TombstoneImage
	for _, ts := range rf.img.Tombstones {
		tombstone = ts
	}
	if tombstone.PID != rf.tombstone || rf.img.Revocations[0].SID != "someOtherSession" {
		t.Fatalf("cut: tombstones=%+v revocations=%+v", rf.img.Tombstones, rf.img.Revocations)
	}
	// A lease of the revoked session issued BEFORE the revocation.
	revokedScope := rf.f.scope
	revokedScope.SID = "someOtherSession"
	imported := imageSwitch(t, rf.f, rf.cfg, rf.img, rf.resources, rf.rows)
	r := imported
	ack := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &tombstone.Handle, AttachEpoch: 1, TerminalID: tombstone.Terminal.ID,
		StdoutACK: tombstone.FinalEnd[0], StderrACK: tombstone.FinalEnd[1], StdoutFIN: tombstone.FinalFIN[0], StderrFIN: tombstone.FinalFIN[1]}
	if reply, err := rf.f.request(proto.ProcessResumeReq{Op: "resume", Handle: &tombstone.Handle, AttachEpoch: 1}, "_TINBOX.actor.original"); !errors.Is(err, ErrOutputDiscarded) {
		t.Fatalf("resume of a tombstone after the switch: %+v %v", reply, err)
	}
	if reply, err := rf.f.request(ack, "_TINBOX.actor.original"); err != nil || reply.Phase != "settled" {
		t.Fatalf("settle a tombstone after the switch: %+v %v", reply, err)
	}
	// The revocation: a pre-revocation lease can never create; a fresh one can.
	// Leases are signed by the registry: issue one now, on a clock set
	// before the revocation instant.
	// Ten seconds before the revocation with the default 30 s TTL: the lease
	// is still VALID at the create instant, so the refusal below is the
	// revocation's, not an expiry (S2 internal review P8).
	rf.clock.Store(rf.img.Revocations[0].AtBootNS - 10*int64(time.Second))
	probe0, err := r.Handle(revokedScope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 500, Op: "probe",
		ActorFP: revokedScope.ActorFP, ClientLineage: revokedScope.ClientLineage}, "_TINBOX.actor.x")
	if err != nil {
		t.Fatal(err)
	}
	old := *probe0.Lease
	rf.clock.Store(rf.img.FrozenAtBootNS + int64(time.Second))
	spec := proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exit 0"}}
	create := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 501, Op: "create", ActorFP: revokedScope.ActorFP,
		Handle: &proto.ProcessHandle{Lease: old, RequestID: "pre-revocation"}, AttachEpoch: 1, Exec: &spec}
	if _, err := r.Handle(revokedScope, create, "_TINBOX.actor.x"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("pre-revocation lease after the switch: %v", err)
	}
	probe, err := r.Handle(revokedScope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 502, Op: "probe",
		ActorFP: revokedScope.ActorFP, ClientLineage: revokedScope.ClientLineage}, "_TINBOX.actor.x")
	if err != nil {
		t.Fatal(err)
	}
	create.CallID, create.Handle = 503, &proto.ProcessHandle{Lease: *probe.Lease, RequestID: "post-revocation"}
	reply, err := r.Handle(revokedScope, create, "_TINBOX.actor.x")
	if err != nil || reply.PID == "" {
		t.Fatalf("post-revocation lease after the switch: %+v %v", reply, err)
	}
	got, fin, _, terminal := failureCollect(t, r, revokedScope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion,
		CallID: 600, ActorFP: revokedScope.ActorFP, Handle: create.Handle, AttachEpoch: 2}) // a new inbox needs a new epoch
	if len(got[0]) != 0 || !fin[0] || !fin[1] || terminal.ExitCode != 0 {
		t.Fatalf("post-revocation task: %+v", terminal)
	}
	_ = rf.f.manager.Signal(rf.live, syscall.SIGKILL)
}

// origin: hot-update S2 plan exit #5 — five in-process round trips (a live
// task, a terminal task and a tombstone each) return every descriptor,
// memfd and goroutine. The anchor exerciseRuntimeImageRoundTrip is what
// the leak-shape gate pins.
func TestRuntimeImageRoundTripReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	exerciseRuntimeImageRoundTrip(t) // warm the poller and the fixture
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseRuntimeImageRoundTrip(t)
	}
	testharness.AssertNoGoroutineLeak(t, "runtime-image-round-trip", beforeG)
	testharness.AssertNoFDLeak(t, "runtime-image-round-trip", beforeFD, 0)
}

func exerciseRuntimeImageRoundTrip(t *testing.T) {
	t.Helper()
	rf := newImageRejectFixture(t)
	live := rf.task(rf.live)
	usedBefore := rf.f.r.ReplayBudgetUsed()
	imported := imageSwitch(t, rf.f, rf.cfg, rf.img, rf.resources, rf.rows)
	if got := imported.ReplayBudgetUsed(); got != usedBefore || got != rf.img.BudgetUsed {
		t.Fatalf("budget %d after import, %d before, %d exported", got, usedBefore, rf.img.BudgetUsed)
	}
	// Terminal task: settle. Tombstone: settle. Live: kill, drain, settle.
	terminal := rf.task(rf.terminal)
	replyT, err := rf.f.request(proto.ProcessResumeReq{Op: "resume", Handle: &terminal.Handle, AttachEpoch: 1}, "_TINBOX.actor.original")
	if err != nil || replyT.Phase != "terminal" || replyT.ExitCode != 7 {
		t.Fatalf("terminal after the switch: %+v %v", replyT, err)
	}
	ackT := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &terminal.Handle, AttachEpoch: 1, TerminalID: replyT.TerminalID,
		StdoutACK: replyT.Stdout.Offset + uint64(len(replyT.Stdout.Data)), StdoutFIN: replyT.Stdout.FIN,
		StderrACK: replyT.Stderr.Offset + uint64(len(replyT.Stderr.Data)), StderrFIN: replyT.Stderr.FIN}
	if reply, err := rf.f.request(ackT, "_TINBOX.actor.original"); err != nil || reply.Phase != "settled" {
		t.Fatalf("settle terminal: %+v %v", reply, err)
	}
	ts := rf.img.Tombstones[0]
	ackE := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &ts.Handle, AttachEpoch: 1, TerminalID: ts.Terminal.ID,
		StdoutACK: ts.FinalEnd[0], StderrACK: ts.FinalEnd[1], StdoutFIN: ts.FinalFIN[0], StderrFIN: ts.FinalFIN[1]}
	if reply, err := rf.f.request(ackE, "_TINBOX.actor.original"); err != nil || reply.Phase != "settled" {
		t.Fatalf("settle tombstone: %+v %v", reply, err)
	}
	_ = rf.f.manager.Signal(rf.live, syscall.SIGKILL)
	client := &runtimeClient{handle: live.Handle, epoch: 1, inbox: "_TINBOX.actor.original"}
	rf.f.drain(t, client)
	rf.f.settle(t, client)
	rf.f.close(t)
	if rows := rf.f.manager.Snapshot(); len(rows) != 0 {
		t.Fatalf("round trip retained %d process owners", len(rows))
	}
	if used := imported.ReplayBudgetUsed(); used != 0 {
		t.Fatalf("round trip retained %d bytes of replay budget", used)
	}
}

// origin: hot-update S2 plan §5.1 — a calibration sample of the fence's
// own cost with eight fully backpressured tasks. The only assertion is the
// budget; the numbers are logged as a sample, NOT claimed as a bound.
func TestRuntimeFreezeLatencySample(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f, cfg := runtimeFixtureConfig(t, 256)
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { f.close(t) })
	for i := 0; i < 8; i++ {
		h := f.lease(t, fmt.Sprintf("load%d", i))
		created := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "head -c 1024 /dev/zero; head -c 1024 /dev/zero >&2; exec sleep 30"}})
		f.waitFor(t, "full logs", func() bool {
			r.mu.Lock()
			defer r.mu.Unlock()
			owner := r.tasks[created.PID]
			return owner != nil && owner.output[0] != nil && owner.output[1] != nil &&
				owner.output[0].Snapshot().Replay.End == 256 && owner.output[1].Snapshot().Replay.End == 256
		})
	}
	began := time.Now()
	imageFreeze(t, r)
	freeze := time.Since(began)
	began = time.Now()
	rows, err := f.manager.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	img, resources, err := r.Export()
	if err != nil {
		t.Fatal(err)
	}
	export := time.Since(began)
	began = time.Now()
	src := newImageDupSource(t, resources)
	imported, _, err := Import(cfg, img, rows, src)
	if err != nil {
		t.Fatal(err)
	}
	importTime := time.Since(began)
	if err := r.Discard(); err != nil {
		t.Fatal(err)
	}
	f.r = imported
	began = time.Now()
	f.manager.Thaw()
	if err := imported.Thaw(); err != nil {
		t.Fatal(err)
	}
	thaw := time.Since(began)
	t.Logf("calibration sample, NOT a budget claim: 8 backpressured tasks — freeze=%v export=%v import=%v thaw=%v", freeze, export, importTime, thaw)
	for _, d := range []time.Duration{freeze, export, importTime, thaw} {
		if d > 5*time.Second {
			t.Fatalf("fence step exceeded the 5 s budget: %v", d)
		}
	}
	_ = f.manager.SignalAll(syscall.SIGKILL)
}

// origin: hot-update S2 internal review protocol-V1 / §22.4 item 7(b) — a
// revocation is the ONE ownership decision that legitimately lands under the
// fence, so its instant is later than the cut's: the image must still
// import, and the importing image's Thaw carries the deferred kill out. The
// first version refused every revocation instant later than
// frozen_at_boot_ns, which turned exactly the documented (b) arm into a
// post-exec import refusal.
func TestRuntimeImageRevocationUnderFenceIsCarriedOutAfterImport(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	var clock atomic.Int64
	clock.Store(int64(time.Hour))
	f, cfg := runtimeFixtureConfig(t, 256)
	cfg.Clock = func() (int64, error) { return clock.Load(), nil }
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { f.close(t) })
	h := f.lease(t, "revoked-under-fence")
	created := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf alive; exec sleep 30"}})
	client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.waitFor(t, "running", func() bool {
		reply, err := f.request(client.req("resume"), client.inbox)
		return err == nil && reply.Phase == "running"
	})
	imageFreeze(t, r)
	clock.Add(int64(time.Second)) // the revocation instant is strictly after the cut
	if killed, err := r.Revoke(f.scope.SID, "session_destroyed"); killed != 1 || err != nil {
		t.Fatalf("revoke under the fence: %d %v", killed, err)
	}
	rows, err := f.manager.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	osPID := rows[0].OSPID
	if state := imageProcState(t, osPID); state == "" || state == "Z" {
		t.Fatalf("revoke under the fence killed the child (state %q)", state)
	}
	img, resources, err := r.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(img.Revocations) != 1 || img.Revocations[0].AtBootNS <= img.FrozenAtBootNS {
		t.Fatalf("exported revocation does not post-date the cut: %+v vs %d", img.Revocations, img.FrozenAtBootNS)
	}
	var ti TaskImage
	for _, task := range img.Tasks {
		if task.PID == created.PID {
			ti = task
		}
	}
	if ti.Revoked == "" {
		t.Fatal("exported task lost its revocation mark")
	}
	imported := imageSwitch(t, f, cfg, img, resources, rows) // Thaw carries the kill out
	// A revoked live task answers ErrRevoked until its terminal is recorded;
	// wait for the importing image's observer to record it, then drain.
	f.waitFor(t, "revoked task terminal after the switch", func() bool {
		imported.mu.Lock()
		defer imported.mu.Unlock()
		task := imported.tasks[created.PID]
		return task != nil && task.terminalAt != 0
	})
	f.drain(t, client)
	if client.terminal.Signal != int(syscall.SIGKILL) || !strings.HasPrefix(client.terminal.Error, "revoked:") || string(client.stdout) != "alive" {
		t.Fatalf("terminal after the switch: %+v stdout=%q", client.terminal, client.stdout)
	}
	if err := syscall.Kill(osPID, 0); err == nil {
		if state := imageProcState(t, osPID); state != "" && state != "Z" {
			t.Fatalf("child survived the importing image's Thaw (state %q)", state)
		}
	}
	// The revocation itself crossed: a lease issued before it cannot create.
	if _, err := f.r.Handle(f.scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: f.calls.Add(1), Op: "create", ActorFP: f.scope.ActorFP,
		Handle: &proto.ProcessHandle{Lease: h.Lease, RequestID: "after-switch"}, AttachEpoch: 1, Exec: &proto.ExecReq{Argv: []string{"/bin/true"}}}, client.inbox); !errors.Is(err, ErrRevoked) {
		t.Fatalf("pre-revocation lease after the switch: %v", err)
	}
	f.settle(t, client)
	_ = imported
}
