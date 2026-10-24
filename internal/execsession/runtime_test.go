//go:build linux

// Real: exec.Cmd children, owned stdin/stdout/stderr pipes and bounded replay.
// Stubbed: broker authorization, NATS delivery and the agent lifecycle courier.
// No freeze/export/import or full application hot update is exercised here.
// origin: docs/reviews/hot-update-plan.md §4.2.
package execsession

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
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

const runtimeTestBudget = 10 * time.Second

type runtimeChild struct {
	logical string
	pid     int
	stdout  *os.File // observation only; Runtime is the sole closing owner
	stderr  *os.File
	inodes  [2]uint64
}

type runtimeFixture struct {
	r        *Runtime
	manager  workload.Manager
	scope    procrequest.Scope
	calls    atomic.Uint64
	spawned  atomic.Int32
	mu       sync.Mutex
	started  map[string][]proto.ExecReq
	children []runtimeChild
	exited   map[string][]int
	// startedWithoutIdentity counts Started hooks fired without the adopted
	// child's OS identity (S1 requires it to follow the real Start).
	startedWithoutIdentity int
	// control, when set, is inherited by every child as fd 3: image tests
	// gate a child's progress on a line from it instead of on sleep timing.
	control *os.File
	// warning is returned from every Start (an Output prefix on stderr);
	// refuseSpawn, when set, makes Start fail with that text before any
	// child exists (an imported image that must never spawn).
	warning     string
	refuseSpawn string
}

// runtimeImageEnvPrefix names the same-PID harness's environment keys
// (runtime_image_exec_test.go); children and the exec'd image strip them.
const runtimeImageEnvPrefix = "TETHER_TEST_RUNTIME_IMAGE_"

// Fixture switches a spec may carry in Env (never forwarded to the child
// as such — they select the adapter's shape). runtimeFixtureAdopt makes
// the adapter Adopt (not AdoptDraining) so the reaper collects the exit on
// its own; runtimeFixturePTYStdout selects the post-Start pump-setup
// failure child of runtime_failure_test.go (an aborted stderr stream).
const (
	runtimeFixtureShape     = "TETHER_RUNTIME_FIXTURE_SHAPE"
	runtimeFixtureAdopt     = "adopt"
	runtimeFixturePTYStdout = "pty-stdout"
)

func runtimeScope() procrequest.Scope {
	digest := sha256.Sum256([]byte("authenticated runtime actor"))
	return procrequest.Scope{SID: "sessionA", NID: "nodeA", ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "clientA"}
}

func newRuntimeFixture(t *testing.T, capacity int64) *runtimeFixture {
	t.Helper()
	f, cfg := runtimeFixtureConfig(t, capacity)
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { f.close(t) })
	return f
}

// runtimeFixtureConfig builds the fixture and the adapter Config without
// constructing a runtime, so that New and Import share one adapter (the
// image tests import into the same fixture they exported from).
func runtimeFixtureConfig(t *testing.T, capacity int64) (*runtimeFixture, Config) {
	t.Helper()
	dir := os.Getenv(runtimeImageWork)
	if dir == "" {
		dir = t.TempDir()
	}

	f := &runtimeFixture{scope: runtimeScope(), started: make(map[string][]proto.ExecReq), exited: make(map[string][]int)}
	// origin: hot-update S5 PROC-LEAK. Export/discard deliberately leaves
	// tasks alive; the fixture still owns every group it started.
	t.Cleanup(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, child := range f.children {
			_ = syscall.Kill(-child.pid, syscall.SIGKILL)
		}
	})
	cfg := Config{
		Instance: "original-runtime", Manager: &f.manager, ReplayCapacity: capacity,
		Started: func(pid string, spec proto.ExecReq, id workload.Identity) {
			f.mu.Lock()
			if id.OSPID <= 0 || id.ID != pid {
				// The S1 contract: Started fires after the real Start, with the
				// adopted child's identity in hand.
				f.startedWithoutIdentity++
			}
			f.started[pid] = append(f.started[pid], spec)
			f.mu.Unlock()
		},
		Exited: func(pid string, code int) { f.mu.Lock(); f.exited[pid] = append(f.exited[pid], code); f.mu.Unlock() },
	}
	cfg.Start = func(pid string, spec *proto.ExecReq) (*workload.Stdio, string, error) {
		if spec.Cwd == "" {
			copy := *spec
			copy.Cwd = dir
			spec = &copy
		}
		shape := spec.Env[runtimeFixtureShape]
		if shape == runtimeFixturePTYStdout {
			stdio, err := failureStartPTYStdoutChild(&f.manager, pid, spec)
			if err == nil {
				f.spawned.Add(1)
				if row, ok := f.manager.Lookup(pid); ok {
					group := row.OSPID
					t.Cleanup(func() { _ = syscall.Kill(-group, syscall.SIGKILL) })
				}
			}
			return stdio, "", err
		}
		stdio, err := workload.NewStdio()
		if err != nil {
			return nil, "", err
		}
		cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
		cmd.Dir, cmd.Env = spec.Cwd, runtimeChildEnvironment()
		if cmd.Dir == "" {
			cmd.Dir = dir
		}
		for k, v := range spec.Env {
			if k == runtimeFixtureShape {
				continue
			}
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if f.control != nil {
			cmd.ExtraFiles = []*os.File{f.control}
		}
		stdio.Bind(cmd)
		if f.refuseSpawn != "" {
			stdio.Close()
			return nil, "", errors.New(f.refuseSpawn)
		}
		if err := cmd.Start(); err != nil {
			stdio.Close()
			return nil, "", err
		}
		// Record ownership before adoption or any assertion can fail. The outer
		// same-PID harness retains this receipt when exec erases Cleanup callbacks.
		if work := os.Getenv(runtimeImageWork); work != "" {
			if err := os.WriteFile(filepath.Join(work, fmt.Sprintf("task-group-%d", cmd.Process.Pid)), nil, 0600); err != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = cmd.Wait()
				stdio.Close()
				return nil, "", err
			}
		}
		f.spawned.Add(1)
		stdio.CloseChild()
		identity := workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now().UTC()}
		adopt := f.manager.AdoptDraining
		if shape == runtimeFixtureAdopt {
			adopt = f.manager.Adopt
		}
		if err := adopt(identity, cmd.Process); err != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			stdio.Close()
			return nil, "", err
		}
		child := runtimeChild{logical: pid, pid: cmd.Process.Pid, stdout: stdio.Stdout, stderr: stdio.Stderr}
		for i, file := range []*os.File{stdio.Stdout, stdio.Stderr} {
			inode, err := runtimePipeInode(file)
			if err != nil {
				_ = f.manager.Signal(pid, syscall.SIGKILL)
				_ = f.manager.AllowReap(pid)
				_, _ = f.manager.Wait(pid)
				_ = f.manager.Forget(pid)
				stdio.Close()
				return nil, "", err
			}
			child.inodes[i] = inode
		}
		f.mu.Lock()
		f.children = append(f.children, child)
		f.mu.Unlock()
		return stdio, f.warning, nil
	}
	return f, cfg
}

// runtimeChildEnvironment is the environment every fixture child inherits:
// the process environment minus the handoff dispatch and minus this
// package's same-PID harness keys, so a business child can never see (or
// prove it saw) the harness's identity — the child half of the two-sided
// stripping rule (S2 plan exit #6).
func runtimeChildEnvironment() []string {
	env := handoff.CleanEnvironment(os.Environ())
	out := env[:0]
	for _, entry := range env {
		if strings.HasPrefix(entry, runtimeImageEnvPrefix) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func (f *runtimeFixture) close(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestBudget)
	defer cancel()
	if err := f.r.Close(ctx); err != nil {
		t.Errorf("close real runtime: %v", err)
	}
}

func runtimePipeInode(file *os.File) (uint64, error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return 0, err
	}
	var stat unix.Stat_t
	var statErr error
	err = raw.Control(func(fd uintptr) { statErr = unix.Fstat(int(fd), &stat) })
	return stat.Ino, errors.Join(err, statErr)
}

func (f *runtimeFixture) request(req proto.ProcessResumeReq, inbox string) (proto.ProcessResumeReply, error) {
	req.Version, req.CallID, req.ActorFP = proto.ProcessResumeVersion, f.calls.Add(1), f.scope.ActorFP
	reply, err := f.r.Handle(f.scope, req, inbox)
	if reply.CallID != req.CallID || reply.Version != proto.ProcessResumeVersion || reply.TargetInstance != "original-runtime" {
		return reply, fmt.Errorf("reply identity mismatch: version=%d call=%d instance=%q", reply.Version, reply.CallID, reply.TargetInstance)
	}
	return reply, err
}

func (f *runtimeFixture) lease(t *testing.T, requestID string) proto.ProcessHandle {
	t.Helper()
	reply, err := f.request(proto.ProcessResumeReq{Op: "probe", ClientLineage: f.scope.ClientLineage}, "_TINBOX.actor.probe")
	if err != nil || reply.Kind != "capability" || reply.Lease == nil {
		t.Fatalf("probe: kind=%q hasLease=%t err=%v", reply.Kind, reply.Lease != nil, err)
	}
	return proto.ProcessHandle{Lease: *reply.Lease, RequestID: requestID}
}

func (f *runtimeFixture) create(t *testing.T, handle proto.ProcessHandle, spec proto.ExecReq) proto.ProcessResumeReply {
	t.Helper()
	reply, err := f.request(proto.ProcessResumeReq{Op: "create", Handle: &handle, AttachEpoch: 1, Exec: &spec}, "_TINBOX.actor.original")
	if err != nil || reply.PID == "" {
		t.Fatalf("create: phase=%q pid=%q err=%v", reply.Phase, reply.PID, err)
	}
	return reply
}

func (f *runtimeFixture) waitFor(t *testing.T, label string, ready func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), runtimeTestBudget)
	defer cancel()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", label, ctx.Err())
		}
	}
}

type runtimeClient struct {
	handle               proto.ProcessHandle
	epoch                uint64
	inbox                string
	stdout, stderr       []byte
	stdoutFIN, stderrFIN bool
	terminal             proto.ProcessResumeReply
}

func (c *runtimeClient) req(op string) proto.ProcessResumeReq {
	return proto.ProcessResumeReq{Op: op, Handle: &c.handle, AttachEpoch: c.epoch, StdoutACK: uint64(len(c.stdout)), StderrACK: uint64(len(c.stderr)), StdoutFIN: c.stdoutFIN, StderrFIN: c.stderrFIN}
}

func (c *runtimeClient) accept(t *testing.T, reply proto.ProcessResumeReply) {
	t.Helper()
	for i, batch := range []*proto.ProcessOutput{reply.Stdout, reply.Stderr} {
		if batch == nil {
			continue
		}
		bytes, fin := &c.stdout, &c.stdoutFIN
		if i == 1 {
			bytes, fin = &c.stderr, &c.stderrFIN
		}
		// The success mainline never sees an aborted stream, so any Error here
		// is fatal; the aborted contract has its own tests in runtime_failure_test.go.
		if batch.Offset != uint64(len(*bytes)) || len(batch.Data) > 32<<10 || batch.Error != "" {
			t.Fatalf("stream %d: offset=%d want=%d bytes=%d error=%q", i, batch.Offset, len(*bytes), len(batch.Data), batch.Error)
		}
		if *fin && (len(batch.Data) != 0 || !batch.FIN) {
			t.Fatalf("stream %d advanced after final receipt", i)
		}
		*bytes = append(*bytes, batch.Data...)
		*fin = batch.FIN
	}
	if reply.Phase == "terminal" {
		c.terminal = reply
	}
}

func (f *runtimeFixture) drain(t *testing.T, client *runtimeClient) {
	t.Helper()
	f.waitFor(t, "original streams and exit", func() bool {
		reply, err := f.request(client.req("resume"), client.inbox)
		if err != nil {
			t.Fatalf("resume original process: %v", err)
		}
		client.accept(t, reply)
		return client.stdoutFIN && client.stderrFIN && client.terminal.TerminalID != ""
	})
}

func (f *runtimeFixture) settle(t *testing.T, client *runtimeClient) {
	t.Helper()
	req := client.req("terminal-ack")
	req.TerminalID = client.terminal.TerminalID
	for i := 0; i < 2; i++ {
		reply, err := f.request(req, client.inbox)
		if err != nil || reply.Phase != "settled" || reply.TerminalID != req.TerminalID {
			t.Fatalf("terminal ACK/retry: phase=%q terminal=%q err=%v", reply.Phase, reply.TerminalID, err)
		}
	}
}

func (f *runtimeFixture) requireLifecycle(t *testing.T, pid string, exit int) runtimeChild {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.spawned.Load() != 1 || len(f.children) != 1 || f.children[0].logical != pid {
		t.Fatalf("duplicate/mismatched real children: physical=%d adopted=%d wanted-task=%q", f.spawned.Load(), len(f.children), pid)
	}
	if len(f.started) != 1 || len(f.started[pid]) != 1 || f.started[pid][0].ActorFP != f.scope.ActorFP || f.startedWithoutIdentity != 0 {
		t.Fatalf("incorrect lifecycle creation receipts: tasks=%d starts=%d without-identity=%d", len(f.started), len(f.started[pid]), f.startedWithoutIdentity)
	}
	if codes := f.exited[pid]; len(codes) != 1 || codes[0] != exit {
		t.Fatalf("lifecycle exit receipt: %v want [%d]", codes, exit)
	}
	return f.children[0]
}

func runtimePayload(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte((i*31 + i/17) % 251)
	}
	return data
}

func TestRuntimeDuplicateCreateKeepsOneProcessAndExactStreams(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newRuntimeFixture(t, 2048)
	h := f.lease(t, "originalRequest")
	input := runtimePayload(48 << 10)
	spec := proto.ExecReq{Argv: []string{"/bin/sh", "-c", "cat; printf 'stdout-tail'; printf 'stderr-tail' >&2; exit 23"}, Stdin: input}
	first := f.create(t, h, spec)
	// Discard the first response as if its connection vanished. All retries
	// use the persisted original handle and never choose a replacement lease.
	const retries = 16
	var wg sync.WaitGroup
	results := make(chan error, retries)
	for i := 0; i < retries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, err := f.request(proto.ProcessResumeReq{Op: "create", Handle: &h, AttachEpoch: 1, Exec: &spec}, "_TINBOX.actor.original")
			if err == nil && reply.PID != first.PID {
				err = fmt.Errorf("retry PID=%q original=%q", reply.PID, first.PID)
			}
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.drain(t, client)
	wantOut := append(append([]byte(nil), input...), []byte("stdout-tail")...)
	if !bytes.Equal(client.stdout, wantOut) || !bytes.Equal(client.stderr, []byte("stderr-tail")) || client.terminal.ExitCode != 23 || client.terminal.Signal != 0 || client.terminal.Error != "" {
		t.Fatalf("original execution bytes/result: stdout=%d want=%d stderr=%q exit=%d signal=%d error=%q", len(client.stdout), len(wantOut), client.stderr, client.terminal.ExitCode, client.terminal.Signal, client.terminal.Error)
	}
	f.requireLifecycle(t, first.PID, 23)
	f.settle(t, client)
	if rows := f.manager.Snapshot(); len(rows) != 0 {
		t.Fatalf("settled execution retained process owner: %d", len(rows))
	}
}

func TestRuntimeDisconnectedClientResumesOriginalBackpressuredPipes(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	const capacity = 1024
	f := newRuntimeFixture(t, capacity)
	h := f.lease(t, "disconnectedRequest")
	input := runtimePayload(256 << 10)
	first := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "cat; printf 'original-stderr' >&2; exit 23"}, Stdin: input})
	f.waitFor(t, "real output replay to fill without client ACK", func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		task := f.r.tasks[first.PID]
		return task != nil && task.output[0] != nil && task.logs[1].Snapshot().End-task.logs[1].Snapshot().Base == capacity
	})
	f.mu.Lock()
	if len(f.children) != 1 {
		f.mu.Unlock()
		t.Fatal("original physical child missing")
	}
	original := f.children[0]
	f.mu.Unlock()
	if err := syscall.Kill(original.pid, 0); err != nil {
		t.Fatalf("original child was lost while client was absent: %v", err)
	}
	f.r.mu.Lock()
	task := f.r.tasks[first.PID]
	inputState := task.input.Snapshot()
	if inputState.Replay.Base == inputState.Replay.End {
		f.r.mu.Unlock()
		t.Fatal("fixture did not exert real downstream backpressure on original input")
	}
	f.r.mu.Unlock()
	// The first newly attached response is also lost; neither its bytes nor
	// its absence acknowledges the retained prefix.
	req := proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 2}
	lost, err := f.request(req, "_TINBOX.actor.reconnected")
	if err != nil || lost.PID != first.PID || lost.Stdout == nil || len(lost.Stdout.Data) != capacity {
		t.Fatalf("first replay after disconnection: pid=%q stdout=%v err=%v", lost.PID, lost.Stdout, err)
	}
	retry, err := f.request(req, "_TINBOX.actor.reconnected")
	if err != nil || retry.Stdout == nil || retry.Stdout.Offset != lost.Stdout.Offset || !bytes.Equal(retry.Stdout.Data, lost.Stdout.Data) {
		t.Fatalf("lost replay response consumed original bytes: err=%v", err)
	}
	client := &runtimeClient{handle: h, epoch: 2, inbox: "_TINBOX.actor.reconnected"}
	f.drain(t, client)
	if !bytes.Equal(client.stdout, input) || !bytes.Equal(client.stderr, []byte("original-stderr")) || client.terminal.ExitCode != 23 {
		t.Fatalf("resumed original execution: stdout=%d want=%d stderr=%q exit=%d", len(client.stdout), len(input), client.stderr, client.terminal.ExitCode)
	}
	child := f.requireLifecycle(t, first.PID, 23)
	if child.pid != original.pid {
		t.Fatalf("reconnected execution respawned: pid=%d original=%d", child.pid, original.pid)
	}
	for i, file := range []*os.File{child.stdout, child.stderr} {
		inode, err := runtimePipeInode(file)
		if err != nil || inode != child.inodes[i] {
			t.Fatalf("original output pipe %d replaced/closed before settlement: inode=%d want=%d err=%v", i, inode, child.inodes[i], err)
		}
	}
	f.settle(t, client)
}

func TestRuntimeRejectsCrossActorStaleAttachmentAndInvalidACK(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newRuntimeFixture(t, 128)
	h := f.lease(t, "ackRequest")
	first := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf 'stdout'; printf 'stderr' >&2; exit 23"}})
	var terminal proto.ProcessResumeReply
	f.waitFor(t, "retained terminal before output ACK", func() bool {
		reply, err := f.request(proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 2}, "_TINBOX.actor.current")
		if err != nil {
			t.Fatal(err)
		}
		terminal = reply
		return reply.Phase == "terminal" && reply.Stdout != nil && reply.Stderr != nil && reply.Stdout.FIN && reply.Stderr.FIN
	})
	for _, field := range []string{"actor", "session", "node", "lineage"} {
		wrong := f.scope
		switch field {
		case "actor":
			digest := sha256.Sum256([]byte("other actor"))
			wrong.ActorFP = "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:])
		case "session":
			wrong.SID += "other"
		case "node":
			wrong.NID += "other"
		case "lineage":
			wrong.ClientLineage += "other"
		}
		req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: f.calls.Add(1), ActorFP: wrong.ActorFP, Op: "resume", Handle: &h, AttachEpoch: 3}
		if _, err := f.r.Handle(wrong, req, "_TINBOX.other.reply"); !errors.Is(err, procrequest.ErrIdentity) {
			t.Fatalf("%s stole output ownership: %v", field, err)
		}
	}
	if _, err := f.request(proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1}, "_TINBOX.actor.original"); !errors.Is(err, procrequest.ErrStale) {
		t.Fatalf("old attachment reclaimed ownership: %v", err)
	}
	bad := proto.ProcessResumeReq{Op: "ack", Handle: &h, AttachEpoch: 2, StdoutACK: 3, StderrACK: uint64(len(terminal.Stderr.Data) + 1)}
	if _, err := f.request(bad, "_TINBOX.actor.current"); !errors.Is(err, workload.ErrReplayOffset) {
		t.Fatalf("future stderr ACK accepted: %v", err)
	}
	unconsumed, err := f.request(proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 2}, "_TINBOX.actor.current")
	if err != nil || unconsumed.Stdout == nil || unconsumed.Stderr == nil || unconsumed.Stdout.Offset != 0 || unconsumed.Stderr.Offset != 0 || !bytes.Equal(unconsumed.Stdout.Data, []byte("stdout")) || !bytes.Equal(unconsumed.Stderr.Data, []byte("stderr")) {
		t.Fatalf("invalid dual-stream ACK partially released bytes: err=%v", err)
	}
	if _, err := f.request(proto.ProcessResumeReq{Op: "terminal-ack", Handle: &h, AttachEpoch: 2, TerminalID: terminal.TerminalID}, "_TINBOX.actor.current"); !errors.Is(err, procrequest.ErrConflict) {
		t.Fatalf("settled without output FINs: %v", err)
	}
	client := &runtimeClient{handle: h, epoch: 2, inbox: "_TINBOX.actor.current"}
	f.drain(t, client)
	badTerminal := client.req("terminal-ack")
	badTerminal.TerminalID = "wrongTerminal"
	if _, err := f.request(badTerminal, client.inbox); !errors.Is(err, procrequest.ErrConflict) {
		t.Fatalf("wrong terminal ID settled result: %v", err)
	}
	still, err := f.r.registry.Lookup(f.scope, h)
	if err != nil || still.Phase != "terminal" {
		t.Fatalf("failed terminal ACK retired responsibility: phase=%q err=%v", still.Phase, err)
	}
	f.requireLifecycle(t, first.PID, 23)
	f.settle(t, client)
}

func TestRuntimeRepeatedExecutionReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseRuntimeExecution(t)
		// The post-start setup failure retires an aborted stream instead of a
		// clean pair of EOFs; both shapes must return every pump and memfd.
		// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T10
		exerciseRuntimeSetupFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "reliable-exec-runtime", beforeG)
	testharness.AssertNoFDLeak(t, "reliable-exec-runtime", beforeFD, 0)
}

func exerciseRuntimeExecution(t *testing.T) {
	t.Helper()
	f := newRuntimeFixture(t, 256)
	h := f.lease(t, "cycleRequest")
	created := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "cat; printf 'err' >&2; exit 23"}, Stdin: []byte("original-input")})
	client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.drain(t, client)
	if !bytes.Equal(client.stdout, []byte("original-input")) || !bytes.Equal(client.stderr, []byte("err")) || client.terminal.ExitCode != 23 {
		t.Fatal("cycle did not complete the original bytes and terminal")
	}
	f.requireLifecycle(t, created.PID, 23)
	f.settle(t, client)
	f.close(t)
	if rows := f.manager.Snapshot(); len(rows) != 0 {
		t.Fatalf("cycle retained %d process owners", len(rows))
	}
	f.r.mu.Lock()
	remaining := len(f.r.tasks)
	f.r.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("cycle retained %d stream owners", remaining)
	}
}
