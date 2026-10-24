//go:build linux

package execclient

// Real agent.Run, forwarded handler, PTY child, replay syscalls, NATS and
// original PTYClient.Run. Only register authority and the broker scope stamp
// are supplied by the fixture; replies come directly from the agent.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

type replayInputSource struct {
	stages  chan []byte
	current []byte
	cancel  chan struct{}
	once    sync.Once
}

func (s *replayInputSource) Read(p []byte) (int, error) {
	if len(s.current) == 0 {
		select {
		case s.current = <-s.stages:
		case <-s.cancel:
			return 0, context.Canceled
		}
	}
	n := copy(p, s.current)
	s.current = s.current[n:]
	return n, nil
}
func (s *replayInputSource) Cancel() error { s.once.Do(func() { close(s.cancel) }); return nil }

// origin: hot-update S4 internal review rv-eof rv-eof-2
func TestPTYReplayPrefixAbortDrainsThroughAgentAndSettles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseReplayAbortThroughAgent(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "agent replay abort settlement", beforeG)
	testharness.AssertNoFDLeak(t, "agent replay abort settlement", beforeFD, 1)
}

func exerciseReplayAbortThroughAgent(t *testing.T) {
	t.Helper()
	f := newClientFixture(t)
	// An empty real image exposes its imported Runtime for passive budget and
	// replay observations. It carries no task to replace the original ctl's run.
	var manager workload.Manager
	rt, err := execsession.New(execsession.Config{Instance: "replay-agent", Manager: &manager,
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {},
		Start: func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
			return nil, "", errors.New("unexpected exec spawn")
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.Thaw(); _ = rt.Close(context.WithoutCancel(t.Context())) })
	cut, err := agentimage.Freeze(agentimage.Owners{Runtime: rt, Manager: &manager, Lineage: agentimage.Lineage{Generation: 1}}, time.Now().Add(clientTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	source := handofftest.DupResources(t, nil)
	boot := agentboot.FromCut(cut.Bundle(), nil, source, nil)
	a, err := agent.New(agent.Config{NATSURL: f.server.ClientURL(), SID: "lab", NID: "node", Home: t.TempDir(),
		Logger: testharness.SilentLog(), RemoteFSMode: "off", RosterRefreshInterval: -1, HeartbeatInterval: 10 * time.Millisecond,
		RegisterTimeout: time.Second, BootImage: boot})
	if err != nil {
		t.Fatal(err)
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	var readyOnce sync.Once
	if _, err := f.responder.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.register.req", func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = m.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.responder.Subscribe(proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat", func(*nats.Msg) { readyOnce.Do(func() { close(ready) }) }); err != nil {
		t.Fatal(err)
	}
	replies, err := f.responder.SubscribeSync(auth.InboxPrefixFor(f.actor) + ".>")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.responder.FlushTimeout(clientTestBudget); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-agentDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(clientTestBudget):
			t.Error("agent failed to join")
		}
	})
	select {
	case <-ready:
	case <-time.After(clientTestBudget):
		t.Fatal("agent subscriptions not ready")
	}
	imported := boot.Imported().Runtime
	type probeResult struct {
		c   *PTYClient
		err error
	}
	probeDone := make(chan probeResult, 1)
	go func() { c, err := ProbePTY(t.Context(), f.nc, f.subject); probeDone <- probeResult{c, err} }()
	forward := func(ex clientExchange) proto.ProcessResumeReply {
		req := ex.req
		req.ActorFP = f.fp // The fixture supplies only the broker's authenticated stamp.
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.responder.PublishRequest(proto.SubjCmdForwarded("lab", "node", proto.ProcessResumeVerb), ex.msg.Reply, body); err != nil {
			t.Fatal(err)
		}
		msg, err := replies.NextMsg(clientTestBudget)
		if err != nil {
			t.Fatal(err)
		}
		var reply proto.ProcessResumeReply
		if err := json.Unmarshal(msg.Data, &reply); err != nil || reply.CallID != req.CallID {
			t.Fatalf("agent wire reply: %+v %v", reply, err)
		}
		return reply
	}
	for _, op := range []string{"probe", proto.ProcessOpPTYProbe} {
		reply := forward(f.next(t, op))
		if reply.Kind != "capability" || reply.PTYVersion != proto.PTYVersion {
			t.Fatalf("agent capability: %+v", reply)
		}
	}
	var c *PTYClient
	select {
	case p := <-probeDone:
		if p.err != nil {
			t.Fatal(p.err)
		}
		c = p.c
	case <-time.After(clientTestBudget):
		t.Fatal("probe failed to join")
	}
	t.Cleanup(func() { _ = c.Close() })
	in := &replayInputSource{stages: make(chan []byte, 2), cancel: make(chan struct{})}
	out := newPTYTestSink(nil)
	release := filepath.Join(t.TempDir(), "release")
	script := `import os,sys,time,tty
assert not os.environ.get('_TETHER_HANDOFF')
tty.setraw(0)
os.write(1,b'START')
left=int(sys.argv[1])
while left:
 data=os.read(0,min(left,32768))
 assert data and data==b'a'*len(data)
 left-=len(data)
os.write(1,b'READY')
while not os.path.exists(sys.argv[2]): time.sleep(.002)
os.write(1,b'TAIL')
`
	const prior = upgradebudget.PTYInputBytes - 2
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"python3", "-c", script, fmt.Sprint(prior), release}}, in, out, nil)
	initialSent, faulted := false, false
	var originalPID, terminalID string
	restore := func() {}
	defer func() { restore() }()
	for {
		ex := f.next(t, "")
		reply := forward(ex)
		if reply.Kind != "status" {
			t.Fatalf("committed run became refusal on %s: %+v", ex.req.Op, reply)
		}
		if originalPID == "" {
			originalPID = reply.PID
		}
		if reply.PID != originalPID || reply.TaskKind != proto.ProcessKindPTY || reply.Stderr == nil || reply.Stderr.Offset != 0 || len(reply.Stderr.Data) != 0 || !reply.Stderr.FIN {
			t.Fatalf("agent lost original task/merged envelope: %+v", reply)
		}
		if !initialSent && reply.Stdout != nil && bytes.Contains(reply.Stdout.Data, []byte("START")) {
			in.stages <- bytes.Repeat([]byte("a"), prior)
			initialSent = true
		}
		if !faulted && reply.InputACK == prior && bytes.Contains(out.bytes(), []byte("READY")) {
			if err := imported.Freeze(time.Now().Add(clientTestBudget)); err != nil {
				t.Fatal(err)
			}
			image, resources, err := imported.Export()
			if err != nil {
				t.Fatal(err)
			}
			if len(image.Tasks) != 1 || len(resources) != 1 || image.Tasks[0].PTY.Input.Base != prior || image.Tasks[0].PTY.Input.End != prior {
				t.Fatal("ring wrap premise missing")
			}
			restore = clientReplayPrefixFault(t, resources[0].Files["input-replay"])
			if err := imported.Thaw(); err != nil {
				t.Fatal(err)
			}
			in.stages <- []byte("pending\n")
			faulted = true
		}
		if faulted && ex.req.Op == proto.ProcessOpInput && ex.req.InputSeq == prior {
			restore()
			if !reply.InputEnded || reply.InputError != "pty input aborted: replay_error" || reply.InputACK != prior || reply.InputAccepted != prior+2 || reply.Phase != "running" {
				t.Fatalf("actual replay prefix abort lost its drainable status: %+v", reply)
			}
			if err := os.WriteFile(release, []byte("exit"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if reply.Phase == "terminal" {
			if reply.ExitCode != 0 || reply.TerminalID == "" {
				t.Fatal("true rc0 terminal missing")
			}
			terminalID = reply.TerminalID
		}
		if ex.req.Op == "terminal-ack" {
			if !faulted || terminalID == "" || ex.req.TerminalID != terminalID || !ex.req.StdoutFIN || !ex.req.StderrFIN || ex.req.StdoutACK != uint64(len("STARTREADYTAIL")) || reply.Phase != "settled" {
				t.Fatalf("original client did not settle true terminal: %+v %+v", ex.req, reply)
			}
			break
		}
	}
	result, err := run.finish(t)
	var aborted *SettledError
	if !errors.As(err, &aborted) || result.InputACK != prior || result.InputAccepted != prior+2 || result.InputAbort != "pty input aborted: replay_error" || result.ExitCode != 0 || result.PID != originalPID || string(out.bytes()) != "STARTREADYTAIL" || imported.ReplayBudgetUsed() != 0 {
		t.Fatalf("abort did not drain/settle/release: %+v err=%v output=%q budget=%d", result, err, out.bytes(), imported.ReplayBudgetUsed())
	}
}

// origin: hot-update S4 plan §3.2 / T12 — first wrapped pwrite succeeds in a
// MAP_SHARED alias of the real replay; the second hits unmapped address zero.
func clientReplayPrefixFault(t *testing.T, file *os.File) func() {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	target, backup := -1, -1
	var inner error
	if err := raw.Control(func(fd uintptr) { target = int(fd); backup, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || inner != nil {
		t.Fatalf("backup: %v %v", err, inner)
	}
	page := uintptr(os.Getpagesize())
	base := uintptr(upgradebudget.PTYInputBytes) - page
	addr, _, errno := syscall.Syscall6(syscall.SYS_MMAP, base, page, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_FIXED_NOREPLACE, uintptr(backup), base)
	if errno != 0 || addr != base {
		t.Fatalf("alias map: %v", errno)
	}
	memory, err := os.OpenFile("/proc/self/mem", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = memory.Close() }()
	memRaw, err := memory.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := memRaw.Control(func(fd uintptr) { inner = unix.Dup3(int(fd), target, unix.O_CLOEXEC) }); err != nil || inner != nil {
		t.Fatalf("fault install: %v %v", err, inner)
	}
	return sync.OnceFunc(func() {
		if err := unix.Dup3(backup, target, unix.O_CLOEXEC); err != nil {
			t.Error(err)
		}
		_ = unix.Close(backup)
		if _, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, addr, page, 0); errno != 0 {
			t.Error(errno)
		}
	})
}
