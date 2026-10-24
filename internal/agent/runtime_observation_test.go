//go:build linux

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

const agentRuntimeMode = "_TETHER_AGENT_RUNTIME_OBSERVATION"
const agentRuntimeParentG = "_TETHER_AGENT_RUNTIME_PARENT_G"

type agentRuntimeSample struct {
	counters        drillobserve.RuntimeCounters
	goroutines, fds int
	logs            [2]uint64
	err             error
}

type agentRuntimeObservation struct {
	lastLogs [2]uint64
	a        *Agent
	nc       *nats.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan error
	samples  chan agentRuntimeSample
	lost     atomic.Bool
	binding  drillobserve.RuntimeBinding
	last     drillobserve.RuntimeCounters
	baseline drillobserve.RuntimeCounters
	exeSHA   [32]byte
}

// The parent leak assertions cover the supervisor and fixture only. Each child
// independently settles five real exec/PTY rounds BEFORE Run or the image ends;
// reexec/process exit must never erase the product's resource-growth evidence.
// origin: hot-update S10 plan §4.1 / §5.3 / §10.2 P07 / §11.2
func TestAgentRuntimeGaugeComesFromObservedProcess(t *testing.T) {
	mode := os.Getenv(agentRuntimeMode)
	if mode != "" {
		// Business children must not inherit the test's dispatch keys. Hook is
		// installed once before Run and remains immutable until process exit.
		if err := os.Unsetenv(agentRuntimeMode); err != nil {
			t.Fatal(err)
		}
		f := newAgentRuntimeObservation(t, mode)
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseAgentRuntimeObservation(t, f, round)
		}
		settled := nextAgentRuntimeSample(t, f)
		if settled.Goroutines > f.baseline.Goroutines+2 || settled.FDs != f.baseline.FDs {
			t.Fatalf("same-generation resources grew: G %d -> %d, FD %d -> %d", f.baseline.Goroutines, settled.Goroutines, f.baseline.FDs, settled.FDs)
		}
		testharness.AssertNoGoroutineLeak(t, "observed agent same-generation exec/PTY x5", beforeG)
		testharness.AssertNoFDLeak(t, "observed agent same-generation exec/PTY x5", beforeFD, 0)
		stopAgentRuntimeObservation(t, f)
		return
	}

	// A conspicuously different parent G count makes a harness gauge dressed
	// in the child's PID/stamp fail even if all identity fields look plausible.
	parked := make(chan struct{})
	var workers sync.WaitGroup
	for range 128 {
		workers.Add(1)
		go func() { defer workers.Done(); <-parked }()
	}
	defer func() { close(parked); workers.Wait() }()
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAgentRuntimeObservation(t, nil, round)
	}
	testharness.AssertNoGoroutineLeak(t, "agent observation supervisor x5", beforeG)
	testharness.AssertNoFDLeak(t, "agent observation supervisor x5", beforeFD, 0)
}

func exerciseAgentRuntimeObservation(t *testing.T, f *agentRuntimeObservation, round int) {
	t.Helper()
	if f == nil {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAgentRuntimeGaugeComesFromObservedProcess$", "-test.count=1")
		mode := "cold"
		if round%2 != 0 {
			mode = "restored"
		}
		env := drillobserve.StripEnvironment(handoff.CleanEnvironment(os.Environ()))
		for _, entry := range env {
			if !strings.HasPrefix(entry, agentRuntimeMode+"=") && !strings.HasPrefix(entry, agentRuntimeParentG+"=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, agentRuntimeMode+"="+mode, agentRuntimeParentG+"="+strconv.Itoa(runtime.NumGoroutine()))
		out, err := testharness.CombinedOutput(cmd)
		if err != nil {
			t.Fatalf("observed agent %s: %v\n%s", mode, err, out)
		}
		if !strings.Contains(string(out), "agent-runtime-observation-complete") {
			t.Fatalf("observed agent %s omitted its completed resource/exit receipt: %s", mode, out)
		}
		return
	}
	for _, kind := range []string{"", proto.ProcessKindPTY} {
		exerciseObservedAgentTask(t, f, kind, round)
	}
}

func newAgentRuntimeObservation(t *testing.T, mode string) *agentRuntimeObservation {
	t.Helper()
	url := startNATS(t)
	ptyRegisterResponder(t, url, "")
	a := newBootAgent(t, url, "node")
	if mode == "restored" {
		// Use the production import/activation seam, not a fixture assignment to
		// imageGeneration. The source has no business resources to duplicate.
		frozen := freezeBootAgent(t, a)
		img, source := cutBootImage(t, a, frozen)
		var err error
		a, err = New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(),
			BootImage: img, RemoteFSMode: "off", RosterRefreshInterval: -1,
			Logger: testharness.SilentLog(), RegisterTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		cleanupBootAgent(t, a)
		if err := frozen.Discard(); err != nil {
			t.Fatal(err)
		}
		source.Close()
		if a.imageGeneration != 7 {
			t.Fatal("restored generation was not adopted")
		}
	} else if mode != "cold" {
		t.Fatal("invalid observation helper mode")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	ctx, cancel := context.WithCancel(t.Context()) // ctx-root: observed agent lifetime, including independent Run exit
	f := &agentRuntimeObservation{a: a, nc: nc, ctx: ctx, cancel: cancel, done: make(chan error, 1),
		samples: make(chan agentRuntimeSample, 64),
		binding: drillobserve.RuntimeBinding{Domain: "agent", InstanceID: a.instanceID, Generation: a.imageGeneration}}
	exe, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, exe)
	closeErr := exe.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("executing image hash: %v / %v", readErr, closeErr)
	}
	copy(f.exeSHA[:], hash.Sum(nil))
	var logs [2]atomic.Uint64
	drillobserve.Hook = func(e drillobserve.Event) {
		if e.Kind == "domain-live" {
			switch e.Domain {
			case "exec":
				logs[0].Store(e.Values[1])
			case "pty":
				logs[1].Store(e.Values[1])
			}
		}
		if e.Kind != drillobserve.RuntimeSampleKind {
			return
		}
		c, err := drillobserve.DecodeRuntimeSample(e)
		s := agentRuntimeSample{counters: c, err: err, goroutines: runtime.NumGoroutine(),
			fds: testharness.FDCount() - 1, logs: [2]uint64{logs[0].Load(), logs[1].Load()}}
		select {
		case f.samples <- s:
		default:
			f.lost.Store(true)
		}
	}
	go func() { f.done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-f.done:
		case <-time.After(bootTestBudget):
			t.Error("agent Run did not join")
		}
	})
	// Wait for real control readiness before fixing a resource baseline; a
	// pre-connect baseline would mistake NATS's steady owners for leaked work.
	waitFor(t, bootTestBudget, func() bool {
		a.sessCancelMu.Lock()
		defer a.sessCancelMu.Unlock()
		return a.sessCancel != nil
	})
	// Server readiness probes and imported control descriptors retire
	// asynchronously. Fix the baseline only after three unchanged live ticks;
	// otherwise their late close can masquerade as a missing settled FD.
	deadline := time.Now().Add(bootTestBudget)
	stable := 0
	for stable < 3 {
		if time.Now().After(deadline) {
			t.Fatal("agent startup resources did not stabilize")
		}
		current := nextAgentRuntimeSample(t, f)
		if current.Goroutines == f.baseline.Goroutines && current.FDs == f.baseline.FDs {
			stable++
		} else {
			stable = 1
		}
		f.baseline = current
	}
	return f
}

func nextAgentRuntimeSample(t *testing.T, f *agentRuntimeObservation) drillobserve.RuntimeCounters {
	t.Helper()
	// Discard queued earlier ticks. The accepted sample must be produced after
	// this call, not a stale pre-settlement observation dressed as fresh proof.
	for len(f.samples) != 0 {
		<-f.samples
	}
	var requested unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &requested); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(bootTestBudget)
	defer timer.Stop()
	for {
		select {
		case s := <-f.samples:
			c := s.counters
			if s.err == nil && c.AtBootNS < uint64(requested.Nano()) {
				continue
			}
			if s.err != nil || f.lost.Load() {
				t.Fatalf("runtime sample invalid or dropped: %v", s.err)
			}
			ticks, err := readStartTimeTicks(os.Getpid())
			pidns, nsErr := os.Readlink("/proc/self/ns/pid")
			scope := sha256.Sum256([]byte("agent:" + f.binding.InstanceID))
			if err != nil || nsErr != nil || c.Binding != f.binding || c.PID != uint64(os.Getpid()) ||
				c.StartTicks != uint64(ticks) || c.BootID != readBootID() || c.PIDNS != pidns || c.ExeSHA != f.exeSHA ||
				c.Unknown != drillobserve.RuntimeUnknownCounters || c.ScopeDigest != hex.EncodeToString(scope[:]) {
				t.Fatalf("runtime sample crossed process/image/generation: pid=%d generation=%d, want pid=%d generation=%d", c.PID, c.Binding.Generation, os.Getpid(), f.binding.Generation)
			}
			if c.Sequence <= f.last.Sequence || c.AtBootNS <= f.last.AtBootNS || c.Nonce == f.last.Nonce {
				t.Fatal("runtime sample replayed sequence, time or nonce")
			}
			if f.last.Sequence != 0 && c.Sequence == f.last.Sequence+1 {
				gap := time.Duration(c.AtBootNS - f.last.AtBootNS)
				if gap < 200*time.Millisecond || gap > time.Second {
					t.Fatalf("runtime sample cadence is not 250ms: %s", gap)
				}
			}
			// These are independent reads inside the observed process's hook; /proc
			// Threads and the caller's runtime cannot substitute for this Go gauge.
			if delta := int(c.Goroutines) - s.goroutines; delta < -2 || delta > 2 {
				t.Fatalf("runtime G did not come from observed process: sample=%d actual=%d", c.Goroutines, s.goroutines)
			}
			if delta := int(c.FDs) - s.fds; delta < -1 || delta > 1 {
				t.Fatalf("runtime FD count differs from observed process: sample=%d actual=%d", c.FDs, s.fds)
			}
			f.last, f.lastLogs = c, s.logs
			return c
		case <-timer.C:
			t.Fatal("agent Run did not emit a fresh runtime sample")
			return drillobserve.RuntimeCounters{}
		}
	}
}

func exerciseObservedAgentTask(t *testing.T, f *agentRuntimeObservation, kind string, round int) {
	t.Helper()
	scope := ptyTestScope(f.a)
	q := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1, Op: "probe", Kind: kind,
		ActorFP: scope.ActorFP, ClientLineage: scope.ClientLineage}
	probe, err := f.a.processRuntime.Handle(scope, q, "_TINBOX.observed")
	if err != nil || probe.Lease == nil {
		t.Fatalf("observed %s task probe: %v", kind, err)
	}
	q.Handle = &proto.ProcessHandle{Lease: *probe.Lease, RequestID: fmt.Sprintf("observed-%s-%d", kind, round)}
	q.Op, q.CallID, q.AttachEpoch = "create", 2, 1
	// The child waits for release so the periodic domain observer must see live
	// resources, then exits naturally with exact bytes and a nonzero status.
	gate := filepath.Join(t.TempDir(), "release")
	argv := []string{"/bin/sh", "-c", "printf ready; read line; printf tail; exit 23"}
	if kind == proto.ProcessKindPTY {
		argv[2] = "stty -echo; " + argv[2]
		q.Run = &proto.RunReq{Cwd: t.TempDir(), Argv: argv, ActorFP: scope.ActorFP}
	} else {
		argv = []string{"/bin/sh", "-c", "printf ready; while [ ! -e \"$1\" ]; do sleep 0.01; done; printf tail; exit 23", "observed", gate}
		q.Exec = &proto.ExecReq{Cwd: t.TempDir(), Argv: argv}
	}
	created, err := f.a.processRuntime.Handle(scope, q, "_TINBOX.observed")
	if err != nil || created.PID == "" {
		t.Fatalf("observed %s create: %v", kind, err)
	}
	q.Exec, q.Run, q.Op = nil, nil, "resume"
	waitFor(t, bootTestBudget, func() bool {
		q.CallID++
		r, err := f.a.processRuntime.Handle(scope, q, "_TINBOX.observed")
		return err == nil && r.Phase == "running" && r.Stdout != nil && string(r.Stdout.Data) == "ready"
	})
	live := nextAgentRuntimeSample(t, f)
	if live.FDs <= f.baseline.FDs {
		t.Fatal("runtime gauge missed live task descriptors")
	}
	// Domain-live is emitted immediately before each runtime sample. It must
	// report the real replay owners, not turn reservation counts into task counts.
	domain := 0
	if kind == proto.ProcessKindPTY {
		domain = 1
	}
	if f.lastLogs[domain] == 0 {
		t.Fatal("runtime callback missed live domain replay owners")
	}
	if f.a.processRuntime.ReplayBudgetUsed() == 0 {
		t.Fatal("live task has no replay reservation")
	}
	if kind == proto.ProcessKindPTY {
		q.Op, q.Input, q.CallID = "input", []byte("finish\n"), q.CallID+1
		if _, err := f.a.processRuntime.Handle(scope, q, "_TINBOX.observed"); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(gate, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	q.Op, q.Input = "resume", nil
	var terminal proto.ProcessResumeReply
	waitFor(t, bootTestBudget, func() bool {
		q.CallID++
		r, err := f.a.processRuntime.Handle(scope, q, "_TINBOX.observed")
		terminal = r
		return err == nil && r.Phase == "terminal" && r.Stdout != nil && r.Stderr != nil &&
			(r.Stdout.FIN || r.Stdout.Error != "") && r.Stderr.FIN
	})
	if terminal.ExitCode != 23 || terminal.TerminalID == "" || string(terminal.Stdout.Data) != "readytail" || len(terminal.Stderr.Data) != 0 {
		t.Fatalf("observed %s lost exact terminal/bytes: exit=%d stdout=%q", kind, terminal.ExitCode, terminal.Stdout.Data)
	}
	q.Op, q.CallID, q.TerminalID = "terminal-ack", q.CallID+1, terminal.TerminalID
	q.StdoutACK, q.StderrACK = terminal.Stdout.Offset+uint64(len(terminal.Stdout.Data)), terminal.Stderr.Offset
	q.StdoutFIN, q.StderrFIN = terminal.Stdout.FIN, true
	settled, err := f.a.processRuntime.Handle(scope, q, "_TINBOX.observed")
	if err != nil || settled.Phase != "settled" {
		t.Fatalf("observed %s settlement: %s %v", kind, settled.Phase, err)
	}
	if f.a.processRuntime.ReplayBudgetUsed() != 0 || len(f.a.workloads.Snapshot()) != 0 {
		t.Fatal("settlement retained replay or process ownership")
	}
	nextAgentRuntimeSample(t, f)
	if f.lastLogs != ([2]uint64{}) {
		t.Fatal("runtime callback retained settled replay owners")
	}
}

func stopAgentRuntimeObservation(t *testing.T, f *agentRuntimeObservation) {
	t.Helper()
	// Eviction ends Run while its parent context remains live. Binding the
	// sampler to ctx rather than watchCtx would leak it on this real exit path.
	if err := f.nc.Publish(proto.SubjSysEvents, []byte(`{"type":"agent_evicted","sid":"lab","nid":"node"}`)); err != nil {
		t.Fatal(err)
	}
	if err := f.nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-f.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		f.done <- err // cleanup still joins the same completed Run
	case <-time.After(bootTestBudget):
		t.Fatal("eviction did not end Run")
	}
	if f.ctx.Err() != nil {
		t.Fatal("fixture canceled the parent before testing Run exit")
	}
	// Permit a tick already in flight at cancellation, then require silence
	// across three full sample intervals with the process still alive.
	var stopped unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &stopped); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(750 * time.Millisecond)
	defer timer.Stop()
	quiet := false
	for !quiet {
		select {
		case sample := <-f.samples:
			if sample.err != nil || sample.counters.AtBootNS > uint64(stopped.Nano()) {
				t.Fatal("runtime observer outlived Agent.Run")
			}
		case <-timer.C:
			quiet = true
		}
	}
	f.cancel()
	fmt.Println("agent-runtime-observation-complete")
}
