package execclient

// Real: in-process NATS, private subscriptions, the public ProbePTY/Run API.
// Stubbed: agent receipts and the broker authentication stamp.
// This file does not claim real PTY, broker ACL, or same-PID exec coverage.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

type ptyTestClock struct {
	base time.Time
	ns   atomic.Int64
}

func newPTYTestClock() *ptyTestClock                  { return &ptyTestClock{base: time.Now()} }
func (c *ptyTestClock) now() time.Time                { return c.base.Add(time.Duration(c.ns.Load())) }
func (c *ptyTestClock) set(elapsed time.Duration)     { c.ns.Store(int64(elapsed)) }
func (c *ptyTestClock) advance(elapsed time.Duration) { c.ns.Add(int64(elapsed)) }

type ptyTestSource struct {
	data      []byte
	repeat    bool
	eof       bool
	readCount atomic.Int64
	readBytes atomic.Int64
	started   chan struct{}
	cancelled chan struct{}
	once      sync.Once
	startOnce sync.Once
}

func newPTYTestSource(data []byte, repeat, eof bool) *ptyTestSource {
	return &ptyTestSource{data: append([]byte(nil), data...), repeat: repeat, eof: eof,
		started: make(chan struct{}), cancelled: make(chan struct{})}
}

func (s *ptyTestSource) Read(p []byte) (int, error) {
	s.readCount.Add(1)
	s.startOnce.Do(func() { close(s.started) })
	if s.repeat {
		for i := range p {
			p[i] = byte((int(s.readBytes.Load()) + i) % 251)
		}
		s.readBytes.Add(int64(len(p)))
		return len(p), nil
	}
	if len(s.data) > 0 {
		n := copy(p, s.data)
		s.data = s.data[n:]
		s.readBytes.Add(int64(n))
		if len(s.data) == 0 && s.eof {
			return n, io.EOF
		}
		return n, nil
	}
	if s.eof {
		return 0, io.EOF
	}
	<-s.cancelled
	return 0, context.Canceled
}

func (s *ptyTestSource) Cancel() error {
	s.once.Do(func() { close(s.cancelled) })
	return nil
}

type ptyTestSink struct {
	mu        sync.Mutex
	data      bytes.Buffer
	limit     int
	fail      error
	blocked   <-chan struct{}
	started   chan struct{}
	cancelled chan struct{}
	once      sync.Once
	startOnce sync.Once
	writes    atomic.Int64
}

func newPTYTestSink(blocked <-chan struct{}) *ptyTestSink {
	return &ptyTestSink{blocked: blocked, started: make(chan struct{}), cancelled: make(chan struct{})}
}

func (s *ptyTestSink) Write(p []byte) (int, error) {
	s.writes.Add(1)
	s.startOnce.Do(func() { close(s.started) })
	if s.blocked != nil {
		select {
		case <-s.blocked:
		case <-s.cancelled:
			return 0, context.Canceled
		}
	}
	if s.limit > 0 {
		p = p[:min(len(p), s.limit)]
	}
	s.mu.Lock()
	n, _ := s.data.Write(p)
	s.mu.Unlock()
	return n, s.fail
}

func (s *ptyTestSink) Cancel() error {
	s.once.Do(func() { close(s.cancelled) })
	return nil
}

func (s *ptyTestSink) bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data.Bytes()...)
}

func probePTYFixture(t *testing.T, f *clientFixture) *PTYClient {
	t.Helper()
	type outcome struct {
		c   *PTYClient
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		c, err := ProbePTY(t.Context(), f.nc, f.subject)
		done <- outcome{c, err}
	}()
	for _, op := range []string{"probe", proto.ProcessOpPTYProbe} {
		ex := f.next(t, op)
		if ex.req.Kind != proto.ProcessKindPTY || ex.req.Run != nil || ex.req.Handle != nil {
			t.Fatalf("probe mutated business state: %+v", ex.req)
		}
		reply := f.capability(ex)
		reply.PTYVersion, reply.TaskKind = proto.PTYVersion, proto.ProcessKindPTY
		if op == proto.ProcessOpPTYProbe {
			reply.Lease.LeaseID = "lease-2"
		}
		sendClientReply(t, ex, reply)
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		t.Cleanup(func() { _ = got.c.Close() })
		return got.c
	case <-time.After(clientTestBudget):
		t.Fatal("PTY probe did not finish")
	}
	return nil
}

func startPTYFixture(t *testing.T, c *PTYClient, spec proto.RunReq, in Source, out Sink, events <-chan ControlEvent) *clientRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	r := &clientRun{done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(r.done)
		r.result, r.err = c.Run(ctx, spec, in, out, events)
	}()
	t.Cleanup(func() {
		cancel()
		_ = in.Cancel()
		_ = out.Cancel()
		select {
		case <-r.done:
		case <-time.After(clientTestBudget):
			t.Error("PTY Run did not join its workers")
		}
	})
	return r
}

func ptyStatus(ex clientExchange, applied, accepted, control uint64) proto.ProcessResumeReply {
	r := clientStatus(ex)
	r.TaskKind = proto.ProcessKindPTY
	r.Stderr = &proto.ProcessOutput{FIN: true}
	r.InputACK, r.InputAccepted, r.InputWindow = applied, accepted, upgradebudget.PTYInputBytes-(accepted-applied)
	r.ControlACK = control
	return r
}

func ptyTerminal(ex clientExchange, applied, accepted, control uint64, abort string) proto.ProcessResumeReply {
	r := ptyStatus(ex, applied, accepted, control)
	r.Phase, r.TerminalID, r.ExitCode = "terminal", "original-terminal", 23
	r.Stdout = &proto.ProcessOutput{Offset: ex.req.StdoutACK, FIN: true}
	r.InputEnded, r.InputError = true, abort
	r.InputFIN = abort == ""
	return r
}

func requirePTYHandle(t *testing.T, original clientExchange, ex clientExchange) {
	t.Helper()
	if !reflect.DeepEqual(ex.req.Handle, original.req.Handle) || ex.req.AttachEpoch != 1 || ex.msg.Reply != original.msg.Reply {
		t.Fatalf("retry changed the original task/attachment: original=%+v retry=%+v", original.req, ex.req)
	}
}

// origin: hot-update S4 plan §7.2 / T40
func TestPTYClientSchedulesKAWhileOutputBlocked(t *testing.T) {
	t.Run("zero_input_credit_never_reads_source", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		clock := newPTYTestClock()
		c.now = clock.now
		in := newPTYTestSource(nil, true, false)
		out := newPTYTestSink(make(chan struct{}))
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}, KAIntervalMS: 1000}, in, out, nil)
		create := f.next(t, "create")
		reply := ptyStatus(create, 0, 0, 0)
		reply.InputWindow = 0
		reply.Stdout = &proto.ProcessOutput{Data: []byte("blocked")}
		sendClientReply(t, create, reply)
		select {
		case <-out.started:
		case <-time.After(clientTestBudget):
			t.Fatal("output did not block")
		}
		control := uint64(0)
		for range 8 {
			clock.advance(time.Second)
			ex := f.next(t, "")
			if ex.req.ControlSeq != 0 {
				control = ex.req.ControlSeq
			}
			if ex.req.Op == proto.ProcessOpInput {
				t.Fatal("input was sent with no credit")
			}
			r := ptyStatus(ex, 0, 0, control)
			r.InputWindow = 0
			r.Stdout = &proto.ProcessOutput{Data: []byte("blocked")}
			sendClientReply(t, ex, r)
		}
		run.cancel()
		_, err := run.finish(t)
		if !errors.Is(err, context.Canceled) || in.readCount.Load() != 0 || control == 0 {
			t.Fatalf("no-credit read or missing KA premise: reads=%d control=%d err=%v", in.readCount.Load(), control, err)
		}
	})
	f := newClientFixture(t)
	c := probePTYFixture(t, f)
	clock := newPTYTestClock()
	c.now = clock.now
	blocked := make(chan struct{})
	in := newPTYTestSource(nil, true, false)
	out := newPTYTestSink(blocked)
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}, KAIntervalMS: 1000}, in, out, nil)
	create := f.next(t, "create")
	if create.req.Handle.Lease.LeaseID != "lease-2" || create.req.StderrFIN || create.req.Exec != nil {
		t.Fatalf("create did not use the path-probe lease/clean PTY payload: %+v", create.req)
	}
	output := bytes.Repeat([]byte("o"), upgradebudget.PTYBatchBytes)
	reply := ptyStatus(create, 0, 0, 0)
	reply.Stdout = &proto.ProcessOutput{Data: output}
	sendClientReply(t, create, reply)
	select {
	case <-out.started:
	case <-time.After(clientTestBudget):
		t.Fatal("stdout worker never blocked on the sink")
	}
	// Lose an ordinary ACK, then make KA due while it is in flight. A single
	// attempt must yield; the old infinite call wrapper repeats this ACK.
	lost := f.next(t, "")
	clock.advance(2 * time.Second)
	accepted, control := uint64(0), uint64(0)
	kaReceipts := 0
	requests := 0
	creditExhausted := false
	for requests < 60 && (accepted < upgradebudget.PTYInputBytes || kaReceipts < 3 || !creditExhausted) {
		ex := f.next(t, "")
		requirePTYHandle(t, create, ex)
		if ex.req.CallID == lost.req.CallID {
			t.Fatal("one lost output RPC monopolized the owner instead of yielding to KA")
		}
		if ex.req.StdoutACK != 0 || ex.req.StdoutFIN {
			t.Fatalf("blocked sink acknowledged undelivered output: %+v", ex.req)
		}
		switch ex.req.Op {
		case proto.ProcessOpInput:
			if len(ex.req.Input) > upgradebudget.PTYBatchBytes || ex.req.InputSeq != accepted {
				t.Fatalf("unbounded or noncontiguous input: %+v", ex.req)
			}
			accepted += uint64(len(ex.req.Input))
		case proto.ProcessOpKeepalive:
			if ex.req.ControlSeq != control+1 {
				t.Fatalf("KA did not use a new control sequence: got %d after %d", ex.req.ControlSeq, control)
			}
			control = ex.req.ControlSeq
			kaReceipts++
		case "ack":
		default:
			t.Fatalf("unexpected op while stdout is blocked: %s", ex.req.Op)
		}
		if accepted > upgradebudget.PTYInputBytes {
			t.Fatal("input exceeded its fixed capacity")
		}
		reply := ptyStatus(ex, 0, accepted, control)
		reply.Stdout = &proto.ProcessOutput{Data: output}
		sendClientReply(t, ex, reply)
		clock.advance(time.Second)
		requests++
		if accepted == upgradebudget.PTYInputBytes && requests >= 30 {
			creditExhausted = true
		}
	}
	if accepted != upgradebudget.PTYInputBytes || kaReceipts < 3 || !creditExhausted {
		t.Fatalf("nonempty scheduling premise failed: accepted=%d KA=%d requests=%d", accepted, kaReceipts, requests)
	}
	if got := in.readBytes.Load(); got != upgradebudget.PTYInputBytes {
		t.Fatalf("stdin read beyond exhausted credit: got %d want %d", got, upgradebudget.PTYInputBytes)
	}
	if out.writes.Load() != 1 || len(out.bytes()) != 0 {
		t.Fatalf("blocked output spawned additional batches: writes=%d bytes=%d", out.writes.Load(), len(out.bytes()))
	}
	run.cancel()
	result, err := run.finish(t)
	if !errors.Is(err, context.Canceled) || result.InputACK != 0 || result.InputAccepted != accepted || result.StdoutBytes != 0 {
		t.Fatalf("cancel/ACK accounting: %+v err=%v", result, err)
	}
}

// origin: hot-update S4 plan §7.2 / T41
func TestPTYClientRecoveryDeadlineIsAbsolute(t *testing.T) {
	t.Run("offline_watchdog_and_absolute_expiry", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		clock := newPTYTestClock()
		c.now = clock.now
		in, out := newPTYTestSource(nil, false, false), newPTYTestSink(nil)
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, in, out, nil)
		create := f.next(t, "create")
		sendClientReply(t, create, ptyStatus(create, 0, 0, 0))
		firstFailure := f.next(t, "ack")
		// The first failure arrives at t=0. Each subsequent request proves that
		// this reply was consumed before the injected clock moves again.
		refuse := func(ex clientExchange, code string) {
			sendClientReply(t, ex, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion,
				CallID: ex.req.CallID, Kind: "error", Code: code, Error: code})
		}
		refuse(firstFailure, "node_offline")
		for _, elapsed := range []time.Duration{60 * time.Second, upgradebudget.ComponentHandover, upgradebudget.PTYRecovery - time.Second} {
			ex := f.next(t, "ack")
			requirePTYHandle(t, create, ex)
			clock.set(elapsed)
			refuse(ex, "agent_upgrading")
		}
		ex := f.next(t, "ack")
		clock.set(upgradebudget.PTYRecovery)
		refuse(ex, proto.CodeProcessRecovering)
		result, err := run.finish(t)
		if err == nil || !strings.Contains(err.Error(), "recovery deadline exceeded") || !result.CreateSent ||
			result.Handle != *create.req.Handle || !strings.Contains(err.Error(), "task may have executed") {
			t.Fatalf("180s absolute recovery did not terminate: result=%+v err=%v", result, err)
		}
	})
	t.Run("same_instance_recovers_after_watchdog", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		clock := newPTYTestClock()
		c.now = clock.now
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}},
			newPTYTestSource(nil, false, false), newPTYTestSink(nil), nil)
		create := f.next(t, "create")
		sendClientReply(t, create, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: create.req.CallID,
			Kind: "error", Code: "node_offline", Error: "node_offline"})
		retry := f.next(t, "create")
		requirePTYHandle(t, create, retry)
		clock.set(upgradebudget.ComponentHandover)
		sendClientReply(t, retry, ptyStatus(retry, 0, 0, 0))
		ex := f.next(t, "ack")
		clock.set(upgradebudget.PTYRecovery + time.Second)
		sendClientReply(t, ex, ptyStatus(ex, 0, 0, 0))
		ex = f.next(t, "ack") // a valid original status ended the earlier outage
		// A real input failure remains an abort even with no outstanding
		// bytes; clean source closure alone preserves the remote exit code.
		sendClientReply(t, ex, ptyTerminal(ex, 0, 0, 0, "pty input aborted: write_error"))
		settle := f.next(t, "terminal-ack")
		r := ptyTerminal(settle, 0, 0, 0, "pty input aborted: write_error")
		r.Phase = "settled"
		sendClientReply(t, settle, r)
		result, err := run.finish(t)
		var abort *SettledError
		if !errors.As(err, &abort) || result.ExitCode != 23 || result.Handle != *create.req.Handle || result.PID != "original-process" {
			t.Fatalf("original task did not recover past 120s: %+v err=%v", result, err)
		}
	})
	t.Run("recovery_rejects_changed_instance", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		clock := newPTYTestClock()
		c.now = clock.now
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}},
			newPTYTestSource(nil, false, false), newPTYTestSink(nil), nil)
		create := f.next(t, "create")
		sendClientReply(t, create, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: create.req.CallID,
			Kind: "error", Code: "node_offline", Error: "node_offline"})
		ex := f.next(t, "create")
		requirePTYHandle(t, create, ex)
		clock.set(upgradebudget.ComponentHandover)
		reply := ptyStatus(ex, 0, 0, 0)
		reply.TargetInstance = "replacement-instance"
		sendClientReply(t, ex, reply)
		result, err := run.finish(t)
		if err == nil || !strings.Contains(err.Error(), "identity mismatch") || result.Handle != *create.req.Handle {
			t.Fatalf("foreign recovery status was accepted: %+v err=%v", result, err)
		}
	})
	t.Run("late_call_does_not_advance_input_or_output", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}},
			newPTYTestSource([]byte("abcdef"), false, false), newPTYTestSink(nil), nil)
		create := f.next(t, "create")
		sendClientReply(t, create, ptyStatus(create, 0, 0, 0))
		var input clientExchange
		for {
			ex := f.next(t, "")
			if ex.req.Op == proto.ProcessOpInput {
				input = ex
				break
			}
			sendClientReply(t, ex, ptyStatus(ex, 0, 0, 0))
		}
		// Do not answer input. A later ACK remains independent of this packet.
		current := f.next(t, "ack")
		late := ptyStatus(input, 6, 6, 0)
		late.Stdout = &proto.ProcessOutput{Data: []byte("late")}
		sendClientReply(t, current, late)
		sendClientReply(t, current, ptyStatus(current, 0, 0, 0))
		next := f.next(t, "")
		if next.req.StdoutACK != 0 {
			t.Fatal("a stale CallID advanced stdout ACK")
		}
		if next.req.Op == proto.ProcessOpInput && (next.req.InputSeq != 0 || !bytes.Equal(next.req.Input, []byte("abcdef"))) {
			t.Fatalf("a stale CallID released original input: %+v", next.req)
		}
		run.cancel()
		result, err := run.finish(t)
		if !errors.Is(err, context.Canceled) || result.InputACK != 0 || result.InputAccepted != 0 || result.StdoutBytes != 0 {
			t.Fatalf("stale reply advanced client state: result=%+v err=%v", result, err)
		}
	})
}
