package execclient

// Real: embedded NATS and the reliable PTY client's cancellation/retry loops.
// Stubbed: agent input/control/terminal receipts; no kernel PTY is allocated.
// Resource claims here cover local workers, subscriptions, and NATS sockets.

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S4 plan §7.2 / §8.4 — five real cancel/retry lifecycles.
func TestPTYClientRepeatedCancelAndRetryReleasesResources(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for range 5 {
		t.Run("cancel_retry", func(t *testing.T) { exercisePTYClientCancelAndRetry(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "PTY client cancel/retry", beforeG)
	// Each round closes both connections and waits for server shutdown. One FD
	// allows only unrelated process-wide netpoller noise, not a socket per run.
	testharness.AssertNoFDLeak(t, "PTY client cancel/retry", beforeFD, 1)
}

func exercisePTYClientCancelAndRetry(t *testing.T) {
	t.Helper()
	f := newClientFixture(t)
	c := probePTYFixture(t, f)
	blocked := make(chan struct{})
	in, out := newPTYTestSource(nil, false, false), newPTYTestSink(blocked)
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, in, out, nil)
	create := f.next(t, "create")
	// Lose create's reply: the retry must preserve both the handle and intent.
	retry := f.next(t, "create")
	requirePTYHandle(t, create, retry)
	if !reflect.DeepEqual(create.req.Run, retry.req.Run) || retry.req.CallID == create.req.CallID {
		t.Fatalf("create retry changed intent or reused its stale correlation: %+v", retry.req)
	}
	reply := ptyStatus(retry, 0, 0, 0)
	reply.Stdout = &proto.ProcessOutput{Data: []byte("blocked")}
	sendClientReply(t, retry, reply)
	// Both waits are real worker handshakes, not readiness sleeps.
	select {
	case <-out.started:
	case <-time.After(clientTestBudget):
		t.Fatal("output worker did not enter blocked Write")
	}
	ex := f.next(t, "ack")
	sendClientReply(t, ex, ptyStatus(ex, 0, 0, 0))
	select {
	case <-in.started:
	case <-time.After(clientTestBudget):
		t.Fatal("input worker did not enter blocked Read")
	}
	// Concurrent Close must cancel, wake, and join both workers. Repeat Close
	// after return to prove subscription ownership is settled and idempotent.
	closed := make(chan struct{})
	go func() { _ = c.Close(); close(closed) }()
	result, err := run.finish(t)
	select {
	case <-closed:
	case <-time.After(clientTestBudget):
		t.Fatal("Close did not join Run")
	}
	if !errors.Is(err, context.Canceled) || !result.CreateSent || result.Handle != *create.req.Handle ||
		result.StdoutBytes != 0 || len(out.bytes()) != 0 {
		t.Fatalf("cancel lost ownership/accounting: %+v err=%v", result, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if f.nc.NumSubscriptions() != 0 {
		t.Fatalf("private reply subscription leaked: %d", f.nc.NumSubscriptions())
	}
	if f.nc.IsClosed() {
		t.Fatal("Close closed the caller's shared NATS connection")
	}
	if _, err := c.Run(t.Context(), proto.RunReq{}, in, out, nil); err == nil {
		t.Fatal("a cancelled client created a second task")
	}
}

// origin: hot-update S4 plan §3.2 / §7.2 — Q never releases byte responsibility.
func TestPTYClientRetainsAcceptedInputUntilAppliedAndSettlesAbort(t *testing.T) {
	// origin: hot-update S4 plan §3.2 / T37. Clean remote source closure
	// differs from lost delivery; every other cause still reports an abort.
	for _, tc := range []struct {
		name, cause string
		a, q, sent  uint64
		data        string
		clean       bool
	}{
		{"source_end_applied", "source_ended", 6, 6, 6, "", true},
		{"source_end_unapplied", "source_ended", 2, 6, 6, "cdef", false},
		{"source_end_unaccepted", "source_ended", 2, 2, 6, "cdef", false},
		{"source_end_unsent", "source_ended", 2, 2, 2, "cdef", false},
		{"hangup_applied", "hangup", 6, 6, 6, "", false},
		{"write_error_applied", "write_error", 6, 6, 6, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ptyRun{input: ptyInput{applied: tc.a, accepted: tc.q, sentEnd: tc.sent,
				data: []byte(tc.data), abort: "pty input aborted: " + tc.cause}, terminal: &proto.ProcessResumeReply{ExitCode: 23}}
			result, err := r.finish()
			if (err == nil) != tc.clean || (result.InputAbort == "") != tc.clean || result.InputACK != tc.a || result.InputAccepted != tc.q || result.ExitCode != 23 {
				t.Fatalf("closure lost delivery responsibility: %+v err=%v", result, err)
			}
		})
	}
	t.Run("accepted_receipt_retains_owned_suffix", func(t *testing.T) {
		input := ptyInput{data: []byte("cdef"), applied: 2, accepted: 2, sentEnd: 6}
		err := input.accept(proto.ProcessResumeReply{InputACK: 2, InputAccepted: 6, InputWindow: upgradebudget.PTYInputBytes - 4})
		if err != nil || !bytes.Equal(input.data, []byte("cdef")) || input.applied != 2 {
			t.Fatalf("accepted receipt released unapplied bytes: data=%q A=%d Q=%d err=%v", input.data, input.applied, input.accepted, err)
		}
	})
	f := newClientFixture(t)
	c := probePTYFixture(t, f)
	in := newPTYTestSource([]byte("abcdef"), false, false)
	out := newPTYTestSink(nil)
	out.limit = 2 // Real worker short writes must produce only actual-prefix ACKs.
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, in, out, nil)
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
	if input.req.InputSeq != 0 || !bytes.Equal(input.req.Input, []byte("abcdef")) || input.req.InputFIN {
		t.Fatalf("raw input packet changed: %+v", input.req)
	}
	accepted := ptyStatus(input, 0, 6, 0)
	accepted.InputWindow = 0
	sendClientReply(t, input, accepted)
	// Repeating Q with no A is legal. If Q released the client buffer, this
	// second status or the final A=2 receipt loses the still-owned suffix.
	ex := f.next(t, "ack")
	repeated := ptyStatus(ex, 0, 6, 0)
	repeated.InputWindow = 0
	sendClientReply(t, ex, repeated)
	ex = f.next(t, "ack")
	terminal := ptyTerminal(ex, 2, 6, 0, "pty input aborted: write_error")
	terminal.Stdout = &proto.ProcessOutput{Data: []byte("tail"), FIN: true}
	terminal.ExitCode = 0 // rc0 cannot hide four unapplied accepted bytes.
	sendClientReply(t, ex, terminal)
	for {
		ex = f.next(t, "")
		if ex.req.Op == "terminal-ack" {
			break
		}
		if ex.req.StdoutACK > 4 || ex.req.StdoutACK%2 != 0 {
			t.Fatalf("short-write ACK: %+v", ex.req)
		}
		r := ptyTerminal(ex, 2, 6, 0, "pty input aborted: write_error")
		r.ExitCode = 0
		r.Stdout = &proto.ProcessOutput{Offset: ex.req.StdoutACK, Data: []byte("tail")[ex.req.StdoutACK:], FIN: true}
		sendClientReply(t, ex, r)
	}
	if ex.req.StdoutACK != 4 || !ex.req.StdoutFIN || !ex.req.StderrFIN || ex.req.TerminalID != "original-terminal" {
		t.Fatalf("input abort prevented tail settlement: %+v", ex.req)
	}
	// Lose the first settlement reply after the owner has retired. Its retry
	// carries registry metadata only; it must not erase the final A/Q abort.
	retry := f.next(t, "terminal-ack")
	requirePTYHandle(t, ex, retry)
	if retry.req.TerminalID != ex.req.TerminalID || retry.req.StdoutACK != 4 || !retry.req.StdoutFIN || !retry.req.StderrFIN {
		t.Fatalf("settlement retry lost its completion proof: %+v", retry.req)
	}
	settled := ptyTerminal(retry, 0, 0, 0, "")
	settled.Phase, settled.ExitCode = "settled", 0
	settled.Stdout, settled.InputEnded, settled.InputFIN = nil, false, false
	settled.InputWindow = 0
	sendClientReply(t, retry, settled)
	result, err := run.finish(t)
	var abort *SettledError
	if !errors.As(err, &abort) || result.ExitCode != 0 || result.InputACK != 2 || result.InputAccepted != 6 ||
		result.InputAbort != "pty input aborted: write_error" || !bytes.Equal(out.bytes(), []byte("tail")) ||
		!strings.Contains(err.Error(), "applied 2, accepted 6") {
		t.Fatalf("abort was hidden or did not settle: %+v err=%v output=%q", result, err, out.bytes())
	}
}

// origin: hot-update S4 plan §3.4 / §7.2 — lost receipts preserve control payloads.
func TestPTYClientRetriesControlsWithoutChangingSequenceOrPayload(t *testing.T) {
	f := newClientFixture(t)
	c := probePTYFixture(t, f)
	events := make(chan ControlEvent, 8)
	for _, event := range []ControlEvent{
		{Op: proto.ProcessOpResize, Cols: 80, Rows: 24},
		{Op: proto.ProcessOpResize, Cols: 132, Rows: 40},
		{Op: proto.ProcessOpSignal, Signal: 2},
		{Op: proto.ProcessOpKeepalive},
		{Op: proto.ProcessOpHangup},
	} {
		events <- event
	}
	close(events)
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}},
		newPTYTestSource(nil, false, false), newPTYTestSink(nil), events)
	create := f.next(t, "create")
	reply := ptyStatus(create, 0, 0, 0)
	reply.InputWindow = 0
	sendClientReply(t, create, reply)
	control := uint64(0)
	for control < 5 {
		var original clientExchange
		for {
			ex := f.next(t, "")
			if ex.req.ControlSeq != 0 {
				original = ex
				break
			}
			r := ptyStatus(ex, 0, 0, control)
			r.InputWindow = 0
			sendClientReply(t, ex, r)
		}
		if original.req.ControlSeq != control+1 {
			t.Fatalf("control sequence skipped: %+v", original.req)
		}
		// Drop all receipts until this exact operation is retried. Returning a
		// status ACK would itself supply the receipt and legitimately end retry.
		for {
			ex := f.next(t, "")
			requirePTYHandle(t, create, ex)
			if ex.req.ControlSeq == 0 {
				continue
			}
			if ex.req.ControlSeq != original.req.ControlSeq || ex.req.Op != original.req.Op ||
				!reflect.DeepEqual(ex.req.Resize, original.req.Resize) || ex.req.Signal != original.req.Signal ||
				ex.req.CallID == original.req.CallID {
				t.Fatalf("lost control receipt changed its retry: first=%+v retry=%+v", original.req, ex.req)
			}
			control++
			r := ptyStatus(ex, 0, 0, control)
			r.InputWindow = 0
			sendClientReply(t, ex, r)
			break
		}
	}
	run.cancel()
	if _, err := run.finish(t); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// origin: hot-update S4 plan §2.2 / §7.2 — fallback is confined to probing.
func TestPTYClientProbeRefusalsAndCreateBoundary(t *testing.T) {
	t.Run("first_create_recovering_keeps_original", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, newPTYTestSource(nil, false, true), newPTYTestSink(nil), nil)
		first := f.next(t, "create")
		sendClientReply(t, first, proto.ProcessResumeReply{Version: 1, CallID: first.req.CallID,
			Kind: "error", Code: proto.CodeProcessRecovering, Error: "retained recovery"})
		retry := f.next(t, "create")
		requirePTYHandle(t, first, retry)
		sendClientReply(t, retry, ptyStatus(retry, 0, 0, 0))
		for {
			ex := f.next(t, "")
			if ex.req.Op == proto.ProcessOpInput && ex.req.InputFIN {
				sendClientReply(t, ex, ptyTerminal(ex, 0, 0, 0, ""))
				break
			}
			sendClientReply(t, ex, ptyStatus(ex, 0, 0, 0))
		}
		ack := f.next(t, "terminal-ack")
		reply := ptyTerminal(ack, 0, 0, 0, "")
		reply.Phase = "settled"
		sendClientReply(t, ack, reply)
		result, err := run.finish(t)
		if err != nil || result.ExitCode != 23 || result.Handle != *first.req.Handle || !result.CreateSent {
			t.Fatalf("recovery replaced or abandoned the original terminal: %+v err=%v", result, err)
		}
	})
	t.Run("first_create_busy_is_admission_refusal", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, newPTYTestSource(nil, false, false), newPTYTestSink(nil), nil)
		ex := f.next(t, "create")
		sendClientReply(t, ex, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: ex.req.CallID, Kind: "error", Code: proto.CodeProcessBusy})
		result, err := run.finish(t)
		var pe *ProtocolError
		if !errors.As(err, &pe) || pe.AfterCreate || pe.Code != proto.CodeProcessBusy || !result.CreateSent || result.PID != "" {
			t.Fatalf("first create busy was ambiguous or authorized fallback: %+v err=%v", result, err)
		}
	})
	t.Run("lost_create_then_recovering_retries_original", func(t *testing.T) {
		f := newClientFixture(t)
		c := probePTYFixture(t, f)
		run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, newPTYTestSource(nil, false, false), newPTYTestSink(nil), nil)
		first := f.next(t, "create")
		retry := f.next(t, "create")
		sendClientReply(t, retry, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: retry.req.CallID, Kind: "error", Code: proto.CodeProcessRecovering})
		retry = f.next(t, "create")
		requirePTYHandle(t, first, retry)
		_ = c.Close()
		result, err := run.finish(t)
		if !errors.Is(err, context.Canceled) || !result.CreateSent {
			t.Fatalf("recovery-pending escaped recovery: %+v err=%v", result, err)
		}
	})
	for _, tc := range []struct {
		name, code             string
		unsupported, malformed bool
	}{
		{"old_agent", "", true, false},
		{"old_broker", proto.CodeProcessUnsupportedOp, true, false},
		{"permission", "not_a_member", false, false},
		{"identity", proto.CodeProcessIdentity, false, false},
		{"busy", proto.CodeProcessBusy, false, false},
		{"malformed_old_capability", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClientFixture(t)
			done := make(chan error, 1)
			go func() {
				c, err := ProbePTY(t.Context(), f.nc, f.subject)
				if c != nil {
					_ = c.Close()
				}
				done <- err
			}()
			ex := f.next(t, "probe")
			r := f.capability(ex)
			if tc.code == proto.CodeProcessUnsupportedOp {
				r.PTYVersion, r.TaskKind = proto.PTYVersion, proto.ProcessKindPTY
				sendClientReply(t, ex, r)
				ex = f.next(t, proto.ProcessOpPTYProbe)
			}
			if tc.code != "" {
				r = proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: ex.req.CallID,
					Kind: "error", Code: tc.code, Error: tc.code}
			} else if tc.malformed {
				r.Lease.SID = "foreign"
			}
			sendClientReply(t, ex, r)
			select {
			case err := <-done:
				if err == nil || errors.Is(err, ErrUnsupported) != tc.unsupported {
					t.Fatalf("probe classification: %v", err)
				}
			case <-time.After(clientTestBudget):
				t.Fatal("probe did not finish")
			}
		})
	}
	for _, code := range []string{proto.CodeProcessUnsupported, proto.CodeProcessUnsupportedOp, "not_a_member", proto.CodeProcessInstanceLost} {
		t.Run("after_create_"+code, func(t *testing.T) {
			f := newClientFixture(t)
			c := probePTYFixture(t, f)
			run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}},
				newPTYTestSource(nil, false, false), newPTYTestSink(nil), nil)
			ex := f.next(t, "create")
			sendClientReply(t, ex, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: ex.req.CallID,
				Kind: "error", Code: code, Error: code})
			result, err := run.finish(t)
			var pe *ProtocolError
			if errors.Is(err, ErrUnsupported) || !errors.As(err, &pe) || !pe.AfterCreate || !result.CreateSent {
				t.Fatalf("post-create rejection authorized fallback: result=%+v err=%v", result, err)
			}
		})
	}
}

// origin: hot-update S4 plan §3.2 / §6.2 — FIN is a byte boundary, not HUP.
func TestPTYClientFINAndExpiredSettlementKeepAllEnds(t *testing.T) {
	// origin: hot-update S4 plan §3.2–§3.3 / T36 / T37. A compact retry
	// receipt proves settlement only; it cannot change the validated terminal
	// or manufacture fresh credit after its resource owner has retired.
	for _, tc := range []struct {
		name   string
		change func(*proto.ProcessResumeReply)
	}{
		{"exact", func(*proto.ProcessResumeReply) {}},
		{"different_terminal", func(r *proto.ProcessResumeReply) { r.TerminalID = "replacement" }},
		{"different_exit", func(r *proto.ProcessResumeReply) { r.ExitCode = 23 }},
		{"different_error", func(r *proto.ProcessResumeReply) { r.Error = "replacement" }},
		{"new_input_ack", func(r *proto.ProcessResumeReply) { r.InputACK = 2 }},
		{"new_input_accepted", func(r *proto.ProcessResumeReply) { r.InputAccepted = 6 }},
		{"new_input_credit", func(r *proto.ProcessResumeReply) { r.InputWindow = 1 }},
		{"new_input_fin", func(r *proto.ProcessResumeReply) { r.InputFIN = true }},
		{"new_input_error", func(r *proto.ProcessResumeReply) { r.InputError = "replacement" }},
		{"new_control_ack", func(r *proto.ProcessResumeReply) { r.ControlACK = 1 }},
		{"new_control_error", func(r *proto.ProcessResumeReply) { r.ControlError = "replacement" }},
	} {
		t.Run("compact_settlement_"+tc.name, func(t *testing.T) {
			h := proto.ProcessHandle{RequestID: "original", Lease: proto.ProcessLease{TargetInstance: "original-runtime"}}
			r := ptyRun{phase: ptySettling, result: Result{Handle: h, PID: "original-pid"},
				terminal: &proto.ProcessResumeReply{TerminalID: "original-terminal"},
				input:    ptyInput{applied: 2, accepted: 6, sentEnd: 6, ended: true, data: []byte("cdef"), abort: "pty input aborted: write_error"}}
			req := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &h, AttachEpoch: 1, TerminalID: "original-terminal"}
			reply := proto.ProcessResumeReply{Kind: "status", RequestID: h.RequestID, TargetInstance: h.Lease.TargetInstance,
				PID: "original-pid", AttachEpoch: 1, TaskKind: proto.ProcessKindPTY, Phase: "settled", TerminalID: "original-terminal", Stderr: &proto.ProcessOutput{FIN: true}}
			tc.change(&reply)
			err := r.accept(req, reply)
			if (err == nil) != (tc.name == "exact") || r.input.applied != 2 || r.input.accepted != 6 || r.input.abort != "pty input aborted: write_error" ||
				!bytes.Equal(r.input.data, []byte("cdef")) || (r.phase == ptyDone) != (tc.name == "exact") {
				t.Fatalf("compact receipt changed final responsibility: input=%+v phase=%d err=%v", r.input, r.phase, err)
			}
		})
	}
	f := newClientFixture(t)
	c := probePTYFixture(t, f)
	out := newPTYTestSink(nil)
	data := []byte{0x03, 0x04, 0x1b, 0xf0, 0x9f, 0x98, 0x80}
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, newPTYTestSource(data, false, true), out, nil)
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
	if !input.req.InputFIN || input.req.InputSeq != 0 || !bytes.Equal(input.req.Input, data) {
		t.Fatalf("FIN or raw terminal bytes changed: %+v", input.req)
	}
	r := ptyStatus(input, uint64(len(data)), uint64(len(data)), 0)
	r.InputFIN, r.InputEnded = true, true
	sendClientReply(t, input, r)
	ex := f.next(t, "ack")
	terminal := ptyTerminal(ex, uint64(len(data)), uint64(len(data)), 0, "")
	terminal.Stdout = &proto.ProcessOutput{Data: []byte("tail"), FIN: true}
	sendClientReply(t, ex, terminal)
	for {
		ex = f.next(t, "")
		if ex.req.Op == "terminal-ack" {
			break
		}
		r := ptyTerminal(ex, uint64(len(data)), uint64(len(data)), 0, "")
		r.Stdout = &proto.ProcessOutput{Offset: ex.req.StdoutACK, Data: []byte("tail")[ex.req.StdoutACK:], FIN: true}
		sendClientReply(t, ex, r)
	}
	if !ex.req.StdoutFIN || !ex.req.StderrFIN || ex.req.StdoutACK != 4 {
		t.Fatalf("missing ends: %+v", ex.req)
	}
	// Lose the first terminal-ack reply; the same terminal/ends must be retried.
	retry := f.next(t, "terminal-ack")
	requirePTYHandle(t, create, retry)
	if retry.req.TerminalID != ex.req.TerminalID || retry.req.StdoutACK != 4 || !retry.req.StdoutFIN || !retry.req.StderrFIN {
		t.Fatalf("settlement retry lost completion proof: %+v", retry.req)
	}
	sendClientReply(t, retry, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: retry.req.CallID,
		Kind: "error", Code: proto.CodeProcessExpired, Error: "process_expired"})
	result, err := run.finish(t)
	if err != nil || result.ExitCode != 23 || result.InputACK != uint64(len(data)) || !bytes.Equal(out.bytes(), []byte("tail")) {
		t.Fatalf("complete settlement failed: %+v err=%v", result, err)
	}
}

// origin: hot-update S4 plan §7.2 — protocol batches cannot inflate the local queue.
func TestPTYClientRejectsOversizedOutputBeforeWriting(t *testing.T) {
	f := newClientFixture(t)
	c := probePTYFixture(t, f)
	out := newPTYTestSink(nil)
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, newPTYTestSource(nil, false, false), out, nil)
	ex := f.next(t, "create")
	r := ptyStatus(ex, 0, 0, 0)
	r.Stdout = &proto.ProcessOutput{Data: bytes.Repeat([]byte("x"), upgradebudget.PTYBatchBytes+1)}
	sendClientReply(t, ex, r)
	_, err := run.finish(t)
	if err == nil || len(out.bytes()) != 0 || out.writes.Load() != 0 {
		t.Fatalf("oversized output reached sink: %v", err)
	}
}

// origin: hot-update S4 plan §7.2 — local failure still owes its written prefix.
func TestPTYClientAcknowledgesOnlyWrittenOutputPrefix(t *testing.T) {
	f := newClientFixture(t)
	c := probePTYFixture(t, f)
	out := newPTYTestSink(nil)
	out.limit, out.fail = 2, errors.New("fixture sink failed")
	run := startPTYFixture(t, c, proto.RunReq{Argv: []string{"fixture"}}, newPTYTestSource(nil, false, false), out, nil)
	create := f.next(t, "create")
	r := ptyStatus(create, 0, 0, 0)
	r.InputWindow = 0
	r.Stdout = &proto.ProcessOutput{Data: []byte("abcdef"), FIN: true}
	sendClientReply(t, create, r)
	// A write can finish while the owner is already awaiting an earlier ACK.
	// Supply that reply, then require the later ACK to carry exactly n=2.
	for {
		ex := f.next(t, "ack")
		if ex.req.StdoutACK != 0 {
			if ex.req.StdoutACK != 2 || ex.req.StdoutFIN || ex.req.TerminalID != "" {
				t.Fatalf("failed short write acknowledged undelivered bytes/end: %+v", ex.req)
			}
			sendClientReply(t, ex, ptyStatus(ex, 0, 0, 0))
			break
		}
		sendClientReply(t, ex, ptyStatus(ex, 0, 0, 0))
	}
	result, err := run.finish(t)
	if !errors.Is(err, out.fail) || result.StdoutBytes != 2 || !bytes.Equal(out.bytes(), []byte("ab")) {
		t.Fatalf("failed writer lost its actual prefix: %+v err=%v output=%q", result, err, out.bytes())
	}
}
