package execclient

// Real: NATS protocol messages, private inboxes and the public Run entrypoint.
// Stubbed: broker authorization and agent replies, except the runtime integration.
// This file does not claim agent reexec, controller handoff or hot-update readiness.
// origin: docs/reviews/hot-update-plan.md §3.1.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/workload"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

const clientTestBudget = 5 * time.Second

type clientFixture struct {
	server             *natsserver.Server
	nc, responder      *nats.Conn
	sub                *nats.Subscription
	actor, fp, subject string
}

type clientExchange struct {
	msg *nats.Msg
	req proto.ProcessResumeReq
}

type clientRun struct {
	done   chan struct{}
	cancel context.CancelFunc
	result Result
	err    error
}

func newClientFixture(t *testing.T) *clientFixture {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	s := natstest.RunServer(&opts)
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(s.ClientURL(), natsinbox.InboxConnectOptions(actor)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	responder, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(responder.Close)
	// Observe every verb on this node so a legacy fallback cannot hide.
	sub, err := responder.SubscribeSync(proto.SubjCmdBy("lab", actor, "node", "*"))
	if err != nil {
		t.Fatal(err)
	}
	if err := responder.FlushTimeout(clientTestBudget); err != nil {
		t.Fatal(err)
	}
	return &clientFixture{s, nc, responder, sub, actor, fp, proto.SubjCmdBy("lab", actor, "node", proto.ProcessResumeVerb)}
}

func (f *clientFixture) start(t *testing.T, timeout time.Duration, stdout, stderr io.Writer) *clientRun {
	t.Helper()
	return f.startSpec(t, timeout, proto.ExecReq{Argv: []string{"/bin/echo", "intent"}, Stdin: []byte("input")}, stdout, stderr)
}

func (f *clientFixture) startSpec(t *testing.T, timeout time.Duration, spec proto.ExecReq, stdout, stderr io.Writer) *clientRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	r := &clientRun{done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(r.done)
		r.result, r.err = Run(ctx, f.nc, f.subject, spec, stdout, stderr)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(clientTestBudget):
			t.Error("client did not stop")
		}
	})
	return r
}

func (r *clientRun) finish(t *testing.T) (Result, error) {
	t.Helper()
	select {
	case <-r.done:
		return r.result, r.err
	case <-time.After(clientTestBudget):
		t.Fatal("client did not finish")
	}
	return Result{}, nil
}

func (f *clientFixture) next(t *testing.T, op string) clientExchange {
	t.Helper()
	msg, err := f.sub.NextMsg(clientTestBudget)
	if err != nil {
		t.Fatalf("waiting for %s: %v", op, err)
	}
	if msg.Subject != f.subject || !strings.HasPrefix(msg.Reply, auth.InboxPrefixFor(f.actor)+".") {
		t.Fatalf("request used legacy verb or foreign reply: subject=%q reply=%q", msg.Subject, msg.Reply)
	}
	var req proto.ProcessResumeReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		t.Fatal(err)
	}
	if (op != "" && req.Op != op) || req.CallID == 0 || req.Version != proto.ProcessResumeVersion {
		t.Fatalf("expected %s, got op=%s call=%d version=%d", op, req.Op, req.CallID, req.Version)
	}
	return clientExchange{msg, req}
}

func sendClientReply(t *testing.T, ex clientExchange, reply proto.ProcessResumeReply) {
	t.Helper()
	body, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if err := ex.msg.Respond(body); err != nil {
		t.Fatal(err)
	}
}

func (f *clientFixture) capability(ex clientExchange) proto.ProcessResumeReply {
	return proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: ex.req.CallID, Kind: "capability", TargetInstance: "original-instance",
		Lease: &proto.ProcessLease{Version: proto.ProcessResumeVersion, LeaseID: "lease-1", TargetInstance: "original-instance", BootID: "original-boot",
			SID: "lab", NID: "node", ActorFP: f.fp, ClientLineage: ex.req.ClientLineage, IssuedBootNS: 11, ExpiresBootNS: 999, MAC: "opaque-test-MAC"}}
}

func (f *clientFixture) negotiate(t *testing.T) clientExchange {
	t.Helper()
	probe := f.next(t, "probe")
	sendClientReply(t, probe, f.capability(probe))
	return f.next(t, "create")
}

func clientStatus(ex clientExchange) proto.ProcessResumeReply {
	return proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: ex.req.CallID, Kind: "status", RequestID: ex.req.Handle.RequestID,
		PID: "original-process", TargetInstance: ex.req.Handle.Lease.TargetInstance, AttachEpoch: ex.req.AttachEpoch, Phase: "running"}
}

func terminalClientStatus(ex clientExchange) proto.ProcessResumeReply {
	r := clientStatus(ex)
	r.Phase, r.TerminalID, r.ExitCode = "terminal", "original-terminal", 17
	r.Stdout, r.Stderr = &proto.ProcessOutput{Offset: ex.req.StdoutACK, FIN: true}, &proto.ProcessOutput{Offset: ex.req.StderrACK, FIN: true}
	return r
}

func settleClient(t *testing.T, f *clientFixture, r *clientRun) Result {
	t.Helper()
	ex := f.next(t, "terminal-ack")
	if !ex.req.StdoutFIN || !ex.req.StderrFIN || ex.req.TerminalID != "original-terminal" {
		t.Fatalf("premature settlement: %+v", ex.req)
	}
	reply := clientStatus(ex)
	reply.Phase, reply.TerminalID = "settled", ex.req.TerminalID
	sendClientReply(t, ex, reply)
	result, err := r.finish(t)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type partialDestination struct {
	bytes.Buffer
	n   int
	err error
}

func (w *partialDestination) Write(p []byte) (int, error) {
	n := w.n
	if n > len(p) {
		n = len(p)
	}
	_, _ = w.Buffer.Write(p[:n])
	return n, w.err
}

func TestRunAcknowledgesOnlyBytesWritten(t *testing.T) {
	sinkErr := errors.New("destination failed after a partial write")
	for _, tc := range []struct {
		name           string
		n              int
		returned, want error
	}{
		{"partial_error", 2, sinkErr, sinkErr}, {"partial_nil", 2, nil, io.ErrShortWrite}, {"complete_with_error", 5, sinkErr, sinkErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClientFixture(t)
			out, stderr := &partialDestination{n: tc.n, err: tc.returned}, &bytes.Buffer{}
			run := f.start(t, clientTestBudget, out, stderr)
			create := f.negotiate(t)
			reply := clientStatus(create)
			reply.Stdout, reply.Stderr = &proto.ProcessOutput{Data: []byte("abcde"), FIN: true}, &proto.ProcessOutput{Data: []byte("unwritten"), FIN: true}
			sendClientReply(t, create, reply)
			ack := f.next(t, "ack")
			if ack.req.StdoutACK != uint64(tc.n) || ack.req.StderrACK != 0 || ack.req.StdoutFIN || ack.req.StderrFIN || ack.req.Exec != nil || !reflect.DeepEqual(ack.req.Handle, create.req.Handle) {
				t.Fatalf("ACK exceeded actual destination acceptance: %+v", ack.req)
			}
			sendClientReply(t, ack, clientStatus(ack))
			_, err := run.finish(t)
			if !errors.Is(err, tc.want) || out.String() != "abcde"[:tc.n] || stderr.Len() != 0 {
				t.Fatalf("wrong partial result: out=%q errout=%q err=%v", out.String(), stderr.String(), err)
			}
		})
	}
}

func TestRunDeduplicatesOverlapAndPreservesFIN(t *testing.T) {
	f := newClientFixture(t)
	var out, errout bytes.Buffer
	run := f.start(t, clientTestBudget, &out, &errout)
	ex := f.negotiate(t)
	for _, step := range []struct {
		batch    proto.ProcessOutput
		expected uint64
	}{
		{proto.ProcessOutput{Data: []byte("abc")}, 3},
		{proto.ProcessOutput{Data: []byte("abc")}, 3},
		{proto.ProcessOutput{Offset: 1, Data: []byte("bcde")}, 5},
	} {
		reply := clientStatus(ex)
		reply.Stdout = &step.batch
		sendClientReply(t, ex, reply)
		ex = f.next(t, "ack")
		if ex.req.StdoutACK != step.expected || ex.req.StdoutFIN {
			t.Fatalf("wrong overlap ACK: %+v", ex.req)
		}
	}
	reply := terminalClientStatus(ex)
	reply.Stdout = &proto.ProcessOutput{Offset: 1, Data: []byte("bcde"), FIN: true}
	reply.Stderr = &proto.ProcessOutput{Data: []byte("ERR"), FIN: true}
	sendClientReply(t, ex, reply)
	result := settleClient(t, f, run)
	if out.String() != "abcde" || errout.String() != "ERR" || result.ExitCode != 17 {
		t.Fatalf("duplicate/lost output: stdout=%q stderr=%q result=%+v", out.String(), errout.String(), result)
	}
}

func TestRunRejectsOutputGapsAndConflictingFIN(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first, bad proto.ProcessOutput
		want       string
	}{
		{"gap", proto.ProcessOutput{Data: []byte("abc")}, proto.ProcessOutput{Offset: 4, Data: []byte("x")}, "gap"},
		{"stale_final_offset", proto.ProcessOutput{Data: []byte("abc")}, proto.ProcessOutput{Data: []byte("ab"), FIN: true}, "conflicting final"},
		{"bytes_after_FIN", proto.ProcessOutput{Data: []byte("abc"), FIN: true}, proto.ProcessOutput{Offset: 3, Data: []byte("x")}, "after FIN"},
		{"overflow", proto.ProcessOutput{Data: []byte("abc")}, proto.ProcessOutput{Offset: ^uint64(0), Data: []byte("x")}, "gap or invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClientFixture(t)
			var out bytes.Buffer
			run := f.start(t, clientTestBudget, &out, io.Discard)
			create := f.negotiate(t)
			reply := clientStatus(create)
			reply.Stdout = &tc.first
			sendClientReply(t, create, reply)
			ack := f.next(t, "ack")
			reply = clientStatus(ack)
			reply.Stdout = &tc.bad
			sendClientReply(t, ack, reply)
			partial := f.next(t, "ack")
			if partial.req.StdoutACK != 3 || partial.req.StdoutFIN != tc.first.FIN {
				t.Fatalf("bad data was acknowledged: %+v", partial.req)
			}
			sendClientReply(t, partial, clientStatus(partial))
			_, err := run.finish(t)
			if err == nil || !strings.Contains(err.Error(), tc.want) || out.String() != "abc" {
				t.Fatalf("bad stream accepted: out=%q err=%v", out.String(), err)
			}
		})
	}
}

func TestRunRejectsChangedTaskIdentityBeforeWriting(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*proto.ProcessResumeReply)
	}{
		{"request", func(r *proto.ProcessResumeReply) { r.RequestID = "another-request" }},
		{"instance", func(r *proto.ProcessResumeReply) { r.TargetInstance = "replacement-instance" }},
		{"attach", func(r *proto.ProcessResumeReply) { r.AttachEpoch++ }},
		{"pid", func(r *proto.ProcessResumeReply) { r.PID = "replacement-process" }},
		{"kind", func(r *proto.ProcessResumeReply) { r.Kind = "capability" }},
		{"phase", func(r *proto.ProcessResumeReply) { r.Phase = "imaginary" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClientFixture(t)
			var out bytes.Buffer
			run := f.start(t, clientTestBudget, &out, io.Discard)
			create := f.negotiate(t)
			sendClientReply(t, create, clientStatus(create))
			ack := f.next(t, "ack")
			reply := terminalClientStatus(ack)
			reply.Stdout.Data = []byte("must-not-write")
			tc.change(&reply)
			sendClientReply(t, ack, reply)
			_, err := run.finish(t)
			if err == nil || out.Len() != 0 || errors.Is(err, ErrUnsupported) {
				t.Fatalf("changed task accepted or downgraded: out=%q err=%v", out.String(), err)
			}
		})
	}
}

func TestRunIgnoresLateCallReplies(t *testing.T) {
	f := newClientFixture(t)
	var out bytes.Buffer
	run := f.start(t, clientTestBudget, &out, io.Discard)
	probe := f.next(t, "probe")
	sendClientReply(t, probe, f.capability(probe))
	create := f.next(t, "create")
	// Ordered on one responder connection: both obsolete responses reach the
	// private inbox before the current response, not after Run has returned.
	sendClientReply(t, probe, proto.ProcessResumeReply{Version: 999, CallID: probe.req.CallID, Kind: "error", Error: "late obsolete error"})
	reply := clientStatus(create)
	reply.Stdout = &proto.ProcessOutput{Data: []byte("abc")}
	sendClientReply(t, create, reply)
	ack := f.next(t, "ack")
	late := clientStatus(create)
	late.PID = "wrong-old-pid"
	late.Stdout = &proto.ProcessOutput{Data: []byte("must-not-write")}
	sendClientReply(t, create, late)
	sendClientReply(t, ack, terminalClientStatus(ack))
	settleClient(t, f, run)
	if out.String() != "abc" {
		t.Fatalf("late reply corrupted stream: %q", out.String())
	}
}

func TestRunRetriesLostCreateAndTerminalWithOriginalHandle(t *testing.T) {
	f := newClientFixture(t)
	run := f.start(t, 2*clientTestBudget, io.Discard, io.Discard)
	first := f.negotiate(t)
	// Drop a request's response, not the request itself: this is the ambiguous
	// commit window in which generating a new handle could execute twice.
	retry := f.next(t, "create")
	if !bytes.Equal(first.msg.Data, retry.msg.Data) || first.msg.Reply != retry.msg.Reply || retry.req.Handle == nil {
		t.Fatal("lost create response changed handle, intent, CallID or inbox")
	}
	sendClientReply(t, retry, terminalClientStatus(retry))
	terminal := f.next(t, "terminal-ack")
	terminalRetry := f.next(t, "terminal-ack")
	if !bytes.Equal(terminal.msg.Data, terminalRetry.msg.Data) || !reflect.DeepEqual(first.req.Handle, terminalRetry.req.Handle) || terminalRetry.req.TerminalID != "original-terminal" {
		t.Fatal("lost terminal ACK response changed settlement identity")
	}
	reply := clientStatus(terminalRetry)
	reply.Phase, reply.TerminalID = "settled", terminalRetry.req.TerminalID
	sendClientReply(t, terminalRetry, reply)
	result, err := run.finish(t)
	if err != nil || result.ExitCode != 17 || !reflect.DeepEqual(&result.Handle, first.req.Handle) {
		t.Fatalf("retry result changed: %+v %v", result, err)
	}
}

func TestRunProbeAuthorizationErrorCannotDowngrade(t *testing.T) {
	for _, tc := range []struct {
		reason      string
		unsupported bool
	}{
		{"not_a_member", false}, {"session_deleting", false}, {"permission denied", false}, {"unsupported process protocol", true},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			f := newClientFixture(t)
			run := f.start(t, clientTestBudget, io.Discard, io.Discard)
			probe := f.next(t, "probe")
			sendClientReply(t, probe, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: probe.req.CallID, Kind: "error", Error: tc.reason})
			_, err := run.finish(t)
			if err == nil || errors.Is(err, ErrUnsupported) != tc.unsupported {
				t.Fatalf("wrong downgrade decision: reason=%s err=%v", tc.reason, err)
			}
			if !tc.unsupported && err.Error() != tc.reason {
				t.Fatalf("authorization denial was lost: %v", err)
			}
		})
	}
}

func TestRunRejectsCapabilityIdentityBeforeCreate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*proto.ProcessResumeReply)
	}{
		{"actor", func(r *proto.ProcessResumeReply) { r.Lease.ActorFP = "foreign-actor" }},
		{"session", func(r *proto.ProcessResumeReply) { r.Lease.SID = "other-session" }},
		{"node", func(r *proto.ProcessResumeReply) { r.Lease.NID = "other-node" }},
		{"lineage", func(r *proto.ProcessResumeReply) { r.Lease.ClientLineage = "other-client" }},
		{"instance", func(r *proto.ProcessResumeReply) { r.TargetInstance = "replacement-instance" }},
		{"version", func(r *proto.ProcessResumeReply) { r.Lease.Version++ }},
		{"missing_lease", func(r *proto.ProcessResumeReply) { r.Lease = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClientFixture(t)
			run := f.start(t, clientTestBudget, io.Discard, io.Discard)
			probe := f.next(t, "probe")
			reply := f.capability(probe)
			tc.change(&reply)
			sendClientReply(t, probe, reply)
			_, err := run.finish(t)
			if err == nil || errors.Is(err, ErrUnsupported) {
				t.Fatalf("invalid capability accepted or downgraded: %v", err)
			}
			// Run has returned. A marker through the same client publisher
			// fences any create/legacy request it might have sent first.
			marker := []byte("client-capability-fence")
			if err := f.nc.Publish(f.subject, marker); err != nil {
				t.Fatal(err)
			}
			message, err := f.sub.NextMsg(clientTestBudget)
			if err != nil || !bytes.Equal(message.Data, marker) {
				t.Fatalf("rejected capability caused execution: %v %v", message, err)
			}
		})
	}
}

func TestRunNoRespondersHasBoundedRequestRate(t *testing.T) {
	f := newClientFixture(t)
	if err := f.sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := f.responder.FlushTimeout(clientTestBudget); err != nil {
		t.Fatal(err)
	}
	before, err := f.server.Varz(nil)
	if err != nil {
		t.Fatal(err)
	}
	// Only this client can publish in this isolated real server. InMsgs thus
	// measures actual network requests despite there being no observer SUB,
	// which would itself suppress the server's real no-responders response.
	run := f.start(t, 350*time.Millisecond, io.Discard, io.Discard)
	_, runErr := run.finish(t)
	if !errors.Is(runErr, context.DeadlineExceeded) && !errors.Is(runErr, ErrUnsupported) {
		t.Fatalf("wrong absent-peer result: %v", runErr)
	}
	if err := f.nc.FlushTimeout(clientTestBudget); err != nil {
		t.Fatal(err)
	}
	after, err := f.server.Varz(nil)
	if err != nil {
		t.Fatal(err)
	}
	sent := after.InMsgs - before.InMsgs
	if sent == 0 || sent > 5 {
		t.Fatalf("no-responder retry spun on immediate 503 responses: real requests=%d in a 350ms caller budget", sent)
	}
}

func execClientPayload(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + 7) % 251)
	}
	return data
}

func TestExecClientChildHelper(t *testing.T) {
	if os.Getenv("TETHER_EXECCLIENT_CHILD_HELPER") != "1" {
		return
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil || !bytes.Equal(input, []byte("real input")) {
		os.Exit(91)
	}
	if _, err := os.Stdout.Write(execClientPayload(80 << 10)); err != nil {
		os.Exit(92)
	}
	if _, err := os.Stderr.Write(execClientPayload(25 << 10)); err != nil {
		os.Exit(93)
	}
	os.Exit(17)
}

func TestRunRealChildSurvivesLostCreateAndSettlementReplies(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("bounded replay uses Linux memfd")
	}
	f := newClientFixture(t)
	var manager workload.Manager
	var starts, exits atomic.Int32
	rt, err := execsession.New(execsession.Config{Instance: "real-original-instance", Manager: &manager, ReplayCapacity: 4096,
		Started: func(string, proto.ExecReq, workload.Identity) {},
		Exited: func(_ string, code int) {
			if code == 17 {
				exits.Add(1)
			}
		},
		Start: func(id string, spec *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := workload.NewStdio()
			if err != nil {
				return nil, "", err
			}
			cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
			cmd.Env = append(os.Environ(), "TETHER_EXECCLIENT_CHILD_HELPER=1")
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			stdio.Bind(cmd)
			if err := cmd.Start(); err != nil {
				stdio.Close()
				return nil, "", err
			}
			stdio.CloseChild()
			if err := manager.AdoptDraining(workload.Identity{ID: id, OSPID: cmd.Process.Pid, StartedAt: time.Now().UTC()}, cmd.Process); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				stdio.Close()
				return nil, "", err
			}
			starts.Add(1)
			return stdio, "", nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), clientTestBudget)
		defer cancel()
		if err := rt.Close(ctx); err != nil {
			t.Errorf("runtime cleanup: %v", err)
		}
		for _, row := range manager.Snapshot() {
			if row.Exit != nil {
				if err := manager.Forget(row.ID); err != nil {
					t.Error(err)
				}
			}
		}
	})
	var out, errout bytes.Buffer
	run := f.startSpec(t, 3*clientTestBudget, proto.ExecReq{Argv: []string{os.Args[0], "-test.run=^TestExecClientChildHelper$"}, Stdin: []byte("real input")}, &out, &errout)
	var createWire, terminalWire []byte
	var originalPID string
	createCalls, terminalCalls := 0, 0
	for {
		ex := f.next(t, "")
		req := ex.req
		req.ActorFP = f.fp // This test stubs only the broker authentication stamp.
		lineage := req.ClientLineage
		if req.Handle != nil {
			lineage = req.Handle.Lease.ClientLineage
		}
		reply, err := rt.Handle(procrequest.Scope{SID: "lab", NID: "node", ActorFP: f.fp, ClientLineage: lineage}, req, ex.msg.Reply)
		if err != nil {
			t.Fatalf("real runtime refused %s: %v", req.Op, err)
		}
		if req.Op == "create" {
			createCalls++
			if createCalls == 1 {
				createWire, originalPID = append([]byte(nil), ex.msg.Data...), reply.PID
				continue
			}
			if !bytes.Equal(createWire, ex.msg.Data) || reply.PID != originalPID {
				t.Fatal("ambiguous create spawned a new identity")
			}
		}
		if req.Op == "terminal-ack" {
			terminalCalls++
			if terminalCalls == 1 {
				terminalWire = append([]byte(nil), ex.msg.Data...)
				continue
			}
			if !bytes.Equal(terminalWire, ex.msg.Data) || reply.Phase != "settled" {
				t.Fatal("ambiguous settlement changed request or phase")
			}
		}
		sendClientReply(t, ex, reply)
		if req.Op == "terminal-ack" {
			break
		}
	}
	result, err := run.finish(t)
	if err != nil || result.ExitCode != 17 || result.PID != originalPID || starts.Load() != 1 || exits.Load() != 1 || createCalls < 2 || terminalCalls < 2 {
		t.Fatalf("child/retry lifecycle mismatch: result=%+v err=%v starts=%d exits=%d createCalls=%d terminalCalls=%d", result, err, starts.Load(), exits.Load(), createCalls, terminalCalls)
	}
	if !bytes.Equal(out.Bytes(), execClientPayload(80<<10)) || !bytes.Equal(errout.Bytes(), execClientPayload(25<<10)) {
		t.Fatalf("real pipe bytes lost or repeated: stdout=%d stderr=%d", out.Len(), errout.Len())
	}
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T11
func TestRunSettlesAbortedStreamAndReportsIt(t *testing.T) {
	f := newClientFixture(t)
	var out bytes.Buffer
	run := f.start(t, clientTestBudget, &out, io.Discard)
	create := f.negotiate(t)
	reply := clientStatus(create)
	reply.Stdout = &proto.ProcessOutput{Data: []byte("abc")}
	sendClientReply(t, create, reply)
	ack := f.next(t, "ack")
	if ack.req.StdoutACK != 3 || ack.req.StdoutFIN {
		t.Fatalf("bytes acknowledged incorrectly before the abort: %+v", ack.req)
	}
	terminal := clientStatus(ack)
	terminal.Phase, terminal.TerminalID, terminal.ExitCode = "terminal", "original-terminal", 17
	terminal.Stdout = &proto.ProcessOutput{Offset: 3, Error: "source failed"}
	terminal.Stderr = &proto.ProcessOutput{FIN: true}
	sendClientReply(t, ack, terminal)
	// The aborted stream is acknowledged at its final offset with FIN false
	// (settleClient would call that premature); the true EOF is echoed.
	settle := f.next(t, "terminal-ack")
	if settle.req.StdoutACK != 3 || settle.req.StdoutFIN || settle.req.StderrACK != 0 || !settle.req.StderrFIN || settle.req.TerminalID != "original-terminal" {
		t.Fatalf("aborted stream settled incorrectly: %+v", settle.req)
	}
	settled := clientStatus(settle)
	settled.Phase, settled.TerminalID = "settled", settle.req.TerminalID
	sendClientReply(t, settle, settled)
	result, err := run.finish(t)
	var unclean *SettledError
	if !errors.As(err, &unclean) || errors.Is(err, ErrUnsupported) {
		t.Fatalf("aborted stream did not settle into a SettledError: %v", err)
	}
	if result.ExitCode != 17 || result.StdoutAbort != "source failed" || result.StdoutBytes != 3 || result.TerminalError != "" || result.StderrAbort != "" || out.String() != "abc" {
		t.Fatalf("settled result: %+v out=%q", result, out.String())
	}
	if !strings.Contains(err.Error(), "stdout aborted after 3 byte(s): source failed") {
		t.Fatalf("abort not reported: %v", err)
	}
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T12
func TestRunRejectsConflictingStreamEndMarkers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first, bad proto.ProcessOutput
		want       string
		partialFIN bool
	}{
		{"fin_then_error", proto.ProcessOutput{Data: []byte("abc"), FIN: true}, proto.ProcessOutput{Offset: 3, Error: "x"}, "conflicting stream end", true},
		{"error_then_fin", proto.ProcessOutput{Data: []byte("abc"), Error: "x"}, proto.ProcessOutput{Offset: 3, FIN: true}, "conflicting stream end", false},
		{"error_then_bytes", proto.ProcessOutput{Data: []byte("abc"), Error: "x"}, proto.ProcessOutput{Offset: 3, Data: []byte("d")}, "after abort", false},
		{"both_markers", proto.ProcessOutput{Data: []byte("abc")}, proto.ProcessOutput{Offset: 3, FIN: true, Error: "x"}, "conflicting stream end", false},
		{"stale_abort_offset", proto.ProcessOutput{Data: []byte("abc")}, proto.ProcessOutput{Data: []byte("ab"), Error: "x"}, "conflicting final", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newClientFixture(t)
			var out bytes.Buffer
			run := f.start(t, clientTestBudget, &out, io.Discard)
			create := f.negotiate(t)
			reply := clientStatus(create)
			reply.Stdout = &tc.first
			sendClientReply(t, create, reply)
			ack := f.next(t, "ack")
			reply = clientStatus(ack)
			reply.Stdout = &tc.bad
			sendClientReply(t, ack, reply)
			partial := f.next(t, "ack")
			if partial.req.StdoutACK != 3 || partial.req.StdoutFIN != tc.partialFIN {
				t.Fatalf("bad data was acknowledged: %+v", partial.req)
			}
			sendClientReply(t, partial, clientStatus(partial))
			_, err := run.finish(t)
			if err == nil || !strings.Contains(err.Error(), tc.want) || out.String() != "abc" {
				t.Fatalf("bad stream accepted: out=%q err=%v", out.String(), err)
			}
		})
	}
}

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T13
func TestRunRealChildSetupFailureSettlesAbortedStream(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("bounded replay uses Linux memfd")
	}
	f := newClientFixture(t)
	var manager workload.Manager
	// An eight-byte stderr log cannot hold the sixteen-byte startup warning:
	// a real child exists and its stdout pump runs when stderr setup fails.
	rt, err := execsession.New(execsession.Config{Instance: "real-original-instance", Manager: &manager, ReplayCapacity: 8,
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {},
		Start: func(id string, spec *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := workload.NewStdio()
			if err != nil {
				return nil, "", err
			}
			cmd := exec.Command("/bin/sh", "-c", "exec sleep 30")
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			stdio.Bind(cmd)
			if err := cmd.Start(); err != nil {
				stdio.Close()
				return nil, "", err
			}
			stdio.CloseChild()
			if err := manager.AdoptDraining(workload.Identity{ID: id, OSPID: cmd.Process.Pid, StartedAt: time.Now().UTC()}, cmd.Process); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				stdio.Close()
				return nil, "", err
			}
			return stdio, strings.Repeat("w", 16), nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), clientTestBudget)
		defer cancel()
		if err := rt.Close(ctx); err != nil {
			t.Errorf("runtime cleanup: %v", err)
		}
	})
	var out, errout bytes.Buffer
	run := f.startSpec(t, 3*clientTestBudget, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "exec sleep 30"}}, &out, &errout)
	var settleReq proto.ProcessResumeReq
	for {
		ex := f.next(t, "")
		req := ex.req
		req.ActorFP = f.fp // This test stubs only the broker authentication stamp.
		lineage := req.ClientLineage
		if req.Handle != nil {
			lineage = req.Handle.Lease.ClientLineage
		}
		reply, err := rt.Handle(procrequest.Scope{SID: "lab", NID: "node", ActorFP: f.fp, ClientLineage: lineage}, req, ex.msg.Reply)
		if err != nil {
			t.Fatalf("real runtime refused %s: %v", req.Op, err)
		}
		sendClientReply(t, ex, reply)
		if req.Op == "terminal-ack" {
			settleReq = req
			if reply.Phase != "settled" {
				t.Fatalf("terminal-ack did not settle: %+v", reply)
			}
			break
		}
	}
	// Both ends agree on the contract: stdout drained to a true EOF (echoed
	// as FIN), stderr never got a pump and is acknowledged at 0 with FIN false.
	if !settleReq.StdoutFIN || settleReq.StdoutACK != 0 || settleReq.StderrFIN || settleReq.StderrACK != 0 {
		t.Fatalf("settlement flags: %+v", settleReq)
	}
	result, err := run.finish(t)
	var unclean *SettledError
	if !errors.As(err, &unclean) {
		t.Fatalf("setup failure did not settle into a SettledError: %v (result=%+v)", err, result)
	}
	if result.TerminalError == "" || result.StderrAbort == "" || result.StdoutAbort != "" || result.Signal != 9 || result.ExitCode != -1 || out.Len() != 0 || errout.Len() != 0 {
		t.Fatalf("settled setup failure: %+v out=%d err=%d", result, out.Len(), errout.Len())
	}
	if rows := manager.Snapshot(); len(rows) != 0 {
		t.Fatalf("manager retained the killed child: %+v", rows)
	}
}

// origin: S0 internal review G3 — the CLI classifies a failure by the text
// before its first colon, so the task's own terminal error must lead the
// message verbatim and a stream abort that repeats it must not be echoed.
func TestSettledErrorLeadsWithTerminalErrorVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result Result
		want   string
	}{
		{"terminal_then_abort", Result{TerminalError: "remote_fs_spawn_timeout: /mnt hung", StderrAbort: "source failed", StderrBytes: 7},
			"remote_fs_spawn_timeout: /mnt hung; stderr aborted after 7 byte(s): source failed"},
		{"abort_repeating_terminal", Result{TerminalError: "workload: output requires unused replay log", StderrAbort: "workload: output requires unused replay log"},
			"workload: output requires unused replay log; stderr aborted after 0 byte(s)"},
		{"aborts_only", Result{StdoutAbort: "input/output error", StdoutBytes: 40960, StderrAbort: "x", StderrBytes: 1},
			"stdout aborted after 40960 byte(s): input/output error; stderr aborted after 1 byte(s): x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (&SettledError{Result: tc.result}).Error(); got != tc.want {
				t.Fatalf("SettledError.Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.2 — CreateSent
// flips only once the create request reached the transport: a create whose
// reply never comes leaves PID empty but CreateSent true (the child may be
// running), while a create the transport refused to carry leaves it false.
func TestRunReportsCreateSentOnlyAfterPublish(t *testing.T) {
	t.Run("lost_reply_then_timeout", func(t *testing.T) {
		f := newClientFixture(t)
		run := f.start(t, 1500*time.Millisecond, io.Discard, io.Discard)
		create := f.negotiate(t)
		if create.req.Op != "create" {
			t.Fatalf("expected create, got %s", create.req.Op)
		}
		result, err := run.finish(t) // nobody answers; the caller's timeout expires
		if !errors.Is(err, context.DeadlineExceeded) || !result.CreateSent || result.PID != "" || result.Executed {
			t.Fatalf("create sent but unanswered: err=%v result=%+v", err, result)
		}
	})
	t.Run("client_refused_create", func(t *testing.T) {
		f := newClientFixture(t)
		// A create body over the wire limit never leaves the client: the call
		// refuses it before PublishRequest with the broker's own typed code
		// (S1 review ctl-V1), so nothing is sent and nothing can be admitted.
		spec := proto.ExecReq{Argv: []string{"/bin/true"}, Stdin: bytes.Repeat([]byte("x"), proto.ProcessRequestMaxBytes+1)}
		run := f.startSpec(t, clientTestBudget, spec, io.Discard, io.Discard)
		probe := f.next(t, "probe")
		sendClientReply(t, probe, f.capability(probe))
		result, err := run.finish(t)
		var pe *ProtocolError
		if !errors.As(err, &pe) || pe.Code != proto.CodeProcessTooLarge || pe.AfterCreate || result.CreateSent {
			t.Fatalf("a create the client refused must not count as sent: err=%v result=%+v", err, result)
		}
		if _, err := f.sub.NextMsg(300 * time.Millisecond); err == nil {
			t.Fatal("the over-limit create was published anyway")
		}
	})
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.2 — Executed is
// derived from evidence that the child ran; a failStart terminal (exit -1,
// no signal, two empty EOFs, never running) keeps it false so the CLI can
// classify a never-started command by the agent's own code.
func TestRunMarksExecutedOnlyAfterEvidence(t *testing.T) {
	t.Run("fail_start_shape", func(t *testing.T) {
		f := newClientFixture(t)
		run := f.start(t, clientTestBudget, io.Discard, io.Discard)
		create := f.negotiate(t)
		terminal := clientStatus(create)
		terminal.Phase, terminal.TerminalID, terminal.ExitCode, terminal.Error = "terminal", "original-terminal", -1, "remote_fs_spawn_timeout: /mnt hung"
		terminal.Stdout, terminal.Stderr = &proto.ProcessOutput{FIN: true}, &proto.ProcessOutput{FIN: true}
		sendClientReply(t, create, terminal)
		settle := f.next(t, "terminal-ack")
		settled := clientStatus(settle)
		settled.Phase, settled.TerminalID = "settled", settle.req.TerminalID
		sendClientReply(t, settle, settled)
		result, err := run.finish(t)
		var unclean *SettledError
		if !errors.As(err, &unclean) || result.Executed || result.TerminalError == "" || !result.CreateSent {
			t.Fatalf("failStart shape: err=%v result=%+v", err, result)
		}
	})
	t.Run("running_then_signal", func(t *testing.T) {
		f := newClientFixture(t)
		run := f.start(t, clientTestBudget, io.Discard, io.Discard)
		create := f.negotiate(t)
		sendClientReply(t, create, clientStatus(create)) // phase running, no bytes yet
		ack := f.next(t, "ack")
		terminal := clientStatus(ack)
		terminal.Phase, terminal.TerminalID, terminal.ExitCode, terminal.Signal = "terminal", "original-terminal", -1, 9
		terminal.Stdout, terminal.Stderr = &proto.ProcessOutput{FIN: true}, &proto.ProcessOutput{FIN: true}
		sendClientReply(t, ack, terminal)
		result := settleClient(t, f, run)
		if !result.Executed || result.Signal != 9 {
			t.Fatalf("a running child that died by signal is executed: %+v", result)
		}
	})
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.2 — a Kind
// "error" reply surfaces as a typed ProtocolError carrying the wire code and
// the phase; only the unsupported code (or its legacy prose from a peer
// without codes) may become ErrUnsupported, and only before create.
func TestRunSurfacesTypedRefusalWithPhase(t *testing.T) {
	t.Run("probe_permission", func(t *testing.T) {
		f := newClientFixture(t)
		run := f.start(t, clientTestBudget, io.Discard, io.Discard)
		probe := f.next(t, "probe")
		sendClientReply(t, probe, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: probe.req.CallID, Kind: "error", Code: "not_a_member", Error: "not_a_member"})
		_, err := run.finish(t)
		var pe *ProtocolError
		if !errors.As(err, &pe) || pe.Code != "not_a_member" || pe.AfterCreate || errors.Is(err, ErrUnsupported) {
			t.Fatalf("probe refusal: %v (%+v)", err, pe)
		}
	})
	t.Run("after_create_identity", func(t *testing.T) {
		f := newClientFixture(t)
		run := f.start(t, clientTestBudget, io.Discard, io.Discard)
		create := f.negotiate(t)
		sendClientReply(t, create, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: create.req.CallID, Kind: "error", Code: proto.CodeProcessInstanceLost, Error: "process request: lease was issued by another runtime instance"})
		result, err := run.finish(t)
		var pe *ProtocolError
		if !errors.As(err, &pe) || pe.Code != proto.CodeProcessInstanceLost || !pe.AfterCreate || !result.CreateSent {
			t.Fatalf("post-create refusal: %v (%+v) result=%+v", err, pe, result)
		}
	})
	t.Run("unsupported_by_code_and_by_legacy_prose", func(t *testing.T) {
		for _, tc := range []struct{ code, msg string }{{proto.CodeProcessUnsupported, "anything"}, {"", "unsupported process protocol"}} {
			f := newClientFixture(t)
			run := f.start(t, clientTestBudget, io.Discard, io.Discard)
			probe := f.next(t, "probe")
			sendClientReply(t, probe, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: probe.req.CallID, Kind: "error", Code: tc.code, Error: tc.msg})
			if _, err := run.finish(t); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("code=%q msg=%q: %v", tc.code, tc.msg, err)
			}
		}
	})
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.2 — a process
// refusal after create is never a reason to fall back to legacy exec: that
// would run the command a second time.
func TestRunNeverDowngradesAfterCreate(t *testing.T) {
	f := newClientFixture(t)
	run := f.start(t, clientTestBudget, io.Discard, io.Discard)
	create := f.negotiate(t)
	sendClientReply(t, create, clientStatus(create))
	ack := f.next(t, "ack")
	sendClientReply(t, ack, proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: ack.req.CallID, Kind: "error", Code: proto.CodeProcessUnsupported, Error: "unsupported process protocol"})
	_, err := run.finish(t)
	var pe *ProtocolError
	if errors.Is(err, ErrUnsupported) || !errors.As(err, &pe) || !pe.AfterCreate {
		t.Fatalf("an unsupported refusal after create must not downgrade: %v", err)
	}
	if msg, err := f.sub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("client kept talking after the refusal: %s", msg.Subject)
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.2 — an expired
// settlement completes the run only when the client already holds both stream
// ends and the terminal; the same code during resume is a real loss.
func TestRunConvergesOnExpiredSettlementOnlyWhenHoldingBothEnds(t *testing.T) {
	expired := func(ex clientExchange) proto.ProcessResumeReply {
		return proto.ProcessResumeReply{Version: proto.ProcessResumeVersion, CallID: ex.req.CallID, Kind: "error", Code: proto.CodeProcessExpired, Error: "process request: result expired; execution is not retried"}
	}
	t.Run("holding_both_ends", func(t *testing.T) {
		f := newClientFixture(t)
		var out bytes.Buffer
		run := f.start(t, clientTestBudget, &out, io.Discard)
		create := f.negotiate(t)
		terminal := terminalClientStatus(create)
		terminal.Stdout = &proto.ProcessOutput{Data: []byte("abc"), FIN: true}
		sendClientReply(t, create, terminal)
		settle := f.next(t, "terminal-ack")
		sendClientReply(t, settle, expired(settle))
		result, err := run.finish(t)
		if err != nil || result.ExitCode != 17 || out.String() != "abc" {
			t.Fatalf("expired settlement with both ends held must complete: err=%v result=%+v", err, result)
		}
	})
	t.Run("during_resume", func(t *testing.T) {
		f := newClientFixture(t)
		run := f.start(t, clientTestBudget, io.Discard, io.Discard)
		create := f.negotiate(t)
		sendClientReply(t, create, clientStatus(create))
		ack := f.next(t, "ack")
		sendClientReply(t, ack, expired(ack))
		_, err := run.finish(t)
		var pe *ProtocolError
		if !errors.As(err, &pe) || pe.Code != proto.CodeProcessExpired {
			t.Fatalf("expired during resume must surface as an error: %v", err)
		}
	})
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §4.2 — one Run keeps
// AttachEpoch 1 and the same inbox across a NATS reconnect of the ctl's own
// connection: nats.go re-subscribes the inbox, the registry attaches
// idempotently, and no republish storm is buffered while reconnecting.
func TestRunKeepsAttachEpochAcrossReconnect(t *testing.T) {
	f := newClientFixture(t)
	var out bytes.Buffer
	run := f.start(t, 3*clientTestBudget, &out, io.Discard)
	create := f.negotiate(t)
	reply := clientStatus(create)
	reply.Stdout = &proto.ProcessOutput{Data: []byte("abc")}
	sendClientReply(t, create, reply)
	ack := f.next(t, "ack")
	if ack.req.AttachEpoch != 1 || ack.msg.Reply != create.msg.Reply {
		t.Fatalf("epoch/inbox changed before the reconnect: %+v reply=%s", ack.req, ack.msg.Reply)
	}
	// Kick the ctl's connection server-side and do not answer the ack it
	// just sent; the client must come back on the same inbox with epoch 1.
	cid, err := f.nc.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.DisconnectClientByID(cid); err != nil {
		t.Fatalf("ctl connection not found on the server: %v", err)
	}
	var after clientExchange
	deadline := time.Now().Add(3 * clientTestBudget)
	copies := 0
	for {
		msg, err := f.sub.NextMsg(time.Until(deadline))
		if err != nil {
			t.Fatalf("client never resumed after the reconnect (copies seen=%d): %v", copies, err)
		}
		var req proto.ProcessResumeReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.CallID == ack.req.CallID {
			copies++ // the resend of the unanswered ack
			if copies > 3 {
				t.Fatalf("the unanswered ack was republished %d times: a reconnect must not buffer a storm", copies)
			}
		}
		if req.AttachEpoch != 1 || msg.Reply != create.msg.Reply {
			t.Fatalf("reconnect changed the attachment: epoch=%d reply=%s", req.AttachEpoch, msg.Reply)
		}
		if f.nc.Status() == nats.CONNECTED && !f.nc.IsReconnecting() {
			after = clientExchange{msg, req}
			break
		}
	}
	// The storm assertion proper (S1 review E14): with the reconnect done,
	// drain what nats.go flushed on reconnect. A client that republished
	// while reconnecting shows up here as further copies of the SAME ack
	// CallID; the reconnect-aware call loop sends at most one more (its own
	// attempt-timeout resend). This is where the "do not republish while
	// reconnecting" mutation must fail, not at settleClient's op mismatch.
	drained := 0
	drainUntil := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(drainUntil) {
		msg, err := f.sub.NextMsg(time.Until(drainUntil))
		if err != nil {
			break
		}
		var req proto.ProcessResumeReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.CallID == after.req.CallID {
			after = clientExchange{msg, req} // answer the newest copy below
			drained++
			continue
		}
		t.Fatalf("unexpected request while draining the reconnect: op=%s call=%d (ack call=%d)", req.Op, req.CallID, after.req.CallID)
	}
	if copies+drained > 3 {
		t.Fatalf("the unanswered ack was republished %d times across the reconnect: a reconnect must not buffer a storm", copies+drained)
	}
	terminal := terminalClientStatus(after)
	terminal.Stdout = &proto.ProcessOutput{Offset: 3, FIN: true}
	sendClientReply(t, after, terminal)
	result := settleClient(t, f, run)
	if result.ExitCode != 17 || out.String() != "abc" {
		t.Fatalf("result after reconnect: %+v out=%q", result, out.String())
	}
}

// origin: hot-update S1 internal review C4 — an idle reliable exec must not
// poll the broker at the 20 ms floor forever. The cadence backs off to
// idlePollMax while nothing arrives (≈ 8 ACKs in the first second instead of
// ~48) and snaps back to the floor the moment bytes flow, so the first byte
// after silence is delivered within idlePollMax plus one round trip.
func TestRunIdleAcksBackOffAndBytesResetTheCadence(t *testing.T) {
	f := newClientFixture(t)
	var stdout bytes.Buffer
	run := f.start(t, 10*time.Second, &stdout, io.Discard)
	create := f.negotiate(t)
	sendClientReply(t, create, clientStatus(create))
	// One idle second: every ack is answered with an empty running batch.
	idleAcks := 0
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		msg, err := f.sub.NextMsg(time.Until(deadline))
		if err != nil {
			break
		}
		var req proto.ProcessResumeReq
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.Op != "ack" {
			t.Fatalf("unexpected request during idle: %s (%v)", msg.Data, err)
		}
		idleAcks++
		sendClientReply(t, clientExchange{msg, req}, clientStatus(clientExchange{msg, req}))
	}
	// 20+40+80+160+250+250+250 ≈ 1.05 s → 6–8 acks; the old 20 ms floor gave ~48.
	if idleAcks < 3 || idleAcks > 12 {
		t.Fatalf("idle acks in one second = %d, want the backed-off cadence (3..12)", idleAcks)
	}
	// Bytes arrive: the next ack must follow at the 20 ms floor, not after a
	// 250 ms idle interval.
	ex := f.next(t, "ack")
	withData := clientStatus(ex)
	withData.Stdout = &proto.ProcessOutput{Offset: 0, Data: []byte("burst")}
	sent := time.Now()
	sendClientReply(t, ex, withData)
	next := f.next(t, "ack")
	if next.req.StdoutACK != 5 {
		t.Fatalf("ack after data = %d, want 5", next.req.StdoutACK)
	}
	if gap := time.Since(sent); gap > 150*time.Millisecond {
		t.Fatalf("the ack after data came %s later: bytes did not reset the idle cadence", gap)
	}
	sendClientReply(t, next, terminalClientStatus(next))
	if got := settleClient(t, f, run); got.ExitCode != 17 || stdout.String() != "burst" {
		t.Fatalf("settled result: %+v stdout=%q", got, stdout.String())
	}
}
