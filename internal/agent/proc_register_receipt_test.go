package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// proc_register_receipt_test.go — real register/courier requests over embedded
// NATS, plus a real exec child. Only broker replies are controlled by the test.
// No broker database reconciliation, hot exec or crash durability is claimed.
// origin: docs/reviews/hot-update-plan.md §4.2, register ACK ownership.

type receiptFixture struct {
	a      *Agent
	client *nats.Conn
	server *nats.Conn
	regs   *nats.Subscription
	events *nats.Subscription
	ctx    context.Context
}

func newReceiptFixture(t *testing.T) *receiptFixture {
	t.Helper()
	url := startNATS(t)
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "receipt-agent", Home: t.TempDir(),
		Logger: testharness.SilentLog(), RemoteFSMode: "off"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	server, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	regs, err := server.SubscribeSync(proto.SubjNodeRegister("lab", "receipt-agent"))
	if err != nil {
		t.Fatal(err)
	}
	events, err := server.SubscribeSync(proto.SubjectPrefix + ".s.lab.ev.node.receipt-agent.proc.*.*")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.FlushTimeout(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	a.setRunCtx(ctx)
	a.ncBox.Store(client)
	t.Cleanup(func() { cancel(); a.reapManagedChildren() })
	return &receiptFixture{a, client, server, regs, events, ctx}
}

func receiptNextRegister(t *testing.T, f *receiptFixture) (*nats.Msg, proto.NodeRegisterReq) {
	t.Helper()
	msg, err := f.regs.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var req proto.NodeRegisterReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		t.Fatal(err)
	}
	if msg.Reply == "" || req.NID != "receipt-agent" {
		t.Fatalf("not a real register request: reply=%q body=%+v", msg.Reply, req)
	}
	return msg, req
}

func receiptRespond(t *testing.T, msg *nats.Msg, body any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.Respond(data); err != nil {
		t.Fatal(err)
	}
}

func receiptLaunchRegister(t *testing.T, f *receiptFixture) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		defer close(done)
		resp, err := f.a.register(ctx, f.client)
		if err == nil && (!resp.OK || resp.Lease != nil) {
			err = fmt.Errorf("unexpected register verdict: %+v", resp)
		}
		result <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("register call did not stop")
		}
	})
	return result
}

func receiptWaitRegister(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("register did not finish after its ACK")
	}
}

func receiptPending(t *testing.T, c *procCourier, pid string, kind procEventKind) *pendingProcEvent {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending[courierKey(pid, kind)]
}

// The observable positive oracle is an actual broker-bound event with its real
// code, not only a private queue count. Delivery is driven in bounded rounds.
func receiptDeliverOne(t *testing.T, f *receiptFixture, pid string, kind procEventKind, code int) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f.a.courier.deliverRound(f.ctx) }()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("courier delivery did not stop")
		}
	})
	msg, err := f.events.NextMsg(4 * time.Second)
	if err != nil {
		t.Fatalf("retained %s event did not reach NATS: %v", kind, err)
	}
	_, _, gotPID, gotKind, ok := proto.ParseEvProc(msg.Subject)
	if !ok || gotPID != pid || gotKind != string(kind) {
		t.Fatalf("wrong event: %s", msg.Subject)
	}
	if kind == procEventExit {
		var event proto.ProcExitEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil || event.PID != pid || event.ExitCode != code || event.EndedAt.IsZero() {
			t.Fatalf("wrong exit payload: %+v err=%v", event, err)
		}
	}
	receiptRespond(t, msg, proto.ProcEventAck{OK: true})
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("courier did not finish after event ACK")
	}
	if receiptPending(t, f.a.courier, pid, kind) != nil {
		t.Fatal("positive event ACK did not settle the delivered event")
	}
}

func TestRegisterReceiptKeepsLateChildExit(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newReceiptFixture(t)
	done := make(chan struct{})
	go func() { defer close(done); f.a.onNATSReconnect(f.client) }()
	t.Cleanup(func() {
		f.client.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("reconnect did not stop")
		}
	})
	register, req := receiptNextRegister(t, f)
	if len(req.LocalProcesses) != 0 {
		t.Fatalf("late-child fixture had a nonempty original census: %+v", req.LocalProcesses)
	}
	inbox := f.server.NewInbox()
	chunks, err := f.server.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.FlushTimeout(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "exit 23"}})
	if err != nil {
		t.Fatal(err)
	}
	childDone := make(chan struct{})
	go func() {
		defer close(childDone)
		f.a.handleExecForwarded(f.client, &nats.Msg{Reply: inbox, Data: payload})
	}()
	t.Cleanup(func() {
		f.a.reapManagedChildren()
		select {
		case <-childDone:
		case <-time.After(5 * time.Second):
			t.Error("real child handler did not stop")
		}
	})
	var pid string
	for _, kind := range []string{"started", "exit"} {
		msg, err := chunks.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var chunk proto.ExecChunk
		if err := json.Unmarshal(msg.Data, &chunk); err != nil || chunk.Kind != kind || chunk.PID == "" {
			t.Fatalf("real exec chunk: %+v err=%v", chunk, err)
		}
		if kind == "started" {
			pid = chunk.PID
		} else if chunk.PID != pid || chunk.ExitCode != 23 {
			t.Fatalf("child identity/result changed: %+v", chunk)
		}
	}
	select {
	case <-childDone:
	case <-time.After(3 * time.Second):
		t.Fatal("child did not transfer its result before register ACK")
	}
	if ev := receiptPending(t, f.a.courier, pid, procEventExit); ev == nil || ev.rc != 23 {
		t.Fatal("real late child did not enqueue the exit prerequisite")
	}
	receiptRespond(t, register, proto.NodeRegisterResp{OK: true})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect did not complete")
	}
	receiptDeliverOne(t, f, pid, procEventStarted, 0)
	receiptDeliverOne(t, f, pid, procEventExit, 23)
}

func TestRegisterReceiptConcurrentRepliesAreScoped(t *testing.T) {
	f := newReceiptFixture(t)
	f.a.courier.enqueueExit("first", 11)
	firstDone := receiptLaunchRegister(t, f)
	first, req1 := receiptNextRegister(t, f)
	if len(req1.LocalProcesses) != 1 || req1.LocalProcesses[0].PID != "first" {
		t.Fatalf("first request: %+v", req1.LocalProcesses)
	}
	f.a.courier.enqueueExit("second", 22)
	secondDone := receiptLaunchRegister(t, f)
	second, req2 := receiptNextRegister(t, f)
	if len(req2.LocalProcesses) != 2 || req2.LocalProcesses[1].PID != "second" {
		t.Fatalf("second request did not include its new exit: %+v", req2.LocalProcesses)
	}
	f.a.courier.enqueueExit("third", 33)
	// Reply in reverse order; second's retained-write-failure row must not be
	// mistaken for acknowledgement by the older first request.
	receiptRespond(t, second, proto.NodeRegisterResp{OK: true, AcceptedProcesses: []string{"second"}})
	receiptWaitRegister(t, secondDone)
	if receiptPending(t, f.a.courier, "first", procEventExit) != nil {
		t.Fatal("represented terminal exit was not settled by the newer reply")
	}
	receiptRespond(t, first, proto.NodeRegisterResp{OK: true})
	receiptWaitRegister(t, firstDone)
	if ev := receiptPending(t, f.a.courier, "second", procEventExit); ev == nil || ev.rc != 22 {
		t.Fatal("older reply stole the second request's retained exit")
	}
	if ev := receiptPending(t, f.a.courier, "third", procEventExit); ev == nil || ev.rc != 33 {
		t.Fatal("reply settled an exit absent from both requests")
	}
}

func TestRegisterReceiptRetryCarriesFreshExit(t *testing.T) {
	f := newReceiptFixture(t)
	done := receiptLaunchRegister(t, f)
	first, req1 := receiptNextRegister(t, f)
	if len(req1.LocalProcesses) != 0 {
		t.Fatal("first attempt must precede the late exit")
	}
	f.a.courier.enqueueExit("retry-exit", 41)
	receiptRespond(t, first, proto.NodeRegisterResp{Code: proto.CodeLeaderUnavailable})
	second, req2 := receiptNextRegister(t, f)
	if len(req2.LocalProcesses) != 1 || req2.LocalProcesses[0].PID != "retry-exit" || req2.LocalProcesses[0].RC == nil || *req2.LocalProcesses[0].RC != 41 {
		t.Fatalf("retry reused a stale census: %+v", req2.LocalProcesses)
	}
	receiptRespond(t, second, proto.NodeRegisterResp{OK: true})
	receiptWaitRegister(t, done)
	if receiptPending(t, f.a.courier, "retry-exit", procEventExit) != nil {
		t.Fatal("fresh retry body did not carry its own matching receipt")
	}
}

func TestRegisterReceiptDelayedACKKeepsReplacement(t *testing.T) {
	for _, channel := range []string{"register", "courier"} {
		t.Run(channel, func(t *testing.T) {
			f := newReceiptFixture(t)
			f.a.courier.enqueueExit("same-key", 17)
			var msg *nats.Msg
			var done <-chan error
			if channel == "register" {
				done = receiptLaunchRegister(t, f)
				var req proto.NodeRegisterReq
				msg, req = receiptNextRegister(t, f)
				if len(req.LocalProcesses) != 1 || req.LocalProcesses[0].RC == nil || *req.LocalProcesses[0].RC != 17 {
					t.Fatalf("old register did not represent the original event: %+v", req.LocalProcesses)
				}
			} else {
				result := make(chan error, 1)
				done = result
				old := receiptPending(t, f.a.courier, "same-key", procEventExit)
				go func() { f.a.courier.attempt(f.ctx, f.client, old); result <- nil }()
				var err error
				msg, err = f.events.NextMsg(4 * time.Second)
				if err != nil {
					t.Fatal(err)
				}
				var event proto.ProcExitEvent
				if err := json.Unmarshal(msg.Data, &event); err != nil || event.ExitCode != 17 {
					t.Fatalf("old courier request: %+v err=%v", event, err)
				}
			}
			f.a.courier.enqueueExit("same-key", 29)
			if channel == "register" {
				receiptRespond(t, msg, proto.NodeRegisterResp{OK: true})
			} else {
				receiptRespond(t, msg, proto.ProcEventAck{OK: true})
			}
			receiptWaitRegister(t, done)
			receiptDeliverOne(t, f, "same-key", procEventExit, 29)
		})
	}
}

func TestRegisterReceiptRequiresExactExitRepresentation(t *testing.T) {
	for _, variant := range []string{"missing-receipt", "missing-row", "other-pid", "running", "nil-rc", "wrong-rc", "nil-ended", "wrong-ended", "rejected", "lease", "still-running"} {
		t.Run(variant, func(t *testing.T) {
			c := newProcCourier(nil)
			c.enqueueExit("exit", 73)
			receipt, rows := c.registerReceipt(), c.pendingExitSnapshot()
			original := receipt[courierKey("exit", procEventExit)]
			response := proto.NodeRegisterResp{OK: true}
			switch variant {
			case "missing-receipt":
				receipt = nil
			case "missing-row":
				rows = nil
			case "other-pid":
				rows[0].PID = "other"
			case "running":
				rows[0].State = "running"
			case "nil-rc":
				rows[0].RC = nil
			case "wrong-rc":
				*rows[0].RC = 74
			case "nil-ended":
				rows[0].EndedAt = nil
			case "wrong-ended":
				*rows[0].EndedAt = rows[0].EndedAt.Add(time.Nanosecond)
			case "rejected":
				response.OK = false
			case "lease":
				response.Lease = &proto.NodeLease{AssignedNID: "other"}
			case "still-running":
				response.AcceptedProcesses = []string{"exit"}
			}
			c.onRegisterSuccess(receipt, rows, response)
			if ev := receiptPending(t, c, "exit", procEventExit); ev != original || ev.rc != 73 {
				t.Fatal("unacknowledged event was removed or altered")
			}
			// Positive control: an old broker already has it terminal, so it
			// is absent from BOTH response lists. Reconciled-list membership
			// must not be required, and 'never settle' must not pass either.
			c.onRegisterSuccess(c.registerReceipt(), c.pendingExitSnapshot(), proto.NodeRegisterResp{OK: true})
			if receiptPending(t, c, "exit", procEventExit) != nil {
				t.Fatal("represented terminal event did not settle against an old-broker response")
			}
		})
	}
}
