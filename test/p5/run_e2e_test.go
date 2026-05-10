// Package p5_test exercises the P5 PTY `run` flow end-to-end with
// embedded NATS + in-process broker + in-process agent. Same anonymous
// harness shape as test/p4/exec_e2e_test.go (auth_callout coverage is
// already proven there and in test/p3); P5 only adds new business
// subjects on top, so we don't re-validate the JWT layer here.
package p5_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// ----- harness --------------------------------------------------------------
//
// Generic primitives live in internal/testharness; the helpers below
// are the phase-specific bits.

func startNATS(t *testing.T) string                  { return testharness.StartNATS(t) }
func openDB(t *testing.T) *sql.DB                    { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                        { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string)     { return testharness.FreshUserPub(t) }

func seed(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	if _, err := session.Create(db, sid, sid, fp, "test-pin-hash", time.Now().UTC()); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
}

func startBroker(t *testing.T, url string, db *sql.DB) func() {
	t.Helper()
	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	for i := 0; i < 30; i++ {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

func startAgent(t *testing.T, url, sid, nid string) func() {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit")
		}
	}
}

// runResult is what runRun collects across one full lifecycle.
type runResult struct {
	PID      string
	Out      bytes.Buffer
	ExitCode int
	Failed   string
}

// runRun mimics what cmd/tether/run.go does (minus raw mode / SIGWINCH /
// stdin pump): publish run.req, wait for ready, sub .out, pub attach,
// drain .out + lifecycle until exit or failed.
func runRun(t *testing.T, nc *nats.Conn, sid, actor, nid string, argv []string, stdin []byte) runResult {
	t.Helper()

	body, _ := json.Marshal(proto.RunReq{Argv: argv, Cols: 80, Rows: 24})

	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy(sid, actor, nid, "run"), inbox, body); err != nil {
		t.Fatal(err)
	}

	// Step A: wait for the first chunk (ready or failed).
	first, err := sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatalf("waiting for first chunk: %v", err)
	}
	var initial proto.RunChunk
	if err := json.Unmarshal(first.Data, &initial); err != nil {
		t.Fatalf("malformed first chunk: %v", err)
	}
	if initial.Kind == "failed" {
		return runResult{Failed: initial.Reason}
	}
	if initial.Kind != "ready" {
		t.Fatalf("expected ready first chunk, got %+v", initial)
	}
	res := runResult{PID: initial.PID}

	// Step B: subscribe pty.<pid>.out BEFORE attach.
	outCh := make(chan []byte, 64)
	outSub, err := nc.Subscribe(proto.SubjPtyOut(sid, res.PID), func(m *nats.Msg) {
		b := make([]byte, len(m.Data))
		copy(b, m.Data)
		outCh <- b
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outSub.Unsubscribe() }()

	// Step C: attach.
	att, _ := json.Marshal(proto.PtyAttachEvent{Cols: 80, Rows: 24})
	if err := nc.Publish(proto.SubjPtyAttach(sid, res.PID), att); err != nil {
		t.Fatal(err)
	}

	// Step D: optional stdin.
	if len(stdin) > 0 {
		// Wait a beat so the agent has fork+exec'd before we push bytes.
		time.Sleep(50 * time.Millisecond)
		if err := nc.Publish(proto.SubjPtyIn(sid, res.PID), stdin); err != nil {
			t.Fatal(err)
		}
	}

	// Step E: drain until exit/failed on the inbox.
	deadline := time.Now().Add(30 * time.Second)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatalf("run timed out, partial: pid=%s out=%q", res.PID, res.Out.String())
		}
		select {
		case b := <-outCh:
			res.Out.Write(b)
		default:
			msg, err := sub.NextMsg(50 * time.Millisecond)
			if err == nil {
				var c proto.RunChunk
				if err := json.Unmarshal(msg.Data, &c); err != nil {
					continue
				}
				switch c.Kind {
				case "started":
					// ok
				case "exit":
					// drain remaining out
					drainTimer := time.NewTimer(150 * time.Millisecond)
					for {
						select {
						case b := <-outCh:
							res.Out.Write(b)
						case <-drainTimer.C:
							res.ExitCode = c.ExitCode
							return res
						}
					}
				case "failed":
					res.Failed = c.Reason
					return res
				}
			}
		}
	}
}

// ----- tests ----------------------------------------------------------------

func TestRunHelloWorld(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	res := runRun(t, nc, "lab", pub, "lab-1", []string{"echo", "hello-pty"}, nil)
	if res.Failed != "" {
		t.Fatalf("run failed: %s", res.Failed)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code: got %d want 0", res.ExitCode)
	}
	if !strings.Contains(res.Out.String(), "hello-pty") {
		t.Errorf("output missing 'hello-pty': %q", res.Out.String())
	}
}

func TestRunExitCodePropagates(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	res := runRun(t, nc, "lab", pub, "lab-1", []string{"sh", "-c", "exit 11"}, nil)
	if res.Failed != "" {
		t.Fatalf("run failed: %s", res.Failed)
	}
	if res.ExitCode != 11 {
		t.Errorf("exit code: got %d want 11", res.ExitCode)
	}
}

// TestAttachTimeout is the architecture-mandated test for C.5.1:
// agent pubs ready, ctl never sends attach → ctl receives a
// failed{reason:attach_timeout} chunk; agent leaks no PTY.
//
// Default deadline is 15s (raised from 3s for high-RTT WSS deployments).
// Override via TETHER_AGENT_ATTACH_DEADLINE so the test stays fast —
// 500ms is plenty for the in-process loopback scenario here.
func TestAttachTimeout(t *testing.T) {
	t.Setenv("TETHER_AGENT_ATTACH_DEADLINE", "500ms")
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.RunReq{Argv: []string{"sleep", "60"}, Cols: 80, Rows: 24})
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy("lab", pub, "lab-1", "run"), inbox, body); err != nil {
		t.Fatal(err)
	}

	// Read 'ready'.
	msg1, err := sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ready proto.RunChunk
	_ = json.Unmarshal(msg1.Data, &ready)
	if ready.Kind != "ready" {
		t.Fatalf("expected ready, got %+v", ready)
	}

	// Deliberately do NOT publish attach. Wait for failed within 2s
	// (agent timeout overridden to 500ms above).
	msg2, err := sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatalf("waiting for attach_timeout: %v", err)
	}
	var failed proto.RunChunk
	_ = json.Unmarshal(msg2.Data, &failed)
	if failed.Kind != "failed" || failed.Reason != "attach_timeout" {
		t.Errorf("expected failed{attach_timeout}, got %+v", failed)
	}
}

// Kill verb: Ctrl-C sends SIGINT to the child's process group. We trap
// SIGINT in a small shell loop to verify exit code 130 (= 128 + SIGINT).
func TestKillSendsSIGINT(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	// Long-running child that traps SIGINT and exits 130.
	body, _ := json.Marshal(proto.RunReq{
		Argv: []string{"sh", "-c", "trap 'exit 130' INT; sleep 30"},
		Cols: 80, Rows: 24,
	})
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy("lab", pub, "lab-1", "run"), inbox, body); err != nil {
		t.Fatal(err)
	}
	msg1, err := sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ready proto.RunChunk
	_ = json.Unmarshal(msg1.Data, &ready)
	if ready.Kind != "ready" {
		t.Fatalf("expected ready, got %+v", ready)
	}
	att, _ := json.Marshal(proto.PtyAttachEvent{Cols: 80, Rows: 24})
	if err := nc.Publish(proto.SubjPtyAttach("lab", ready.PID), att); err != nil {
		t.Fatal(err)
	}

	// Wait for `started` first so the trap is installed.
	startMsg, err := sub.NextMsg(30 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var startedChunk proto.RunChunk
	_ = json.Unmarshal(startMsg.Data, &startedChunk)
	if startedChunk.Kind != "started" {
		t.Fatalf("expected started, got %+v", startedChunk)
	}
	time.Sleep(150 * time.Millisecond) // trap install slack

	// Send kill.req.
	killBody, _ := json.Marshal(proto.KillReq{PID: ready.PID, Signal: int(2)})
	resp, err := nc.Request(
		proto.SubjCmdBy("lab", pub, "lab-1", "kill"), killBody, 2*time.Second)
	if err != nil {
		t.Fatalf("kill.req: %v", err)
	}
	var killResp proto.KillResp
	_ = json.Unmarshal(resp.Data, &killResp)
	if !killResp.OK {
		t.Fatalf("kill rejected: %+v", killResp)
	}

	// Expect exit 130 within 2s.
	deadline := time.Now().Add(3 * time.Second)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatal("did not receive exit chunk after kill")
		}
		msg, err := sub.NextMsg(left)
		if err != nil {
			t.Fatal(err)
		}
		var c proto.RunChunk
		_ = json.Unmarshal(msg.Data, &c)
		if c.Kind == "exit" {
			if c.ExitCode != 130 {
				t.Errorf("exit code on SIGINT: got %d want 130", c.ExitCode)
			}
			return
		}
	}
}

// run is gated on session ACTIVE — same C.1 §6 rule as exec.
func TestRunRejectedForDeletingSession(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	if err := session.Tombstone(db, "lab", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.RunReq{Argv: []string{"echo", "x"}})
	resp, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "run"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var c proto.RunChunk
	_ = json.Unmarshal(resp.Data, &c)
	if c.Kind != "failed" || !strings.Contains(c.Reason, "session_not_found_or_deleting") {
		t.Errorf("expected session_not_found_or_deleting, got %+v", c)
	}
}

// run to a non-existent node returns failed{node_not_found} promptly,
// not a NATS timeout (mirrors exec's F2 fix).
func TestRunRejectedForMissingNode(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	body, _ := json.Marshal(proto.RunReq{Argv: []string{"echo", "x"}})
	resp, err := nc.Request(proto.SubjCmdBy("lab", pub, "ghost", "run"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var c proto.RunChunk
	_ = json.Unmarshal(resp.Data, &c)
	if c.Kind != "failed" || !strings.Contains(c.Reason, "node_not_found") {
		t.Errorf("expected node_not_found, got %+v", c)
	}
}
