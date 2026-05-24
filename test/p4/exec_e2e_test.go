// Package p4_test exercises the P4 exec/ps surface end-to-end:
// embedded NATS + in-process broker + in-process agent + a ctl
// nats.Conn issuing ExecReqs, with stdout/stderr streaming back via
// the request's reply inbox.
//
// auth_callout is intentionally NOT enabled in this suite — the test/p3
// suite already covers the NATS-level permission boundary, and P4 only
// adds new business subjects on top. We seed the SQLite sessions /
// members rows directly so the broker's app-layer member checks pass.
package p4_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// ----- harness --------------------------------------------------------------
//
// Generic primitives (NATS, in-memory DB, identity, log sink) live in
// internal/testharness — same source-of-truth across test/p2..p7. The
// helpers below are the phase-specific bits (broker / agent wiring,
// seed shape) and a thin adapter layer over the package-locals the
// rest of this file already used.

func startNATS(t *testing.T) string                  { return testharness.StartNATS(t) }
func openDB(t *testing.T) *sql.DB                    { return testharness.OpenDB(t) }
func silentLog() *slog.Logger                        { return testharness.SilentLog() }
func freshUserPub(t *testing.T) (pub, fp string)     { return testharness.FreshUserPub(t) }

// seed creates the "lab" session and adds the given fingerprint as a
// member, so broker.handleExecReq's member check passes.
func seed(t *testing.T, db *sql.DB, sid, fp string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := session.Create(db, sid, sid, fp, "test-pin-hash", now); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
	// Create() already added fp as the owner; nothing else to do.
}

// startBroker boots a broker without auth_callout.
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

	// crude readiness: probe by direct nats.Connect.
	for i := 0; i < 30; i++ {
		if nc, err := nats.Connect(url); err == nil {
			nc.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let subscribes settle

	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	}
}

// startAgent boots an in-process agent for the given (sid, nid).
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

	// wait until the broker reports the node ONLINE in the DB OR a few seconds.
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

// runExec mimics what cmd/tether/exec.go does: streaming inbox + dispatch.
type execResult struct {
	PID      string
	Stdout   string
	Stderr   string
	ExitCode int
	Err      string
}

func runExec(t *testing.T, nc *nats.Conn, sid, actor, nid string, argv []string) execResult {
	t.Helper()
	body, _ := json.Marshal(proto.ExecReq{Argv: argv})

	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy(sid, actor, nid, "exec"), inbox, body); err != nil {
		t.Fatal(err)
	}

	var res execResult
	deadline := time.Now().Add(5 * time.Second)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatalf("exec timed out, partial: %+v", res)
		}
		msg, err := sub.NextMsg(left)
		if err != nil {
			t.Fatalf("NextMsg: %v (partial=%+v)", err, res)
		}
		var c proto.ExecChunk
		if err := json.Unmarshal(msg.Data, &c); err != nil {
			t.Fatalf("malformed chunk: %v", err)
		}
		switch c.Kind {
		case "started":
			res.PID = c.PID
		case "stdout":
			res.Stdout += string(c.Data)
		case "stderr":
			res.Stderr += string(c.Data)
		case "exit":
			res.ExitCode = c.ExitCode
			return res
		case "error":
			res.Err = c.Error
			return res
		}
	}
}

// ----- tests ----------------------------------------------------------------

func TestExecHelloWorld(t *testing.T) {
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

	res := runExec(t, nc, "lab", pub, "lab-1", []string{"echo", "hello"})
	if res.Err != "" {
		t.Fatalf("error chunk: %s", res.Err)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code: got %d want 0", res.ExitCode)
	}
	if !strings.Contains(res.Stdout, "hello") {
		t.Errorf("stdout %q must contain 'hello'", res.Stdout)
	}
	if res.PID == "" {
		t.Error("no PID in started chunk")
	}
}

func TestExecFalseExitCode(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	res := runExec(t, nc, "lab", pub, "lab-1", []string{"sh", "-c", "exit 7"})
	if res.Err != "" {
		t.Fatalf("error: %s", res.Err)
	}
	if res.ExitCode != 7 {
		t.Errorf("exit code: got %d want 7", res.ExitCode)
	}
}

func TestExecStderrCapture(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	res := runExec(t, nc, "lab", pub, "lab-1",
		[]string{"sh", "-c", "echo to-stdout; echo to-stderr 1>&2; exit 0"})
	if res.ExitCode != 0 {
		t.Fatalf("exit %d, err=%q", res.ExitCode, res.Err)
	}
	if !strings.Contains(res.Stdout, "to-stdout") {
		t.Errorf("stdout missing: %q", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "to-stderr") {
		t.Errorf("stderr missing: %q", res.Stderr)
	}
}

// PsReq goes via ctrl.by.<actor>.s.<sid>.ps.req. The broker queries the
// processes table and replies. Verify a finished `echo` shows up as
// EXITED (we pass --all logic via the test by reading every entry).
func TestPsListsExitedProcess(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seed(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	res := runExec(t, nc, "lab", pub, "lab-1", []string{"echo", "ps-test"})
	if res.ExitCode != 0 {
		t.Fatalf("exec failed: %+v", res)
	}

	// proc events are async; poll the DB until the row reflects EXITED.
	deadline := time.Now().Add(2 * time.Second)
	for {
		p, err := proc.Get(db, res.PID)
		if err == nil && p.Status == proc.StateExited {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("process never reached EXITED in DB: pid=%s", res.PID)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Now ask via the ps subject and confirm it appears. The new
	// default `PsReq{}` filters to active processes only; this test
	// expects to see an EXITED row, so it sends IncludeExited=true
	// (the `-a` semantic).
	body, _ := json.Marshal(proto.PsReq{IncludeExited: true})
	msg, err := nc.Request(proto.SubjCtrlPs(pub, "lab"), body, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.PsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != "" {
		t.Fatalf("ps rejected: %s %s", resp.Code, resp.Error)
	}
	found := false
	for _, p := range resp.Processes {
		if p.PID == res.PID && p.Status == "EXITED" && p.ExitCode == 0 {
			found = true
		}
	}
	if !found {
		t.Errorf("ps did not include the EXITED echo: %+v", resp.Processes)
	}
}

func TestExecRejectedForNonMember(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	_, ownerFP := freshUserPub(t)
	seed(t, db, "lab", ownerFP) // owner is some other identity
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()

	intruder, _ := freshUserPub(t)
	nc, _ := nats.Connect(url)
	defer nc.Close()

	res := runExec(t, nc, "lab", intruder, "lab-1", []string{"echo", "x"})
	if res.Err == "" {
		t.Fatalf("non-member exec should be rejected, got %+v", res)
	}
	if !strings.Contains(res.Err, "not_a_member") {
		t.Errorf("expected not_a_member error, got %q", res.Err)
	}
}

func TestExecRejectedForUnknownSession(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, _ := freshUserPub(t)
	defer startBroker(t, url, db)()

	nc, _ := nats.Connect(url)
	defer nc.Close()

	res := runExec(t, nc, "ghost", pub, "lab-1", []string{"echo", "x"})
	if res.Err == "" {
		t.Fatalf("exec on unknown session should be rejected, got %+v", res)
	}
	if !strings.Contains(res.Err, "session_not_found") {
		t.Errorf("expected session_not_found, got %q", res.Err)
	}
}
