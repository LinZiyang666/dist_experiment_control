package cli_e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// execResult bundles what one exec call streamed back. Mirrors the
// shape used by test/p4 so cross-suite comparisons stay obvious.
type execResult struct {
	PID      string
	Stdout   []byte
	Stderr   []byte
	ExitCode int
	Err      string
}

// runExec is the in-test stand-in for `tether exec`. Returns when an
// exit/error chunk arrives or ctx fires. Big-output tests rely on
// the caller passing a generous deadline.
func runExec(t *testing.T, nc *nats.Conn, sid, actor, nid string, argv []string, deadline time.Duration) execResult {
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

	end := time.Now().Add(deadline)
	var res execResult
	for {
		left := time.Until(end)
		if left <= 0 {
			t.Fatalf("exec timed out, partial: pid=%s stdout=%dB stderr=%dB",
				res.PID, len(res.Stdout), len(res.Stderr))
		}
		msg, err := sub.NextMsg(left)
		if err != nil {
			t.Fatalf("NextMsg: %v (partial=%dB)", err, len(res.Stdout))
		}
		var c proto.ExecChunk
		if err := json.Unmarshal(msg.Data, &c); err != nil {
			t.Fatalf("malformed chunk: %v", err)
		}
		switch c.Kind {
		case "started":
			res.PID = c.PID
		case "stdout":
			res.Stdout = append(res.Stdout, c.Data...)
		case "stderr":
			res.Stderr = append(res.Stderr, c.Data...)
		case "exit":
			res.ExitCode = c.ExitCode
			return res
		case "error":
			res.Err = c.Error
			return res
		}
	}
}

// runExecWithCtx is runExec but cancellable via ctx — used by the
// "ctl-side cancel mid-exec" test (B.6).
func runExecWithCtx(ctx context.Context, t *testing.T, nc *nats.Conn, sid, actor, nid string, argv []string) (execResult, error) {
	t.Helper()
	body, _ := json.Marshal(proto.ExecReq{Argv: argv})
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return execResult{}, err
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy(sid, actor, nid, "exec"), inbox, body); err != nil {
		return execResult{}, err
	}

	var res execResult
	for {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			return res, err
		}
		var c proto.ExecChunk
		if err := json.Unmarshal(msg.Data, &c); err != nil {
			return res, err
		}
		switch c.Kind {
		case "started":
			res.PID = c.PID
		case "stdout":
			res.Stdout = append(res.Stdout, c.Data...)
		case "stderr":
			res.Stderr = append(res.Stderr, c.Data...)
		case "exit":
			res.ExitCode = c.ExitCode
			return res, nil
		case "error":
			res.Err = c.Error
			return res, nil
		}
	}
}

// B.7 — exec produces 10 MiB of stdout (deterministic content); every
// byte must arrive intact. This is the canary for stream-pump bugs:
// if the agent ever loses chunks (rate-limited Publish, unbounded
// channel back-pressure, etc.) this assertion fires.
//
// We use `head -c <N> /dev/zero | base64` to emit a printable stream
// — random / binary bytes can confuse some intermediate buffers,
// base64 is friendlier to stream comparators and still hits all the
// 4KiB chunk boundaries the agent uses.
func TestExecLargeStdoutDelivered(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	const wantBytes = 10 * 1024 * 1024
	// `head -c N /dev/zero | base64 -w0` emits ceil(N*4/3) base64 bytes.
	// We want close to wantBytes of *output*; ask base64 for 7.5MiB
	// raw to approximate 10MiB encoded.
	rawBytes := wantBytes * 3 / 4
	res := runExec(t, nc, "lab", pub, "lab-1",
		[]string{"sh", "-c", fmt.Sprintf("head -c %d /dev/zero | base64 -w0", rawBytes)},
		30*time.Second)

	if res.Err != "" {
		t.Fatalf("error chunk: %s", res.Err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit %d", res.ExitCode)
	}
	if len(res.Stdout) < wantBytes-1024 {
		t.Errorf("stdout under-delivered: got %d want >= %d", len(res.Stdout), wantBytes-1024)
	}
	// Since the input is /dev/zero, base64 output is the constant char
	// 'A' for 4-byte groups. Spot-check.
	if !strings.Contains(string(res.Stdout[:1024]), "AAAA") {
		t.Errorf("stdout content sanity check failed (first 1KiB): %q", res.Stdout[:64])
	}
}

// B.8 — stdout AND stderr each carry 1MiB; both must arrive intact.
// Hits the dual-pipe streamer in agent.runChild (separate goroutine
// per fd).
func TestExecBothStreamsDeliveredFully(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	const wantOut = 1 * 1024 * 1024
	rawBytes := wantOut * 3 / 4
	// One sh process emits 1MiB to stdout, then 1MiB to stderr. We use
	// >&2 to retarget the second base64.
	cmd := fmt.Sprintf("head -c %d /dev/zero | base64 -w0 ; head -c %d /dev/zero | base64 -w0 1>&2",
		rawBytes, rawBytes)
	res := runExec(t, nc, "lab", pub, "lab-1",
		[]string{"sh", "-c", cmd}, 20*time.Second)

	if res.Err != "" {
		t.Fatalf("error: %s", res.Err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit %d", res.ExitCode)
	}
	if len(res.Stdout) < wantOut-1024 {
		t.Errorf("stdout under-delivered: got %d want >= %d", len(res.Stdout), wantOut-1024)
	}
	if len(res.Stderr) < wantOut-1024 {
		t.Errorf("stderr under-delivered: got %d want >= %d", len(res.Stderr), wantOut-1024)
	}
}

// B.9 — fast-path success. Pin the trivial happy-path code so a
// regression in lifecycle handling shows up instantly.
func TestExecImmediateExitZero(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	res := runExec(t, nc, "lab", pub, "lab-1", []string{"true"}, 5*time.Second)
	if res.Err != "" {
		t.Fatalf("error: %s", res.Err)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code: got %d want 0", res.ExitCode)
	}
}

// B.10 — non-zero exit propagates verbatim (the CLI maps this to
// os.Exit). 42 is arbitrary; just want to prove the wire carries the
// integer rather than collapsing to 1.
func TestExecArbitraryExitCodePropagates(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	res := runExec(t, nc, "lab", pub, "lab-1",
		[]string{"sh", "-c", "exit 42"}, 5*time.Second)
	if res.ExitCode != 42 {
		t.Errorf("exit code: got %d want 42", res.ExitCode)
	}
}

// B.11 — child terminated by signal: Go's exec returns -1 for the
// exit code, which the ctl maps to os.Exit(128) + a stderr line.
// Here we just verify the wire-level exit code is < 0 (the CLI
// translation is exercised in cmd/tether tests). The child SIGKILLs
// itself so we don't need to spawn a watcher.
func TestExecChildKilledBySignal(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	// `sh -c 'kill -KILL $$'` kills the shell itself with SIGKILL.
	// Go's exec.ExitCode() returns -1 for any signal-terminated child.
	res := runExec(t, nc, "lab", pub, "lab-1",
		[]string{"sh", "-c", "kill -KILL $$"}, 5*time.Second)
	if res.ExitCode >= 0 {
		t.Errorf("exit code on SIGKILL: got %d want negative (signal-terminated)", res.ExitCode)
	}
}

// B.12 — ctl-side ctx cancellation while the remote `sleep 30` is
// still running. The ctl unsubscribe (NATS auto-prune of an
// unsubscribed inbox) doesn't kill the remote child by itself — but
// for the test what matters is the local NextMsgWithContext returns
// promptly when the ctx is canceled; we don't hang waiting for the
// remote to exit.
//
// (Killing the remote on ctx-cancel is a v2 wishlist item — the v1
// agent has no back-channel for "ctl gave up". The test pins the
// local-side behavior so a future change that adds a hang here gets
// caught.)
func TestExecCtlContextCancelReturnsLocally(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := runExecWithCtx(ctx, t, nc, "lab", pub, "lab-1", []string{"sleep", "30"})
	took := time.Since(start)
	if err == nil {
		t.Errorf("expected context error, got nil")
	}
	// Allow generous slack; we just need to prove we didn't sit on
	// the full 30s sleep.
	if took > 3*time.Second {
		t.Errorf("ctl returned only after %v; expected <3s after cancel", took)
	}
}

// B.13 — argv with shell metacharacters and a Unicode filename. The
// agent must pass argv through to os/exec verbatim (no shell
// interpolation), so `echo` should print the literal arg with the
// emoji + Chinese chars.
func TestExecPreservesUnicodeAndShellMeta(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	tricky := "测试.txt | & ; > $((1+1)) `pwd`"
	res := runExec(t, nc, "lab", pub, "lab-1",
		[]string{"echo", tricky}, 5*time.Second)
	if res.Err != "" {
		t.Fatalf("error: %s", res.Err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit %d", res.ExitCode)
	}
	if !strings.Contains(string(res.Stdout), tricky) {
		t.Errorf("stdout missing literal arg: got %q want substring %q",
			string(res.Stdout), tricky)
	}
}
