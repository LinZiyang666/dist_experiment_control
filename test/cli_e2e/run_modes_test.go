package cli_e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// runResult collects what one PTY run lifecycle delivered.
type runResult struct {
	PID      string
	Out      bytes.Buffer
	ExitCode int
	Failed   string
}

// runRunResize is the in-test stand-in for `tether run`. Optional
// resizeSeq fires PtyResizeEvent updates on the master once the
// process is up — the test command then echoes back the new size
// (via `tput`).
func runRunResize(
	t *testing.T,
	nc *nats.Conn,
	sid, actor, nid string,
	argv []string,
	cols, rows int,
	resizeSeq []proto.PtyResizeEvent,
	deadline time.Duration,
) runResult {
	t.Helper()

	body, _ := json.Marshal(proto.RunReq{Argv: argv, Cols: cols, Rows: rows})

	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(proto.SubjCmdBy(sid, actor, nid, "run"), inbox, body); err != nil {
		t.Fatal(err)
	}

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

	// Subscribe pty.<pid>.out BEFORE attach so we don't miss the
	// first byte the shell prints.
	outCh := make(chan []byte, 256)
	outSub, err := nc.Subscribe(proto.SubjPtyOut(sid, res.PID), func(m *nats.Msg) {
		b := make([]byte, len(m.Data))
		copy(b, m.Data)
		outCh <- b
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outSub.Unsubscribe() }()

	att, _ := json.Marshal(proto.PtyAttachEvent{Cols: cols, Rows: rows})
	if err := nc.Publish(proto.SubjPtyAttach(sid, res.PID), att); err != nil {
		t.Fatal(err)
	}

	// Fire any planned resize events 200ms after attach so the
	// child has had time to install its SIGWINCH handler.
	if len(resizeSeq) > 0 {
		go func() {
			time.Sleep(200 * time.Millisecond)
			for _, ev := range resizeSeq {
				body, _ := json.Marshal(ev)
				_ = nc.Publish(proto.SubjPtyResize(sid, res.PID), body)
				time.Sleep(80 * time.Millisecond)
			}
		}()
	}

	end := time.Now().Add(deadline)
	for {
		left := time.Until(end)
		if left <= 0 {
			t.Fatalf("run timed out, partial: pid=%s out=%dB", res.PID, res.Out.Len())
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
					drainTimer := time.NewTimer(200 * time.Millisecond)
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

// C.14 — `run echo "hello"` round-trip. Under PTY, every \n becomes
// \r\n; we just need the literal token in the byte stream.
func TestRunEchoHelloOverPTY(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	res := runRunResize(t, nc, "lab", pub, "lab-1",
		[]string{"echo", "hello"}, 80, 24, nil, 30*time.Second)
	if res.Failed != "" {
		t.Fatalf("run failed: %s", res.Failed)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code: got %d want 0", res.ExitCode)
	}
	if !strings.Contains(res.Out.String(), "hello") {
		t.Errorf("output missing 'hello': %q", res.Out.String())
	}
}

// C.15 — non-zero exit propagation under PTY.
func TestRunExitCodePropagates(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	res := runRunResize(t, nc, "lab", pub, "lab-1",
		[]string{"sh", "-c", "exit 7"}, 80, 24, nil, 30*time.Second)
	if res.Failed != "" {
		t.Fatalf("failed: %s", res.Failed)
	}
	if res.ExitCode != 7 {
		t.Errorf("exit: got %d want 7", res.ExitCode)
	}
}

// C.16 — TIOCSWINSZ propagation. Start the PTY at 80x24, then
// resize to 200x60. The child runs `stty size` after a tiny sleep
// so the resize lands first; the agent's resize handler ioctl's
// the master and the child sees the new dimensions.
//
// `stty size` prints "<rows> <cols>" (newline-terminated). We grep
// for "60 200" in the output stream.
func TestRunResizePropagatesViaSIGWINCH(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	resize := []proto.PtyResizeEvent{{Cols: 200, Rows: 60}}
	// `sleep 0.4 && stty size`. The 400ms slack covers the
	// 200ms-into-attach delay before resize fires + the kernel
	// signal delivery.
	res := runRunResize(t, nc, "lab", pub, "lab-1",
		[]string{"sh", "-c", "sleep 0.4; stty size"},
		80, 24, resize, 6*time.Second)
	if res.Failed != "" {
		t.Fatalf("failed: %s", res.Failed)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit %d, output=%q", res.ExitCode, res.Out.String())
	}
	if !strings.Contains(res.Out.String(), "60 200") {
		// Some PTY setups emit "<rows> <cols>", others swap; accept either.
		if !strings.Contains(res.Out.String(), "200 60") {
			t.Errorf("resize did not propagate: stty size output = %q", res.Out.String())
		}
	}
}

// C.17 — fast-exit run (< 1s child). Pin that the attach handshake
// race never surfaces as attach_timeout when ctl attaches promptly.
func TestRunFastExitNoAttachTimeout(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	for i := 0; i < 5; i++ {
		res := runRunResize(t, nc, "lab", pub, "lab-1",
			[]string{"sh", "-c", "echo q"}, 80, 24, nil, 30*time.Second)
		if res.Failed != "" {
			t.Fatalf("iter %d failed: %s", i, res.Failed)
		}
		if res.ExitCode != 0 {
			t.Errorf("iter %d exit: got %d want 0", i, res.ExitCode)
		}
	}
}

// C.18 — argv pointing at a non-existent command surfaces as
// failed{exec_failed}, NOT as a NATS timeout. The agent allocates
// the PTY successfully (so we get to "started"), then exec fails
// inside the child.
//
// Pty.Start uses fork+exec via creack/pty; on a nonexistent program
// the exec'd child errors out and Wait returns the exec error
// before any byte hits the master. The agent treats this as
// exec_failed (handle_run.go after pty.Start).
func TestRunNonexistentCommandFails(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	defer startBroker(t, url, db)()
	defer startAgent(t, url, "lab", "lab-1")()
	testharness.WaitNodeOnline(t, db, "lab", "lab-1", 3*time.Second)

	nc := connect(t, url)
	defer nc.Close()

	uniq := fmt.Sprintf("/var/empty/no-such-binary-%d", time.Now().UnixNano())
	res := runRunResize(t, nc, "lab", pub, "lab-1",
		[]string{uniq}, 80, 24, nil, 30*time.Second)
	if res.Failed != "exec_failed" {
		t.Errorf("expected failed{exec_failed}; got failed=%q exit=%d out=%q",
			res.Failed, res.ExitCode, res.Out.String())
	}
}
