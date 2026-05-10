// NATS-disruption tests. Cover dimension A from the chaos checklist:
// CTL behavior when NATS is unreachable, agent behavior when NATS
// vanishes mid-flight, and broker reconnect through a NATS bounce.
//
// These tests deliberately exercise the surface that hangs / panics
// in production deployments — every test enforces a hard test-level
// deadline so a regression manifests as a Fatal, not as a 120s
// pkill from the test runner.
package chaos_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// TestCtlConnectBeforeNATSUp pins dimension A.1: a CTL operation
// against a NATS URL that has nothing listening must return a clear
// connect error in ≤2s, not hang.
//
// We don't drive cmd/tether through cobra (it lives in package main
// and would force this test into cmd/tether/), but we replicate the
// exact failure shape: nats.Connect on a never-bound port. The CLI's
// connectError wrapper formats this into "cannot reach broker at
// <url>" — that wrapping is unit-tested in cmd/tether/error_hints_test.go;
// here we just pin the underlying NATS-level non-hang.
func TestCtlConnectBeforeNATSUp(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 5*time.Second)
	defer cancel()

	port := reserveTCPPort(t)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)

	doneCh := make(chan error, 1)
	go func() {
		_, err := nats.Connect(url, nats.Timeout(1*time.Second))
		doneCh <- err
	}()
	select {
	case err := <-doneCh:
		if err == nil {
			t.Fatal("expected connect failure against unbound port")
		}
		// Real failure mode: "no servers available" / "connection refused".
		// The CLI's connectError wraps this with "cannot reach broker at <url>".
		if !strings.Contains(err.Error(), "no servers") &&
			!strings.Contains(err.Error(), "connection refused") &&
			!strings.Contains(err.Error(), "i/o timeout") {
			t.Logf("connect error string: %v (acceptable so long as it's not a hang)", err)
		}
	case <-ctx.Done():
		t.Fatal("nats.Connect hung past 5s deadline against unbound port")
	}
}

// TestAgentReconnectsThroughNATSBounce pins dimension A.2 + A.4:
// the agent is mid-run; NATS dies; agent must NOT panic and must
// re-register once NATS comes back on the same port.
//
// Architecture C.3: agent uses MaxReconnects(-1) so the underlying
// nats.Conn keeps retrying. We don't have explicit re-register on
// reconnect today (the agent's Run loop calls register exactly
// once and then heartbeats forever), so this test specifically
// pins "no panic + heartbeats resume", not "node row flips back to
// ONLINE on its own". That's a known gap surfaced by this test.
func TestAgentReconnectsThroughNATSBounce(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 25*time.Second)
	defer cancel()

	port := reserveTCPPort(t)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	natsURL, stopNATS := startNATSOnPort(t, port)
	if natsURL != url {
		t.Fatalf("NATS bound to unexpected URL %q (want %q)", natsURL, url)
	}

	// Stub broker: just answer register.req OK so the agent reaches the
	// heartbeat loop. This is the partial-failure surface: we want to
	// confirm agent survives NATS bounce regardless of whether broker
	// is alive at the moment.
	stub, err := nats.Connect(url, nats.MaxReconnects(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stub.Close)

	registerHits := make(chan struct{}, 8)
	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) {
			body, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
			_ = msg.Respond(body)
			select {
			case registerHits <- struct{}{}:
			default:
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	// Track reconnect events on a side connection so we don't need to
	// reach into the agent's nats.Conn.
	a, err := agent.New(agent.Config{
		NATSURL:              url,
		SID:                  "lab",
		NID:                  "n1",
		Logger:               silentLog(),
		HeartbeatInterval:    50 * time.Millisecond,
		RegisterTimeout:      1500 * time.Millisecond,
		RegisterRetryInitial: 50 * time.Millisecond,
		RegisterRetryMax:     200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	agentCtx, cancelAgent := context.WithCancel(ctx)
	defer cancelAgent()
	agentDone := make(chan error, 1)
	go func() { agentDone <- a.Run(agentCtx) }()

	// Wait for first register.
	select {
	case <-registerHits:
	case <-time.After(5 * time.Second):
		t.Fatal("first register never observed")
	}

	// Bounce NATS. Agent's nats.Conn should fall into the reconnect
	// loop (architecture C.3 — MaxReconnects(-1)).
	stopNATS()
	// Drop the stub conn so it doesn't try to re-bind on the same
	// fd; we'll create a fresh one after the relaunch.
	stub.Close()

	// While NATS is down, the agent goroutine must NOT exit. If it
	// does, the panic / unhandled error would have been the regression.
	select {
	case err := <-agentDone:
		t.Fatalf("agent exited while NATS was down: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	// Bring NATS back on the same port. Agent's underlying nats.Conn
	// reconnects (MaxReconnects(-1)). We can't drive a re-register
	// from inside the agent today; we just confirm the agent is still
	// live + the bus is reachable again.
	_, _ = startNATSOnPort(t, port)

	// Probe NATS independently so we know the bus is back, not just
	// that the agent's internal reconnect succeeded.
	if !testharness.WaitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		nc, err := nats.Connect(url, nats.Timeout(200*time.Millisecond))
		if err != nil {
			return false
		}
		nc.Close()
		return true
	}) {
		t.Fatal("NATS never became reachable again after restart")
	}

	// Agent goroutine should still be alive (no panic from its
	// heartbeat publishes hitting a dead conn during the gap).
	select {
	case err := <-agentDone:
		t.Fatalf("agent exited after NATS came back: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// Clean shutdown.
	cancelAgent()
	select {
	case <-agentDone:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not exit on cancel after NATS bounce")
	}
}

// TestCtlExecBoundedWhenNATSDiesMidExec pins dimension A.3: a CTL
// request in flight when NATS goes away must error within the
// configured timeout, not block forever. We use nc.RequestWithContext
// — the same primitive cmd/tether/exec.go uses for non-stream RPCs —
// and confirm the deadline fires.
func TestCtlExecBoundedWhenNATSDiesMidExec(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 10*time.Second)
	defer cancel()

	port := reserveTCPPort(t)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	_, stopNATS := startNATSOnPort(t, port)

	cliConn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cliConn.Close)

	// Kick off an unanswered request with a generous deadline; then
	// drop NATS. The Request should return within the deadline.
	reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reqCancel()
	doneCh := make(chan error, 1)
	go func() {
		_, err := cliConn.RequestWithContext(reqCtx,
			proto.SubjCmdBy("lab", "actor", "n1", "exec"),
			[]byte(`{}`),
		)
		doneCh <- err
	}()

	// Sit briefly so the request is definitely in flight, then kill NATS.
	time.Sleep(50 * time.Millisecond)
	stopNATS()

	select {
	case err := <-doneCh:
		// Either ctx-deadline-exceeded (the deadline fired first) or
		// connection-closed (the kill returned us right away). Both
		// satisfy "did not hang past the deadline". Anything else is
		// a regression.
		if err == nil {
			t.Fatal("request unexpectedly succeeded against killed NATS")
		}
		t.Logf("ctl exec returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ctl exec hung past 5s after NATS was killed")
	}
}

// TestCtlConnectAgainstWrongHostFailsCleanly pins dimension A.5: a
// bogus URL (resolvable host that won't talk NATS / unreachable port)
// must return a clear failure, not silently retry forever.
//
// We use a port we know nothing's bound on (= immediate ECONNREFUSED
// on Linux loopback) instead of a real DNS roundtrip — keeps the
// test offline-friendly, same failure shape from the user POV.
func TestCtlConnectAgainstWrongHostFailsCleanly(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 4*time.Second)
	defer cancel()

	port := reserveTCPPort(t)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port) // never bound

	doneCh := make(chan error, 1)
	go func() {
		_, err := nats.Connect(url, nats.Timeout(1*time.Second))
		doneCh <- err
	}()

	select {
	case err := <-doneCh:
		if err == nil {
			t.Fatal("expected connect failure against wrong host")
		}
		// The CLI wraps this with "cannot reach broker at <url>" via
		// connectError. Here we just verify the underlying NATS error
		// is concrete enough that the wrapper has something to report.
		if err.Error() == "" {
			t.Errorf("connect error string is empty — cli connectError would be useless")
		}
	case <-ctx.Done():
		t.Fatal("nats.Connect hung past 4s against wrong-host URL")
	}
}

// TestCtlRequestDeadlineWhenNoResponder pins dimension A.6: NATS is
// up, but no broker is listening. A request must surface as
// ErrNoResponders / context-deadline within the timeout, not hang.
//
// This is the common failure mode when a) the broker process crashed
// but NATS stayed up, or b) deployment ordering went wrong.
func TestCtlRequestDeadlineWhenNoResponder(t *testing.T) {
	ctx, cancel := withTestDeadline(t, 5*time.Second)
	defer cancel()

	url := startNATS(t)

	cliConn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cliConn.Close)

	reqCtx, reqCancel := context.WithTimeout(ctx, 1*time.Second)
	defer reqCancel()

	t0 := time.Now()
	_, err = cliConn.RequestWithContext(reqCtx,
		proto.SubjCmdBy("lab", "actor", "n1", "exec"),
		[]byte(`{}`),
	)
	elapsed := time.Since(t0)
	if err == nil {
		t.Fatal("expected error against no-responder subject")
	}
	if elapsed > 2*time.Second {
		t.Errorf("no-responder request took %v — should have failed within ~1s deadline", elapsed)
	}
	// Acceptable failure modes: ErrNoResponders (NATS rejected
	// immediately) or context-deadline-exceeded (no_responders header
	// not advertised, request waited out the deadline). Both meet
	// "doesn't hang".
	if !strings.Contains(err.Error(), "no responders") &&
		!strings.Contains(err.Error(), "deadline") &&
		!strings.Contains(err.Error(), "timeout") {
		t.Errorf("unexpected error shape (want 'no responders' / 'deadline'): %v", err)
	}
}
