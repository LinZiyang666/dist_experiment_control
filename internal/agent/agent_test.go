package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestNewRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"empty NATSURL", Config{SID: "lab", NID: "lab-1"}},
		{"bad SID (uppercase)", Config{NATSURL: "nats://x", SID: "Lab", NID: "lab-1"}},
		{"reserved SID", Config{NATSURL: "nats://x", SID: "default", NID: "lab-1"}},
		{"empty SID", Config{NATSURL: "nats://x", NID: "lab-1"}},
		{"bad NID (uppercase)", Config{NATSURL: "nats://x", SID: "lab", NID: "Lab-1"}},
		{"empty NID", Config{NATSURL: "nats://x", SID: "lab"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.cfg); err == nil {
				t.Errorf("New(%+v) should have errored", c.cfg)
			}
		})
	}
}

func TestNewSetsDefaults(t *testing.T) {
	a, err := New(Config{NATSURL: "nats://x", SID: "lab", NID: "lab-1"})
	if err != nil {
		t.Fatal(err)
	}
	if a.cfg.HeartbeatInterval == 0 || a.cfg.RegisterTimeout == 0 || a.cfg.Logger == nil {
		t.Fatalf("New must populate defaults: %+v", a.cfg)
	}
}

func startNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

// Run a real agent against a stub NATS subscriber that ACKs register and
// counts heartbeats. Exercises the register req/reply path + heartbeat
// ticker without depending on the broker package (no test-time cycles).
func TestAgentRegisterAndHeartbeats(t *testing.T) {
	url := startNATS(t)

	stub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	var registerSeen atomic.Int32
	var heartbeatSeen atomic.Int32

	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) {
			registerSeen.Add(1)
			resp, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
			_ = msg.Respond(resp)
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.heartbeat",
		func(msg *nats.Msg) { heartbeatSeen.Add(1) },
	); err != nil {
		t.Fatal(err)
	}
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	a, err := New(Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-1",
		HeartbeatInterval: 30 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for registerSeen.Load() < 1 || heartbeatSeen.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected ≥1 register and ≥2 heartbeats; got reg=%d hb=%d",
				registerSeen.Load(), heartbeatSeen.Load())
		}
		time.Sleep(15 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("agent.Run returned non-canceled error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not exit on cancel")
	}
}

// When the broker stub rejects the register, agent.Run must surface a clear
// error instead of dropping into the heartbeat loop.
func TestAgentSurfacesRegisterRejection(t *testing.T) {
	url := startNATS(t)
	stub, _ := nats.Connect(url)
	defer stub.Close()

	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) {
			resp, _ := json.Marshal(proto.NodeRegisterResp{
				OK: false, Code: "proto_mismatch", Error: "version test",
			})
			_ = msg.Respond(resp)
		},
	); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, _ := New(Config{
		NATSURL:         url,
		SID:             "lab",
		NID:             "lab-1",
		RegisterTimeout: 500 * time.Millisecond,
	})
	err := a.Run(context.Background())
	if err == nil {
		t.Fatal("expected register rejection error")
	}
	if !strings.Contains(err.Error(), "proto_mismatch") {
		t.Errorf("error should mention reject code; got %v", err)
	}
}

// TestAgentRegisterRetriesOnLeaderUnavailable is the audit-M2 / write-forward-F3 regression:
// a transient leader_unavailable reply (a raft leader failover during register) must be
// RETRIED, not treated as a permanent rejection that EXITS the agent process. The stub
// returns CodeLeaderUnavailable for the first two attempts, then OK — the agent must keep
// running, retry, and succeed (contrast TestAgentSurfacesRegisterRejection, where a permanent
// code makes Run return immediately).
func TestAgentRegisterRetriesOnLeaderUnavailable(t *testing.T) {
	url := startNATS(t)
	stub, _ := nats.Connect(url)
	defer stub.Close()

	var attempts atomic.Int32
	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) {
			var resp []byte
			if attempts.Add(1) <= 2 {
				resp, _ = json.Marshal(proto.NodeRegisterResp{
					OK: false, Code: proto.CodeLeaderUnavailable, Error: "no leader right now",
				})
			} else {
				resp, _ = json.Marshal(proto.NodeRegisterResp{OK: true})
			}
			_ = msg.Respond(resp)
		},
	); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, err := New(Config{
		NATSURL:              url,
		SID:                  "lab",
		NID:                  "lab-1",
		HeartbeatInterval:    50 * time.Millisecond,
		RegisterTimeout:      200 * time.Millisecond,
		RegisterRetryInitial: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// The agent must NOT exit on the transient codes — it must retry until the OK on attempt 3.
	deadline := time.Now().Add(3 * time.Second)
	for attempts.Load() < 3 {
		select {
		case err := <-done:
			t.Fatalf("agent EXITED on a transient leader_unavailable instead of retrying: %v (attempts=%d)", err, attempts.Load())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("register did not reach attempt 3 (got %d) — retry on leader_unavailable not working", attempts.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not exit after cancel")
	}
}

// Register must retry on transient failures (no responders, garbled reply,
// per-attempt timeout) until the parent context is canceled or the broker
// returns a real OK reply. Mirrors the reviewer's e2e regression test
// (test/p2/agent_startup_resilience_test.go) at the package level so the
// retry branches are covered by `go test -cover ./internal/agent`.
func TestAgentRegisterRetriesUntilResponderAppears(t *testing.T) {
	url := startNATS(t)

	a, err := New(Config{
		NATSURL:              url,
		SID:                  "lab",
		NID:                  "lab-1",
		HeartbeatInterval:    50 * time.Millisecond,
		RegisterTimeout:      100 * time.Millisecond,
		RegisterRetryInitial: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Agent must keep running while no responder is up.
	select {
	case err := <-done:
		t.Fatalf("agent exited too early: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Now stand up the broker stub mid-flight.
	stub, _ := nats.Connect(url)
	defer stub.Close()
	var registerSeen atomic.Int32
	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) {
			registerSeen.Add(1)
			resp, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
			_ = msg.Respond(resp)
		},
	); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	deadline := time.Now().Add(2 * time.Second)
	for registerSeen.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("register did not retry after responder appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not exit after cancel")
	}
}

// Garbled JSON reply is treated as transient (broker bug or partial deploy);
// agent retries instead of bailing out.
func TestAgentRetriesOnGarbledReply(t *testing.T) {
	url := startNATS(t)
	stub, _ := nats.Connect(url)
	defer stub.Close()

	var attempts atomic.Int32
	if _, err := stub.Subscribe(
		proto.SubjectPrefix+".ctrl.s.*.node.*.register.req",
		func(msg *nats.Msg) {
			n := attempts.Add(1)
			if n == 1 {
				_ = msg.Respond([]byte("not json"))
				return
			}
			resp, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
			_ = msg.Respond(resp)
		},
	); err != nil {
		t.Fatal(err)
	}
	_ = stub.Flush()

	a, _ := New(Config{
		NATSURL:              url,
		SID:                  "lab",
		NID:                  "lab-1",
		HeartbeatInterval:    50 * time.Millisecond,
		RegisterTimeout:      200 * time.Millisecond,
		RegisterRetryInitial: 30 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	deadline := time.Now().Add(1 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected ≥ 2 attempts, got %d", attempts.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	<-done
}

// Architecture C.3: connection-level reconnect is unbounded. When NATS is
// unreachable at start (closed port, server not up yet), the agent must
// keep retrying nats.Connect rather than fast-failing. Verify by setting
// a tight ctx timeout and asserting the surfaced error is
// context.DeadlineExceeded — not a fast-fail from nats.ErrNoServers.
//
// Replaces the previous TestAgentRunFailsOnBadNATSURL, which encoded the
// (incorrect) fast-fail expectation.
func TestAgentRetriesUntilCtxCancelOnUnreachableNATS(t *testing.T) {
	a, _ := New(Config{
		NATSURL:              "nats://127.0.0.1:1", // port 1 — closed
		SID:                  "lab",
		NID:                  "lab-1",
		RegisterRetryInitial: 30 * time.Millisecond,
		RegisterRetryMax:     100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := a.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded after retries, got %v", err)
	}
}
