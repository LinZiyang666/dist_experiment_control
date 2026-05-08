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
	"github.com/nats-io/nats.go"
	natstest "github.com/nats-io/nats-server/v2/test"
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
	for {
		if registerSeen.Load() >= 1 && heartbeatSeen.Load() >= 2 {
			break
		}
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

// Bad NATS URL: New accepts it (no connect at New time), Run fails fast.
func TestAgentRunFailsOnBadNATSURL(t *testing.T) {
	a, _ := New(Config{
		NATSURL: "nats://127.0.0.1:1", // no broker on port 1
		SID:     "lab",
		NID:     "lab-1",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := a.Run(ctx); err == nil {
		t.Fatal("expected NATS connect to fail")
	}
}
