package p2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestAgentSurvivesInitialNATSUnavailable(t *testing.T) {
	port := reserveTCPPort(t)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)

	a, err := agent.New(agent.Config{
		NATSURL:              url,
		SID:                  "lab",
		NID:                  "late-nats",
		HeartbeatInterval:    20 * time.Millisecond,
		RegisterTimeout:      100 * time.Millisecond,
		RegisterRetryInitial: 20 * time.Millisecond,
		RegisterRetryMax:     100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("agent exited before NATS became available: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	startedURL := startNATSOnPort(t, port)

	stub, err := nats.Connect(startedURL)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := stub.Flush(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for registerSeen.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("agent did not register after NATS became available")
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

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	_, portText, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func startNATSOnPort(t *testing.T, port int) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Host = "127.0.0.1"
	opts.Port = port
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
