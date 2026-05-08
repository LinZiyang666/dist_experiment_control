package p2_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

func TestAgentSurvivesMissingRegisterResponder(t *testing.T) {
	url := startNATS(t)

	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "late-broker",
		HeartbeatInterval: 20 * time.Millisecond,
		RegisterTimeout:   100 * time.Millisecond,
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
		t.Fatalf("agent exited before broker responder appeared: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	stub, err := nats.Connect(url)
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
			t.Fatal("agent did not retry register after broker responder appeared")
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
