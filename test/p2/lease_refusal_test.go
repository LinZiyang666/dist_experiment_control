package p2_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §0.1 E2
//
// THE UNISSUABLE-LEASE REFUSAL IS NOT HONOURED BY THE AGENT, so the E2 fan-out
// survives on that path.
//
// internal/broker/broker.go's contested arm now emits a lease with an EMPTY
// AssignedNID when no name can be issued (basename longer than
// proto.MaxLeaseBasenameLen, or the suffix space exhausted), and its comment
// states: "That is still a refusal the challenger must honour: we have PROVEN a
// different live process holds this name". The broker then returns having
// written NOTHING.
//
// The agent does not honour it. acceptableLeaseName rejects "" (instance.go),
// the adoption branch is skipped, the `else if` arm only LOGS, and session()
// falls straight through to nc.Subscribe on the BASENAME's forwarded subject.
// So the challenger ends up subscribed to the same subject as the incumbent —
// which is exactly the live-fleet defect of §0.1 E2: one exec, two start rows,
// two exit rows, rc=20 and rc=120.
//
// This test asserts the property the broker comment claims: after a refusal,
// nobody answers on the basename's forwarded tree. It uses the claim-probe verb
// as the oracle because that is the broker's own liveness question, and
// ErrNoResponders is the answer that means "no interest exists".
//
// MUTATION: give the agent an arm that treats a lease with an empty AssignedNID
// as terminal-for-this-session (return without subscribing, or exit) and this
// goes green; remove it and it goes red again.
func TestUnissuableLeaseRefusalStopsTheAgentSubscribingOnTheBasename(t *testing.T) {
	url := testharness.StartNATS(t)

	obs, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close()

	var registers atomic.Int64
	sub, err := obs.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		var legacy struct {
			RosterRefreshOnly bool `json:"roster_refresh_only"`
		}
		_ = json.Unmarshal(m.Data, &legacy)
		if legacy.RosterRefreshOnly {
			_ = m.Respond([]byte(`{"ok":true}`))
			return
		}
		registers.Add(1)
		// Byte-for-byte what handleRegister sends on the unissuable path.
		_ = m.Respond([]byte(`{"ok":true,"lease":{"assigned_nid":"","basename":"gpu1"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	_ = obs.Flush()

	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "gpu1",
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("agent did not exit")
		}
	}()

	// Wait until the refusal has actually been delivered.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && registers.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if registers.Load() == 0 {
		t.Fatal("the agent never registered; the harness did not exercise the refusal path")
	}
	time.Sleep(400 * time.Millisecond) // let the subscribe step run if it is going to

	_, perr := obs.Request(proto.SubjCmdForwarded("lab", "gpu1", proto.ClaimProbeVerb), nil, 400*time.Millisecond)
	if perr == nil {
		t.Fatalf("a REFUSED instance answered on the basename's forwarded subject.\n" +
			"handleRegister proved a different live process holds \"gpu1\" and wrote nothing, but the\n" +
			"agent only logged the refusal and went on to nc.Subscribe(SubjCmdForwarded(sid, \"gpu1\", \"*\")).\n" +
			"Both processes now receive every command addressed to that name — plan §0.1 E2, restored\n" +
			"with the increment installed, on any basename longer than proto.MaxLeaseBasenameLen or any\n" +
			"basename whose suffix space is full.")
	}
}
