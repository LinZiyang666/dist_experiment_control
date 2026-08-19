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

// origin: docs/reviews/cloned-credential-instances-plan.md §6 — the
// new-agent × OLD-broker quadrant.
//
// An old broker has no `lease` key in its register reply and no knowledge of
// `instance_id`. The plan says the agent "必须把 nil Lease 当 legacy 模式且永不
// 重建（有测试钉住）" — but nothing in the tree pinned it, so this is that pin.
//
// It matters because the failure mode is not a wrong value, it is a LOOP: if a
// nil Lease were ever read as "I was renamed", session() would return
// rebuild=true, Run would re-enter session(), and the agent would re-register
// forever against a broker that can never satisfy it. On the reference
// deployment that is a pod hot-looping at RestartSec on every clone at once.
//
// The old broker is simulated at the wire, not with an old binary: a bare NATS
// responder that answers SubjNodeRegister with exactly the bytes a pre-feature
// broker emits.
//
// MUTATIONS that must turn this red:
//  1. drop the `lease != nil` guard in internal/agent/agent.go's session() so a
//     nil lease takes the adoption branch,
//  2. make the adoption branch fire when AssignedNID == the presented nid,
//  3. have the agent retry/rebuild when a reply carries no lease.
func TestNilLeaseFromALegacyBrokerIsNotARebuild(t *testing.T) {
	url := testharness.StartNATS(t)

	obs, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close()

	var registers atomic.Int64
	var sawInstanceID atomic.Bool
	sub, err := obs.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		// Decode the way an OLD broker would: into a struct that predates this
		// increment. Unknown keys are dropped by encoding/json, which is the
		// whole reason an additive field is safe.
		var legacy struct {
			ProtoVersion      int    `json:"proto_version"`
			NID               string `json:"nid"`
			RosterRefreshOnly bool   `json:"roster_refresh_only"`
		}
		_ = json.Unmarshal(m.Data, &legacy)
		if legacy.RosterRefreshOnly {
			_ = m.Respond([]byte(`{"ok":true}`))
			return
		}
		registers.Add(1)
		// Not vacuous: prove the agent under test really is the NEW one, i.e.
		// that a legacy broker is being fed the new register body.
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(m.Data, &raw); err == nil {
			if _, ok := raw["instance_id"]; ok {
				sawInstanceID.Store(true)
			}
		}
		// Exactly what a pre-feature broker replies: OK, and no `lease` key.
		_ = m.Respond([]byte(`{"ok":true}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	var beats atomic.Int64
	hb, err := obs.Subscribe(proto.SubjNodeHeartbeat("lab", "gpu1"), func(*nats.Msg) {
		beats.Add(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hb.Unsubscribe() }()
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

	// Let several heartbeat intervals pass. A rebuild loop shows up as a
	// climbing register count; legacy mode registers exactly once.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && beats.Load() < 5 {
		time.Sleep(50 * time.Millisecond)
	}

	if got := registers.Load(); got != 1 {
		t.Errorf("a nil Lease must be legacy mode: want exactly 1 register, got %d "+
			"(more than one means the agent rebuilt/retried against a broker that never sends a lease)", got)
	}
	if !sawInstanceID.Load() {
		t.Errorf("vacuity check failed: the register body carried no instance_id, so this test " +
			"did not actually exercise a NEW agent against a legacy broker")
	}
	if beats.Load() == 0 {
		t.Errorf("the agent never reached its heartbeat loop under a legacy broker reply")
	}
}

// TestLeaseReplyIsWhatTriggersTheRebuild is the POSITIVE CONTROL for the test
// above: it proves the register counter there can discriminate at all, by
// showing that the ONE wire difference an old broker cannot produce — a `lease`
// key — does produce a rebuild and a re-register under the assigned name.
//
// Without this control, TestNilLeaseFromALegacyBrokerIsNotARebuild would pass
// even if the agent could never rebuild for any reason.
func TestLeaseReplyIsWhatTriggersTheRebuild(t *testing.T) {
	url := testharness.StartNATS(t)

	obs, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close()

	var basenameRegisters, leasedRegisters atomic.Int64
	subBase, err := obs.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		var legacy struct {
			RosterRefreshOnly bool `json:"roster_refresh_only"`
		}
		_ = json.Unmarshal(m.Data, &legacy)
		if legacy.RosterRefreshOnly {
			_ = m.Respond([]byte(`{"ok":true}`))
			return
		}
		basenameRegisters.Add(1)
		_ = m.Respond([]byte(`{"ok":true,"lease":{"assigned_nid":"gpu1-02","basename":"gpu1"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subBase.Unsubscribe() }()

	subLeased, err := obs.Subscribe(proto.SubjNodeRegister("lab", "gpu1-02"), func(m *nats.Msg) {
		var body struct {
			NID               string `json:"nid"`
			LeasedNID         bool   `json:"leased_nid"`
			RosterRefreshOnly bool   `json:"roster_refresh_only"`
		}
		_ = json.Unmarshal(m.Data, &body)
		if body.RosterRefreshOnly {
			_ = m.Respond([]byte(`{"ok":true}`))
			return
		}
		if body.NID == "gpu1-02" && body.LeasedNID {
			leasedRegisters.Add(1)
		}
		_ = m.Respond([]byte(`{"ok":true}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subLeased.Unsubscribe() }()
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

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && leasedRegisters.Load() == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if leasedRegisters.Load() == 0 {
		t.Fatalf("the agent never re-registered under the assigned lease name (basename registers=%d)",
			basenameRegisters.Load())
	}
}

// TestAssignedLeaseNameIsValidatedBeforeAdoption pins that the routing identity
// the agent adopts from the WIRE is validated exactly as strictly as the one it
// adopts from the ENVIRONMENT.
//
// origin: docs/reviews/cloned-credential-instances-plan.md §4.1 — NodeLease is
// new client-supplied-from-the-broker wire, and internal/agent/agent.go's
// re-exec restore path already calls proto.ValidateNID on TETHER_ROUTING_NID
// while the register-reply path adopts lease.AssignedNID unchecked.
//
// The adopted name is concatenated into NATS subjects (SubjCmdForwarded /
// SubjNodeRegister / SubjNodeHeartbeat) and into the CONNECT name that
// auth_callout parses. A name containing a '.' silently changes the token count
// of every one of them: dispatchForwarded's `len(parts) != 10` guard then drops
// every command, and the CONNECT name no longer parses — with auth_callout on,
// that is a FATAL connect error, i.e. the agent exits and its supervisor
// restarts it forever.
//
// MUTATION that must turn this red: remove the validation (today's state).
func TestAssignedLeaseNameIsValidatedBeforeAdoption(t *testing.T) {
	url := testharness.StartNATS(t)

	obs, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close()

	// Any register on ANY nid under this session, so an adoption of a
	// subject-shaped name is observable rather than merely inferred.
	var adopted atomic.Value // string
	adopted.Store("")
	all, err := obs.Subscribe("tether.v2.ctrl.s.lab.node.>", func(m *nats.Msg) {
		var body struct {
			NID               string `json:"nid"`
			RosterRefreshOnly bool   `json:"roster_refresh_only"`
		}
		_ = json.Unmarshal(m.Data, &body)
		if body.RosterRefreshOnly {
			_ = m.Respond([]byte(`{"ok":true}`))
			return
		}
		if body.NID != "" && body.NID != "gpu1" {
			adopted.Store(body.NID)
			_ = m.Respond([]byte(`{"ok":true}`))
			return
		}
		if body.NID == "" {
			// A heartbeat, not a register: carries no nid in the body, and the
			// subject it arrived on is the adopted identity.
			return
		}
		// A broker (buggy, older, or an inbox-spoofing session peer — the
		// `_INBOX.>` ACL gap is a KNOWN open defect, plan §10) hands back a
		// name that is not a legal nid.
		_ = m.Respond([]byte(`{"ok":true,"lease":{"assigned_nid":"gpu1.evil","basename":"gpu1"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = all.Unsubscribe() }()
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
		}
	}()

	time.Sleep(1500 * time.Millisecond)
	if got, _ := adopted.Load().(string); got != "" {
		t.Errorf("the agent adopted %q as its routing identity — proto.ValidateNID rejects it, "+
			"and it is spliced straight into NATS subjects and the auth CONNECT name", got)
	}
}
