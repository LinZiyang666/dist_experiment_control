package agent

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// lease_release_test.go — what the agent's farewell actually puts on the wire.
//
// origin: prerelease audit broker-core/BC-F1.
//
// The broker's "take the released lease row OFFLINE" branch was gated on
// req.LeasedNID, and releaseLeaseName never set it. So the branch was dead in
// production for the whole v0.5.1 line while the broker's own comments described
// it as the fix for F11's suffix drift, and the broker-side test was green
// because its helper filled in a field the product does not send.
//
// The lesson is not "set the field". It is that a message-shape claim has to be
// checked against the MESSAGE THE PRODUCT EMITS, so this test captures a real
// publish from releaseLeaseName rather than constructing a request itself.

// captureFarewell runs the real releaseLeaseName against a real bus and returns
// the decoded body it published.
func captureFarewell(t *testing.T, configuredNID, routingNID string) proto.NodeRegisterReq {
	t.Helper()
	url := testharness.StartNATS(t)
	a, err := New(Config{
		NATSURL: url, SID: "lab", NID: configuredNID,
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: time.Second,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	a.instanceID = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
	if routingNID != "" {
		leased := routingNID
		a.routingNID.Store(&leased)
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	got := make(chan proto.NodeRegisterReq, 1)
	sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", nidOf(a)), func(m *nats.Msg) {
		var req proto.NodeRegisterReq
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Errorf("farewell body is not a NodeRegisterReq: %v", err)
			return
		}
		select {
		case got <- req:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	pub, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	releaseLeaseName(a, pub)

	select {
	case req := <-got:
		return req
	case <-time.After(5 * time.Second):
		t.Fatal("releaseLeaseName published no farewell")
		return proto.NodeRegisterReq{}
	}
}

// TestTheFarewellOfALeasedInstanceSaysItIsLeased is the guard that would have
// caught BC-F1 at the source.
func TestTheFarewellOfALeasedInstanceSaysItIsLeased(t *testing.T) {
	req := captureFarewell(t, "gpu1", "gpu1-02")

	if req.NID != "gpu1-02" {
		t.Errorf("farewell NID=%q; an agent under a lease must release the name it HOLDS", req.NID)
	}
	if !req.LeasedNID {
		t.Error("the farewell of a LEASED instance does not say so.\n\n" +
			"That was BC-F1: the broker's release-the-row gate required this field, the agent never " +
			"sent it, so for the whole v0.5.1 line the released name stayed ONLINE for the full " +
			"OfflineAfter window and the agent's own restart was issued the next suffix. The broker " +
			"now decides from agent_provisioning and only falls back to this field, but the two " +
			"halves must still agree — a broker that cannot read that table has nothing else to go on.")
	}
	// The flags that make the message safe against an N-1 broker are not optional
	// decoration; a farewell missing them is destructive, not merely ignored.
	if !req.ReleasingName || !req.RosterRefreshOnly || req.ProtoVersion != proto.ProtoVersion {
		t.Errorf("farewell lost a load-bearing flag: ReleasingName=%v RosterRefreshOnly=%v proto=%d.\n\n"+
			"Without RosterRefreshOnly a pre-feature broker runs the farewell as an ordinary register "+
			"with an EMPTY snapshot: it closes every RUNNING row this node has, and the surviving "+
			"children come back as orphans and are SIGKILLed.",
			req.ReleasingName, req.RosterRefreshOnly, req.ProtoVersion)
	}
}

// TestTheFarewellOfAConfiguredAgentDoesNotClaimALease is the other direction. A
// device running under its own configured name must not tell the broker its row
// is a lease — that row's liveness belongs to the heartbeat, and the broker's
// fallback path (agent_provisioning unreadable) trusts this field.
func TestTheFarewellOfAConfiguredAgentDoesNotClaimALease(t *testing.T) {
	req := captureFarewell(t, "gpu1", "")

	if req.NID != "gpu1" {
		t.Errorf("farewell NID=%q, want the configured name", req.NID)
	}
	if req.LeasedNID {
		t.Error("a configured agent's farewell claims to be a lease.\n\n" +
			"On the broker's fallback path that is enough to take a real device's row OFFLINE on a " +
			"clean shutdown, which fails its admitACL node-ONLINE check and drops it from /sub.")
	}
}
