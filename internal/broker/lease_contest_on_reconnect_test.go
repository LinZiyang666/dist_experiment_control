package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §3.2
//
// The plan's §3.2 rule is driven by nodes.last_heartbeat_at precisely so it
// survives a broker restart / leader election, and the in-memory leaseHolder
// map is documented as "probe avoidance" only. But the probe it falls back to
// is an INTEREST probe on the agent's OWN forwarded subject — and on the
// nats.go RECONNECT path (Conn.doReconnect calls resendSubscriptions() at
// nats.go@v1.52.0:3379, strictly BEFORE it invokes the ReconnectedCB at :3400)
// the agent's subscription is already live when internal/agent's
// onNATSReconnect re-registers.
//
// So after any broker restart or leader election, a LONE agent's next
// reconnect-register is adjudicated CONTESTED against itself.
func TestLoneAgentReRegisteringOverAReplayedSubscriptionIsNotContested(t *testing.T) {
	url := testharness.StartNATS(t)
	b := leaseBroker(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	b.nc.Store(nc)

	// The agent never stopped heartbeating, so its row is always fresh.
	seedBeat(t, b, "gpu1", 0)
	// nats.go replayed the forwarded subscription before the ReconnectedCB —
	// and a current agent ANSWERS claim-probe from that subscription with its
	// own instance id (internal/agent, replyClaimProbe). That answer is what
	// lets the broker tell "someone else holds this name" from "the thing
	// answering is the very process re-registering"; without it a lone agent
	// would be renamed on every network blip.
	sub, err := nc.Subscribe(proto.SubjCmdForwarded("lab", "gpu1", "*"), func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond([]byte(`{"instance_id":"` + testInstanceA + `"}`))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// leaseHolder is EMPTY: this broker process just restarted (single mode) or
	// just won an election (cluster mode). Same instance id as before.
	lease, code, err := adjudicated(t, b, "lab", "gpu1", &proto.NodeRegisterReq{InstanceID: testInstanceA})
	if err != nil || code != "" {
		t.Fatalf("adjudicate: code=%q err=%v", code, err)
	}
	if lease != nil {
		t.Fatalf("a LONE agent re-registering after a broker restart was assigned %q: "+
			"the interest probe answered on the agent's OWN replayed subscription. "+
			"handleRegister then returns without registerNode/reconcileOnRegister/roster, "+
			"and internal/agent's onNATSReconnect ignores resp.Lease entirely",
			lease.AssignedNID)
	}
}
