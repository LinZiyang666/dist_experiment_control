package broker

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

func opsLeaseBrokerWithBus(t *testing.T) *Broker {
	t.Helper()
	url := testharness.StartNATS(t)
	b := leaseBroker(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("broker conn: %v", err)
	}
	t.Cleanup(nc.Close)
	b.nc.Store(nc)
	return b
}

// subscribeAs installs the forwarded subscription a live agent installs, and
// answers claim-probe with `iid` the way internal/agent/instance.go does.
func subscribeAs(t *testing.T, b *Broker, sid, nid, iid string) {
	t.Helper()
	nc, err := nats.Connect(b.nc.Load().ConnectedUrl())
	if err != nil {
		t.Fatalf("agent conn: %v", err)
	}
	t.Cleanup(nc.Close)
	sub, err := nc.Subscribe(proto.SubjCmdForwarded(sid, nid, "*"), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		payload := []byte(`{"instance_id":"` + iid + `"}`)
		_ = m.Respond(payload)
	})
	if err != nil {
		t.Fatalf("agent sub: %v", err)
	}
	// NO AutoUnsubscribe here. It was `sub.AutoUnsubscribe(1 << 30)`, and that
	// made the server stop reporting interest for this subject — every probe
	// came back ErrNoResponders, so the test read as "the grant arm skipped the
	// probe" when the probe had in fact run and been told nobody was there.
	// A helper that silently removes the very interest the test is about turns
	// a passing guard into a false accusation.
	_ = sub
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// THE #72 / stale-beat INPUT: the incumbent is a LIVE SUBSCRIBER whose
// heartbeat has been silent past LeaseGrace. Plan R1 asks exactly this: what
// does a partitioned-but-alive agent meet? The grant arm short-circuits BEFORE
// the probe, so the challenger takes the name without ever asking whether the
// incumbent is still there — and both are then subscribed to one forwarded
// subject.
func TestStaleBeatGrantIgnoresALiveIncumbentSubscriber(t *testing.T) {
	b := opsLeaseBrokerWithBus(t)
	seedBeat(t, b, "jupyter", 30*time.Second) // beat silent past LeaseGrace (6s)
	subscribeAs(t, b, "lab", "jupyter", testInstanceA)

	lease, code, err := adjudicated(t, b, "lab", "jupyter", &proto.NodeRegisterReq{InstanceID: testInstanceB})
	if err != nil || code != "" {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if lease == nil {
		t.Fatal("the challenger was GRANTED \"jupyter\" while a live process is still subscribed " +
			"under it and answering claim-probe with a DIFFERENT instance id. Both are now on one " +
			"forwarded subject: every exec runs twice. The probe is skipped because the grant arm " +
			"tests only the heartbeat clock (`!exists || age > grace`), and a #72-shaped agent " +
			"(socket ESTABLISHED, subscriptions live, heartbeat stopped) is precisely an agent " +
			"whose clock says dead and whose socket says alive.")
	}
}
