package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

func TestExternalReviewCapabilityGateDoesNotTrustArbitraryDevLabels(t *testing.T) {
	for _, release := range []string{"v0.2.8-dev", "legacy-dev", "v0.0.0-dev"} {
		if isP13Capable(release) {
			t.Errorf("release %q is not proof that this agent implements P13", release)
		}
	}
}

func TestExternalReviewProxyTokenDeniedWhileSwitchOff(t *testing.T) {
	_, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	alloc, err := port.AllocateProxy(b.cfg.DB, sid, "lab-1", b.cfg.PortAllocCfg())
	if err != nil {
		t.Fatal(err)
	}

	// The allocation row can remain visible briefly between CloseProxy and
	// port.Free. Once the master switch is OFF, that row must not authorize a
	// fresh REGISTER in the gap.
	if err := b.tunnelTokenLookup(sid, "lab-1", alloc.Port, alloc.TokenHash); err == nil {
		t.Fatal("proxy token was authorized while the session proxy switch was OFF")
	}
}

func TestExternalReviewZeroEpochHeartbeatRetriesUnreadyProxy(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	epoch, err := session.BumpProxyEpoch(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now(), ProxyEpoch: 0})
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1"), body); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-ch:
		var d proto.ProxyDirective
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if !d.Enabled || d.Epoch != epoch {
			t.Fatalf("retry directive=%+v want enabled epoch %d", d, epoch)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ProxyEpoch=0 heartbeat never reached the unready retry path")
	}
}

func TestExternalReviewHeartbeatConvergesAfterDBEpochRewind(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	epoch, err := session.BumpProxyEpoch(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.SetProxyReady(b.cfg.DB, sid, "lab-1", true); err != nil {
		t.Fatal(err)
	}

	ch := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// Agent epoch 5 against restored broker epoch 1 must receive the restored
	// authoritative state even though its scalar epoch is numerically higher.
	b.repairProxyEpoch(sid, "lab-1", 5)
	select {
	case msg := <-ch:
		var d proto.ProxyDirective
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.Epoch != epoch {
			t.Fatalf("restore directive epoch=%d want %d", d.Epoch, epoch)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("heartbeat did not converge a higher agent epoch after DB rewind")
	}
}
