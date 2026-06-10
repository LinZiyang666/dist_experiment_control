package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

func TestExternalReviewMalformedProxySetDoesNotDisableSession(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)

	var enabled proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &enabled)
	if !enabled.OK {
		t.Fatalf("enable precondition failed: %+v", enabled)
	}

	reply, err := nc.Request(proto.SubjCtrlProxySet(owner, sid), []byte(`{"enabled":`), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var got proto.ProxySetResp
	if err := json.Unmarshal(reply.Data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Code != "json_parse" {
		t.Fatalf("malformed proxy.set code=%q want json_parse", got.Code)
	}
	if on, err := session.GetProxyEnabled(b.cfg.DB, sid); err != nil || !on {
		t.Fatalf("malformed proxy.set changed the switch: on=%v err=%v", on, err)
	}
}

func TestExternalReviewHeartbeatRepairsMissedProxyOff(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)

	// round-6 F2: repair is capability-gated — register the node as capable.
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	oldEpoch, err := session.BumpProxyEpoch(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, false); err != nil {
		t.Fatal(err)
	}
	offEpoch, err := session.BumpProxyEpoch(b.cfg.DB, sid)
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

	b.repairProxyEpoch(sid, "lab-1", oldEpoch)

	select {
	case msg := <-ch:
		var d proto.ProxyDirective
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.Enabled || d.Epoch != offEpoch {
			t.Fatalf("repair directive=%+v want disabled epoch %d", d, offEpoch)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("missed proxy-OFF directive is never repaired while NATS remains connected")
	}
}

func TestExternalReviewPreP13AgentGetsNoProxyAllocation(t *testing.T) {
	_, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID:            sid,
		NID:            "legacy-1",
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: "v0.2.8",
		OS:             "linux",
		Arch:           "amd64",
		BootID:         "boot-legacy",
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}

	d := b.proxyDirectiveForRegister(sid, "legacy-1", proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: "v0.2.8",
		NID:            "legacy-1",
	})
	if d != nil {
		t.Fatalf("pre-P13 agent received a proxy directive and allocation: %+v", d)
	}
}
