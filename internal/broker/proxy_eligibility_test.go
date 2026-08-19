// proxy_eligibility_test.go — the register-time proxy eligibility fold. #78's
// opt-out and the cloned-credential lease both express "this node must not hold
// a public egress port" as a conjunct of ONE expression; these tests pin that
// the PERSISTED column and the DIRECTIVE gate stay in agreement, because they
// are computed at two different sites from the same request.
// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q1
package broker

import (
	"testing"

	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
)

// leasedRegister drives the REAL register path for an agent running under a
// broker-assigned lease name.
func leasedRegister(t *testing.T, nc *nats.Conn, sid, nid string, leased bool) proto.NodeRegisterResp {
	t.Helper()
	var resp proto.NodeRegisterResp
	req(t, nc, proto.SubjNodeRegister(sid, nid), proto.NodeRegisterReq{
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: "v0.5.0",
		NID:            nid,
		Capabilities:   []string{proto.CapProxyV1},
		LeasedNID:      leased,
	}, &resp)
	if !resp.OK {
		t.Fatalf("register(%s, leased=%v) refused: %+v", nid, leased, resp)
	}
	return resp
}

// A leased instance is ephemeral by premise: it must never be handed a public
// egress port whose disappearance breaks every /sub subscriber, and it must
// never receive the session's Shadowsocks PSKs.
//
// The fold into nodes.proxy_capable is only HALF the gate. In SINGLE mode — the
// entire live fleet — the register reply's directive is minted by
// proxyDirectiveForRegister, which gates on nodeParticipatesInProxy(req), a
// SECOND expression whose own doc comment claims it "mirrors handleRegister's
// nodes.proxy_capable fold so the directive path and the persisted column can
// never disagree within one register".
//
// MUTATION that must turn this red: add `&& !req.LeasedNID` to
// nodeParticipatesInProxy (proxy.go) — i.e. the fix — and remove it again.
func TestLeasedInstanceIsProxyIneligible(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BumpProxyEpoch(b.cfg.DB, sid); err != nil {
		t.Fatal(err)
	}

	// The basename holder: an ordinary participating device.
	if resp := leasedRegister(t, nc, sid, "lab-1", false); resp.Proxy == nil || resp.Proxy.PublicPort == 0 {
		t.Fatalf("the basename holder must still get its exit: %+v", resp.Proxy)
	}

	// The leased clone.
	resp := leasedRegister(t, nc, sid, "lab-1-02", true)

	if b.nodeProxyCapable(sid, "lab-1-02") {
		t.Fatal("a leased instance must persist proxy_capable=0")
	}
	if resp.Proxy != nil {
		t.Fatalf("a leased instance must receive NO proxy directive; got Enabled=%v port=%d token=%q keys=%d",
			resp.Proxy.Enabled, resp.Proxy.PublicPort, resp.Proxy.Token, len(resp.Proxy.Keys))
	}
	if existing, _ := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1-02"); existing != nil {
		t.Fatalf("a leased instance must not hold a __proxy__ allocation, got %+v", existing)
	}
	for _, nid := range b.onlineNIDs(sid) {
		if nid == "lab-1-02" {
			t.Fatal("a leased instance must not appear in onlineNIDs (the repair/allocate pool)")
		}
	}
}

// The operator-visible half. The plan (§2 Q1) requires the leased case to be
// disclosed with its OWN reason value and explicitly forbids reusing #78's
// OptedOut hint, whose remedy (edit agent.yaml proxy.participate) does not
// exist in a baked clone image. Today there is no disclosure channel at all:
// once the directive leak above is fixed, the leased row renders with
// Ready=false / PublicPort=0 / OptedOut=false — byte-identical to a node whose
// SS server crashed. The one row the whole increment delivers is undebuggable.
//
// The post-fix render shape is reproduced here with the session's proxy switch
// OFF, which is the only way to reach "capable-looking node, no allocation"
// through the real register path while the leak exists.
//
// MUTATION: whatever field is added to disclose it (e.g. ProxyNodeEntry.Leased
// or a ready_reason value) — delete it and this goes red. Rendering the case
// through OptedOut instead also goes red, on the second assertion.
func TestLeasedIneligibilityIsDistinguishableFromTheOptOutHint(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	// Proxy switch OFF: no directive is minted for anyone, so this is exactly
	// the row shape a leased instance has once the directive leak is closed.
	leasedRegister(t, nc, sid, "lab-1", false)
	leasedRegister(t, nc, sid, "lab-1-02", true)
	// The BINDING is what makes lab-1 a device and lab-1-02 a lease, and it is
	// the same evidence `node ls` reads (external review F12: proxy status used
	// to parse the name instead, and disagreed with every other subsystem about
	// a device the operator had named `gpu-02`). Without any binding at all the
	// classifier fails closed and reports nothing leased — correct on a broker
	// running without auth_callout, and the reason this fixture must seed one.
	if _, err := b.cfg.DB.Exec(
		`INSERT INTO agent_provisioning(sid, nid, agent_fp) VALUES (?,?,?)`,
		sid, "lab-1", "SHA256:the-image-credential"); err != nil {
		t.Fatalf("seed binding: %v", err)
	}

	nodes, err := b.proxyStatusNodes(sid)
	if err != nil {
		t.Fatal(err)
	}
	var leased, plain proto.ProxyNodeEntry
	var foundLeased, foundPlain bool
	for _, n := range nodes {
		switch n.NID {
		case "lab-1-02":
			leased, foundLeased = n, true
		case "lab-1":
			plain, foundPlain = n, true
		}
	}
	if !foundLeased || !foundPlain {
		t.Fatalf("both nodes must be listed in proxy status, got %+v", nodes)
	}
	if leased.OptedOut {
		t.Fatal("a leased instance must not be reported as OPTED OUT — that names an agent.yaml key " +
			"that does not exist in a baked clone image, and points the operator at the wrong remedy")
	}
	// Everything `proxy status` renders about a node: Status / Ready /
	// PublicPort / PublicHost / HomeBroker / OptedOut / ReadyReason.
	if leased.Ready == plain.Ready && leased.PublicPort == plain.PublicPort &&
		leased.OptedOut == plain.OptedOut && leased.ReadyReason == plain.ReadyReason {
		t.Fatalf("`proxy status` renders the LEASED row %+v identically to a merely-not-ready row %+v: "+
			"nothing in ProxyNodeEntry says WHY this instance has no exit, so a deliberate "+
			"ineligibility is indistinguishable from a capability defect", leased, plain)
	}
}

// The two halves of the gate disagreeing is not a cosmetic inconsistency: it
// creates a mint/free PING-PONG that runs on the LIVE fleet's own path.
// proxyDirectiveForRegister mints the row and the token (it does not consult
// LeasedNID); repairProxy — which single mode runs on EVERY heartbeat — sees
// proxy_capable=0 and calls freeOptOutProxyRowSingle, which CloseProxy()s the
// bound public port, FREEs the allocation and emits an audit.port{freed}.
//
// So each register of a leased clone: burns a public port, hands the clone the
// session's Shadowsocks PSKs, makes it bind a listener nobody can reach (/sub
// filters proxy_capable=1), makes it overwrite the SHARED state.json (see
// internal/agent/proxy_lease_test.go), and then revokes the token under its
// feet — leaving its tunnel supervisor re-dialling a FREED token forever, with
// the proxy_bind_stalled alert that would surface it gated to cluster mode.
//
// MUTATION: add `&& !req.LeasedNID` to nodeParticipatesInProxy and this goes
// red at the first assertion (no row is ever minted).
func TestLeasedInstanceDoesNotChurnPublicPortsAgainstTheHeartbeatRepair(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BumpProxyEpoch(b.cfg.DB, sid); err != nil {
		t.Fatal(err)
	}

	// THE CHURN THIS TEST WAS WRITTEN FOR IS NOW UNREACHABLE. The reviewer's
	// scenario needed the broker to keep minting a token-bearing directive for a
	// leased instance, which the register-reply gate (nodeParticipatesInProxy,
	// with !LeasedNID folded in) no longer does. So the loop now asserts the
	// ABSENCE of the mint — a directive here would mean an ephemeral instance
	// had been handed a public egress port, which is the thing Q1 exists to
	// prevent — and no row can leak because none is ever created.
	for i := 0; i < 3; i++ {
		resp := leasedRegister(t, nc, sid, "lab-1-02", true)
		if resp.Proxy != nil && resp.Proxy.Token != "" {
			t.Fatalf("iteration %d: a LEASED instance was handed a token-bearing proxy directive; "+
				"it is ephemeral by premise and its public port disappears with its pod, while the "+
				"/sub render (gated on proxy_capable) filters it out so nobody would ever use it", i)
		}

		// The real single-mode convergence loop: one heartbeat.
		if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1-02"), nil); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if existing, _ := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1-02"); existing == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if existing, _ := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1-02"); existing != nil {
			t.Fatalf("iteration %d: a leased instance owns a __proxy__ row %+v — no directive was "+
				"issued, so nothing should have allocated one", i, existing)
		}
	}
}

// The positive half of the ordering claim, on the PROXY surface specifically:
// a contested register must leave the incumbent's exit completely alone.
// node.Register's ON CONFLICT clause zeroes proxy_ready (round-6 F8, and that
// invariant must NOT be relaxed), so a contested register that fell through to
// registerNode would knock a healthy exit out of /sub on every clone arrival —
// with no port event and no log line to explain it.
//
// MUTATION: move the adjudicateLease call below registerNode in handleRegister
// and this goes red on proxy_ready.
func TestContestedRegisterLeavesTheIncumbentsExitIntact(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BumpProxyEpoch(b.cfg.DB, sid); err != nil {
		t.Fatal(err)
	}
	const holderID = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
	const cloneID = "bbbbbbbbbbbbbbbbbbbbbbbbbb"

	instRegister := func(nid, iid string) proto.NodeRegisterResp {
		t.Helper()
		var resp proto.NodeRegisterResp
		req(t, nc, proto.SubjNodeRegister(sid, nid), proto.NodeRegisterReq{
			ProtoVersion: proto.ProtoVersion, ReleaseVersion: "v0.5.0", NID: nid,
			Capabilities: []string{proto.CapProxyV1}, InstanceID: iid,
		}, &resp)
		return resp
	}

	holder := instRegister("lab-1", holderID)
	if holder.Proxy == nil || holder.Proxy.PublicPort == 0 {
		t.Fatalf("the incumbent must get an exit: %+v", holder.Proxy)
	}
	wantPort := holder.Proxy.PublicPort
	// The incumbent has bound and ACKed: it is being rendered into /sub.
	if err := node.SetProxyReady(b.cfg.DB, sid, "lab-1", true); err != nil {
		t.Fatal(err)
	}

	// A live incumbent: interest exists on its forwarded subject, which is what
	// probeNameInUse actually measures.
	sub, err := nc.Subscribe(proto.SubjCmdForwarded(sid, "lab-1", "*"), func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	clone := instRegister("lab-1", cloneID)
	if clone.Lease == nil || clone.Lease.AssignedNID == "lab-1" {
		t.Fatalf("the second instance must be leased a different name, got %+v", clone.Lease)
	}

	if !b.nodeProxyReady(sid, "lab-1") {
		t.Fatal("the clone's register cleared the INCUMBENT's proxy_ready — it drops out of /sub " +
			"with no port event and no log line")
	}
	if !b.nodeProxyCapable(sid, "lab-1") {
		t.Fatal("the clone's register cleared the incumbent's proxy_capable")
	}
	existing, err := port.LookupProxyByNode(b.cfg.DB, sid, "lab-1")
	if err != nil || existing == nil || existing.Port != wantPort {
		t.Fatalf("the incumbent's __proxy__ allocation moved or vanished: want port %d, got %+v (err=%v)",
			wantPort, existing, err)
	}
	if clone.Proxy != nil {
		t.Fatalf("a contested register must carry no proxy directive at all, got %+v", clone.Proxy)
	}
}
