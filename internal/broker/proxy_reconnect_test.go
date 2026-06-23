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

// --- Fix C: tunnelTokenLookup transient-vs-authoritative DENY taxonomy -------

// TestTunnelTokenLookupAbsentIsRevoked: a missing/unknown token (ErrNotFound)
// stays the authoritative, anti-enumeration token_unknown_or_revoked.
func TestTunnelTokenLookupAbsentIsRevoked(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}}
	err := b.tunnelTokenLookup("lab", "lab-1", 14000, "no-such-token-hash", 0)
	if err == nil || err.Error() != "token_unknown_or_revoked" {
		t.Fatalf("absent token: want token_unknown_or_revoked, got %v", err)
	}
}

// TestTunnelTokenLookupTransientStoreErrorIsTryAgain: a transient store fault
// (here: a closed DB) must NOT masquerade as a revocation — it must return the
// transient `try_again` reason so a reconnecting agent retries instead of
// terminating its supervisor forever (the DB-triggerable false-online hole).
func TestTunnelTokenLookupTransientStoreErrorIsTryAgain(t *testing.T) {
	db := openDB(t)
	b := &Broker{cfg: Config{DB: db, Logger: silentLogger()}}
	_ = db.Close() // induce a non-ErrNotFound store error on the next query
	err := b.tunnelTokenLookup("lab", "lab-1", 14000, "any-hash", 0)
	if err == nil || err.Error() != "try_again" {
		t.Fatalf("transient store error: want try_again, got %v", err)
	}
}

// TestTunnelTokenLookupMismatchIsRevoked: a found row whose port/sid/nid does
// not match stays the AUTHORITATIVE terminal token_unknown_or_revoked (Fix C
// must not route an authoritative verdict through the transient branch).
func TestTunnelTokenLookupMismatchIsRevoked(t *testing.T) {
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
	// Correct token hash but WRONG port → authoritative mismatch (terminal).
	if err := b.tunnelTokenLookup(sid, "lab-1", alloc.Port+1, alloc.TokenHash, 0); err == nil || err.Error() != "token_unknown_or_revoked" {
		t.Fatalf("port mismatch: want token_unknown_or_revoked, got %v", err)
	}
}

// TestTunnelTokenLookupProxyOffIsTerminalNotTransient: a __proxy__ REGISTER
// while the master switch is OFF (healthy DB, ALLOCATED row present) MUST be the
// authoritative terminal reason, NOT the new transient try_again — else a
// disabled exit could resurrect on the agent's reconnect retry loop (the exact
// kill-switch invariant Fix C's __proxy__-gate split must preserve).
func TestTunnelTokenLookupProxyOffIsTerminalNotTransient(t *testing.T) {
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
	// Switch never enabled → GetProxyEnabled returns (false, nil) → terminal.
	if err := b.tunnelTokenLookup(sid, "lab-1", alloc.Port, alloc.TokenHash, 0); err == nil || err.Error() != "token_unknown_or_revoked" {
		t.Fatalf("proxy-off __proxy__ REGISTER: want terminal token_unknown_or_revoked, got %v", err)
	}
}

// --- Fix D: repairProxy must not nudge a self-healing not-ready node ----------

func registerCapableOnNode(t *testing.T, b *Broker, sid string) {
	t.Helper()
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
}

// TestRepairProxyNoStormWhenOnNotReadyAtSamePair: with proxy ON, the node
// not-ready (tunnel self-healing), and the heartbeat reporting the EXACT
// current (generation, epoch), repairProxy must push NOTHING — otherwise the
// agent `default:` branch re-ACKs ready=true every heartbeat and flaps /sub.
func TestRepairProxyNoStormWhenOnNotReadyAtSamePair(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	registerCapableOnNode(t, b, sid)

	epoch, err := session.GetProxyEpoch(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}
	gen := b.proxyGenLoad()

	ch := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// proxy_ready stays 0 (never published). Heartbeat matches the pair exactly.
	body, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now(), ProxyGeneration: gen, ProxyEpoch: epoch})
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1"), body); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()

	select {
	case msg := <-ch:
		t.Fatalf("repairProxy must not nudge an on&&!ready node at the same pair; got push %s", string(msg.Data))
	case <-time.After(400 * time.Millisecond):
		// no push — correct.
	}
}

// TestRepairProxyPushesOnGenEpochMismatch: a genuine epoch divergence (agent
// behind) at not-ready still re-pushes — Fix D suppresses ONLY the not-ready-
// only case, never a real mismatch.
func TestRepairProxyPushesOnGenEpochMismatch(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	registerCapableOnNode(t, b, sid)

	// Advance the session epoch so the heartbeat (epoch 0) genuinely lags.
	newEpoch, err := session.BumpProxyEpoch(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}
	gen := b.proxyGenLoad()

	ch := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(proto.HeartbeatPayload{Ts: time.Now(), ProxyGeneration: gen, ProxyEpoch: 0})
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1"), body); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()

	select {
	case msg := <-ch:
		var d proto.ProxyDirective
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if !d.Enabled || d.Epoch != newEpoch {
			t.Fatalf("epoch-mismatch heartbeat want enabled epoch %d, got %+v", newEpoch, d)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("genuine epoch mismatch must still re-push the directive")
	}
}
