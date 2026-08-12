package broker

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxysub"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

func TestExternalReviewConvergedHeartbeatDoesNotEscalateGeneration(t *testing.T) {
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

	gen := b.proxyGenLoad()
	var persistedBefore int64
	if err := b.cfg.DB.QueryRow(`SELECT generation FROM proxy_meta WHERE id=1`).Scan(&persistedBefore); err != nil {
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

	body, err := json.Marshal(proto.HeartbeatPayload{
		Ts:              time.Now(),
		ProxyGeneration: gen,
		ProxyEpoch:      epoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1"), body); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-ch:
		t.Fatalf("converged heartbeat unexpectedly triggered directive: %s", msg.Data)
	case <-time.After(300 * time.Millisecond):
	}

	if got := b.proxyGenLoad(); got != gen {
		t.Fatalf("converged heartbeat escalated in-memory generation: got %d want %d", got, gen)
	}
	var persistedAfter int64
	if err := b.cfg.DB.QueryRow(`SELECT generation FROM proxy_meta WHERE id=1`).Scan(&persistedAfter); err != nil {
		t.Fatal(err)
	}
	if persistedAfter != persistedBefore {
		t.Fatalf(
			"converged heartbeat escalated persisted generation: before %d after %d",
			persistedBefore, persistedAfter,
		)
	}
}

func TestExternalReviewGenerationEscalationRejectsUnrepresentableAgentValue(t *testing.T) {
	_, _, _, b := proxyTestBroker(t)
	before := b.proxyGenLoad()

	if b.escalateProxyGen(maxProxyGeneration) {
		t.Fatal("escalation reported success for an agent generation at the configured maximum")
	}
	if got := b.proxyGenLoad(); got != before {
		t.Fatalf("failed escalation changed in-memory generation: got %d want %d", got, before)
	}

	var persisted int64
	if err := b.cfg.DB.QueryRow(`SELECT generation FROM proxy_meta WHERE id=1`).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != before {
		t.Fatalf("failed escalation persisted out-of-range generation %d; want %d", persisted, before)
	}
}

func TestExternalReviewLegacyHeartbeatCannotEscalateProxyGeneration(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "legacy-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.8", ProxyCapable: false,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}

	before := b.proxyGenLoad()
	body, err := json.Marshal(proto.HeartbeatPayload{
		Ts:              time.Now(),
		ProxyGeneration: before + 100,
		ProxyEpoch:      0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "legacy-1"), body); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := b.proxyGenLoad(); got != before {
			t.Fatalf("pre-P13 heartbeat escalated global generation: got %d want %d", got, before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestExternalReviewHeartbeatCannotExhaustGlobalGeneration(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(proto.HeartbeatPayload{
		Ts:              time.Now(),
		ProxyGeneration: maxProxyGeneration - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1"), body); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if b.proxyGenLoad() >= maxProxyGeneration {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := New(Config{
		NATSURL: b.cfg.NATSURL,
		DB:      b.cfg.DB,
		Now:     b.cfg.Now,
		Logger:  b.cfg.Logger,
	}); err != nil {
		t.Fatalf("one authenticated heartbeat made the next broker startup impossible: %v", err)
	}
}

func TestExternalReviewRegisterClearsStaleProxyReady(t *testing.T) {
	_, _, sid, b := proxyTestBroker(t)
	in := node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}
	if err := node.Register(b.cfg.DB, in, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := node.SetProxyReady(b.cfg.DB, sid, "lab-1", true); err != nil {
		t.Fatal(err)
	}

	in.BootID = "new-agent-process"
	if err := node.Register(b.cfg.DB, in, time.Now()); err != nil {
		t.Fatal(err)
	}
	var ready int
	if err := b.cfg.DB.QueryRow(
		`SELECT proxy_ready FROM nodes WHERE sid=? AND nid=?`, sid, "lab-1",
	).Scan(&ready); err != nil {
		t.Fatal(err)
	}
	if ready != 0 {
		t.Fatalf("register preserved proxy_ready=%d before the new process re-established its data plane", ready)
	}
}

// TestBrokerNewRejectsNonLoopbackSubscriptionListen pins the PREFLIGHT half of
// the two-stage listener contract: a non-loopback /sub address is a POLICY
// violation (the surface is unauthenticated + Caddy-fronted), so it must be
// rejected at New() by the shared no-I/O validator — before any listener bind
// or DB side effect — not deep inside Run(). This is the fail-closed direction:
// an operator upgrading with --config-check learns the address is illegal
// without the broker ever having half-started.
// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F10
func TestBrokerNewRejectsNonLoopbackSubscriptionListen(t *testing.T) {
	if _, err := New(Config{
		// New only validates this URL; it does not connect. Keeping the fixture
		// offline proves the policy rejection really precedes network I/O and
		// lets this pure-preflight test run in a no-network sandbox.
		NATSURL:     "nats://127.0.0.1:4222",
		DB:          openDB(t),
		Logger:      silentLogger(),
		SubHTTPAddr: "0.0.0.0:0", // wildcard bind = non-loopback = policy error
	}); err == nil {
		t.Fatal("New must reject a non-loopback subscription listener at preflight (fail-closed), not defer it to Run")
	}
}

// TestExternalReviewBrokerPropagatesSubscriptionListenerStartupFailure pins the
// RUNTIME half of the same contract (F10): an address that is format-valid AND
// loopback (so the New() preflight legitimately passes) but whose port is
// already occupied is an availability failure only Bind can discover. Run must
// propagate it and exit, never leave a healthy-looking broker with a dead
// subscription endpoint. Using a genuinely-occupied loopback port keeps this
// test about the Run/bind layer now that pure address policy is caught earlier.
// origin: docs/reviews/g75-g78-deploy-defaults-external-review.md F10
func TestExternalReviewBrokerPropagatesSubscriptionListenerStartupFailure(t *testing.T) {
	// Hold a loopback port so the bind inside Run fails with EADDRINUSE — a
	// real runtime-availability error, not a config-policy one.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()

	b, err := New(Config{
		NATSURL:     startNATS(t),
		DB:          openDB(t),
		Logger:      silentLogger(),
		SubHTTPAddr: occupied.Addr().String(), // loopback + valid ⇒ preflight passes
	})
	if err != nil {
		t.Fatalf("New must accept a loopback listener even when the port is busy (availability is not the preflight's job): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("broker exited successfully despite an unbindable subscription listener")
		}
	case <-time.After(500 * time.Millisecond):
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("broker kept running after its configured subscription listener failed to bind")
	}
}

func TestExternalReviewRevokeDoesNotSucceedWithoutEpochBump(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	if _, err := proxysub.Create(b.cfg.DB, sid, "alice", "reviewer", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.cfg.DB.Exec(`
CREATE TRIGGER external_review_fail_proxy_epoch
BEFORE UPDATE OF proxy_epoch ON sessions
BEGIN
	SELECT RAISE(ABORT, 'external review epoch failure');
END;`); err != nil {
		t.Fatal(err)
	}

	var resp proto.ProxySubRevokeResp
	req(t, nc, proto.SubjCtrlProxySubRevoke(owner, sid), proto.ProxySubRevokeReq{Name: "alice"}, &resp)
	if resp.OK {
		t.Fatalf("revoke reported success even though its required proxy epoch bump failed")
	}
}

func TestExternalReviewEnableRollsBackSwitchWhenEpochBumpFails(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)
	if _, err := b.cfg.DB.Exec(`
CREATE TRIGGER external_review_fail_proxy_epoch
BEFORE UPDATE OF proxy_epoch ON sessions
BEGIN
	SELECT RAISE(ABORT, 'external review epoch failure');
END;`); err != nil {
		t.Fatal(err)
	}

	var resp proto.ProxySetResp
	req(t, nc, proto.SubjCtrlProxySet(owner, sid), proto.ProxySetReq{Enabled: true}, &resp)
	if resp.OK {
		t.Fatalf("enable unexpectedly succeeded with a failed proxy epoch bump")
	}
	enabled, err := session.GetProxyEnabled(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatalf("failed enable left the authorization switch enabled")
	}
}

func TestExternalReviewSubscriberChangeWhileOffDoesNotPushEnable(t *testing.T) {
	nc, owner, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
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

	var resp proto.ProxySubCreateResp
	req(t, nc, proto.SubjCtrlProxySubCreate(owner, sid), proto.ProxySubCreateReq{Name: "while-off"}, &resp)
	if !resp.OK {
		t.Fatalf("create subscriber while off failed: %+v", resp)
	}
	select {
	case msg := <-ch:
		t.Fatalf("subscriber change while proxy was OFF pushed a directive: %s", msg.Data)
	case <-time.After(300 * time.Millisecond):
	}
}
