package agent

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: p13_external_review_round6_test.go (renamed in B6) — docs/reviews/p13-external-review-round6.md
func TestExternalReviewFailClosedReconnectCanReapplyCurrentDirective(t *testing.T) {
	a := newProxyTestAgent(t)
	directive := &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "current-token",
		Cipher:     "chacha20-ietf-poly1305",
		Keys:       []proto.ProxyKey{{SubID: "current", Secret: "current-key"}},
		Generation: 100,
		Epoch:      5,
	}
	a.applyProxyDirective(nil, directive)
	if !runningSrv(a) {
		t.Fatal("precondition: proxy should be serving")
	}

	a.failClosedFire()
	if runningSrv(a) {
		t.Fatalf("fail-closed teardown left proxy serving")
	}

	a.applyProxyDirective(nil, directive)
	if !runningSrv(a) {
		t.Fatalf("authoritative reconnect directive at the current pair was dropped after fail-closed teardown")
	}
}

func TestExternalReviewDuplicateCurrentDirectiveReacksReady(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	a := newProxyTestAgent(t)
	directive := &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "current-token",
		Cipher:     "chacha20-ietf-poly1305",
		Keys:       []proto.ProxyKey{{SubID: "current", Secret: "current-key"}},
		Generation: 100,
		Epoch:      5,
	}
	a.applyProxyDirective(nc, directive)

	ready := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjEvNodeProxyReady(a.cfg.SID, a.cfg.NID, "ready"), ready)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// Models broker readiness state being cleared/lost while the proxy is still
	// serving. The broker's convergence loop re-sends the same authoritative
	// pair; the agent must re-ACK rather than drop before pubProxyReady.
	a.applyProxyDirective(nc, directive)
	select {
	case <-ready:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("duplicate current directive did not re-ACK readiness")
	}
}

// failClosedArmed reads the fail-closed countdown under its own mutex.
func failClosedArmed(a *Agent) bool {
	a.flcMu.Lock()
	defer a.flcMu.Unlock()
	return a.flcTimer != nil
}

// connCallbacks applies the CURRENT session's real option set, so the test drives
// the very closures nats.go would call rather than the helpers they wrap.
func connCallbacks(t *testing.T, a *Agent) nats.Options {
	t.Helper()
	var o nats.Options
	for _, opt := range a.buildConnOptions() {
		if err := opt(&o); err != nil {
			t.Fatalf("apply option: %v", err)
		}
	}
	if o.DisconnectedErrCB == nil || o.ReconnectedCB == nil {
		t.Fatal("buildConnOptions no longer installs both handlers; this guard is testing nothing")
	}
	return o
}

// The h100 sequence (2026-10-03, 9h55m egress outage while the broker reported
// READY): a reconnect pass is still retrying its register, the link drops again
// (the disconnect arms the countdown), and the reconnect that ends that partition
// is DROPPED by the single-flight. The cancel used to live only at the pass's
// entry, so nothing cancelled this countdown and a healthy agent tore its SS exit
// down one grace later. The control half proves the same handler-armed countdown
// really does fire in this harness, so "still serving" is not vacuous.
// origin: proxy fail-closed stale-timer defect (h100); hot-update S6 proxyfix
func TestDroppedReconnectCancelsFailClosedCountdown(t *testing.T) {
	const grace = 80 * time.Millisecond
	serve := func(t *testing.T) *Agent {
		a := newProxyTestAgent(t)
		a.cfg.ProxyFailClosedGrace = grace
		a.applyProxyDirective(nil, &proto.ProxyDirective{
			Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
			Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
		})
		if !runningSrv(a) {
			t.Fatal("precondition: proxy should be serving")
		}
		t.Cleanup(func() {
			a.stopRedialWatchdog()
			a.cancelFailClosed()
			a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Epoch: 1 << 30})
		})
		return a
	}

	t.Run("dropped reconnect", func(t *testing.T) {
		a := serve(t)
		o := connCallbacks(t, a)
		a.reconnectInFlight.Store(true) // pass A is still inside its register retry
		o.DisconnectedErrCB(nil, nil)
		if !failClosedArmed(a) {
			t.Fatal("precondition: the disconnect handler did not arm the countdown")
		}
		o.ReconnectedCB(nil)
		if !a.reconnectInFlight.Load() {
			t.Fatal("precondition: the reconnect was not the single-flight-dropped one")
		}
		if failClosedArmed(a) {
			t.Fatal("a reconnect dropped by the single-flight left the fail-closed countdown armed; " +
				"nothing else will cancel it, so a healthy agent loses its SS exit one grace later (h100)")
		}
		for deadline := time.Now().Add(4 * grace); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if !runningSrv(a) {
				t.Fatal("SS exit torn down although the partition ended with a reconnect")
			}
		}
	})

	t.Run("control: no reconnect fires", func(t *testing.T) {
		a := serve(t)
		o := connCallbacks(t, a)
		o.DisconnectedErrCB(nil, nil)
		deadline := time.Now().Add(2 * time.Second)
		for runningSrv(a) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if runningSrv(a) {
			t.Fatal("the handler-armed countdown never fired; the dropped-reconnect assertion is vacuous")
		}
	})
}

// The second half of the same ordering defect: the reconnect pass runs on its own
// goroutine AFTER nats.go's callback queue has moved on. When it cancelled at its
// entry, a disconnect that landed between its reconnect callback and the goroutine
// had its countdown erased — fail-open for a partition that then persisted. The
// pass must leave the countdown to the callbacks, even when its register succeeds.
// origin: proxy fail-closed stale-timer defect (h100); hot-update S6 proxyfix
func TestReconnectPassLeavesLaterDisconnectCountdownArmed(t *testing.T) {
	url := testharness.StartNATS(t)
	a, err := New(Config{
		NATSURL: url, SID: "lab", NID: "gpu1", Home: t.TempDir(),
		Logger: testharness.SilentLog(), HeartbeatInterval: time.Second, RegisterTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	broker, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	registers := make(chan struct{}, 4)
	sub, err := broker.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		registers <- struct{}{} // before Respond: the pass may return the instant the reply lands
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := broker.Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.setRunCtx(ctx)
	a.cfg.ProxyFailClosedGrace = time.Minute
	defer a.cancelFailClosed()

	client, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	a.armFailClosed() // the disconnect that followed this pass's reconnect callback
	a.onNATSReconnect(client)
	select {
	case <-registers:
	default:
		t.Fatal("precondition: the reconnect pass never registered, so it proved nothing")
	}
	if !failClosedArmed(a) {
		t.Fatal("the reconnect pass cancelled a countdown armed by a LATER disconnect; " +
			"the partition that followed is now fail-open")
	}
}
