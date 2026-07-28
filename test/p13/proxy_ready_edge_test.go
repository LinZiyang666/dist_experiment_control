// proxy_ready_edge_test.go — the data-plane drop is an EVENT, and Defect B must be
// asserted on the event, not on the level it briefly produces.
//
// origin: lane-XC cross-cutting review of batch B, resolving the recurring
// "proxy_ready did not clear on tunnel drop" failure of
// TestProxyFalseOnlineRecoversAfterTunnelDrop (proxy_reconnect_e2e_test.go).
//
// WHY THIS FILE EXISTS
// --------------------
// nodes.proxy_ready is cleared and restored by the SAME agent goroutine:
// tunnel.Client.supervise publishes the down edge the instant runAcceptLoop returns,
// then redials after jitter(backoffBase) with backoffBase = 500ms and publishes the
// up edge. So proxy_ready == 0 is a TRANSIENT LEVEL whose width is uniform in
// (0, 500ms] plus one handshake — measured at 305ms / 417ms / 447ms on an idle
// -race box by the diagnostic below.
//
// A test that polls `!proxyReady(...)` is therefore sampling a window, not waiting
// for a settled state: once the agent self-heals, the predicate is false forever, so
// raising the poll DEADLINE cannot help. Under the full parallel matrix the polling
// goroutine only has to be descheduled across that one window to miss it, and the
// test then reports a product defect that did not happen (docs/testing-standards.md
// §T4). The edge assertion below is immune to that: the NATS message is buffered by
// the subscription and is still there whenever the test gets scheduled again.
package p13_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/proto"
)

// TestProxyPublishesUnreadyEdgeOnTunnelDrop is the load-robust form of the Defect-B
// assertion: a pure data-plane sever (NATS untouched) MUST make the agent publish the
// unready ACK, which is the only thing that clears nodes.proxy_ready and drops the exit
// from /sub. It also records the width of the clear window, so the next person to look
// at a "proxy_ready did not clear" failure can see what they are up against.
//
// Mutation that proves it: delete the `a.pubProxyReady(nc, up)` call in
// internal/agent/agent.go's onTunnelSessionState (or the notifyState(publicPort, false)
// in internal/tunnel/tunnel.go's supervise) and this goes red in ~1ms of real signal
// rather than after a 15s poll that may or may not have sampled the window.
func TestProxyPublishesUnreadyEdgeOnTunnelDrop(t *testing.T) {
	f := startFullStackProxy(t)

	// Subscribe to BOTH edges BEFORE severing. Buffered channels: the assertion is
	// "the event was published", which survives an arbitrarily late reader.
	down := make(chan *nats.Msg, 8)
	up := make(chan *nats.Msg, 8)
	subD, err := f.nc.ChanSubscribe(proto.SubjEvNodeProxyReady("lab", "lab-1", "unready"), down)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subD.Unsubscribe() }()
	subU, err := f.nc.ChanSubscribe(proto.SubjEvNodeProxyReady("lab", "lab-1", "ready"), up)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subU.Unsubscribe() }()
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}

	t0 := time.Now()
	f.relay.sever() // data-plane blip; NATS untouched

	var tDown, tUp time.Duration
	select {
	case <-down:
		tDown = time.Since(t0)
	case <-time.After(15 * time.Second):
		t.Fatal("no proxy.unready edge after a tunnel drop (Defect B: the broker is never told " +
			"the data plane died, so /sub keeps advertising a dead exit)")
	}

	// The recovery edge, for the same reason: the agent must self-heal over the tunnel
	// alone. Asserted as an event so it cannot be missed by a descheduled poller either.
	select {
	case <-up:
		tUp = time.Since(t0)
	case <-time.After(15 * time.Second):
		t.Fatal("no proxy.ready edge — the tunnel reconnect did not self-heal")
	}

	if tUp <= tDown {
		t.Fatalf("edges out of order: unready at %v, ready at %v", tDown, tUp)
	}
	t.Logf("proxy_ready==0 window: unready at %v, ready at %v, width %v "+
		"(this is the window a level-poll has to land inside)", tDown, tUp, tUp-tDown)
}
