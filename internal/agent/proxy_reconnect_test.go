package agent

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// TestProxyReadinessHookPublishesAndFilters: the tunnel session-state hook
// publishes proxy ready/unready ONLY for the embedded proxy's public port; a
// transition on a regular `expose` port is ignored (Defect B + the port
// filter). Also pins nil-nc safety.
func TestProxyReadinessHookPublishesAndFilters(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	a := newProxyTestAgent(t)
	a.ncBox.Store(nc)
	a.proxyPublicPort.Store(14004) // the proxy port

	unready := make(chan *nats.Msg, 4)
	ready := make(chan *nats.Msg, 4)
	subU, err := nc.ChanSubscribe(proto.SubjEvNodeProxyReady(a.cfg.SID, a.cfg.NID, "unready"), unready)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subU.Unsubscribe() }()
	subR, err := nc.ChanSubscribe(proto.SubjEvNodeProxyReady(a.cfg.SID, a.cfg.NID, "ready"), ready)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subR.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// A regular (non-proxy) expose port flap must NOT touch proxy readiness.
	a.onTunnelSessionState(14001, false)
	select {
	case <-unready:
		t.Fatal("a non-proxy expose drop must not publish proxy unready")
	case <-time.After(200 * time.Millisecond):
	}

	// The proxy port down → unready.
	a.onTunnelSessionState(14004, false)
	select {
	case <-unready:
	case <-time.After(time.Second):
		t.Fatal("proxy port drop did not publish unready")
	}

	// The proxy port up → ready.
	a.onTunnelSessionState(14004, true)
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("proxy port recovery did not publish ready")
	}

	// nil nc must not panic (e.g. a transition during a NATS reconnect gap).
	a.ncBox.Store(nil)
	a.onTunnelSessionState(14004, false) // must be a safe no-op
}

// TestProxyReadinessHookNoDeadlockUnderConcurrentDirective pins the lock-order
// invariant (p.mu → c.mu): AddProxy → tunnel.Open runs under p.mu, so a
// session-state callback firing while p.mu is held MUST NOT take p.mu, or the
// first-drop callback would deadlock. We hold the proxy runtime mutex and
// require the hook to complete regardless.
func TestProxyReadinessHookNoDeadlockUnderConcurrentDirective(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	a := newProxyTestAgent(t)
	a.ncBox.Store(nc)
	a.proxyPublicPort.Store(14004)

	a.proxy.mu.Lock() // simulate applyProxyDirective / proxyStartLocked holding p.mu
	defer a.proxy.mu.Unlock()

	done := make(chan struct{})
	go func() {
		a.onTunnelSessionState(14004, false)
		a.onTunnelSessionState(14004, true)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session-state hook blocked on p.mu — lock-order invariant violated")
	}
}
