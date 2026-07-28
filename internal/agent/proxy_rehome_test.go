package agent

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/proto"
)

// c5_proxy_rehome_test.go — locks the Stage-C agent-side fixes: M2 (ProxyBound reflects tunnel
// liveness, not just p.srv), N1 (rehome scoped to the current allocation's port), M4 (an ApplyHome
// no-op on an absent tunnel session must NOT falsely advance the home epoch).

func newC5ProxyAgent(t *testing.T, adapter ExposeAdapter) *Agent {
	t.Helper()
	a, err := New(Config{
		NATSURL:       "nats://127.0.0.1:4222",
		SID:           "lab",
		NID:           "lab-1",
		Home:          t.TempDir(),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: adapter,
	})
	if err != nil {
		t.Fatal(err)
	}
	a.runCtx = context.Background()
	return a
}

func c5FullDirective(port int, homeEpoch int64) *proto.ProxyDirective {
	return &proto.ProxyDirective{
		Enabled: true, PublicPort: port, Token: "tok", Cipher: ssproxy.Cipher,
		Keys:  []proto.ProxyKey{{SubID: "s0", Secret: "cccccccccccccccc"}},
		Epoch: 1,
		Home:  &proto.HomeDirective{PublicPort: port, BrokerAddr: "home-1", Epoch: homeEpoch, CertPins: proto.CertPins{Current: "pin1"}},
	}
}

// origin: c5_proxy_rehome_test.go (renamed in B6) — docs/reviews/c5-plan.md, docs/reviews/c5-review.md
//
// M2: a pure tunnel drop (SS server still up) must report ProxyBound==false, so a cluster broker does
// not resurrect proxy_ready over a dead tunnel.
func TestC5ProxyBoundReflectsTunnelLiveness(t *testing.T) {
	a := newProxyTestAgent(t) // noopProxyAdapter (no home); establishes a real SS server
	if a.proxyBound() {
		t.Fatal("not serving yet → ProxyBound must be false")
	}
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: ssproxy.Cipher,
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "cccccccccccccccc"}}, Epoch: 1,
	})
	if a.proxy == nil || a.proxy.srv == nil {
		t.Fatal("server should be running after a full enable")
	}
	if !a.proxyBound() {
		t.Fatal("serving + tunnel up → ProxyBound must be true")
	}
	// Simulate a reverse-tunnel DROP (the SS server stays up). proxyPublicPort was stored as 14000.
	a.onTunnelSessionState(14000, false)
	if a.proxyBound() {
		t.Fatal("M2: a tunnel-down with the SS server still up must report ProxyBound==false")
	}
	// A tunnel reconnect restores it.
	a.onTunnelSessionState(14000, true)
	if !a.proxyBound() {
		t.Fatal("a tunnel reconnect must restore ProxyBound==true")
	}
}

// #33 (R8a): a SUCCESSFUL rehome must RESTORE ProxyBound.
//
// This is the assertion this file was missing. TestC5ProxyBoundReflectsTunnelLiveness above
// pins the two notifyState edges (drop clears, redial restores) but never covered the third
// writer of a live tunnel: ApplyHome. The rehome success branch updated homeAddr/homeEpoch and
// nothing else, and ApplyHome→OpenHome never calls notifyState — so after a home broker died
// (drop edge wrote proxyTunnelUp=false) and the rehome to the survivor SUCCEEDED, the flag
// stayed false over a tunnel that was demonstrably up.
//
// The observable damage was inverted and therefore looked non-deterministic: a FLAPPING tunnel
// self-healed (its redial edge fires notifyState(true)) while a STABLE rehomed one never did —
// its heartbeats reported proxy_ready=false forever, and after proxyRehomeDwell bad reads the
// broker's reaper minted a fresh port+token and stranded the data plane.
func TestC5ProxyRehomeSuccessRestoresProxyBound(t *testing.T) {
	adapter := newFakeHomeAdapter(func(int, int64, int) error { return nil }) // ApplyHome succeeds
	a := newC5ProxyAgent(t, adapter)

	// Serving, tunnel live.
	a.applyProxyDirective(context.Background(), nil, c5FullDirective(14000, 1))
	if a.proxy == nil || a.proxy.srv == nil {
		t.Fatal("server should be running after a full enable")
	}
	a.onTunnelSessionState(14000, true)
	if !a.proxyBound() {
		t.Fatal("serving + tunnel up → ProxyBound must be true")
	}

	// The home broker dies: the supervisor's drop edge clears tunnel liveness.
	a.onTunnelSessionState(14000, false)
	if a.proxyBound() {
		t.Fatal("a tunnel drop must clear ProxyBound")
	}

	// The rehome to the surviving broker SUCCEEDS: ApplyHome returns nil AND HasSession is true.
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: ssproxy.Cipher,
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "cccccccccccccccc"}}, Epoch: 1,
		Home: &proto.HomeDirective{PublicPort: 14000, BrokerAddr: "home-2", Epoch: 2, CertPins: proto.CertPins{Current: "pin2"}},
	})
	if a.proxy.homeEpoch != 2 || a.proxy.homeAddr != "home-2" {
		t.Fatalf("rehome did not take: homeAddr=%q homeEpoch=%d want (home-2, 2)", a.proxy.homeAddr, a.proxy.homeEpoch)
	}
	if !a.proxyBound() {
		t.Fatal("#33: a SUCCESSFUL rehome proved a live tunnel but left ProxyBound false — " +
			"every heartbeat now reports proxy_ready=false and the broker's reaper will strand this expose")
	}
}

// N1: a rehome directive minted for a DIFFERENT (prior) allocation port must be ignored — its
// higher per-allocation epoch must not fence the current epoch-0 home.
func TestC5ProxyRehomeScopedByPort(t *testing.T) {
	adapter := newFakeHomeAdapter(func(int, int64, int) error { return nil })
	a := newC5ProxyAgent(t, adapter)
	a.applyProxyDirective(context.Background(), nil, c5FullDirective(14000, 0))
	if a.proxy == nil || a.proxy.srv == nil {
		t.Fatal("server should be running")
	}
	callsBefore := adapter.calls
	// A STALE rehome for a different port (14001), higher epoch — must be scoped out.
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "cccccccccccccccc"}}, Epoch: 1,
		Home: &proto.HomeDirective{PublicPort: 14001, BrokerAddr: "stale", Epoch: 5, CertPins: proto.CertPins{Current: "p"}},
	})
	if a.proxy.homeEpoch != 0 {
		t.Fatalf("N1: a rehome for a different port must be ignored; homeEpoch=%d want 0", a.proxy.homeEpoch)
	}
	if adapter.calls != callsBefore {
		t.Fatalf("N1: ApplyHome must NOT be called for an out-of-allocation rehome")
	}
}

// M4 + N2: ApplyHome returns nil (success no-op) when the tunnel session is absent — that is NOT a
// real rehome; (M4) the home epoch must not advance, and (N2) the node must end UNREADY — the exact-
// equal keyset re-ACK must NOT override the unready and keep /sub vending a home with no live tunnel.
// Driven over a real NATS conn so the readiness publishes are actually observed (a nil conn would make
// pubProxyReady a no-op and hide the N2 override — the closure-review's false-confidence trap).
func TestC5ProxyRehomeAbsentSessionUnreadyAndNoFalseAdvance(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var mu sync.Mutex
	var seq []string // ordered ready/unready kinds the agent publishes
	sub, err := nc.Subscribe(proto.SubjEvNodeProxyReady("lab", "lab-1", "*"), func(m *nats.Msg) {
		parts := strings.Split(m.Subject, ".")
		mu.Lock()
		seq = append(seq, parts[len(parts)-1]) // trailing token = kind
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	adapter := newFakeHomeAdapter(func(int, int64, int) error { return nil }) // ApplyHome "succeeds"
	adapter.checkSessions = true
	a := newC5ProxyAgent(t, adapter)
	a.applyProxyDirective(context.Background(), nc, c5FullDirective(14000, 0)) // establish → "ready"
	if a.proxy == nil || a.proxy.srv == nil {
		t.Fatal("server should be running")
	}
	// The tunnel session vanishes WITHOUT nilling p.srv (e.g. a superseded-epoch DENY dropped it).
	adapter.mu.Lock()
	adapter.hasSession[14000] = false
	adapter.mu.Unlock()
	// A newer Home for the SAME port arrives; ApplyHome no-ops (session absent) but returns nil.
	a.applyProxyDirective(context.Background(), nc, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "cccccccccccccccc"}}, Epoch: 1,
		Home: &proto.HomeDirective{PublicPort: 14000, BrokerAddr: "home-2", Epoch: 1, CertPins: proto.CertPins{Current: "p2"}},
	})
	if a.proxy.homeEpoch != 0 {
		t.Fatalf("M4: an ApplyHome no-op (absent session) must NOT advance homeEpoch; got %d want 0", a.proxy.homeEpoch)
	}
	// Let the publishes flush.
	_ = nc.Flush()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seq)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seq) == 0 {
		t.Fatal("expected at least one readiness publish")
	}
	// N2: the FINAL readiness must be unready — a failed rehome must not be overridden back to ready.
	if last := seq[len(seq)-1]; last != "unready" {
		t.Fatalf("N2: after a failed rehome the node must end UNREADY; readiness sequence=%v", seq)
	}
}
