// proxy_optout_test.go — the agent half of #78's proxy.participate opt-out:
// the local directive gate (the N-1 belt against pre-#78 brokers that keep
// pushing) and the boot-time footprint clear.
// origin: docs/deploy-tier-gotchas.md #78
package agent

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// countingProxyAdapter records AddProxy calls (opt-out must produce ZERO).
type countingProxyAdapter struct {
	mu    sync.Mutex
	calls int
}

func (c *countingProxyAdapter) AddProxy(PortToken) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return nil
}
func (c *countingProxyAdapter) RemoveProxy(string, int) error { return nil }
func (c *countingProxyAdapter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newOptOutAgent(t *testing.T, adapter ExposeAdapter) *Agent {
	t.Helper()
	a, err := New(Config{
		NATSURL:       "nats://127.0.0.1:4222",
		SID:           "lab",
		NID:           "lab-1",
		Home:          t.TempDir(),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: adapter,
		ProxyOptOut:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	a.runCtx = context.Background()
	return a
}

func TestProxyOptOutIgnoresDirectives(t *testing.T) {
	adapter := &countingProxyAdapter{}
	a := newOptOutAgent(t, adapter)

	// A full token-bearing directive — the strongest possible instruction —
	// must build nothing and dial nothing.
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: 1, Epoch: 1,
	})
	if a.proxy.srv != nil {
		t.Fatal("opted-out agent built an SS server")
	}
	if got := adapter.count(); got != 0 {
		t.Fatalf("opted-out agent dialed the tunnel %d times (the #78 flood)", got)
	}
	// Repeated pushes (an old broker's 5s repair loop) stay no-ops.
	for i := 0; i < 5; i++ {
		a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
			Enabled: true, Cipher: "chacha20-ietf-poly1305",
			Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: 1, Epoch: int64(2 + i),
		})
	}
	if got := adapter.count(); got != 0 {
		t.Fatalf("keyset re-pushes made %d dials on an opted-out agent", got)
	}
	// A disable push is equally inert (nothing is running).
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{Enabled: false, Epoch: 99})
	if a.proxy.srv != nil {
		t.Fatal("disable on an opted-out agent should be a no-op")
	}
}

func TestProxyOptOutClearsPersistedFootprint(t *testing.T) {
	home := t.TempDir()
	// A participating agent persists a footprint…
	a1, err := New(Config{
		NATSURL: "nats://127.0.0.1:4222", SID: "lab", NID: "lab-1", Home: home,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: &countingProxyAdapter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a1.stateStore.SetProxy(&ProxyState{PublicPort: 14000, LocalPort: 1080, Token: "tok", Epoch: 3}); err != nil {
		t.Fatal(err)
	}
	// …then the operator flips participate: false and restarts: construction
	// must wipe it, or the keyset-bootstrap arm would re-dial from the
	// footprint against an old broker.
	a2, err := New(Config{
		NATSURL: "nats://127.0.0.1:4222", SID: "lab", NID: "lab-1", Home: home,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: &countingProxyAdapter{},
		ProxyOptOut:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ps, err := a2.stateStore.GetProxy()
	if err != nil {
		t.Fatal(err)
	}
	if ps != nil {
		t.Fatalf("opted-out boot must clear the persisted proxy footprint, still have %+v", ps)
	}
}

func TestRegisterReqCarriesProxyOptOut(t *testing.T) {
	// SOURCE-LEVEL wiring pin (the TestAgentDaemonArmsPanicSink precedent):
	// register() must copy Config.ProxyOptOut into the wire req. Deleting
	// that one line is invisible to every behavioral test here (running the
	// real register needs NATS + an identity), and it would silently turn
	// the opt-out into an agent-local-only belt — the broker would keep
	// allocating ports for a node that never dials.
	src, err := os.ReadFile("agent.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "ProxyOptOut: a.cfg.ProxyOptOut") {
		t.Fatal("register() no longer wires Config.ProxyOptOut into NodeRegisterReq — " +
			"the broker-side fold (nodes.proxy_capable) would never see the opt-out")
	}
}
