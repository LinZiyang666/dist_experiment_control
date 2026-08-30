package agent

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"encoding/json"
	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/proto"
)

func newProxyTestAgent(t *testing.T) *Agent {
	t.Helper()
	a, err := New(Config{
		NATSURL:       "nats://127.0.0.1:4222",
		SID:           "lab",
		NID:           "lab-1",
		Home:          t.TempDir(),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: noopProxyAdapter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	a.runCtx = context.Background()
	return a
}

type noopProxyAdapter struct{}

func (noopProxyAdapter) AddProxy(PortToken) error      { return nil }
func (noopProxyAdapter) RemoveProxy(string, int) error { return nil }

// applyProxyDirective must be race-free under concurrent enable / keyset-update
// / disable, and converge to a clean stopped state. The -race detector is the
// real assertion; we also check the final teardown nils the server.
func TestApplyProxyDirectiveConcurrent(t *testing.T) {
	a := newProxyTestAgent(t)

	// Initial full directive starts the SS server (nil ExposeAdapter → no tunnel).
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys:  []proto.ProxyKey{{SubID: "s0", Secret: "p0"}},
		Epoch: 1,
	})
	if a.proxy == nil || a.proxy.srv == nil {
		t.Fatal("server should be running after enable")
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 1; i <= 30; i++ {
				// Keyset-only deltas with monotonic-ish epochs (out-of-order across goroutines).
				a.applyProxyDirective(nil, &proto.ProxyDirective{
					Enabled: true,
					Keys:    []proto.ProxyKey{{SubID: "s0", Secret: "p0"}, {SubID: "s1", Secret: "p1"}},
					Epoch:   int64(g*100 + i),
				})
			}
		}(g)
	}
	wg.Wait()

	// A keyset push that arrives while running but with no server (after a
	// teardown) must not panic. First disable (newer pair), then a keyset-only
	// delta that's newer still.
	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Generation: 1 << 30})
	if a.proxy.srv != nil {
		t.Fatal("server should be stopped after disable")
	}
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: 1 << 31, Epoch: 9999,
	})
	// No persisted footprint + keyset-only → stays stopped (awaits full directive).
	if a.proxy.srv != nil {
		t.Fatal("keyset-only push with no footprint must not start a server")
	}
}

func runningSrv(a *Agent) bool {
	if a.proxy == nil {
		return false
	}
	a.proxy.mu.Lock()
	defer a.proxy.mu.Unlock()
	return a.proxy.srv != nil
}

// B1: a sustained NATS partition (no reconnect within the grace) must tear the
// SS server down fail-closed; a reconnect within the window must cancel it.
func TestFailClosedTearsDownAfterGrace(t *testing.T) {
	a := newProxyTestAgent(t)
	a.cfg.ProxyFailClosedGrace = 60 * time.Millisecond
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
	})
	if !runningSrv(a) {
		t.Fatal("precondition: server should be running")
	}
	a.armFailClosed() // simulate DisconnectErr

	deadline := time.Now().Add(2 * time.Second)
	for runningSrv(a) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runningSrv(a) {
		t.Fatal("fail-closed did not tear the SS server down after the grace window")
	}
}

func TestFailClosedCancelledByReconnect(t *testing.T) {
	a := newProxyTestAgent(t)
	a.cfg.ProxyFailClosedGrace = 200 * time.Millisecond
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
	})
	a.armFailClosed()
	a.cancelFailClosed() // reconnect cancels the countdown
	time.Sleep(300 * time.Millisecond)
	if !runningSrv(a) {
		t.Fatal("a reconnect within the window must keep the SS server running")
	}
	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Epoch: 1 << 30}) // cleanup
}

// Round-2 F2: out-of-order LIVE PUSHES are de-duplicated by arrival sequence
// (handleProxyKeysForwarded), so a stale older push can't resurrect a revoked
// key — WITHOUT an epoch lower-guard (which would break DB-restore). A higher
// seq carrying a lower epoch (the restore case) still applies.
func TestProxyPushSequenceOrdersOutOfOrderGoroutines(t *testing.T) {
	a := newProxyTestAgent(t)
	enc := func(d proto.ProxyDirective) *nats.Msg {
		b, _ := json.Marshal(d)
		return &nats.Msg{Data: b}
	}
	// seq 1: enable epoch 5 with key s0.
	a.handleProxyKeysForwarded(nil, enc(proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 5,
	}), 1)
	// seq 3: revoke everyone (epoch 6, empty keys).
	a.handleProxyKeysForwarded(nil, enc(proto.ProxyDirective{Enabled: true, Keys: nil, Epoch: 6}), 3)
	// seq 2 arrives LATE (a reordered goroutine) trying to bring s0 back — must
	// be dropped because seq 2 < the applied seq 3.
	a.handleProxyKeysForwarded(nil, enc(proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 5,
	}), 2)

	a.proxy.mu.Lock()
	gotEpoch := a.proxy.epoch
	a.proxy.mu.Unlock()
	if gotEpoch != 6 {
		t.Fatalf("a reordered older push was applied: epoch=%d want 6", gotEpoch)
	}
	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Epoch: 1 << 30})
}

// Round-2 F4 (production fix): once Run sets proxyDraining, dispatchForwarded
// must NOT register a new proxy handler — so proxyHandlerWG.Wait is a sound
// shutdown barrier (no Add can race the Wait). A handler that started before
// draining is still waited for.
func TestProxyDrainBarrierBlocksNewHandlers(t *testing.T) {
	a := newProxyTestAgent(t)
	enc := func() *nats.Msg {
		return &nats.Msg{
			Subject: proto.SubjCmdForwarded("lab", "lab-1", "proxy-keys"),
			Data:    []byte(`{"enabled":false,"epoch":1}`),
		}
	}
	// Before draining: dispatch registers + runs a handler.
	a.dispatchForwarded(nil, enc())
	a.proxyHandlerWG.Wait() // returns once that handler finished

	// Set the drain barrier (what Run's shutdown does).
	a.proxyDrainMu.Lock()
	a.proxyDraining = true
	before := a.proxyDispatchN
	a.proxyDrainMu.Unlock()

	// After draining: dispatch must be a no-op (no new Add / sequence bump).
	a.dispatchForwarded(nil, enc())
	a.proxyDrainMu.Lock()
	after := a.proxyDispatchN
	a.proxyDrainMu.Unlock()
	if after != before {
		t.Fatal("dispatch registered a proxy handler after the drain barrier was set")
	}
	// Wait is immediate (no pending handlers) — the barrier held.
	done := make(chan struct{})
	go func() { a.proxyHandlerWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("proxyHandlerWG.Wait did not return after drain")
	}
}
