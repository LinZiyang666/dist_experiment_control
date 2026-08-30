// proxy_dial_backoff_test.go — the #78 first-dial backoff in proxyStartLocked.
// The broker's heartbeat convergence loop re-pushes the proxy directive every
// ~5s to a not-ready node; before #78 every push re-dialed unconditionally, so
// a node behind a half-open :7000 dialed (and made the broker WARN) forever at
// 5s. These tests drive applyProxyDirective exactly like that loop does
// (keyset-only pushes bootstrapping from the persisted footprint) against an
// always-failing adapter, on an injected clock.
// origin: docs/deploy-tier-gotchas.md #78
package agent

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// failingProxyAdapter counts AddProxy attempts and always fails (the
// half-open-:7000 world: the dial never succeeds). failUntil lets a case flip
// it healthy mid-test.
type failingProxyAdapter struct {
	mu       sync.Mutex
	attempts int
	healthy  bool
}

func (f *failingProxyAdapter) AddProxy(PortToken) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.healthy {
		return nil
	}
	return errDialRefused
}
func (f *failingProxyAdapter) RemoveProxy(string, int) error { return nil }
func (f *failingProxyAdapter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}
func (f *failingProxyAdapter) setHealthy(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthy = v
}

var errDialRefused = &net_OpError{}

// net_OpError is a minimal error stand-in (the adapter's real failure is a
// *net.OpError; the backoff logic never inspects the type).
type net_OpError struct{}

func (*net_OpError) Error() string { return "dial tcp 127.0.0.1:7000: connect: connection refused" }

// backoffHarness wires an agent with an injected clock + failing adapter.
type backoffHarness struct {
	a       *Agent
	adapter *failingProxyAdapter
	mu      sync.Mutex
	now     time.Time
}

func newBackoffHarness(t *testing.T) *backoffHarness {
	t.Helper()
	h := &backoffHarness{adapter: &failingProxyAdapter{}, now: time.Unix(1_700_000_000, 0)}
	a, err := New(Config{
		NATSURL:       "nats://127.0.0.1:4222",
		SID:           "lab",
		NID:           "lab-1",
		Home:          t.TempDir(),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: h.adapter,
		Now: func() time.Time {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.now
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	a.runCtx = context.Background()
	h.a = a
	return h
}

func (h *backoffHarness) advance(d time.Duration) {
	h.mu.Lock()
	h.now = h.now.Add(d)
	h.mu.Unlock()
}

// pushFull delivers a token-bearing directive (the broker's first full push).
func (h *backoffHarness) pushFull(gen, epoch int64, port int, token string) {
	h.a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: port, Token: token, Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: gen, Epoch: epoch,
	})
}

// pushKeys delivers the keyset-only repair push (what repairProxy actually
// re-sends every heartbeat — port=0, token="" — bootstrapping from the
// persisted footprint).
func (h *backoffHarness) pushKeys(gen, epoch int64) {
	h.a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: gen, Epoch: epoch,
	})
}

func TestProxyDialBackoffSuppressesSameIdentityRetries(t *testing.T) {
	h := newBackoffHarness(t)

	// First full push: dials once, fails, footprint persisted (round-2 F3).
	h.pushFull(1, 1, 14000, "tok")
	if got := h.adapter.count(); got != 1 {
		t.Fatalf("first push must dial exactly once, got %d", got)
	}
	if h.a.proxy.srv != nil {
		t.Fatal("failed dial must leave no running server")
	}

	// The 5s repair loop: keyset pushes at each heartbeat. Backoff schedule
	// after 1 failure = Base(5s), so the +5s push IS due (dials, fails →
	// schedule 10s), then the next two 5s pushes (+10s, +15s from start of
	// that run) sit inside the 10s→20s windows.
	h.advance(5 * time.Second)
	h.pushKeys(1, 1)
	if got := h.adapter.count(); got != 2 {
		t.Fatalf("push at +5s (first retry, due) must dial, got %d attempts", got)
	}
	h.advance(5 * time.Second)
	h.pushKeys(1, 1)
	if got := h.adapter.count(); got != 2 {
		t.Fatalf("push at +5s into a 10s window must be SUPPRESSED, got %d attempts", got)
	}
	h.advance(5 * time.Second)
	h.pushKeys(1, 1)
	if got := h.adapter.count(); got != 3 {
		t.Fatalf("push at +10s (window elapsed) must dial, got %d attempts", got)
	}

	// While suppressed, the node must stay honest: no server, unready.
	if h.a.proxy.srv != nil || h.a.proxyTunnelUp.Load() {
		t.Fatal("suppressed retry must not fake a running/ready proxy")
	}
}

func TestProxyDialBackoffCapsAndHoldsAtCap(t *testing.T) {
	h := newBackoffHarness(t)
	h.pushFull(1, 1, 14000, "tok")
	// Drive failures until the schedule is capped (5s→10s→…→5min).
	for i := 0; i < 10; i++ {
		h.advance(6 * time.Minute) // always past any window ⇒ every push dials
		h.pushKeys(1, 1)
	}
	base := h.adapter.count()
	// Now inside a capped 5min window: a 4-minute-later push is suppressed…
	h.advance(4 * time.Minute)
	h.pushKeys(1, 1)
	if got := h.adapter.count(); got != base {
		t.Fatalf("push inside the 5min cap window must be suppressed, got %d (base %d)", got, base)
	}
	// …and a 5min+ one dials.
	h.advance(90 * time.Second)
	h.pushKeys(1, 1)
	if got := h.adapter.count(); got != base+1 {
		t.Fatalf("push past the cap window must dial, got %d (base %d)", got, base)
	}
}

func TestProxyDialBackoffBypassOnNewIdentity(t *testing.T) {
	cases := []struct {
		name string
		push func(h *backoffHarness)
	}{
		// proxy off/on mints a new epoch — the operator's explicit action
		// must never wait out a backoff window.
		{"epoch bump", func(h *backoffHarness) { h.pushKeys(1, 2) }},
		// a reaper rotation mints a fresh port+token at the same keyset pair.
		{"port+token rotation", func(h *backoffHarness) { h.pushFull(1, 1, 14002, "tok2") }},
		// a generation bump (DB restore convergence).
		{"generation bump", func(h *backoffHarness) { h.pushFull(2, 1, 14000, "tok") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBackoffHarness(t)
			h.pushFull(1, 1, 14000, "tok")
			// Fail a few times to deepen the window well past 5s.
			for i := 0; i < 4; i++ {
				h.advance(6 * time.Minute)
				h.pushKeys(1, 1)
			}
			base := h.adapter.count()
			h.advance(1 * time.Second) // deep inside the current window
			tc.push(h)
			if got := h.adapter.count(); got != base+1 {
				t.Fatalf("%s must bypass the backoff window immediately, got %d attempts (base %d)", tc.name, got, base)
			}
		})
	}
}

func TestProxyDialBackoffNewIdentityStartsFreshSchedule(t *testing.T) {
	// A new identity is a NEW failure story: the deep window earned by the
	// old (gen,epoch,…) must not carry over. Without the reset, the first
	// failure of the new identity would inherit fails=N and schedule its
	// first retry at Base·2^N instead of Base.
	h := newBackoffHarness(t)
	h.pushFull(1, 1, 14000, "tok")
	for i := 0; i < 5; i++ { // deepen to a multi-minute window
		h.advance(6 * time.Minute)
		h.pushKeys(1, 1)
	}
	// Operator does proxy off/on → new epoch. Bypass dials (fails once).
	h.advance(1 * time.Second)
	h.pushKeys(1, 2)
	base := h.adapter.count()
	// The NEW identity's schedule after ONE failure must be Base (5s): a
	// push at +5s dials. With the inherited deep schedule it would sit
	// suppressed for minutes.
	h.advance(5 * time.Second)
	h.pushKeys(1, 2)
	if got := h.adapter.count(); got != base+1 {
		t.Fatalf("first retry of a fresh identity must be due after Base, got %d attempts (base %d)", got, base)
	}
}

func TestProxyDialBackoffResetOnSuccessAndReconnect(t *testing.T) {
	// The observable contract is the UNION of the reset paths (Recover's
	// self-reset on success, the OFF-teardown reset, the new-identity
	// reset): any new proxy lifecycle after a success starts its failure
	// schedule at Base.
	t.Run("post-success lifecycle starts the schedule at Base", func(t *testing.T) {
		h := newBackoffHarness(t)
		h.pushFull(1, 1, 14000, "tok")
		for i := 0; i < 4; i++ { // deepen the schedule
			h.advance(6 * time.Minute)
			h.pushKeys(1, 1)
		}
		h.adapter.setHealthy(true)
		h.advance(6 * time.Minute)
		h.pushKeys(1, 1) // dials, succeeds
		if h.a.proxy.srv == nil {
			t.Fatal("healthy dial must establish the proxy")
		}
		// Break it again: teardown via authoritative OFF, re-enable, fail once —
		// the next retry must be due after Base (5s), not the old deep window.
		h.adapter.setHealthy(false)
		h.a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Generation: 1, Epoch: 5})
		h.pushFull(1, 6, 14000, "tok3")
		base := h.adapter.count()
		h.advance(5 * time.Second)
		h.pushKeys(1, 6)
		if got := h.adapter.count(); got != base+1 {
			t.Fatalf("after a success+OFF the schedule must restart at Base: push at +5s should dial, got %d (base %d)", got, base)
		}
	})

	t.Run("reconnect reset clears suppression for the same identity", func(t *testing.T) {
		h := newBackoffHarness(t)
		h.pushFull(1, 1, 14000, "tok")
		for i := 0; i < 4; i++ {
			h.advance(6 * time.Minute)
			h.pushKeys(1, 1)
		}
		base := h.adapter.count()
		h.advance(1 * time.Second) // deep inside the window
		// A NATS reconnect re-registers and the register reply re-delivers
		// the SAME directive — without the reset it would still be
		// suppressed and a healed network would wait out the whole window.
		resetProxyDialBackoff(h.a)
		h.pushKeys(1, 1)
		if got := h.adapter.count(); got != base+1 {
			t.Fatalf("post-reconnect push of the same identity must dial immediately, got %d (base %d)", got, base)
		}
	})
}
