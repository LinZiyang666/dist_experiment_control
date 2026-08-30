package agent

import (
	"context"
	"encoding/json"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// proxy_lifetime_test.go pins WHO MAY STOP the embedded SS server.
//
// origin: 2026-08-21 weilandserver incident (docs/reviews/proxy-lifecycle-plan.md).
// The SS server was anchored to the per-session runCtx, so a control-plane session
// rebuild killed the data plane and left the runtime holding a corpse: the node
// advertised a READY exit with no listener for 7h40m and only a session-wide
// `proxy off/on` brought it back.
//
// This is deliberately ONE TABLE rather than a file of separate cases. The repo has
// paid for the alternative: test/determinism/test_naming_test.go's header records that
// the tunnel fence was rediscovered in review rounds 2, 5 and 6 because each round
// tested one verb instead of tabulating them, and notes that "had round 2 been written
// as a {verb, killFn} table, rounds 5 and 6 would structurally not have happened".
// A new stopper (or a new event that must NOT stop it) is one row here.

// servingNow reports whether the agent's SS server can actually serve — the honest
// predicate, not the `p.srv != nil` pointer check the incident turned on.
func servingNow(a *Agent) bool {
	p := a.proxy
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.servingLocked()
}

// enableProxyForTest brings the SS server up via the normal full-directive path and
// fails the test if it is not serving afterwards.
func enableProxyForTest(t *testing.T, a *Agent, epoch int64) {
	t.Helper()
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: epoch,
	})
	if !servingNow(a) {
		t.Fatal("precondition failed: SS server is not serving after a full enable directive")
	}
}

// waitNotServing polls for the server to go down (fail-closed fires from a timer).
func waitNotServing(a *Agent, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !servingNow(a) {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return !servingNow(a)
}

func TestProxySSServerLifetimeStopperTable(t *testing.T) {
	for _, tc := range []struct {
		name string
		// stops is the whole point of the row: true = this event is ALLOWED to stop
		// the SS server; false = this event MUST NOT stop it.
		stops bool
		why   string
		// setup runs BEFORE the SS server is built, and returns whatever `act` needs later.
		//
		// origin: proxy-lifecycle internal review, BLOCKER (discipline lane). The ctx rows
		// originally installed a fresh cancellable ctx AFTER enableProxyForTest had already
		// built the server, then cancelled it — so they cancelled a ctx the server had never
		// been anchored to and could not fail even against an implementation that re-anchors.
		// The reviewer proved it by reintroducing the incident (a ctx-watch goroutine in
		// proxyStartLocked) and observing the whole repo stay green. To exercise the anchor,
		// the ctx must be CURRENT AT BUILD TIME, which is what this hook is for.
		setup func(t *testing.T, a *Agent) func()
		act   func(t *testing.T, a *Agent, armed func())
	}{
		// ---- the closed set of legitimate stoppers -------------------------------
		{
			name:  "authoritative proxy OFF",
			stops: true,
			why:   "the operator switched the subscription off; the exit must stop serving",
			act: func(t *testing.T, a *Agent, _ func()) {
				a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Epoch: 99})
			},
		},
		{
			name:  "fail-closed fire after the partition grace",
			stops: true,
			why:   "a revoked subscriber must not keep egressing while we are partitioned from the broker",
			act: func(t *testing.T, a *Agent, _ func()) {
				a.cfg.ProxyFailClosedGrace = 20 * time.Millisecond
				a.armFailClosed()
				if !waitNotServing(a, 2*time.Second) {
					t.Fatal("fail-closed did not stop the server")
				}
			},
		},
		{
			name:  "agent exit",
			stops: true,
			why:   "the SS server no longer hangs from any ctx, so Run's exit must stop it explicitly",
			act: func(t *testing.T, a *Agent, _ func()) {
				stopProxyOnRunExit(a)
			},
		},

		{
			name: "teardown-then-rebuild (full token-bearing directive)",
			// The net observable is "still serving", but this row exists because it is the
			// FOURTH member of the closed stopper set and the only one whose stop is immediately
			// followed by a start. Without it the table pins 3 of 4 stoppers and a regression
			// that turned this arm into a no-op-on-the-old-server would pass.
			// origin: proxy-lifecycle internal review MAJOR (tests lane).
			stops: false,
			why:   "a fresh token means a new server; the OLD instance must be stopped, not abandoned",
			act: func(t *testing.T, a *Agent, _ func()) {
				p := a.proxy
				p.mu.Lock()
				old := p.srv
				p.mu.Unlock()
				if old == nil {
					t.Fatal("precondition: no server to replace")
				}
				a.applyProxyDirective(nil, &proto.ProxyDirective{
					Enabled: true, PublicPort: 14000, Token: "tok2", Cipher: "chacha20-ietf-poly1305",
					Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 9,
				})
				p.mu.Lock()
				replaced := p.srv != old
				p.mu.Unlock()
				if !replaced {
					t.Fatal("a token-bearing directive must build a NEW server (the type is single-use)")
				}
				if old.Serving() {
					t.Fatal("the PREVIOUS server is still serving after teardown-then-rebuild: it was " +
						"abandoned rather than stopped, so its listener and goroutines leak and two " +
						"servers may hold the same local port")
				}
			},
		},

		// ---- events that MUST NOT stop it (the incident's regression) ------------
		{
			name:  "session ctx cancelled (session rebuild)",
			stops: false,
			why: "THE INCIDENT. A control-plane session rebuild cancels runCtx every time nats.go " +
				"reconnects. The data plane must survive it — the tunnel client already does, and " +
				"policy says only ProxyFailClosedGrace may tear SS down for a partition.",
			// Install the cancellable runCtx BEFORE the server is built, so this row exercises
			// the very anchor the incident used. Cancelling a ctx installed afterwards asserts
			// nothing (internal review BLOCKER).
			setup: func(t *testing.T, a *Agent) func() {
				ctx, cancel := context.WithCancel(context.Background())
				a.setRunCtx(ctx)
				return cancel
			},
			act: func(t *testing.T, a *Agent, cancelRun func()) {
				cancelRun()
				// Give any ctx-watch goroutine a generous chance to act on the cancel.
				time.Sleep(150 * time.Millisecond)
			},
		},
		{
			name:  "keyset-only push at a newer epoch",
			stops: false,
			why:   "a routine key rotation swaps keys in place; it is not a teardown",
			act: func(t *testing.T, a *Agent, _ func()) {
				a.applyProxyDirective(nil, &proto.ProxyDirective{
					Enabled: true, Keys: []proto.ProxyKey{{SubID: "s1", Secret: "p1"}}, Epoch: 50,
				})
			},
		},
		{
			name:  "keyset-only push at the SAME epoch",
			stops: false,
			why:   "the broker's steady-state re-push; it is idempotent, not a stopper",
			act: func(t *testing.T, a *Agent, _ func()) {
				a.applyProxyDirective(nil, &proto.ProxyDirective{
					Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
				})
			},
		},
		{
			name:  "parent ctx cancelled without Run exiting",
			stops: false,
			why: "cancelling the ctx that was current at build time must be inert now that Start takes " +
				"none — this is what makes the stopper set closed rather than 'whoever holds a ctx'",
			// Same shape as the row above but cancels via a DIFFERENT route (the stored cancel
			// captured at setup, with runCtx left installed), so a fix that special-cases one
			// call site does not satisfy both rows.
			setup: func(t *testing.T, a *Agent) func() {
				ctx, cancel := context.WithCancel(context.Background())
				a.setRunCtx(ctx)
				return func() {
					cancel()
					// Leave the cancelled ctx installed: a reader that consults loadRunCtx
					// later must still not conclude the server should die.
					a.setRunCtx(ctx)
				}
			},
			act: func(t *testing.T, a *Agent, cancelRun func()) {
				cancelRun()
				time.Sleep(150 * time.Millisecond)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newProxyTestAgent(t)
			var armed func()
			if tc.setup != nil {
				armed = tc.setup(t, a)
			}
			enableProxyForTest(t, a, 1)

			tc.act(t, a, armed)

			serving := servingNow(a)
			switch {
			case tc.stops && serving:
				t.Fatalf("%q must STOP the SS server but it is still serving (%s)", tc.name, tc.why)
			case !tc.stops && !serving:
				t.Fatalf("%q must NOT stop the SS server but it is no longer serving (%s)", tc.name, tc.why)
			}
		})
	}
}

// A corpse — a stopped server the runtime still points at — must be reaped and rebuilt
// by the very next keyset-only push, at the SAME public port. This is the direct
// regression for the 7h40m outage: everything needed to self-heal was already present
// (the persisted footprint was intact and the bootstrap arm knew how to use it); the
// only missing piece was noticing the server was dead.
func TestCorpseIsRebuiltByTheNextKeysetPush(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)

	p := a.proxy
	p.mu.Lock()
	portBefore := p.publicPort
	p.srv.Stop() // stop it but LEAVE p.srv pointing at the corpse — exactly the incident state
	p.mu.Unlock()

	if servingNow(a) {
		t.Fatal("precondition failed: the corpse should not be serving")
	}
	if !runningSrv(a) {
		t.Fatal("precondition failed: p.srv must still be non-nil — that is what made the bug invisible")
	}

	// The broker's steady-state push: keyset-only, SAME (gen, epoch). Pre-fix this hit
	// the exact-equal re-ACK (publishing READY for a dead server) or SetKeys (logging a
	// WARN and returning) — 5416 times.
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
	})

	if !servingNow(a) {
		t.Fatal("a keyset-only push must reap the corpse and rebuild from the persisted footprint; " +
			"without this the node stays a black hole until an operator runs a session-wide proxy off/on")
	}
	p.mu.Lock()
	portAfter := p.publicPort
	p.mu.Unlock()
	if portAfter != portBefore {
		t.Fatalf("self-heal must keep the SAME public port (was %d, now %d): renumbering is what made "+
			"the manual workaround so expensive", portBefore, portAfter)
	}
}

// origin: proxy-lifecycle external review F3
// The persisted local port is an implementation detail between the SS listener and the tunnel,
// not a public allocation. If another socket takes it after the corpse is stopped, rebuilding on
// that exact port forever turns a recoverable local collision into another permanent dark exit;
// the bootstrap path can safely fall back to an OS-chosen local port while keeping PublicPort.
func TestCorpseRebuildSurvivesPersistedLocalPortCollision(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)

	p := a.proxy
	p.mu.Lock()
	localPort := p.localPort
	publicPort := p.publicPort
	p.srv.Stop()
	p.mu.Unlock()
	if localPort == 0 {
		t.Fatal("precondition: enabled proxy has no local listener port")
	}

	blocker, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(localPort)))
	if err != nil {
		t.Fatalf("occupy the stopped server's local port: %v", err)
	}
	defer func() { _ = blocker.Close() }()

	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
	})
	if !servingNow(a) {
		t.Fatalf("corpse rebuild stayed dark because persisted local port %d was occupied; that port is "+
			"not user-visible, so bootstrap should retry on an OS-chosen port while retaining public port %d",
			localPort, publicPort)
	}
	p.mu.Lock()
	gotPublic := p.publicPort
	gotLocal := p.localPort
	p.mu.Unlock()
	if gotPublic != publicPort || gotLocal == localPort {
		t.Fatalf("fallback rebuilt at public/local %d/%d, want public=%d and a fresh local port (old=%d)",
			gotPublic, gotLocal, publicPort, localPort)
	}
}

// The self-heal must not become a resurrection path for an exit the operator switched
// off. After an authoritative OFF the in-memory latch refuses a rebuild at the equal
// pair — and it must hold even when the on-disk footprint survives, because the wipe
// is an unchecked write (`_ = a.stateStore.SetProxy(nil)`).
func TestAuthoritativeOffIsNotUndoneByAKeysetPush(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)

	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Epoch: 2})
	if servingNow(a) {
		t.Fatal("authoritative OFF must stop the server")
	}

	// Simulate the footprint wipe having FAILED (the error is discarded in production),
	// so the only thing standing between a revoked exit and a rebuild is the latch.
	// LocalPort MUST be 0 (OS-chosen). Writing a privileged port here would make the
	// rebuild fail on permissions and the test would pass for the wrong reason — it did,
	// until the mutation run caught it.
	if a.stateStore == nil {
		t.Fatal("precondition: no state store, so there is no footprint for refootprint to act on")
	}
	if err := a.stateStore.SetProxy(&ProxyState{
		PublicPort: 14000, LocalPort: 0, Token: "tok", Epoch: 2,
	}); err != nil {
		t.Fatalf("precondition: seed footprint: %v", err)
	}

	// A tokenless push at the applied pair — precisely what refootprint would accept
	// were the latch missing.
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 2,
	})
	if servingNow(a) {
		t.Fatal("a switched-off exit was resurrected by a tokenless keyset push: the refootprint " +
			"escape hatch must be fenced by the authoritativelyOff latch, not by a disk write whose " +
			"error is discarded")
	}
}

// A corpse must report itself honestly to the broker: NOT bound (so /sub stops vending
// it), but still carrying its true applied (gen, epoch) — because the broker only pushes
// an authoritative OFF to a node whose reported epoch is > 0. Reporting (0,0) here would
// deadlock: no OFF is pushed, and the reap only runs on the directive path.
func TestCorpseReportsUnboundButKeepsItsAppliedPair(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 7)
	a.proxyTunnelUp.Store(true) // pretend the tunnel is up, isolating the SS-side signal

	p := a.proxy
	p.mu.Lock()
	p.srv.Stop()
	p.mu.Unlock()

	if a.proxyBound() {
		t.Fatal("a corpse must report ProxyBound=false — reporting true is literally how the broker " +
			"advertised a dead exit as READY for 7h40m")
	}
	gen, epoch := a.proxyGenEpoch()
	if epoch != 7 {
		t.Fatalf("a corpse must keep reporting its applied epoch (got gen=%d epoch=%d, want epoch=7): "+
			"the broker only pushes an authoritative OFF when the reported epoch is > 0, so zeroing it "+
			"here strands the node with no OFF and no reap", gen, epoch)
	}
}

// The self-heal constructs a FRESH ssproxy.Server every time (the type is single-use), each
// with its own accept-loop goroutine. A repeated corpse->rebuild cycle must therefore not
// accumulate goroutines — otherwise the fix for a 7h40m outage becomes a slow leak on any node
// that flaps. Uses the repo's own NumGoroutine poll-with-tolerance gate, deliberately not goleak.
func TestRepeatedCorpseRebuildDoesNotLeakGoroutines(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)

	// Each cycle exercises BOTH rebuild routes, because they clean up differently:
	//   (a) corpse -> self-heal: the old server was already stopped, so this proves the
	//       rebuild itself does not accumulate;
	//   (b) authoritative OFF -> full re-enable: this goes through proxyTeardownLocked, so it
	//       proves the teardown actually STOPS the old server rather than just dropping the
	//       pointer. Route (a) alone cannot catch a missing Stop() — verified by mutation.
	keys := []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}
	before := runtime.NumGoroutine()
	const cycles = 6
	epoch := int64(1)
	for i := 0; i < cycles; i++ {
		p := a.proxy
		p.mu.Lock()
		p.srv.Stop() // corpse it, leaving p.srv non-nil
		p.mu.Unlock()
		a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: true, Keys: keys, Epoch: epoch})
		if !servingNow(a) {
			t.Fatalf("cycle %d route (a): self-heal did not restore a serving server", i)
		}

		a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Epoch: epoch + 1})
		if servingNow(a) {
			t.Fatalf("cycle %d route (b): authoritative OFF left the server serving", i)
		}
		a.applyProxyDirective(nil, &proto.ProxyDirective{
			Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
			Keys: keys, Epoch: epoch + 2,
		})
		if !servingNow(a) {
			t.Fatalf("cycle %d route (b): full re-enable did not restore a serving server", i)
		}
		epoch += 3
	}
	stopProxyOnRunExit(a)

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before+3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+3 {
		t.Fatalf("goroutine leak across %d corpse/rebuild cycles: before=%d after=%d "+
			"(each rebuild constructs a fresh single-use Server; a stale accept loop per cycle "+
			"turns the self-heal into a leak)", cycles, before, n)
	}
}

// A fail-closed timer that fires DURING agent shutdown must not wipe the footprint the
// next start needs. time.Timer.Stop() does not join an AfterFunc that is already running,
// so cancelFailClosed alone cannot close this window.
func TestFailClosedDuringAgentExitKeepsTheFootprint(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)

	stopProxyOnRunExit(a) // latches agentExiting
	a.failClosedFire()    // the racing timer callback

	ps := a.loadProxyStateSafe()
	if ps == nil || ps.PublicPort == 0 || ps.Token == "" {
		t.Fatal("a fail-closed firing during agent exit wiped the persisted footprint; the next start " +
			"has nothing to bootstrap from and the node comes up dark")
	}
}

// applyProxyKeysetLocked's ErrStopped arm is the LAST line of defence: the head reap plus the
// !servingLocked() switch arm normally intercept a corpse before this runs, so the arm is only
// reachable when a server dies BETWEEN the serving check and the SetKeys call. That makes it
// nearly unreachable end-to-end and therefore easy to ship broken — the internal review found
// that reverting it to the pre-fix "log a WARN and return" left the entire suite green. This
// drives the arm directly.
//
// origin: proxy-lifecycle internal review MAJOR (tests lane).
func TestKeysetArmRebuildsWhenSetKeysReportsStopped(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)

	p := a.proxy
	p.mu.Lock()
	portBefore := p.publicPort
	p.srv.Stop() // corpse it WITHOUT going through applyProxyDirective, so no reap has run
	// Call the keyset arm directly: this is the state a race between the serving check and
	// SetKeys would produce.
	applyProxyKeysetLocked(a, p, nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
	}, toSSKeys([]proto.ProxyKey{{SubID: "s0", Secret: "p0"}}))
	serving := p.servingLocked()
	portAfter := p.publicPort
	p.mu.Unlock()

	if !serving {
		t.Fatal("a SetKeys failure reporting ErrStopped must reap and rebuild in the same call; " +
			"logging a WARN and returning is what let a dead server sit in the runtime for 7h40m")
	}
	if portAfter != portBefore {
		t.Fatalf("rebuild changed the public port (%d -> %d); the self-heal must be invisible "+
			"to subscribers apart from one reset", portBefore, portAfter)
	}
}

// authoritativelyOff must CLEAR on a legitimate re-enable. A latch that sticks would permanently
// disarm the refootprint self-heal on that node — turning a safety fence into the outage.
//
// origin: proxy-lifecycle internal review MAJOR (statemachine lane).
func TestAuthoritativeOffLatchClearsOnReEnable(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)

	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Epoch: 2})
	p := a.proxy
	p.mu.Lock()
	latched := p.authoritativelyOff
	p.mu.Unlock()
	if !latched {
		t.Fatal("authoritative OFF must latch")
	}

	// Authoritative re-enable (token-bearing, newer epoch) — the operator turned it back on.
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 3,
	})
	p.mu.Lock()
	stillLatched := p.authoritativelyOff
	p.mu.Unlock()
	if stillLatched {
		t.Fatal("authoritativelyOff must clear on a successful re-enable; a stuck latch disarms " +
			"the refootprint self-heal on this node forever")
	}

	// And prove the consequence: after the latch clears, a corpse DOES self-heal again.
	p.mu.Lock()
	p.srv.Stop()
	p.mu.Unlock()
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 3,
	})
	if !servingNow(a) {
		t.Fatal("self-heal did not resume after the latch cleared — the fence outlived its cause")
	}
}

// A LOCAL fail-closed teardown must NOT be undone by a tokenless keyset push. Round-6 F7
// requires a token-bearing authoritative directive to come back from fail-closed; the
// refootprint hatch added by this increment must not quietly widen that to "any push at the
// same pair", because the only other thing standing in the way is the footprint wipe — and
// that wipe is an unchecked disk write.
//
// origin: proxy-lifecycle internal review MAJOR (security lane).
func TestFailClosedIsNotUndoneByATokenlessPush(t *testing.T) {
	a := newProxyTestAgent(t)
	a.cfg.ProxyFailClosedGrace = 20 * time.Millisecond
	enableProxyForTest(t, a, 1)

	a.armFailClosed()
	if !waitNotServing(a, 2*time.Second) {
		t.Fatal("precondition: fail-closed did not stop the server")
	}
	p := a.proxy
	p.mu.Lock()
	needs := p.needsReestablish
	p.mu.Unlock()
	if !needs {
		t.Fatal("precondition: fail-closed must set needsReestablish (round-6 F7)")
	}

	// Simulate the footprint wipe having FAILED, so the latch is the only fence left.
	if err := a.stateStore.SetProxy(&ProxyState{
		PublicPort: 14000, LocalPort: 0, Token: "tok", Epoch: 1,
	}); err != nil {
		t.Fatalf("precondition: seed footprint: %v", err)
	}

	// Tokenless push at the exact applied pair — what refootprint would otherwise accept.
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
	})
	if servingNow(a) {
		t.Fatal("a tokenless push rebuilt the exit after a fail-closed teardown: F7 requires a " +
			"token-bearing authoritative directive, and refootprint must not bypass it")
	}

	// The AUTHORITATIVE route must still work — the fence must not be a dead end.
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 1,
	})
	if !servingNow(a) {
		t.Fatal("the token-bearing reestablish directive must rebuild after fail-closed (F7); " +
			"if this fails the fence has become a permanent outage")
	}
}

// After Run's exit teardown has latched, a late directive must not build anything. Removing
// the ctx anchor also removed the implicit "dies with the process ctx" cleanup, so a server
// started here would have no stopper left at all.
//
// origin: proxy-lifecycle internal review MAJOR (statemachine lane).
func TestLateDirectiveAfterAgentExitBuildsNothing(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 1)
	stopProxyOnRunExit(a)
	if servingNow(a) {
		t.Fatal("precondition: exit teardown must stop the server")
	}

	// A full, token-bearing, strictly-newer directive — the most persuasive kind.
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "tok", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 99,
	})
	if servingNow(a) {
		t.Fatal("a directive arriving after agent exit started an SS server that nothing can stop: " +
			"the exit teardown has already run and every stopper requires a live agent")
	}
}

// (The acceptExited half of Serving() is exercised in the ssproxy package itself, where the
// helper that constructs that state can stay unexported — external review F6.)

// A corpse produced by something OTHER than a directive-bearing event must still recover, and
// on a SINGLE broker the heartbeat is the only edge that can start it. This pins the whole
// agent-side chain: reap on the heartbeat -> report unready AND (0,0) -> the keyset push the
// broker then sends rebuilds us on the SAME public port.
//
// Why (0,0) matters, and why publishing unready alone is not enough: repairProxy returns
// immediately when `on && ready && agentGen == brokerGen && agentEpoch == epoch`
// (CONVERGENCE-FIRST), and separately suppresses a nudge when `!ready && agentGen == brokerGen
// && agentEpoch == epoch` (Fix D). A corpse that only flipped ready to false would still match
// the second and never be repaired. Nil'ing p.srv makes proxyGenEpoch report (0,0), so NEITHER
// early return matches.
//
// origin: proxy-lifecycle external review F4.
func TestHeartbeatReapGivesSingleBrokerCorpseAnExitEdge(t *testing.T) {
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 7)
	a.proxyTunnelUp.Store(true)

	p := a.proxy
	p.mu.Lock()
	portBefore := p.publicPort
	p.srv.Stop() // corpse it WITHOUT any directive, register reply or session rebuild
	p.mu.Unlock()

	// Before the heartbeat edge runs, the report is exactly what makes the broker return early.
	if gen, epoch := a.proxyGenEpoch(); gen == 0 && epoch == 0 {
		t.Fatal("precondition: an un-reaped corpse still reports its applied pair (that is the trap)")
	}

	reapProxyCorpseOnHeartbeat(a, nil)

	if runningSrv(a) {
		t.Fatal("the heartbeat edge must reap the corpse; without it nothing else will on a single broker")
	}
	if a.proxyBound() {
		t.Fatal("a reaped corpse must report ProxyBound=false")
	}
	gen, epoch := a.proxyGenEpoch()
	if gen != 0 || epoch != 0 {
		t.Fatalf("after the reap the report must be (0,0), got (%d,%d): a corpse that keeps its "+
			"applied pair matches repairProxy's CONVERGENCE-FIRST return and is never repaired", gen, epoch)
	}

	// That report is what unblocks the broker's keyset push; applying it must restore service
	// on the SAME public port (no renumbering, no operator action).
	a.applyProxyDirective(nil, &proto.ProxyDirective{
		Enabled: true, Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Epoch: 7,
	})
	if !servingNow(a) {
		t.Fatal("the repair push must rebuild the exit after a heartbeat-driven reap")
	}
	p.mu.Lock()
	portAfter := p.publicPort
	p.mu.Unlock()
	if portAfter != portBefore {
		t.Fatalf("public port changed during heartbeat-driven self-heal (%d -> %d)", portBefore, portAfter)
	}
}

// origin: proxy-lifecycle independent external rereview R1
// Exercise the real ticker and NATS publish, rather than calling the reap helper directly or
// merely finding its name in heartbeatLoop's AST. F4 depends on ORDER: the corpse must be reaped
// before the heartbeat snapshots generation/epoch and ProxyBound. Moving the call below Publish
// leaves the AST wiring gate green but makes this first report advertise the stale applied pair.
func TestHeartbeatPublishesReapedCorpseStateOnItsFirstTick(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	a := newProxyTestAgent(t)
	a.cfg.HeartbeatInterval = 10 * time.Millisecond
	enableProxyForTest(t, a, 7)
	a.proxyTunnelUp.Store(true)
	p := a.proxy
	p.mu.Lock()
	p.srv.Stop() // corpse without a directive or session rebuild
	p.mu.Unlock()

	heartbeats := make(chan *nats.Msg, 2)
	sub, err := nc.ChanSubscribe(proto.SubjNodeHeartbeat(a.cfg.SID, nidOf(a)), heartbeats)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.heartbeatLoop(ctx, nc) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("heartbeatLoop did not stop after cancellation")
		}
	}()

	var msg *nats.Msg
	select {
	case msg = <-heartbeats:
	case <-time.After(time.Second):
		t.Fatal("no heartbeat published; test did not reach the production wiring")
	}
	var hb proto.HeartbeatPayload
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		t.Fatalf("decode heartbeat: %v", err)
	}
	if hb.ProxyGeneration != 0 || hb.ProxyEpoch != 0 || hb.ProxyBound {
		t.Fatalf("first heartbeat captured corpse state before reaping: gen=%d epoch=%d bound=%v; "+
			"want (0,0,false) so single-broker repair bypasses both exact-pair early returns",
			hb.ProxyGeneration, hb.ProxyEpoch, hb.ProxyBound)
	}
	if runningSrv(a) {
		t.Fatal("heartbeat reported a reaped state but left the corpse pointer installed")
	}
}
