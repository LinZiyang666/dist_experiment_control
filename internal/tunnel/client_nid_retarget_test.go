package tunnel

import (
	"context"
	"sync"
	"testing"
	"time"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §0.4
//
// SetNID retargets only FUTURE REGISTER lines. Sessions already installed under
// the OLD name keep bridging the incumbent's public port, because the tunnel
// Client is anchored to the PROCESS ctx (cmd/tether/agent.go: adapter.Start(ctx)
// with the signal ctx), not to the NATS session that adopted the lease. So the
// window in which a demoted instance still serves a name it no longer owns is
// bounded only by the next transport drop.
func TestSetNIDRetiresSessionsRegisteredUnderTheOldName(t *testing.T) {
	h := newReconnectHarness(t, silentLog())
	h.waitRoundTrip(t, 3*time.Second) // installed as "lab-1"

	h.cli.SetNID("lab-1-02") // the agent adopted a broker-assigned lease name

	// Nothing retires the live session: the public port keeps being served by a
	// client that is no longer "lab-1".
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !h.cli.SessionUp(h.publicPort) {
			return // desired behaviour: adoption retired the stale session
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("after SetNID the client still owns public port %d under the previous name; "+
		"the incumbent's exit is only reclaimed opportunistically, on the next transport drop",
		h.publicPort)
}

// origin: docs/reviews/cloned-credential-instances-plan.md §0.4
//
// The other half of the same seam, and the part that DOES work: once the
// transport drops, the redial presents the new name, tunnelTokenLookup answers
// token_unknown_or_revoked (the allocation row still carries the basename), and
// denyIsTransient classifies that terminal — so the supervisor exits instead of
// hammering. This is the property the SetNID seam actually buys.
func TestRedialAfterSetNIDIsTerminallyDeniedUnderTheNewName(t *testing.T) {
	h := newReconnectHarness(t, silentLog())
	h.waitRoundTrip(t, 3*time.Second)

	h.cli.SetNID("lab-1-02")
	if !h.srv.DropTransport(h.publicPort) {
		t.Fatal("no live session to drop")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !h.cli.SessionUp(h.publicPort) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a redial under the lease name must be terminally denied and drop the slot")
}

// origin: docs/reviews/cloned-credential-instances-external-review.md F6
//
// RETIRING A NAME MUST EMIT THE DOWN EDGE.
//
// Everything that mirrors a port's health — the agent's proxyTunnelUp, the
// broker-visible ProxyBound — is edge-driven. SetNID used to cancel and close
// silently, so those mirrors stayed latched at "up" for a bridge being torn
// down: the operator saw a working exit on a tunnel that no longer existed, and
// nothing ever corrected it because the correcting event was the one not sent.
//
// MUTATION: delete the notifyState(sess.publicPort, false) call in SetNID's
// retire loop and this goes red.
func TestRetiringSessionsOnRenameEmitsTheDownEdge(t *testing.T) {
	var mu sync.Mutex
	var downs []int
	c := &Client{
		sid:      "lab",
		ctx:      context.Background(),
		sessions: map[int]*clientSession{},
		stateHook: func(port int, up bool) {
			mu.Lock()
			defer mu.Unlock()
			if !up {
				downs = append(downs, port)
			}
		},
	}
	first := "gpu1"
	c.nid.Store(&first)
	for _, port := range []int{14001, 14002} {
		ctx, cancel := context.WithCancel(context.Background())
		c.sessions[port] = &clientSession{publicPort: port, cancel: cancel}
		_ = ctx
	}

	c.SetNID("gpu1-02")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(downs)
		mu.Unlock()
		if n == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	got := append([]int(nil), downs...)
	mu.Unlock()
	t.Fatalf("renaming retired the sessions without a down edge (got %v, want both ports): every "+
		"mirror of these ports' health stays latched at UP for a bridge that is gone, so the "+
		"operator is shown a working exit on a dead tunnel", got)
}
