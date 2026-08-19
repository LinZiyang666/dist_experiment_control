// proxy_lease_test.go — the agent half of the cloned-credential lease's proxy
// eligibility rule: an instance running under a broker-assigned lease name is
// ephemeral by premise and must build no SS server, dial no tunnel, and — on
// the reference deployment, where ~/.tether is a SHARED NFS mount at one inode
// — write nothing into the basename holder's state.json.
// origin: docs/reviews/cloned-credential-instances-plan.md §2 Q1, §0.6
package agent

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// A leased instance that is handed a full token-bearing directive today builds
// the server, dials the tunnel and OVERWRITES the shared proxy footprint. The
// broker-side fold (nodes.proxy_capable) is not a belt for this: in SINGLE mode
// proxyDirectiveForRegister mints the directive from nodeParticipatesInProxy,
// which does not consult LeasedNID.
//
// The write is the serious half. The plan's §0.6 spike established that both
// instances see ONE state.json at ONE inode, and concluded the replay gate must
// be memory-only precisely so the holder's file is never touched. Handing the
// clone a directive re-opens that door from the other side: proxyStartLocked
// persists the footprint unconditionally (proxy.go:403-409), so the clone's
// port+token replace the holder's. On the holder's next restart it replays a
// token the broker cannot match, AllocateProxy re-mints, and `created` resets —
// the exact unexplained 14008 symptom the plan documents in §0.2 / §0.6-2.
//
// MUTATION: add a leased arm to applyProxyDirective (the #78 refuse-arm shape)
// and this passes; remove it and it goes red again.
func TestLeasedInstanceRefusesProxyDirectivesAndNeverTouchesTheSharedFootprint(t *testing.T) {
	home := t.TempDir() // the shared NFS ~/.tether: one directory, two processes

	holderFootprint := &ProxyState{PublicPort: 14008, LocalPort: 1080, Token: "holder-token", Epoch: 7}

	// The basename holder's live footprint, already on disk.
	holder, err := New(Config{
		NATSURL: "nats://127.0.0.1:4222", SID: "lab", NID: "lab-1", Home: home,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: &countingProxyAdapter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.stateStore.SetProxy(holderFootprint); err != nil {
		t.Fatal(err)
	}

	// The clone: same image, same agent.yaml basename, same shared home — but
	// the broker contested its register and assigned it a lease name.
	adapter := &countingProxyAdapter{}
	clone, err := New(Config{
		NATSURL: "nats://127.0.0.1:4222", SID: "lab", NID: "lab-1", Home: home,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExposeAdapter: adapter,
	})
	if err != nil {
		t.Fatal(err)
	}
	adoptRoutingNID(clone, "lab-1-02")
	if nidOf(clone) != "lab-1-02" {
		t.Fatalf("adoption did not take: %q", nidOf(clone))
	}
	clone.runCtx = context.Background()

	// Exactly what a single-mode broker replies to this register today.
	clone.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14001, Token: "clone-token", Cipher: "chacha20-ietf-poly1305",
		Keys: []proto.ProxyKey{{SubID: "s0", Secret: "p0"}}, Generation: 1, Epoch: 1,
	})
	t.Cleanup(func() { clone.proxyTeardownLocked(clone.proxy, nil, false) })

	if clone.proxy != nil && clone.proxy.srv != nil {
		t.Error("a leased instance built an SS server — it is ephemeral by premise and holds a public " +
			"egress port whose disappearance breaks every /sub subscriber, while being filtered OUT " +
			"of the /sub render (proxy_capable=1) so nobody ever uses it")
	}
	if got := adapter.count(); got != 0 {
		t.Errorf("a leased instance dialed the tunnel %d times", got)
	}

	ps, err := holder.stateStore.GetProxy()
	if err != nil {
		t.Fatal(err)
	}
	if ps == nil || ps.Token != holderFootprint.Token || ps.PublicPort != holderFootprint.PublicPort {
		t.Fatalf("the clone OVERWROTE the basename holder's shared state.json: want %+v, got %+v — "+
			"on the holder's next restart it replays a token the broker cannot match, "+
			"AllocateProxy re-mints, and its public port silently changes", holderFootprint, ps)
	}
}
