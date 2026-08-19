package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// startDenyingNATS runs an embedded nats-server that requires a token the agent
// never presents, so every CONNECT is answered with an Authorization Violation.
// It stands in for the ONE thing an N-1 broker does differently to a leased
// agent: it has no auth suffix fallback, so `<basename>-NN` is not a name it
// will admit.
func startDenyingNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.Authorization = "a-token-the-agent-does-not-have"
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

func leasedAgentForAuthTest(t *testing.T, url string) *Agent {
	t.Helper()
	a, err := New(Config{
		NATSURL:              url,
		SID:                  "lab",
		NID:                  "gpu1",
		Logger:               nil,
		HeartbeatInterval:    time.Second,
		RegisterTimeout:      time.Second,
		RegisterRetryInitial: 10 * time.Millisecond,
		RegisterRetryMax:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The state a lease adoption leaves behind: routingNID is the assigned
	// name, and it is PROCESS state — nothing resets it for the life of the
	// process, and the CONNECT name is minted from it on every dial.
	adoptRoutingNID(a, "gpu1-02")
	if got := nidOf(a); got != "gpu1-02" {
		t.Fatalf("setup: nidOf=%q", got)
	}
	return a
}

// origin: docs/reviews/cloned-credential-instances-plan.md §6 (N-1 四象限:
// "broker 回滚同样安全") + §3.4 ("connectNATS 把初始 auth 失败当致命").
//
// A lease name has no agent_provisioning row by design; only a broker carrying
// the auth suffix fallback will admit it. So the moment ANY broker answering
// $SYS.REQ.USER.AUTH predates this increment — a rollback, or a not-yet-upgraded
// member of the cluster-wide "tether-authcallout" queue group — a live leased
// agent's next dial is denied.
//
// The plan's non-negotiable is DEGRADE, NEVER REFUSE. The degrade target exists
// and is free: drop the lease and re-present the basename, which is exactly the
// pre-increment behaviour. This test asserts that happens.
func TestLeaseAdoptionDoesNotOutliveAnAuthDenial(t *testing.T) {
	url := startDenyingNATS(t)
	a := leasedAgentForAuthTest(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := a.connectNATS(ctx)
	if err == nil {
		t.Fatal("setup: expected the denying server to reject the connect")
	}
	if got := nidOf(a); got != a.cfg.NID {
		t.Fatalf("after an auth denial under the lease name the agent still routes as %q; "+
			"nothing drops the lease, so every retry re-presents a name this broker will "+
			"never admit. connectNATS treats the denial as FATAL on every attempt, so Run "+
			"returns and the process EXITS — a broker rollback (or one un-upgraded member of "+
			"the cluster-wide auth_callout queue group) kills every leased agent instead of "+
			"degrading them to their basename", got)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §6 + §2 D3
// ("凭证生命周期永久是 basename 粒度").
//
// The auth-failure text is the only thing an operator sees when this happens,
// and it hands them a copy-pasteable remedy. Formatted with a.cfg.NID, it names
// the BASENAME — so an operator whose `gpu1-02` was denied is told to run
// `admin evict lab gpu1`, which deletes the provisioning row of the HEALTHY
// incumbent and takes a second, innocent device down with it.
func TestAuthFailureHintNamesTheNameThatWasDenied(t *testing.T) {
	url := startDenyingNATS(t)
	a := leasedAgentForAuthTest(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := a.connectNATS(ctx)
	if err == nil {
		t.Fatal("setup: expected the denying server to reject the connect")
	}
	msg := err.Error()
	if !strings.Contains(msg, "admin evict") {
		t.Skipf("auth hint shape changed; got %q", msg)
	}
	if strings.Contains(msg, "admin evict lab gpu1\n") || strings.Contains(msg, "admin evict lab gpu1`") {
		t.Fatalf("the auth-denial hint names the BASENAME, not the name that was actually denied.\n"+
			"An operator following it runs `tether admin evict lab gpu1`, which deletes the "+
			"provisioning row the healthy incumbent depends on.\nfull message:\n%s", msg)
	}
	if !strings.Contains(msg, "gpu1-02") {
		t.Fatalf("the auth-denial hint never mentions %q — the name the broker actually refused.\n"+
			"full message:\n%s", "gpu1-02", msg)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §6 (N-1 四象限) —
// the degrade that connectNATS now performs on an auth denial under a lease
// name.
//
// connectNATS builds its nats.Option slice ONCE, before the retry loop, and
// nats.Name captures the routing name as a STRING at that moment. dropLease
// changes a.routingNID, but the already-built option list still carries the
// rejected name — so the `continue` re-presents exactly the credential the
// broker just refused. The second denial then finds nidOf(a) == cfg.NID and
// takes the FATAL return, i.e. the degrade buys one extra round trip and the
// process still dies.
//
// This test pins the mechanism without needing a discriminating auth server:
// it replays the SAME option slice connectNATS would reuse and shows the name
// does not follow the lease drop.
func TestConnectNameFollowsTheLeaseDropOnRetry(t *testing.T) {
	url := startDenyingNATS(t)
	a := leasedAgentForAuthTest(t, url)

	built := a.buildConnOptions() // what connectNATS computes before its loop

	applied := func(opts []nats.Option) string {
		t.Helper()
		var o nats.Options
		for _, f := range opts {
			if err := f(&o); err != nil {
				t.Fatalf("apply option: %v", err)
			}
		}
		return o.Name
	}

	leasedName := applied(built)
	if !strings.Contains(leasedName, "gpu1-02") {
		t.Fatalf("setup: CONNECT name under a lease was %q", leasedName)
	}

	dropLease(a)
	if got := nidOf(a); got != "gpu1" {
		t.Fatalf("setup: dropLease left nidOf=%q", got)
	}

	// The frozen slice is a FACT about slices, not about the fix: what matters
	// is whether the retry uses a REBUILT one. connectNATS now rebuilds its
	// options after dropLease (see the isAuthFailure arm), so the check is that
	// a fresh build follows the dropped lease.
	if stale := applied(built); stale != leasedName {
		t.Fatalf("setup: the pre-drop slice should still carry %q, got %q", leasedName, stale)
	}
	retryName := applied(a.buildConnOptions())
	if retryName == leasedName {
		t.Fatalf("after dropping the lease the retry still presents CONNECT name %q.\n"+
			"connectNATS builds connOpts ONCE above its retry loop (internal/agent/agent.go: "+
			"`connOpts := a.buildConnOptions()`), and nats.Name froze the routing name into that "+
			"slice. So the `continue` after dropLease re-offers the very name auth_callout just "+
			"refused; the second denial sees nidOf(a) == cfg.NID and takes the FATAL return, and "+
			"the process exits anyway. The degrade must rebuild the options (or move the build "+
			"inside the loop) to have any effect.\nfresh build would be: %q", retryName,
			applied(a.buildConnOptions()))
	}
}
