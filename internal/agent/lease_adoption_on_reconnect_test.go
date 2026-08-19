package agent

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §3.1
//
// Lease adoption lives ONLY in Agent.session (internal/agent/agent.go). The
// OTHER register site — onNATSReconnect (internal/agent/proxy.go) — re-registers
// on every nats.go reconnect and never looks at resp.Lease. On that path the
// forwarded subscription has already been replayed by nats.go, so the instance
// keeps its basename subscription AND its basename tunnel REGISTER identity
// (Client.SetNID is never called) while the broker believes it refused the
// register outright.
func TestReconnectRegisterAdoptsAnAssignedLeaseName(t *testing.T) {
	url := testharness.StartNATS(t)
	a, err := New(Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "gpu1",
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: time.Second,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	// A broker that answers every register with a CONTESTED verdict.
	sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{
			OK:    true,
			Lease: &proto.NodeLease{AssignedNID: "gpu1-02", Basename: "gpu1"},
		})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.setRunCtx(ctx)

	acli, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer acli.Close()

	a.onNATSReconnect(acli)

	if got := nidOf(a); got != "gpu1-02" {
		t.Fatalf("after a CONTESTED register on the reconnect path the agent still routes as %q; "+
			"the broker assigned gpu1-02, refused to register gpu1, and this process keeps "+
			"subscribing and dialling the tunnel under the basename", got)
	}
}

// origin: docs/reviews/cloned-credential-instances-external-review-tasklist.md D2
//
// A non-nil lease verdict is always terminal for the connection that presented
// the contested name. This includes the refusal shape (empty AssignedNID): on a
// reconnect nats.go has already replayed the old forwarded subscription, so
// leaving that connection alive lets the refused clone keep receiving commands
// under the incumbent's name.
func TestReconnectLeaseRefusalRetiresTheContestedSession(t *testing.T) {
	url := testharness.StartNATS(t)
	a, err := New(Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "gpu1",
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: time.Second,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{
			OK:    true,
			Lease: &proto.NodeLease{Basename: "gpu1"},
		})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	runCtx, runCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer runCancel()
	a.setRunCtx(runCtx)
	sessionCtx, sessionCancel := context.WithCancel(runCtx)
	a.sessCancelMu.Lock()
	a.sessCancel = sessionCancel
	a.sessCancelMu.Unlock()
	// Exercise the terminal refusal branch too. "Give up competing" cannot mean
	// "keep the already-replayed contested subscription forever": the broker
	// has just said this process does not hold that name. Earlier retries have
	// the same ordering bug (they wait before teardown); the terminal branch is
	// the unbounded form and is the release-safety invariant this test protects.
	a.leaseRefusals.Store(maxLeaseRefusals - 1)

	acli, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer acli.Close()
	a.onNATSReconnect(acli)

	select {
	case <-sessionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("refused reconnect left the contested session and its replayed subscription alive")
	}
	if !a.rebuilding.Load() || !a.rebuildRequested.Load() {
		t.Fatalf("refused reconnect did not request a clean rebuild: rebuilding=%v requested=%v",
			a.rebuilding.Load(), a.rebuildRequested.Load())
	}
}
