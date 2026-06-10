package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// An agent must converge after a broker DB restore rewinds the epoch. Under the
// round-3 (generation, epoch) ordering, a DB restore IS a new broker
// incarnation, so the restored directive carries a HIGHER Generation — that is
// what authorizes applying a numerically-lower epoch. (Updated from the round-2
// scalar-epoch form per the round-3 ordering contract; without a generation a
// lower same-incarnation epoch is correctly stale, per
// TestExternalReviewStaleRegisterReplyCannotOverrideNewerPush.)
func TestExternalReviewLowerEpochConvergesAfterBrokerRestore(t *testing.T) {
	a := newProxyTestAgent(t)
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "token",
		Keys:       []proto.ProxyKey{{SubID: "old", Secret: "old-secret"}},
		Generation: 100,
		Epoch:      5,
	})
	defer a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled:    false,
		Generation: 300,
		Epoch:      6,
	})

	// Restore = a NEWER broker incarnation (Generation 200 > 100) whose epoch
	// rewound to 2. The agent must apply it despite the lower epoch.
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled:    true,
		Keys:       []proto.ProxyKey{{SubID: "restored", Secret: "restored-secret"}},
		Generation: 200,
		Epoch:      2,
	})

	p := a.ensureProxyRuntime()
	p.mu.Lock()
	got := p.epoch
	p.mu.Unlock()
	if got != 2 {
		t.Fatalf("lower authoritative epoch was dropped: got epoch %d want 2", got)
	}
}

type failFirstProxyAdapter struct {
	mu    sync.Mutex
	adds  int
	token string
}

func (a *failFirstProxyAdapter) AddProxy(p PortToken) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.adds++
	a.token = p.Token
	if a.adds == 1 {
		return errors.New("transient tunnel failure")
	}
	return nil
}

func (*failFirstProxyAdapter) RemoveProxy(string, int) error { return nil }

// After a transient first AddProxy failure, the broker retains the allocation
// and subsequent ON/keyset pushes are tokenless. The connected agent must have
// a repair path instead of staying unready until an unrelated reconnect.
func TestExternalReviewTransientProxyStartFailureCanRecover(t *testing.T) {
	adapter := &failFirstProxyAdapter{}
	a := newProxyTestAgent(t)
	a.cfg.ExposeAdapter = adapter

	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "token",
		Keys:       []proto.ProxyKey{{SubID: "alice", Secret: "alice-secret"}},
		Epoch:      1,
	})
	if runningSrv(a) {
		t.Fatal("precondition: first tunnel start should fail")
	}

	// This is what enableProxy/bumpAndPushKeyset sends once the allocation
	// already exists: no raw token is available in the broker DB.
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: true,
		Keys:    []proto.ProxyKey{{SubID: "alice", Secret: "alice-secret"}},
		Epoch:   2,
	})
	if !runningSrv(a) {
		t.Fatal("connected agent has no recovery path after a transient proxy start failure")
	}
}

// Live pushes are sequence-ordered, but reconnect register replies bypass that
// sequence. An enabled reply computed before a newer OFF must not resurrect
// the proxy merely because its goroutine resumes after the OFF push.
func TestExternalReviewStaleRegisterReplyCannotOverrideNewerPush(t *testing.T) {
	a := newProxyTestAgent(t)
	encode := func(d proto.ProxyDirective) *nats.Msg {
		body, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return &nats.Msg{Data: body}
	}

	staleRegister := &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "old-token",
		Keys:       []proto.ProxyKey{{SubID: "old", Secret: "old-secret"}},
		Epoch:      1,
	}
	a.applyProxyDirective(context.Background(), nil, staleRegister)
	if !runningSrv(a) {
		t.Fatal("precondition: epoch-1 register directive did not start the proxy")
	}

	// Newer authoritative OFF push is applied.
	a.handleProxyKeysForwarded(nil, encode(proto.ProxyDirective{
		Enabled: false,
		Epoch:   2,
	}), 1)

	// A reconnect register reply was computed while ON at epoch 1, then its
	// goroutine resumes after the epoch-2 OFF. Register replies carry no seq.
	a.applyProxyDirective(context.Background(), nil, staleRegister)

	p := a.ensureProxyRuntime()
	p.mu.Lock()
	got := p.epoch
	serving := p.srv != nil
	p.mu.Unlock()
	if got != 2 || serving {
		t.Fatalf("stale register reply resurrected proxy after OFF: epoch=%d serving=%v", got, serving)
	}
}
