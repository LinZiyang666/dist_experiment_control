package agent

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// origin: p13_external_review_round6_test.go (renamed in B6) — docs/reviews/p13-external-review-round6.md
func TestExternalReviewFailClosedReconnectCanReapplyCurrentDirective(t *testing.T) {
	a := newProxyTestAgent(t)
	directive := &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "current-token",
		Cipher:     "chacha20-ietf-poly1305",
		Keys:       []proto.ProxyKey{{SubID: "current", Secret: "current-key"}},
		Generation: 100,
		Epoch:      5,
	}
	a.applyProxyDirective(nil, directive)
	if !runningSrv(a) {
		t.Fatal("precondition: proxy should be serving")
	}

	a.failClosedFire()
	if runningSrv(a) {
		t.Fatalf("fail-closed teardown left proxy serving")
	}

	a.applyProxyDirective(nil, directive)
	if !runningSrv(a) {
		t.Fatalf("authoritative reconnect directive at the current pair was dropped after fail-closed teardown")
	}
}

func TestExternalReviewDuplicateCurrentDirectiveReacksReady(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	a := newProxyTestAgent(t)
	directive := &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "current-token",
		Cipher:     "chacha20-ietf-poly1305",
		Keys:       []proto.ProxyKey{{SubID: "current", Secret: "current-key"}},
		Generation: 100,
		Epoch:      5,
	}
	a.applyProxyDirective(nc, directive)

	ready := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjEvNodeProxyReady(a.cfg.SID, a.cfg.NID, "ready"), ready)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// Models broker readiness state being cleared/lost while the proxy is still
	// serving. The broker's convergence loop re-sends the same authoritative
	// pair; the agent must re-ACK rather than drop before pubProxyReady.
	a.applyProxyDirective(nc, directive)
	select {
	case <-ready:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("duplicate current directive did not re-ACK readiness")
	}
}
