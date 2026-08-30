package agent

import (
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// origin: p13_external_review_round8_test.go (renamed in B6) — docs/reviews/p13-external-review-round8.md
func TestExternalReviewProxyWithoutTunnelDoesNotAckReady(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	a := newProxyTestAgent(t)
	a.cfg.ExposeAdapter = nil

	ready := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjEvNodeProxyReady(a.cfg.SID, a.cfg.NID, "ready"), ready)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	a.applyProxyDirective(nc, &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "token",
		Keys:       []proto.ProxyKey{{SubID: "alice", Secret: "alice-secret"}},
		Generation: 1,
		Epoch:      1,
	})

	if runningSrv(a) {
		t.Fatal("agent without a tunnel adapter started a proxy runtime")
	}
	select {
	case <-ready:
		t.Fatal("agent without a tunnel adapter falsely ACKed proxy readiness")
	case <-time.After(250 * time.Millisecond):
	}
}

func TestExternalReviewTunnelAdapterLocalMapIsConcurrentSafe(t *testing.T) {
	a := &TunnelExposeAdapter{localFor: map[int]int{}}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				port := 14000 + (i+offset)%16
				a.setLocal(port, 20000+i)
				_, _ = a.lookupLocal(port)
				a.deleteLocal(port)
			}
		}(g)
	}
	wg.Wait()
}
