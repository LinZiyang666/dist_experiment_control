package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// A reconnect register reply can be computed while the switch is OFF, then
// arrive after a newer live enable push. The older reply must not tear the
// newly enabled proxy down.
func TestExternalReviewStaleNilRegisterReplyDoesNotOverrideEnable(t *testing.T) {
	a := newProxyTestAgent(t)
	a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled:    true,
		PublicPort: 14000,
		Token:      "new-token",
		Cipher:     "chacha20-ietf-poly1305",
		Keys:       []proto.ProxyKey{{SubID: "alice", Secret: "alice-psk"}},
		Epoch:      2,
	})
	defer a.applyProxyDirective(context.Background(), nil, &proto.ProxyDirective{
		Enabled: false,
		Epoch:   3,
	})

	if !runningSrv(a) {
		t.Fatal("precondition: epoch-2 enable should be serving")
	}

	// nil is the proxy-OFF register representation and carries no epoch.
	a.applyProxyDirective(context.Background(), nil, nil)
	if !runningSrv(a) {
		t.Fatal("stale proxy-OFF register reply tore down a newer epoch-2 enable")
	}
}

func TestExternalReviewConcurrentProxyRuntimeInitialization(t *testing.T) {
	a := newProxyTestAgent(t)

	const workers = 32
	start := make(chan struct{})
	results := make(chan *proxyRuntime, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- a.ensureProxyRuntime()
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var first *proxyRuntime
	for got := range results {
		if first == nil {
			first = got
			continue
		}
		if got != first {
			t.Fatal("concurrent lazy initialization created multiple proxy runtimes")
		}
	}
}

type externalReviewFailSecondAdapter struct {
	mu   sync.Mutex
	adds int
}

func (a *externalReviewFailSecondAdapter) AddProxy(PortToken) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.adds++
	if a.adds == 2 {
		return errors.New("injected tunnel failure")
	}
	return nil
}

func (*externalReviewFailSecondAdapter) RemoveProxy(string, int) error { return nil }

func TestExternalReviewFailedProxyRebuildPublishesUnready(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	events := make(chan *nats.Msg, 4)
	sub, err := nc.ChanSubscribe("tether.v1.s.lab.ev.node.lab-1.proxy.*", events)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	a, err := New(Config{
		NATSURL:       url,
		SID:           "lab",
		NID:           "lab-1",
		Home:          t.TempDir(),
		ExposeAdapter: &externalReviewFailSecondAdapter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	a.runCtx = context.Background()

	a.applyProxyDirective(context.Background(), nc, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14000, Token: "token-1",
		Keys: []proto.ProxyKey{{SubID: "alice", Secret: "alice-psk"}}, Epoch: 1,
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-events:
		if msg.Subject != proto.SubjEvNodeProxyReady("lab", "lab-1", "ready") {
			t.Fatalf("first event=%q want ready", msg.Subject)
		}
	case <-time.After(time.Second):
		t.Fatal("initial proxy start did not publish ready")
	}

	// A fresh token forces a full rebuild. The injected second AddProxy fails
	// after the old server has already been stopped.
	a.applyProxyDirective(context.Background(), nc, &proto.ProxyDirective{
		Enabled: true, PublicPort: 14001, Token: "token-2",
		Keys: []proto.ProxyKey{{SubID: "alice", Secret: "alice-psk"}}, Epoch: 2,
	})
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if runningSrv(a) {
		t.Fatal("failed rebuild unexpectedly left a server running")
	}
	select {
	case msg := <-events:
		if msg.Subject != proto.SubjEvNodeProxyReady("lab", "lab-1", "unready") {
			t.Fatalf("rebuild failure event=%q want unready", msg.Subject)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("failed rebuild left broker proxy_ready stale because no unready event was published")
	}
}
