package agent

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S5 plan §7.2 / §8
func TestProxyApplyEntrypointsAreSerialized(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProxyApplyEntrypoints(t)
	}
	testharness.AssertNoGoroutineLeak(t, "proxy serialized entrypoints", beforeG)
	testharness.AssertNoFDLeak(t, "proxy serialized entrypoints", beforeFD, 0)
}

type proxyApplyProbeAdapter struct {
	a       *Agent
	entered chan bool
	proceed chan struct{}
	removed chan bool
}

func (p *proxyApplyProbeAdapter) AddProxy(context.Context, PortToken) error {
	// A real Start has happened. The apply reservation must cover both register
	// and push entrypoints, with table/serialization mutexes free during IO.
	free := p.a.proxy.mu.TryLock()
	localOwnerPublished := false
	if free {
		localOwnerPublished = p.a.proxy.srv != nil && p.a.proxy.srv.Serving()
		p.a.proxy.mu.Unlock()
	}
	serializationFree := p.a.proxyApplyMu.TryLock()
	owned := false
	if serializationFree {
		owned = p.a.proxy.applyDone != nil
		p.a.proxyApplyMu.Unlock()
	}
	p.entered <- free && serializationFree && owned && localOwnerPublished
	<-p.proceed
	return nil
}

func (p *proxyApplyProbeAdapter) RemoveProxy(string, int) error {
	free := p.a.proxy.mu.TryLock()
	if free {
		p.a.proxy.mu.Unlock()
	}
	serializationFree := p.a.proxyApplyMu.TryLock()
	if serializationFree {
		p.a.proxyApplyMu.Unlock()
	}
	p.removed <- free && serializationFree
	// Exercise the actual readiness hook while teardown owns the old server.
	p.a.onTunnelSessionState(14015, false)
	return nil
}

func exerciseProxyApplyEntrypoints(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	adapter := &proxyApplyProbeAdapter{a: a, entered: make(chan bool, 1), proceed: make(chan struct{}), removed: make(chan bool, 4)}
	a.cfg.ExposeAdapter = adapter
	defer stopProxyOnRunExit(a)
	if err := restoreProxyCut(a, proxyCutState{dispatchSequence: 90, lastAppliedSequence: 89}); err != nil {
		t.Fatal(err)
	}
	restored, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	if restored.dispatchSequence != 90 || restored.lastAppliedSequence != 89 {
		t.Fatal("restore lost one member of the dispatch/applied sequence pair")
	}
	// A queued cut must honor its entry deadline without stealing ownership
	// from an apply that is still completing IO. Release the premise even when
	// a mutation makes the waiter hang, so the oracle itself does not leak.
	held := beginProxyApply(a)
	waiting := make(chan error, 1)
	go func() { _, err := snapshotProxyCutUntil(a, time.Now().Add(20*time.Millisecond)); waiting <- err }()
	select {
	case err := <-waiting:
		held()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("queued proxy cut ignored its entry deadline")
		}
	case <-time.After(3 * time.Second):
		held()
		<-waiting
		t.Fatal("queued proxy cut waited beyond its entry deadline")
	}
	// Exactly the method the initial register and reconnect paths call.
	done := make(chan struct{})
	go func() {
		a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: true, PublicPort: 14015, Token: "allocation", Generation: 1, Epoch: 1,
			Keys: []proto.ProxyKey{{SubID: "alice", Secret: "material"}}})
		close(done)
	}()
	select {
	case valid := <-adapter.entered:
		if !valid {
			close(adapter.proceed)
			<-done
			t.Fatal("register apply bypassed proxyApplyMu ownership or held a table lock across Start/AddProxy")
		}
	case <-time.After(3 * time.Second):
		close(adapter.proceed)
		t.Fatal("register never reached real SS Start and AddProxy")
	}
	body, err := json.Marshal(proto.ProxyDirective{Enabled: true, Generation: 1, Epoch: 2,
		Keys: []proto.ProxyKey{{SubID: "bob", Secret: "survivor"}}})
	if err != nil {
		t.Fatal(err)
	}
	a.dispatchForwarded(nil, &nats.Msg{Subject: proto.SubjCmdForwarded("lab", "lab-1", "proxy-keys"), Data: body})
	close(adapter.proceed)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("register apply did not finish")
	}
	a.proxyHandlerWG.Wait()
	st, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	if st.dispatchSequence != 91 || st.lastAppliedSequence != 91 || st.epoch != 2 {
		t.Fatal("restored dispatch/applied pair was not installed before callback dispatch")
	}
	// A late register cannot overwrite the newer push, and an old goroutine
	// cannot advance ordering merely by carrying a greater keyset epoch.
	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: true, Generation: 1, Epoch: 1})
	stale, _ := json.Marshal(proto.ProxyDirective{Enabled: false, Generation: 9, Epoch: 99})
	a.handleProxyKeysForwarded(nil, &nats.Msg{Data: stale}, 90)
	if a.proxy.epoch != 2 || !a.proxyBound() {
		t.Fatal("stale register/push resurrected or revoked current authority")
	}
	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: false, Generation: 1, Epoch: 3})
	if !<-adapter.removed {
		t.Fatal("RemoveProxy/join held a proxy table or apply mutex")
	}
	for _, bad := range []proxyCutState{
		{dispatchSequence: -1}, {lastAppliedSequence: -1},
		{dispatchSequence: 4, lastAppliedSequence: 5}, {generation: -1},
		{timer: proxyFailClosedState{CleanupPending: true}},
	} {
		b := newProxyTestAgent(t)
		if restoreProxyCut(b, bad) == nil {
			t.Fatal("invalid paired sequence/timer cut was accepted")
		}
	}
	b := newProxyTestAgent(t)
	if err := restoreProxyCut(b, proxyCutState{dispatchSequence: math.MaxInt64, lastAppliedSequence: math.MaxInt64}); err != nil {
		t.Fatal(err)
	}
	b.dispatchForwarded(nil, &nats.Msg{Subject: proto.SubjCmdForwarded("lab", "lab-1", "proxy-keys"), Data: stale})
	b.proxyHandlerWG.Wait()
	if b.proxyDispatchN != math.MaxInt64 || b.lastAppliedPushSeq != math.MaxInt64 {
		t.Fatal("dispatch sequence wrapped at MaxInt64")
	}
}
