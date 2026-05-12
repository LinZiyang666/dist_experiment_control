package cli

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// fakeTransport is the injectable Transport for unit tests. It records
// call counts and serves canned slices.
type fakeTransport struct {
	nodes    []NodeInfo
	sessions []SessionInfo
	ports    []PortInfo

	nodeCalls    atomic.Int32
	sessionCalls atomic.Int32
	portCalls    atomic.Int32

	// inject error if non-nil
	err error

	// block until ctx cancellation if true (for budget tests)
	hang bool
}

func (f *fakeTransport) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	f.nodeCalls.Add(1)
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.nodes, nil
}
func (f *fakeTransport) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	f.sessionCalls.Add(1)
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.sessions, nil
}
func (f *fakeTransport) ListPorts(ctx context.Context) ([]PortInfo, error) {
	f.portCalls.Add(1)
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.ports, nil
}
func (f *fakeTransport) Close() {}

func sampleCtx() CompletionContext {
	return CompletionContext{
		Home:        "/home/test",
		NATSURL:     "nats://t:4222",
		ActorPubKey: "Uactor",
		SID:         "lab",
	}
}

// 1. Cache TTL: two calls within TTL hit cache (one transport call).
func TestCompletionCache_HitWithinTTL(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{nodes: []NodeInfo{
		{NID: "a100", Status: "ONLINE"},
		{NID: "timan1", Status: "ONLINE"},
		{NID: "ghost", Status: "OFFLINE"},
	}}
	c1, d1 := CompleteOnlineNodes(ft, sampleCtx(), "")
	c2, d2 := CompleteOnlineNodes(ft, sampleCtx(), "")

	if got := ft.nodeCalls.Load(); got != 1 {
		t.Errorf("transport called %d times, want 1 (cache should serve second call)", got)
	}
	if len(c1) != 2 || len(c2) != 2 {
		t.Errorf("expected 2 ONLINE nodes both calls, got %v / %v", c1, c2)
	}
	if d1 != cobra.ShellCompDirectiveNoFileComp || d2 != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("directive: got %v / %v, want NoFileComp", d1, d2)
	}
}

// 2. Identity-keyed cache: same NATSURL + sid but different actor → miss.
func TestCompletionCache_IdentityKeyed(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{nodes: []NodeInfo{{NID: "x", Status: "ONLINE"}}}

	a := sampleCtx()
	a.ActorPubKey = "Uactor-A"
	b := sampleCtx()
	b.ActorPubKey = "Uactor-B"

	CompleteOnlineNodes(ft, a, "")
	CompleteOnlineNodes(ft, b, "")

	if got := ft.nodeCalls.Load(); got != 2 {
		t.Errorf("transport called %d times, want 2 (different actors must not share cache)", got)
	}
}

// 3. Prefix filter.
func TestCompletionPrefixFilter(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{nodes: []NodeInfo{
		{NID: "a100", Status: "ONLINE"},
		{NID: "pc732", Status: "ONLINE"},
		{NID: "timan1", Status: "ONLINE"},
		{NID: "timan107", Status: "ONLINE"},
		{NID: "timan108", Status: "ONLINE"},
	}}
	got, _ := CompleteOnlineNodes(ft, sampleCtx(), "tim")
	if len(got) != 3 {
		t.Errorf("prefix tim* got %v, want 3", got)
	}
	for _, c := range got {
		if c[:3] != "tim" {
			t.Errorf("candidate %q does not match prefix tim", c)
		}
	}
}

// 4. Auth split: VisibleSessions returns all ACTIVE; OwnedSessions filters to IsOwner.
func TestCompletionAuthSplit(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{sessions: []SessionInfo{
		{SID: "lab", State: "ACTIVE", IsOwner: true},
		{SID: "shared", State: "ACTIVE", IsOwner: false},
		{SID: "deleting", State: "DELETING", IsOwner: true},
	}}
	visible, _ := CompleteVisibleSessions(ft, sampleCtx(), "")
	owned, _ := CompleteOwnedSessions(ft, sampleCtx(), "")

	if len(visible) != 2 {
		t.Errorf("visible got %v, want [lab, shared] (DELETING excluded)", visible)
	}
	if len(owned) != 1 || owned[0] != "lab" {
		t.Errorf("owned got %v, want [lab] only", owned)
	}
}

// 5. ALLOCATED-only filter for expose names.
func TestCompletionAllocatedOnly(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{ports: []PortInfo{
		{Name: "web", NID: "a100", State: "ALLOCATED"},
		{Name: "old", NID: "a100", State: "FREED"},
		{Name: "rev", NID: "a100", State: "REVOKED"},
		{Name: "other", NID: "timan1", State: "ALLOCATED"},
	}}
	// No nid filter: get all ALLOCATED across nodes.
	all, _ := CompleteAllocatedExposeNames(ft, sampleCtx(), "", "")
	if len(all) != 2 {
		t.Errorf("all-ALLOCATED got %v, want [other, web]", all)
	}
	// nid filter: only a100 ALLOCATED.
	ClearCompletionCacheForTest()
	a100Only, _ := CompleteAllocatedExposeNames(ft, sampleCtx(), "a100", "")
	if len(a100Only) != 1 || a100Only[0] != "web" {
		t.Errorf("nid=a100 got %v, want [web]", a100Only)
	}
}

// 6. No-identity / no-active-session short-circuits without transport call.
func TestCompletionNoIdentityShortCircuit(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{nodes: []NodeInfo{{NID: "x", Status: "ONLINE"}}}
	empty := CompletionContext{} // no actor, no sid

	out, dir := CompleteOnlineNodes(ft, empty, "")
	if out != nil {
		t.Errorf("expected nil candidates on missing identity, got %v", out)
	}
	if dir != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("directive on empty identity got %v, want NoFileComp", dir)
	}
	if got := ft.nodeCalls.Load(); got != 0 {
		t.Errorf("transport called %d times despite no identity, want 0", got)
	}

	// session helpers: empty ActorPubKey → short-circuit even with sid set
	cctxOnlySID := CompletionContext{SID: "lab"}
	if out, _ := CompleteVisibleSessions(ft, cctxOnlySID, ""); out != nil {
		t.Errorf("CompleteVisibleSessions with no actor returned %v", out)
	}
	if got := ft.sessionCalls.Load(); got != 0 {
		t.Errorf("session transport called %d times despite no identity", got)
	}
}

// Helper: a transport whose ListNodes hangs until ctx done.
func TestCompletionBudget_HangingTransport(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{hang: true}
	start := time.Now()
	out, dir := CompleteOnlineNodes(ft, sampleCtx(), "")
	elapsed := time.Since(start)

	if elapsed < CompletionBudget {
		t.Errorf("returned in %s, expected at least CompletionBudget=%s", elapsed, CompletionBudget)
	}
	if elapsed > CompletionBudget+200*time.Millisecond {
		t.Errorf("returned in %s, exceeded budget+slop (budget=%s)", elapsed, CompletionBudget)
	}
	if out != nil {
		t.Errorf("expected nil candidates on timeout, got %v", out)
	}
	if dir != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("directive on timeout got %v, want NoFileComp", dir)
	}
}

// Transport-level test 1: controlled stub listener that accepts TCP but
// never speaks NATS protocol. natsTransport.dial should hit the
// completionDialTimeout (750 ms) and return well within CompletionBudget.
func TestNATSTransport_DialTimeoutControlledStub(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Accept loop: take the connection but never write the NATS INFO line.
	// This holds the TCP layer open so dial cannot fail fast on RST; it
	// must wait for completionDialTimeout to fire.
	stopAccept := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				<-stopAccept
			}(conn)
		}
	}()
	t.Cleanup(func() { close(stopAccept) })

	url := "nats://" + ln.Addr().String()
	// Build a minimal natsTransport bypassing NewCompletionTransport (which
	// would short-circuit on missing identity). Use a fake identity that
	// only needs PublicKey + Seed for the connect attempt — the dial will
	// fail before any nkey exchange.
	tt := &natsTransport{
		cctx: CompletionContext{NATSURL: url, ActorPubKey: "Ufake", SID: "lab"},
		id:   &Identity{PublicKey: "Ufake", Seed: []byte("SUFAKE")},
	}
	t.Cleanup(tt.Close)

	gBefore := runtime.NumGoroutine()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), CompletionBudget)
	defer cancel()
	_, err = tt.dial(ctx, CtlNameUnactivated)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("dial should have errored against stub listener, got nil")
	}
	if elapsed > CompletionBudget+200*time.Millisecond {
		t.Errorf("dial returned in %s, exceeded budget+slop", elapsed)
	}
	if elapsed < 500*time.Millisecond {
		// Should be near completionDialTimeout (750 ms) or CompletionBudget
		// (1 s). If much faster, the stub closed unexpectedly and we
		// haven't actually exercised the timeout path.
		t.Logf("dial returned suspiciously fast in %s — verify stub setup", elapsed)
	}

	// Give cleanup goroutines a moment to settle, then check no big leak.
	time.Sleep(100 * time.Millisecond)
	gAfter := runtime.NumGoroutine()
	// Allow some slack — the test framework + stub accept loop add a few.
	if gAfter > gBefore+5 {
		t.Errorf("goroutine leak: before=%d after=%d", gBefore, gAfter)
	}
}

// Transport-level test 2: empty broker URL yields noop transport.
func TestNewCompletionTransport_EmptyURL(t *testing.T) {
	// Empty home → ResolveNATSURLFromHome returns the (unchanged) flag
	// default. Pass empty flag and flagChanged=false to force the
	// "no URL configured" path.
	tr := NewCompletionTransport("/nonexistent-home-xyz", "", false)
	if _, ok := tr.(noopTransport); !ok {
		t.Errorf("expected noopTransport on empty URL+no identity, got %T", tr)
	}
}

// Transport-level test 3: noop transport returns empty + no error from
// every method (used by helpers when identity / URL is missing).
func TestNoopTransport(t *testing.T) {
	tr := noopTransport{}
	ctx := context.Background()
	if nodes, err := tr.ListNodes(ctx); err != nil || nodes != nil {
		t.Errorf("noop ListNodes: nodes=%v err=%v, want nil nil", nodes, err)
	}
	if sess, err := tr.ListSessions(ctx); err != nil || sess != nil {
		t.Errorf("noop ListSessions: sess=%v err=%v, want nil nil", sess, err)
	}
	if ports, err := tr.ListPorts(ctx); err != nil || ports != nil {
		t.Errorf("noop ListPorts: ports=%v err=%v, want nil nil", ports, err)
	}
	tr.Close()
}

// Bonus: transport error surfaces as silent NoFileComp (not Error).
func TestCompletionSilentOnTransportError(t *testing.T) {
	ClearCompletionCacheForTest()
	t.Cleanup(ClearCompletionCacheForTest)

	ft := &fakeTransport{err: errors.New("simulated broker unreachable")}
	out, dir := CompleteOnlineNodes(ft, sampleCtx(), "")
	if out != nil {
		t.Errorf("expected nil on transport error, got %v", out)
	}
	if dir != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("directive on error got %v, want NoFileComp (silent)", dir)
	}
}
