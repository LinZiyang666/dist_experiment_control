package agent

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

func d6Logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeHomeAdapter implements ExposeAdapter + homeApplier with a SCRIPTED
// ApplyHome that records calls (count, per-port concurrency) so the agent rehome
// retry/dedup logic can be exercised without a real tunnel.
type fakeHomeAdapter struct {
	mu            sync.Mutex
	calls         int
	addCalls      int
	maxPerPort    map[int]int // peak concurrent ApplyHome in-flight per port
	curPerPort    map[int]int
	lastEpoch     map[int]int64
	lastPin       map[int]string
	hasSession    map[int]bool
	checkSessions bool
	// respond returns the error ApplyHome should return for (port,epoch). nil = success.
	respond func(port int, epoch int64, call int) error
}

func newFakeHomeAdapter(respond func(port int, epoch int64, call int) error) *fakeHomeAdapter {
	return &fakeHomeAdapter{
		maxPerPort: map[int]int{}, curPerPort: map[int]int{}, lastEpoch: map[int]int64{}, lastPin: map[int]string{}, hasSession: map[int]bool{}, respond: respond,
	}
}

func (f *fakeHomeAdapter) Start(context.Context) {}
func (f *fakeHomeAdapter) AddProxy(p PortToken) error {
	f.mu.Lock()
	f.addCalls++
	f.mu.Unlock()
	if p.HomeBrokerAddr != "" && p.CertPins.Current == "" {
		return tunnel.ErrHomePinsRequired
	}
	f.mu.Lock()
	f.hasSession[p.Port] = true
	f.mu.Unlock()
	return nil
}
func (f *fakeHomeAdapter) RemoveProxy(string, int) error { return nil }
func (f *fakeHomeAdapter) ApplyHome(port int, _ string, epoch int64, pins proto.CertPins) error {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.curPerPort[port]++
	if f.curPerPort[port] > f.maxPerPort[port] {
		f.maxPerPort[port] = f.curPerPort[port]
	}
	f.lastEpoch[port] = epoch
	f.lastPin[port] = pins.Current
	f.mu.Unlock()

	err := f.respond(port, epoch, call)

	f.mu.Lock()
	f.curPerPort[port]--
	f.mu.Unlock()
	return err
}
func (f *fakeHomeAdapter) HasSession(port int) bool {
	if !f.checkSessions {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hasSession[port]
}

func newRehomeTestAgent(t *testing.T, adapter ExposeAdapter) *Agent {
	t.Helper()
	a := &Agent{
		cfg:            Config{Logger: d6Logger(), ExposeAdapter: adapter, NID: "lab-1", SID: "lab"},
		stateStore:     newStateStore(t.TempDir(), "lab"),
		rehomeWant:     map[int]proto.HomeDirective{},
		rehomeRunning:  map[int]bool{},
		rehomeSeq:      map[int]uint64{},
		deferredReplay: map[int]bool{},
	}
	return a
}

func homeAssign(port int, epoch int64) *proto.HomeAssignment {
	return homeAssignPin(port, epoch, "sha256:x")
}

func homeAssignPin(port int, epoch int64, pin string) *proto.HomeAssignment {
	return &proto.HomeAssignment{Directives: []proto.HomeDirective{{
		Name: "svc", PublicPort: port, NodeID: "node-B", BrokerAddr: "10.0.0.2:7000", Epoch: epoch,
		CertPins: proto.CertPins{Current: pin},
	}}}
}

func transientDeny() error {
	return fmt.Errorf("tunnel client: Open: %w", &tunnel.DenyError{Reason: proto.ReasonHomeCatchingUp})
}

// TestD6ApplyOneHomeRetriesThenConverges (A4 M1 / A6 B1, R-15): a rehome whose
// first dials return home_catching_up retries until the home catches up, then
// persists the new home epoch.
func TestD6ApplyOneHomeRetriesThenConverges(t *testing.T) {
	port := 14000
	a := newRehomeTestAgent(t, newFakeHomeAdapter(func(_ int, _ int64, call int) error {
		if call < 3 {
			return transientDeny() // catch up on the 3rd dial
		}
		return nil
	}))
	if err := a.stateStore.AddPort(PortToken{Name: "svc", Port: port, LocalPort: 9000, Token: "tok"}); err != nil {
		t.Fatal(err)
	}
	// short backoff so the test is fast: the loop's base is 500ms; drive enough wall time.
	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sf, _ := a.stateStore.load()
		if sf != nil && len(sf.PortTokens) == 1 && sf.PortTokens[0].Epoch == 1 && sf.PortTokens[0].HomeBrokerAddr == "10.0.0.2:7000" {
			return // converged + persisted
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("rehome never converged + persisted the new home epoch")
}

// TestD6ApplyOneHomeTerminalStops: a non-transient DENY (or any non-catching_up
// error) stops immediately — no retry.
func TestD6ApplyOneHomeTerminalStops(t *testing.T) {
	port := 14000
	fake := newFakeHomeAdapter(func(_ int, _ int64, _ int) error {
		return fmt.Errorf("tunnel client: Open: %w", &tunnel.DenyError{Reason: "token_unknown_or_revoked"})
	})
	a := newRehomeTestAgent(t, fake)
	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	time.Sleep(300 * time.Millisecond)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.calls != 1 {
		t.Fatalf("terminal DENY must stop after 1 call, got %d", fake.calls)
	}
}

// TestD6ApplyOneHomePinsRequiredStops: ErrHomePinsRequired (a clustered home with
// no pins) is terminal here (the directive will be re-delivered with pins on the
// next reconnect) — it must NOT loop forever.
func TestD6ApplyOneHomePinsRequiredStops(t *testing.T) {
	port := 14000
	fake := newFakeHomeAdapter(func(_ int, _ int64, _ int) error { return tunnel.ErrHomePinsRequired })
	a := newRehomeTestAgent(t, fake)
	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	time.Sleep(300 * time.Millisecond)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.calls != 1 {
		t.Fatalf("ErrHomePinsRequired must stop after 1 call, got %d", fake.calls)
	}
}

// TestD6RehomeDedupBoundedGoroutines (A4 B1 / A6 B2): a reconnect STORM of
// applyHomeDirectives for the SAME port spawns AT MOST ONE retry loop (one
// concurrent ApplyHome per port), and the live goroutine count returns to a
// baseline after convergence — NOT growing with the storm size.
func TestD6RehomeDedupBoundedGoroutines(t *testing.T) {
	port := 14000
	release := make(chan struct{})
	fake := newFakeHomeAdapter(func(_ int, _ int64, call int) error {
		if call == 1 {
			<-release // hold the first loop in-flight while the storm fires
		}
		return nil
	})
	a := newRehomeTestAgent(t, fake)

	base := runtime.NumGoroutine()
	// Storm: 50 reconnects fire applyHomeDirectives for the same port.
	for i := 0; i < 50; i++ {
		a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	}
	time.Sleep(100 * time.Millisecond)

	fake.mu.Lock()
	maxPer := fake.maxPerPort[port]
	fake.mu.Unlock()
	if maxPer > 1 {
		t.Fatalf("dedup failed: %d concurrent ApplyHome for one port (want ≤1)", maxPer)
	}
	// Goroutines must be bounded by #ports (1), not by the 50 reconnects.
	if peak := runtime.NumGoroutine() - base; peak > 5 {
		t.Fatalf("rehome storm leaked goroutines: +%d (want bounded by #ports)", peak)
	}
	close(release)

	// After release, the single loop converges and the goroutine count settles back.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine()-base <= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("rehome goroutines did not settle: +%d", runtime.NumGoroutine()-base)
}

// TestD6ReviewStaleTerminalDoesNotDropNewerDirective covers the handoff race
// where a stale lower-epoch rehome attempt reaches the old/ex-home after the
// broker row has already advanced. That old attempt may receive a terminal
// token_unknown_or_revoked, but a newer directive recorded while it was in
// flight must still be applied. Dropping rehomeWant unconditionally on exit
// loses the newer directive and wedges the expose until another reconnect.
func TestD6ReviewStaleTerminalDoesNotDropNewerDirective(t *testing.T) {
	port := 14000
	enteredEpoch1 := make(chan struct{})
	releaseEpoch1 := make(chan struct{})
	epoch2Called := make(chan struct{})
	fake := newFakeHomeAdapter(func(_ int, epoch int64, _ int) error {
		if epoch == 1 {
			close(enteredEpoch1)
			<-releaseEpoch1
			return fmt.Errorf("tunnel client: Open: %w", &tunnel.DenyError{Reason: "token_unknown_or_revoked"})
		}
		if epoch == 2 {
			close(epoch2Called)
			return nil
		}
		return nil
	})
	a := newRehomeTestAgent(t, fake)

	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	select {
	case <-enteredEpoch1:
	case <-time.After(time.Second):
		t.Fatal("epoch-1 rehome did not start")
	}
	a.applyHomeDirectives(context.Background(), homeAssign(port, 2))
	close(releaseEpoch1)

	select {
	case <-epoch2Called:
	case <-time.After(2 * time.Second):
		fake.mu.Lock()
		calls := fake.calls
		lastEpoch := fake.lastEpoch[port]
		fake.mu.Unlock()
		t.Fatalf("newer epoch-2 directive was dropped after stale terminal epoch-1 result; calls=%d last_epoch=%d", calls, lastEpoch)
	}
}

func TestD6ReviewDirectiveArrivingDuringCleanupRestartsWorker(t *testing.T) {
	port := 14000
	epoch2Called := make(chan struct{})
	fake := newFakeHomeAdapter(func(_ int, epoch int64, _ int) error {
		if epoch == 2 {
			close(epoch2Called)
		}
		return nil
	})
	a := newRehomeTestAgent(t, fake)

	var once sync.Once
	afterRehomeWantSettledHook = func(agent *Agent, gotPort int) {
		if agent != a || gotPort != port {
			return
		}
		once.Do(func() {
			agent.applyHomeDirectives(context.Background(), homeAssign(port, 2))
		})
	}
	defer func() { afterRehomeWantSettledHook = nil }()

	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	select {
	case <-epoch2Called:
	case <-time.After(2 * time.Second):
		fake.mu.Lock()
		calls := fake.calls
		lastEpoch := fake.lastEpoch[port]
		fake.mu.Unlock()
		t.Fatalf("new directive recorded during cleanup did not restart worker; calls=%d last_epoch=%d", calls, lastEpoch)
	}
}

// TestD6ReviewDeferredReplayOpensWhenPinsArrive covers the documented R-22 boot
// path: a clustered expose persisted in state.json has no persisted cert pins, so
// replayPortsFromState defers it with ErrHomePinsRequired. The next register
// reply delivers pins for the same epoch; the agent must then actually open the
// expose from state.json. Calling ApplyHome on a non-open tunnel session is a
// no-op, so without a replay/open path this leaves the expose down forever.
func TestD6ReviewDeferredReplayOpensWhenPinsArrive(t *testing.T) {
	port := 14000
	fake := newFakeHomeAdapter(func(_ int, _ int64, _ int) error { return nil })
	fake.checkSessions = true
	a := newRehomeTestAgent(t, fake)
	if err := a.stateStore.AddPort(PortToken{
		Name: "svc", Port: port, LocalPort: 9000, Token: "tok",
		HomeBrokerAddr: "10.0.0.2:7000", Epoch: 1,
	}); err != nil {
		t.Fatal(err)
	}

	a.replayPortsFromState()
	fake.mu.Lock()
	addsAfterReplay := fake.addCalls
	fake.mu.Unlock()
	if addsAfterReplay != 1 {
		t.Fatalf("boot replay should attempt AddProxy once and defer on missing pins, got %d calls", addsAfterReplay)
	}

	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		fake.mu.Lock()
		adds := fake.addCalls
		fake.mu.Unlock()
		if adds > addsAfterReplay {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	fake.mu.Lock()
	adds := fake.addCalls
	applyCalls := fake.calls
	fake.mu.Unlock()
	t.Fatalf("pins-arrived directive did not reopen deferred replay from state.json: AddProxy calls=%d ApplyHome calls=%d", adds, applyCalls)
}

func TestD9PreD6StateDirectiveOpensBeforeReplay(t *testing.T) {
	port := 14000
	fake := newFakeHomeAdapter(func(_ int, _ int64, _ int) error { return nil })
	fake.checkSessions = true
	a := newRehomeTestAgent(t, fake)
	if err := a.stateStore.AddPort(PortToken{Name: "svc", Port: port, LocalPort: 9000, Token: "tok"}); err != nil {
		t.Fatal(err)
	}

	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		fake.mu.Lock()
		adds := fake.addCalls
		applyCalls := fake.calls
		fake.mu.Unlock()
		if adds == 1 && applyCalls == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	fake.mu.Lock()
	adds := fake.addCalls
	applyCalls := fake.calls
	fake.mu.Unlock()
	t.Fatalf("pre-D6 persisted expose was not opened from directive pins before replay: AddProxy=%d ApplyHome=%d", adds, applyCalls)
}

// TestD6ReviewDeferredReplaySurvivesUnreadableState checks the degraded state
// read path of the deferred replay fix. If state.json is temporarily unreadable
// or unparseable when pins arrive, the agent has not opened anything yet and
// must keep the deferred marker for a later healthy read. Treating that failed
// open-from-state as success silently loses the expose.
func TestD6ReviewDeferredReplaySurvivesUnreadableState(t *testing.T) {
	port := 14000
	fake := newFakeHomeAdapter(func(_ int, _ int64, _ int) error { return nil })
	a := newRehomeTestAgent(t, fake)

	if err := os.MkdirAll(filepath.Dir(a.stateStore.path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(a.stateStore.path, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	a.rehomeMu.Lock()
	a.deferredReplay[port] = true
	a.rehomeMu.Unlock()

	a.applyHomeDirectives(context.Background(), homeAssign(port, 1))
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		a.rehomeMu.Lock()
		running := a.rehomeRunning[port]
		deferred := a.deferredReplay[port]
		a.rehomeMu.Unlock()
		if !running {
			if !deferred {
				t.Fatal("deferred replay marker was cleared even though state.json could not be read and no tunnel was opened")
			}
			fake.mu.Lock()
			adds := fake.addCalls
			fake.mu.Unlock()
			if adds != 0 {
				t.Fatalf("unreadable state must not call AddProxy, got %d calls", adds)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("rehome loop did not exit after unreadable state")
}

// TestD6ReviewSameEpochPinUpdateQueuedDuringLoop covers the agent-side half of
// pure-pin rotation. applyHomeDirectives records equal-epoch directives as the
// latest want, so the active per-port loop must apply an equal-epoch update that
// arrives while an older same-epoch attempt is in flight. Comparing only
// `want.Epoch > processed.Epoch` drops the new pins.
func TestD6ReviewSameEpochPinUpdateQueuedDuringLoop(t *testing.T) {
	port := 14000
	entered := make(chan struct{})
	release := make(chan struct{})
	secondCalled := make(chan struct{})
	fake := newFakeHomeAdapter(func(_ int, _ int64, call int) error {
		if call == 1 {
			close(entered)
			<-release
			return nil
		}
		if call == 2 {
			close(secondCalled)
		}
		return nil
	})
	a := newRehomeTestAgent(t, fake)

	a.applyHomeDirectives(context.Background(), homeAssignPin(port, 3, "sha256:old"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first same-epoch pin apply did not start")
	}
	a.applyHomeDirectives(context.Background(), homeAssignPin(port, 3, "sha256:new"))
	close(release)

	select {
	case <-secondCalled:
	case <-time.After(2 * time.Second):
		fake.mu.Lock()
		calls := fake.calls
		lastPin := fake.lastPin[port]
		fake.mu.Unlock()
		t.Fatalf("queued same-epoch pin update was dropped; calls=%d last_pin=%q", calls, lastPin)
	}
}

// TestD6UpdatePortHomeMonotone (L-4): UpdatePortHome never downgrades the
// persisted epoch — a stale/no-op rehome at a lower epoch leaves it untouched.
func TestD6UpdatePortHomeMonotone(t *testing.T) {
	store := newStateStore(t.TempDir(), "lab")
	if err := store.AddPort(PortToken{Name: "svc", Port: 14000, LocalPort: 9000, Token: "tok"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdatePortHome("svc", "10.0.0.2:7000", 5); err != nil {
		t.Fatal(err)
	}
	// A stale lower-epoch persist must be ignored.
	if err := store.UpdatePortHome("svc", "10.0.0.9:7000", 4); err != nil {
		t.Fatal(err)
	}
	sf, _ := store.load()
	if sf.PortTokens[0].Epoch != 5 || sf.PortTokens[0].HomeBrokerAddr != "10.0.0.2:7000" {
		t.Fatalf("monotone violated: epoch=%d addr=%q (want 5 / 10.0.0.2:7000)", sf.PortTokens[0].Epoch, sf.PortTokens[0].HomeBrokerAddr)
	}
	// A higher epoch DOES update.
	if err := store.UpdatePortHome("svc", "10.0.0.3:7000", 6); err != nil {
		t.Fatal(err)
	}
	sf, _ = store.load()
	if sf.PortTokens[0].Epoch != 6 {
		t.Fatalf("higher epoch must update, got %d", sf.PortTokens[0].Epoch)
	}
}

// TestD6PortTokenHomePersistence (C1 / review A6 M1): a PortToken's HomeBrokerAddr
// + Epoch ROUND-TRIP through state.json so a restart re-targets the right home;
// CertPins are NEVER persisted (json:"-") — re-delivered on register.
func TestD6PortTokenHomePersistence(t *testing.T) {
	store := newStateStore(t.TempDir(), "lab")
	if err := store.AddPort(PortToken{
		Name: "svc", Port: 14000, LocalPort: 9000, Token: "tok",
		HomeBrokerAddr: "10.0.0.2:7000", Epoch: 3,
		CertPins: proto.CertPins{Current: "sha256:secret"},
	}); err != nil {
		t.Fatal(err)
	}
	sf, err := store.load()
	if err != nil {
		t.Fatal(err)
	}
	p := sf.PortTokens[0]
	if p.HomeBrokerAddr != "10.0.0.2:7000" || p.Epoch != 3 {
		t.Fatalf("home addr/epoch did not persist: %+v", p)
	}
	if p.CertPins.Current != "" {
		t.Fatalf("CertPins must NOT persist (json:\"-\"), got %q", p.CertPins.Current)
	}
}
