package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// expose_test.go — the agent half of gotcha #86: a fresh expose's first REGISTER at its home may be
// answered with the TRANSIENT home_catching_up (the home has not applied the allocation yet). The
// agent retries inside the broker's forward window and, if the budget runs out, reports the transient
// code rather than frpc_failed.

type scriptedAdapter struct {
	errs  []error // returned in order; nil = success; the last one repeats
	calls int
	// each, when set, is how long every AddProxy takes before answering — honouring ctx the way
	// the production adapter does (a call that outlives ctx returns ctx's error, never its
	// scripted answer). Zero = answer at once.
	each time.Duration
}

func (s *scriptedAdapter) AddProxy(ctx context.Context, _ PortToken) error {
	s.calls++
	if s.each > 0 {
		select {
		case <-time.After(s.each):
		case <-ctx.Done():
			return fmt.Errorf("scripted adapter: open cut: %w", ctx.Err())
		}
	}
	if s.calls <= len(s.errs) {
		return s.errs[s.calls-1]
	}
	return s.errs[len(s.errs)-1]
}
func (s *scriptedAdapter) RemoveProxy(string, int) error { return nil }

// ladderCtx is the ladder's whole-budget ctx as the handler builds it (exposeOpenContext).
func ladderCtx(t *testing.T, budget time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	t.Cleanup(cancel)
	return ctx
}

// origin: simcluster-speed Block 2b, drill 74 solos 2026-09-19 (agent_rejected:frpc_failed 3/7 with
// `broker denied REGISTER: token_unknown_or_revoked` in agt1's slog seconds after a rebalance).
func TestAddProxyWithTransientRetryRetriesOnlyTransientDenies(t *testing.T) {
	catching := &tunnel.DenyError{Reason: proto.ReasonHomeCatchingUp}
	tryAgain := &tunnel.DenyError{Reason: "try_again"}
	terminal := &tunnel.DenyError{Reason: "token_unknown_or_revoked"}
	dial := errors.New("dial tcp 10.0.0.2:7000: connection refused")
	noSleep := func(time.Duration) {}

	t.Run("catching up twice, then open: three calls, success", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{catching, catching, nil}}
		if err := addProxyWithTransientRetry(ladderCtx(t, time.Second), ad, PortToken{}, time.Millisecond, noSleep); err != nil || ad.calls != 3 {
			t.Fatalf("err=%v calls=%d, want nil after 3 calls", err, ad.calls)
		}
	})
	t.Run("try_again is transient too", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{tryAgain, nil}}
		if err := addProxyWithTransientRetry(ladderCtx(t, time.Second), ad, PortToken{}, time.Millisecond, noSleep); err != nil || ad.calls != 2 {
			t.Fatalf("err=%v calls=%d, want nil after 2 calls", err, ad.calls)
		}
	})
	t.Run("a terminal deny is returned at once", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{terminal}}
		err := addProxyWithTransientRetry(ladderCtx(t, time.Second), ad, PortToken{}, time.Millisecond, noSleep)
		if !errors.Is(err, tunnel.ErrRegisterDenied) || ad.calls != 1 {
			t.Fatalf("err=%v calls=%d, want the terminal deny after exactly one call", err, ad.calls)
		}
		if denyIsTransientForExpose(err) {
			t.Fatal("token_unknown_or_revoked must not be classified transient")
		}
	})
	t.Run("a dial failure is not a deny and is returned at once", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{dial}}
		if err := addProxyWithTransientRetry(ladderCtx(t, time.Second), ad, PortToken{}, time.Millisecond, noSleep); !errors.Is(err, dial) || ad.calls != 1 {
			t.Fatalf("err=%v calls=%d, want the dial error after one call", err, ad.calls)
		}
	})
	t.Run("a ctx with no deadline never retries a transient deny", func(t *testing.T) {
		// The proxy path opens with no deadline by design; without one there is no budget to spend a
		// step of, so the ladder is a single call there (the transient is the caller's to retry).
		ad := &scriptedAdapter{errs: []error{catching}}
		if err := addProxyWithTransientRetry(context.Background(), ad, PortToken{}, time.Millisecond, noSleep); !denyIsTransientForExpose(err) || ad.calls != 1 {
			t.Fatalf("err=%v calls=%d, want the transient deny after exactly one call", err, ad.calls)
		}
	})
	// origin: simcluster-speed external review F1 — each call is bounded by the ladder's ONE deadline.
	t.Run("F1: a slow retry is cut at the deadline and reports the earlier transient, not the cut", func(t *testing.T) {
		// 300 ms budget, 50 ms step, 150 ms per call: call 1 answers catching_up at ~150 ms (≥ 150 ms
		// left > one step, so the ladder sleeps to ~200 ms); call 2 starts with ~100 ms left and would
		// answer nil at ~350 ms — past the budget, the review's 5.45 s shape in miniature. It must be
		// CUT at 300 ms instead, and the ladder must report the state the ctl can act on. The margins
		// are deliberate (re-review R1): call 1 may run up to 100 ms late before the retry decision
		// flips, so a loaded -race matrix cannot turn this into a false red.
		ad := &scriptedAdapter{errs: []error{catching, nil}, each: 150 * time.Millisecond}
		start := time.Now()
		err := addProxyWithTransientRetry(ladderCtx(t, 300*time.Millisecond), ad, PortToken{}, 50*time.Millisecond, time.Sleep)
		took := time.Since(start)
		if err == nil {
			t.Fatalf("calls=%d took=%s: the second call finished AFTER the 300 ms budget and was reported as success — the broker has rolled this allocation back", ad.calls, took)
		}
		if !denyIsTransientForExpose(err) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err=%v: want the earlier home_catching_up deny wrapped with the deadline cut", err)
		}
		if ad.calls != 2 || took > 900*time.Millisecond {
			t.Fatalf("calls=%d took=%s: the ladder must have tried once more and been cut at the deadline, not run the call to completion", ad.calls, took)
		}
	})
	t.Run("F1: a deadline cut with no earlier deny is the cut itself", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{nil}, each: 500 * time.Millisecond}
		err := addProxyWithTransientRetry(ladderCtx(t, 60*time.Millisecond), ad, PortToken{}, 5*time.Millisecond, time.Sleep)
		if !errors.Is(err, context.DeadlineExceeded) || denyIsTransientForExpose(err) || ad.calls != 1 {
			t.Fatalf("err=%v calls=%d: a first call that never answered inside the budget is a plain deadline, not a transient", err, ad.calls)
		}
	})
	t.Run("budget spent on a home that never catches up: last transient error back, bounded calls", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{catching}}
		var slept time.Duration
		sleep := func(d time.Duration) { slept += d }
		// A real clock bounds the loop: 20 ms budget in 5 ms steps ⇒ at most a handful of calls. The
		// call runs under its own 2 s watchdog so a retry that ignores the budget reads as a fast red,
		// not as a hung test (the broker would have rolled the allocation back long before).
		start := time.Now()
		done := make(chan error, 1)
		go func() {
			done <- addProxyWithTransientRetry(ladderCtx(t, 20*time.Millisecond), ad, PortToken{}, 5*time.Millisecond, func(d time.Duration) { sleep(d); time.Sleep(d) })
		}()
		var err error
		select {
		case err = <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("the retry did not return within 2 s of a 20 ms budget (calls so far %d)", ad.calls)
		}
		if !denyIsTransientForExpose(err) {
			t.Fatalf("want the last transient deny back when the budget is spent, got %v", err)
		}
		if ad.calls < 2 || ad.calls > 6 || slept > 25*time.Millisecond || time.Since(start) > 200*time.Millisecond {
			t.Fatalf("calls=%d slept=%s took=%s — the retry must stay inside the budget", ad.calls, slept, time.Since(start))
		}
	})
}

// TestHandleExposeForwardedRetriesATransientHomeAndReportsTheTransientCode drives the HANDLER — not
// the ladder helper — over a real nats.Msg with a scripted adapter, so the two things only the call
// site decides are pinned: the wired retry budget (a zero budget would make "catching up twice, then
// open" a failure) and the reply Code selection (a home that stayed catching up for the whole budget
// is reported as home_catching_up, exit 75 "retry", not frpc_failed, exit 64 "read the agent logs").
// Round-2 review R4-2-F8 replayed both regressions with every hermetic test green.
// origin: simcluster-speed review round 2 R4-2-F8 (gotcha #86 half ③)
func TestHandleExposeForwardedRetriesATransientHomeAndReportsTheTransientCode(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	catching := &tunnel.DenyError{Reason: proto.ReasonHomeCatchingUp}
	body, _ := json.Marshal(proto.ExposeForwardedReq{Name: "web", Port: 14003, LocalPort: 8080, Token: "tok"})

	run := func(t *testing.T, ad *scriptedAdapter) (proto.ExposeForwardedResp, *stateStore) {
		t.Helper()
		store := newStateStore(t.TempDir(), "lab")
		a := &Agent{cfg: Config{ExposeAdapter: ad, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, stateStore: store}
		subj := "test.expose.forwarded." + t.Name()
		sub, err := nc.Subscribe(subj, func(m *nats.Msg) { a.handleExposeForwarded(nc, m) })
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		reply, err := nc.Request(subj, body, exposeOpenRetryBudget+2*time.Second)
		if err != nil {
			t.Fatalf("no reply from the handler: %v", err)
		}
		var resp proto.ExposeForwardedResp
		if err := json.Unmarshal(reply.Data, &resp); err != nil {
			t.Fatal(err)
		}
		return resp, store
	}
	portsIn := func(store *stateStore) int {
		sf, err := store.load()
		if err != nil {
			t.Fatal(err)
		}
		return len(sf.PortTokens)
	}

	t.Run("catching up twice, then open: OK after three AddProxy calls, port persisted", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{catching, catching, nil}}
		resp, store := run(t, ad)
		if !resp.OK || ad.calls != 3 {
			t.Fatalf("resp=%+v calls=%d, want OK after 3 calls (a zero wired budget answers on the first deny)", resp, ad.calls)
		}
		if n := portsIn(store); n != 1 {
			t.Fatalf("state.json holds %d ports after a successful expose, want 1", n)
		}
	})
	t.Run("catching up for the whole budget: home_catching_up, not frpc_failed, port rolled back", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{catching}}
		resp, store := run(t, ad)
		if resp.OK || resp.Code != proto.ReasonHomeCatchingUp {
			t.Fatalf("resp=%+v, want Code=%q (the transient the ctl maps to exit 75)", resp, proto.ReasonHomeCatchingUp)
		}
		if ad.calls < 2 {
			t.Fatalf("the handler gave up after %d call(s); the wired budget must allow retries", ad.calls)
		}
		if n := portsIn(store); n != 0 {
			t.Fatalf("state.json still holds %d port(s) after a failed expose; the next reconcile would re-establish a proxy the broker rolled back", n)
		}
	})
	t.Run("a terminal deny stays frpc_failed", func(t *testing.T) {
		ad := &scriptedAdapter{errs: []error{&tunnel.DenyError{Reason: "token_unknown_or_revoked"}}}
		resp, _ := run(t, ad)
		if resp.OK || resp.Code != "frpc_failed" || ad.calls != 1 {
			t.Fatalf("resp=%+v calls=%d, want frpc_failed after exactly one call", resp, ad.calls)
		}
	})
}

// TestHandleExposeForwardedAnswersInsideTheBrokersForwardWindow is the external review's F1 shape at
// the handler: the first REGISTER takes 2.6 s and is answered home_catching_up, the retry would take
// another 2.6 s and succeed — each call inside the broker's 5 s ExposeForwardTimeout, the two together
// 5.45 s past it. Before the fix the agent installed the session at ~5.5 s and replied OK to a request
// the broker had already timed out and rolled back (agent_no_responders), leaving a port the agent
// served and the broker did not know about. The request here waits exactly the broker's 5 s.
// origin: simcluster-speed external review F1
func TestHandleExposeForwardedAnswersInsideTheBrokersForwardWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("real 2.6 s handshakes")
	}
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	const brokerForwardTimeout = 5 * time.Second // proto/broker default ExposeForwardTimeout
	ad := &scriptedAdapter{errs: []error{&tunnel.DenyError{Reason: proto.ReasonHomeCatchingUp}, nil}, each: 2600 * time.Millisecond}
	store := newStateStore(t.TempDir(), "lab")
	a := &Agent{cfg: Config{ExposeAdapter: ad, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, stateStore: store}
	handled := make(chan struct{})
	sub, err := nc.Subscribe("test.expose.forwarded.f1", func(m *nats.Msg) { a.handleExposeForwarded(nc, m); close(handled) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	body, _ := json.Marshal(proto.ExposeForwardedReq{Name: "web", Port: 14003, LocalPort: 8080, Token: "tok"})

	start := time.Now()
	reply, reqErr := nc.Request("test.expose.forwarded.f1", body, brokerForwardTimeout)
	elapsed := time.Since(start)
	<-handled
	if reqErr != nil {
		t.Fatalf("no reply inside the broker's %s forward window (elapsed %s, calls %d): the broker has answered agent_no_responders and rolled the allocation back while the agent is still opening", brokerForwardTimeout, elapsed, ad.calls)
	}
	var resp proto.ExposeForwardedResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatalf("resp=%+v after %s: the slow retry must not be reported as a success (it was cut at the ladder's deadline)", resp, elapsed)
	}
	if resp.Code != proto.ReasonHomeCatchingUp {
		t.Fatalf("resp=%+v: the cut retry after a catching-up deny must still read home_catching_up (exit 75, retry), got %q", resp, resp.Code)
	}
	if elapsed > exposeOpenRetryBudget+time.Second {
		t.Fatalf("the handler answered after %s; the whole ladder is bounded by the %s budget", elapsed, exposeOpenRetryBudget)
	}
	sf, err := store.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(sf.PortTokens) != 0 {
		t.Fatalf("state.json holds %d port(s) after the cut open; the failure the agent reported must not leave a port it would replay", len(sf.PortTokens))
	}
}
