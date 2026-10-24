package brokerupgrade

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

// forwardResult is one reply the forwarder delivered.
type forwardResult struct {
	response upgradewire.Response
	at       time.Time
}

func (f *agentRouteFixture) forwardRoute(body []byte, timeout time.Duration, request func(string, []byte, time.Duration) ([]byte, error)) AgentRoute {
	return AgentRoute{DB: f.db, SID: "lab", NID: "node", Actor: f.actor, ActorFP: f.fp, Reply: f.privateReply(), Body: body, Timeout: timeout, Request: request}
}

func collect(t *testing.T, results chan forwardResult) func([]byte) {
	return func(body []byte) {
		response, err := upgradewire.DecodeResponse(body)
		if err != nil {
			t.Errorf("forwarder reply is not a transaction envelope: %q", body)
		}
		results <- forwardResult{response: response, at: time.Now()}
	}
}

// audits counts the forwarder's audit calls by outcome code.
type audits struct {
	mu    sync.Mutex
	codes []string
}

func (a *audits) record(code string) { a.mu.Lock(); a.codes = append(a.codes, code); a.mu.Unlock() }
func (a *audits) seen() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.codes...)
}

// The forwarder answers every refusal on the caller's goroutine and relays an
// admitted request on its own goroutine with the request's original deadline:
// the caller is never held by the agent, the relay never gets more time than
// the requester was promised, a deadline spent before sending is a plain
// deferral even for a mutation, and the in-flight bound refuses (nothing sent)
// instead of queueing, then frees its slots when the relays end.
// origin: S8 BROKER-EXPORTER R-9 (FIX-DESCRIBE request R-9); hot-update S8 plan §7.2 / §11.2 P15
func TestAgentForwardRelaysOffTheCallerWithinTheOriginalDeadline(t *testing.T) {
	t.Run("refusals_answer_on_the_caller", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		var forwarder AgentForwarder
		var audited audits
		results := make(chan forwardResult, 3)
		forwarder.Forward(false, "not_owner", "denied", f.forwardRoute(f.prepare(t, nil), time.Second, nil), collect(t, results), audited.record)
		route := f.forwardRoute(f.prepare(t, nil), time.Second, func(string, []byte, time.Duration) ([]byte, error) {
			t.Error("a legacy target was forwarded")
			return nil, nil
		})
		forwarder.Forward(true, "", "", route, collect(t, results), audited.record)
		// A denial the shared policy does not audit is still answered; a request
		// without a reply inbox is audited and never answered.
		forwarder.Forward(false, "bad_request", "malformed", f.forwardRoute(f.prepare(t, nil), time.Second, nil), collect(t, results), audited.record)
		silent := f.forwardRoute(f.prepare(t, nil), time.Second, nil)
		silent.Reply = ""
		forwarder.Forward(false, "not_owner", "denied", silent, collect(t, results), audited.record)
		if len(results) != 3 {
			t.Fatalf("refusals were not answered (only to a reply inbox) before Forward returned: %d", len(results))
		}
		if got := (<-results).response; got.Code != "not_owner" {
			t.Fatalf("admission denial: %+v", got)
		}
		if got := (<-results).response; got.Code != upgradetx.CodeDeferred {
			t.Fatalf("legacy target: %+v", got)
		}
		if got := (<-results).response; got.Code != "bad_request" {
			t.Fatalf("unaudited denial: %+v", got)
		}
		if got := audited.seen(); len(got) != 3 || got[0] != "not_owner" || got[1] != upgradetx.CodeDeferred || got[2] != "not_owner" {
			t.Fatalf("audit outcomes: %q", got)
		}
	})
	t.Run("silent_agent_keeps_its_own_deadline", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		f.advertise(t, "upgrade-tx-v1")
		var forwarder AgentForwarder
		results := make(chan forwardResult, 1)
		const timeout = 400 * time.Millisecond
		granted := make(chan time.Duration, 1)
		start := time.Now()
		forwarder.Forward(true, "", "", f.forwardRoute(f.prepare(t, nil), timeout, func(_ string, _ []byte, d time.Duration) ([]byte, error) {
			granted <- d
			time.Sleep(d)
			return nil, nats.ErrTimeout
		}), collect(t, results), nil)
		if returned := time.Since(start); returned > timeout/4 {
			t.Fatalf("Forward held its caller for %v while the agent was silent", returned)
		}
		if d := <-granted; d <= 0 || d > timeout {
			t.Fatalf("relay granted %v, original budget %v", d, timeout)
		}
		r := <-results
		if r.response.Code != upgradetx.CodeDeferred || !strings.Contains(r.response.Error, "reply is unavailable") {
			t.Fatalf("silent agent prepare: %+v", r.response)
		}
		if elapsed := r.at.Sub(start); elapsed < timeout-50*time.Millisecond || elapsed > timeout+300*time.Millisecond {
			t.Fatalf("silent agent answered after %v, original deadline %v", elapsed, timeout)
		}
	})
	t.Run("time_before_the_relay_counts_against_the_deadline", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		f.advertise(t, "upgrade-tx-v1")
		var forwarder AgentForwarder
		results := make(chan forwardResult, 1)
		route := f.forwardRoute(f.prepare(t, nil), 400*time.Millisecond, func(_ string, _ []byte, d time.Duration) ([]byte, error) {
			if d <= 0 || d > 150*time.Millisecond {
				t.Errorf("relay granted %v of a budget whose first 250ms were already spent", d)
			}
			return f.reply, nil
		})
		route.Received = time.Now().Add(-250 * time.Millisecond)
		forwarder.Forward(true, "", "", route, collect(t, results), nil)
		if got := (<-results).response; got.Code != "" {
			t.Fatalf("relayed reply: %+v", got)
		}
	})
	t.Run("expired_mutation_is_a_plain_deferral", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		f.advertise(t, "upgrade-tx-v1")
		var forwarder AgentForwarder
		results := make(chan forwardResult, 1)
		forwarder.Forward(true, "", "", f.forwardRoute(f.cancel(t), 0, func(string, []byte, time.Duration) ([]byte, error) {
			t.Error("an expired request was sent")
			return nil, nil
		}), collect(t, results), nil)
		r := <-results
		// An accepted refusal would be projected as upgrade_unsettled (74).
		if r.response.Code != upgradetx.CodeDeferred || !strings.Contains(r.response.Error, "before forwarding") {
			t.Fatalf("expired mutation claimed a possible effect: %+v", r.response)
		}
	})
	t.Run("in_flight_bound_refuses_and_frees", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		f.advertise(t, "upgrade-tx-v1")
		var forwarder AgentForwarder
		release := make(chan struct{})
		var sent sync.WaitGroup
		results := make(chan forwardResult, AgentForwardLimit+2)
		blocking := func(string, []byte, time.Duration) ([]byte, error) {
			sent.Done()
			<-release
			return f.reply, nil
		}
		sent.Add(AgentForwardLimit)
		for i := 0; i < AgentForwardLimit; i++ {
			forwarder.Forward(true, "", "", f.forwardRoute(f.prepare(t, nil), time.Minute, blocking), collect(t, results), nil)
		}
		sent.Wait()
		forwarder.Forward(true, "", "", f.forwardRoute(f.prepare(t, nil), time.Minute, func(string, []byte, time.Duration) ([]byte, error) {
			t.Error("a request beyond the in-flight bound was sent")
			return nil, nil
		}), collect(t, results), nil)
		if got := (<-results).response; got.Code != upgradetx.CodeDeferred || !strings.Contains(got.Error, "capacity") {
			t.Fatalf("request beyond the bound: %+v", got)
		}
		close(release)
		for i := 0; i < AgentForwardLimit; i++ {
			if got := (<-results).response; got.Code != "" || got.View.Historical.Key != f.key {
				t.Fatalf("relayed reply %d: %+v", i, got)
			}
		}
		answered := make(chan struct{})
		forwarder.Forward(true, "", "", f.forwardRoute(f.prepare(t, nil), time.Minute, func(string, []byte, time.Duration) ([]byte, error) {
			close(answered)
			return f.reply, nil
		}), collect(t, results), nil)
		select {
		case <-answered:
		case r := <-results:
			t.Fatalf("slots were not freed after the relays ended: %+v", r.response)
		case <-time.After(10 * time.Second):
			t.Fatal("a request after the relays ended was neither sent nor refused")
		}
		if got := (<-results).response; got.Code != "" {
			t.Fatalf("slots were not freed after the relays ended: %+v", got)
		}
	})
}
