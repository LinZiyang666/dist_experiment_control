package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// alert_webhook_beat_test.go — what webhookPoster.beat actually counts.
//
// ⚠ THIS TEST IS RED ON THE TREE IT WAS WRITTEN AGAINST. It is a review artefact demonstrating a
// defect, not a passing gate. See /tmp/tether-b2-review/review-B7.md finding B7-01.
//
// THE DEFECT
// ----------
// alert_webhook.go:90-92 documents the field as "called once per DELIVERED event", and
// clusterwrite.go:461-464 tells a status reader to "interpret Iters as 'alerts delivered'". But
// Run (:165-171) calls p.beat() unconditionally after p.deliver(ctx, ev), and deliver (:175-196)
// returns nothing and swallows every failure it can have:
//
//	json.Marshal error          -> Warn + return
//	http.NewRequestWithContext  -> Warn + return
//	p.client.Do error           -> Warn + return   (endpoint down, DNS gone, TLS refused)
//	resp.StatusCode >= 400      -> Warn, falls through
//
// So ITERS in `tether admin runtime` counts events DEQUEUED, and it is published under a label that
// says DELIVERED. An operator whose webhook token expired sees ITERS climbing and concludes the
// alerts went out. This is the F-03 lesson with the sign flipped: instead of a field with no writer,
// a field with a writer that means something else.
//
// It matters more than a naming nit because the webhook is the only PUSH path off the broker. Every
// other alert surface requires someone to go look.
//
// HOW THIS WAS RESOLVED, AND WHY THIS TEST IS INVERTED RATHER THAN DELETED
// ------------------------------------------------------------------------
// The paragraph above ends with: "if the project decides ITERS should mean 'processed', the fix is to
// change BOTH comments and the CLI legend — but then this test should be inverted, not deleted, so the
// meaning stays pinned." That is the branch that was taken, and this is that inversion.
//
// The first fix gated the beat on a 2xx. The independent external review (B2-4) then showed that this
// breaks a SHARED contract: loopStat defines Iters/LastIter as per-completed-iteration liveness and
// ClusterLoopInfo repeats that an event-driven loop with Iters==0 means "nothing happened", so a live
// poster draining events into a 401 endpoint reported iterations=0 / last_iter=null — indistinguishable
// from a consumer that never ran. Two correct complaints about one integer is the tell that it was being
// asked two questions.
//
// So there are now two counters: `beat` is liveness (fires per completed iteration, whatever the endpoint
// said) and accepted/rejected are the delivery outcome, published as RuntimeReport.AlertWebhook. THE
// CONCERN THIS TEST WAS WRITTEN FOR IS UNCHANGED — "an operator with a dead endpoint must not read
// progress as proof the alerts went out" — it has simply moved to the field that can express it. This
// test now asserts exactly that: on a 401, the beat DOES fire (the loop is alive) and `accepted` stays
// zero (nothing was delivered). Under the old shape the second half was inexpressible; under a shape
// that beats but does not count outcomes, the second half fails here.
//
// TestWebhookOutcomeCountersDistinguishTheThreeStates (alert_webhook_outcome_test.go) carries the full
// three-state table; this keeps the original 401-over-a-real-HTTP-server scenario, since that is the
// shape the fleet actually hits when a webhook token expires.
func TestWebhookRejectedDeliveryIsLiveButNotDelivered(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		// A live endpoint that REFUSES the payload — an expired token, a revoked integration, a
		// misconfigured route. The POST completes; the alert does not arrive.
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	p, err := newWebhookPoster(srv.URL, silentLogger())
	if err != nil {
		t.Fatalf("newWebhookPoster: %v", err)
	}

	var beats atomic.Int64
	p.beat = func() { beats.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()

	p.Post(WebhookEvent{DedupKey: "broker_down:pc732", Transition: "raise"})

	// Wait for the POST to have been attempted and the loop to have come back round.
	deadline := time.Now().Add(3 * time.Second)
	for hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() == 0 {
		t.Fatal("fixture broken: the poster never reached the endpoint")
	}
	// Give Run a moment to execute whatever it does after deliver returns.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	// HALF ONE — LIVENESS. The iteration completed, so the loop must say so. Suppressing the beat here
	// is what made a live consumer indistinguishable from a dead one (external review B2-4).
	if got := beats.Load(); got != 1 {
		t.Errorf("beat fired %d time(s) for an event that WAS dequeued and attempted, want 1.\n"+
			"Iters/LastIter are the shared per-completed-iteration liveness contract (loopStat, "+
			"ClusterLoopInfo). A rejected POST is a completed iteration; reporting zero makes a live "+
			"poster read as a loop that never ran, and points the operator at the wrong component.", got)
	}
	// HALF TWO — OUTCOME, which is the concern this test was originally written for. A 401 endpoint must
	// not be reportable as a successful delivery anywhere.
	st, _, _ := p.Stats()
	if st.Accepted != 0 {
		t.Errorf("accepted = %d for an event the endpoint REJECTED with 401, want 0.\n"+
			"This is the original finding: an operator whose webhook token expired must not read progress "+
			"as proof the alerts went out. Liveness may climb (the loop IS working); delivery may not.",
			st.Accepted)
	}
	if st.Rejected != 1 {
		t.Errorf("rejected = %d, want 1 — a 401 is a completed attempt that FAILED, and it has to be "+
			"counted somewhere or accepted+rejected stops accounting for the loop's iterations", st.Rejected)
	}
}

// TestWebhookLoopBeatTracksCompletedIterations is the independent external-review counterexample to
// the delivery-success interpretation above. loopSet documents Beat as per-iteration liveness, and the
// admin protocol says an event-driven loop with Iters==0 means "nothing happened". A rejected POST is
// still a completed loop iteration; suppressing the beat makes a live consumer look inert. Delivery
// success needs its own counter rather than changing the meaning of the shared liveness counter.
func TestWebhookLoopBeatTracksCompletedIterations(t *testing.T) {
	var hits atomic.Int64
	p, err := newWebhookPoster("http://review.invalid/hook", silentLogger())
	if err != nil {
		t.Fatalf("newWebhookPoster: %v", err)
	}
	p.client = &http.Client{Transport: externalReviewRoundTripper(func(*http.Request) (*http.Response, error) {
		hits.Add(1)
		return &http.Response{StatusCode: http.StatusUnauthorized, Body: http.NoBody}, nil
	})}

	var beats atomic.Int64
	p.beat = func() { beats.Add(1) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()

	p.Post(WebhookEvent{DedupKey: "broker_down:pc733", Transition: "raise"})
	deadline := time.Now().Add(3 * time.Second)
	for hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() == 0 {
		cancel()
		<-done
		t.Fatal("fixture broken: the poster never reached the endpoint")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if got := beats.Load(); got != 1 {
		t.Fatalf("completed one webhook-loop iteration but recorded %d beats; cluster_loops is a "+
			"liveness surface, so endpoint acceptance must not erase completed work", got)
	}
}

type externalReviewRoundTripper func(*http.Request) (*http.Response, error)

func (f externalReviewRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
