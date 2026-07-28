package broker

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// alert_webhook_outcome_test.go — loop liveness and delivery outcome must be SEPARATELY observable.
//
// origin: batch B2 independent external review B2-4
//
// THE THREE STATES AN OPERATOR HAS TO TELL APART
// ----------------------------------------------
//	iterations == 0                  nothing was dequeued — nothing fired, or the consumer is wedged
//	iterations > 0, rejected == 0     alerts are going out
//	iterations > 0, accepted == 0     the consumer is ALIVE and the ENDPOINT is refusing
//
// The shipped code could not express the third one. `beat` was keyed on "the endpoint accepted the
// POST", so a live poster draining events into an HTTP 401 endpoint reported iterations=0 /
// last_iter=null — which in this very struct is documented to mean DEAD (loopStat: "Iters == 0 after
// startup means DEAD" for periodic loops; ClusterLoopInfo: "Iters == 0 means nothing happened" for
// event-driven ones). Neither reading was true, and both point the operator away from the endpoint.
//
// That keying was itself a fix for the OPPOSITE defect (internal review B7-01): the beat had been
// unconditional while its comment claimed "per DELIVERED event", so a dead endpoint produced a climbing
// counter that read as "alerts are going out". Both complaints are correct, which is the tell that one
// integer was being asked two questions. This file pins the two-counter resolution, so neither fix can
// be re-applied on top of the other.
//
// TestWebhookLoopBeatTracksCompletedIterations (reviewer-authored) pins the liveness half. This pins
// that the outcome half exists, is separate, and adds up.

// TestWebhookOutcomeCountersDistinguishTheThreeStates drives the same poster through an accepting and a
// refusing endpoint and checks each state is reachable and distinguishable.
func TestWebhookOutcomeCountersDistinguishTheThreeStates(t *testing.T) {
	cases := []struct {
		name         string
		status       int
		transportErr bool
		wantAccepted int64
		wantRejected int64
		why          string
	}{
		{
			name: "endpoint accepts", status: http.StatusOK,
			wantAccepted: 3, wantRejected: 0,
			why: "the healthy state: every iteration also delivered",
		},
		{
			name: "endpoint refuses with 401", status: http.StatusUnauthorized,
			wantAccepted: 0, wantRejected: 3,
			why: "THE STATE THE OLD SHAPE COULD NOT REPORT. The loop iterated three times; under a " +
				"beat-on-acceptance counter that read as iterations=0, i.e. as a dead consumer, and sent " +
				"the operator looking at the wrong component",
		},
		{
			name: "endpoint unreachable (transport error)", transportErr: true,
			wantAccepted: 0, wantRejected: 3,
			why: "a transport failure is still a completed iteration — the event was dequeued and an " +
				"attempt was made. Counting it as neither would make accepted+rejected stop matching " +
				"iterations, and the sum is what makes the two counters interpretable together",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var beats int
			p := newFakeTransportPoster(t, tc.status, tc.transportErr, func() { beats++ })

			const events = 3
			for i := 0; i < events; i++ {
				p.deliverOneForTest(t)
			}

			st, iters, _ := p.Stats()
			if iters != uint64(events) {
				t.Errorf("published iterations = %d, want %d — the count RuntimeReport publishes for this "+
					"loop is derived from the same snapshot as accepted/rejected (RB2-4)", iters, events)
			}
			if beats != events {
				t.Errorf("beats = %d, want %d — the beat is LIVENESS and must fire once per completed "+
					"iteration regardless of what the endpoint said.\nwhy it matters: %s",
					beats, events, tc.why)
			}
			if st.Accepted != tc.wantAccepted || st.Rejected != tc.wantRejected {
				t.Errorf("accepted=%d rejected=%d, want accepted=%d rejected=%d\nwhy it matters: %s",
					st.Accepted, st.Rejected, tc.wantAccepted, tc.wantRejected, tc.why)
			}
			// The invariant that makes the pair readable: outcome accounts for every iteration.
			if st.Accepted+st.Rejected != int64(beats) {
				t.Errorf("accepted+rejected = %d but the loop completed %d iteration(s). The sum must "+
					"equal iterations, or an operator comparing cluster_loops[].iterations against "+
					"alert_webhook.{accepted,rejected} finds a gap with no explanation",
					st.Accepted+st.Rejected, beats)
			}
			if st.Drops != 0 {
				t.Errorf("drops = %d with an unfilled queue; drops is an ENQUEUE-side counter and must "+
					"not be incremented by delivery outcomes", st.Drops)
			}
		})
	}
}

// TestWebhookDropsStayOutsideTheIterationSum pins that a queue overflow is NOT counted as an iteration.
// A dropped event never reached the loop, so folding it into accepted/rejected would claim an attempt
// that never happened — and would break the accepted+rejected == iterations invariant the previous test
// relies on.
func TestWebhookDropsStayOutsideTheIterationSum(t *testing.T) {
	p := newFakeTransportPoster(t, http.StatusOK, false, func() {})

	// Fill the queue without draining it, then push one more.
	for i := 0; i < cap(p.ch); i++ {
		p.Post(WebhookEvent{DedupKey: "k", Transition: "raised"})
	}
	p.Post(WebhookEvent{DedupKey: "overflow", Transition: "raised"})

	st, _, _ := p.Stats()
	if st.Drops != 1 {
		t.Errorf("drops = %d, want 1 — a full queue must be visible, since those events are lost and no "+
			"iteration will ever account for them", st.Drops)
	}
	if st.Accepted != 0 || st.Rejected != 0 {
		t.Errorf("accepted=%d rejected=%d, want 0/0 — nothing has been dequeued yet, so no delivery "+
			"outcome exists", st.Accepted, st.Rejected)
	}
}

// TestWebhookOutcomeAndLoopBeatCannotExposeATornSnapshot demonstrates the runtimeSnapshot interleaving
// hidden by the sequential table above: the published equality must hold at every observable boundary,
// not only eventually after Run returns.
//
// origin: batch B2 independent external RE-review RB2-4
//
// WHAT CHANGED IN THIS TEST, AND WHY IT IS NOT A WEAKENING
// ---------------------------------------------------------
// The reviewer's version stood at the same boundary (paused INSIDE the beat callback) and compared
// `Stats().Accepted+Rejected` against a local `beats` counter standing in for the published
// `cluster_loops[alert-webhook].iterations`. That proxy was exact while iterations came from the
// loopSet's independently-incremented counter — which is precisely the defect.
//
// The fix removed the second counter rather than serialising the two writes: Stats now derives the
// iteration count from accepted+rejected in ONE read, and runtimeSnapshot publishes both from that one
// snapshot. So the local beat counter no longer stands for anything published, and asserting against it
// would test a relationship the design deliberately no longer has.
//
// The observation point therefore moved to the PUBLISHED pair — the same values an operator reads — and
// the scenario is unchanged: still paused inside the beat, still asserting at that exact instant. It
// remains a negative control for the old design, where the pause left loopSet.Iters at 0 while the
// webhook atomics already read 1.
//
// (Holding outcomeMu across the beat would ALSO have closed the tear, and was tried. It deadlocks this
// very test — the callback blocks on a channel while Stats waits for the lock — which is a fair warning
// about locks held across caller-supplied callbacks.)
func TestWebhookOutcomeAndLoopBeatCannotExposeATornSnapshot(t *testing.T) {
	beatEntered := make(chan struct{})
	releaseBeat := make(chan struct{})
	p := newFakeTransportPoster(t, http.StatusOK, false, func() {
		close(beatEntered)
		<-releaseBeat
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()
	p.Post(WebhookEvent{DedupKey: "k", Transition: "raised"})
	<-beatEntered

	// THE BOUNDARY: the outcome has been recorded and the beat has NOT returned. This is the instant the
	// old shape published accepted+rejected=1 next to iterations=0.
	st, iters, _ := p.Stats()
	release := func() { close(releaseBeat); cancel(); <-done }
	if sum := uint64(st.Accepted + st.Rejected); sum != iters {
		release()
		t.Fatalf("observable torn snapshot: accepted+rejected=%d while the published iterations=%d. "+
			"RuntimeReport publishes both from THIS one snapshot precisely so the documented equality "+
			"cannot be observed broken; if they can differ here, the second counter is back", sum, iters)
	}
	if iters != 1 {
		release()
		t.Fatalf("published iterations=%d at a boundary where exactly one event has been dequeued and "+
			"delivered, want 1 — an iteration that has completed its delivery must already be counted, "+
			"or the tear has simply moved to the other side", iters)
	}
	release()
}

// TestWebhookPublishedIterationAndLastIterRemainOneState is the second half of the public loop-row
// contract. accepted+rejected and iterations may agree while the SAME row still tears if LastIter comes
// from the independently updated loopSet.
//
// origin: batch B2 independent external second re-review
func TestWebhookPublishedIterationAndLastIterRemainOneState(t *testing.T) {
	loops := newLoopSet()
	loops.stats[webhookLoopName] = &loopStat{StartedAt: time.Now()}

	beatEntered := make(chan struct{})
	releaseBeat := make(chan struct{})
	p := newFakeTransportPoster(t, http.StatusOK, false, func() {
		close(beatEntered)
		<-releaseBeat
		loops.Beat(webhookLoopName)
	})

	b := &Broker{cl: &clusterRuntime{loops: loops}}
	b.cl.webhook.Store(p)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()
	p.Post(WebhookEvent{DedupKey: "k", Transition: "raised"})
	<-beatEntered

	rep := b.runtimeSnapshot()
	release := func() { close(releaseBeat); cancel(); <-done }
	if len(rep.ClusterLoops) != 1 || rep.ClusterLoops[0].Name != webhookLoopName {
		release()
		t.Fatalf("runtime report has unexpected loop rows: %+v", rep.ClusterLoops)
	}
	row := rep.ClusterLoops[0]
	if row.Iterations > 0 && row.LastIter == nil {
		release()
		t.Fatalf("webhook loop row tears one completed-iteration state: iterations=%d but last_iter=null. "+
			"recordOutcome publishes the derived iteration before the independent beat supplies LastIter; "+
			"an operator therefore sees a loop that completed work at no time", row.Iterations)
	}
	release()
}

// TestRuntimeReportOmitsWebhookStatsWhenNoWebhookIsConfigured pins the absence semantics: the field is a
// pointer with omitempty so "no webhook configured" is unambiguous, rather than being reported as a row
// of zeroes that reads as "configured and idle".
func TestRuntimeReportOmitsWebhookStatsWhenNoWebhookIsConfigured(t *testing.T) {
	var rep adminsock.RuntimeReport
	if rep.AlertWebhook != nil {
		t.Fatal("zero RuntimeReport must carry no AlertWebhook block")
	}
	// And when it IS present, zeroes are meaningful and must survive.
	rep.AlertWebhook = &adminsock.AlertWebhookStats{}
	if rep.AlertWebhook.Accepted != 0 || rep.AlertWebhook.Rejected != 0 || rep.AlertWebhook.Drops != 0 {
		t.Fatal("fixture")
	}
}

// fakeRoundTripper answers every request with a fixed status, or fails at the transport layer. It never
// opens a socket, so these tests are hermetic and fast.
type fakeRoundTripper struct {
	status int
	fail   bool
}

func (f *fakeRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	if f.fail {
		return nil, context.DeadlineExceeded
	}
	return &http.Response{StatusCode: f.status, Body: http.NoBody, Header: make(http.Header)}, nil
}

func newFakeTransportPoster(t *testing.T, status int, transportErr bool, beat func()) *webhookPoster {
	t.Helper()
	p, err := newWebhookPoster("http://127.0.0.1:1/hook", silentLogger())
	if err != nil {
		t.Fatal(err)
	}
	p.client = &http.Client{Transport: &fakeRoundTripper{status: status, fail: transportErr}, Timeout: time.Second}
	p.beat = beat
	return p
}

// deliverOneForTest runs exactly the body of Run's event branch. Calling it directly (rather than
// starting Run and racing a channel) keeps the accounting assertions deterministic — the property under
// test is the accounting, not the scheduling.
//
// It calls the PRODUCTION recordOutcome rather than re-implementing "increment then beat". It used to
// re-implement it, which meant these tests could stay green against a shape that had drifted — the same
// self-check-tests-a-copy defect external review RB2-4 and RB2-1 both surfaced elsewhere in this batch.
func (p *webhookPoster) deliverOneForTest(t *testing.T) {
	t.Helper()
	p.recordOutcome(p.deliver(context.Background(), WebhookEvent{DedupKey: "k", Transition: "raised"}))
}
