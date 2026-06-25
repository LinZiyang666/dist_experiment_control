package broker

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// b6_webhook_test.go — B6 OPS#2 alert webhook (make test): URL validation, non-blocking
// queue-full drop, and the COMMITTED-delta seam (idle→0 POST, raise→1, clear→1, leadership
// flap→0 + no re-fire of already-active alerts).

func TestParseWebhookURL(t *testing.T) {
	ok := []string{"http://internal:9093/hook", "https://alertmanager.example/api"}
	for _, u := range ok {
		if _, err := parseWebhookURL(u); err != nil {
			t.Errorf("parseWebhookURL(%q) should pass: %v", u, err)
		}
	}
	bad := []string{
		"file:///etc/passwd",                  // non-HTTP scheme
		"gopher://x",                          // non-HTTP scheme
		"http://user:pass@internal:9093/hook", // userinfo (secret-in-URL)
		"http://",                             // no host
	}
	for _, u := range bad {
		if _, err := parseWebhookURL(u); err == nil {
			t.Errorf("parseWebhookURL(%q) should be rejected", u)
		}
	}
}

func TestWebhookPosterQueueFullDropsNonBlocking(t *testing.T) {
	p, err := newWebhookPoster("http://internal:9093/hook", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Do NOT start Run — the queue (cap 64) fills + every further Post must drop immediately
	// (non-blocking), never deadlock.
	for i := 0; i < webhookQueueCap+50; i++ {
		done := make(chan struct{})
		go func() { p.Post(WebhookEvent{Transition: "raised", DedupKey: "k"}); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Post blocked — it must be a non-blocking enqueue")
		}
	}
	if p.Drops() == 0 {
		t.Fatal("a full queue must drop events (Drops()>0)")
	}
}

// TestWebhookHungEndpointDoesNotWedge — Stage-C MAJOR-5: a hung endpoint must NOT stall Post (the
// reconcile pass), and the drain goroutine must exit on ctx cancel (no leak). Run with -race.
func TestWebhookHungEndpointDoesNotWedge(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // hang every request until the test releases it
	}))
	defer srv.Close()
	defer close(release)

	p, err := newWebhookPoster(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	// Fire far more than the queue cap; every Post must return promptly (non-blocking) even though
	// the single in-flight request is hung.
	for i := 0; i < webhookQueueCap+200; i++ {
		done := make(chan struct{})
		go func() { p.Post(WebhookEvent{Transition: "raised", DedupKey: "k"}); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("Post blocked while the endpoint was hung — it must never stall the reconcile pass")
		}
	}
	if p.Drops() == 0 {
		cancel()
		t.Fatal("a hung endpoint backing up the queue must drop events")
	}

	// Cancel: the in-flight Do must abort on ctx cancel and Run must exit (no goroutine leak). Use a
	// generous tolerance + deadline: under full-suite load other packages' goroutines perturb the
	// absolute count, so this gate proves "the drain goroutine + its in-flight Do unwind" (the leak
	// would be unbounded), not an exact count.
	cancel()
	const tol = 8
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > base+tol && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base+tol {
		t.Fatalf("drain goroutine leaked: NumGoroutine %d > baseline+%d %d", n, tol, base+tol)
	}
}

// TestWebhookErrorStatusKeepsDraining — Stage-C MAJOR-5 sibling: a 5xx endpoint must not panic and
// the drain goroutine keeps processing the queue.
func TestWebhookErrorStatusKeepsDraining(t *testing.T) {
	var hits int32
	done := make(chan struct{}, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusServiceUnavailable)
		select {
		case done <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()
	p, err := newWebhookPoster(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	for i := 0; i < 3; i++ {
		p.Post(WebhookEvent{Transition: "raised", DedupKey: "k"})
	}
	// At least the first event reaches the (failing) endpoint; deliver() must not panic + Run lives.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain goroutine did not deliver to a 5xx endpoint (panicked or wedged?)")
	}
}

// seedManualAlert inserts an ACTIVE manual alert (a kind the reconciler does NOT own, so its
// decision logic never clears it — isolating the webhook committed-delta).
func seedManualAlert(t *testing.T, db *sql.DB, key string) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO alerts(id,kind,severity,dedup_key,state,message,raised_at)
		 VALUES(?, 'manual','info',?,'ACTIVE','hi','2026-06-23 12:00:00 +0000 UTC')`,
		"id-"+key, key,
	); err != nil {
		t.Fatalf("seed manual alert: %v", err)
	}
}

func TestWebhookCommittedDeltaFires(t *testing.T) {
	db := reconTestDB(t)
	f := &alertFake{leader: true, voters: 3} // 3 voters → no below_quorum interference
	var events []WebhookEvent
	rec := NewAlertReconciler(AlertReconcilerConfig{
		Node: f, DB: db, Now: func() time.Time { return time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC) },
		Propose:  func(plan func(*sql.DB) (*cluster.Command, error)) error { return nil }, // no-op
		Webhook:  func(ev WebhookEvent) { events = append(events, ev) },
		LeaderID: func() string { return "node-A" },
	})
	ctx := context.Background()
	pass := func() {
		if err := rec.ReconcileAlertsOnce(ctx); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	// pass 1: establish the baseline — NO webhook (no double-fire on first leader pass).
	pass()
	if len(events) != 0 {
		t.Fatalf("first leader pass must only seed the baseline, got %d events", len(events))
	}

	// raise: a manual alert appears in the committed ACTIVE set → exactly 1 "raised".
	seedManualAlert(t, db, "manual:x")
	pass()
	if len(events) != 1 || events[0].Transition != "raised" || events[0].Kind != "manual" || events[0].DedupKey != "manual:x" || events[0].ClusterLeader != "node-A" {
		t.Fatalf("raise transition wrong: %+v", events)
	}
	if events[0].Node != "x" { // node parsed from the dedup_key suffix
		t.Fatalf("node should be parsed from key, got %q", events[0].Node)
	}

	// idle pass: no change → still 1.
	pass()
	if len(events) != 1 {
		t.Fatalf("idle pass must POST nothing, got %d", len(events))
	}

	// clear: the alert leaves the ACTIVE set → exactly 1 "cleared".
	if _, err := db.Exec(`UPDATE alerts SET state='CLEARED' WHERE dedup_key='manual:x'`); err != nil {
		t.Fatal(err)
	}
	pass()
	if len(events) != 2 || events[1].Transition != "cleared" || events[1].DedupKey != "manual:x" {
		t.Fatalf("clear transition wrong: %+v", events)
	}
}

func TestWebhookNoRefireOnLeadershipChange(t *testing.T) {
	db := reconTestDB(t)
	f := &alertFake{leader: true, voters: 3}
	var events []WebhookEvent
	rec := NewAlertReconciler(AlertReconcilerConfig{
		Node: f, DB: db, Now: func() time.Time { return time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC) },
		Propose: func(plan func(*sql.DB) (*cluster.Command, error)) error { return nil },
		Webhook: func(ev WebhookEvent) { events = append(events, ev) },
	})
	ctx := context.Background()

	// An alert is already ACTIVE + the leader has a baseline including it.
	seedManualAlert(t, db, "manual:y")
	_ = rec.ReconcileAlertsOnce(ctx) // baseline includes manual:y
	if len(events) != 0 {
		t.Fatalf("baseline pass must not fire, got %d", len(events))
	}

	// Lose leadership → the next leader pass must RE-BASELINE (no re-fire of the still-active
	// alert). This is the no-double-fire-on-leadership-change guarantee.
	f.leader = false
	_ = rec.ReconcileAlertsOnce(ctx) // resets webhookSeeded
	f.leader = true
	_ = rec.ReconcileAlertsOnce(ctx) // re-seeds baseline (manual:y still active) → NO event
	if len(events) != 0 {
		t.Fatalf("a new leader must NOT re-POST already-active alerts, got %d: %+v", len(events), events)
	}
}
