package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
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

// webhookWireKeys is the EXACT public key whitelist of the on-the-wire payload (R12 #5). Every key is
// non-secret cluster topology. A change here is a deliberate wire-contract change (bump schema_version).
var webhookWireKeys = map[string]bool{
	"schema": true, "schema_version": true, "transition": true, "kind": true, "severity": true,
	"dedup_key": true, "message": true, "node": true, "cluster_leader": true, "ts": true,
}

// TestWebhookPayloadFieldSetIsExactlyTheWhitelist is the struct-level SECURITY guard: any field added to
// webhookPayload MUST be a consciously-whitelisted PUBLIC key. A future edit that adds a secret-bearing
// field (pin/seed/nkey/token/…) fails here before it can ever ship on the wire.
func TestWebhookPayloadFieldSetIsExactlyTheWhitelist(t *testing.T) {
	remaining := make(map[string]bool, len(webhookWireKeys))
	for k := range webhookWireKeys {
		remaining[k] = true
	}
	tp := reflect.TypeOf(webhookPayload{})
	for i := 0; i < tp.NumField(); i++ {
		key := strings.Split(tp.Field(i).Tag.Get("json"), ",")[0]
		if !webhookWireKeys[key] {
			t.Errorf("webhookPayload adds NON-whitelisted wire field %q — if it is a secret it MUST NOT ship; if public, add it to webhookWireKeys knowingly", key)
		}
		delete(remaining, key)
	}
	if len(remaining) != 0 {
		t.Errorf("webhookPayload is MISSING expected wire fields: %v", remaining)
	}
}

// TestWebhookOnWirePayloadFieldByField captures the ACTUAL POST body a real deliver() sends and asserts
// every field verbatim + the schema identity + the exact key whitelist (no stray keys).
func TestWebhookOnWirePayloadFieldByField(t *testing.T) {
	bodyCh := make(chan []byte, 1)
	ctCh := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		b, _ := io.ReadAll(r.Body)
		select {
		case bodyCh <- b:
			ctCh <- r.Header.Get("Content-Type")
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p, err := newWebhookPoster(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	p.Post(WebhookEvent{
		Transition: "raised", Kind: "broker_down", Severity: "warning",
		DedupKey: "broker_down:brk2", Message: "brk2 unreachable", Node: "brk2",
		ClusterLeader: "brk1", Ts: "2026-07-19T12:00:00Z",
	})

	var raw []byte
	select {
	case raw = <-bodyCh:
	case <-time.After(3 * time.Second):
		t.Fatal("no webhook POST received")
	}
	if ct := <-ctCh; ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}

	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("on-wire payload is not valid JSON: %v (%s)", err, raw)
	}
	for k := range got {
		if !webhookWireKeys[k] {
			t.Errorf("on-wire payload has non-whitelisted key %q: %s", k, raw)
		}
	}
	wantStr := map[string]string{
		"schema": "tether_alert_webhook", "transition": "raised", "kind": "broker_down",
		"severity": "warning", "dedup_key": "broker_down:brk2", "message": "brk2 unreachable",
		"node": "brk2", "cluster_leader": "brk1", "ts": "2026-07-19T12:00:00Z",
	}
	for k, want := range wantStr {
		if got[k] != want {
			t.Errorf("field %q = %v, want %q", k, got[k], want)
		}
	}
	if got["schema_version"] != float64(1) {
		t.Errorf("schema_version = %v, want 1", got["schema_version"])
	}
}

// TestWebhookPayloadOmitsNodeWhenClusterWide pins the one omitempty field: a cluster-wide alert (no node)
// must not emit a "node" key at all.
func TestWebhookPayloadOmitsNodeWhenClusterWide(t *testing.T) {
	raw, err := json.Marshal(webhookPayloadFor(WebhookEvent{Transition: "cleared", Kind: "below_quorum", DedupKey: "below_quorum"}))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if _, present := got["node"]; present {
		t.Errorf("cluster-wide alert must omit the node key, got %s", raw)
	}
}

// TestWebhookPayloadIsInjectionSafeAndSecretFree is the NEGATIVE assertion: an adversarial/forged alert
// whose every string field embeds a JSON-injection attempt (`","pin":"…`) MUST NOT smuggle extra keys —
// the serialized payload has EXACTLY the public whitelist, so no secret-named field can be forged in.
func TestWebhookPayloadIsInjectionSafeAndSecretFree(t *testing.T) {
	ev := WebhookEvent{
		Transition:    `raised","pin":"1379","account_seed":"SXXX`,
		Kind:          `k","nkey":"UXXX`,
		Severity:      `s","token":"tok`,
		DedupKey:      `d","secret":"leak`,
		Message:       `m","password":"pw","private_key":"pk`,
		Node:          `n","cert":"c`,
		ClusterLeader: `l","seed":"s`,
		Ts:            `t","key":"k`,
	}
	raw, err := json.Marshal(webhookPayloadFor(ev))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("payload not valid JSON under adversarial input: %v", err)
	}
	if len(got) != len(webhookWireKeys) {
		t.Fatalf("JSON-injection smuggled or dropped keys: got %d keys %v, want exactly %d whitelisted", len(got), keysOf(got), len(webhookWireKeys))
	}
	for k := range got {
		if !webhookWireKeys[k] {
			t.Errorf("adversarial input smuggled non-whitelisted key %q", k)
		}
	}
	// No secret-named KEY appears (the crafted tokens are harmlessly escaped INSIDE string VALUES).
	for _, forbidden := range []string{"pin", "token", "secret", "password", "private_key", "nkey", "seed", "account_seed", "cert", "key"} {
		if _, ok := got[forbidden]; ok {
			t.Errorf("forged secret key %q present in payload", forbidden)
		}
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
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
		// A GENUINE handoff: while we are not leader, node-B is the leader (≠ our SelfID). node-B's
		// reconciler owns the deltas, so on our return we must RE-BASELINE, not re-POST.
		SelfID:   "node-A",
		LeaderID: func() string { return "node-B" },
	})
	ctx := context.Background()

	// An alert is already ACTIVE + the leader has a baseline including it.
	seedManualAlert(t, db, "manual:y")
	_ = rec.ReconcileAlertsOnce(ctx) // baseline includes manual:y
	if len(events) != 0 {
		t.Fatalf("baseline pass must not fire, got %d", len(events))
	}

	// Lose leadership to ANOTHER node → the next leader pass must RE-BASELINE (no re-fire of the
	// still-active alert). This is the no-double-fire-on-leadership-change guarantee.
	f.leader = false
	_ = rec.ReconcileAlertsOnce(ctx) // genuine handoff (LeaderID=node-B≠node-A) → resets webhookSeeded
	f.leader = true
	_ = rec.ReconcileAlertsOnce(ctx) // re-seeds baseline (manual:y still active) → NO event
	if len(events) != 0 {
		t.Fatalf("a new leader must NOT re-POST already-active alerts, got %d: %+v", len(events), events)
	}
}

// TestWebhookSurvivesSameNodeLeaseBlip (#93/H13) is the fix proof: a transition that commits while
// the leader is briefly non-leader due to a SAME-NODE raft lease blip (a sub-second step-down /
// re-elect with NO other node winning) must still fire as "raised" on the leader's return — it must
// NOT be silently absorbed into a re-seeded baseline and dropped. Reverting the conditional reset in
// ReconcileAlertsOnce to an unconditional `webhookSeeded = false` makes this test go RED (0 events),
// which is exactly the deploy-tier drill-93 WEBHOOK-raised miss on a saturated host.
func TestWebhookSurvivesSameNodeLeaseBlip(t *testing.T) {
	db := reconTestDB(t)
	f := &alertFake{leader: true, voters: 3}
	var events []WebhookEvent
	leaderID := "node-A"
	rec := NewAlertReconciler(AlertReconcilerConfig{
		Node: f, DB: db, Now: func() time.Time { return time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC) },
		Propose:  func(plan func(*sql.DB) (*cluster.Command, error)) error { return nil },
		Webhook:  func(ev WebhookEvent) { events = append(events, ev) },
		SelfID:   "node-A",
		LeaderID: func() string { return leaderID },
	})
	ctx := context.Background()

	// pass 1: establish an empty baseline as the stable leader.
	_ = rec.ReconcileAlertsOnce(ctx)
	if len(events) != 0 {
		t.Fatalf("baseline pass must not fire, got %d", len(events))
	}

	// The blip: this node briefly steps down. NO other node wins (mid-election), so LeaderID is
	// empty — a same-node lease blip, NOT a handoff. The baseline MUST be preserved.
	f.leader, leaderID = false, ""
	_ = rec.ReconcileAlertsOnce(ctx)

	// A manual alert commits during the blip window (raised by an operator through raft).
	seedManualAlert(t, db, "manual:blip")

	// The node re-wins the SAME term-family election and resumes as leader.
	f.leader, leaderID = true, "node-A"
	_ = rec.ReconcileAlertsOnce(ctx)

	if len(events) != 1 || events[0].Transition != "raised" || events[0].DedupKey != "manual:blip" {
		t.Fatalf("#93/H13 regression: a transition committed during a same-node lease blip was DROPPED "+
			"(a re-seed absorbed it) — got %d events: %+v", len(events), events)
	}
}
