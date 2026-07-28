package subhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proxysub"
)

// origin: p13_external_review_test.go (renamed in B6) — docs/reviews/p13-external-review.md
func TestExternalReviewServeRejectsNonLoopbackAddress(t *testing.T) {
	// Batch-A A4 retargeted this from Serve() to Bind(). Serve was a two-line
	// wrapper (Bind + ServeListener) with no production caller — this test was
	// its only user — and the loopback refusal it guards happens inside Bind.
	// Testing Bind directly keeps the guarantee while removing the dead wrapper;
	// it is also strictly more precise, since it can no longer pass for the
	// wrong reason (e.g. ServeListener failing first).
	ln, err := Bind("0.0.0.0:0")
	if err == nil {
		_ = ln.Close()
		t.Fatal("subscription HTTP accepted a non-loopback listener")
	}
}

func TestExternalReviewSubscriptionDoesNotRenderNodesWhileProxyOff(t *testing.T) {
	db := newDB(t)
	s, err := proxysub.Create(db, "lab", "alice", "reviewer", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Models an OFF cleanup failure after the switch committed: allocation and
	// stale ready state remain, but the master switch is authoritative.
	seedNode(t, db, "stale-ready", 14000, "ONLINE", 1)

	req := httptest.NewRequest(http.MethodGet, "/sub/"+s.Token, nil)
	w := httptest.NewRecorder()
	Handler(Config{DB: db, PublicHost: "broker.example.com"}).ServeHTTP(w, req)
	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "stale-ready") {
		t.Fatalf("subscription rendered a stale node while the session proxy switch was OFF:\n%s", body)
	}
}
