package subhttp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proxysub"
)

func TestExternalReviewServeRejectsNonLoopbackAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := Serve(ctx, "0.0.0.0:0", Config{DB: newDB(t)})
	if err == nil {
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
