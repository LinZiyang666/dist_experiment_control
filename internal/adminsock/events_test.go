package adminsock

import (
	"context"
	"testing"
	"time"
)

// events_test.go (#30) — the OpEvents dispatch contract: it routes to the EventsTail backend hook,
// passes the parsed filters through, defaults N, validates --since, and errors cleanly when the hook
// is not wired (the anti-half-wire proof at the server layer).

func TestDispatchEventsWiresBackend(t *testing.T) {
	// nil hook → unavailable (never a panic or a fabricated empty OK).
	s := New("/unused", Backend{})
	if resp := s.dispatch(Request{Op: OpEvents}); resp.OK || resp.Error == "" {
		t.Errorf("nil EventsTail must error, got %+v", resp)
	}

	// wired hook → filters are parsed + passed through, N defaults to 50.
	var gotN int
	var gotSince time.Duration
	var gotKind string
	sOK := New("/unused", Backend{EventsTail: func(_ context.Context, n int, since time.Duration, kind string) ([]AuditEntry, bool, error) {
		gotN, gotSince, gotKind = n, since, kind
		return []AuditEntry{{Subject: "tether.v2.sys.events", Seq: 9, Body: map[string]any{"type": kind}}}, true, nil
	}})

	resp := sOK.dispatch(Request{Op: OpEvents, Since: "90m", EventKind: "disk_pressure"})
	if !resp.OK || resp.Error != "" || len(resp.Events) != 1 {
		t.Fatalf("wired hook: got %+v", resp)
	}
	// N-5: the truncated flag from the hook must propagate onto the Response envelope.
	if !resp.Truncated {
		t.Errorf("EventsTail truncated=true must propagate to Response.Truncated, got %+v", resp)
	}
	if gotN != 50 {
		t.Errorf("N should default to 50, got %d", gotN)
	}
	if gotSince != 90*time.Minute {
		t.Errorf("since not parsed/passed: got %v want 90m", gotSince)
	}
	if gotKind != "disk_pressure" {
		t.Errorf("kind not passed: got %q", gotKind)
	}
	if resp.Events[0].Seq != 9 {
		t.Errorf("events not passed through: %+v", resp.Events)
	}
}

func TestDispatchEventsRejectsBadSince(t *testing.T) {
	called := false
	s := New("/unused", Backend{EventsTail: func(context.Context, int, time.Duration, string) ([]AuditEntry, bool, error) {
		called = true
		return nil, false, nil
	}})
	resp := s.dispatch(Request{Op: OpEvents, Since: "junk"})
	if resp.OK || resp.Code != CodeBadRequest {
		t.Errorf("a malformed --since must be bad_request, got %+v", resp)
	}
	if called {
		t.Error("EventsTail must NOT be invoked when --since fails to parse")
	}
	// A negative window is also rejected (would otherwise silently mean 'no bound').
	if resp := s.dispatch(Request{Op: OpEvents, Since: "-1h"}); resp.OK || resp.Code != CodeBadRequest {
		t.Errorf("a negative --since must be bad_request, got %+v", resp)
	}
}
