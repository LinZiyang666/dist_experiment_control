package tunnel

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// Round-4 F3: `proxy off` must kill the data plane WITHOUT a DB query. After
// CloseSession(sid), every installed public listener for that session must be
// closed and a fresh REGISTER on the same port must be refused by kill
// generation — even if the broker's allocation-store read later fails.
func TestExternalReviewCloseSessionKillsListenersWithoutDB(t *testing.T) {
	controlPort := findFreePort(t)
	p1 := findFreePort(t)
	p2 := findFreePort(t)

	srv := NewServer(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1",
		func(_, _ string, _ int, _ string, _ int64) error { return nil },
		silentLog(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}

	cli := NewClient(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"lab", "lab-1",
		func(int) (int, error) { return 1, nil },
		silentLog(),
	)
	cli.Start(ctx)
	// Two public ports owned by the same session.
	if err := cli.Open(p1, 1, "tok1"); err != nil {
		t.Fatalf("open p1: %v", err)
	}
	if err := cli.Open(p2, 1, "tok2"); err != nil {
		t.Fatalf("open p2: %v", err)
	}
	// Let both listeners bind.
	if !waitListening(p1, true) || !waitListening(p2, true) {
		t.Fatal("precondition: both public listeners should be up")
	}

	closed := srv.CloseSession("lab")
	if len(closed) != 2 {
		t.Fatalf("CloseSession returned %v, want 2 ports", closed)
	}

	// Both listeners must be gone (no DB was consulted).
	if waitListening(p1, false) == false || waitListening(p2, false) == false {
		t.Fatal("CloseSession did not close both public listeners")
	}

	// A different session is untouched.
	if n := srv.CloseSession("other"); len(n) != 0 {
		t.Fatalf("CloseSession(other) closed %v, want none", n)
	}
}

// waitListening polls until a TCP dial to 127.0.0.1:port matches want (up vs
// down), returning whether it reached that state within the deadline.
func waitListening(port int, wantUp bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		up := err == nil
		if c != nil {
			_ = c.Close()
		}
		if up == wantUp {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
