package broker

import (
	"testing"
	"time"
)

// #3 dwell state machine: unobserved/has-contact => not quorum-lost; stale<dwell => lost+remaining;
// stale>=dwell => lost+0; contact-returns resets.
func TestForceSingleArmDwell(t *testing.T) {
	a := newForceSingleArm()
	a.dwell = 10 * time.Second
	t0 := time.Unix(1700000000, 0)

	if rem, lost := a.dwellState(t0); lost {
		t.Fatalf("unobserved must be NOT quorum-lost; rem=%v lost=%v", rem, lost)
	}
	a.observeLeadership(true, t0) // first stale observation
	if rem, lost := a.dwellState(t0.Add(3 * time.Second)); !lost || rem <= 0 {
		t.Fatalf("stale<dwell must be lost with remaining>0; rem=%v lost=%v", rem, lost)
	}
	if rem, lost := a.dwellState(t0.Add(10 * time.Second)); !lost || rem != 0 {
		t.Fatalf("stale>=dwell must be lost with remaining==0; rem=%v lost=%v", rem, lost)
	}
	a.observeLeadership(false, t0.Add(11*time.Second)) // contact returns
	if _, lost := a.dwellState(t0.Add(12 * time.Second)); lost {
		t.Fatal("contact-returns must reset to NOT quorum-lost")
	}
}

// #12 arm token: a lone/expired/wrong/replayed token is refused; a fresh valid token is single-shot.
func TestForceSingleArmToken(t *testing.T) {
	a := newForceSingleArm()
	t0 := time.Unix(1700000000, 0)

	if a.consume("anything", t0) {
		t.Fatal("a lone commit (no prior arm) must be refused")
	}
	tok, err := a.mint(t0, 60*time.Second)
	if err != nil || tok == "" {
		t.Fatalf("mint returned empty token / error: %v", err)
	}
	if a.consume("wrong-token", t0) {
		t.Fatal("a wrong token must be refused")
	}
	// a failed consume clears the slot (anti-linger) — re-mint to continue.
	tok, err = a.mint(t0, 60*time.Second)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if a.consume(tok, t0.Add(61*time.Second)) {
		t.Fatal("an expired token must be refused")
	}
	tok, err = a.mint(t0, 60*time.Second)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if !a.consume(tok, t0.Add(30*time.Second)) {
		t.Fatal("a fresh valid token must be accepted")
	}
	if a.consume(tok, t0.Add(31*time.Second)) {
		t.Fatal("a replayed token must be refused (single-shot)")
	}
}
