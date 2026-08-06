package backoff

// origin: docs/reviews/h1-plan.md workstream E1 (2026-08-04 incident: two
// undamped hot loops — 100ms WARN spam to 5.3GB, 20s rotation churn to 24k
// rows).

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)

func TestTrackerSchedule(t *testing.T) {
	tr := New(Policy{Base: 20 * time.Second, Cap: 10 * time.Minute})

	// Healthy: always due.
	if !tr.Due(t0) {
		t.Fatal("healthy tracker must be due")
	}

	// The doubling ladder: 20s, 40s, 80s, ..., capped at 10min.
	wantDelays := []time.Duration{
		20 * time.Second, 40 * time.Second, 80 * time.Second,
		160 * time.Second, 320 * time.Second, 600 * time.Second, // 640 > cap → 600
		600 * time.Second,
	}
	now := t0
	for i, want := range wantDelays {
		tr.Fail(now, "dial")
		if tr.Due(now.Add(want - time.Millisecond)) {
			t.Fatalf("step %d: due %v after fail, want to wait %v", i, want-time.Millisecond, want)
		}
		if !tr.Due(now.Add(want)) {
			t.Fatalf("step %d: not due at scheduled instant %v", i, want)
		}
		now = now.Add(want)
	}
}

func TestTrackerLogDiscipline(t *testing.T) {
	tr := New(Policy{Base: time.Second, Cap: time.Minute})

	if !tr.Fail(t0, "dial") {
		t.Fatal("first failure of a run must log")
	}
	for i := 0; i < 5; i++ {
		if tr.Fail(t0.Add(time.Duration(i)*time.Second), "dial") {
			t.Fatalf("repeat failure %d of the same class must be suppressed", i)
		}
	}
	if tr.Suppressed() != 5 {
		t.Fatalf("suppressed=%d, want 5", tr.Suppressed())
	}
	// A class CHANGE mid-run must log — a new problem must not hide behind an
	// old problem's suppression window.
	if !tr.Fail(t0.Add(10*time.Second), "storage") {
		t.Fatal("class change must log")
	}
	if tr.Suppressed() != 0 {
		t.Fatalf("class change must reset suppression counter, got %d", tr.Suppressed())
	}

	sup, ok := tr.Recover(t0.Add(20 * time.Second))
	if !ok {
		t.Fatal("recovery after a long run must report")
	}
	if sup != 0 {
		t.Fatalf("suppressed-at-recovery=%d, want 0 (class change just logged)", sup)
	}
	if tr.Failing() || !tr.Due(t0) {
		t.Fatal("recovered tracker must be healthy")
	}
}

// TestTrackerRecoverMinHoldFloor is the anti-flap floor (h1 plan critique-4):
// a failure run shorter than one Base is oscillation — Recover must fold it
// (no log, schedule kept) instead of resetting, or a 100ms-granularity
// flapping condition would emit a Warn/Info pair per flip.
func TestTrackerRecoverMinHoldFloor(t *testing.T) {
	tr := New(Policy{Base: 20 * time.Second, Cap: 10 * time.Minute})
	tr.Fail(t0, "dial")
	if _, ok := tr.Recover(t0.Add(5 * time.Second)); ok {
		t.Fatal("recovery inside the Base window must be folded (anti-flap floor)")
	}
	if !tr.Failing() {
		t.Fatal("folded blip must keep the failure run alive")
	}
	if _, ok := tr.Recover(t0.Add(25 * time.Second)); !ok {
		t.Fatal("recovery past the Base window must report")
	}
}

func TestTrackerDecaySteps(t *testing.T) {
	tr := New(Policy{Base: 20 * time.Second, Cap: 10 * time.Minute})
	now := t0
	for i := 0; i < 4; i++ { // fails=4 → next delay would be 320s
		tr.Fail(now, "dial")
		now = now.Add(time.Minute)
	}
	tr.Decay() // fails 4→3: next Fail schedules Base·2³ = 160s
	tr.Fail(now, "dial")
	if tr.Due(now.Add(160*time.Second - time.Millisecond)) {
		t.Fatal("decay must step down ONE notch, not reset")
	}
	if !tr.Due(now.Add(160 * time.Second)) {
		t.Fatal("decayed schedule must still be due at the stepped-down instant")
	}
	// Decay never drops below one notch (fails=1 keeps Base).
	tr2 := New(Policy{Base: 20 * time.Second, Cap: time.Minute})
	tr2.Fail(t0, "dial")
	tr2.Decay()
	if tr2.Fails() != 1 {
		t.Fatalf("decay below fails=1: got %d", tr2.Fails())
	}
}
