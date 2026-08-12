// Package backoff is the ONE reusable damper for the broker's hot convergence
// loops (h1 E1). It exists because two loops shipped without any: the D5
// audit-publisher retried a failing JetStream stream-create every 100ms for
// five weeks (5.3GB of identical WARN lines — the 2026-08-04 incident's log
// half), and the proxy reaper rotated a dead node's __proxy__ allocation
// every 20s forever (24k leaked rows — the incident's storage half).
//
// Two orthogonal concerns, one type:
//
//   - ATTEMPT pacing: Due(now) says whether the owner may try again;
//     Fail(...) schedules the next attempt at Base·2ⁿ capped at Cap.
//   - LOG discipline: Fail returns LogNow=true only on the FIRST failure of a
//     run and on an error-CLASS change; Recover reports how many identical
//     failures were suppressed so the recovery line carries the count.
//     Classes are caller-chosen coarse buckets (an enum-ish short string),
//     NEVER err.Error() — free-text classes would defeat suppression on any
//     variable detail (a port number, a byte count).
//
// A Tracker is NOT goroutine-safe BY DESIGN: every owner in this repo is a
// single-goroutine loop (the reaper tick, the publisher tick). Owners pin
// that in a comment at the field site.
package backoff

import "time"

// Policy is the schedule: first retry after Base, doubling to at most Cap.
type Policy struct {
	Base time.Duration
	Cap  time.Duration
}

// Tracker carries one failure-run's state. The zero value (with a Policy set
// via New) is "healthy": Due is true, nothing suppressed.
type Tracker struct {
	pol        Policy
	fails      int       // consecutive Fail calls in this run
	class      string    // error class of the current run
	next       time.Time // earliest next attempt
	runStarted time.Time // first Fail of this run (Recover's min-hold floor)
	suppressed int       // failures since the last logged line
}

// New returns a healthy Tracker with the given policy.
func New(pol Policy) *Tracker {
	return &Tracker{pol: pol}
}

// Due reports whether the owner may attempt now. A healthy tracker is always
// due; a failing one is due once the scheduled backoff instant has passed.
func (t *Tracker) Due(now time.Time) bool {
	return t.fails == 0 || !now.Before(t.next)
}

// Fail records one failed attempt of the given class and returns whether the
// owner should LOG it now. The schedule doubles per consecutive failure
// (Base·2ⁿ, capped); the log discipline fires on the first failure of a run
// and whenever the class changes mid-run (a NEW problem must not hide behind
// an old one's suppression).
func (t *Tracker) Fail(now time.Time, class string) (logNow bool) {
	logNow = t.fails == 0 || class != t.class
	if t.fails == 0 {
		t.runStarted = now
	}
	if logNow {
		t.suppressed = 0
	} else {
		t.suppressed++
	}
	t.class = class
	delay := t.pol.Base << uint(min(t.fails, 30)) // shift-guard: 2³⁰ already dwarfs any Cap
	if delay > t.pol.Cap || delay <= 0 {
		delay = t.pol.Cap
	}
	t.next = now.Add(delay)
	t.fails++
	return logNow
}

// FailReaffirm is Fail plus a time-based reaffirmation, for a damper that
// wants to re-log a still-active run once per Cap window rather than staying
// silent for the whole process lifetime (the REGISTER-read damper, #78 / g75-g78
// external review F4). It returns:
//
//   - logNow: true on the first failure of a run, on a class change, OR when a
//     run's backoff instant has passed (a reaffirmation).
//   - suppressed: how many failures were swallowed since the last logged line —
//     the count the owner prints. It is RESET to 0 whenever logNow is true (the
//     logged line is itself NOT a suppressed event, so the next line must not
//     re-report it). This is the primitive the external review asked for: the
//     owner must NOT hand-cobble Due+Fail, which double-counts the reaffirmation.
//
// The schedule advances exactly as Fail's, so consecutive reaffirmations stay
// one-per-Cap.
func (t *Tracker) FailReaffirm(now time.Time, class string) (logNow bool, suppressed int) {
	reaffirm := t.fails > 0 && !now.Before(t.next)
	// Fail resets suppressed to 0 on its own logNow (first-of-run / class
	// change); we only need to capture-and-reset for the reaffirmation case,
	// where Fail's logNow is false but we still log.
	before := t.suppressed
	logNow = t.Fail(now, class)
	switch {
	case logNow:
		// Fail already zeroed t.suppressed; report what it zeroed (0 on a
		// class change since the prior run's count belongs to the prior class,
		// which Fail's own logNow handled — here `before` is the run's tail).
		return true, before
	case reaffirm:
		// A reaffirmation: report the genuinely-suppressed repeats accumulated
		// before this line, then reset so the NEXT line starts from zero.
		t.suppressed = 0
		return true, before
	default:
		return false, 0
	}
}

// Recover ends a failure run. ok=true means the owner should log the
// recovery (with Suppressed()'s count); ok=false folds a blip into the run —
// the anti-flap floor (h1 plan, critique-4): a run that lasted less than one
// Base is oscillation, not recovery, and logging every flip would recreate
// the log storm this package exists to end. A folded blip does NOT reset the
// schedule; the caller keeps failing/decaying as it was.
func (t *Tracker) Recover(now time.Time) (suppressed int, ok bool) {
	if t.fails == 0 {
		return 0, false
	}
	if now.Sub(t.runStarted) < t.pol.Base {
		return 0, false
	}
	suppressed = t.suppressed
	*t = Tracker{pol: t.pol}
	return suppressed, true
}

// Decay steps the schedule DOWN one notch instead of resetting it — the
// treatment for a short healthy blip inside a longer failure run (h1 E2
// clear-side hysteresis: a link that heals for 30s and breaks again must not
// restart the reaper at full 20s cadence).
func (t *Tracker) Decay() {
	if t.fails > 1 {
		t.fails--
	}
}

// Failing reports whether a failure run is in progress.
func (t *Tracker) Failing() bool { return t.fails > 0 }

// Fails reports the consecutive-failure count of the current run.
func (t *Tracker) Fails() int { return t.fails }

// Suppressed reports how many failures were swallowed since the last logged
// line of the current run.
func (t *Tracker) Suppressed() int { return t.suppressed }
