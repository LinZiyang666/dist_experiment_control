package testharness

import (
	"os"
	"runtime"
	"testing"
	"time"
)

// leakgate.go — the repo's goroutine + fd leak gate, in ONE place (B9).
//
// WHY THIS FILE EXISTS
// --------------------
// CLAUDE.md §5 makes the in-repo NumGoroutine + fd baseline a hard gate for anything touching
// tunnels, PTYs, reconcile, transports or Raft, and deliberately does NOT use goleak. That gate was
// implemented FOUR separate times — test/d4, test/d5, test/d8 and test/concurrency each carried its own
// copy — and the copies had drifted only cosmetically (a local variable named `n` in two of them and `nb`
// in the other two, one comment block present in one). Algorithm, poll count, tolerance, interval and
// stack-dump size were identical in all four, so collapsing them loses nothing.
//
// It is worth having one copy for a reason beyond tidiness: the tolerance and the poll window are
// DERIVED numbers, and a derivation that lives in four places is a derivation that will be tuned in one.
//
// WHY NOT goleak (restating the decision so it is not re-litigated)
// ----------------------------------------------------------------
// goleak compares goroutine STACKS against an ignore list. It has no opinion on how many times a subject
// was exercised, which is the defect this batch actually fixed (a leak assertion that opened ONE tunnel
// session could not see a per-session leak against any usable tolerance). It also has no fd axis, and a
// leaked NATS reply-inbox subscription is an fd leak that a POOLED goroutine masks — which is precisely
// why the fd half exists. Adding a dependency that solves neither is a net loss.

// GoroutineLeakTolerance is the noise floor for a goroutine-count delta.
//
// The Go runtime keeps a handful of helper goroutines that come and go independently of anything under
// test (GC workers, the scavenger, dial-retry timers), so a delta of one or two is not evidence. Below
// this number the gate produces false positives; above it, real leaks hide.
//
// IT IS NOT A KNOB TO TURN WHEN AN ASSERTION FAILS. The correct response to "my leak test is flaky at
// ±2" is to exercise the subject more times, not to widen the floor: at N exercises a per-exercise leak
// of 1 shows up as +N. That is what LeakExerciseRounds below is for, and
// test/determinism/leak_assert_shape_test.go is what makes it a property of the repo rather than a claim
// in a comment — an earlier draft of this file asserted the repo already complied, and six of twelve call
// sites did not.
const GoroutineLeakTolerance = 2

// LeakExerciseRounds is how many times a leak assertion must exercise its subject.
//
// Five, because the tolerance is two: at five exercises a per-exercise leak of one shows up as +5, which
// clears the noise floor with margin, while the cost is five cheap setup/teardown cycles rather than one.
// Enforced by test/determinism's leak_assert_shape_test.go, which fails any leak assertion whose test has
// no loop and no recorded exemption.
const LeakExerciseRounds = 5

// goroutineLeakPollAttempts / goroutineLeakPollInterval bound how long the gate waits for teardown.
//
// Post-cancel teardown has fan-in latency: sub.Unsubscribe drains asynchronously, nc.Drain spawns a
// flusher, and deferred chains run after Run returns. The question the gate asks is "did these
// goroutines exit AT ALL", not "had they exited by the next nanosecond" — so it polls for ~1s and
// succeeds on the first sample at or below the floor.
const (
	goroutineLeakPollAttempts = 50
	goroutineLeakPollInterval = 20 * time.Millisecond
)

// AssertNoGoroutineLeak compares the live goroutine count to a baseline taken before the subject ran.
//
// On failure it dumps every goroutine's stack, because "+7" alone does not tell you WHICH ones stayed;
// 64KiB has been enough for this project's goroutine inventory throughout.
func AssertNoGoroutineLeak(t *testing.T, label string, before int) {
	t.Helper()
	var last int
	for i := 0; i < goroutineLeakPollAttempts; i++ {
		runtime.Gosched()
		last = runtime.NumGoroutine()
		if last-before <= GoroutineLeakTolerance {
			return
		}
		time.Sleep(goroutineLeakPollInterval)
	}
	buf := make([]byte, 64*1024)
	n := runtime.Stack(buf, true)
	t.Errorf("%s: goroutine leak: before=%d after=%d (+%d)\n%s",
		label, before, last, last-before, buf[:n])
}

// FDCount returns this process's open file-descriptor count, or -1 where it cannot be measured (any
// platform without /proc/self/fd).
//
// The fd axis is not redundant with the goroutine axis: a leaked NATS reply-inbox subscription holds a
// descriptor, and if the goroutine that owned it came from a pool the goroutine count returns to
// baseline while the fd does not. test/d4's copy of this helper carried that explanation and it is
// carried here verbatim in substance rather than dropped as a duplicate comment.
// FDCountUnmeasurable is what FDCount returns on a platform with no /proc/self/fd.
//
// FDCountFailed is what it returns when the platform HAS one and the read still failed —
// which on Linux is overwhelmingly "out of file descriptors", because ReadDir needs one.
// The two must not be the same value: the first means "cannot judge", the second means
// "the thing this gate exists to detect is happening right now".
const (
	FDCountUnmeasurable = -1
	FDCountFailed       = -2
)

func FDCount() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		// origin: prerelease audit, §3 MINOR sweep. This returned -1 for BOTH cases and
		// AssertNoFDLeak treated any negative as "unmeasurable, pass" — so the gate
		// reported PASS in the one condition it was built to catch. Opening a directory
		// requires a descriptor; a process that has run out cannot read /proc/self/fd.
		if _, serr := os.Stat("/proc/self"); serr == nil {
			return FDCountFailed
		}
		return FDCountUnmeasurable
	}
	return len(entries)
}

// AssertNoFDLeak compares the live fd count to a baseline, with a CALLER-SUPPLIED tolerance.
//
// The tolerance is a parameter and stays one, permanently. Three different numbers are in use and each
// has a written derivation: the tunnel and storage suites use a small floor derived from the netpoller
// plus TIME_WAIT sockets, while the broker suite needs a larger one because every iteration opens a
// fresh NATS connection. Collapsing them to a single constant would either loosen the tight gates to the
// broker's number or make the broker gate flake at the tight one.
//
// A negative baseline means the platform cannot measure fds; the assertion then no-ops rather than
// failing, so a suite that runs on both Linux and macOS keeps one code path.
func AssertNoFDLeak(t *testing.T, label string, before, tolerance int) {
	t.Helper()
	if before == FDCountFailed {
		t.Errorf("%s: could not count file descriptors even though /proc/self exists — the most "+
			"likely cause is that the process is OUT of them, which is the condition this gate "+
			"exists to detect", label)
		return
	}
	if before < 0 {
		return // unmeasurable on this platform
	}
	var last int
	for i := 0; i < goroutineLeakPollAttempts; i++ {
		last = FDCount()
		if last == FDCountFailed {
			t.Errorf("%s: file-descriptor count became unreadable mid-test (before=%d). On Linux "+
				"that means the process ran out of descriptors — a PASS here would be the gate "+
				"reporting success at the exact moment it should fire", label, before)
			return
		}
		if last == FDCountUnmeasurable || last-before <= tolerance {
			return
		}
		time.Sleep(goroutineLeakPollInterval)
	}
	t.Errorf("%s: fd leak: before=%d after=%d (+%d, tolerance %d)", label, before, last, last-before, tolerance)
}
