package testharness

import (
	"runtime"
	"testing"
)

// leakgate_test.go — the leak gate's own behaviour.
//
// origin: prerelease audit round 2, CC-2.
//
// internal/testharness/leakgate.go is the shared fd/goroutine gate EVERY suite in this
// repo calls, and the §3 MINOR sweep changed its most important branch — the one that
// turns "the process is out of descriptors" from a silent PASS into a failure — with no
// test of any kind. A gate that is itself ungated is the worst place in a test tree for a
// regression to hide: it does not fail, it stops failing.

// origin: prerelease audit round 2, CC-2.
//
// "CANNOT MEASURE" AND "MEASURING FAILED" MUST NOT BE THE SAME VALUE.
//
// FDCount used to return -1 for both, and AssertNoFDLeak treats any negative baseline as
// "unmeasurable, no-op" — so on Linux, where the read fails overwhelmingly because the
// process has run out of descriptors, the gate reported PASS in the exact condition it
// was built to catch. Opening a directory needs a descriptor; a process with none cannot
// read /proc/self/fd.
func TestTheFdCounterSeparatesUnmeasurableFromFailed(t *testing.T) {
	if FDCountUnmeasurable == FDCountFailed {
		t.Fatal("FDCountUnmeasurable and FDCountFailed are the same value.\n\n" +
			"AssertNoFDLeak no-ops on 'unmeasurable' and FAILS on 'failed'. Collapsing them " +
			"restores the original defect: on Linux the read fails because the process is out " +
			"of descriptors, and the gate answers PASS.")
	}
	if FDCountUnmeasurable >= 0 || FDCountFailed >= 0 {
		t.Fatalf("both sentinels must be negative so a real count can never collide with them "+
			"(unmeasurable=%d failed=%d)", FDCountUnmeasurable, FDCountFailed)
	}
	if runtime.GOOS != "linux" {
		t.Skipf("no /proc/self/fd on %s; the live counter cannot be exercised here", runtime.GOOS)
	}
	if n := FDCount(); n < 0 {
		t.Fatalf("FDCount() = %d on Linux, where /proc/self/fd exists and this process plainly "+
			"has descriptors open — the counter has gone blind, and every fd assertion in the "+
			"repo silently no-ops", n)
	}
}

// origin: prerelease audit round 2, CC-2.
//
// AssertNoFDLeak must FAIL on a FDCountFailed baseline rather than no-op. The two live
// one line apart in the same function and the ordering is what makes the sentinel worth
// having: `if before < 0 { return }` placed first would swallow it.
//
// Driven through a synthetic *testing.T, because the property under test is "this
// assertion fails", which cannot be observed by letting it fail the real one.
func TestAssertNoFDLeakFailsOnAnUnreadableBaseline(t *testing.T) {
	spy := &testing.T{}
	AssertNoFDLeak(spy, "cc-2", FDCountFailed, 0)
	if !spy.Failed() {
		t.Fatal("AssertNoFDLeak PASSED with a baseline of FDCountFailed.\n\n" +
			"On Linux that baseline means the process could not read /proc/self/fd, whose most " +
			"likely cause is that it has no descriptors left — which is precisely the condition " +
			"this gate exists to detect. Passing there is the gate reporting success at the " +
			"moment it should fire.")
	}

	// The other sentinel must still no-op, or a macOS run turns into a suite-wide failure.
	quiet := &testing.T{}
	AssertNoFDLeak(quiet, "cc-2", FDCountUnmeasurable, 0)
	if quiet.Failed() {
		t.Error("AssertNoFDLeak failed on an UNMEASURABLE baseline; a platform with no " +
			"/proc/self/fd must no-op so one code path serves Linux and macOS")
	}
}
