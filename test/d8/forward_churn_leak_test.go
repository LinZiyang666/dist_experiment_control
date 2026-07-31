//go:build d8_integration

package d8_test

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// forward_churn_leak_test.go — the D8 suite's leak gate.
//
// origin: line-2 §12. `assertNoGoroutineLeak` was DEFINED in test/d8/setup_test.go and called from
// nowhere in the package — zero call sites, against 11 in test/concurrency, 1 in test/d4 and 1 in
// test/d5. Two separate review lanes noticed the dangling helper and both wrote it up as "wire it or
// delete it", which framed it as tidying. It is not tidying: the fact underneath is that the
// file-transfer / alert-signal / drain-marker line — the one D8 exists to cover — had NO leak gate at
// all, while CLAUDE.md §5 requires one for anything touching transfers or reconcile.
//
// The helper was there because someone intended this and stopped. Deleting it would have made the gap
// invisible; this file closes it.
//
// WHAT IT MEASURES, AND WHY THE BASELINE IS TAKEN LATE
// ---------------------------------------------------
// Same shape as test/d4's TestD4ForwardChurnNoGoroutineLeak: the cluster, servers and responders are
// all up BEFORE the baseline is taken, so what is measured is the per-request churn (the forwarder
// opens a reply inbox per Request, and a leaked subscription shows up as a leaked fd that a pooled
// goroutine can otherwise mask) rather than the harness's own footprint.
//
// It runs as a TestD8Matrix subtest rather than a top-level test, for CONSISTENCY with the rest of this
// suite — every other d8 assertion is a subtest, and a lone top-level function beside them reads as an
// oversight.
//
// origin: line-2 closure verification §5.4, which corrected the reason this comment used to give. It said
// "the e2e runner invokes this suite with `-run TestD8Matrix`, so a new top-level function here would
// compile, pass locally, and never once be executed by the gate". That is FALSE and measurably so:
// test/e2e/all_phases_test.go spawns `go test -race -count=1 -tags d8_integration -timeout 300s
// ./test/d8/...` with NO -run filter — the `-run TestD8Matrix` a reader might remember applies to the outer
// test/e2e package, not to this one. A top-level function here WOULD be executed.
//
// The wiring was right and the argument for it was wrong, which is the more dangerous combination: the next
// person adding a d8 assertion would have believed a rule about how this suite is invoked that does not
// hold, and would have contorted their test to satisfy it.
func testD8ForwardChurnNoLeak(t *testing.T) {
	c := startD8Cluster(t, 3)
	fwds := wireForwarders(t, c)
	leader := c.leaderIdx(t)

	payload, err := json.Marshal(broker.AlertSignalPayload{
		Kind:     "replication_degraded",
		Node:     "d8-leakgate",
		Active:   true,
		Severity: "warn",
		Message:  "leak gate probe",
	})
	if err != nil {
		t.Fatalf("marshal alert signal: %v", err)
	}

	// Settle, then baseline. GC first so a finished goroutine's stack is actually reclaimed before the
	// count is read; without it the "before" number carries the harness's transients and the gate
	// becomes noise in both directions.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	before := runtime.NumGoroutine()
	fdBefore := testharness.FDCount()

	// 40 round trips through the leader responder. The signal is transition-gated, so all but the first
	// are no-op raft-wise — which is exactly what this measures: the REQUEST/REPLY machinery, not the
	// commit path.
	for i := 0; i < 40; i++ {
		if ferr := fwds[leader].Forward(broker.VerbAlertSignal, "", payload); ferr != nil {
			t.Fatalf("forward alert signal %d: %v", i, ferr)
		}
	}

	// Through the LOCAL wrapper, not testharness directly. setup_test.go's assertNoGoroutineLeak exists
	// precisely so this suite's call sites read the same as d4's and d5's, and its own doc says "the
	// local name stays so this suite's call sites do not churn" — calling past it would have left the
	// helper with zero callers, which is the exact condition that made this file necessary.
	assertNoGoroutineLeak(t, "d8-alert-signal-forward-churn", before)
	// No local fd wrapper exists in this suite (d4 has one), so this goes straight to testharness.
	// Tolerance matches the broker-side gate's: every iteration opens a reply inbox, and TIME_WAIT
	// sockets linger past the assertion window on a loaded box.
	testharness.AssertNoFDLeak(t, "d8-alert-signal-forward-churn", fdBefore, 16)
}
