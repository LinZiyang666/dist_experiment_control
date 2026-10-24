//go:build linux

// Real: descriptor duplication and cleanup after a failed preparation.
// Stubbed: no broker, NATS, or daemon coordinator; no exec continuity claim.
// Keep leak helpers outside handoff's in-package tests: testharness reaches
// handoff through auth/proto/xfervalue, so an internal import forms a cycle.
// origin: S8 internal review FIX-VETTAGS F1
package handoff_test

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func snapshotState() handoff.State {
	return handoff.State{Role: "agent-runtime", Instance: "test-instance", Generation: 2, Data: json.RawMessage(`{"tasks":1}`)}
}

// origin: S8 internal review FIX-VETTAGS F1
// origin: docs/reviews/hot-update-plan.md §4.1.
func TestPreparePartialFailureReleasesDescriptorCopies(t *testing.T) {
	good, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = good.Close() }()
	closed, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	resources := []handoff.Resource{
		{ID: "first", Kind: "file", Files: map[string]*os.File{"f": good}},
		{ID: "second", Kind: "file", Files: map[string]*os.File{"f": closed}},
	}
	fds, goroutines := testharness.FDCount(), runtime.NumGoroutine()
	// The structural leak gate resolves literal bounds; five exercises exceed
	// its two-goroutine noise allowance without adding a ledger exception.
	for i := 0; i < 5; i++ {
		p, err := handoff.Prepare(snapshotState(), resources)
		if p != nil {
			_ = p.Close()
		}
		if err == nil || p != nil {
			t.Fatalf("closed second resource accepted: (%v, %v)", p, err)
		}
	}
	testharness.AssertNoFDLeak(t, "partially duplicated snapshot", fds, 0)
	testharness.AssertNoGoroutineLeak(t, "partially duplicated snapshot", goroutines)
	if _, err := good.Stat(); err != nil {
		t.Fatalf("failed Prepare consumed caller file: %v", err)
	}
}
