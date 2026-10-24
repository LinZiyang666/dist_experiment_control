package xferworker

import (
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// foreignManager is a manager this package did not build.
type foreignManager struct{ x.WorkerManager }

// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (transfer census).
// The S8 census must name every entry and every retained consumer-cleanup
// duty by its own identity, settled or not, in a stable order; it must answer
// while an entry's step or a cut holds its tokens (a parked byte actor cannot
// stall a describe), list exactly the frozen image's entries during a cut, and
// refuse a manager whose registry it cannot read instead of reporting none.
func TestWorkerCensusNamesEveryRetainedEntry(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	if entries, cleanup, ok := Census(o); !ok || len(entries) != 0 || len(cleanup) != 0 {
		t.Fatalf("an empty worker was not an empty, readable census: %v %v %v", entries, cleanup, ok)
	}
	beta, alpha := f.prepare("b", "beta"), f.prepare("b", "alpha")
	for _, p := range []x.Prepare{beta, alpha} {
		if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	o.mu.Lock()
	o.entries[key(beta.Identity)].value.Status.Phase = x.PhaseSettled
	o.mu.Unlock()
	duty, err := jsstream.ImportConsumerCleanup(jsstream.ConsumerCleanupState{OwnerID: "censuscleanupduty"})
	if err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	o.cleanup = append(o.cleanup, duty)
	o.mu.Unlock()
	t.Cleanup(func() {
		o.mu.Lock()
		o.cleanup = nil // the imported duty has no server to bind on Stop
		o.mu.Unlock()
	})
	want := []CensusEntry{{Identity: alpha.Identity}, {Identity: beta.Identity, Settled: true}}
	check := func(stage string) {
		t.Helper()
		done := make(chan struct{})
		var entries []CensusEntry
		var cleanup []string
		var ok bool
		go func() { defer close(done); entries, cleanup, ok = Census(o) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: census waited behind a held token", stage)
		}
		if !ok || !reflect.DeepEqual(entries, want) || !reflect.DeepEqual(cleanup, []string{duty.ID()}) {
			t.Fatalf("%s: census = %+v %v %v, want %+v and the cleanup duty", stage, entries, cleanup, ok, want)
		}
	}
	check("idle")
	// A running step holds its entry token and a cut holds the lifecycle
	// token; neither may stall the census.
	e := o.entries[key(alpha.Identity)]
	e.token <- struct{}{}
	o.lifecycle <- struct{}{}
	check("tokens held")
	<-o.lifecycle
	<-e.token
	_, state := exportWorker(t, o)
	check("frozen")
	if len(state.Entries) != len(want) || len(state.ConsumerCleanup) != 1 {
		t.Fatalf("frozen image carries %d entries and %d duties; census named %d and 1", len(state.Entries), len(state.ConsumerCleanup), len(want))
	}
	// The imported duty has no server to rebind; drop it before the thaw.
	o.mu.Lock()
	o.cleanup = nil
	o.mu.Unlock()
	if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	for name, m := range map[string]Manager{"nil": nil, "foreign": foreignManager{}} {
		if entries, cleanup, ok := Census(m); ok || entries != nil || cleanup != nil {
			t.Fatalf("%s manager read as an enumerable census: %v %v", name, entries, cleanup)
		}
	}
}
