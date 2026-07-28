package adminsock

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestDispatchRuntimeWiresBackend: OpRuntime routes to the RuntimeSnapshot backend hook and returns
// its report; a Backend that did NOT wire the hook replies with a stable error (never a panic / a
// fabricated empty snapshot).
func TestDispatchRuntimeWiresBackend(t *testing.T) {
	// nil hook → unavailable.
	s := New("/unused", Backend{})
	if resp := s.dispatch(Request{Op: OpRuntime}); resp.Error == "" || resp.Code != CodeBadRequest {
		t.Errorf("nil RuntimeSnapshot should error with bad_request, got %+v", resp)
	}

	// hook that returns nil → unavailable (not OK-with-empty).
	sNil := New("/unused", Backend{RuntimeSnapshot: func() *RuntimeReport { return nil }})
	if resp := sNil.dispatch(Request{Op: OpRuntime}); resp.OK || resp.Error == "" {
		t.Errorf("nil report should error, got %+v", resp)
	}

	// wired hook → the report is returned verbatim.
	want := &RuntimeReport{
		Schema: "admin_runtime", SchemaVersion: 2,
		Goroutines: 7, Threads: 3, OpenFDs: 12, RSSBytes: 4096, UptimeSeconds: 1.5,
		Reconcilers: []ReconcilerTick{{Name: "ports", IntervalMS: 1000, Authority: "leader", LastTick: time.Unix(100, 0), Runs: 4}},
	}
	called := 0
	sOK := New("/unused", Backend{RuntimeSnapshot: func() *RuntimeReport { called++; return want }})
	resp := sOK.dispatch(Request{Op: OpRuntime})
	if !resp.OK || resp.Error != "" || resp.Runtime == nil {
		t.Fatalf("wired hook: got %+v", resp)
	}
	if called != 1 {
		t.Errorf("RuntimeSnapshot called %d times, want exactly 1", called)
	}
	if resp.Runtime.Goroutines != 7 || resp.Runtime.Threads != 3 || resp.Runtime.OpenFDs != 12 {
		t.Errorf("report not passed through: %+v", resp.Runtime)
	}
	if len(resp.Runtime.Reconcilers) != 1 || resp.Runtime.Reconcilers[0].Name != "ports" {
		t.Errorf("reconcilers not passed through: %+v", resp.Runtime.Reconcilers)
	}
}

// TestRuntimeReportRoundTripsOnWire: the RuntimeReport survives JSON marshal/unmarshal (the admin
// socket is line-delimited JSON), including the reconciler last-tick timestamp.
func TestRuntimeReportRoundTripsOnWire(t *testing.T) {
	in := Response{Op: OpRuntime, OK: true, Runtime: &RuntimeReport{
		Schema: "admin_runtime", SchemaVersion: 2,
		Goroutines: 41, Threads: 8, OpenFDs: 20, RSSBytes: 1 << 24, UptimeSeconds: 3600,
		Reconcilers: []ReconcilerTick{
			{Name: "node-states", IntervalMS: 1000, Authority: "any", LastTick: time.Unix(1700, 0).UTC(), Runs: 3600, Skips: 0},
			{Name: "home-delivery", IntervalMS: 5000, Authority: "leader", Runs: 0, Skips: 12, LastErr: "boom"},
		},
	}}
	b, err := json.Marshal(&in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"schema":"admin_runtime"`) || !strings.Contains(string(b), `"goroutines":41`) {
		t.Errorf("wire form missing fields: %s", b)
	}
	var out Response
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Runtime == nil || out.Runtime.Goroutines != 41 || out.Runtime.Threads != 8 {
		t.Fatalf("decode lost fields: %+v", out.Runtime)
	}
	if len(out.Runtime.Reconcilers) != 2 {
		t.Fatalf("decode lost reconcilers: %+v", out.Runtime.Reconcilers)
	}
	if !out.Runtime.Reconcilers[0].LastTick.Equal(time.Unix(1700, 0)) {
		t.Errorf("last_tick did not round-trip: %v", out.Runtime.Reconcilers[0].LastTick)
	}
	if out.Runtime.Reconcilers[1].LastErr != "boom" {
		t.Errorf("last_err did not round-trip: %q", out.Runtime.Reconcilers[1].LastErr)
	}
}
