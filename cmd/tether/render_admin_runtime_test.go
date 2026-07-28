package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/spf13/cobra"
)

// render_admin_runtime_test.go — coverage for renderAdminRuntime's HUMAN table.
//
// WHY THIS FILE EXISTS
// --------------------
// Before it, nothing in the repo referenced renderAdminRuntime, and no test mentioned any of its
// column headers. B7 added two columns to the reconciler table (SLOWEST / OVERRUNS), replaced
// LEADER_ONLY with AUTHORITY, and added an entire second table (CLUSTER_LOOP) — all of it shipped to
// operators with zero exercise. The JSON path has two tests (admin_runtime_test.go); the path an
// operator actually reads had none.
//
// The load-bearing property is not "it prints something". It is that the two tables are separately
// aligned (they have 9 and 5 columns; a single tabwriter block would smear one into the other) and
// that the three values an operator has to TELL APART render differently:
//
//	a periodic loop with 0 iterations  -> a literal 0 under ITERS with a real cadence  (dead)
//	an event-driven loop with 0        -> "-" under CADENCE                            (benign)
//	a loop that never beat             -> "never" under LAST_ITER
//
// Mutation that proves it: drop the `if len(rep.ClusterLoops) > 0` block in admin.go, or change the
// ITERS verb from %d to %s, or reinstate a single shared header row — each turns this red.
func TestRenderAdminRuntimeEmitsBothTablesSeparately(t *testing.T) {
	now := time.Now()
	rep := &adminsock.RuntimeReport{
		Schema: "admin_runtime", SchemaVersion: 1,
		Goroutines: 41, Threads: 8, OpenFDs: 20, RSSBytes: 1 << 24, UptimeSeconds: 3600,
		Reconcilers: []adminsock.ReconcilerTick{
			{Name: "node-states", IntervalMS: 1000, Authority: "any", LastTick: now.Add(-2 * time.Second), Runs: 3600},
			{Name: "home-delivery", IntervalMS: 5000, Authority: "leader", Skips: 12,
				MaxDurMS: 7000, LastDurMS: 20, Overruns: 3, LastErr: "boom"},
		},
		ClusterLoops: []adminsock.ClusterLoopInfo{
			// Periodic and alive.
			// LastIter is *time.Time: nil means "never beat", which is what distinguishes the DEAD row
			// below from this live one. It was a value type when this test was written; the review that
			// added this file also found that `time.Time,omitempty` is inert, so it became a pointer.
			{Name: "observe", StartedAt: now.Add(-time.Hour), CadenceMS: 5000, Iterations: 720,
				LastIter: ptrTime(now.Add(-3 * time.Second))},
			// Periodic and DEAD (topology-reconcile with an empty NatsConfPath does exactly this).
			{Name: "topology-reconcile", StartedAt: now.Add(-time.Hour), CadenceMS: 5000},
			// Event-driven: a zero here means "nothing happened", not "dead".
			{Name: "alert-webhook", StartedAt: now.Add(-time.Hour)},
		},
	}

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	if err := renderAdminRuntime(cmd, rep); err != nil {
		t.Fatalf("render: %v", err)
	}
	got := out.String()

	for _, want := range []string{
		"RECONCILER", "AUTHORITY", "SLOWEST", "OVERRUNS",
		"CLUSTER_LOOP", "CADENCE", "ITERS", "LAST_ITER",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("column %q missing from the operator table:\n%s", want, got)
		}
	}
	// AUTHORITY must carry the WORD. A bool here would mean the CLI is rendering a stale wire shape.
	if !strings.Contains(got, "leader") || !strings.Contains(got, "any") {
		t.Errorf("AUTHORITY must render the named authority, not a bool:\n%s", got)
	}
	// LEADER_ONLY is gone from the wire; it must be gone from the render too.
	if strings.Contains(got, "LEADER_ONLY") {
		t.Errorf("the LEADER_ONLY column outlived the field it rendered:\n%s", got)
	}
	// SLOWEST is the HIGH-WATER mark (7s), not the last duration (20ms) — that is the whole reason the
	// column exists, so an operator can still see a pass that wedged once and recovered.
	if !strings.Contains(got, "7s") {
		t.Errorf("SLOWEST must render MaxDurMS (7s), not LastDurMS:\n%s", got)
	}

	// The two tables must be separately column-aligned. tabwriter terminates a column block on a line
	// with no tab-delimited cells, so the blank line the CLUSTER_LOOP header is prefixed with is what
	// keeps a 5-column table from being padded to a 9-column one. Assert the structure directly.
	lines := strings.Split(got, "\n")
	reconIdx, loopIdx := -1, -1
	for i, l := range lines {
		if strings.HasPrefix(l, "RECONCILER") {
			reconIdx = i
		}
		if strings.HasPrefix(l, "CLUSTER_LOOP") {
			loopIdx = i
		}
	}
	if reconIdx < 0 || loopIdx < 0 {
		t.Fatalf("expected both table headers at the start of a line:\n%s", got)
	}
	if loopIdx <= reconIdx {
		t.Fatalf("CLUSTER_LOOP must follow RECONCILER, got %d then %d", reconIdx, loopIdx)
	}
	if strings.TrimSpace(lines[loopIdx-1]) != "" {
		t.Errorf("the CLUSTER_LOOP header must be preceded by a blank line — that blank line is what "+
			"terminates the reconciler table's tabwriter column block. Without it the 5-column loop "+
			"table gets padded to the 9-column reconciler widths.\nline before header: %q\n%s",
			lines[loopIdx-1], got)
	}

	// The three loop rows must be TELLABLE APART by an operator reading the text.
	loopRows := map[string]string{}
	for _, l := range lines[loopIdx+1:] {
		f := strings.Fields(l)
		if len(f) == 0 {
			continue
		}
		loopRows[f[0]] = l
	}
	dead, ok := loopRows["topology-reconcile"]
	if !ok {
		t.Fatalf("no row for the dead periodic loop:\n%s", got)
	}
	if !strings.Contains(dead, "5s") {
		t.Errorf("a periodic loop must render its declared cadence — a 0 ITERS is only diagnosable "+
			"against one: %q", dead)
	}
	if !strings.Contains(dead, "never") {
		t.Errorf("a loop that never completed an iteration must render LAST_ITER as %q, not a "+
			"zero-time: %q", "never", dead)
	}
	event, ok := loopRows["alert-webhook"]
	if !ok {
		t.Fatalf("no row for the event-driven loop:\n%s", got)
	}
	if strings.Contains(event, "5s") || strings.Contains(event, "ms") {
		t.Errorf("the event-driven poster must NOT render a cadence — CADENCE %q is the only thing "+
			"telling an operator its 0 iterations are benign: %q", "-", event)
	}
	alive, ok := loopRows["observe"]
	if !ok {
		t.Fatalf("no row for the live loop:\n%s", got)
	}
	if !strings.Contains(alive, "720") {
		t.Errorf("a live loop must render its iteration count: %q", alive)
	}
}

// ptrTime is the pointer helper the *time.Time field needs. Go has no address-of-literal, and the
// pointer exists so "never beat" is nil rather than the zero time (which JSON renders as a plausible
// 0001-01-01 timestamp).
func ptrTime(t time.Time) *time.Time { return &t }
