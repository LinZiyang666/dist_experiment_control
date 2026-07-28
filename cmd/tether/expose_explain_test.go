package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: b4_expose_explain_test.go (renamed in B6) — docs/reviews/b4-plan.md, docs/reviews/b4-review.md
//
// TestB4PsPortsByteStableSingleBroker (plan §F.20): with every port un-homed (single broker), the
// PORTS table has NO HOME column — byte-identical to pre-B4. A clustered set (some homed) adds the
// HOME column, "(moved)" for epoch>0, and "-" for un-homed rows.
func TestB4PsPortsByteStableSingleBroker(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	single := []proto.PsPortEntry{
		{Port: 14000, Name: "jup", NID: "a100", LocalPort: 8888, State: "ALLOCATED", CreatedAt: now},
	}
	var sb bytes.Buffer
	if err := renderPortsTable(&sb, single, false, now); err != nil {
		t.Fatal(err)
	}
	out := sb.String()
	if strings.Contains(out, "HOME") {
		t.Fatalf("single-broker PORTS must NOT have a HOME column:\n%s", out)
	}
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "CREATED") || !strings.Contains(out, "jup") {
		t.Fatalf("single-broker PORTS missing expected columns/rows:\n%s", out)
	}

	clustered := []proto.PsPortEntry{
		{Port: 14000, Name: "homed", NID: "a100", LocalPort: 8888, State: "ALLOCATED", CreatedAt: now, HomeBroker: "brk-a"},
		{Port: 14001, Name: "moved", NID: "a100", LocalPort: 8889, State: "ALLOCATED", CreatedAt: now, HomeBroker: "brk-b", Epoch: 2},
		{Port: 14002, Name: "unhomed", NID: "b200", LocalPort: 8890, State: "ALLOCATED", CreatedAt: now},
	}
	var cb bytes.Buffer
	if err := renderPortsTable(&cb, clustered, false, now); err != nil {
		t.Fatal(err)
	}
	cout := cb.String()
	if !strings.Contains(cout, "HOME") {
		t.Fatalf("clustered PORTS must have a HOME column:\n%s", cout)
	}
	if !strings.Contains(cout, "brk-b (moved)") {
		t.Fatalf("a rehomed (epoch>0) port must render (moved):\n%s", cout)
	}
	// the un-homed row's HOME cell is "-"
	if !strings.Contains(cout, "unhomed") || !strings.Contains(cout, "-") {
		t.Fatalf("un-homed row must show '-' in HOME:\n%s", cout)
	}
}

// TestB4PickExposeByName (plan §F.21 helper): prefer the live ALLOCATED row; fall back to the
// latest matching row; report not-found.
func TestB4PickExposeByName(t *testing.T) {
	t0 := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	ports := []proto.PsPortEntry{
		{Name: "svc", State: "FREED", Port: 14000, CreatedAt: t0},
		{Name: "svc", State: "ALLOCATED", Port: 14005, CreatedAt: t0.Add(time.Hour)},
		{Name: "other", State: "ALLOCATED", Port: 14009, CreatedAt: t0},
	}
	got, ok := pickExposeByName(ports, "svc")
	if !ok || got.State != "ALLOCATED" || got.Port != 14005 {
		t.Fatalf("must prefer the ALLOCATED row: %+v ok=%v", got, ok)
	}
	// no ALLOCATED → latest by CreatedAt
	freedOnly := []proto.PsPortEntry{
		{Name: "z", State: "FREED", Port: 1, CreatedAt: t0},
		{Name: "z", State: "REVOKED", Port: 2, CreatedAt: t0.Add(time.Hour)},
	}
	got, ok = pickExposeByName(freedOnly, "z")
	if !ok || got.Port != 2 {
		t.Fatalf("no-ALLOCATED must return latest: %+v ok=%v", got, ok)
	}
	if _, ok := pickExposeByName(ports, "nope"); ok {
		t.Fatal("unknown name must report not-found")
	}
}

// TestB4RenderExposeExplainHonest (plan §F.21): human output shows only recorded fields + a single
// footer note for the deferred ones — never empty labeled placeholders. rebuild on/off + moved are
// rendered correctly.
func TestB4RenderExposeExplainHonest(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	p := proto.PsPortEntry{Name: "jup", NID: "a100", LocalPort: 8888, Port: 14000, State: "ALLOCATED",
		CreatedByFP: "SHA256:fp", CreatedAt: now, HomeBroker: "brk-b", Epoch: 2, RebuildOff: true}
	var b bytes.Buffer
	renderExposeExplain(&b, p, !p.RebuildOff, p.Epoch > 0)
	out := b.String()
	for _, want := range []string{"jup", "a100", "ALLOCATED", "brk-b", "moved", "off", "rehome events"} {
		if !strings.Contains(out, want) {
			t.Errorf("explain output missing %q:\n%s", want, out)
		}
	}
	// honesty: deferred fields appear ONLY in the single footer note (a line starting with "#"),
	// never as their own labeled placeholder row.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue // the footer note is allowed to name them
		}
		for _, bad := range []string{"last_error", "reconnects", "ready_reason"} {
			if strings.Contains(line, bad) {
				t.Errorf("explain must NOT print a labeled %q row (deferred): %q", bad, line)
			}
		}
	}

	// rebuild ON renders "on"
	var b2 bytes.Buffer
	on := proto.PsPortEntry{Name: "x", NID: "n", LocalPort: 1, Port: 14001, State: "ALLOCATED", CreatedAt: now}
	renderExposeExplain(&b2, on, true, false)
	if !strings.Contains(b2.String(), "on") || strings.Contains(b2.String(), "moved") {
		t.Errorf("rebuild-ON un-moved render wrong:\n%s", b2.String())
	}
}

// TestB4ExposeExplainJSONNoDeferredKeys (plan §F.21): the machine schema carries only recorded
// fields; the deferred observability keys are ABSENT (not null) and schema_version stays 1.
func TestB4ExposeExplainJSONNoDeferredKeys(t *testing.T) {
	j := exposeExplainJSON{Schema: "expose_explain", SchemaVersion: 1, Name: "jup", Node: "a100",
		LocalPort: 8888, PublicPort: 14000, State: "ALLOCATED", Rebuild: false, HomeBroker: "brk-b", Epoch: 2, Moved: true, CreatedByFP: "SHA256:fp"}
	b, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, `"schema":"expose_explain"`) || !strings.Contains(s, `"schema_version":1`) {
		t.Fatalf("schema discriminators missing: %s", s)
	}
	if !strings.Contains(s, `"rebuild":false`) {
		t.Fatalf("rebuild field must always be present: %s", s)
	}
	for _, bad := range []string{"last_error", "reconnects", "ready_reason", "last_rehome_at"} {
		if strings.Contains(s, bad) {
			t.Fatalf("deferred key %q must be absent from the schema: %s", bad, s)
		}
	}

	// m1 (Stage-C): `moved` is script-switched ⇒ must be present even when false (a bool's absence
	// is ambiguous). An un-moved expose still serializes "moved":false.
	unmoved, err := json.Marshal(exposeExplainJSON{Schema: "expose_explain", SchemaVersion: 1, Name: "j", Node: "n", State: "ALLOCATED", Rebuild: true, Moved: false})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(unmoved), `"moved":false`) {
		t.Fatalf("moved must be present-and-false on an un-moved expose: %s", unmoved)
	}
}

// TestB4ExposeExplainNotFoundIsUsageExit (Stage-C J1): a missing expose name is a usage error
// (exit 64), NOT broker-unreachable (exit 69) — a successful round-trip to a healthy broker that
// simply has no such row. This locks the error CLASS the not-found path returns.
func TestB4ExposeExplainNotFoundIsUsageExit(t *testing.T) {
	err := usageErr("expose explain: no expose named %q in this session", "nope")
	if got := classifyExit(err); got != exitUsage {
		t.Fatalf("expose-explain not-found exit class = %d, want %d (exitUsage); must not be exitUnavailable=%d", got, exitUsage, exitUnavailable)
	}
}
