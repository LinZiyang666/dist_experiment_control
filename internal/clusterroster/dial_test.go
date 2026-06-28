package clusterroster

import (
	"strings"
	"testing"
)

// Empty learned+seeds with floorCSV==base returns base byte-for-byte — the non-cluster invariant both the
// agent (effectiveDialURLs) and the ctl (DialFor) rely on.
func TestBuildDialStringByteEquivalence(t *testing.T) {
	base := "wss://b.example:443/p"
	if got := BuildDialString(nil, nil, base); got != base {
		t.Fatalf("empty learned+seeds + floor==base must return base unchanged: %q", got)
	}
	if got := BuildDialString([]string{}, []string{}, base); got != base {
		t.Fatalf("non-nil empty slices must also be byte-equivalent: %q", got)
	}
}

// Tiers (learned → seeds → floor), de-dup in first-seen order, floor parts LAST and never dropped.
func TestBuildDialStringFloorLastAndDedup(t *testing.T) {
	learned := []string{"wss://v1:443", "wss://v2:443"}
	seeds := []string{"wss://v2:443", "wss://s1:443"} // v2 duplicates a learned entry
	floor := "wss://v1:443,wss://floor:443"           // v1 duplicates a learned entry
	got := BuildDialString(learned, seeds, floor)
	want := "wss://v1:443,wss://v2:443,wss://s1:443,wss://floor:443"
	if got != want {
		t.Fatalf("dial=%q\nwant=%q", got, want)
	}
}

// The floor is the permanent fallback: present even when learned tiers cover other endpoints.
func TestBuildDialStringFloorNeverDropped(t *testing.T) {
	got := BuildDialString([]string{"wss://v:443"}, nil, "wss://floor:443")
	if !strings.HasSuffix(got, "wss://floor:443") {
		t.Fatalf("floor must remain present (last): %q", got)
	}
}

// Blank/whitespace entries are skipped (trimmed).
func TestBuildDialStringSkipsBlanks(t *testing.T) {
	got := BuildDialString([]string{"  ", ""}, []string{" wss://s:443 "}, " ,wss://floor:443, ")
	want := "wss://s:443,wss://floor:443"
	if got != want {
		t.Fatalf("dial=%q want %q", got, want)
	}
}
