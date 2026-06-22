package d5_test

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
)

// TestD5DedupWindowExceedsElectionDrain (E-A3 / §6.3): the JetStream Duplicates window on
// the audit streams MUST exceed the worst-case (election + post-election tail-drain) so a
// re-publish from a NEW leader still lands within the window and is collapsed. Asserted BY
// REFERENCE against the real constants (shrinking either the window or the margin makes
// this go red) — never a hardcoded value on both sides. The k_fence=10 factor mirrors the
// TFence derivation (T = 10 x electionTimeout): the drain bound is generously 10 elections.
func TestD5DedupWindowExceedsElectionDrain(t *testing.T) {
	const kDrain = 10
	bound := kDrain * cluster.MultinodeElectionTimeout
	if jsstream.AuditDedupWindow <= bound {
		t.Fatalf("AuditDedupWindow=%s must exceed %d x MultinodeElectionTimeout=%s (election+drain bound)",
			jsstream.AuditDedupWindow, kDrain, bound)
	}
	// Sanity: the window is a positive duration (a 0 window disables dedup entirely).
	if jsstream.AuditDedupWindow <= 0 {
		t.Fatal("AuditDedupWindow must be a positive duration")
	}
}
