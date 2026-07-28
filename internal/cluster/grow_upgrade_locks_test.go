package cluster

import (
	"testing"
	"time"
)

// origin: codex_allgreen_external_review_test.go (renamed in B6) — docs/reviews/codex-allgreen-external-review.md
//
// The grow and upgrade locks are one mutual-exclusion domain.  Their broker
// handlers currently check the opposite marker before Propose, so two concurrent
// callers can both pass that read and then have these commands committed in
// sequence.  The replicated write itself must preserve the mutex invariant.
func TestCodexReviewGrowAndUpgradeLocksCannotCoexist(t *testing.T) {
	f, _ := freshFSM(t, t.TempDir())
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

	upgrade, err := PlanSetUpgradeActive(now)
	if err != nil {
		t.Fatal(err)
	}
	grow, err := PlanSetGrowActive("joiner-b", now)
	if err != nil {
		t.Fatal(err)
	}
	d7Apply(t, f, 1, upgrade)
	d7Apply(t, f, 2, grow)

	var active int
	if err := f.db.QueryRow(`SELECT count(*) FROM cluster_meta WHERE key IN (?, ?)`,
		MetaKeyUpgradeActive, MetaKeyGrowActive).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active > 1 {
		t.Fatalf("mutually exclusive membership locks coexist after serialized Raft apply: active=%d", active)
	}
}
