package cluster_test

import (
	"database/sql"
	"testing"
)

// TestApply_AdvancesAppliedIndexAndWritesValue is the happy path: a baked op
// commits, its value is visible, and applied_index advances. The first-Apply case
// catches the "bare UPDATE writes zero rows on the D0-empty cluster_meta" bug
// (§2.2): if applied_index were written with UPDATE instead of UPSERT, it would
// stay 0 forever.
func TestApply_AdvancesAppliedIndexAndWritesValue(t *testing.T) {
	n, _ := newTestNode(t)

	before, err := n.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatalf("fresh node applied_index = %d, want 0 (D0 ships cluster_meta empty)", before)
	}

	if err := n.ApplyMetaSet("t:alpha", "one"); err != nil {
		t.Fatalf("ApplyMetaSet: %v", err)
	}
	after1, err := n.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if after1 == 0 {
		t.Fatal("applied_index still 0 after first Apply — the UPSERT seed did not write (bare-UPDATE bug?)")
	}
	if v, ok := readMeta(t, n, "t:alpha"); !ok || v != "one" {
		t.Fatalf("t:alpha = (%q,%v), want (\"one\",true)", v, ok)
	}

	// A second op strictly advances applied_index.
	if err := n.ApplyMetaSet("t:beta", "two"); err != nil {
		t.Fatal(err)
	}
	after2, err := n.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	if after2 <= after1 {
		t.Fatalf("applied_index did not advance: after1=%d after2=%d", after1, after2)
	}

	// Upsert semantics: re-setting a key overwrites, not duplicates.
	if err := n.ApplyMetaSet("t:alpha", "one-prime"); err != nil {
		t.Fatal(err)
	}
	if v, ok := readMeta(t, n, "t:alpha"); !ok || v != "one-prime" {
		t.Fatalf("t:alpha after re-set = (%q,%v), want (\"one-prime\",true)", v, ok)
	}
}

// TestApply_RejectsKeyOutsideTestPrefix pins the D1 scaffolding guard: the
// representative op may only write under the test-reserved prefix, so it can
// never collide with the reserved cursor rows or a real table (§2.10).
func TestApply_RejectsKeyOutsideTestPrefix(t *testing.T) {
	n, _ := newTestNode(t)
	err := n.ApplyMetaSet("applied_index", "999") // would clobber the cursor
	if err == nil {
		t.Fatal("ApplyMetaSet must reject a key outside the t: test prefix")
	}
}

// TestApply_VerifyLeaderReadSucceedsAtN1 exercises the read-consistency seam: at
// N=1 the VerifyLeader barrier trivially succeeds and the read sees committed
// state. (The seam exists for D2+ consumers; D1 only proves it is wired.)
func TestApply_VerifyLeaderReadSucceedsAtN1(t *testing.T) {
	n, _ := newTestNode(t)
	if err := n.ApplyMetaSet("t:k", "v"); err != nil {
		t.Fatal(err)
	}
	var got string
	err := n.VerifyLeaderRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, "t:k").Scan(&got)
	})
	if err != nil {
		t.Fatalf("VerifyLeaderRead at N=1 must succeed: %v", err)
	}
	if got != "v" {
		t.Fatalf("VerifyLeaderRead saw %q, want \"v\"", got)
	}
}
