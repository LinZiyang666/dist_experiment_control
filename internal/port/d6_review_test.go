package port

import (
	"testing"
)

// TestD6PlanAllocateHomedApplies (review A2 M1): the homed PlanAllocate branch is
// never APPLIED elsewhere (only substring-checked), so a VALUES-order typo in its
// second copy of the column list ships green. This APPLIES the baked homed INSERT
// and asserts every column lands correctly (home_broker/epoch + the unchanged
// shared columns) — catching a re-baked-prefix drift.
func TestD6PlanAllocateHomedApplies(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	alloc, cmd, err := PlanAllocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:caller", "node-2", false, tinyBand())
	if err != nil {
		t.Fatalf("plan allocate(home): %v", err)
	}
	if _, err := db.Exec(cmd.Body[0].SQL); err != nil {
		t.Fatalf("apply homed INSERT: %v", err)
	}
	got, err := LookupByName(db, "lab", "jupyter")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.Port != alloc.Port || got.SID != "lab" || got.NID != "lab-1" || got.Name != "jupyter" ||
		got.LocalPort != 8888 || got.State != StateAllocated || got.CreatedByFP != "SHA256:caller" {
		t.Fatalf("homed INSERT applied wrong shared columns: %+v", got)
	}
	// home_broker/epoch land via the homed branch.
	gotHome, gotEpoch := readHomeEpoch(t, db, alloc.Port)
	if gotHome != "node-2" || gotEpoch != 0 {
		t.Fatalf("homed INSERT: home=%q epoch=%d, want node-2/0", gotHome, gotEpoch)
	}
	// token_hash matches the returned Allocation (the row is dial-authorizable).
	if got.TokenHash != alloc.TokenHash {
		t.Fatalf("token_hash mismatch: row=%q alloc=%q", got.TokenHash, alloc.TokenHash)
	}
}

// TestD6LookupByTokenHashLegacy (review A1/A6 M4): the widened SELECT must scan a
// LEGACY row (created by the live port.Allocate direct mutator, which leaves
// home_broker=” / epoch=0) back as Allocation{HomeBroker:"", Epoch:0} — the
// inert-branch precondition that keeps tunnelTokenLookup byte-equivalent to pre-D6.
func TestD6LookupByTokenHashLegacy(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")
	a, err := Allocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:a", false, tinyBand())
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	got, err := LookupByTokenHash(db, a.TokenHash)
	if err != nil {
		t.Fatalf("lookup by token hash: %v", err)
	}
	if got.HomeBroker != "" || got.Epoch != 0 {
		t.Fatalf("legacy row must scan as home=''/epoch=0, got home=%q epoch=%d", got.HomeBroker, got.Epoch)
	}
}
