package port

import (
	"testing"
	"time"
)

// proxy_home_lookup_test.go (C6 Stage-C root-cause regression) — LookupProxyByNode MUST return the
// row's real home_broker + epoch. The pre-fix 10-column scanOne dropped both, so the C5/C6 proxy reaper
// saw HomeBroker="" Epoch=0 and perpetually rehomed every healthy proxy (HR3), rehome events carried
// epoch:0 (HR2), and proxy status disagreed with --homes (HR1). This pins the home-aware fix.
func TestLookupProxyByNodeReturnsHomeAndEpoch(t *testing.T) {
	db := openDB(t)
	seedSessionAndNode(t, db, "lab", "lab-1")

	alloc, err := AllocateProxy(db, "lab", "lab-1", nil)
	if err != nil {
		t.Fatalf("allocate proxy: %v", err)
	}
	// Re-point the home + bump the epoch (the D6 rehome path) so home_broker/epoch are non-default.
	newEpoch, cmd, err := PlanReassignHome(db, alloc.Port, "brk-HOME", time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("plan reassign: %v", err)
	}
	for _, st := range cmd.Body {
		if _, err := db.Exec(st.SQL); err != nil {
			t.Fatalf("apply reassign: %v", err)
		}
	}

	got, err := LookupProxyByNode(db, "lab", "lab-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.HomeBroker != "brk-HOME" {
		t.Errorf("LookupProxyByNode dropped home_broker: got %q want %q", got.HomeBroker, "brk-HOME")
	}
	if got.Epoch != newEpoch {
		t.Errorf("LookupProxyByNode dropped epoch: got %d want %d", got.Epoch, newEpoch)
	}
}
