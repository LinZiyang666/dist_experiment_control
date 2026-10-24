package xfertrack_test

import (
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

// The S8 broker census names every tracked entry by its business key and the
// EntryNonce of its slot (the identity a replacement slot can never reuse),
// through the owner's serial token and its deadline. A manager this package
// did not build, an expired deadline or a stopped owner cannot say what it
// holds and refuses instead of reporting an idle tracker.
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 BROKER-EXPORTER R-11
func TestTrackerCensusNamesEveryEntryOrRefuses(t *testing.T) {
	f := newRecoveryFixture(t)
	second := newRecoveryTransfer(t, "b", "pull")
	entries, err := xfertrack.Census(f.m, recoveryDeadline(t))
	if err != nil || len(entries) != 1 || entries[0] != (xfertrack.CensusEntry{SID: f.id.SID, TransferID: f.id.TransferID, EntryNonce: f.id.EntryNonce}) {
		t.Fatalf("tracker census: %+v %v", entries, err)
	}
	if other, err := xfertrack.Census(second.m, recoveryDeadline(t)); err != nil || len(other) != 1 || other[0].EntryNonce != second.id.EntryNonce || other[0].Settled {
		t.Fatalf("second tracker census: %+v %v", other, err)
	}
	if got, err := xfertrack.Census(nil, recoveryDeadline(t)); err == nil {
		t.Fatalf("a manager the tracker did not build reported %+v", got)
	}
	expired, err := controlbudget.After(0)
	if err == nil {
		if got, err := xfertrack.Census(f.m, expired); err == nil {
			t.Fatalf("an expired census reported %+v", got)
		}
	}
	if err := f.m.Stop(recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	got, err := xfertrack.Census(f.m, recoveryDeadline(t))
	var code interface{ TransferCode() string }
	if !errors.As(err, &code) || code.TransferCode() != x.CodeOwnerRecovering {
		t.Fatalf("a stopped tracker reported %+v: %v", got, err)
	}
}
