package xfercapacity

import (
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// Imported IDs remain occupied and all adoption is atomic on refusal.
// origin: hot-update S7 plan §8.3 / T17
func TestTransferCapacityAdoptsWithoutReusingLiveReservations(t *testing.T) {
	a := New("owner-")
	if err := a.Adopt([]x.ReservationID{"owner-1"}, []x.Reservation{{FDs: 1}}); err != nil {
		t.Fatal(err)
	}
	id, err := a.Reserve(x.Reservation{FDs: 2})
	if err != nil || id == "owner-1" {
		t.Fatalf("reused inherited reservation: %s %v", id, err)
	}
	for _, ids := range [][]x.ReservationID{{"owner-1", "fresh"}, {"fresh", "fresh"}} {
		if err := a.Adopt(ids, []x.Reservation{{FDs: 1}, {FDs: 1}}); err == nil {
			t.Fatal("duplicate adoption accepted")
		}
		if a.used.FDs != 3 || len(a.held) != 2 {
			t.Fatal("refused adoption changed live capacity")
		}
	}
	if err := a.Release("owner-1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Release(id); err != nil {
		t.Fatal(err)
	}
	if a.used != (x.Reservation{}) {
		t.Fatal("release lost imported ownership")
	}
}
