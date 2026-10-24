package xferauth

import (
	"errors"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/session"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// The authoritative tombstone result decides retry semantics; only the
// settlement barrier can block a cascade. Cascade failure never changes a
// successfully settled removal into an rc75, and the resumer retains its work.
// A governed settled retry returns OK per the lead RM-RETRY decision; only
// the legacy retry retains already_deleting.
// origin: hot-update S7 plan lead LQ1 / INTEG-G preserved lead RM-RETRY decision
func TestSessionRemovalUsesAuthoritativeTombstoneAndSettlement(t *testing.T) {
	pending := &PendingError{Transfers: []PendingTransfer{{TransferID: "original", Reason: "receiver_commit_unsettled"}}}
	for _, tc := range []struct {
		name, body               string
		tombstone                error
		governed                 bool
		settlement               error
		code                     string
		tombs, settles, cascades int
		abandon                  bool
	}{
		{name: "malformed", body: `{"abandon_unsettled_transfers":"yes"}`, code: "json_parse"},
		{name: "missing", tombstone: session.ErrNotFound, code: "not_found", tombs: 1},
		{name: "store", tombstone: errors.New("store unavailable"), code: "store_error", tombs: 1},
		{name: "legacy first removal", tombs: 1, settles: 1, cascades: 1},
		{name: "legacy retry", tombstone: session.ErrDeleting, code: "already_deleting", tombs: 1},
		{name: "legacy flagged retry", body: `{"abandon_unsettled_transfers":true}`, tombstone: session.ErrDeleting, code: "already_deleting", tombs: 1},
		{name: "new pending", governed: true, settlement: pending, code: x.CodeOwnerRecovering, tombs: 1, settles: 1},
		{name: "governed retry pending", governed: true, tombstone: session.ErrDeleting, settlement: pending, code: x.CodeOwnerRecovering, tombs: 1, settles: 1},
		{name: "flag cannot waive history", body: `{"abandon_unsettled_transfers":true}`, governed: true, settlement: pending, code: x.CodeOwnerRecovering, tombs: 1, settles: 1, abandon: true},
		{name: "settled first removal", governed: true, tombs: 1, settles: 1, cascades: 1},
		{name: "settled retry", governed: true, tombstone: session.ErrDeleting, tombs: 1, settles: 1, cascades: 1},
		{name: "settled flagged retry", body: `{"abandon_unsettled_transfers":true}`, governed: true, tombstone: session.ErrDeleting, tombs: 1, settles: 1, cascades: 1, abandon: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tombs, settles, cascades := 0, 0, 0
			r := RemoveSession([]byte(tc.body), func() error { tombs++; return tc.tombstone }, tc.governed, func(abandon bool) error {
				if tombs != 1 || abandon != tc.abandon {
					t.Fatal("settlement lost tombstone ordering or explicit authority")
				}
				settles++
				return tc.settlement
			}, func() {
				if settles != 1 {
					t.Fatal("cascade preceded settlement")
				}
				cascades++
			})
			if r.Code != tc.code || r.OK != (tc.code == "") || tombs != tc.tombs || settles != tc.settles || cascades != tc.cascades {
				t.Fatalf("reply/order changed: %+v calls=%d/%d/%d", r, tombs, settles, cascades)
			}
			if tc.settlement == pending && (!strings.Contains(r.Error, "original") || !strings.Contains(r.Error, "receiver_commit_unsettled")) {
				t.Fatal("lost pending identity/reason")
			}
			if tc.code == "not_found" || tc.code == "store_error" {
				if r.Error != tc.tombstone.Error() {
					t.Fatal("legacy refusal detail changed")
				}
			}
		})
	}
}
