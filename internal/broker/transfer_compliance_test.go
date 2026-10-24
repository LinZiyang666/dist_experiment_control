package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/xfertrack"
)

func TestTransferTrackerRejectsDuplicateWithoutReplacingOriginal(t *testing.T) {
	tracker := xfertrack.NewLegacyTracker()
	original := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "same-id", Actor: "original"})
	if code := putLegacyTransfer(tracker, original); code != "" {
		t.Fatalf("first put rejected: %s", code)
	}

	replacement := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "same-id", Actor: "replacement"})
	if code := putLegacyTransfer(tracker, replacement); code != "transfer_id_in_flight" {
		t.Fatalf("duplicate put code=%q, want transfer_id_in_flight", code)
	}
	if got := tracker.Get("same-id"); got != original {
		t.Fatal("duplicate put replaced the original in-flight transfer")
	}
}

// Reduce the two-result admission API only for legacy tests that assert the
// response code. The production leaf retains the entry, bucket and watchdog;
// this adapter holds no shadow state and never converts a refusal to success.
// origin: hot-update S7 plan §5.1 / §11.4 / P31.
func putLegacyTransfer(tracker *xfertrack.LegacyTracker, entry *xfertrack.LegacyEntry) string {
	code, _ := tracker.Put(entry)
	return code
}
