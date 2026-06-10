package broker

import "testing"

func TestTransferTrackerRejectsDuplicateWithoutReplacingOriginal(t *testing.T) {
	tracker := newTransferTracker()
	original := &transferEntry{transferID: "same-id", actor: "original"}
	if code := tracker.put(original); code != "" {
		t.Fatalf("first put rejected: %s", code)
	}

	replacement := &transferEntry{transferID: "same-id", actor: "replacement"}
	if code := tracker.put(replacement); code != "transfer_id_in_flight" {
		t.Fatalf("duplicate put code=%q, want transfer_id_in_flight", code)
	}
	if got := tracker.get("same-id"); got != original {
		t.Fatal("duplicate put replaced the original in-flight transfer")
	}
}
