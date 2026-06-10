package main

import "testing"

func TestPullDuplicateIDRejectionDoesNotFinalizeOriginalTransfer(t *testing.T) {
	if pullPrepareFailureNeedsFinalize("transfer_id_in_flight") {
		t.Fatal("duplicate pull rejection must not finalize the original in-flight transfer")
	}
	for _, code := range []string{"path_outside_roots", "transfer_disabled", "io_error"} {
		if !pullPrepareFailureNeedsFinalize(code) {
			t.Fatalf("agent-side prepare failure %q still needs broker finalization", code)
		}
	}
}
