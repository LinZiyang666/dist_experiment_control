package xferserve

import (
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: S7 internal review rv-terminal / FIX-TERM request 3
func TestTransferFailedCleanupRequiresPositiveWriterDisposition(t *testing.T) {
	row := x.LedgerRow{Identity: x.Identity{ObjectVersion: "original"}, Intent: x.Intent{Verb: "push", Tier: "b"},
		CanonicalTerminal: &x.CanonicalRecord{Kind: "failed"}}
	if err := writerRetired(row, false); err == nil {
		t.Fatal("elapsed time invented writer retirement")
	}
	row.Cleanup.WriterFence.WriterRetired = true
	if err := writerRetired(row, false); err != nil {
		t.Fatalf("serialized authenticated writer witness cannot release failed cleanup: %v", err)
	}
	row.Cleanup.WriterFence.WriterRetired = false
	row.Identity.ObjectVersion, row.Intent.Tier = "", "a"
	if err := writerRetired(row, false); err != nil {
		t.Fatalf("inline transfer's retained no-publisher disposition required a fictitious OBJ writer: %v", err)
	}
	row.Intent.Tier = "b"
	if err := writerRetired(row, false); err == nil {
		t.Fatal("missing version proved no publisher for an object transfer")
	}
}
