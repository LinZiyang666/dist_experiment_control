package xferfinal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Copy into internal/xferfinal. Reuses the package's complete finalizer fixture,
// replacing only the receiver with a permanently unavailable endpoint.
// origin: S7 internal review rv-wiring F5
func TestTransferAbandonedDecisionRetiresWithoutReceiver(t *testing.T) {
	f := newFinalFixture(t, "")
	f.row.Intent.Tier = "a"
	f.row.ReceiverOutcome = nil
	f.row.OutcomeUnknown = true
	var audit schema.AuditTransfer
	if err := json.Unmarshal(fixtureRecord(f.row.Identity, "failed", true).Bytes, &audit); err != nil {
		t.Fatal(err)
	}
	audit.Code = x.ReasonSettlementAbandoned
	canonical, err := schema.CanonicalTransferAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	f.row.CanonicalTerminal = &canonical
	f.receipts[f.row.CanonicalTerminal.Digest] = x.HistoryReceipt{Stream: "history", Sequence: 2, RecordDigest: f.row.CanonicalTerminal.Digest}
	f.resolveErr = errors.New("original agent permanently unavailable")
	finalizer, err := New(f.config())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := finalizer.Stop(finalDeadline(t, time.Second)); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	status, err := finalizer.Advance(f.row.Identity, finalDeadline(t, time.Second))
	if err != nil || status.Phase != x.PhaseSettled {
		t.Fatalf("explicit outcome-unknown terminal cannot release LQ1 deletion: phase=%s err=%v calls=%v", status.Phase, err, f.events)
	}
}

// Administrative retirement changes receiver certainty only; every reachable
// producer's cleanup receipt must still prove its own completed retirement.
// origin: S7 internal review rv-wiring F5
func TestTransferAbandonmentRetirementRequiresExactAdministrativeDecision(t *testing.T) {
	for _, fault := range []string{"code", "flag", "writer", "consumer", "tmp", "directory"} {
		t.Run(fault, func(t *testing.T) {
			f := newFinalFixture(t, "")
			f.row.OutcomeUnknown = true
			var audit schema.AuditTransfer
			if err := json.Unmarshal(fixtureRecord(f.row.Identity, "failed", true).Bytes, &audit); err != nil {
				t.Fatal(err)
			}
			audit.Code = x.ReasonSettlementAbandoned

			canonical, err := schema.CanonicalTransferAudit(audit)
			if err != nil {
				t.Fatal(err)
			}
			// Corrupt the encoded boundary after the real encoder accepted
			// it: its own validation must not mask the finalizer's guard.
			if fault == "code" || fault == "flag" {
				if fault == "code" {
					audit.Code = "another-code"
				} else {
					audit.OutcomeUnknown = false
				}
				canonical.Bytes, _ = json.Marshal(audit)
				digest := sha256.Sum256(canonical.Bytes)
				req := sha256.Sum256(append([]byte("xferaudit-record:"), canonical.Bytes...))
				canonical.Digest, canonical.ReqID = hex.EncodeToString(digest[:]), hex.EncodeToString(req[:])
			}
			f.row.CanonicalTerminal = &canonical
			f.row.HistoryReceipts = []x.HistoryReceipt{f.receipts[f.row.CanonicalStart.Digest], {Stream: "history", Sequence: 2, RecordDigest: canonical.Digest}}
			switch fault {
			case "writer":
				f.cleanup.WriterRetired = false
			case "consumer":
				f.cleanup.ConsumersRetired = false
			case "tmp":
				f.cleanup.TmpUnlinked = false
			case "directory":
				f.cleanup.DirectorySynced = false
			}
			port, err := New(f.config())
			if err != nil {
				t.Fatal(err)
			}
			_, err = port.(*owner).cleanup(f.row, finalDeadline(t, time.Second))
			if stopErr := port.Stop(finalDeadline(t, time.Second)); stopErr != nil {
				t.Fatal(stopErr)
			}
			if err == nil {
				t.Fatal("administrative retirement accepted contradictory or incomplete evidence")
			}
		})
	}
}
