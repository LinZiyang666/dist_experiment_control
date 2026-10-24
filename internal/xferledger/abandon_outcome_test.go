package xferledger

import (
	"errors"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"testing"
)

// Unknown is the administrative label of retained inflight evidence, never
// an invented no-effect outcome or an unrecognized persisted enum member.
// origin: S7 internal review rv-terminal F5 / rv-recovery F08
func TestTransferAbandonmentCodecRejectsInventedReceiverOutcome(t *testing.T) {
	for _, outcome := range []x.PrimitiveOutcome{x.OutcomeNotEntered, x.OutcomeLanded, "unknown-invented"} {
		t.Run(string(outcome), func(t *testing.T) {
			store := newStore(t, testConfig(t))
			prior := update(t, store, RowV1{}, startRow(t), x.MutationAdmit)
			next := decidedRow(t, prior)
			next.OutcomeUnknown = true
			next.ReceiverOutcome.Outcome, next.ReceiverOutcome.Code = outcome, x.ReasonSettlementAbandoned
			next.CanonicalTerminal = canonical(t, next, "failed", x.ReasonSettlementAbandoned)
			if _, err := store.Update(rowKey(prior), prior.Revision, x.LedgerMutation{Kind: x.MutationAbandon, EntryNonce: prior.EntryNonce, Row: next}, testDeadline(t)); !errors.Is(err, ErrConflict) {
				t.Fatalf("invented receiver outcome %q became a durable abandonment: %v", outcome, err)
			}
		})
	}
}
