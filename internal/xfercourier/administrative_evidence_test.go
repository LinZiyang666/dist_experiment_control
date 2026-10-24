package xfercourier

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Pure canonical/receipt validation: no IO, timers or owner roots. An original
// sender must accept an exact administrative decision without pretending the
// absent receiver reported a filesystem outcome. Ordinary ACK rules stay exact.
// origin: S7 internal review TRANSFER-MAKETEST F7 / S7 plan lead LQ1 / §6.4 / T31
func TestTransferAdministrativeEvidenceKeepsUnknownOutcomeAndExactReceipt(t *testing.T) {
	for _, primitive := range []x.PrimitiveOutcome{x.OutcomeInflight, x.OutcomeLanded} {
		evidence := administrativeEvidence(t, primitive)
		if err := ValidateEvidence(evidence); err != nil {
			t.Fatalf("exact administrative decision cannot retire the original sender: %s: %v", primitive, err)
		}
		state := x.CourierState{Identity: evidence.Identity, Outcome: evidence.Outcome, Canonical: evidence.Canonical}
		if err := validateReceipt(state, *evidence.Receipt); err == nil {
			t.Fatal("administrative evidence became an ordinary physical receiver ACK")
		}
	}
	for _, fault := range []string{"receipt_unknown", "kind", "code", "outcome_code", "bytes", "sha", "slot", "digest", "request", "history", "sequence", "identity", "canonical_kind", "canonical_digest", "canonical_request", "canonical"} {
		t.Run(fault, func(t *testing.T) {
			evidence := administrativeEvidence(t, x.OutcomeInflight)
			r := evidence.Receipt
			switch fault {
			case "receipt_unknown":
				r.OutcomeUnknown = false
			case "kind":
				r.Kind = "complete"
			case "code":
				r.Code = x.CodeCanceled
			case "outcome_code":
				evidence.Outcome.Code = x.CodeCanceled
			case "bytes":
				r.Bytes++
			case "sha":
				r.SHA256 = strings.Repeat("d", 64)
			case "slot":
				r.TerminalID = strings.Repeat("d", 64)
			case "digest":
				r.CanonicalDigest = strings.Repeat("d", 64)
			case "request":
				r.ReqID = strings.Repeat("d", 64)
			case "history":
				r.HistoryStream = "history-foreign"
			case "sequence":
				r.HistorySeq = 0
			case "identity":
				evidence.Identity.AgentInstance = "foreign-agent"
			case "canonical_kind":
				evidence.Canonical.Kind = "complete"
			case "canonical_digest":
				evidence.Canonical.Digest = strings.Repeat("d", 64)
			case "canonical_request":
				evidence.Canonical.ReqID = strings.Repeat("d", 64)
			case "canonical":
				evidence.Canonical.Bytes = []byte(`{"kind":"failed","outcome_unknown":true}`)
			}
			if err := ValidateEvidence(evidence); err == nil {
				t.Fatal("contradictory administrative evidence retired a sender")
			}
		})
	}
}

func administrativeEvidence(t *testing.T, primitive x.PrimitiveOutcome) x.CanonicalOutcome {
	t.Helper()
	id := x.Identity{SID: "session", NID: "node", TransferID: "transfer", AgentInstance: "agent", ActorFP: "actor", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "version"}
	outcome := x.ReceiverOutcome{Outcome: primitive, Code: x.ReasonSettlementAbandoned}
	if primitive == x.OutcomeLanded {
		outcome.Bytes, outcome.SHA256, outcome.Primitive = 123, strings.Repeat("b", 64), x.PrimitiveRename
	}
	record, err := schema.CanonicalTransferAudit(schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "failed", Verb: "pull", Tier: "b", Ts: time.Unix(100, 0).UTC(),
		Session: id.SID, Node: id.NID, TransferID: id.TransferID, ActorFp: id.ActorFP, AgentInstance: id.AgentInstance, EntryNonce: id.EntryNonce,
		IntentDigest: id.IntentDigest, ObjectVersion: id.ObjectVersion, TerminalID: terminalID(id), Code: outcome.Code, OutcomeUnknown: true, Bytes: outcome.Bytes, SHA256: outcome.SHA256})
	if err != nil {
		t.Fatal(err)
	}
	return x.CanonicalOutcome{Identity: id, Outcome: outcome, Canonical: record, Receipt: &x.TerminalReceipt{TerminalID: terminalID(id), Kind: "failed",
		CanonicalDigest: record.Digest, ReqID: record.ReqID, HistoryStream: historyStream(id.SID), HistorySeq: 2, Code: outcome.Code, OutcomeUnknown: true, Bytes: outcome.Bytes, SHA256: outcome.SHA256}}
}
