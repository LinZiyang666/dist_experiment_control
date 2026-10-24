package xferledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// The envelope versions the disk codec independently of public wire/image ABI.
// RowV1 is the frozen neutral record, including exact publication obligations.
// origin: hot-update S7 plan §5.1 / T15 / T22
const ledgerSchema = "xfer-ledger-v1"

const maxRowBytes = 32 << 20 // The shared private-state ceiling also bounds disk decode.

var (
	ErrConflict   = errors.New(xfercontract.CodeTerminalConflict)
	ErrUnknown    = errors.New(xfercontract.CodeResultUnknown)
	ErrDurability = errors.New(xfercontract.CodeDurabilityUnknown)
)

type diskRecord struct {
	Schema string `json:"schema"`
	Digest string `json:"digest"`
	Row    RowV1  `json:"row"`
}

// Only the audit owner renders these bytes. This read-only projection checks
// their existing identity and the lead's abandonment flag without re-encoding
// or dropping additive fields that the audit schema gains during integration.
type canonicalAudit struct {
	schema.AuditTransfer
	OutcomeUnknown bool `json:"outcome_unknown,omitempty"`
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func rowKey(row RowV1) xfercontract.Key {
	return xfercontract.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}
}

// Canonical bytes are opaque to this owner: the audit producer owns field order
// and timestamps. Validate their content keys without re-rendering the record.
func validCanonical(rec *xfercontract.CanonicalRecord, id xfercontract.Identity, start bool) bool {
	if rec == nil || rec.Identity != id || !json.Valid(rec.Bytes) || rec.Digest != digest(rec.Bytes) {
		return false
	}
	if rec.ReqID != digest(append([]byte("xferaudit-record:"), rec.Bytes...)) {
		return false
	}
	var audit canonicalAudit
	if json.Unmarshal(rec.Bytes, &audit) != nil || audit.Kind != rec.Kind || audit.Session != id.SID ||
		audit.TransferID != id.TransferID || audit.Node != id.NID || audit.ActorFp != id.ActorFP || audit.Ts.IsZero() {
		return false
	}
	return (start && rec.Kind == "start") || (!start && (rec.Kind == "complete" || rec.Kind == "failed"))
}

func phaseRank(p xfercontract.Phase) int {
	switch p {
	case xfercontract.PhaseOpen:
		return 1
	case xfercontract.PhasePrepared:
		return 2
	case xfercontract.PhaseSending, xfercontract.PhaseReceiving:
		return 3
	case xfercontract.PhaseCommitReserved:
		return 4
	case xfercontract.PhaseDecided:
		return 5
	case xfercontract.PhaseTerminalPending:
		return 6
	case xfercontract.PhaseHistoryConfirmed:
		return 7
	case xfercontract.PhaseCleanupPending:
		return 8
	case xfercontract.PhaseSettled:
		return 9
	}
	return 0
}

func deliveryRank(d xfercontract.AuditDelivery) int {
	switch d {
	case xfercontract.DeliveryPending:
		return 1
	case xfercontract.DeliveryRaftCommitted:
		return 2
	case xfercontract.DeliveryHistoryConfirmed:
		return 3
	}
	return 0
}

func receiptFor(row RowV1, rec *xfercontract.CanonicalRecord) (xfercontract.HistoryReceipt, bool) {
	if rec != nil {
		for _, receipt := range row.HistoryReceipts {
			if receipt.RecordDigest == rec.Digest {
				return receipt, true
			}
		}
	}
	return xfercontract.HistoryReceipt{}, false
}

func validateReceipts(row RowV1) error {
	seen := make(map[string]bool)
	var stream string
	var sequence uint64
	for _, receipt := range row.HistoryReceipts {
		matches := row.CanonicalStart != nil && receipt.RecordDigest == row.CanonicalStart.Digest
		matches = matches || (row.CanonicalTerminal != nil && receipt.RecordDigest == row.CanonicalTerminal.Digest)
		if !matches || receipt.Stream == "" || receipt.Sequence == 0 || seen[receipt.RecordDigest] ||
			(stream != "" && (stream != receipt.Stream || sequence == receipt.Sequence)) {
			return fmt.Errorf("%w: history receipt does not prove this decision", ErrConflict)
		}
		seen[receipt.RecordDigest], stream, sequence = true, receipt.Stream, receipt.Sequence
	}
	_, start := receiptFor(row, row.CanonicalStart)
	_, terminal := receiptFor(row, row.CanonicalTerminal)
	if (row.StartDelivery == xfercontract.DeliveryHistoryConfirmed) != start ||
		(row.TerminalDelivery == xfercontract.DeliveryHistoryConfirmed) != terminal ||
		(phaseRank(row.Phase) >= phaseRank(xfercontract.PhaseHistoryConfirmed) && (!start || !terminal)) {
		return fmt.Errorf("%w: history confirmation requires exact receipts", ErrConflict)
	}
	return nil
}

func validateRow(row RowV1) error {
	id, budget := row.Identity, row.BudgetSeal
	if id.SID == "" || id.TransferID == "" || id.NID == "" || id.ActorFP == "" || id.AgentInstance == "" ||
		id.EntryNonce == "" || id.IntentDigest == "" || row.EntryNonce != id.EntryNonce || row.Revision == 0 ||
		row.StartedAt.IsZero() || row.Origin.BrokerID == "" || row.Origin.InstanceID == "" || row.Origin.BootID == "" ||
		row.Permit.Identity != id || row.Permit.Origin != row.Origin || row.Permit.PermitNonce == "" ||
		!row.Permit.CreateUntil.After(row.Permit.IssuedAt) || phaseRank(row.Phase) == 0 {
		return fmt.Errorf("%w: incomplete ledger identity", ErrConflict)
	}
	if (row.Intent.Tier != "a" && row.Intent.Tier != "b") || (row.Intent.Verb != "push" && row.Intent.Verb != "pull") ||
		(row.Intent.Tier == "b" && id.ObjectVersion == "") || (row.Intent.Tier == "a" && id.ObjectVersion != "") ||
		!budget.StartedAt.Equal(row.StartedAt) || budget.RateLimitBPS != row.Intent.RateLimitBPS ||
		budget.RateLimitBPS < 0 || budget.Size < 0 || budget.EffectiveBudgetNS <= 0 || budget.RemainingNS < 0 ||
		budget.RemainingNS > budget.EffectiveBudgetNS {
		return fmt.Errorf("%w: invalid original transfer budget", ErrConflict)
	}
	if !validCanonical(row.CanonicalStart, id, true) || deliveryRank(row.StartDelivery) == 0 {
		return fmt.Errorf("%w: missing canonical START", ErrConflict)
	}
	var start canonicalAudit
	if json.Unmarshal(row.CanonicalStart.Bytes, &start) != nil || !start.Ts.Equal(row.StartedAt) ||
		start.Verb != row.Intent.Verb || start.Tier != row.Intent.Tier {
		return fmt.Errorf("%w: START disagrees with original admission", ErrConflict)
	}
	if row.SourceSeal != nil {
		seal := row.SourceSeal
		if !budget.SourceSealed || seal.Size != budget.Size || seal.RateLimitBPS != budget.RateLimitBPS ||
			seal.SHA256 == "" || seal.SourceFingerprint == "" {
			return fmt.Errorf("%w: inconsistent source seal", ErrConflict)
		}
	} else if budget.SourceSealed {
		return fmt.Errorf("%w: missing source seal", ErrConflict)
	}
	if row.Grant != nil {
		terminalID := digest([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
		if row.Grant.Identity != id || row.Grant.GrantNonce == "" || row.Grant.ReceiverInstance == "" ||
			row.Grant.TerminalID != terminalID || phaseRank(row.Phase) < phaseRank(xfercontract.PhaseCommitReserved) {
			return fmt.Errorf("%w: inconsistent commit grant", ErrConflict)
		}
	} else if row.Phase == xfercontract.PhaseCommitReserved {
		return fmt.Errorf("%w: missing commit grant", ErrConflict)
	}
	if row.CanonicalTerminal == nil {
		if phaseRank(row.Phase) >= phaseRank(xfercontract.PhaseDecided) || !validUnresolvedOutcome(row) ||
			row.TerminalDelivery != "" || row.OutcomeUnknown {
			return fmt.Errorf("%w: missing canonical TERMINAL", ErrConflict)
		}
	} else {
		if !validCanonical(row.CanonicalTerminal, id, false) || row.ReceiverOutcome == nil ||
			deliveryRank(row.TerminalDelivery) == 0 || phaseRank(row.Phase) < phaseRank(xfercontract.PhaseDecided) {
			return fmt.Errorf("%w: incomplete terminal decision", ErrConflict)
		}
		if err := validateOutcome(row); err != nil {
			return err
		}
	}
	return validateReceipts(row)
}

func validateOutcome(row RowV1) error {
	out := row.ReceiverOutcome
	var audit canonicalAudit
	if json.Unmarshal(row.CanonicalTerminal.Bytes, &audit) != nil || audit.Code != out.Code ||
		audit.Bytes != out.Bytes || audit.SHA256 != out.SHA256 || audit.OutcomeUnknown != row.OutcomeUnknown ||
		audit.Verb != row.Intent.Verb || audit.Tier != row.Intent.Tier {
		return fmt.Errorf("%w: canonical terminal disagrees with receiver outcome", ErrConflict)
	}
	if (row.Grant == nil && out.Grant != (xfercontract.CommitGrant{})) ||
		(row.Grant != nil && out.Grant != *row.Grant) {
		return fmt.Errorf("%w: receiver grant changed", ErrConflict)
	}
	if row.OutcomeUnknown {
		if !audit.OutcomeUnknown || audit.Code != xfercontract.ReasonSettlementAbandoned ||
			row.CanonicalTerminal.Kind != "failed" || out.Code != xfercontract.ReasonSettlementAbandoned ||
			!validAbandonedOutcome(row) {
			return fmt.Errorf("%w: abandonment cannot claim completion", ErrConflict)
		}
		return nil // An explicit abandonment is not a claim that no primitive ran.
	}
	valid := false
	switch out.Outcome {
	case xfercontract.OutcomeNotEntered:
		valid = row.CanonicalTerminal.Kind == "failed"
	case xfercontract.OutcomeInflight:
		valid = false
	case xfercontract.OutcomeLanded:
		valid = row.Grant != nil && row.CanonicalTerminal.Kind == "complete"
	}
	if !valid {
		return fmt.Errorf("%w: unresolved receiver outcome", ErrConflict)
	}
	return nil
}

// The administrative unknown label uses the existing inflight value, with or
// without an issued grant. A previously observed landed-but-unsynced result
// retains its exact evidence. Neither branch invents a physical no-effect
// result, and an unrecognized enum cannot become durable via the unknown bit.
// origin: S7 internal review rv-terminal F5 / rv-recovery F08
func validAbandonedOutcome(row RowV1) bool {
	out := row.ReceiverOutcome
	if out.ParentSynced || out.Bytes < 0 || out.Bytes > row.BudgetSeal.Size {
		return false
	}
	switch out.Outcome {
	case xfercontract.OutcomeInflight:
		return true
	case xfercontract.OutcomeLanded:
		return row.Grant != nil && row.SourceSeal != nil && out.Bytes == row.SourceSeal.Size &&
			out.SHA256 == row.SourceSeal.SHA256 && (out.Primitive == xfercontract.PrimitiveRename || out.Primitive == xfercontract.PrimitiveLink)
	case xfercontract.OutcomeNotEntered:
		return false
	default:
		return false
	}
}

// Inflight and landed-but-unsynced are durable evidence, not terminal decisions.
// Retaining them allows the original receiver to finish after a reply loss.
// origin: hot-update S7 plan §4 / T12 / T13
func validUnresolvedOutcome(row RowV1) bool {
	out := row.ReceiverOutcome
	if out == nil {
		return true
	}
	if row.Phase != xfercontract.PhaseCommitReserved || row.Grant == nil || out.Grant != *row.Grant || out.ParentSynced ||
		(out.Primitive != xfercontract.PrimitiveRename && out.Primitive != xfercontract.PrimitiveLink) {
		return false
	}
	switch out.Outcome {
	case xfercontract.OutcomeInflight:
		return true
	case xfercontract.OutcomeLanded:
		return row.SourceSeal != nil && out.Bytes == row.SourceSeal.Size && out.SHA256 == row.SourceSeal.SHA256 && row.Cleanup.DirectorySyncPending
	case xfercontract.OutcomeNotEntered:
		// A commit-reserved row never persists an unentered outcome; named so a
		// future member must choose. origin: hot-update S7 plan §11.2 / T52
		return false
	default:
		return false
	}
}

// Once landing is observed it cannot be revised into not-entered, even if the
// receiver later reports an error. Only its original parent-sync can advance.
func compatibleOutcome(old, next RowV1) bool {
	before, after := old.ReceiverOutcome, next.ReceiverOutcome
	if before == nil || reflect.DeepEqual(before, after) {
		return true
	}
	if old.CanonicalTerminal != nil || after == nil || before.Grant != after.Grant || before.Primitive != after.Primitive {
		return false
	}
	if before.Outcome == xfercontract.OutcomeInflight {
		return true
	}
	return before.Outcome == xfercontract.OutcomeLanded && after.Outcome == xfercontract.OutcomeLanded &&
		before.Bytes == after.Bytes && before.SHA256 == after.SHA256 && (!before.ParentSynced || after.ParentSynced)
}

func knownMutation(kind xfercontract.LedgerMutationKind) bool {
	switch kind {
	case xfercontract.MutationAdmit, xfercontract.MutationSourceSeal, xfercontract.MutationReserve,
		xfercontract.MutationDecide, xfercontract.MutationHistory, xfercontract.MutationCleanup, xfercontract.MutationAbandon:
		return true
	}
	return false
}

// compatibleAdvance reconciles older primary rows with newer outbox/image
// responsibility. Revision precedence never permits a different canonical row.
// origin: hot-update S7 plan §5.1 / T22
func compatibleAdvance(old, next RowV1) bool {
	if old.Identity != next.Identity || old.EntryNonce != next.EntryNonce || old.Origin != next.Origin ||
		!reflect.DeepEqual(old.Permit, next.Permit) || old.Intent != next.Intent || !old.StartedAt.Equal(next.StartedAt) ||
		!reflect.DeepEqual(old.CanonicalStart, next.CanonicalStart) || !retentionAdvance(old, next) ||
		!reflect.DeepEqual(old.Settlement, next.Settlement) || phaseRank(next.Phase) < phaseRank(old.Phase) ||
		deliveryRank(next.StartDelivery) < deliveryRank(old.StartDelivery) ||
		deliveryRank(next.TerminalDelivery) < deliveryRank(old.TerminalDelivery) {
		return false
	}
	if (old.SourceSeal != nil && !reflect.DeepEqual(old.SourceSeal, next.SourceSeal)) ||
		(old.Grant != nil && !reflect.DeepEqual(old.Grant, next.Grant)) ||
		!compatibleOutcome(old, next) ||
		(old.CanonicalTerminal != nil && (!reflect.DeepEqual(old.CanonicalTerminal, next.CanonicalTerminal) || old.OutcomeUnknown != next.OutcomeUnknown)) {
		return false
	}
	if old.BudgetSeal != next.BudgetSeal {
		// Pull discovery may tighten the provisional ceiling once, always from
		// the original start. A reconnect cannot replenish RemainingNS.
		if old.SourceSeal != nil || old.Grant != nil || old.CanonicalTerminal != nil || next.SourceSeal == nil ||
			next.BudgetSeal.EffectiveBudgetNS > old.BudgetSeal.EffectiveBudgetNS ||
			next.BudgetSeal.RemainingNS > old.BudgetSeal.RemainingNS {
			return false
		}
	}
	for _, receipt := range old.HistoryReceipts {
		found := false
		for _, candidate := range next.HistoryReceipts {
			found = found || candidate == receipt
		}
		if !found {
			return false
		}
	}
	return true
}

// Exact history completion starts result retention once; it may be later than
// the byte deadline. START admission has no terminal result to retain, and no
// later callback, replay, source update or replacement owner may renew it.
// Both history slots must be exact before the original boot-bound retention
// deadline is installed; a later callback cannot replenish either clock.
// origin: hot-update S7 plan §1.2 / §5.1 / T13 / INTEG-E storage regression
// origin: hot-update S7 plan §4 / §5.1 / T39
func retentionAdvance(old, next RowV1) bool {
	if reflect.DeepEqual(old.Retention, next.Retention) {
		return true
	}
	return old.Retention.ResultUntil.IsZero() && old.Retention.Deadline == (xfercontract.BootDeadline{}) &&
		old.Retention.Identity == next.Retention.Identity && old.Retention.CreateUntil.Equal(next.Retention.CreateUntil) &&
		(old.StartDelivery != xfercontract.DeliveryHistoryConfirmed || old.TerminalDelivery != xfercontract.DeliveryHistoryConfirmed) &&
		next.StartDelivery == xfercontract.DeliveryHistoryConfirmed && next.TerminalDelivery == xfercontract.DeliveryHistoryConfirmed &&
		phaseRank(next.Phase) >= phaseRank(xfercontract.PhaseHistoryConfirmed) &&
		!next.Retention.ResultUntil.IsZero() && !next.Retention.ResultUntil.Before(next.Permit.CreateUntil) &&
		next.Retention.Deadline.BootID != "" && next.Retention.Deadline.BootID == next.Origin.BootID &&
		next.Retention.Deadline.Deadline > 0
}

func validateMutation(old *RowV1, next RowV1, kind xfercontract.LedgerMutationKind) error {
	if err := validateRow(next); err != nil {
		return err
	}
	if old == nil {
		if kind != xfercontract.MutationAdmit || next.Phase != xfercontract.PhaseOpen || next.Grant != nil ||
			next.CanonicalTerminal != nil || len(next.HistoryReceipts) != 0 || next.StartDelivery != xfercontract.DeliveryPending {
			return fmt.Errorf("%w: first revision must admit START", ErrConflict)
		}
		return nil
	}
	if !compatibleAdvance(*old, next) {
		return fmt.Errorf("%w: nonmonotonic ledger mutation", ErrConflict)
	}
	allowed := *old
	allowed.Revision = next.Revision
	switch kind {
	case xfercontract.MutationAdmit:
		return fmt.Errorf("%w: entry already admitted", ErrConflict)
	case xfercontract.MutationSourceSeal:
		if old.Grant != nil || old.CanonicalTerminal != nil || next.SourceSeal == nil {
			return fmt.Errorf("%w: source seal after decision", ErrConflict)
		}
		allowed.SourceSeal, allowed.BudgetSeal, allowed.Phase = next.SourceSeal, next.BudgetSeal, next.Phase
	case xfercontract.MutationReserve:
		if next.Grant == nil || next.Phase != xfercontract.PhaseCommitReserved {
			return fmt.Errorf("%w: reserve needs a retained grant", ErrConflict)
		}
		allowed.Grant, allowed.Phase = next.Grant, next.Phase
	case xfercontract.MutationDecide, xfercontract.MutationAbandon:
		if (next.CanonicalTerminal == nil && (kind != xfercontract.MutationDecide || next.ReceiverOutcome == nil || !validUnresolvedOutcome(next))) || next.OutcomeUnknown != (kind == xfercontract.MutationAbandon) {
			return fmt.Errorf("%w: invalid decision kind", ErrConflict)
		}
		allowed.ReceiverOutcome, allowed.CanonicalTerminal = next.ReceiverOutcome, next.CanonicalTerminal
		allowed.TerminalDelivery, allowed.Phase = next.TerminalDelivery, next.Phase
		allowed.Cleanup, allowed.OutcomeUnknown = next.Cleanup, next.OutcomeUnknown
	case xfercontract.MutationHistory:
		allowed.HistoryReceipts, allowed.StartDelivery = next.HistoryReceipts, next.StartDelivery
		allowed.TerminalDelivery, allowed.Phase = next.TerminalDelivery, next.Phase
		allowed.Retention = next.Retention
	case xfercontract.MutationCleanup:
		if old.StartDelivery != xfercontract.DeliveryHistoryConfirmed || old.TerminalDelivery != xfercontract.DeliveryHistoryConfirmed {
			return fmt.Errorf("%w: cleanup before exact history", ErrConflict)
		}
		allowed.Cleanup, allowed.Phase = next.Cleanup, next.Phase
	}
	// Comparing the entire row also rejects unknown mutation kinds and future
	// fields until this owner explicitly assigns their mutation responsibility.
	if kind != xfercontract.MutationSourceSeal && kind != xfercontract.MutationReserve &&
		kind != xfercontract.MutationDecide && kind != xfercontract.MutationAbandon &&
		kind != xfercontract.MutationHistory && kind != xfercontract.MutationCleanup {
		return fmt.Errorf("%w: unknown ledger mutation", ErrConflict)
	}
	if !reflect.DeepEqual(allowed, next) {
		return fmt.Errorf("%w: mutation changed unrelated responsibility", ErrConflict)
	}
	return nil
}

// A codec roundtrip detaches all byte slices/pointers and removes monotonic
// time metadata before comparisons with records recovered from disk.
func copyRow(row RowV1) (RowV1, error) {
	data, err := json.Marshal(row)
	if err != nil || len(data) > maxRowBytes {
		return RowV1{}, fmt.Errorf("%w: row cannot be encoded", ErrUnknown)
	}
	var cloned RowV1
	if err := json.Unmarshal(data, &cloned); err != nil {
		return RowV1{}, err
	}
	return cloned, nil
}

func encodeRow(row RowV1) ([]byte, error) {
	data, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(diskRecord{Schema: ledgerSchema, Digest: digest(data), Row: row})
	if len(encoded) > maxRowBytes {
		return nil, fmt.Errorf("%w: row exceeds codec limit", ErrUnknown)
	}
	return encoded, err
}

func decodeRow(data []byte) (RowV1, error) {
	var record diskRecord
	if len(data) > maxRowBytes || json.Unmarshal(data, &record) != nil || record.Schema != ledgerSchema {
		return RowV1{}, fmt.Errorf("%w: unreadable ledger schema", ErrUnknown)
	}
	body, err := json.Marshal(record.Row)
	if err != nil || record.Digest != digest(body) {
		return RowV1{}, fmt.Errorf("%w: ledger digest mismatch", ErrUnknown)
	}
	// Reject unknown/duplicate fields and trailing input rather than silently
	// dropping responsibility when a future or damaged row reaches this reader.
	encoded, err := encodeRow(record.Row)
	var compact bytes.Buffer
	if err != nil || json.Compact(&compact, data) != nil || !bytes.Equal(encoded, compact.Bytes()) {
		return RowV1{}, fmt.Errorf("%w: noncanonical ledger envelope", ErrUnknown)
	}
	return record.Row, validateRow(record.Row)
}
