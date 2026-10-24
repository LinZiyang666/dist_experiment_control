package xferimage

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// References close against full identity and revision, never just transfer ID.
// A sleeping import must protect pending grants/publications and BOTH cleanup
// directories before any eager scan or reaper sees the replacement backend.
// origin: hot-update S7 plan §5.1 / §5.3 / §8.2 / T20
func validateTracker(s xfercontract.TrackerState) error {
	if err := trackerBounds(s); err != nil {
		return err
	}
	rows, err := trackerRows(s)
	if err != nil {
		return err
	}
	// Each closure check reads the same row index; the order is the original
	// single-pass order, so the first refusal is unchanged.
	for _, check := range []func(xfercontract.TrackerState, map[xfercontract.Key]xfercontract.LedgerRow) error{
		trackerReaped, trackerWatchdogs, trackerPending, trackerGrantSlots, trackerCleanups, trackerCensus,
	} {
		if err := check(s, rows); err != nil {
			return err
		}
	}
	// Validate boot and enum closure even in nested cleanup consumers and config
	// attempts. A top-level-only boot check leaves a deadline-renewal loophole.
	return trackerValues(reflect.ValueOf(s), s.Origin.BootID)
}

// trackerBounds refuses the image header and every collection ceiling before
// any row index is built from the decoded input.
func trackerBounds(s xfercontract.TrackerState) error {
	if s.Schema != xfercontract.StateSchema || !trackerOrigin(s.Origin) || s.Generation < s.Origin.Generation ||
		!trackerAdmission(s.Admission, s.Generation) || !trackerAdmission(s.ProvisioningFence, s.Generation) ||
		len(s.Entries)+len(s.Reaped) > trackerMaxEntries || len(s.WatchdogDeadlines) > trackerMaxEntries ||
		len(s.PendingStart) > trackerMaxEntries || len(s.PendingTerminal) > trackerMaxEntries || len(s.GrantSlots) > trackerMaxEntries ||
		len(s.Cleanup) > trackerMaxEntries || len(s.ConfigAttempts) > trackerMaxEntries ||
		len(s.Census.Rows) > trackerMaxEntries || len(s.Census.Entries) > trackerMaxEntries || s.Census.Origin != s.Origin {
		return errTrackerImage
	}
	return nil
}

func trackerRows(s xfercontract.TrackerState) (map[xfercontract.Key]xfercontract.LedgerRow, error) {
	rows := make(map[xfercontract.Key]xfercontract.LedgerRow, len(s.Entries))
	for _, row := range s.Entries {
		key := trackerKey(row.Identity)
		if _, exists := rows[key]; exists || !trackerRow(row, s.Origin) || row.Revision > s.LedgerRevision {
			return nil, errTrackerImage
		}
		rows[key] = row
	}
	return rows, nil
}

func trackerReaped(s xfercontract.TrackerState, rows map[xfercontract.Key]xfercontract.LedgerRow) error {
	seen := make(map[xfercontract.Key]bool)
	for _, reaped := range s.Reaped {
		key := trackerKey(reaped.Identity)
		if _, live := rows[key]; live || seen[key] || !trackerIdentity(reaped.Identity) || reaped.At.IsZero() ||
			!trackerRetention(reaped.Retention, reaped.Identity, s.Origin.BootID) {
			return errTrackerImage
		}
		seen[key] = true
	}
	return nil
}

func trackerWatchdogs(s xfercontract.TrackerState, rows map[xfercontract.Key]xfercontract.LedgerRow) error {
	seen := make(map[xfercontract.Key]bool)
	for _, watchdog := range s.WatchdogDeadlines {
		key := trackerKey(watchdog.Identity)
		row, exists := rows[key]
		if !exists || seen[key] || row.Identity != watchdog.Identity || !trackerDeadline(watchdog.ByteDeadline, s.Origin.BootID) {
			return errTrackerImage
		}
		seen[key] = true
	}
	return nil
}

func trackerPending(s xfercontract.TrackerState, rows map[xfercontract.Key]xfercontract.LedgerRow) error {
	for _, pending := range []struct {
		records []xfercontract.CanonicalRecord
		start   bool
	}{{s.PendingStart, true}, {s.PendingTerminal, false}} {
		seen := make(map[xfercontract.Key]bool)
		for _, record := range pending.records {
			key := trackerKey(record.Identity)
			row, exists := rows[key]
			expected, delivery := row.CanonicalTerminal, row.TerminalDelivery
			if pending.start {
				expected, delivery = row.CanonicalStart, row.StartDelivery
			}
			if !exists || seen[key] || expected == nil || !trackerEqual(record, *expected) || delivery == xfercontract.DeliveryHistoryConfirmed {
				return errTrackerImage
			}
			seen[key] = true
		}
		// Dropping a pending record on import would leave the row live while
		// losing the publication owner that can ever discharge that protection.
		for key, row := range rows {
			record, delivery := row.CanonicalTerminal, row.TerminalDelivery
			if pending.start {
				record, delivery = row.CanonicalStart, row.StartDelivery
			}
			if record != nil && delivery != xfercontract.DeliveryHistoryConfirmed && !seen[key] {
				return errTrackerImage
			}
		}
	}
	return nil
}

func trackerGrantSlots(s xfercontract.TrackerState, rows map[xfercontract.Key]xfercontract.LedgerRow) error {
	seen := make(map[xfercontract.Key]bool)
	for _, slot := range s.GrantSlots {
		key := trackerKey(slot.Identity)
		row, exists := rows[key]
		// The reservation revision can precede later history/cleanup revisions;
		// retaining the original grant is not permission to rewrite its proof.
		if !exists || seen[key] || row.Identity != slot.Identity || row.Grant == nil || *row.Grant != slot.Grant ||
			slot.Revision == 0 || slot.Revision > row.Revision {
			return errTrackerImage
		}
		seen[key] = true
	}
	for key, row := range rows {
		if row.Grant != nil && row.Phase != xfercontract.PhaseSettled && !seen[key] {
			return errTrackerImage
		}
	}
	return nil
}

func trackerCleanups(s xfercontract.TrackerState, rows map[xfercontract.Key]xfercontract.LedgerRow) error {
	seen := make(map[xfercontract.Key]bool)
	for _, cleanup := range s.Cleanup {
		key := trackerKey(cleanup.Identity)
		row, exists := rows[key]
		if !exists || seen[key] || !trackerEqual(cleanup, row.Cleanup) {
			return errTrackerImage
		}
		seen[key] = true
	}
	for key, row := range rows {
		if trackerCleanupPending(row.Cleanup) && !seen[key] {
			return errTrackerImage
		}
	}
	return nil
}

func trackerCensus(s xfercontract.TrackerState, rows map[xfercontract.Key]xfercontract.LedgerRow) error {
	seen := make(map[xfercontract.Key]bool)
	for _, row := range s.Census.Rows {
		key := trackerKey(row.Identity)
		expected, exists := rows[key]
		if !exists || seen[key] || !trackerEqual(row, expected) {
			return errTrackerImage
		}
		seen[key] = true
	}
	if s.Census.Ready && s.Census.Complete && len(seen) != len(rows) {
		return errTrackerImage
	}
	seen = make(map[xfercontract.Key]bool)
	for _, protection := range s.Census.Entries {
		key := trackerKey(protection.Identity)
		row, exists := rows[key]
		if !exists || seen[key] || row.Identity != protection.Identity || row.Phase != protection.Phase ||
			protection.ObjectVersion != row.Identity.ObjectVersion || protection.OwnerGeneration == 0 || protection.OwnerGeneration > s.Generation ||
			!protection.CreationUntil.Equal(row.Permit.CreateUntil) || protection.WriterRetired != row.Cleanup.WriterFence.WriterRetired {
			return errTrackerImage
		}
		seen[key] = true
	}
	if s.Census.Ready && s.Census.Complete && len(seen) != len(rows) {
		return errTrackerImage
	}
	return nil
}

// trackerRow is the conjunction of the row's admission facts, retained
// evidence, phase shape and terminal decision, checked in that order.
func trackerRow(r xfercontract.LedgerRow, origin xfercontract.Origin) bool {
	return trackerRowAdmission(r, origin) && trackerRowEvidence(r) && trackerRowPhase(r) && trackerRowTerminal(r, origin)
}

func trackerRowAdmission(r xfercontract.LedgerRow, origin xfercontract.Origin) bool {
	if !trackerIdentity(r.Identity) || r.Origin != origin || r.Revision == 0 || r.EntryNonce != r.Identity.EntryNonce ||
		r.Permit.Identity != r.Identity || r.Permit.Origin != origin || r.Permit.PermitNonce == "" || r.Permit.Signature == "" ||
		r.Permit.IssuedAt.IsZero() || !r.Permit.CreateUntil.After(r.Permit.IssuedAt) || r.StartedAt.IsZero() ||
		!r.StartedAt.Equal(r.BudgetSeal.StartedAt) || !trackerBudget(r.BudgetSeal) ||
		(r.Intent.Verb != "push" && r.Intent.Verb != "pull") || (r.Intent.Tier != "a" && r.Intent.Tier != "b") ||
		(r.Intent.Tier == "a" && r.Identity.ObjectVersion != "") || (r.Intent.Tier == "b" && r.Identity.ObjectVersion == "") ||
		r.Intent.RateLimitBPS != r.BudgetSeal.RateLimitBPS || !trackerRowRetention(r, origin.BootID) ||
		!r.Retention.CreateUntil.Equal(r.Permit.CreateUntil) || r.Cleanup.Identity != r.Identity {
		return false
	}
	// The codec never re-derives a rate budget. It only rejects values outside
	// the frozen tier bounds, leaving P01's exact formula at the original start.
	// origin: hot-update S7 plan §1.2 / T20
	if r.Intent.Tier == "a" && (r.BudgetSeal.Size > 8<<20 || r.BudgetSeal.EffectiveBudgetNS != int64(30*time.Second)) ||
		r.Intent.Tier == "b" && r.BudgetSeal.EffectiveBudgetNS < int64(300*time.Second) {
		return false
	}
	return true
}

func trackerRowEvidence(r xfercontract.LedgerRow) bool {
	if r.SourceSeal != nil && (!r.BudgetSeal.SourceSealed || r.SourceSeal.Size != r.BudgetSeal.Size ||
		r.SourceSeal.RateLimitBPS != r.BudgetSeal.RateLimitBPS || !trackerSHA(r.SourceSeal.SHA256) || r.SourceSeal.SourceFingerprint == "" ||
		(r.Intent.Tier == "b" && (r.SourceSeal.ChunkSize == 0 || uint64(r.SourceSeal.Chunks) !=
			(uint64(r.SourceSeal.Size)+uint64(r.SourceSeal.ChunkSize)-1)/uint64(r.SourceSeal.ChunkSize)))) {
		return false
	}
	if r.Grant != nil && !trackerGrant(*r.Grant, r.Identity) {
		return false
	}
	if r.ReceiverOutcome != nil {
		outcome := r.ReceiverOutcome
		// Administrative abandonment has no primitive evidence to invent. Its
		// inflight placeholder may be empty; actual landed/known results may not.
		// origin: S7 internal review rv-recovery F08 / FIX-TERM codec handoff
		if outcome.Primitive == "" && (!r.OutcomeUnknown || outcome.Outcome != xfercontract.OutcomeInflight) {
			return false
		}
		if r.Grant != nil && outcome.Grant != *r.Grant || outcome.Bytes < 0 || outcome.Bytes > r.BudgetSeal.Size ||
			(outcome.Outcome == xfercontract.OutcomeLanded && (r.Grant == nil || !trackerSHA(outcome.SHA256))) {
			return false
		}
	}
	return true
}

func trackerRowPhase(r xfercontract.LedgerRow) bool {
	if r.Phase == xfercontract.PhaseCommitReserved && r.Grant == nil {
		return false
	}
	if r.Phase == xfercontract.PhaseOpen || r.Phase == xfercontract.PhasePrepared ||
		r.Phase == xfercontract.PhaseSending || r.Phase == xfercontract.PhaseReceiving {
		if r.Grant != nil || r.ReceiverOutcome != nil || r.CanonicalTerminal != nil {
			return false
		}
	}
	if !trackerPublication(r.CanonicalStart, r.StartDelivery, r.HistoryReceipts, r.Identity, true) ||
		!trackerPublication(r.CanonicalTerminal, r.TerminalDelivery, r.HistoryReceipts, r.Identity, false) {
		return false
	}
	if r.Phase == xfercontract.PhaseDecided || r.Phase == xfercontract.PhaseTerminalPending ||
		r.Phase == xfercontract.PhaseHistoryConfirmed || r.Phase == xfercontract.PhaseCleanupPending || r.Phase == xfercontract.PhaseSettled {
		if r.CanonicalTerminal == nil {
			return false
		}
	}
	if r.Phase == xfercontract.PhaseHistoryConfirmed || r.Phase == xfercontract.PhaseCleanupPending || r.Phase == xfercontract.PhaseSettled {
		if r.StartDelivery != xfercontract.DeliveryHistoryConfirmed || r.TerminalDelivery != xfercontract.DeliveryHistoryConfirmed {
			return false
		}
	}
	return true
}

func trackerRowTerminal(r xfercontract.LedgerRow, origin xfercontract.Origin) bool {
	if r.CanonicalTerminal != nil && r.CanonicalTerminal.Kind == "complete" &&
		(r.ReceiverOutcome == nil || r.ReceiverOutcome.Outcome != xfercontract.OutcomeLanded || !r.ReceiverOutcome.ParentSynced || r.OutcomeUnknown) {
		return false
	}
	// A watchdog cannot infer failure after a durable grant. Only explicit
	// operator abandonment may settle a receiver whose primitive is unresolved.
	// origin: hot-update S7 plan §4 / §5.1 / lead LQ1 / T20
	if r.CanonicalTerminal != nil && r.CanonicalTerminal.Kind == "failed" && r.Grant != nil && !r.OutcomeUnknown &&
		(r.ReceiverOutcome == nil || r.ReceiverOutcome.Outcome == xfercontract.OutcomeInflight) {
		return false
	}
	if r.OutcomeUnknown && (r.CanonicalTerminal == nil || r.CanonicalTerminal.Kind != "failed") {
		return false
	}
	if r.OutcomeUnknown {
		// The audit producer owns the shared code field; a private reason field
		// would refuse its exact canonical bytes at the image boundary.
		// origin: S7 internal review rv-recovery F08 / FIX-TERM codec handoff
		var abandonment struct {
			Code           string `json:"code"`
			OutcomeUnknown bool   `json:"outcome_unknown"`
		}
		if json.Unmarshal(r.CanonicalTerminal.Bytes, &abandonment) != nil ||
			abandonment.Code != xfercontract.ReasonSettlementAbandoned || !abandonment.OutcomeUnknown {
			return false
		}
	}
	if r.Phase == xfercontract.PhaseSettled && trackerCleanupPending(r.Cleanup) {
		return false
	}
	return r.Settlement == nil || r.Settlement.Identity == r.Identity && r.Settlement.Origin == origin &&
		r.Settlement.ReceiverNkey != "" && r.Settlement.Issuer != "" && r.Settlement.Nonce != "" && r.Settlement.Signature != ""
}

func trackerPublication(record *xfercontract.CanonicalRecord, delivery xfercontract.AuditDelivery, receipts []xfercontract.HistoryReceipt, id xfercontract.Identity, start bool) bool {
	if record == nil {
		return delivery == "" || delivery == xfercontract.DeliveryPending
	}
	if record.Identity != id || !trackerRecord(*record) || (start && record.Kind != "start") ||
		(!start && record.Kind != "complete" && record.Kind != "failed") || delivery == "" {
		return false
	}
	count := 0
	for _, receipt := range receipts {
		if receipt.RecordDigest == record.Digest {
			count++
		}
	}
	return count <= 1 && (delivery != xfercontract.DeliveryHistoryConfirmed || count == 1)
}

func trackerRecord(r xfercontract.CanonicalRecord) bool {
	digest := sha256.Sum256(r.Bytes)
	var record struct {
		Kind string `json:"kind"`
	}
	return trackerIdentity(r.Identity) && r.ReqID != "" && len(r.Bytes) > 0 && len(r.Bytes) <= 1<<20 &&
		json.Unmarshal(r.Bytes, &record) == nil && record.Kind == r.Kind && r.Digest == hex.EncodeToString(digest[:])
}

func trackerKey(id xfercontract.Identity) xfercontract.Key {
	return xfercontract.Key{SID: id.SID, TransferID: id.TransferID}
}

func trackerIdentity(id xfercontract.Identity) bool {
	return id.SID != "" && id.NID != "" && id.ActorFP != "" && id.AgentInstance != "" &&
		id.TransferID != "" && id.EntryNonce != "" && trackerSHA(id.IntentDigest)
}

func trackerOrigin(o xfercontract.Origin) bool {
	return o.BrokerID != "" && o.InstanceID != "" && o.BootID != "" && o.Generation != 0
}

func trackerAdmission(a xfercontract.AdmissionState, generation uint64) bool {
	return a.Closed && a.Inflight == 0 && a.Generation == generation
}

func trackerGrant(g xfercontract.CommitGrant, id xfercontract.Identity) bool {
	return g.Identity == id && g.GrantNonce != "" && g.ReceiverInstance != "" && g.TerminalID != ""
}

func trackerBudget(b xfercontract.BudgetSeal) bool {
	return !b.StartedAt.IsZero() && b.Size >= 0 && b.Size <= 2<<30 && b.RateLimitBPS >= 0 &&
		b.EffectiveBudgetNS > 0 && b.EffectiveBudgetNS <= int64(2108*time.Second) &&
		b.RemainingNS >= 0 && b.RemainingNS <= b.EffectiveBudgetNS
}

func trackerDeadline(d xfercontract.BootDeadline, boot string) bool {
	return d.BootID == boot && d.Deadline > 0
}

func trackerRetention(r xfercontract.RetentionState, id xfercontract.Identity, boot string) bool {
	return r.Identity == id && !r.CreateUntil.IsZero() && !r.ResultUntil.IsZero() &&
		!r.ResultUntil.Before(r.CreateUntil) && trackerDeadline(r.Deadline, boot)
}

// Result retention starts only after exact history confirmation. A live row
// carries no result clock; importing it must not manufacture a new deadline.
// origin: hot-update S7 plan §2.3 / §8.2 / T20
func trackerRowRetention(row xfercontract.LedgerRow, boot string) bool {
	r := row.Retention
	if r.ResultUntil.IsZero() && r.Deadline == (xfercontract.BootDeadline{}) {
		return r.Identity == row.Identity && !r.CreateUntil.IsZero() &&
			row.Phase != xfercontract.PhaseHistoryConfirmed && row.Phase != xfercontract.PhaseCleanupPending && row.Phase != xfercontract.PhaseSettled
	}
	return trackerRetention(r, row.Identity, boot)
}

func trackerCleanupPending(c xfercontract.CleanupState) bool {
	return c.TmpUnlinkPending || c.DirectorySyncPending || c.ObjectPurgePending ||
		c.PrimaryRetirePending || c.OutboxRetirePending || len(c.Consumers) > 0
}

func trackerSHA(s string) bool {
	decoded, err := hex.DecodeString(s)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == s
}

func trackerEqual(a, b any) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	return err == nil && bytes.Equal(left, right)
}

// This closed walk includes optional nested records, without rewriting any
// field. Zero is permitted only where the parent explicitly has no evidence.
// origin: hot-update S7 plan §8.1 / T53
func trackerValues(v reflect.Value, boot string) error {
	if v.Type() == reflect.TypeFor[time.Time]() {
		return nil
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return trackerValues(v.Elem(), boot)
	}
	valid := true
	switch value := v.Interface().(type) {
	case xfercontract.RetentionState:
		// Parent row/reaped validation establishes whether the clock may be absent.
		if value.ResultUntil.IsZero() && value.Deadline == (xfercontract.BootDeadline{}) {
			return nil
		}
	case xfercontract.BootDeadline:
		valid = trackerDeadline(value, boot)
	case xfercontract.Phase:
		valid = trackerPhase(value)
	case xfercontract.AuditDelivery:
		valid = value == "" || value == xfercontract.DeliveryPending || value == xfercontract.DeliveryRaftCommitted || value == xfercontract.DeliveryHistoryConfirmed
	case xfercontract.PrimitiveOutcome:
		valid = value == xfercontract.OutcomeNotEntered || value == xfercontract.OutcomeInflight || value == xfercontract.OutcomeLanded
	case xfercontract.CommitPrimitive:
		// trackerRowEvidence confines the zero value to an abandoned inflight
		// outcome, where the original receiver supplied no syscall evidence.
		valid = value == "" || value == xfercontract.PrimitiveRename || value == xfercontract.PrimitiveLink
	case xfercontract.ReaderPhase:
		valid = value == xfercontract.ReaderOpen || value == xfercontract.ReaderFetching || value == xfercontract.ReaderDirty || value == xfercontract.ReaderDeletePending || value == xfercontract.ReaderStopped
	case xfercontract.HistoryReceipt:
		valid = value.Stream != "" && value.Sequence > 0 && trackerSHA(value.RecordDigest)
	case xfercontract.ReaderState:
		valid = trackerReader(value)
	case xfercontract.CleanupState:
		for _, consumer := range value.Consumers {
			if consumer.ObjectSpec.Identity != value.Identity {
				valid = false
			}
		}
	case xfercontract.RetirementFence:
		valid = !value.Purged || value.WriterRetired && value.ConsumersRetired && value.Receipt != nil
	case xfercontract.ConfigAttempt:
		valid = trackerConfigAttempt(value, boot)
	}
	if !valid {
		return errTrackerImage
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := trackerValues(v.Field(i), boot); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			for i := 0; i < v.Len(); i++ {
				if err := trackerValues(v.Index(i), boot); err != nil {
					return err
				}
			}
		}
	case reflect.Bool, reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint32, reflect.Uint64, reflect.String:
		// Scalar leaves of the measured closure: their value checks ran above.
	case reflect.Invalid, reflect.Int8, reflect.Int16, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.UnsafePointer:
		// trackerMeasure refuses every one of these kinds before validation, and
		// pointers were dereferenced above. Keep the walk closed if that changes.
		return errTrackerImage
	}
	return nil
}

func trackerReader(r xfercontract.ReaderState) bool {
	spec, checkpoint := r.ObjectSpec, r.SinkCheckpoint
	if !trackerIdentity(spec.Identity) || spec.Identity.ObjectVersion == "" || spec.Bucket == "" || spec.Stream == "" ||
		spec.StreamCreated == "" || !trackerSHA(spec.SHA256) || spec.Size < 0 || spec.Size > 2<<30 || spec.ChunkSize == 0 ||
		uint64(spec.Chunks) != (uint64(spec.Size)+uint64(spec.ChunkSize)-1)/uint64(spec.ChunkSize) ||
		checkpoint.ChunkIndex > spec.Chunks || checkpoint.DurableOffset < 0 || checkpoint.DurableOffset > spec.Size ||
		r.ConsumerIdentity == "" || r.ConnectionIdentity == "" || (r.DeletePending && r.Phase != xfercontract.ReaderDeletePending) {
		return false
	}
	expected := int64(checkpoint.ChunkIndex) * int64(spec.ChunkSize)
	if expected > spec.Size {
		expected = spec.Size
	}
	if checkpoint.DurableOffset != expected || (checkpoint.ChunkIndex == 0) != (checkpoint.LastConsumedStreamSeq == 0) {
		return false
	}
	// The hash marshal format records the byte count in its final eight bytes.
	// Restore it purely for validation; never seek or repair the shared sink.
	hash, ok := sha256.New().(encoding.BinaryUnmarshaler)
	if !ok || len(checkpoint.SHA256State) < 8 || hash.UnmarshalBinary(checkpoint.SHA256State) != nil {
		return false
	}
	return binary.BigEndian.Uint64(checkpoint.SHA256State[len(checkpoint.SHA256State)-8:]) == uint64(checkpoint.DurableOffset)
}

func trackerPhase(p xfercontract.Phase) bool {
	switch p {
	case xfercontract.PhaseOpen, xfercontract.PhasePrepared, xfercontract.PhaseSending, xfercontract.PhaseReceiving,
		xfercontract.PhaseCommitReserved, xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending,
		xfercontract.PhaseHistoryConfirmed, xfercontract.PhaseCleanupPending, xfercontract.PhaseSettled:
		return true
	default:
		return false
	}
}

func trackerConfigAttempt(a xfercontract.ConfigAttempt, boot string) bool {
	if a.Patch.Bucket == "" || a.Patch.Bucket != a.Proof.Bucket || a.Proof.BootID != boot ||
		a.Proof.BrokerID == "" || a.Proof.InstanceID == "" || a.Proof.Nonce == "" || a.Proof.Generation == 0 ||
		a.Patch.Replicas < 1 || a.Patch.Replicas > 3 || a.Patch.MaxBytes < 0 || a.Patch.Duplicates < 0 || a.Patch.RecoveryHorizon < 0 ||
		!controlbudget.ValidPhase(a.Attempt.Phase) || a.Attempt.Ordinal == 0 || a.Attempt.LogicalID == "" ||
		a.Attempt.StartedMonoNS <= 0 || a.Attempt.FinishedMonoNS < 0 ||
		(a.Attempt.FinishedMonoNS != 0 && a.Attempt.FinishedMonoNS < a.Attempt.StartedMonoNS) || a.Attempt.BackoffNS < 0 ||
		(a.SentUnknown && (!a.Attempt.Sent || a.Proof.RequestsJoined)) {
		return false
	}
	return a.Seal == nil || a.Seal.Proof == a.Proof && a.Seal.Bucket == a.Patch.Bucket &&
		a.Seal.Stream != "" && a.Seal.StreamCreated != "" && trackerSHA(a.Seal.ConfigDigest) &&
		a.Seal.MaxBytes > 0 && a.Seal.Replicas > 0 && a.Seal.Duplicates >= 0 && a.Seal.MaxAge >= 0 &&
		a.Seal.MaxPayload > 0 && a.Seal.MaxMsgSize > 0
}
