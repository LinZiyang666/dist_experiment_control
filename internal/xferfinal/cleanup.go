package xferfinal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type settlementError struct{ code, detail string }

func (e settlementError) Error() string { return e.code + ": " + e.detail }

func settlementFault(code, detail string) error { return settlementError{code, detail} }

func validateRecords(row xfercontract.LedgerRow) error {
	for _, rec := range []*xfercontract.CanonicalRecord{row.CanonicalStart, row.CanonicalTerminal} {
		if rec == nil {
			continue
		}
		digest := sha256.Sum256(rec.Bytes)
		if rec.Identity != row.Identity || rec.ReqID == "" || len(rec.Bytes) == 0 ||
			rec.Digest != hex.EncodeToString(digest[:]) {
			return errors.New("canonical record identity or digest differs")
		}
		var detail struct {
			Kind           string `json:"kind"`
			OutcomeUnknown bool   `json:"outcome_unknown"`
		}
		if json.Unmarshal(rec.Bytes, &detail) != nil || detail.Kind != rec.Kind ||
			(rec == row.CanonicalTerminal && detail.OutcomeUnknown != row.OutcomeUnknown) {
			return errors.New("canonical record content differs")
		}
	}
	if row.CanonicalStart.Kind != "start" || (row.CanonicalTerminal != nil &&
		row.CanonicalTerminal.Kind != "complete" && row.CanonicalTerminal.Kind != "failed") {
		return errors.New("canonical record kind differs")
	}
	if row.OutcomeUnknown && (row.CanonicalTerminal == nil || row.CanonicalTerminal.Kind != "failed") {
		return errors.New("unknown abandonment outcome must remain failed")
	}
	// Imported receipt state must agree with the same two canonical records.
	// A second cached proof, even at another physical sequence, is a conflict.
	// origin: hot-update S7 plan §5.1–§5.2 / T21
	seen := make(map[string]bool, 2)
	for _, receipt := range row.HistoryReceipts {
		matches := receipt.RecordDigest == row.CanonicalStart.Digest ||
			(row.CanonicalTerminal != nil && receipt.RecordDigest == row.CanonicalTerminal.Digest)
		if !matches || receipt.Stream == "" || receipt.Sequence == 0 || seen[receipt.RecordDigest] {
			return errors.New("cached history receipt conflicts with canonical decision")
		}
		seen[receipt.RecordDigest] = true
	}
	return nil
}

func sameDecision(before, after xfercontract.LedgerRow) error {
	if before.Identity != after.Identity || before.EntryNonce != after.EntryNonce || before.Origin != after.Origin ||
		after.Revision < before.Revision || before.OutcomeUnknown != after.OutcomeUnknown {
		return errors.New("finalizer revision or owner changed")
	}
	for i, rec := range []*xfercontract.CanonicalRecord{before.CanonicalStart, before.CanonicalTerminal} {
		current := []*xfercontract.CanonicalRecord{after.CanonicalStart, after.CanonicalTerminal}[i]
		if rec == nil {
			// A START-only row may acquire its terminal while publication completes.
			continue
		}
		if current == nil || rec.Identity != current.Identity || rec.Kind != current.Kind ||
			rec.ReqID != current.ReqID || rec.Digest != current.Digest || !bytes.Equal(rec.Bytes, current.Bytes) {
			return errors.New("canonical decision changed during acknowledgement")
		}
	}
	for _, prior := range before.HistoryReceipts {
		found := false
		for _, current := range after.HistoryReceipts {
			found = found || prior == current
		}
		if !found {
			return errors.New("durable history receipt disappeared during acknowledgement")
		}
	}
	return validateRecords(after)
}

func receiptFor(row xfercontract.LedgerRow, rec *xfercontract.CanonicalRecord) (xfercontract.HistoryReceipt, bool) {
	if rec != nil {
		for _, receipt := range row.HistoryReceipts {
			if receipt.RecordDigest == rec.Digest && receipt.Stream != "" && receipt.Sequence != 0 {
				return receipt, true
			}
		}
	}
	return xfercontract.HistoryReceipt{}, false
}

// Confirm is deliberately called for a cached receipt too. Its complete scan
// excludes an opposite/duplicate physical terminal; a cached boolean cannot.
// origin: hot-update S7 plan §5.2 / T21
func (o *owner) confirm(row xfercontract.LedgerRow, rec xfercontract.CanonicalRecord, deadline controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	if err := o.check(deadline); err != nil {
		return row, err
	}
	receipt, err := o.cfg.History.Confirm(rec, deadline)
	if err != nil {
		return row, err
	}
	if receipt.Stream == "" || receipt.Sequence == 0 || receipt.RecordDigest != rec.Digest {
		return row, settlementFault(xfercontract.CodeTerminalConflict, "history receipt is not exact")
	}
	if prior, found := receiptFor(row, &rec); found {
		if prior != receipt {
			return row, settlementFault(xfercontract.CodeTerminalConflict, "stored history receipt differs")
		}
		key := xfercontract.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}
		current, readErr := o.read(key, row.Identity, deadline)
		if readErr != nil {
			return row, readErr
		}
		return current, sameDecision(row, current)
	}
	if err = o.check(deadline); err != nil {
		return row, err
	}
	key := xfercontract.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}
	if err = o.cfg.Decisions.AcceptHistory(key, row.EntryNonce, row.Revision, receipt, deadline); err != nil {
		return row, err
	}
	current, err := o.read(key, row.Identity, deadline)
	if err != nil {
		return row, err
	}
	if err = sameDecision(row, current); err != nil {
		return row, err
	}
	if persisted, found := receiptFor(current, &rec); !found || persisted != receipt {
		return row, errors.New("history acknowledgement is not durable")
	}
	o.observe(xfercontract.ObserveHistory, current, receipt.Sequence, "")
	return current, nil
}

func terminalID(id xfercontract.Identity) string {
	digest := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	return hex.EncodeToString(digest[:])
}

func rowStatus(row xfercontract.LedgerRow) xfercontract.Status {
	status := xfercontract.Status{State: xfercontract.StatusLive, Identity: row.Identity,
		Origin: row.Origin, Budget: row.BudgetSeal, Phase: row.Phase, CleanupPending: true}
	_, start := receiptFor(row, row.CanonicalStart)
	receipt, terminal := receiptFor(row, row.CanonicalTerminal)
	if !start || !terminal {
		return status
	}
	if validateRecords(row) != nil {
		return status
	}
	// Read the already-canonical bytes for display, never re-encode them or
	// derive a fresh timestamp/ReqID. The tracker/history ports own that schema.
	var detail struct {
		Bytes          int64  `json:"bytes"`
		SHA256         string `json:"sha256"`
		Code           string `json:"code"`
		OutcomeUnknown bool   `json:"outcome_unknown"`
	}
	if json.Unmarshal(row.CanonicalTerminal.Bytes, &detail) != nil {
		return status
	}
	status.State = xfercontract.StatusTerminal
	status.Terminal = &xfercontract.TerminalReceipt{TerminalID: terminalID(row.Identity),
		Kind: row.CanonicalTerminal.Kind, CanonicalDigest: row.CanonicalTerminal.Digest,
		ReqID: row.CanonicalTerminal.ReqID, HistoryStream: receipt.Stream, HistorySeq: receipt.Sequence,
		Bytes: detail.Bytes, SHA256: detail.SHA256, Code: detail.Code, OutcomeUnknown: detail.OutcomeUnknown}
	return status
}

// Cleanup requires positive evidence from all data owners. History confirmation
// authorizes settlement, but cannot substitute for closing a receiver gate or
// retiring a writer's already-sent attempts.
// origin: hot-update S7 plan §2.3 / §4 / T21
func (o *owner) cleanup(row xfercontract.LedgerRow, deadline controlbudget.Deadline) (xfercontract.RetirementProof, error) {
	proof := xfercontract.RetirementProof{Identity: row.Identity, Origin: row.Origin,
		OwnerGeneration: cleanupGeneration(o.cfg.Decisions, row), EntryNonce: row.EntryNonce, Revision: row.Revision,
		CreationUntil: row.Permit.CreateUntil, WriterRetired: row.Cleanup.WriterFence.WriterRetired,
		ConsumersRetired: row.Cleanup.WriterFence.ConsumersRetired}
	var start, terminal bool
	proof.StartReceipt, start = receiptFor(row, row.CanonicalStart)
	proof.TerminalReceipt, terminal = receiptFor(row, row.CanonicalTerminal)
	// Writer/consumer retirement is established by the object retirer's fence
	// (validateCleanup requires it in the cleanup receipt). Requiring it from
	// the row first was circular: only AcceptCleanup — after Retire — ever
	// records it there, so no production row could settle. The proof handed to
	// the retirer still states only what the row already proves.
	// origin: hot-update S7 plan §2.3 / T21 — S7 production activation
	if !start || !terminal {
		return proof, errors.New("history, writer or consumer retirement remains pending")
	}
	for _, consumer := range row.Cleanup.Consumers {
		if consumer.Phase != xfercontract.ReaderStopped || consumer.DirtyBarrier || consumer.DeletePending ||
			consumer.ObjectSpec.Identity != row.Identity {
			return proof, errors.New("original reader consumer retirement remains pending")
		}
	}
	if err := o.check(deadline); err != nil {
		return proof, err
	}
	// The serialized administrative decision closes the original terminal slot
	// without claiming that its primitive did or did not run. Requiring another
	// answer from that permanently absent receiver would defeat LQ1's exit.
	// Object retirement still has to prove the reachable writer and consumers
	// joined, and fence late publishes; history alone cannot prove those facts.
	// origin: S7 internal review rv-wiring F5 / rv-recovery F08
	if row.OutcomeUnknown {
		var audit struct {
			Kind, Code     string
			OutcomeUnknown bool `json:"outcome_unknown"`
		}
		if json.Unmarshal(row.CanonicalTerminal.Bytes, &audit) != nil || audit.Kind != "failed" ||
			audit.Code != xfercontract.ReasonSettlementAbandoned || !audit.OutcomeUnknown {
			return proof, settlementFault(xfercontract.CodeTerminalConflict, "administrative retirement lacks an exact abandonment")
		}
		proof.ReceiverRetired = true // Administrative release, never a physical no-effect receipt.
		return o.retireObject(row, proof, deadline)
	}
	outcome, err := o.cfg.ResolveReceiver.Resolve(row.Identity, deadline)
	if err != nil {
		return proof, err
	}
	if row.Grant != nil && (outcome.Grant != *row.Grant || outcome.Grant.Identity != row.Identity ||
		outcome.Grant.TerminalID != terminalID(row.Identity)) {
		return proof, settlementFault(xfercontract.CodeTerminalConflict, "receiver grant differs")
	}
	if row.Grant == nil && outcome.Grant != (xfercontract.CommitGrant{}) {
		return proof, settlementFault(xfercontract.CodeTerminalConflict, "unexpected receiver grant")
	}
	if decided := row.ReceiverOutcome; decided != nil &&
		(decided.Grant != outcome.Grant || decided.Primitive != outcome.Primitive || decided.Outcome != outcome.Outcome ||
			decided.Bytes != outcome.Bytes || decided.SHA256 != outcome.SHA256) {
		return proof, settlementFault(xfercontract.CodeTerminalConflict, "receiver outcome differs from durable decision")
	}
	switch outcome.Outcome {
	case xfercontract.OutcomeNotEntered:
		if outcome.TmpCleanupPending {
			return proof, errors.New("receiver tmp cleanup remains pending")
		}
		if row.CanonicalTerminal.Kind != "failed" {
			return proof, settlementFault(xfercontract.CodeTerminalConflict, "complete without a landed receiver")
		}
	case xfercontract.OutcomeLanded:
		if !outcome.ParentSynced || outcome.TmpCleanupPending {
			return proof, settlementFault(xfercontract.CodeDurabilityUnknown, "receiver directory or tmp cleanup remains pending")
		}
		if row.Grant == nil || (row.CanonicalTerminal.Kind != "complete" && !row.OutcomeUnknown) {
			return proof, settlementFault(xfercontract.CodeTerminalConflict, "landed receiver contradicts terminal")
		}
		if row.CanonicalTerminal.Kind == "complete" {
			receipt := rowStatus(row).Terminal
			if receipt == nil || receipt.Bytes != outcome.Bytes || receipt.SHA256 != outcome.SHA256 {
				return proof, settlementFault(xfercontract.CodeTerminalConflict, "landed bytes differ from canonical terminal")
			}
		}
	case xfercontract.OutcomeInflight:
		// Named so a future PrimitiveOutcome member cannot inherit this refusal
		// silently; the default still refuses any unknown persisted value.
		// origin: hot-update S7 plan §11.2 / T52 (enum default gate)
		return proof, settlementFault(xfercontract.CodeCommitInflight, "receiver outcome remains unresolved")
	default:
		return proof, settlementFault(xfercontract.CodeCommitInflight, "receiver outcome remains unresolved")
	}
	proof.ReceiverRetired = true
	return o.retireObject(row, proof, deadline)
}

// Both receiver-confirmed and administrative retirement require the same
// positive data-owner receipt before any ledger responsibility is consumed.
func (o *owner) retireObject(row xfercontract.LedgerRow, proof xfercontract.RetirementProof, deadline controlbudget.Deadline) (xfercontract.RetirementProof, error) {
	if err := o.check(deadline); err != nil {
		return proof, err
	}
	cleanup, err := o.cfg.Objects.Retire(row.Identity, proof, deadline)
	if err != nil {
		return proof, err
	}
	if err = validateCleanup(row, cleanup, proof.OwnerGeneration); err != nil {
		return proof, err
	}
	proof.Cleanup = cleanup
	// The ledger's consume proof now carries the fence-established retirement.
	proof.WriterRetired, proof.ConsumersRetired = cleanup.WriterRetired, cleanup.ConsumersRetired
	return proof, nil
}

// The current decision binding owns cleanup after a cut. Permit origin stays
// unchanged so generations cannot redirect an old entry to another broker.
// origin: hot-update S7 plan §8.2 / T21
func cleanupGeneration(decisions xfercontract.DecisionPort, row xfercontract.LedgerRow) uint64 {
	if owner, ok := decisions.(interface{ OwnerGeneration() uint64 }); ok {
		return owner.OwnerGeneration()
	}
	return row.Origin.Generation
}

func validateCleanup(row xfercontract.LedgerRow, cleanup xfercontract.CleanupReceipt, generations ...uint64) error {
	generation := row.Origin.Generation
	if len(generations) != 0 {
		generation = generations[0]
	}
	if cleanup.Identity != row.Identity || cleanup.OwnerGeneration != generation || cleanup.Digest == "" ||
		!cleanup.WriterRetired || !cleanup.ConsumersRetired || !cleanup.TmpUnlinked || !cleanup.DirectorySynced {
		return errors.New("cleanup receipt is incomplete or belongs to another owner")
	}
	// Tier A has no OBJ. Tier B needs a real fence ACK later than every old
	// attempt expectation, followed by an exact purge of the original NUID.
	// origin: hot-update S7 plan §2.3 / T21
	if row.Identity.ObjectVersion != "" && (!cleanup.ObjectPurged || cleanup.FenceSequence <= row.Cleanup.WriterFence.ExpectedStreamSeq) {
		return errors.New("original object purge or late-publish fence remains pending")
	}
	return nil
}

// RetireLegacyOrphan implements the explicitly retained cap0 age/uptime policy.
// It has no v1 grant or writer state and must never manufacture either. Raw
// metadata plus every chunk identify legacy storage; complete fresh owner and
// ledger censuses exclude live responsibility before the exact NUID is purged.
// origin: hot-update S7 plan §5.3 / T29 / INTEG-R additive P00 amendment
func (o *owner) RetireLegacyOrphan(candidate xfercontract.LegacyOrphanCandidate, deadline controlbudget.Deadline) (xfercontract.LegacyOrphanReceipt, error) {
	var receipt xfercontract.LegacyOrphanReceipt
	if err := o.enter(deadline); err != nil {
		return receipt, err
	}
	defer o.leave()
	cfg := o.cfg.LegacyOrphans
	if cfg == nil || cfg.JS == nil || cfg.Owners == nil || o.cfg.UTC == nil || o.born.IsZero() ||
		!strings.HasPrefix(candidate.Bucket, "xfer-") || candidate.Stream != "OBJ_"+candidate.Bucket ||
		candidate.StreamCreated == "" || candidate.MetadataSequence == 0 || candidate.MetadataDigest == "" ||
		!legacyObjectToken(candidate.Name) || !legacyObjectToken(candidate.NUID) {
		return receipt, errors.New("legacy orphan retirement bindings or candidate are incomplete")
	}
	remaining, err := o.remaining(deadline)
	if err != nil {
		return receipt, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-none: ctx-free finalizer port; Stop joins this bounded operation.
	defer cancel()
	if err = o.legacyOrphanAbsent(candidate, deadline); err != nil {
		return receipt, err
	}
	retirement, err := inspectLegacyOrphan(ctx, o, candidate)
	if err != nil {
		return receipt, err
	}
	if err = fenceLegacyOrphan(ctx, cfg.JS, retirement); err != nil {
		return receipt, err
	}
	stream, raw := retirement.stream, retirement.raw
	subject, tombstone, chunks := retirement.subject, retirement.tombstone, retirement.chunks
	// Recheck the current owners and stream after the fence. A replacement
	// metadata row, new consumer, or newly retained responsibility blocks purge.
	if err = o.legacyOrphanAbsent(candidate, deadline); err != nil {
		return receipt, err
	}
	current, err := stream.Info(ctx)
	if err != nil {
		return receipt, err
	}
	if current.Created.UTC().Format(time.RFC3339Nano) != candidate.StreamCreated || current.State.Consumers != 0 {
		return receipt, errors.New("legacy orphan stream changed before purge")
	}
	latest, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return receipt, err
	}
	if latest.Sequence != raw.Sequence || !bytes.Equal(latest.Data, tombstone) {
		return receipt, errors.New("legacy orphan was replaced before purge")
	}
	if err = stream.Purge(ctx, jetstream.WithPurgeSubject(chunks), jetstream.WithPurgeSequence(raw.Sequence+1)); err != nil {
		return receipt, err
	}
	// A filtered purge can leave newer cap0 chunks beyond the fence. They are
	// unknown late work, not proof of successful physical retirement.
	// origin: hot-update S7 plan §5.3 / T29 / INTEG-R purge readback guard
	if _, err = stream.GetLastMsgForSubject(ctx, chunks); !errors.Is(err, jetstream.ErrMsgNotFound) {
		return receipt, errors.Join(err, errors.New("legacy orphan chunks remain after purge"))
	}
	if err = completeLegacyOrphan(ctx, cfg.JS, retirement, current); err != nil {
		return receipt, err
	}
	return xfercontract.LegacyOrphanReceipt{Candidate: candidate, FenceSequence: raw.Sequence}, nil
}

func legacyObjectToken(value string) bool {
	return value != "" && !strings.ContainsAny(value, ".*> \t\r\n")
}

// Checking only the last chunk would misclassify a mixed/corrupt v1 version
// whose tail lost its marker. Initial retirement requires the entire SDK body;
// a retained legacy tombstone may have already purged some or all of it.
// origin: hot-update S7 plan §5.3 / T29 / INTEG-R legacy classification guard
func validateLegacyChunks(ctx context.Context, stream jetstream.Stream, subject string, object jetstream.ObjectInfo) error {
	var count uint32
	var size uint64
	hash := sha256.New()
	for seq := uint64(1); ; {
		raw, err := stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return err
		}
		if raw.Subject != subject || raw.Sequence < seq || count >= object.Chunks {
			return errors.New("legacy orphan chunk census conflicts with metadata")
		}
		for header := range raw.Header {
			if strings.HasPrefix(strings.ToLower(header), "tether-xfer-") {
				return errors.New("v1 chunks require original owner retirement")
			}
		}
		count++
		size += uint64(len(raw.Data))
		_, _ = hash.Write(raw.Data)
		seq = raw.Sequence + 1
	}
	if size > object.Size || (!object.Deleted && (count != object.Chunks || size != object.Size ||
		object.Digest != "SHA-256="+base64.URLEncoding.EncodeToString(hash.Sum(nil)))) {
		return errors.New("legacy orphan body is incomplete or changed")
	}
	return nil
}

func (o *owner) legacyOrphanAbsent(candidate xfercontract.LegacyOrphanCandidate, deadline controlbudget.Deadline) error {
	if err := o.check(deadline); err != nil {
		return err
	}
	local, err := o.cfg.LegacyOrphans.Owners.Census(xfercontract.Origin{}, deadline)
	if err != nil {
		return err
	}
	if err = legacyCensusAbsent(local, candidate); err != nil {
		return err
	}
	seen := make(map[xfercontract.Origin]bool)
	for _, row := range local.Rows {
		if row.Origin == local.Origin || seen[row.Origin] {
			continue
		}
		remote, err := o.cfg.LegacyOrphans.Owners.Census(row.Origin, deadline)
		if err != nil {
			return err
		}
		if row.Origin == (xfercontract.Origin{}) || remote.Origin != row.Origin {
			return errors.New("legacy orphan original owner identity changed")
		}
		if err = legacyCensusAbsent(remote, candidate); err != nil {
			return err
		}
		seen[row.Origin] = true
	}
	ledger, err := o.cfg.Ledger.Scan(deadline)
	if err != nil {
		return err
	}
	return legacyCensusAbsent(ledger, candidate)
}

func legacyCensusAbsent(census xfercontract.LedgerCensus, candidate xfercontract.LegacyOrphanCandidate) error {
	if !census.Ready || !census.Complete || census.Code != "" {
		return errors.New("legacy orphan census is incomplete")
	}
	sid := strings.TrimPrefix(candidate.Bucket, "xfer-")
	matches := func(id xfercontract.Identity, version string) bool {
		return id.SID == sid && (id.TransferID == candidate.Name || id.ObjectVersion == candidate.NUID || version == candidate.NUID)
	}
	for _, entry := range census.Entries {
		if entry.Identity.SID == "" || entry.Identity.TransferID == "" ||
			(entry.ObjectVersion != "" && entry.Identity.ObjectVersion != "" && entry.ObjectVersion != entry.Identity.ObjectVersion) {
			return errors.New("legacy orphan protection identity is incomplete or conflicting")
		}
		if matches(entry.Identity, entry.ObjectVersion) {
			return errors.New("legacy orphan has a live owner")
		}
	}
	for _, row := range census.Rows {
		if row.Identity.SID == "" || row.Identity.TransferID == "" {
			return errors.New("legacy orphan ledger identity is incomplete")
		}
		if matches(row.Identity, row.Identity.ObjectVersion) {
			return errors.New("legacy orphan has retained ledger responsibility")
		}
	}
	return nil
}

// A retirement keeps the inspected stream incarnation and exact metadata through
// each stage; no later stage rediscovers a replacement object under the name.
// origin: hot-update S10 plan §0 / LINT-L2 maintidx
type legacyOrphanRetirement struct {
	candidate       xfercontract.LegacyOrphanCandidate
	stream          jetstream.Stream
	info            *jetstream.StreamInfo
	raw             *jetstream.RawStreamMsg
	object          jetstream.ObjectInfo
	subject, chunks string
	tombstone       []byte
}

func inspectLegacyOrphan(ctx context.Context, o *owner, candidate xfercontract.LegacyOrphanCandidate) (*legacyOrphanRetirement, error) {
	stream, err := o.cfg.LegacyOrphans.JS.Stream(ctx, candidate.Stream)
	if err != nil {
		return nil, err
	}
	info := stream.CachedInfo()
	if info.Created.UTC().Format(time.RFC3339Nano) != candidate.StreamCreated || info.State.Consumers != 0 {
		return nil, errors.New("legacy orphan stream incarnation or consumers changed")
	}
	subject := "$O." + candidate.Bucket + ".M." + base64.URLEncoding.EncodeToString([]byte(candidate.Name))
	raw, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw.Data)
	if raw.Subject != subject || raw.Sequence != candidate.MetadataSequence || hex.EncodeToString(sum[:]) != candidate.MetadataDigest {
		return nil, errors.New("legacy orphan metadata changed")
	}
	var object jetstream.ObjectInfo
	if json.Unmarshal(raw.Data, &object) != nil || object.Bucket != candidate.Bucket || object.Name != candidate.Name ||
		object.NUID != candidate.NUID || object.Chunks == 0 || object.Size > uint64(proto.XferMaxBytes) ||
		(object.Opts != nil && object.Opts.Link != nil) {
		return nil, errors.New("legacy orphan metadata does not identify a nonempty original object")
	}
	// Only server time proves age; client-controlled mtime cannot authorize
	// cleanup. A cold retry of a tombstone observes the same conservative age
	// floor again, since cap0 has no durable original-owner retirement proof.
	modified := raw.Time
	now := o.cfg.UTC()
	floor := 3 * proto.XferTimeoutTierBFloor
	floor += max(0, proto.XferBudget("b", int64(object.Size), proto.XferPushLegs)-proto.XferTimeoutTierBFloor)
	floor = max(floor, proto.XferTierBMaxBudget-now.Sub(o.born))
	if modified.IsZero() || now.Before(o.born) || now.Before(modified.Add(floor)) {
		return nil, errors.New("legacy orphan age or owner uptime is insufficient")
	}
	chunks := "$O." + candidate.Bucket + ".C." + candidate.NUID
	if err = validateLegacyChunks(ctx, stream, chunks, object); err != nil {
		return nil, err
	}
	return &legacyOrphanRetirement{candidate: candidate, stream: stream, info: info, raw: raw, object: object, subject: subject, chunks: chunks}, nil
}

func fenceLegacyOrphan(ctx context.Context, js jetstream.JetStream, r *legacyOrphanRetirement) error {
	object, raw, info := r.object, r.raw, r.info
	subject, candidate, stream := r.subject, r.candidate, r.stream
	// Retain the original geometry in this SDK-readable deleted record so an
	// uncertain purge is retryable after the conservative age floor. Ordinary SDK
	// and v1 tombstones have zero chunks and cannot enter this legacy path.
	if !object.Deleted {
		object.ModTime = raw.Time
	}
	object.Deleted = true
	tombstone, err := json.Marshal(object)
	if err != nil {
		return err
	}
	if !bytes.Equal(raw.Data, tombstone) {
		msg := nats.NewMsg(subject)
		msg.Data = tombstone
		msg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
		msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(raw.Sequence, 10))
		msg.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(info.State.LastSeq, 10))
		ack, err := js.PublishMsg(ctx, msg, jetstream.WithRetryAttempts(0))
		if err != nil {
			return err
		}
		if ack == nil || ack.Stream != candidate.Stream || ack.Sequence <= info.State.LastSeq {
			return errors.New("legacy orphan tombstone acknowledgement is invalid")
		}
		raw, err = stream.GetLastMsgForSubject(ctx, subject)
		if err != nil {
			return err
		}
		if raw.Sequence != ack.Sequence || !bytes.Equal(raw.Data, tombstone) {
			return errors.New("legacy orphan tombstone readback differs")
		}
	}
	r.raw, r.object, r.tombstone = raw, object, tombstone
	return nil
}

func completeLegacyOrphan(ctx context.Context, js jetstream.JetStream, r *legacyOrphanRetirement, current *jetstream.StreamInfo) error {
	object, raw := r.object, r.raw
	subject, candidate, stream := r.subject, r.candidate, r.stream
	// Persist completion only after exact raw absence. Retaining nonzero
	// geometry after that proof schedules the same NUID forever; a lost CAS or
	// ACK instead leaves a retryable tombstone and never invents completion.
	// origin: S7 internal review rv-gc F5
	object.Size, object.Chunks = 0, 0
	done, err := json.Marshal(object)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = done
	msg.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(raw.Sequence, 10))
	msg.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(current.State.LastSeq, 10))
	ack, err := js.PublishMsg(ctx, msg, jetstream.WithRetryAttempts(0))
	if err != nil {
		return err
	}
	if ack == nil || ack.Stream != candidate.Stream || ack.Sequence <= current.State.LastSeq {
		return errors.New("legacy orphan completion acknowledgement is invalid")
	}
	completed, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil || completed.Sequence != ack.Sequence || !bytes.Equal(completed.Data, done) {
		return errors.Join(err, errors.New("legacy orphan completion readback differs"))
	}
	return nil
}
