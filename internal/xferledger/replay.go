package xferledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// Exact terminal evidence outranks a compatible start-only row regardless of
// its directory. Neither directory wins a disagreement: all evidence survives.
// origin: hot-update S7 plan §5.1 / T22; xfer_inflight.go durable precedence table
func mergeRow(rows map[xfercontract.Key]RowV1, next RowV1) error {
	key := rowKey(next)
	old, found := rows[key]
	if !found {
		rows[key] = next
		return nil
	}
	if old.Revision == next.Revision {
		if !reflect.DeepEqual(old, next) {
			return fmt.Errorf("%w: same revision differs", ErrConflict)
		}
		return nil
	}
	low, high := old, next
	if low.Revision > high.Revision {
		low, high = high, low
	}
	if !compatibleAdvance(low, high) {
		return fmt.Errorf("%w: ledger copies disagree", ErrConflict)
	}
	rows[key] = high
	return nil
}

func (s *ledger) current(key xfercontract.Key, deadline controlbudget.Deadline) (*RowV1, error) {
	rows := make(map[xfercontract.Key]RowV1)
	if cached, ok := s.rows[key]; ok {
		rows[key] = cached
	}
	var failures []error
	// Outbox first is intentional. It may be the only durable terminal while
	// the primary still contains the START that preceded a failed write.
	for _, path := range []string{s.cfg.FallbackRoot, s.cfg.Root} {
		if _, err := s.remaining(deadline); err != nil {
			return nil, err
		}
		// A quarantined/legacy row cannot be excluded merely because its
		// current filename differs from the hash. It may hide this same key.
		// origin: hot-update S7 plan §5.1–§5.3 / T22
		if err := s.scanDirectory(path, deadline, rows); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return nil, fmt.Errorf("%w: incomplete ledger read: %w", ErrUnknown, errors.Join(failures...))
	}
	row, ok := rows[key]
	if !ok {
		return nil, nil
	}
	return &row, nil
}

// Update accepts a full proposed row; the expected revision is always the CAS
// authority. Callers may leave Row.Revision zero, retain the read revision, or
// provide the next revision. No other caller-supplied revision is accepted.
func (s *ledger) Update(key xfercontract.Key, expected uint64, mutation xfercontract.LedgerMutation, deadline controlbudget.Deadline) (RowV1, error) {
	if err := s.lock(deadline); err != nil {
		return RowV1{}, err
	}
	defer s.unlock()
	next, err := copyRow(mutation.Row)
	if err != nil {
		return RowV1{}, err
	}
	if !knownMutation(mutation.Kind) || rowKey(next) != key || mutation.EntryNonce == "" || mutation.EntryNonce != next.EntryNonce ||
		expected == ^uint64(0) || (next.Revision != 0 && next.Revision != expected && next.Revision != expected+1) {
		return RowV1{}, fmt.Errorf("%w: mutation identity or revision mismatch", ErrConflict)
	}
	next.Revision = expected + 1
	old, err := s.current(key, deadline)
	if err != nil {
		return RowV1{}, err
	}
	// The only retry against an older revision is the EXACT in-memory decision
	// whose write failed. It must still pass the same disk conflict census.
	// A sleeping imported owner also retries rows whose last ACK was lost.
	// Equality permits a fresh Sync of that revision, never an altered proposal;
	// this also lets clean siblings reconcile beside a different dirty row.
	// origin: S7 internal review rv-recovery F05
	retry := old != nil && reflect.DeepEqual(*old, next)
	if !retry {
		if (old == nil && expected != 0) || (old != nil && (old.Revision != expected || old.EntryNonce != mutation.EntryNonce)) {
			return RowV1{}, fmt.Errorf("%w: stale ledger revision or entry nonce", ErrConflict)
		}
		if err := validateMutation(old, next, mutation.Kind); err != nil {
			return RowV1{}, err
		}
	}
	// Retain before IO. Both directories can become unwritable after a valid
	// decision; planned export must carry it even though no durable ACK is sent.
	s.rows[key], s.dirty[key] = next, true
	primaryErr := s.writeAt(s.cfg.Root, next, deadline)
	if primaryErr != nil {
		outboxErr := s.writeAt(s.cfg.FallbackRoot, next, deadline)
		if outboxErr != nil {
			retained, _ := copyRow(next)
			return retained, fmt.Errorf("%w: neither ledger copy synced: %w", ErrDurability, errors.Join(primaryErr, outboxErr))
		}
	}
	delete(s.dirty, key)
	return copyRow(next)
}

func (s *ledger) scanDirectory(path string, deadline controlbudget.Deadline, rows map[xfercontract.Key]RowV1) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return err
	}
	var failures []error
	for _, entry := range entries {
		if _, err := s.remaining(deadline); err != nil {
			return err
		}
		name := entry.Name()
		// Abandoned atomic temps never granted authority. Corrupt/quarantined
		// rows, unexpected files and directories remain visible uncertainty.
		if strings.HasPrefix(name, ".xfer-") && strings.HasSuffix(name, ".tmp") {
			continue
		}
		row, found, err := readRow(root, name)
		if err == nil && !found {
			err = fmt.Errorf("%w: ledger row disappeared during scan", ErrUnknown)
		}
		if err == nil {
			err = mergeRow(rows, row)
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// Scan is read-only, including corrupt rows. Ready+Complete is granted only
// after BOTH directory enumerations and memory reconciliation have succeeded;
// incomplete scans still return every known protection and retained decision.
func (s *ledger) Scan(deadline controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	if err := s.lock(deadline); err != nil {
		return xfercontract.LedgerCensus{Code: xfercontract.CodeResultUnknown}, err
	}
	defer s.unlock()
	rows := make(map[xfercontract.Key]RowV1, len(s.rows))
	for key, row := range s.rows {
		rows[key] = row
	}
	var failures []error
	for _, path := range []string{s.cfg.FallbackRoot, s.cfg.Root} {
		if err := s.scanDirectory(path, deadline, rows); err != nil {
			failures = append(failures, err)
		}
	}
	if _, err := s.remaining(deadline); err != nil {
		failures = append(failures, err)
	}
	census := xfercontract.LedgerCensus{}
	keys := make([]xfercontract.Key, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].SID != keys[j].SID {
			return keys[i].SID < keys[j].SID
		}
		return keys[i].TransferID < keys[j].TransferID
	})
	for _, key := range keys {
		row, err := copyRow(rows[key])
		if err != nil {
			failures = append(failures, err)
			continue
		}
		census.Rows = append(census.Rows, row)
		census.Entries = append(census.Entries, xfercontract.Protection{Identity: row.Identity, Phase: row.Phase,
			ObjectVersion: row.Identity.ObjectVersion, CreationUntil: row.Permit.CreateUntil,
			WriterRetired: row.Cleanup.WriterFence.WriterRetired, OwnerGeneration: row.Origin.Generation})
	}
	// A complete conflict scan may still contain an unsynced proposal. Recovery
	// needs that distinction to retry the exact revision with admission closed;
	// the durability code identifies only that case. Both public readiness bits
	// remain false until Update has really synced one durable copy.
	// origin: S7 internal review rv-recovery F05
	if len(s.dirty) != 0 && len(failures) == 0 {
		census.Code = xfercontract.CodeDurabilityUnknown
		return census, fmt.Errorf("%w: retained proposals are not synced: %w", ErrUnknown, ErrDurability)
	}
	if len(failures) != 0 {
		census.Code = xfercontract.CodeResultUnknown
		if errors.Is(errors.Join(failures...), ErrConflict) {
			census.Code = xfercontract.CodeTerminalConflict
		}
		return census, fmt.Errorf("%w: ledger census is incomplete: %w", ErrUnknown, errors.Join(failures...))
	}
	census.Ready, census.Complete = true, true
	// Config has no Origin: an empty/mixed census cannot invent an origin.
	// The tracker binds it to its actual owner before serving remote status.
	if len(census.Rows) > 0 {
		census.Origin = census.Rows[0].Origin
		for _, row := range census.Rows {
			if row.Origin != census.Origin {
				census.Origin = xfercontract.Origin{}
				break
			}
		}
	}
	data, err := json.Marshal(census.Rows)
	if err != nil {
		return xfercontract.LedgerCensus{}, err
	}
	census.Digest = digest(data)
	return census, nil
}

func validateRetirement(row RowV1, nonce string, proof xfercontract.RetirementProof) error {
	start, hasStart := receiptFor(row, row.CanonicalStart)
	terminal, hasTerminal := receiptFor(row, row.CanonicalTerminal)
	cleanup := proof.Cleanup
	if nonce != row.EntryNonce || proof.EntryNonce != nonce || proof.Identity != row.Identity ||
		proof.Origin != row.Origin || proof.Revision != row.Revision || proof.OwnerGeneration == 0 ||
		proof.OwnerGeneration < row.Origin.Generation || cleanup.OwnerGeneration != proof.OwnerGeneration ||
		!proof.CreationUntil.Equal(row.Permit.CreateUntil) || !proof.WriterRetired || !proof.ReceiverRetired ||
		!proof.ConsumersRetired || !hasStart || !hasTerminal || proof.StartReceipt != start || proof.TerminalReceipt != terminal ||
		row.StartDelivery != xfercontract.DeliveryHistoryConfirmed || row.TerminalDelivery != xfercontract.DeliveryHistoryConfirmed ||
		cleanup.Identity != row.Identity || cleanup.Digest == "" || !cleanup.WriterRetired || !cleanup.ConsumersRetired ||
		!cleanup.TmpUnlinked || !cleanup.DirectorySynced ||
		(row.Identity.ObjectVersion != "" && (!cleanup.ObjectPurged || cleanup.FenceSequence == 0)) {
		return fmt.Errorf("%w: retirement lacks exact identity, history or cleanup proof", ErrConflict)
	}
	if row.Cleanup.TmpUnlinkPending || row.Cleanup.DirectorySyncPending || row.Cleanup.ObjectPurgePending {
		return fmt.Errorf("%w: cleanup is still pending", ErrConflict)
	}
	return nil
}

// Retire is one ordered operation, fenced by key+nonce+revision. It cannot
// acknowledge missing/hidden evidence, and never deletes the outbox first.
func (s *ledger) Retire(key xfercontract.Key, nonce string, proof xfercontract.RetirementProof, deadline controlbudget.Deadline) error {
	if err := s.lock(deadline); err != nil {
		return err
	}
	defer s.unlock()
	row, err := s.current(key, deadline)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("%w: retirement has no retained decision", ErrUnknown)
	}
	if s.dirty[key] {
		return fmt.Errorf("%w: decision is not durable", ErrDurability)
	}
	if err := validateRetirement(*row, nonce, proof); err != nil {
		return err
	}
	if _, err := s.remaining(deadline); err != nil {
		return err
	}
	// Admission itself can have fallen back, leaving an outbox START behind a
	// newer primary terminal. Install the exact current decision there before
	// removing primary; every intermediate crash then retains terminal evidence.
	// origin: hot-update S7 plan §5.1 / T22
	if err := s.writeAt(s.cfg.FallbackRoot, *row, deadline); err != nil {
		return fmt.Errorf("%w: retirement outbox barrier: %w", ErrDurability, err)
	}
	if err := s.removeAt(s.cfg.Root, rowName(key)); err != nil {
		return fmt.Errorf("%w: primary retirement: %w", ErrDurability, err)
	}
	if _, err := s.remaining(deadline); err != nil {
		return err
	}
	if err := s.removeAt(s.cfg.FallbackRoot, rowName(key)); err != nil {
		return fmt.Errorf("%w: outbox retirement: %w", ErrDurability, err)
	}
	delete(s.rows, key)
	delete(s.dirty, key)
	if s.cfg.Observe != nil {
		s.cfg.Observe(xfercontract.Observation{Kind: xfercontract.ObserveRetirement, TransferID: key.TransferID,
			EntryNonce: nonce, Revision: row.Revision, RetirementDigest: proof.Cleanup.Digest, DirectorySynced: true})
	}
	return nil
}

// replayStagedTerminal handles a ledger row that already carries the EXACT terminal, from either the
// primary in-flight directory or the fallback outbox. It reports whether the row was disposed of;
// false means RETAINED for a later pass.
// window: the real terminal may already be committed and the process died before the unlink, so
// emitting anything ELSE would produce a second, contradictory terminal that the content-addressed
// reqID cannot collapse. Replaying the identical record is idempotent by construction — and it is
// also the correct answer for the PRE-commit crash, where this row is simply the terminal that was
// decided but never forwarded. No timeout grace applies: the outcome is already decided, so there
// is nothing to wait for.
func (b *Legacy) ReplayTerminal(rec LegacyRecord) bool {
	// A canonical cap0 row carries its own exact START and binds its terminal to that admission
	// nonce. Its only retirement is exact history in BOTH modes: a dedup ledger or the presence of
	// some terminal cannot prove these bytes, and the CAS readback detects a contradicting row.
	// origin: hot-update S7 plan §5.1 / §5.2 / L12 (S7 integration oplegacy F2)
	if rec.Start != nil && rec.Terminal != nil {
		return b.Settle(rec)
	}
	// DETECT A PRIOR COMMIT (re-review F1). The staged row may already be committed — that is
	// precisely the post-commit crash window. Its reqID is content-addressed, so asking the
	// replicated dedup ledger is an exact answer, and its retention (~1M raft indices, months)
	// dwarfs the 2-minute JetStream Duplicates window a replay would otherwise rely on.
	// THE AUDIT INVARIANT, stated (re-review 疑惑 #1): EXACTLY ONE TRUE TERMINAL per transfer.
	// A contradictory pair is worse than a dangling start — a dangling start is detectable and
	// repairable, whereas two terminals claiming different outcomes corrupt the record itself and
	// no consumer can tell which is true. So a staged terminal is re-emitted ONLY when we can
	// positively establish it was NOT already committed. "Cannot verify" therefore means DROP,
	// not replay.
	//
	// The sentence that used to sit here — "in production the check is always available (the
	// ledger dir exists only in cluster mode, where b.cl is wired)" — was the load-bearing
	// justification for F-T1's bug, and it stopped being true when #57 gave single brokers a
	// ledger directory. Single mode is now handled explicitly above rather than falling into
	// the unknown branch.
	// ROUND-3 R3-F1: only a POSITIVE "already committed" answer may drop the outbox.
	//
	// The previous version folded a lookup ERROR into "seen" and deleted the ledger. That is the
	// worst of both worlds: a transient SQLite/disk/closed-DB error is not evidence of a commit,
	// so if the terminal had NOT committed we destroyed the only exact record of it and left a
	// permanent dangling start — reopening #57, the very defect this file exists to close. It also
	// contradicted this file's own comment, which correctly says an unknown state must degrade to
	// an exact replay.
	//
	// So the three states are now distinct:
	//   committed      -> drop the outbox (nothing more to write)
	//   NOT committed  -> replay the EXACT staged terminal (identical bytes ⇒ identical reqID)
	//   unknown        -> RETAIN and retry on the next pass; never emit, never delete
	// SINGLE MODE HAS NO DEDUP LEDGER TO ASK, and that is not the same as "unknown".
	//
	// origin: prerelease audit round 2, F-T1. The condition below used to be
	// `serr != nil || b.cl == nil`, and on a single broker b.cl is ALWAYS nil — so this
	// function could never do anything but retain. That was harmless while #57 was inert
	// on single brokers; the moment the ledger directory was enabled there (round 1's
	// #57 fix), every staged terminal became a row that is kept forever and a transfer
	// whose outcome is never emitted. Half a feature wired is worse than none: the ledger
	// now accumulates and nothing drains it.
	//
	// What single mode CAN do is exactly what commitSyntheticTransferTerminal does there:
	// publish the EXACT staged bytes. The terminal is content-deterministic, so a replay
	// of an already-published one collapses in JetStream's duplicate window, and the
	// residual — one repeated terminal — is strictly better than a transfer with none.
	if !b.Config.Cluster {
		// SINGLE MODE REPLAYS THE EXACT STAGED TERMINAL, deduped by content.
		//
		// origin: prerelease audit external review M-3. This branch used to RETAIN
		// unconditionally, and said so for a reason that was true when it was written:
		// replaying needed a duplicate-suppression key single mode did not have, because
		// JetStream dedups on Nats-Msg-Id and pubAuditTransfer set none. The consequence
		// was a permanent OPEN — a transfer that crashed between staging and publishing
		// got NO terminal at all, and its ledger row was kept forever.
		//
		// pubAuditTransfer now derives the SAME content id from the same record, so the
		// first publish and this replay are the same message to JetStream. That closes
		// both windows with one rule instead of trading them off:
		//
		//   crashed BEFORE publish -> this is the only emission, and it is the right one
		//   crashed AFTER publish  -> identical id, collapsed inside the duplicate window
		//
		// The "EXACTLY ONE TRUE TERMINAL" invariant that made the earlier attempt wrong is
		// therefore preserved by the id rather than by refusing to act. UNLINK ONLY AFTER
		// a successful publish: returning true on a failed publish is how a decided
		// outcome would be dropped, which is worse than replaying it again next pass.
		if rec.Terminal == nil {
			return false // not a staged-terminal row; the caller's other dispositions apply
		}
		payload, merr := json.Marshal(*rec.Terminal)
		if merr != nil {
			b.Config.Logger.Warn("broker: marshal staged transfer terminal for replay",
				"err", merr, "transfer_id", rec.TransferID)
			return false
		}
		msgID, ierr := xferaudit.TransferRecordReqID(*rec.Terminal)
		if ierr != nil {
			// No id means no way to prove a replay is not a duplicate. RETAIN — the old
			// behaviour, now confined to the case that actually justifies it.
			b.Config.Logger.Warn("broker: cannot derive a dedup id for a staged transfer terminal — "+
				"RETAINING rather than risking a second, contradictory terminal",
				"err", ierr, "transfer_id", rec.TransferID)
			return false
		}
		// ASK THE STREAM, not the clock — origin: prerelease audit external review R2-M2.
		// The content-derived Msg-Id makes a replay idempotent only inside JetStream's
		// duplicate window (two minutes here). A broker that crashes after the publish ack,
		// before the unlink, and stays down longer than that comes back to a window which has
		// forgotten the id — and writes a SECOND terminal. The audit stream still knows, and
		// unlike the window it does not expire, so it is the thing to ask.
		//
		// THREE STATES, same discipline as the cluster branch below:
		//   committed     -> drop the ledger row; the record is already correct
		//   NOT committed -> publish the exact staged bytes
		//   unknown       -> RETAIN and retry next pass; never emit, never delete
		if b.Config.StreamCommitted != nil {
			switch committed, cerr := b.Config.StreamCommitted(rec.Terminal.Session, rec.TransferID); {
			case cerr != nil:
				b.Config.Logger.Warn("broker: cannot verify whether a staged transfer terminal was "+
					"already committed — RETAINING (an unverifiable state is not evidence of absence)",
					"err", cerr, "transfer_id", rec.TransferID)
				return false
			case committed:
				b.Config.Logger.Info("broker: staged transfer terminal was already committed to the "+
					"audit stream; dropping the ledger row without re-publishing",
					"transfer_id", rec.TransferID)
				return b.RetireStaged(rec)
			}
		}
		if perr := b.Config.Publish(proto.SubjAuditTransfer(rec.Terminal.Session), payload, msgID); perr != nil {
			b.Config.Logger.Warn("broker: single-mode staged transfer terminal replay failed (retry next pass)",
				"err", perr, "transfer_id", rec.TransferID)
			return false
		}
		return b.RetireStaged(rec)
	}
	seen, serr := b.Config.Committed(*rec.Terminal)
	if serr != nil {
		b.Config.Logger.Warn("broker: cannot determine whether a staged transfer terminal was already committed — RETAINING the outbox for the next pass (deleting it could strand a transfer with no terminal at all)",
			"transfer_id", rec.TransferID, "kind", rec.Terminal.Kind, "err", serr)
		return false
	}
	if seen {
		_ = b.Config.Delete(rec.Session, rec.TransferID)
		if cErr := b.Consume(rec.TransferID); cErr != nil {
			b.Config.Logger.Warn("broker: a staged terminal was already committed but its ledger rows could not "+
				"be retired — RETAINING both so the start row can never be synthesized behind it",
				"err", cErr, "transfer_id", rec.TransferID)
			return false
		}
		b.Config.Logger.Info("broker: staged transfer terminal was already committed before the crash — dropping the ledger without re-emitting (#57)",
			"transfer_id", rec.TransferID, "kind", rec.Terminal.Kind)
		return true
	}
	if !b.Config.Commit(*rec.Terminal) {
		b.Config.Logger.Warn("broker: could not durably commit a STAGED transfer terminal (leaderless?) — leaving the ledger for the next pass (#57)",
			"transfer_id", rec.TransferID, "kind", rec.Terminal.Kind)
		return false
	}
	_ = b.Config.Delete(rec.Session, rec.TransferID)
	if cErr := b.Consume(rec.TransferID); cErr != nil {
		b.Config.Logger.Warn("broker: replayed a staged terminal but could not retire its ledger rows — RETAINING "+
			"both; the replay is idempotent (same bytes ⇒ same reqID), a stranded start row would not be",
			"err", cErr, "transfer_id", rec.TransferID)
		return false
	}
	return true
}

// ledgerSite says which of the two sibling directories a row was read from.
type LegacySite int

const (
	LegacySiteOutbox  LegacySite = iota // the fallback staging dir — scanned FIRST (pass 1)
	LegacySitePrimary                   // the in-flight dir (pass 2)
)

// ledgerDisposition is what a pass must do with ONE ledger row.
type LegacyDisposition int

const (
	LegacyLeave      LegacyDisposition = iota // retain the row untouched; a later pass re-decides
	LegacyReplay                              // replay the EXACT staged terminal, then consume both rows
	LegacySynthesize                          // no terminal exists anywhere — synthesize a deterministic one
)

// ledgerRowInputs is everything the precedence rule needs about ONE row. Assembling it is the
// caller's job (it owns the tracker, the clock and the outbox census); deciding is this file's.
type LegacyRowInputs struct {
	Site LegacySite
	Rec  LegacyRecord

	// Live: a tracker entry exists for this transfer in THIS incarnation.
	Live bool
	// OutboxHasExactTerminal: the outbox census found a readable row carrying an exact terminal for
	// this transfer. Only consulted for a primary start-only row.
	OutboxHasExactTerminal bool
	// OutboxCensusOK: the outbox scan completed. False ⇒ absence there is UNKNOWN, not empty (R6-F1).
	OutboxCensusOK bool
	// OutboxRowUnreadable: the outbox HOLDS a row for this transfer that this process could not turn
	// into a record (unparseable now, or quarantined as .corrupt earlier). The ROW-level twin of
	// OutboxCensusOK: the directory read fine, one file did not. Absence of a parsed terminal is then
	// not evidence of an absent terminal, and the outbox stages terminals ONLY — so whatever is in
	// there is a decided outcome that outranks a start row.
	OutboxRowUnreadable bool
	Now                 time.Time
}

// ledgerRowDisposition applies the durable-precedence rule documented at the top of this file to one
// row. It is PURE — no disk, no clock, no tracker — so every legal combination is reachable from a
// table-driven test instead of only from a crash sequence that has to be staged on disk.
//
// The returned reason is the rule that fired. Production puts it in the finalize log line, and the
// table-driven test asserts on it, so a branch that is reached for the wrong reason is visible rather
// than merely producing the right answer by luck.
func LegacyRowDisposition(in LegacyRowInputs) (LegacyDisposition, string) {
	// A live tracker entry outranks everything at BOTH sites: the transfer is genuinely in flight in
	// this incarnation and its own terminal path will write the terminal and retire the ledger.
	if in.Live {
		return LegacyLeave, "live tracker entry — the in-flight terminal path owns this transfer"
	}

	if in.Site == LegacySiteOutbox {
		// The outbox only ever stages DECIDED terminals. A row here without one is not a start row to
		// be finalized — it is a row this pass has nothing to say about.
		if in.Rec.Terminal == nil {
			return LegacyLeave, "outbox row carries no terminal — the outbox stages terminals only"
		}
		return LegacyReplay, "exact terminal staged in the outbox"
	}

	// Primary site. A terminal staged in place is replayed without consulting the outbox at all: an
	// exact terminal is an exact terminal wherever it lives, and re-emitting the identical record is
	// idempotent by construction (content-addressed reqID).
	if in.Rec.Terminal != nil {
		return LegacyReplay, "exact terminal staged in place"
	}
	// From here the row is start-only, i.e. the only path that can MANUFACTURE a terminal. Every gate
	// below is fail-closed by design.
	if !in.OutboxCensusOK {
		return LegacyLeave, "outbox census failed — a terminal there would outrank this start row, and " +
			"absence in an unreadable directory is UNKNOWN rather than empty (R6-F1)"
	}
	if in.OutboxHasExactTerminal {
		return LegacyLeave, "an exact terminal in the outbox outranks this start row"
	}
	if in.OutboxRowUnreadable {
		return LegacyLeave, "the outbox holds an UNREADABLE row for this transfer — the outbox stages " +
			"terminals only, so that row is a decided outcome this process cannot read, and synthesizing " +
			"beside it would commit a terminal that contradicts evidence still on disk"
	}
	if in.Now.Sub(in.Rec.StartedAt) < LegacyTimeout(in.Rec.Tier, in.Rec.Size)+LegacyStrandedSlack {
		return LegacyLeave, "younger than its tier timeout + slack — slow, not stranded"
	}
	return LegacySynthesize, "stranded past timeout + slack with no exact terminal at either site"
}

// ledgerProtectedObjects returns the object names the DURABLE ledger says are still inside their
// watchdog budget, keyed by "<bucket>/<object>". The object name IS the transfer id at every writer
// (the ctl's Put and the agent's Put both use it), so this is an exact identity match, not a heuristic.
//
// origin: batch-c EXTERNAL review B2. The orphan reaper decided "nobody owns this object" from the
// IN-MEMORY tracker alone — which a restart rebuilds EMPTY. With the tier-B budget now size-derived
// (up to 35m08s) and the home grace still 2 minutes, a broker restart during a large transfer meant
// the reaper deleted an object whose transfer was very much alive, roughly two minutes in. The
// internal review had recorded that as an accepted residual on the grounds that there was no durable
// evidence of ownership; that was simply wrong. This ledger has existed since #57 and already carries
// the transfer id, bucket, size and start time — everything the decision needs.
//
// Read errors yield an EMPTY set and are reported. The caller must treat "I could not read the
// ledger" as "do not delete anything on evidence I do not have", never as "nothing is protected".
// UNRESOLVED ROWS ARE PER-OBJECT EVIDENCE, NOT A CIRCUIT BREAKER.
//
// origin: prerelease audit broker-transfer/BT-F3. An unreadable row used to be
// folded into the returned error, and the caller skipped the WHOLE pass on any
// error. But an unreadable row is PERMANENT: forEachLedgerRecord renames it to
// <hash>.json.corrupt and nothing in the repo ever deletes those, so every later
// pass rediscovered it. One corrupt row — belonging to any session — stopped
// orphan reclamation for EVERY session forever, which is the "tier-B garbage is
// immortal, the small disk fills up" failure this sweep exists to prevent, caused
// by the guard rather than by the thing it guards against.
//
// So the second return is the set of ledger FILENAMES that could not be read. The
// caller protects exactly the objects those name and reaps everything else. err is
// now reserved for a directory-level read failure, which is transient and does
// self-heal.
func (b *Legacy) ProtectedObjects(now time.Time) (map[string]bool, map[string]bool, error) {
	protected := map[string]bool{}
	unresolvedNames := map[string]bool{}
	var firstErr error
	for _, dir := range []string{b.InflightDir(), b.OutboxDir()} {
		if dir == "" {
			continue
		}
		unresolved, err := b.EachRecord(dir, func(_ string, rec LegacyRecord) {
			// A row carrying a TERMINAL is a decided outcome — its object is disposable.
			if rec.Terminal != nil || rec.Bucket == "" || rec.TransferID == "" {
				return
			}
			if rec.StartedAt.IsZero() {
				// No start time to judge against. Protect it: a row we cannot age is exactly the kind of
				// evidence that must not be resolved in favour of deletion.
				protected[rec.Bucket+"/"+rec.TransferID] = true
				return
			}
			budget := LegacyTimeout(rec.Tier, rec.Size) + LegacyStrandedSlack
			if now.Sub(rec.StartedAt) < budget {
				protected[rec.Bucket+"/"+rec.TransferID] = true
			}
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
		for name := range unresolved {
			unresolvedNames[name] = true
		}
	}
	return protected, unresolvedNames, firstErr
}

func (b *Legacy) Finalize() (int, error) {

	dir := b.InflightDir()
	if dir == "" {
		return 0, nil
	}
	outboxDir := b.OutboxDir()
	finalized := 0

	// PASS 1 — THE OUTBOX, AND IT RUNS FIRST (round-5 R5-F2).
	//
	// These rows carry the EXACT terminal, so replaying them needs no guessing and no grace period. More
	// importantly, the outbox exists precisely BECAUSE the primary directory can be unusable — so making
	// its recovery depend on scanning the primary directory first would disable the fallback in the one
	// state it was built for. A primary scan error must never preempt this pass.
	outboxOwned := map[string]bool{}
	obUnreadable, obErr := b.EachRecord(outboxDir, func(_ string, rec LegacyRecord) {
		// The census is maintained regardless of what this row's disposition turns out to be — even a
		// row that is left alone here (a live transfer) still means an exact terminal EXISTS at this
		// site, and pass 2 must not synthesize one from the start row it will find.
		if rec.Terminal != nil {
			outboxOwned[rec.TransferID] = true
		}
		d, _ := LegacyRowDisposition(LegacyRowInputs{
			Site: LegacySiteOutbox,
			Rec:  rec,
			Live: b.Config.Live(rec.TransferID),
			Now:  b.Config.Now(),
		})
		if d == LegacyReplay && b.ReplayTerminal(rec) {
			finalized++
		}
	})

	// blockedByUnreadable collects transfers that can never be finalized while an unreadable outbox row
	// outranks their start row. Reported once per pass below — see the ledgerLeave branch.
	var blockedByUnreadable []string

	// PASS 2 — the primary directory: staged terminals, then start-only rows old enough to synthesize.
	_, prErr := b.EachRecord(dir, func(_ string, rec LegacyRecord) {
		in := LegacyRowInputs{
			Site:                   LegacySitePrimary,
			Rec:                    rec,
			Live:                   b.Config.Live(rec.TransferID),
			OutboxHasExactTerminal: outboxOwned[rec.TransferID],
			OutboxCensusOK:         obErr == nil,
			// The hash is one-way, so the census cannot name the transfers it failed to read — but this
			// side HAS the transfer_id and computes the same filename forward.
			OutboxRowUnreadable: obUnreadable[LegacyFilename(rec.TransferID)],
			Now:                 b.Config.Now(),
		}
		d, why := LegacyRowDisposition(in)
		switch d {
		case LegacyLeave:
			// A row left alone because an UNREADABLE outbox row outranks it is a PERMANENT state, and it
			// must not be a silent one (internal review B8-1).
			//
			// Every other ledgerLeave reason resolves itself: a live transfer finishes, a fresh one ages
			// out, a failed census succeeds on the next pass. This one does not. The unreadable row was
			// quarantined to <name>.corrupt on the pass that found it, the census folds that name back in
			// forever, and nothing removes either file — so the transfer has ZERO terminals in the audit
			// (the true one is in a file no code path will read again, the synthetic one is blocked) and
			// the ledger row is immortal.
			//
			// That is still the right disposition: this file's own argument at replayStagedTerminal is
			// that a dangling start is "detectable and repairable" while a contradictory pair corrupts the
			// record. But "detectable" is a property something has to provide. One Warn on the pass that
			// quarantined the row, possibly weeks ago and possibly rotated away, does not.
			//
			// So: count it, and report it once per pass at Warn with the operator's next step. It is
			// bounded — one line per pass, not per row — and it names the directory to look in.
			if in.OutboxRowUnreadable {
				blockedByUnreadable = append(blockedByUnreadable, rec.TransferID)
			}
			return
		case LegacyReplay:
			if b.ReplayTerminal(rec) {
				finalized++
			}
			return
		case LegacySynthesize:
			// Named rather than left to the fallthrough: the synthesis path is everything below this
			// switch, and a new disposition joining the enum must not inherit it by default. Emitting a
			// synthetic `failed` terminal for a transfer that should have been left alone would put a
			// permanent lie in the audit log.
		}
		// DELIBERATELY the tier floor, not the size-derived budget — see transferTierFloorFor.
		timeout := LegacyTierFloor(rec.Tier)
		synthetic := schema.AuditTransfer{
			V: schema.AuditSchemaVersion, Kind: "failed", Verb: rec.Verb,
			Ts: rec.StartedAt.Add(timeout), Session: rec.Session, Node: rec.Node,
			ActorNkey: rec.ActorNkey, ActorFp: rec.ActorFp,
			TransferID: rec.TransferID, Path: rec.Path, Tier: rec.Tier, Bucket: rec.Bucket,
			DurationMs: timeout.Milliseconds(),
			Code:       "home_broker_restart",
			Error:      "broker home restarted while the transfer was in flight; the crashed incarnation wrote no terminal audit (#57 finalize-on-recovery)",
		}
		// A canonical start-only row binds its deterministic synthetic terminal to the retained
		// admission nonce and settles through exact history; it never takes the Raft/MsgID path.
		// origin: hot-update S7 plan §5.1 / §5.3 / L12 (S7 integration oplegacy F2)
		if rec.Start != nil {
			synthetic = schema.BindLegacyTerminal(synthetic, rec.Start.EntryNonce)
			if rec.Terminal = &synthetic; b.Settle(rec) {
				finalized++
			}
			return
		}
		// M1: the ledger may be deleted ONLY after the synthetic terminal is durably COMMITTED. A
		// fire-and-forget emit would destroy the durable evidence in the leaderless post-crash window
		// (exactly #57's flagship case) — so on a not-yet-committable emit we LEAVE the file and the next
		// pass retries (the deterministic Ts makes the content reqID dedup the re-emit for free).
		if !b.Config.Commit(synthetic) {
			b.Config.Logger.Warn("broker: could not durably commit a stranded-transfer terminal (leaderless?) — leaving the ledger for the next pass (#57)",
				"transfer_id", rec.TransferID, "verb", rec.Verb, "tier", rec.Tier)
			return
		}
		_ = b.Config.Delete(rec.Session, rec.TransferID)
		if cErr := b.Consume(rec.TransferID); cErr != nil {
			// The synthetic is deterministic (Ts = startedAt+timeout), so a re-emit next pass dedups on
			// content. Leaving the row is therefore safe; losing it would not be.
			b.Config.Logger.Warn("broker: committed a stranded-transfer terminal but could not retire its ledger row",
				"err", cErr, "transfer_id", rec.TransferID)
		}
		finalized++
		b.Config.Logger.Info("broker: finalized a stranded in-flight transfer after a home restart (#57)",
			"transfer_id", rec.TransferID, "verb", rec.Verb, "tier", rec.Tier, "session", rec.Session,
			"reason", why)
	})
	// A scan failure of ONE source is a logged, retried condition — not a pass failure. Returning it would
	// hand the caller (the reconcile pass) an error for a run in which the other source made real progress,
	// and would keep doing so for as long as the broken directory stays broken. Only a run in which NO
	// source could be read at all is a genuine failure worth propagating.
	// B8-1: the one ledgerLeave that never resolves itself, surfaced once per pass rather than once ever.
	// Bounded to one line and a capped sample so a hundred stuck rows do not become a hundred log lines.
	if len(blockedByUnreadable) > 0 {
		sample := blockedByUnreadable
		if len(sample) > 5 {
			sample = sample[:5]
		}
		// EXTERNAL REVIEW B2-3: both halves of the previous version of this text were FALSE, and the second
		// half was destructive.
		//
		//   (a) "repair it in place (the next pass replays it)" — forEachLedgerRecord only ever calls fn for
		//       entries ending in ".json" (see the suffix check at the top of its loop). A repaired
		//       *.json.corrupt keeps a name the replay walk skips, so editing the bytes alone changes
		//       nothing, forever. The file has to be RENAMED back to its canonical <hash>.json.
		//
		//   (b) "remove BOTH the .corrupt file and the matching in-flight row" — the in-flight start row is
		//       the ONLY record synthesis can derive a deterministic terminal from (transfer id, verb, tier,
		//       bucket, path, startedAt). Deleting it alongside the corrupt outbox row leaves neither a real
		//       terminal nor anything to synthesize one from: the transfer becomes permanently unauditable,
		//       which is worse than the state this warning is reporting. Removing ONLY the corrupt outbox row
		//       is what unblocks synthesis — the census stops marking the name unresolved, the start row is
		//       then a plain stale start row, and the next pass writes failed/home_broker_restart for it.
		//
		// Stated as two numbered operator actions rather than prose, because the difference between them is
		// one filename and the cost of getting it wrong is an audit record that can never be reconstructed.
		b.Config.Logger.Warn("broker: transfer(s) can never be finalized — an UNREADABLE terminal is staged in "+
			"the outbox and outranks their in-flight row, so no terminal audit will ever be written for them. "+
			"Inspect the quarantined *.json.corrupt files in outbox_dir. (1) RECOVERABLE: repair the JSON and "+
			"RENAME it back to its canonical <hash>.json name — a repaired file left under the .corrupt name is "+
			"never replayed, because the ledger walk only reads *.json. (2) UNRECOVERABLE: delete ONLY the "+
			".corrupt file in outbox_dir and KEEP the matching row in inflight_dir — that row is the only input "+
			"a synthesized terminal can be derived from, so deleting it makes the transfer permanently "+
			"unauditable instead of repairing it",
			"count", len(blockedByUnreadable), "sample_transfer_ids", sample,
			"outbox_dir", outboxDir, "inflight_dir", dir)
	}
	if obErr != nil {
		b.Config.Logger.Warn("broker: could not scan the transfer terminal outbox — retrying on the next pass",
			"err", obErr, "dir", outboxDir)
	}
	if prErr != nil {
		b.Config.Logger.Warn("broker: could not scan the xfer-inflight ledger — retrying on the next pass; any "+
			"exact terminals staged in the fallback outbox were still replayed",
			"err", prErr, "dir", dir)
	}
	if obErr != nil && prErr != nil {
		return finalized, errors.Join(obErr, prErr)
	}
	return finalized, nil
}

func ReadLegacyRecord(path string) (LegacyRecord, error) {
	var rec LegacyRecord
	data, err := os.ReadFile(path)
	if err != nil {
		return rec, err
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return rec, err
	}
	// EXTERNAL REVIEW F6: a decode success is not a valid record. `{}` decodes fine and would drive the
	// finalizer with an empty transfer id / zero StartedAt, which then looks infinitely "stranded". Treat
	// a structurally invalid record as unreadable so it takes the .corrupt path instead.
	if rec.TransferID == "" || rec.Session == "" || rec.Verb == "" || rec.Tier == "" || rec.StartedAt.IsZero() {
		return LegacyRecord{}, fmt.Errorf("xfer-inflight: incomplete ledger record (transfer_id=%q session=%q verb=%q tier=%q started_at=%v)",
			rec.TransferID, rec.Session, rec.Verb, rec.Tier, rec.StartedAt)
	}
	return rec, nil
}

// RetireStaged keeps confirmed history distinct from object/ledger cleanup.
// origin: hot-update S7 plan §5.2 / T21
func (b *Legacy) RetireStaged(rec LegacyRecord) bool {
	if err := b.Config.Delete(rec.Session, rec.TransferID); err != nil {
		return false
	}
	if err := b.Consume(rec.TransferID); err != nil {
		b.Config.Logger.Warn("broker: confirmed transfer cleanup remains pending", "err", err, "transfer_id", rec.TransferID)
		return false
	}
	return true
}
