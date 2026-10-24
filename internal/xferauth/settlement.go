package xferauth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// SessionBarrier runs after the SQL tombstone and before history/OBJ/SQL
// deletion. Implementations must use the same absolute deadline at every hop.
// origin: hot-update S7 plan §6.4 / T31
type SessionBarrier interface {
	Settle(string, bool, controlbudget.Deadline) error
}

// SessionOwners enumerates every original owner, including durable origins
// absent from current membership. A missing owner is an error, never an empty
// list. P23 supplies this routing adapter; OwnerResolver alone cannot enumerate
// the authority set for a session that has moved between brokers.
type SessionOwners interface {
	SessionCensus(string, controlbudget.Deadline) ([]xfercontract.LedgerCensus, error)
}

// OriginalSettlementOwners binds mutation ports to the authority identified by
// the census. A remote census alone does not make the local tracker that owner.
// origin: S7 internal review rv-terminal F4 / rv-recovery F07
type OriginalSettlementOwners interface {
	SettlementOwner(xfercontract.Origin, controlbudget.Deadline) (xfercontract.TrackerPort, xfercontract.FinalizerPort, error)
}

type SettlementConfig struct {
	Owners    SessionOwners
	Tracker   xfercontract.TrackerPort
	Ledger    xfercontract.LedgerPort
	Finalizer xfercontract.FinalizerPort
	UTC       func() time.Time
}

type settlement struct{ cfg SettlementConfig }

// PendingError is intentionally safe for wire/errors: only transfer IDs and
// fixed reasons are retained, never underlying storage errors or capsules.
type PendingError struct{ Transfers []PendingTransfer }
type PendingTransfer struct{ TransferID, Reason string }

func (e *PendingError) Error() string {
	parts := make([]string, 0, len(e.Transfers))
	for _, p := range e.Transfers {
		parts = append(parts, p.TransferID+": "+p.Reason)
	}
	return "transfer settlement pending: " + strings.Join(parts, "; ")
}

func (*PendingError) ExitCode() int { return 75 }

func NewSettlement(cfg SettlementConfig) (SessionBarrier, error) {
	if cfg.Owners == nil || cfg.Tracker == nil || cfg.Ledger == nil || cfg.Finalizer == nil {
		return nil, errors.New("transfer settlement dependencies unavailable")
	}
	if cfg.UTC == nil {
		cfg.UTC = time.Now
	}
	return &settlement{cfg: cfg}, nil
}

func (s *settlement) Settle(sid string, abandon bool, d controlbudget.Deadline) error {
	if !validToken(sid) {
		return errors.New("invalid settlement session")
	}
	rows, pending := s.census(sid, d)
	// A partial census can reveal useful pending IDs, but cannot authorize even
	// an operator abandonment: another origin might already own a terminal.
	if len(pending) != 0 {
		return pendingError(pending)
	}
	for _, row := range rows {
		if pendingReason(row) == "" {
			continue
		}
		if _, err := controlbudget.Remaining(d); err != nil {
			pending = append(pending, PendingTransfer{row.Identity.TransferID, "settlement_deadline"})
			continue
		}
		tracker, finalizer := s.cfg.Tracker, s.cfg.Finalizer
		if owners, ok := s.cfg.Owners.(OriginalSettlementOwners); ok {
			var err error
			tracker, finalizer, err = owners.SettlementOwner(row.Origin, d)
			if err != nil || tracker == nil || finalizer == nil {
				pending = append(pending, PendingTransfer{row.Identity.TransferID, "original_owner_unavailable"})
				continue
			}
		}
		// Cancel closes new byte/commit admission but cannot revoke an already
		// issued Grant or invent a receiver outcome. The finalizer owns progress.
		// The tracker binds a cancel to the entry's original origin; an empty
		// origin was refused as an intent conflict and never reached it.
		// origin: hot-update S7 plan §6.4 / T31 / INTEG-D session abandonment
		_, cancelErr := tracker.Cancel(row.Identity, xfercontract.CancelRequest{Origin: row.Origin, Reason: xfercontract.CancelRevoked}, d)
		if abandon {
			if err := abandonDecision(tracker, row, d); err != nil {
				pending = append(pending, PendingTransfer{row.Identity.TransferID, "abandonment_unconfirmed"})
				continue
			}
			// Publication of the exact decision is judged by the re-census
			// below; retained cleanup is what the operator abandoned.
			_, _ = finalizer.Advance(row.Identity, d)
			continue
		}
		if cancelErr != nil {
			pending = append(pending, PendingTransfer{row.Identity.TransferID, "receiver_unavailable"})
		}
		if _, err := finalizer.Advance(row.Identity, d); err != nil {
			pending = append(pending, PendingTransfer{row.Identity.TransferID, "settlement_unconfirmed"})
		}
	}
	// Re-read the original authority set. A successful callback is not proof
	// that START, TERMINAL, and cleanup are durable at the current revision.
	current, censusPending := s.census(sid, d)
	pending = append(pending, censusPending...)
	if len(censusPending) != 0 {
		for _, row := range rows {
			pending = append(pending, PendingTransfer{row.Identity.TransferID, "origin_census_incomplete"})
		}
	}
	for _, row := range current {
		if reason := pendingReason(row); reason != "" {
			pending = append(pending, PendingTransfer{row.Identity.TransferID, reason})
		}
	}
	if len(pending) != 0 {
		return pendingError(pending)
	}
	return nil
}

// A complete session census is required even when every known local row has
// settled: missing origins cannot be reconstructed from an empty local cache.
// origin: hot-update S7 plan §6.4 / T31
func (s *settlement) census(sid string, d controlbudget.Deadline) ([]xfercontract.LedgerRow, []PendingTransfer) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return nil, []PendingTransfer{{"*", "settlement_deadline"}}
	}
	censuses, err := s.cfg.Owners.SessionCensus(sid, d)
	if _, deadlineErr := controlbudget.Remaining(d); deadlineErr != nil {
		return nil, []PendingTransfer{{"*", "settlement_deadline"}}
	}
	if err != nil || len(censuses) == 0 {
		return nil, []PendingTransfer{{"*", "origin_census_unavailable"}}
	}
	var rows []xfercontract.LedgerRow
	var pending []PendingTransfer
	seen := make(map[string]bool)
	for _, c := range censuses {
		if !c.Ready || !c.Complete || c.Code != "" || !validOrigin(c.Origin) {
			pending = append(pending, PendingTransfer{"*", "origin_census_incomplete"})
		}
		rowIDs := make(map[xfercontract.Identity]bool)
		for _, row := range c.Rows {
			id := row.Identity
			if id.SID != sid {
				continue
			}
			if !validToken(id.TransferID) || row.Origin != c.Origin || row.Revision == 0 || row.EntryNonce != id.EntryNonce || seen[id.TransferID] {
				pending = append(pending, PendingTransfer{"*", "origin_census_conflict"})
				continue
			}
			seen[id.TransferID] = true
			rowIDs[id] = true
			rows = append(rows, row)
		}
		for _, entry := range c.Entries {
			if entry.Identity.SID == sid && !rowIDs[entry.Identity] {
				id := entry.Identity.TransferID
				if !validToken(id) {
					id = "*"
				}
				pending = append(pending, PendingTransfer{id, "durable_entry_unavailable"})
			}
		}
	}
	if len(pending) != 0 {
		for _, row := range rows {
			pending = append(pending, PendingTransfer{row.Identity.TransferID, "origin_census_incomplete"})
		}
	}
	return rows, pending
}

func pendingError(pending []PendingTransfer) *PendingError {
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].TransferID == pending[j].TransferID {
			return pending[i].Reason < pending[j].Reason
		}
		return pending[i].TransferID < pending[j].TransferID
	})
	out := pending[:0]
	for _, p := range pending {
		if len(out) == 0 || out[len(out)-1] != p {
			out = append(out, p)
		}
	}
	return &PendingError{Transfers: out}
}

// Confirmed publication and retired byte/consumer work are independent facts.
// A terminal phase alone must never permit history or object destruction.
// origin: hot-update S7 plan §6.4 / T31
//
// With the operator's explicit abandonment, the exact, history-confirmed
// START and TERMINAL are the whole barrier (lead LQ1: write the terminal,
// then delete in the normal order). Receiver retirement cannot be proved for
// a receiver that is gone for good, and the session deletion itself removes
// the history and the whole OBJ bucket — a superset of the per-object purge,
// which also leaves a late writer attempt nothing to land in. History is never
// waived: an unconfirmed or contradictory record still blocks deletion.
// The historical history-only rationale above cannot prove that a ctl joined.
// The barrier below now requires positive producer retirement even with the flag.
// origin: S7 internal review rv-wiring F5 / s7-p9m remaining request 3
// origin: hot-update S7 plan lead LQ1 / §6.4 / INTEG-D session abandonment
func pendingReason(r xfercontract.LedgerRow) string {
	if r.CanonicalStart != nil && r.CanonicalStart.Kind != "start" {
		return "start_history_conflict"
	}
	if r.CanonicalTerminal != nil && r.CanonicalTerminal.Kind != "complete" && r.CanonicalTerminal.Kind != "failed" {
		return "terminal_history_conflict"
	}
	if !hasHistory(r, r.CanonicalStart, r.StartDelivery) {
		return "start_history_unconfirmed"
	}
	if !hasHistory(r, r.CanonicalTerminal, r.TerminalDelivery) {
		if r.Grant != nil && r.CanonicalTerminal == nil {
			return "receiver_commit_unsettled"
		}
		return "terminal_history_unconfirmed"
	}
	if r.CanonicalStart.Digest == r.CanonicalTerminal.Digest || r.CanonicalStart.ReqID == r.CanonicalTerminal.ReqID {
		return "terminal_history_conflict"
	}
	// The unknown bit only releases receiver uncertainty for the operator's
	// exact failed decision. It cannot make a contradictory complete durable.
	// origin: hot-update S7 plan lead LQ1 / T31
	if r.OutcomeUnknown && !abandonedRecord(r.CanonicalTerminal) {
		return "terminal_history_conflict"
	}
	// Session deletion cannot stand in for a ctl writer's future-gate-close
	// and join witness. Abandonment releases only receiver uncertainty; the
	// reachable producer and consumer retirement still precedes destruction.
	// origin: S7 internal review rv-wiring F5 / s7-p9m remaining request 3
	if !r.OutcomeUnknown {
		outcome := r.ReceiverOutcome
		if outcome == nil || (outcome.Outcome != xfercontract.OutcomeNotEntered && outcome.Outcome != xfercontract.OutcomeLanded) {
			return "receiver_commit_unsettled"
		}
		if outcome.Outcome == xfercontract.OutcomeLanded && !outcome.ParentSynced {
			return "receiver_durability_unconfirmed"
		}
	}
	c := r.Cleanup
	if r.Phase != xfercontract.PhaseSettled || !c.WriterFence.WriterRetired || !c.WriterFence.ConsumersRetired ||
		c.TmpUnlinkPending || c.DirectorySyncPending || c.ObjectPurgePending || c.PrimaryRetirePending || c.OutboxRetirePending {
		return "cleanup_unconfirmed"
	}
	for _, reader := range c.Consumers {
		if reader.Phase != xfercontract.ReaderStopped || reader.DirtyBarrier || reader.DeletePending {
			return "consumer_unretired"
		}
	}
	return ""
}

// The abandoned slot is the shared audit schema's failed record with code
// settlement_abandoned and outcome_unknown=true; the schema has no separate
// reason field, so the tracker's exact bytes carry the code alone.
// origin: hot-update S7 plan lead LQ1 / §5.2 / INTEG-D session abandonment
func abandonedRecord(record *xfercontract.CanonicalRecord) bool {
	var body struct {
		Kind, Code     string
		OutcomeUnknown bool `json:"outcome_unknown"`
	}
	return record.Kind == "failed" && json.Unmarshal(record.Bytes, &body) == nil && body.Kind == "failed" &&
		body.Code == xfercontract.ReasonSettlementAbandoned && body.OutcomeUnknown
}

// Match the exact canonical digest and physical receipt, not just any record
// for the transfer. START publication is still owed after TERMINAL is decided.
// origin: hot-update S7 plan §5.2 / §6.4 / T31
func hasHistory(r xfercontract.LedgerRow, record *xfercontract.CanonicalRecord, delivery xfercontract.AuditDelivery) bool {
	if record == nil || record.Identity != r.Identity || record.Digest == "" || record.ReqID == "" || len(record.Bytes) == 0 || delivery != xfercontract.DeliveryHistoryConfirmed {
		return false
	}
	sum := sha256.Sum256(record.Bytes)
	if hex.EncodeToString(sum[:]) != record.Digest {
		return false
	}
	for _, receipt := range r.HistoryReceipts {
		if receipt.Stream == "history-"+r.Identity.SID && receipt.Sequence != 0 && receipt.RecordDigest == record.Digest {
			return true
		}
	}
	return false
}

// TrackerAbandoner is the tracker's explicit operator decision (lead LQ1).
// The tracker owns its entries' revision serializer: a decision written to
// the ledger behind it forks the revision, and the ledger also refuses a
// terminal without the receiver outcome only the tracker can label.
// origin: hot-update S7 plan lead LQ1 / §6.4 / INTEG-D session abandonment
type TrackerAbandoner interface {
	Abandon(xfercontract.Identity, controlbudget.Deadline) (xfercontract.Status, error)
}

// abandon asks the original tracker for one immutable canonical decision. It
// never fabricates a not-entered receiver result. Replays leave an existing
// terminal untouched, including a landed outcome whose history ACK was lost.
// origin: hot-update S7 plan lead LQ1 / T31
func (s *settlement) abandon(row xfercontract.LedgerRow, d controlbudget.Deadline) error {
	return abandonDecision(s.cfg.Tracker, row, d)
}

func abandonDecision(port xfercontract.TrackerPort, row xfercontract.LedgerRow, d controlbudget.Deadline) error {
	if row.CanonicalTerminal != nil {
		return nil
	}
	// An explicit escape hatch cannot turn a known result into a conflicting
	// failed record. Finish its original decision through the normal
	// finalizer; only an unresolved (or landed-but-unsynced) outcome can be
	// abandoned, and the tracker keeps that evidence beside the unknown bit.
	if out := row.ReceiverOutcome; out != nil && (out.Outcome == xfercontract.OutcomeLanded && out.ParentSynced || out.Outcome == xfercontract.OutcomeNotEntered) {
		return nil
	}
	tracker, ok := port.(TrackerAbandoner)
	if !ok {
		return errors.New("tracker cannot record an abandonment decision")
	}
	if _, err := tracker.Abandon(row.Identity, d); err != nil {
		return errors.New("settlement decision not persisted")
	}
	return nil
}

// origin: hot-update S7 plan §6.4 / T31; moved with the existing SQL invariants.
// DropSessionRows is the H.3 phase ③ SQL atom. ON DELETE CASCADE on
// the FK chain (sessions → members/nodes/agent_provisioning;
// nodes → processes/port_allocations) means a single DELETE FROM
// sessions covers everything; we keep the explicit per-table
// DELETEs anyway so a future schema change that drops a cascade FK
// doesn't silently leak rows.
func DropSessionRows(db *sql.DB, sid string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var state string
	switch err := tx.QueryRow(`SELECT state FROM sessions WHERE sid = ?`, sid).Scan(&state); {
	case err == nil:
		if state != string(session.StateDeleting) {
			return session.ErrDeleting
		}
	case errors.Is(err, sql.ErrNoRows):
		return nil
	default:
		return fmt.Errorf("dropSessionRows lookup: %w", err)
	}

	for _, q := range []string{
		`DELETE FROM port_allocations  WHERE sid = ?`,
		`DELETE FROM processes         WHERE sid = ?`,
		`DELETE FROM proxy_subscribers WHERE sid = ?`,
		`DELETE FROM agent_provisioning WHERE sid = ?`,
		`DELETE FROM nodes             WHERE sid = ?`,
		`DELETE FROM members           WHERE sid = ?`,
		`DELETE FROM sessions          WHERE sid = ?`,
	} {
		if _, err := tx.Exec(q, sid); err != nil {
			return fmt.Errorf("dropSessionRows %q: %w", q, err)
		}
	}
	return tx.Commit()
}

// ListSessionSIDs enumerates the tombstone/active sets without creating streams.
// origin: hot-update S7 plan §6.4 / T31
func ListSessionSIDs(db *sql.DB, state string) ([]string, error) {
	rows, err := db.Query(`SELECT sid FROM sessions WHERE state = ? ORDER BY sid`, state)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			return nil, err
		}
		out = append(out, sid)
	}
	return out, rows.Err()
}

// RemoveSession owns the broker's reply semantics after owner authentication.
// Tombstone's result is authoritative even on a follower whose local SQL may
// lag. Settlement failures retain evidence and return retryable rc75; cascade
// failures after settlement belong to the boot resumer and do not undo success.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31 / P9N decisions 1–3
func RemoveSession(data []byte, tombstone func() error, governed bool, settle func(bool) error, cascade func()) proto.SessionRmResp {
	var req proto.SessionRmReq
	if len(data) != 0 {
		if err := json.Unmarshal(data, &req); err != nil {
			return proto.SessionRmResp{Code: "json_parse", Error: "invalid session removal request"}
		}
	}
	err := tombstone()
	retry := errors.Is(err, session.ErrDeleting)
	if err != nil && !retry {
		if errors.Is(err, session.ErrNotFound) {
			return proto.SessionRmResp{Code: "not_found", Error: err.Error()}
		}
		return proto.SessionRmResp{Code: "store_error", Error: err.Error()}
	}
	return RemoveTombstonedSession(retry, governed, req.AbandonUnsettledTransfers, settle, cascade)
}

// RemoveTombstonedSession consumes the already-authoritative tombstone result.
// The handler cannot re-read follower SQL to contradict a committed tombstone.
// origin: hot-update S7 plan §6.4 / lead integration decisions 1–3
func RemoveTombstonedSession(retry, governed, abandon bool, settle func(bool) error, cascade func()) proto.SessionRmResp {
	// Preserve the legacy already-DELETING contract without invoking a cascade
	// or a nonexistent continuation barrier. Governed retries must settle first.
	if retry && !governed {
		return proto.SessionRmResp{Code: "already_deleting"}
	}
	if err := settle(abandon); err != nil {
		var pending *PendingError
		if errors.As(err, &pending) {
			return proto.SessionRmResp{Code: xfercontract.CodeOwnerRecovering, Error: pending.Error()}
		}
		return proto.SessionRmResp{Code: xfercontract.CodeOwnerRecovering, Error: "session removal pending; retry after settlement"}
	}
	cascade()
	// A governed retry completed settlement and must not become CLI exit 64.
	// origin: hot-update S7 plan lead LQ1 / S7 RM-RETRY lead decision
	return proto.SessionRmResp{OK: true}
}

// SessionHistoryConfig supplies current control-plane backends. The authority
// callback prevents a stale follower from classifying shared streams as orphaned.
// origin: hot-update S7 plan §6.4 / T31 / INTEG-G responsibility move
type SessionHistoryConfig struct {
	DB        *sql.DB
	ReadDB    func() *sql.DB
	JS        jetstream.JetStream
	Logger    *slog.Logger
	Finalize  func(controlbudget.Deadline, <-chan struct{}, string) error
	MayDelete func() bool
}

// ReconcileSessionHistory runs every architecture H.3 startup
// rule against the current SQLite ↔ JetStream state:
//
//   - For every state=DELETING session: re-run finalizeSessionRm.
//   - For every state=ACTIVE session WITHOUT a history-<sid> stream:
//     EnsureHistoryStream (operator may have manually deleted it,
//     or the broker crashed mid-create).
//   - For every history-<sid> stream WITHOUT a SQLite session:
//     DeleteHistoryStream (orphan).
//
// Called once from Run after JS is confirmed available. cfg.JS MUST
// be non-nil at call time. Errors are logged but not fatal — broker
// startup succeeds even if reconcile partially fails; subsequent
// bool ticks can complete the work.
func ReconcileSessionHistory(deadline controlbudget.Deadline, stop <-chan struct{}, cfg SessionHistoryConfig) error {
	ctx, cancel, err := sessionContext(deadline, stop)
	if err != nil {
		return err
	}
	defer cancel()
	deleting, err := ListSessionSIDs(cfg.DB, "DELETING")
	if err != nil {
		return fmt.Errorf("list DELETING: %w", err)
	}
	for _, sid := range deleting {
		if err := cfg.Finalize(deadline, stop, sid); err != nil {
			cfg.Logger.Warn("broker: resume DELETING failed", "sid", sid, "err", err)
		}
	}

	active, err := ListSessionSIDs(cfg.DB, "ACTIVE")
	if err != nil {
		return fmt.Errorf("list ACTIVE: %w", err)
	}
	activeSet := map[string]bool{}
	for _, sid := range active {
		activeSet[sid] = true
		ensureCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := jsstream.EnsureHistoryStream(ensureCtx, cfg.JS, sid, jsstream.ReplicasSingle); err != nil {
			cfg.Logger.Warn("broker: ensure history stream on boot",
				"sid", sid, "err", err)
		}
		cancel()
	}

	// A failed finalize leaves the tombstone AND its evidence intact. Re-read
	// after finalization so a completed session can be reaped in this pass;
	// retaining every remaining tombstone also covers an incomplete census.
	// Do not EnsureHistoryStream for these sessions: an empty replacement is
	// not the original audit proof after a crash.
	// origin: hot-update S7 plan §6.4 / T31
	remainingDeleting, err := ListSessionSIDs(cfg.ReadDB(), "DELETING")
	if err != nil {
		return fmt.Errorf("list protected DELETING: %w", err)
	}
	for _, sid := range remainingDeleting {
		activeSet[sid] = true
	}

	// audit G (CRITICAL data-loss): the orphan DELETE arms below list the SHARED JS meta
	// (ListHistorySIDs / ListXferStreams) but classify "orphan" against the LOCAL activeSet. On a fresh
	// joiner — or ANY not-yet-caught-up follower — the local SQLite view is stale/empty, so EVERY
	// cluster-wide stream looks orphan and would be WIPED (the first real grow would delete all cluster
	// history + every in-flight tier-B bucket). Only a CAUGHT-UP LEADER has an activeSet that reflects
	// the true cluster state. In single mode (b.cl == nil) the local DB IS the authority — reap as before.
	if !cfg.MayDelete() {
		cfg.Logger.Info("broker: skipping boot orphan stream/bucket reap (not a caught-up leader — refuses to wipe cluster-wide history from a stale local view)")
		return nil
	}

	listCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	streamSIDs, err := jsstream.ListHistorySIDs(listCtx, cfg.JS)
	cancel()
	if err != nil {
		return fmt.Errorf("list history streams: %w", err)
	}
	for _, sid := range streamSIDs {
		if activeSet[sid] {
			continue
		}
		dropCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := jsstream.DeleteHistoryStream(dropCtx, cfg.JS, sid); err != nil {
			cfg.Logger.Warn("broker: orphan stream delete",
				"sid", sid, "err", err)
		} else {
			cfg.Logger.Info("broker: orphan history stream deleted", "sid", sid)
		}
		cancel()
	}

	// audit M4 / transfer F1: reap orphan tier-B buckets the same way. A bucket whose session
	// row is gone (no longer ACTIVE, and any DELETING session was finalized above) has no live
	// transfer, so deleting the WHOLE bucket is safe — and necessary: a lingering OBJ_xfer-<sid>
	// below target replicas blocks the D7 retire gate (AllAtTarget) forever. DeleteXferBucket
	// tolerates "already gone", so concurrent brokers racing the delete in cluster mode is
	// harmless (mirrors the history orphan reaper, which is likewise un-gated on home ownership
	// because a truly-orphan stream cannot be live anywhere).
	xferCtx, cancelX := context.WithTimeout(ctx, 3*time.Second)
	xferStreams, xerr := jsstream.ListXferStreams(xferCtx, cfg.JS)
	cancelX()
	if xerr != nil {
		return fmt.Errorf("list xfer streams: %w", xerr)
	}
	for _, name := range xferStreams {
		sid, ok := jsstream.SIDFromXferStream(name)
		if !ok || activeSet[sid] {
			continue
		}
		dropCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := jsstream.DeleteXferBucket(dropCtx, cfg.JS, sid); err != nil {
			cfg.Logger.Warn("broker: orphan xfer bucket delete", "sid", sid, "err", err)
		} else {
			cfg.Logger.Info("broker: orphan xfer bucket deleted", "sid", sid)
		}
		cancel()
	}
	return nil
}

// cascadeSessionRm runs phases ②③④ of architecture H.3 for a
// session that's already in state=DELETING (phase ① done by the
// caller, either handleSessionRm or the boot resumer) and whose
// transfer settlement barrier, if any, has already released it.
// Idempotent: every step is safe to retry, so a crash mid-finalize
// re-runs cleanly when the broker comes back up.
//
// Phase ② DELETE the history-<sid> stream (no-op if already gone).
// Phase ③ DELETE all dependent SQLite rows in one tx; sessions row
//
//	included so future ① on the same sid can re-create.
//
// Phase ④ pub sys.events session_destroyed.
//
// We deliberately keep ② before ③: if ③ runs first and the stream
// delete then fails, we have an orphan stream that the next boot
// would re-pair with a re-created session row of the same sid (bad
// audit-history pollution). Doing ② first means the worst case is
// "DB rows linger after ② succeeded" — those get reaped by re-runs
// of finalizeSessionRm, and any new req gets rejected by C.1 §6 in
// the meantime because state is still DELETING.
func CascadeSession(deadline controlbudget.Deadline, stop <-chan struct{}, js jetstream.JetStream, sid string, drop func(string) error) error {
	ctx, cancel, err := sessionContext(deadline, stop)
	if err != nil {
		return err
	}
	defer cancel()
	if js != nil {
		if err := jsstream.DeleteHistoryStream(ctx, js, sid); err != nil {
			return fmt.Errorf("phase 2: %w", err)
		}
		// Phase ②.5 (audit M4 / transfer F1): delete the tier-B transfer object store too.
		// Without this the OBJ_xfer-<sid> bucket outlives the purged session row; at N>=3 a
		// lingering bucket below target replicas permanently fails the retire gate. Idempotent
		// (DeleteXferBucket tolerates "already gone"), so a crash-mid-rm re-run is clean.
		if err := jsstream.DeleteXferBucket(ctx, js, sid); err != nil {
			return fmt.Errorf("phase 2.5 (xfer bucket): %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := drop(sid); err != nil {
		return fmt.Errorf("phase 3: %w", err)
	}
	return nil
}

// SettleSession is the barrier half of a removal, shared by the handler and
// the boot DELETING resumer. Any error means the barrier did NOT release the
// session: its history/OBJ/SQL evidence must stay. The barrier inherits the
// removal's original absolute deadline; Stop never grants deletion authority.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31
func SettleSession(deadline controlbudget.Deadline, stop <-chan struct{}, barrier SessionBarrier, governed bool, sid string, abandon bool) error {
	if !governed {
		return nil
	}
	if barrier == nil {
		return &PendingError{Transfers: []PendingTransfer{{TransferID: "*", Reason: "origin_census_unavailable"}}}
	}
	// Both callers supply the same bounded admission, including cold startup.
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return err
	}
	select {
	case <-stop:
		return context.Canceled
	default:
	}
	if err := barrier.Settle(sid, abandon, deadline); err != nil {
		return err
	}
	select {
	case <-stop:
		return context.Canceled
	default:
		_, err := controlbudget.Remaining(deadline)
		return err
	}
}

// ResumeDeletingSession is the boot DELETING resumer's entry: it re-reads the
// tombstone it is resuming, then settles and cascades exactly as the
// interactive handler does after its own authoritative tombstone result.
//
// The barrier protects both interactive removal and cold/periodic resumption.
// An unavailable census is retained responsibility, never permission to erase
// history. Only the explicit operator request can stage abandonment decisions,
// and that request reaches the barrier through handleSessionRm alone.
//
// The local state read is right here, and only here: the resumer enumerated
// these sids from the same local store, so the read is self-consistent. The
// handler must not repeat it — on a cluster follower the local replica can
// trail the tombstone the leader just committed for it.
// origin: hot-update S7 plan §6.4 / lead LQ1 / T31; S7 lead integration decision 3
func ResumeDeletingSession(deadline controlbudget.Deadline, stop <-chan struct{}, db *sql.DB, sid string, settle func(controlbudget.Deadline, <-chan struct{}) error, cascade func(controlbudget.Deadline, <-chan struct{}) error) error {
	limit, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	deadline = min(deadline, limit)
	ctx, cancel, err := sessionContext(deadline, stop)
	if err != nil {
		return err
	}
	defer cancel()
	var state string
	err = db.QueryRowContext(ctx, `SELECT state FROM sessions WHERE sid = ?`, sid).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if state != string(session.StateDeleting) {
		return session.ErrDeleting
	}
	if err := settle(deadline, stop); err != nil {
		return err
	}
	return cascade(deadline, stop)
}

// The SQL/SDK adapters have no caller context. Stop owns cancellation, and the
// returned release joins its watcher before an adapter can retire its work.
// origin: S7 internal review INTEG-M F4
func sessionContext(deadline controlbudget.Deadline, stop <-chan struct{}) (context.Context, func(), error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	select {
	case <-stop:
		return nil, nil, context.Canceled
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-none: deadline/Stop control adapter has no caller context.
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() { cancel(); <-joined }, nil
}

// Receiver verification helpers share the settlement phase vocabulary.
// They are separate from operator abandonment and cancellation decisions.
func settlementPhase(p xfercontract.Phase) bool {
	switch p {
	case xfercontract.PhaseOpen, xfercontract.PhasePrepared, xfercontract.PhaseSending, xfercontract.PhaseReceiving,
		xfercontract.PhaseCommitReserved, xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending,
		xfercontract.PhaseHistoryConfirmed, xfercontract.PhaseCleanupPending:
		return true
	case xfercontract.PhaseSettled:
		return false
	default:
		return false
	}
}

// An administrative decision withdraws the capsule even while cleanup
// remains pending. A cached CONNECT cannot reopen the terminal slot.
// origin: S7 internal review rv-wiring F5 / hot-update S7 plan lead LQ1
func retainedSettlement(row xfercontract.LedgerRow, id xfercontract.Identity) (*xfercontract.SettlementCapsule, error) {
	if !row.OutcomeUnknown && row.Identity == id && row.Settlement != nil && row.Settlement.Identity == id && row.Settlement.Origin == row.Origin && row.EntryNonce == id.EntryNonce && row.Revision > 0 && settlementPhase(row.Phase) {
		return row.Settlement, nil
	}
	return nil, ErrDenied
}
