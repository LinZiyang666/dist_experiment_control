package xfertrack

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

var _ Factory = New
var _ ImportFactory = Import
var _ x.DecisionPort = (*decisionView)(nil)

// Each entry owns its proposed revision even when both durable directories are unavailable. No reply may turn that in-memory proposal into a
// durable Grant.
// origin: hot-update S7 plan §4 / §5.1 / T13
type tracked struct {
	row         x.LedgerRow
	dirty       bool
	kind        x.LedgerMutationKind
	watch       x.WatchdogDeadline
	reservation x.ReservationID
	// reaped: the byte budget expired before any terminal existed, so the
	// watchdog (not the receiver's own report) ended this entry.
	reaped bool
}
type owner struct {
	cfg                         Config
	serial                      chan struct{}
	entries                     map[x.Key]*tracked
	reapedReservations          map[x.Key]x.ReservationID
	state                       x.TrackerState
	sleeping, stopped, pristine bool
	halt, joined                chan struct{}
}

func New(cfg Config) (Manager, error) {
	if cfg.Clock == nil || cfg.UTC == nil || cfg.Capacity == nil || cfg.Ledger == nil || cfg.Origin.BootID == "" || cfg.Origin.InstanceID == "" {
		return nil, errors.New("xfertrack: missing owner clock, identity or ledger")
	}
	o := newOwner(cfg)
	o.startLoop()
	return o, nil
}
func newOwner(cfg Config) *owner {
	o := &owner{cfg: cfg, serial: make(chan struct{}, 1), entries: make(map[x.Key]*tracked)}
	o.serial <- struct{}{}
	o.state = x.TrackerState{Schema: x.StateSchema, Origin: cfg.Origin, Generation: cfg.Origin.Generation}
	return o
}
func key(id x.Identity) x.Key   { return x.Key{SID: id.SID, TransferID: id.TransferID} }
func refusal(code string) error { return trackerRefusal(code) }

// trackerRefusal keeps the stable code typed. The controller maps an untyped owner error to result_unknown, which turned an honest, retryable
// single-slot capacity refusal into an exit-74 "outcome unknown".
// origin: hot-update S7 plan §6.1 / §7 — S7 production activation
type trackerRefusal string

func (r trackerRefusal) Error() string        { return "xfertrack: " + string(r) }
func (r trackerRefusal) TransferCode() string { return string(r) }
func digest(b []byte) string                  { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func terminalID(id x.Identity) string {
	return digest([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
}
func validIdentity(id x.Identity) bool {
	return id.SID != "" && id.NID != "" && id.ActorFP != "" && id.AgentInstance != "" && id.TransferID != "" &&
		id.EntryNonce != "" && id.IntentDigest != "" && len(id.TransferID) <= transferIDMaxLen &&
		len(id.SID) <= transferIDMaxLen && len(id.NID) <= transferIDMaxLen && len(id.ActorFP) <= transferIDMaxLen &&
		len(id.AgentInstance) <= transferIDMaxLen && len(id.EntryNonce) <= transferIDMaxLen && len(id.IntentDigest) <= transferIDMaxLen && len(id.ObjectVersion) <= transferIDMaxLen
}
func (o *owner) lookup(id x.Identity) (*tracked, error) {
	e := o.entries[key(id)]
	// origin: hot-update S7 plan §4 / T13 — late callbacks cannot target a reused key.
	if e == nil {
		return nil, refusal(x.CodeUnknown)
	}
	if e.row.Identity != id {
		return nil, refusal(x.CodeIntentConflict)
	}
	return e, nil
}

// The ledger returns the exact proposed revision. An ACK for a different row is a conflict, even if its business key happens to be the same.
func (o *owner) persist(e *tracked, row x.LedgerRow, kind x.LedgerMutationKind, d x.Deadline) error {
	if e.dirty {
		return refusal(x.CodeOwnerRecovering)
	}
	row.Revision = e.row.Revision + 1
	encoded, err := json.Marshal(row)
	if err != nil || len(encoded) > 32<<10 {
		return refusal(x.CodeCapacityExceeded)
	}
	e.row = cloneRow(row)
	e.kind, e.dirty = kind, true
	return o.flush(e, d)
}
func (o *owner) flush(e *tracked, d x.Deadline) error {
	if !e.dirty {
		return nil
	}
	if o.cfg.Ledger == nil {
		return refusal(x.CodeOwnerRecovering)
	}
	proposed := cloneRow(e.row)
	got, err := o.cfg.Ledger.Update(key(proposed.Identity), proposed.Revision-1,
		x.LedgerMutation{Kind: e.kind, EntryNonce: proposed.EntryNonce, Row: proposed}, d)
	if err != nil {
		// An Update can have synced before its reply was lost. Only a complete
		// exact readback discharges that ambiguity; absence preserves the proposal.
		census, scanErr := o.cfg.Ledger.Scan(d)
		if scanErr != nil || !census.Ready || !census.Complete {
			return err
		}
		found := false
		for _, row := range census.Rows {
			if key(row.Identity) == key(proposed.Identity) && reflect.DeepEqual(row, proposed) {
				got = row
				found = true
			}
		}
		if !found {
			return err
		}
	}
	if !reflect.DeepEqual(got, proposed) {
		return refusal(x.CodeTerminalConflict)
	}
	e.dirty = false
	o.state.LedgerRevision = max(o.state.LedgerRevision, got.Revision)
	return nil
}
func (o *owner) Admit(p x.CreationPermit, intent x.Intent, d x.Deadline) (x.Status, error) {
	if err := o.enter(d); err != nil {
		return x.Status{}, err
	}
	defer o.leave()
	if err := o.census(d); err != nil {
		return x.Status{}, err
	}
	if e := o.entries[key(p.Identity)]; e != nil {
		if e.row.Identity != p.Identity || e.row.Intent != intent || e.row.Permit != p {
			return x.Status{}, refusal(x.CodeIntentConflict)
		}
		if err := o.flush(e, d); err != nil {
			return x.Status{}, err
		}
		return o.status(e), nil
	}
	// Expiry fences first creation only. Replays above keep the original entry.
	// origin: hot-update S7 plan §6.1 / T13
	if p.Origin != o.cfg.Origin || !validIdentity(p.Identity) || p.PermitNonce == "" || p.Signature == "" {
		return x.Status{}, refusal(x.CodeIntentConflict)
	}
	now := o.cfg.UTC().UTC()
	if !now.Before(p.CreateUntil) || p.IssuedAt.After(now) {
		return x.Status{}, refusal(x.CodePermitExpired)
	}
	budget, err := budgetFor(intent, intent.Size, intent.Verb == "pull")
	if err != nil {
		return x.Status{}, err
	}
	if transferRequestBounds(p.Identity.TransferID, intent.Path, intent.SHA256) != "" || intent.Path == "" {
		return x.Status{}, refusal(x.CodeIntentConflict)
	}
	if err := admitAfterReaped(o, p.Identity); err != nil {
		return x.Status{}, err
	}
	// Preserve per-session bucket single flight while an object can still be
	// written or read. An exact, history-confirmed terminal ends that byte work:
	// its remaining cleanup purges only its own fixed NUID, and no producer yet
	// supplies the writer-retirement proof that cleanup needs (see INTEG-D
	// request: P09/P17 writer retirement report). Holding the slot until then
	// refused every later Tier B transfer of the session.
	// origin: hot-update S7 plan §1.2 / §5.3 / INTEG-D sequential transfers
	for _, live := range o.entries {
		if intent.Tier == "b" && live.row.Intent.Tier == "b" && live.row.Identity.SID == p.Identity.SID && live.row.Phase == x.PhaseCleanupPending {
			// A finalizer may already have retired this row durably; observe it
			// now rather than refusing until the next watchdog pass does.
			// origin: hot-update S7 plan §5.3 — S7 production activation
			_ = o.settleRetired(live, d)
		}
		decided := live.row.CanonicalTerminal != nil && live.row.TerminalDelivery == x.DeliveryHistoryConfirmed
		if intent.Tier == "b" && live.row.Intent.Tier == "b" && live.row.Identity.SID == p.Identity.SID && live.row.Phase != x.PhaseSettled && !decided {
			return x.Status{}, refusal(x.CodeCapacityExceeded)
		}
	}
	boot, ns, err := o.cfg.Clock()
	if err != nil {
		return x.Status{}, err
	}
	if boot != o.cfg.Origin.BootID || ns <= 0 || ns > int64(^uint64(0)>>1)-int64(budget) {
		return x.Status{}, refusal(x.CodeOwnerRecovering)
	}
	// START is the only admitted delivery slot; TERMINAL gains a pending
	// delivery only when its canonical decision exists. Marking TERMINAL
	// pending here would make the real ledger reject a row with no canonical
	// terminal bytes.
	// origin: hot-update S7 plan §5.1 / T13 / INTEG-E durable admission regression
	// origin: hot-update S7 plan §5.1 / T12 / T39
	row := x.LedgerRow{Identity: p.Identity, Origin: p.Origin, Permit: p, Intent: intent, EntryNonce: p.Identity.EntryNonce,
		StartedAt: now, Phase: x.PhaseOpen, StartDelivery: x.DeliveryPending,
		BudgetSeal: x.BudgetSeal{StartedAt: now, Size: intent.Size, RateLimitBPS: intent.RateLimitBPS, EffectiveBudgetNS: int64(budget), RemainingNS: int64(budget)},
		Retention:  x.RetentionState{Identity: p.Identity, CreateUntil: p.CreateUntil}, Cleanup: x.CleanupState{Identity: p.Identity}}
	row.CanonicalStart = canonical(row, "start", x.ReceiverOutcome{}, now)
	e := &tracked{watch: x.WatchdogDeadline{Identity: p.Identity, ByteDeadline: x.BootDeadline{BootID: boot, Deadline: x.Deadline(ns + int64(budget))}}}
	if err = o.reserveCapacity(e); err != nil {
		return x.Status{}, err
	}
	o.entries[key(p.Identity)] = e
	if err = o.persist(e, row, x.MutationAdmit, d); err != nil {
		return x.Status{}, err
	}
	return o.status(e), nil
}

// Pull discovery narrows the original budget; it cannot renew startedAt or replace a terminal with an old START snapshot.
// origin: hot-update S7 plan §1.2 / §5.1 / T13
func (o *owner) SourceSeal(id x.Identity, seal x.SourceSeal, d x.Deadline) (x.Status, error) {
	if err := o.enter(d); err != nil {
		return x.Status{}, err
	}
	defer o.leave()
	e, err := o.lookup(id)
	if err != nil {
		return x.Status{}, err
	}
	if err = o.flush(e, d); err != nil {
		return x.Status{}, err
	}
	if e.row.SourceSeal != nil {
		if *e.row.SourceSeal != seal {
			return x.Status{}, refusal(x.CodeSourceChanged)
		}
		return o.status(e), nil
	}
	if e.row.Grant != nil || e.row.CanonicalTerminal != nil {
		return x.Status{}, refusal(x.CodeIntentConflict)
	}
	// Tier A is the small inline path: it has no OBJ geometry at all, and an
	// inline seal can never claim chunks or an object-sized prefix. Tier B keeps
	// its strict nonzero geometry. Refusing zero geometry for Tier A made every
	// inline Reserve impossible (it requires this seal).
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A inline seal
	inline := e.row.Intent.Tier == "a"
	if seal.Size < 0 || seal.Size > proto.XferMaxBytes || len(seal.SHA256) != 64 || seal.RateLimitBPS != e.row.Intent.RateLimitBPS ||
		(inline && (seal.ChunkSize != 0 || seal.Chunks != 0 || seal.Size > proto.XferTierAMaxBytes)) ||
		(!inline && (seal.ChunkSize == 0 || uint64(seal.Chunks) != (uint64(seal.Size)+uint64(seal.ChunkSize)-1)/uint64(seal.ChunkSize))) {
		return x.Status{}, refusal(x.CodeSourceChanged)
	}
	if e.row.Intent.Verb == "push" && (seal.Size != e.row.Intent.Size || seal.SHA256 != e.row.Intent.SHA256) {
		return x.Status{}, refusal(x.CodeSourceChanged)
	}
	budget, err := budgetFor(e.row.Intent, seal.Size, false)
	if err != nil {
		return x.Status{}, err
	}
	row := cloneRow(e.row)
	row.SourceSeal = &seal
	row.BudgetSeal.Size, row.BudgetSeal.SourceSealed = seal.Size, true
	elapsed := time.Duration(e.row.BudgetSeal.EffectiveBudgetNS) - o.remaining(e)
	row.BudgetSeal.EffectiveBudgetNS = int64(budget)
	row.BudgetSeal.RemainingNS = max(0, int64(budget-elapsed))
	oldBudget := e.row.BudgetSeal.EffectiveBudgetNS
	e.watch.ByteDeadline.Deadline += x.Deadline(int64(budget) - oldBudget)
	if err = o.persist(e, row, x.MutationSourceSeal, d); err != nil {
		return x.Status{}, err
	}
	return o.status(e), nil
}
func (o *owner) Reserve(id x.Identity, r x.ReserveRequest, d x.Deadline) (x.CommitGrant, error) {
	if err := o.enter(d); err != nil {
		return x.CommitGrant{}, err
	}
	defer o.leave()
	e, err := o.lookup(id)
	if err != nil {
		return x.CommitGrant{}, err
	}
	if r.Origin != e.row.Origin || r.ReceiverInstance == "" {
		return x.CommitGrant{}, refusal(x.CodeIntentConflict)
	}
	if err = o.flush(e, d); err != nil {
		return x.CommitGrant{}, err
	}
	if e.row.Grant != nil {
		if e.row.Grant.ReceiverInstance != r.ReceiverInstance {
			return x.CommitGrant{}, refusal(x.CodeIntentConflict)
		}
		return *e.row.Grant, nil
	}
	// A durable failure closes the slot; a late Reserve cannot reopen its gate.
	// origin: hot-update S7 plan §4 / T12
	if e.row.CanonicalTerminal != nil || e.watch.Fired || o.remaining(e) <= 0 {
		return x.CommitGrant{}, refusal(x.CodeBudgetExceeded)
	}
	if e.row.SourceSeal == nil {
		return x.CommitGrant{}, refusal(x.CodeSourceChanged)
	}
	nonce := make([]byte, 32)
	if _, err = rand.Read(nonce); err != nil {
		return x.CommitGrant{}, err
	}
	grant := x.CommitGrant{Identity: id, GrantNonce: hex.EncodeToString(nonce), ReceiverInstance: r.ReceiverInstance, TerminalID: terminalID(id)}
	row := cloneRow(e.row)
	row.Grant = &grant
	row.Phase = x.PhaseCommitReserved
	if err = o.persist(e, row, x.MutationReserve, d); err != nil {
		return x.CommitGrant{}, err
	}
	o.observe(x.ObserveReserve, e)
	return grant, nil
}

func (o *owner) Finish(id x.Identity, out x.ReceiverOutcome, d x.Deadline) (x.TerminalReceipt, error) {
	if err := o.enter(d); err != nil {
		return x.TerminalReceipt{}, err
	}
	e, err := o.lookup(id)
	if err == nil {
		err = o.finish(e, out, d)
	}
	if err == nil {
		err = o.confirm(e, d)
	}
	var receipt x.TerminalReceipt
	if err == nil {
		receipt, err = terminalReceipt(e.row)
	}
	o.leave()
	return receipt, err
}
func (o *owner) finish(e *tracked, out x.ReceiverOutcome, d x.Deadline) error {
	if err := o.flush(e, d); err != nil {
		return err
	}
	if e.row.Grant == nil {
		if out.Grant != (x.CommitGrant{}) || out.Outcome != x.OutcomeNotEntered || e.row.Phase != x.PhaseOpen && e.row.CanonicalTerminal == nil {
			return refusal(x.CodeTerminalConflict)
		}
	} else if out.Grant != *e.row.Grant {
		return refusal(x.CodeTerminalConflict)
	}
	if e.row.CanonicalTerminal != nil {
		if e.row.ReceiverOutcome == nil || *e.row.ReceiverOutcome != out {
			return refusal(x.CodeTerminalConflict)
		}
		return nil
	}
	row := cloneRow(e.row)
	if row.ReceiverOutcome != nil {
		old := *row.ReceiverOutcome
		if old.Outcome == x.OutcomeLanded && (out.Outcome != x.OutcomeLanded || old.Primitive != out.Primitive || old.Bytes != out.Bytes || old.SHA256 != out.SHA256 || old.ParentSynced && !out.ParentSynced) {
			return refusal(x.CodeTerminalConflict)
		}
	}
	kind := ""
	switch out.Outcome {
	case x.OutcomeNotEntered:
		// Bytes records the synced tmp prefix, not an effect on the destination.
		// Stop and a failed link can both retain the full prefix without entering
		// a successful primitive. Its original size still bounds that evidence.
		// origin: S7 internal review rv-terminal F2 / rv-recovery F03
		if out.Bytes < 0 || out.Bytes > row.BudgetSeal.Size || out.ParentSynced {
			return refusal(x.CodeTerminalConflict)
		}
		kind = "failed"
	case x.OutcomeInflight:
		if row.Grant == nil || out.ParentSynced {
			return refusal(x.CodeTerminalConflict)
		}
	case x.OutcomeLanded:
		if row.Grant == nil || row.SourceSeal == nil || out.Bytes != row.SourceSeal.Size || out.SHA256 != row.SourceSeal.SHA256 ||
			out.Primitive != x.PrimitiveRename && out.Primitive != x.PrimitiveLink {
			return refusal(x.CodeTerminalConflict)
		}
		if out.ParentSynced {
			kind = "complete"
		}
	default:
		return refusal(x.CodeResultUnknown)
	}
	row.ReceiverOutcome = &out
	row.Cleanup.DirectorySyncPending = out.Outcome == x.OutcomeLanded && !out.ParentSynced
	row.Cleanup.TmpUnlinkPending = out.TmpCleanupPending
	if kind != "" {
		row.CanonicalTerminal = canonical(row, kind, out, o.cfg.UTC().UTC())
		row.TerminalDelivery = x.DeliveryPending
		row.Phase = x.PhaseDecided
		row.Cleanup.ObjectPurgePending = row.Intent.Tier == "b"
		row.Cleanup.PrimaryRetirePending = true
		row.Cleanup.OutboxRetirePending = true
	} else {
		row.Phase = x.PhaseCommitReserved
	}
	if err := o.persist(e, row, x.MutationDecide, d); err != nil {
		return err
	}
	o.observe(x.ObserveFinish, e)
	if kind == "" {
		if out.Outcome == x.OutcomeLanded {
			return refusal(x.CodeDurabilityUnknown)
		}
		return refusal(x.CodeCommitInflight)
	}
	return nil
}

func (o *owner) Cancel(id x.Identity, r x.CancelRequest, d x.Deadline) (x.Status, error) {
	if err := o.enter(d); err != nil {
		return x.Status{}, err
	}
	defer o.leave()
	e, err := o.lookup(id)
	if err != nil {
		return x.Status{}, err
	}
	if r.Origin != e.row.Origin {
		return x.Status{}, refusal(x.CodeIntentConflict)
	}
	switch r.Reason {
	case x.CancelUser, x.CancelDeadline, x.CancelRevoked, x.CancelStopped:
	default:
		return x.Status{}, refusal(x.CodeIntentConflict)
	}
	return o.cancel(e, d)
}

// Abandon is the operator's explicit escape hatch for a session whose original receiver cannot settle (lead LQ1). Through this owner's own
// serializer, it decides the entry's one terminal slot as failed/settlement_abandoned with outcome_unknown=true and publishes it like any
// decision. It never invents a receiver result: retained evidence (Grant, inflight or landed-but-unsynced bytes) is kept and only labelled
// abandoned; an unknown outcome stays unknown. An existing terminal, or a receiver that already decided, keeps that decision. Writing the
// ledger behind the tracker would fork its revision.
// origin: hot-update S7 plan lead LQ1 / §6.4 / T31 / INTEG-D session abandonment
func (o *owner) Abandon(id x.Identity, d x.Deadline) (x.Status, error) {
	if err := o.enter(d); err != nil {
		return x.Status{}, err
	}
	defer o.leave()
	e, err := o.lookup(id)
	if err != nil {
		return x.Status{}, err
	}
	e.watch.Fired = true // no later Reserve can grant a byte effect.
	if err = o.flush(e, d); err != nil {
		return o.status(e), err
	}
	if e.row.CanonicalTerminal == nil {
		out := x.ReceiverOutcome{Outcome: x.OutcomeInflight}
		if e.row.Grant != nil {
			out.Grant = *e.row.Grant
		}
		if prior := e.row.ReceiverOutcome; prior != nil {
			if prior.Outcome != x.OutcomeInflight && (prior.Outcome != x.OutcomeLanded || prior.ParentSynced) {
				return o.status(e), refusal(x.CodeTerminalConflict)
			}
			out = *prior
		}
		out.Code, out.Error, out.TmpCleanupPending = x.ReasonSettlementAbandoned, "settlement abandoned by the session owner", false
		row := cloneRow(e.row)
		row.ReceiverOutcome, row.OutcomeUnknown = &out, true
		row.CanonicalTerminal = canonicalRecord(row, "failed", out, o.cfg.UTC().UTC(), true)
		row.TerminalDelivery, row.Phase = x.DeliveryPending, x.PhaseDecided
		row.Cleanup.DirectorySyncPending, row.Cleanup.TmpUnlinkPending = false, false
		row.Cleanup.ObjectPurgePending = row.Intent.Tier == "b"
		row.Cleanup.PrimaryRetirePending, row.Cleanup.OutboxRetirePending = true, true
		if err = o.persist(e, row, x.MutationAbandon, d); err != nil {
			return o.status(e), err
		}
		o.observe(x.ObserveFinish, e)
	}
	if err = o.confirm(e, d); err != nil {
		return o.status(e), err
	}
	return o.status(e), nil
}

func (o *owner) cancel(e *tracked, d x.Deadline) (x.Status, error) {
	e.watch.Fired = true // close future Reserve before asking the original receiver.
	if e.row.CanonicalTerminal != nil {
		return o.status(e), nil
	}
	if o.cfg.ResolveReceiver == nil {
		return o.status(e), refusal(x.CodeResultUnknown)
	}
	o.observe(x.ObserveCallbackEnter, e)
	out, err := o.cfg.ResolveReceiver.Resolve(e.row.Identity, d)
	o.observe(x.ObserveCallbackJoin, e)
	// origin: hot-update S7 plan §4 / T12 / M06 — no receiver evidence means no failure.
	if err != nil {
		return o.status(e), err
	}
	if err = o.finish(e, out, d); err != nil {
		return o.status(e), err
	}
	if err = o.confirm(e, d); err != nil {
		return o.status(e), err
	}
	return o.status(e), nil
}

func (o *owner) confirm(e *tracked, d x.Deadline) error {
	if err := o.flush(e, d); err != nil {
		return err
	}
	if o.cfg.History == nil {
		return refusal(x.CodeResultUnknown)
	}
	for _, record := range []*x.CanonicalRecord{e.row.CanonicalStart, e.row.CanonicalTerminal} {
		if record == nil || hasReceipt(e.row, *record) {
			continue
		}
		// The eager Finish path also owns publication, before the finalizer
		// can run. Emit its pending boundary before the synchronous history ACK.
		// origin: S7 internal review INTEG-O F5
		if record == e.row.CanonicalTerminal && drillobserve.Hook != nil {
			pending := *e
			pending.row.Phase = x.PhaseTerminalPending
			o.observe(x.ObserveFinish, &pending)
		}
		receipt, err := o.cfg.History.Confirm(*cloneRecord(record), d)
		if err != nil {
			return err
		}
		if err = o.acceptHistory(e, receipt, d); err != nil {
			return err
		}
	}
	return nil
}
func hasReceipt(row x.LedgerRow, record x.CanonicalRecord) bool {
	for _, r := range row.HistoryReceipts {
		if r.RecordDigest == record.Digest && r.Stream != "" && r.Sequence > 0 {
			return true
		}
	}
	return false
}
func (o *owner) acceptHistory(e *tracked, r x.HistoryReceipt, d x.Deadline) error {
	if r.Stream == "" || r.Sequence == 0 || r.RecordDigest == "" {
		return refusal(x.CodeTerminalConflict)
	}
	row := cloneRow(e.row)
	if row.CanonicalStart != nil && row.CanonicalStart.Digest == r.RecordDigest {
		row.StartDelivery = x.DeliveryHistoryConfirmed
	} else if row.CanonicalTerminal != nil && row.CanonicalTerminal.Digest == r.RecordDigest {
		row.TerminalDelivery = x.DeliveryHistoryConfirmed
	} else {
		return refusal(x.CodeTerminalConflict)
	}
	for _, old := range row.HistoryReceipts {
		if old.RecordDigest == r.RecordDigest {
			if old != r {
				return refusal(x.CodeTerminalConflict)
			}
			return nil
		}
	}
	row.HistoryReceipts = append(row.HistoryReceipts, r)
	if row.StartDelivery == x.DeliveryHistoryConfirmed && row.TerminalDelivery == x.DeliveryHistoryConfirmed {
		row.Phase = x.PhaseHistoryConfirmed
		row.Retention.ResultUntil = o.cfg.UTC().UTC().Add(xferRecentlyReapedTTL)
		boot, ns, err := o.cfg.Clock()
		if err != nil {
			return err
		}
		row.Retention.Deadline = x.BootDeadline{BootID: boot, Deadline: x.Deadline(ns + int64(xferRecentlyReapedTTL))}
	}
	return o.persist(e, row, x.MutationHistory, d)
}
func terminalReceipt(row x.LedgerRow) (x.TerminalReceipt, error) {
	if row.CanonicalTerminal == nil || row.StartDelivery != x.DeliveryHistoryConfirmed || row.TerminalDelivery != x.DeliveryHistoryConfirmed {
		return x.TerminalReceipt{}, refusal(x.CodeResultUnknown)
	}
	rec := row.CanonicalTerminal
	if !hasReceipt(row, *rec) || row.CanonicalStart == nil || !hasReceipt(row, *row.CanonicalStart) {
		return x.TerminalReceipt{}, refusal(x.CodeTerminalConflict)
	}
	out := x.TerminalReceipt{TerminalID: terminalID(row.Identity), Kind: rec.Kind, CanonicalDigest: rec.Digest, ReqID: rec.ReqID, OutcomeUnknown: row.OutcomeUnknown}
	if row.ReceiverOutcome != nil {
		out.Bytes = row.ReceiverOutcome.Bytes
		out.SHA256 = row.ReceiverOutcome.SHA256
		out.Code = row.ReceiverOutcome.Code
	}
	for _, r := range row.HistoryReceipts {
		if r.RecordDigest == rec.Digest {
			out.HistoryStream = r.Stream
			out.HistorySeq = r.Sequence
		}
	}
	return out, nil
}
func (o *owner) status(e *tracked) x.Status {
	s := x.Status{State: x.StatusLive, Phase: e.row.Phase, Identity: e.row.Identity, Origin: e.row.Origin, Budget: e.row.BudgetSeal,
		CleanupPending: e.row.Cleanup.DirectorySyncPending || e.row.Cleanup.TmpUnlinkPending || e.row.Cleanup.ObjectPurgePending || e.row.Cleanup.PrimaryRetirePending || e.row.Cleanup.OutboxRetirePending}
	s.Budget.RemainingNS = max(0, int64(o.remaining(e)))
	if !e.dirty {
		if receipt, err := terminalReceipt(e.row); err == nil {
			s.State = x.StatusTerminal
			s.Terminal = &receipt
		}
	}
	if e.watch.Fired && s.State == x.StatusLive {
		s.Code = x.CodeResultUnknown
	}
	return s
}

// Canonical bytes preserve the existing audit shape and content-key prefix. The continuation identity is additive; timestamps are minted once
// per slot. Exact history verification decodes this complete identity from the bytes.
// origin: hot-update S7 plan §5.2 / T39
func canonical(row x.LedgerRow, kind string, out x.ReceiverOutcome, at time.Time) *x.CanonicalRecord {
	return canonicalRecord(row, kind, out, at, false)
}

// canonicalRecord also mints the abandoned slot: OutcomeUnknown is part of the shared audit schema, so history and the ledger see the same
// exact bytes.
func canonicalRecord(row x.LedgerRow, kind string, out x.ReceiverOutcome, at time.Time, unknown bool) *x.CanonicalRecord {
	audit := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: row.Intent.Verb, Ts: at.Round(0),
		Session: row.Identity.SID, Node: row.Identity.NID, ActorFp: row.Identity.ActorFP, TransferID: row.Identity.TransferID,
		Path: row.Intent.Path, Size: row.BudgetSeal.Size, SHA256: row.Intent.SHA256, Tier: row.Intent.Tier}
	if row.Intent.Tier == "b" {
		audit.Bucket = "xfer-" + row.Identity.SID
	}
	if kind != "start" {
		audit.Bytes = out.Bytes
		audit.SHA256 = out.SHA256
		audit.Code = out.Code
		audit.Error = out.Error
		audit.DurationMs = msSince(row.StartedAt, at)
	}
	// Use the shared additive audit schema so history and the publisher receive
	// the same complete continuation identity, including the agent instance.
	// origin: hot-update S7 plan §5.2 / T13 / INTEG-E canonical storage regression
	audit.AgentInstance, audit.EntryNonce = row.Identity.AgentInstance, row.EntryNonce
	audit.IntentDigest, audit.ObjectVersion = row.Identity.IntentDigest, row.Identity.ObjectVersion
	if kind != "start" {
		audit.TerminalID = terminalID(row.Identity)
		audit.OutcomeUnknown = unknown
	}
	payload, _ := json.Marshal(audit) // fixed fields contain no user-defined marshalers.
	return &x.CanonicalRecord{Identity: row.Identity, Kind: kind, Bytes: payload, Digest: digest(payload), ReqID: digest(append([]byte("xferaudit-record:"), payload...))}
}
func budgetFor(intent x.Intent, size int64, provisional bool) (time.Duration, error) {
	if intent.Verb != "push" && intent.Verb != "pull" || intent.Tier != "a" && intent.Tier != "b" || size < 0 || size > proto.XferMaxBytes || intent.RateLimitBPS < 0 {
		return 0, refusal(x.CodeIntentConflict)
	}
	if provisional && intent.Tier == "b" {
		return proto.XferTierBMaxBudget, nil
	}
	// Every endpoint consumes the same admission formula before durable work.
	// origin: S7 internal review rv-wiring F8
	budget, err := proto.TransferBudget(intent.Tier, size, intent.RateLimitBPS)
	if err != nil {
		return 0, refusal(x.CodeBudgetExceeded)
	}
	return budget, nil
}

// Reserve the bounded row plus simultaneous export/codec copies before any durable admission or Grant side effect. The image owner accounts
// for the actual sealed descriptor; this credit covers the tracker's retained values.
// origin: hot-update S7 plan §8.3 / T13
func (o *owner) reserveCapacity(e *tracked) error {
	id, err := o.cfg.Capacity.Reserve(x.Reservation{NonReplayBytes: 256 << 10, SealedBytes: 128 << 10})
	if err != nil {
		return err
	}
	if id == "" {
		return refusal(x.CodeCapacityExceeded)
	}
	e.reservation = id
	return nil
}
func (o *owner) releaseCapacity() error {
	// Moving this helper must preserve REC's retained credits during Stop,
	// pristine detach and failed Import; they share the live owner's account.
	// origin: hot-update S7 plan §8.3 / INTEG-W reaped-capacity reconciliation
	if err := releaseReaped(o); err != nil {
		return err
	}
	for _, e := range o.entries {
		if e.reservation == "" {
			continue
		}
		if err := o.cfg.Capacity.Release(e.reservation); err != nil {
			return err
		}
		e.reservation = ""
	}
	return nil
}
func mutationBetween(old, row x.LedgerRow) x.LedgerMutationKind {
	if !reflect.DeepEqual(old.SourceSeal, row.SourceSeal) {
		return x.MutationSourceSeal
	}
	if !reflect.DeepEqual(old.Grant, row.Grant) {
		return x.MutationReserve
	}
	if !reflect.DeepEqual(old.ReceiverOutcome, row.ReceiverOutcome) || !reflect.DeepEqual(old.CanonicalTerminal, row.CanonicalTerminal) {
		return x.MutationDecide
	}
	if !reflect.DeepEqual(old.HistoryReceipts, row.HistoryReceipts) {
		return x.MutationHistory
	}
	return x.MutationCleanup
}
