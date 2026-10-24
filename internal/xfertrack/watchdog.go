package xfertrack

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Only the callback host changes in this legacy move; v1 never calls this synthetic-failure watchdog. Its reserved grants use the owner loop below.
// origin: hot-update S7 plan §5.1 / L13
// watchdogBudget is the duration startTransferWatchdog ACTUALLY arms.
//
// origin: batch-c internal review tests-F5. It is a named function so a test can assert the value the
// watchdog uses rather than re-deriving proto.XferBudget(...) and comparing that to itself. The tests that restated the formula stayed green when the real arm
// site was changed to cover one leg instead of two — i.e. they could not see the very defect they were written for.
//
// The budget is derived from the declared size and covers BOTH object-store crossings, because the watchdog is armed before the prepare is even forwarded. e.size
// is 0 on the pull path (proto.PullPrepareReq carries no size — permanent decision N5), which yields the historical flat floor rather than a zero budget.
func watchdogBudget(e *transferEntry) time.Duration {
	return proto.XferBudget(e.tier, e.size, proto.XferPushLegs)
}

// startTransferWatchdog arms a per-tier timeout. If the receiver-side finalization signal does not arrive in time, the watchdog writes a synthetic audit failed +
// reclaims the bucket. The returned cancel is stored on the entry so a successful finalize stops the watchdog.
func (b *legacyWatchdog) startTransferWatchdog(parent context.Context, e *transferEntry) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	d := watchdogBudget(e)
	go func() {
		// defer, not a call after the select, so that EVERY exit releases the child
		// context — including the two early returns below and any added later.
		//
		// origin: prerelease audit broker-transfer/BT-F1. The comment at the
		// finalizeTransfer call below used to say cancelling here is a no-op. That is
		// true of closing ctx.Done() and false of the thing that actually leaks: a
		// child context stays in its PARENT's children map until it is cancelled, and
		// the parent here is b.runCtx, which lives as long as the broker process. So
		// every timed-out transfer left a cancelCtx that nothing could ever collect.
		// cancel is idempotent, so this does not conflict with entry.cancel being
		// called elsewhere on the ordinary paths.
		defer cancel()
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return
		case <-b.cfg.Stopped:
			return
		case <-t.C:
		}
		ent, ok := b.transfers.claimFinalize(e)
		if !ok || ent == nil {
			return
		}
		// batch C: the push arm used to write "agent_no_responders", which is a WRONG ATTRIBUTION —
		// the agent may have been reachable and transferring the whole time; what ran out was the
		// broker's budget. It is also the code the CLI classifies as EX_TEMPFAIL with the hint "the
		// agent isn't reachable on NATS; check it's running and connected", sending an operator to
		// debug connectivity that was never broken. The genuine no-responders emitters (expose,
		// upgrade, the cluster upgrade trigger) keep that code; only this one changes.
		code := proto.CodeTransferBudgetExceeded
		if ent.verb == "pull" {
			code = "ctl_disconnect"
		}
		b.transfers.markReaped(ent.transferID, b.cfg.Now())
		b.finalizeTransfer(ent, LegacyTerminal(b.transfers.Record(ent), LegacyOutcome{
			Kind: "failed", Ts: b.cfg.Now(), DurationMs: msSince(ent.startedAt, b.cfg.Now()),
			Code:  code,
			Error: fmt.Sprintf("broker watchdog: no %s finalization within %s", ent.verb, d),
		}), false) // false: the watchdog owns entry.cancel — see the deferred cancel above
		b.cfg.Logger.Warn("broker: transfer watchdog fired",
			"transfer_id", ent.transferID, "verb", ent.verb, "tier", ent.tier,
			"code", code)
	}()
	return cancel
}
func msSince(t0, t1 time.Time) int64 {
	if t0.IsZero() {
		return 0
	}
	d := t1.Sub(t0)
	if d < 0 {
		d = 0
	}
	return d.Milliseconds()
}

// LegacyWatchdogHooks adapt the former Broker receiver without importing the outer package. Finalize retains the original claim-before-audit ordering.
type LegacyWatchdogHooks struct {
	Now      func() time.Time
	Logger   *slog.Logger
	Finalize func(*LegacyEntry, schema.AuditTransfer, bool)
	// Stopped ties the legacy watchdog to the broker's normal shutdown: the
	// tracker owns the watchdog's own root, so without this arm a stopped broker
	// would still finalize after its runtime is gone. Nil never fires.
	// origin: hot-update S7 plan §5.1 / L13 / P22 request 1
	Stopped <-chan struct{}
}
type legacyWatchdog struct {
	transfers *transferTracker
	cfg       LegacyWatchdogHooks
}

func (b *legacyWatchdog) finalizeTransfer(e *transferEntry, r schema.AuditTransfer, cancel bool) {
	b.cfg.Finalize(e, r, cancel)
}
func (t *transferTracker) StartWatchdog(e *LegacyEntry, hooks LegacyWatchdogHooks) func() {
	// ctx-root: the explicit legacy tracker owner cancels this watchdog.
	return (&legacyWatchdog{transfers: t, cfg: hooks}).startTransferWatchdog(context.Background(), e)
}

// The serializer is also the callback join. Deadline expiry never leaves a detached worker writing a retired backend after Freeze has reported success.
// origin: hot-update S7 plan §8.2 / T13
func (o *owner) take(d x.Deadline) error {
	_, now, err := o.cfg.Clock()
	if err != nil {
		return err
	}
	if d <= 0 || int64(d) <= now {
		return controlbudget.ErrExpired
	}
	timer := time.NewTimer(time.Duration(int64(d) - now))
	defer timer.Stop()
	select {
	case <-o.serial:
		_, now, err = o.cfg.Clock()
		if err != nil || now >= int64(d) {
			o.leave()
			if err != nil {
				return err
			}
			return controlbudget.ErrExpired
		}
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}
func (o *owner) enter(d x.Deadline) error {
	if err := o.take(d); err != nil {
		return err
	}
	if o.sleeping || o.stopped {
		o.leave()
		return refusal(x.CodeOwnerRecovering)
	}
	return nil
}
func (o *owner) leave() { o.serial <- struct{}{} }
func (o *owner) remaining(e *tracked) time.Duration {
	boot, now, err := o.cfg.Clock()
	if err != nil || boot != e.watch.ByteDeadline.BootID {
		return 0
	}
	return time.Duration(int64(e.watch.ByteDeadline.Deadline) - now)
}
func (o *owner) observe(kind x.ObservationKind, e *tracked) {
	if o.cfg.Observe == nil {
		return
	}
	event := x.Observation{Kind: kind, TransferID: e.row.Identity.TransferID, EntryNonce: e.row.EntryNonce,
		ObjectVersion: e.row.Identity.ObjectVersion, Generation: o.state.Generation, Revision: e.row.Revision,
		Phase: e.row.Phase, OriginalDeadline: e.watch.ByteDeadline}
	if e.row.Grant != nil {
		event.GrantNonce = e.row.Grant.GrantNonce
	}
	if e.row.CanonicalTerminal != nil {
		event.CanonicalDigest = e.row.CanonicalTerminal.Digest
	}
	// Only the original receiver's durable outcome can prove its gate closed.
	// A grant or a terminal publication alone supplies no primitive evidence.
	// origin: S7 internal review INTEG-O F5
	if outcome := e.row.ReceiverOutcome; outcome != nil {
		event.Primitive, event.Outcome, event.ParentSynced = outcome.Primitive, outcome.Outcome, outcome.ParentSynced
		event.AdmissionClosed = outcome.Outcome == x.OutcomeNotEntered || outcome.Outcome == x.OutcomeLanded
	}
	o.cfg.Observe(event)
}
func (o *owner) startLoop() {
	o.halt, o.joined = make(chan struct{}), make(chan struct{})
	halt, joined := o.halt, o.joined
	go func() {
		defer close(joined)
		timer := time.NewTicker(500 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-halt:
				return
			case <-timer.C:
				_, now, err := o.cfg.Clock()
				if err != nil {
					continue
				}
				o.watchdogPass(x.Deadline(now + int64(5*time.Second)))
			}
		}
	}()
}
func (o *owner) watchdogPass(d x.Deadline) {
	if err := o.enter(d); err != nil {
		return
	}
	if err := o.census(d); err != nil {
		o.leave()
		return
	}
	var finalize []x.Identity
	o.pruneReaped()
	for k, e := range o.entries {
		_ = o.settleRetired(e, d)
		if e.row.Phase == x.PhaseSettled {
			now := o.cfg.UTC()
			boot, ns, err := o.cfg.Clock()
			retention := e.row.Retention
			if err == nil && boot == retention.Deadline.BootID && ns >= int64(retention.Deadline.Deadline) && !now.Before(retention.CreateUntil) && !now.Before(retention.ResultUntil) {
				if !e.reaped {
					err = o.cfg.Capacity.Release(e.reservation)
				}
				if err == nil {
					delete(o.entries, k)
					if e.reaped {
						o.rememberReaped(e.row.Identity, now, boot, ns, e.reservation)
					}
				}
			}
			continue
		}
		if err := o.flush(e, d); err != nil {
			continue
		}
		if e.row.CanonicalTerminal == nil && (e.watch.Fired || o.remaining(e) <= 0) {
			e.reaped = e.reaped || o.remaining(e) <= 0
			_, _ = o.cancel(e, d)
		}
		// Live protects against the reaper, not the owner's own settlement work.
		if e.row.CanonicalTerminal != nil {
			finalize = append(finalize, e.row.Identity)
		}
		_ = o.confirm(e, d)
	}
	finalizer := o.cfg.Finalizer
	o.leave()
	// Finalizer submits revision messages through Decisions. Holding serial
	// here would deadlock those callbacks; its own Freeze/Stop joins its work.
	if finalizer != nil {
		for _, id := range finalize {
			_, _ = finalizer.Advance(id, d)
		}
	}
}
func (o *owner) census(d x.Deadline) error {
	return reconcileCensus(o, d, false)
}

// Only a sleeping owner may reconcile unsynced proposals. A complete scan
// proves conflict absence, not durability; flush must establish that next.
// origin: S7 internal review rv-recovery F05
func reconcileCensus(o *owner, d x.Deadline, recovering bool) error {
	if o.state.Census.Ready && o.state.Census.Complete {
		return nil
	}
	if o.cfg.Ledger == nil {
		return refusal(x.CodeOwnerRecovering)
	}
	scan, err := o.cfg.Ledger.Scan(d)
	dirtyScan := recovering && o.sleeping && !scan.Ready && scan.Code == x.CodeDurabilityUnknown
	if err != nil && !dirtyScan {
		return err
	}
	if ((!scan.Ready || !scan.Complete) && !dirtyScan) || scan.Origin != (x.Origin{}) && scan.Origin != o.cfg.Origin {
		return refusal(x.CodeOwnerRecovering)
	}
	for _, row := range scan.Rows {
		if row.Origin != o.cfg.Origin {
			return refusal(x.CodeTerminalConflict)
		}
		if err = validateRow(row); err != nil {
			return err
		}
		if e := o.entries[key(row.Identity)]; e != nil {
			if e.row.Identity != row.Identity {
				return refusal(x.CodeTerminalConflict)
			}
			if e.row.Revision == row.Revision {
				if !reflect.DeepEqual(cloneRow(e.row), cloneRow(row)) {
					return refusal(x.CodeTerminalConflict)
				}
				e.dirty = !scan.Ready
			} else if !e.dirty || e.row.Revision != row.Revision+1 {
				return refusal(x.CodeTerminalConflict)
			} else {
				e.kind = mutationBetween(row, e.row)
			}
		} else {
			if err = admitAfterReaped(o, row.Identity); err != nil {
				return err
			}
			e := &tracked{row: cloneRow(row), watch: o.rowDeadline(row), dirty: !scan.Ready, kind: mutationFor(row)}
			if err = o.reserveCapacity(e); err != nil {
				return err
			}
			o.entries[key(row.Identity)] = e
		}
	}
	for _, e := range o.entries {
		if e.row.Phase == x.PhaseSettled {
			present := false
			for _, row := range scan.Rows {
				if key(row.Identity) == key(e.row.Identity) {
					present = true
				}
			}
			if !present {
				e.dirty = false
			}
		}
	}
	o.state.Census = scan
	return nil
}
func (o *owner) rowDeadline(row x.LedgerRow) x.WatchdogDeadline {
	boot, ns, err := o.cfg.Clock()
	remaining := time.Duration(row.BudgetSeal.EffectiveBudgetNS) - o.cfg.UTC().Sub(row.StartedAt)
	if err != nil || boot != row.Origin.BootID {
		remaining = 0
	}
	return x.WatchdogDeadline{Identity: row.Identity, ByteDeadline: x.BootDeadline{BootID: boot, Deadline: x.Deadline(ns + int64(max(0, remaining)))}}
}
func validateRow(row x.LedgerRow) error {
	encoded, err := json.Marshal(row)
	if err != nil || len(encoded) > 32<<10 {
		return refusal(x.CodeCapacityExceeded)
	}
	if !validIdentity(row.Identity) || row.EntryNonce != row.Identity.EntryNonce || row.Revision == 0 || row.StartedAt.IsZero() ||
		row.BudgetSeal.StartedAt != row.StartedAt || row.BudgetSeal.EffectiveBudgetNS <= 0 || row.BudgetSeal.EffectiveBudgetNS > int64(proto.XferTierBMaxBudget) {
		return refusal(x.CodeTerminalConflict)
	}
	if row.Grant != nil && (row.Grant.Identity != row.Identity || row.Grant.GrantNonce == "" || row.Grant.ReceiverInstance == "" || row.Grant.TerminalID != terminalID(row.Identity)) {
		return refusal(x.CodeTerminalConflict)
	}
	if row.CanonicalStart == nil || row.CanonicalStart.Kind != "start" {
		return refusal(x.CodeTerminalConflict)
	}
	for _, r := range []*x.CanonicalRecord{row.CanonicalStart, row.CanonicalTerminal} {
		if r != nil && (r.Identity != row.Identity || digest(r.Bytes) != r.Digest || digest(append([]byte("xferaudit-record:"), r.Bytes...)) != r.ReqID) {
			return refusal(x.CodeTerminalConflict)
		}
	}
	if row.CanonicalTerminal != nil && (row.ReceiverOutcome == nil || row.CanonicalTerminal.Kind != "complete" && row.CanonicalTerminal.Kind != "failed") {
		return refusal(x.CodeTerminalConflict)
	}
	return nil
}
func (o *owner) Status(r x.StatusRequest, d x.Deadline) (x.Status, error) {
	if err := o.take(d); err != nil {
		return x.Status{}, err
	}
	defer o.leave()
	// Sleeping entries already protect resources before the current store opens.
	if e := o.entries[key(r.Identity)]; e != nil {
		if e.row.Identity != r.Identity || r.Origin != e.row.Origin {
			return x.Status{}, refusal(x.CodeIntentConflict)
		}
		return o.status(e), nil
	}
	if o.sleeping || o.stopped || r.Origin != o.cfg.Origin {
		return x.Status{}, refusal(x.CodeOwnerRecovering)
	}
	if err := o.census(d); err != nil {
		return x.Status{}, err
	}
	if e := o.entries[key(r.Identity)]; e != nil {
		if e.row.Identity != r.Identity {
			return x.Status{}, refusal(x.CodeIntentConflict)
		}
		return o.status(e), nil
	}
	for _, reaped := range o.state.Reaped {
		if reaped.Identity == r.Identity {
			// The settled row aged out, but this id was ended by its watchdog
			// within retention: "unknown" here would read as never-created.
			return x.Status{State: x.StatusUnknown, Identity: r.Identity, Origin: r.Origin, Code: x.CodeResultUnknown,
				Error: "transfer was ended by its byte-budget watchdog; its result retention elapsed"}, nil
		}
	}
	return x.Status{State: x.StatusUnknown, Identity: r.Identity, Origin: r.Origin}, nil
}

// rememberReaped is the production producer of TrackerState.Reaped: the
// tracker form of the legacy recently-reaped set (batch C). A watchdog-ended
// row leaves the live table only after its own result retention, and its
// identity is then remembered for one more xferRecentlyReapedTTL, so a late
// status or receiver report is answered as reaped instead of never-created.
// The set is bounded together with the live table (image ceiling).
// origin: hot-update S7 plan §5 / §8.3 / T20 — S7 production activation
func (o *owner) rememberReaped(id x.Identity, now time.Time, boot string, ns int64, reservation x.ReservationID) {
	if o.reapedReservations == nil {
		o.reapedReservations = make(map[x.Key]x.ReservationID)
	}
	// Keep the original slot's credit until the memory itself expires. Releasing
	// it first creates an unaccounted retained identity at the capacity boundary.
	// origin: S7 internal review rv-gc F4
	o.reapedReservations[key(id)] = reservation
	o.state.Reaped = append(o.state.Reaped, x.ReapedEntry{Identity: id, At: now.UTC(), Retention: x.RetentionState{
		Identity: id, CreateUntil: now.UTC(), ResultUntil: now.UTC().Add(xferRecentlyReapedTTL),
		Deadline: x.BootDeadline{BootID: boot, Deadline: x.Deadline(ns + int64(xferRecentlyReapedTTL))}}})
	o.pruneReaped()
}

// pruneReaped drops only expired memories. Unknown clocks and release failures
// retain protection; capacity pressure cannot evict a still-valid identity.
// The earlier rule was: "pruneReaped drops expired or foreign-boot memories,
// then the oldest ones beyond the shared live+reaped ceiling." That eviction
// could discard still-valid dedupe evidence, so admission now enforces the cap.
// origin: S7 internal review rv-gc F4
func (o *owner) pruneReaped() {
	boot, ns, err := o.cfg.Clock()
	now := o.cfg.UTC()
	kept := o.state.Reaped[:0]
	for _, reaped := range o.state.Reaped {
		r := reaped.Retention
		if err == nil && r.Deadline.BootID == boot && ns >= int64(r.Deadline.Deadline) && !now.Before(r.CreateUntil) && !now.Before(r.ResultUntil) {
			k := key(reaped.Identity)
			if reservation := o.reapedReservations[k]; reservation != "" && o.cfg.Capacity.Release(reservation) == nil {
				delete(o.reapedReservations, k)
				continue
			}
		}
		kept = append(kept, reaped)
	}
	o.state.Reaped = kept
}

// Admission and census share the retained-key fence, including a changed
// nonce at the same business key. Neither can recreate an unexpired result.
func admitAfterReaped(o *owner, id x.Identity) error {
	o.pruneReaped()
	for _, reaped := range o.state.Reaped {
		if key(reaped.Identity) == key(id) {
			if reaped.Identity != id {
				return refusal(x.CodeIntentConflict)
			}
			return refusal(x.CodeResultUnknown)
		}
	}
	if len(o.entries)+len(o.state.Reaped) >= transferTrackerMaxEntries {
		return refusal(x.CodeCapacityExceeded)
	}
	return nil
}

func releaseReaped(o *owner) error {
	for k, reservation := range o.reapedReservations {
		if err := o.cfg.Capacity.Release(reservation); err != nil {
			return err
		}
		delete(o.reapedReservations, k)
	}
	return nil
}

// Imported retention retains its original deadlines. Validate before taking
// credit, and unwind all acquired credit if any later closure check rejects.
// origin: S7 internal review rv-gc F4
func importReaped(o *owner) error {
	o.reapedReservations = make(map[x.Key]x.ReservationID)
	for _, reaped := range o.state.Reaped {
		k, r := key(reaped.Identity), reaped.Retention
		if o.reapedReservations[k] != "" || o.entries[k] != nil || !validIdentity(reaped.Identity) || reaped.At.IsZero() ||
			r.Identity != reaped.Identity || r.CreateUntil.IsZero() || r.ResultUntil.Before(r.CreateUntil) || r.Deadline.BootID != o.state.Origin.BootID || r.Deadline.Deadline <= 0 {
			return refusal(x.CodeTerminalConflict)
		}
		// Identities and both image copies are bounded by validIdentity's token
		// limits; reaped memories retain neither payload nor canonical records.
		reservation, err := o.cfg.Capacity.Reserve(x.Reservation{NonReplayBytes: 8 << 10, SealedBytes: 8 << 10})
		if err != nil {
			return err
		}
		if reservation == "" {
			return refusal(x.CodeCapacityExceeded)
		}
		o.reapedReservations[k] = reservation
	}
	return nil
}

type frozen struct{ owner *owner }

func (o *owner) Freeze(d x.Deadline) (Frozen, error) {
	if err := o.take(d); err != nil {
		return nil, err
	}
	if o.stopped {
		o.leave()
		return nil, refusal(x.CodeOwnerRecovering)
	}
	if !o.sleeping {
		o.sleeping = true
		close(o.halt)
	}
	joined := o.joined
	o.leave()
	if joined != nil {
		if err := o.wait(joined, d); err != nil {
			return nil, err
		}
	}
	return &frozen{owner: o}, nil
}
func (o *owner) wait(done <-chan struct{}, d x.Deadline) error {
	_, ns, err := o.cfg.Clock()
	if err != nil {
		return err
	}
	timer := time.NewTimer(max(0, time.Duration(int64(d)-ns)))
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}
func (f *frozen) Export() (x.TrackerState, []handoff.Resource, error) {
	o := f.owner
	select {
	case <-o.serial:
	default:
		return x.TrackerState{}, nil, refusal(x.CodeOwnerRecovering)
	}
	defer o.leave()
	if !o.sleeping || o.stopped {
		return x.TrackerState{}, nil, refusal(x.CodeOwnerRecovering)
	}
	if o.joined != nil {
		select {
		case <-o.joined:
		default:
			return x.TrackerState{}, nil, refusal(x.CodeOwnerRecovering)
		}
	}
	o.pruneReaped()
	state := o.state
	state.Admission = x.AdmissionState{Closed: true, Generation: state.Generation}
	state.ProvisioningFence = state.Admission
	state.Entries = nil
	state.GrantSlots = nil
	state.WatchdogDeadlines = nil
	state.PendingStart = nil
	state.PendingTerminal = nil
	state.Cleanup = nil
	for _, e := range o.entries {
		state.Entries = append(state.Entries, cloneRow(e.row))
		// The high-water mark includes a retained dirty proposal; its census
		// stays unready, so this does not mint a durable acknowledgement.
		// origin: S7 internal review rv-recovery F02 / F05
		state.LedgerRevision = max(state.LedgerRevision, e.row.Revision)
		state.WatchdogDeadlines = append(state.WatchdogDeadlines, e.watch)
		if e.row.Grant != nil {
			state.GrantSlots = append(state.GrantSlots, x.GrantSlot{Identity: e.row.Identity, Grant: *e.row.Grant, Revision: e.row.Revision})
		}
		if e.row.StartDelivery != x.DeliveryHistoryConfirmed {
			state.PendingStart = append(state.PendingStart, *cloneRecord(e.row.CanonicalStart))
		}
		if e.row.CanonicalTerminal != nil && e.row.TerminalDelivery != x.DeliveryHistoryConfirmed {
			state.PendingTerminal = append(state.PendingTerminal, *cloneRecord(e.row.CanonicalTerminal))
		}
		state.Cleanup = append(state.Cleanup, e.row.Cleanup)
		o.observe(x.ObserveFreeze, e)
	}
	sort.Slice(state.Entries, func(i, j int) bool {
		a, b := state.Entries[i].Identity, state.Entries[j].Identity
		if a.SID != b.SID {
			return a.SID < b.SID
		}
		return a.TransferID < b.TransferID
	})
	// A live registry has advanced beyond the startup disk census. Carry its
	// current rows as protection, but do not export the old census as ready:
	// the next Bind must reconcile primary and outbox against this projection.
	// origin: hot-update S7 plan §5.3 / §8.2 / T36
	state.Census = x.LedgerCensus{Origin: state.Origin, Rows: state.Entries}
	for _, row := range state.Entries {
		state.Census.Entries = append(state.Census.Entries, x.Protection{Identity: row.Identity, Phase: row.Phase, ObjectVersion: row.Identity.ObjectVersion, CreationUntil: row.Permit.CreateUntil, WriterRetired: row.Cleanup.WriterFence.WriterRetired, OwnerGeneration: state.Generation})
	}
	// Image encoding/sealed resources belong to P18/P24; this leaf exports only
	// its actual private closure, with no borrowed descriptors of its own.
	payload, err := json.Marshal(state)
	if err != nil {
		return x.TrackerState{}, nil, err
	}
	var copy x.TrackerState
	if err = json.Unmarshal(payload, &copy); err != nil {
		return x.TrackerState{}, nil, err
	}
	return copy, nil, nil
}
func (f *frozen) DetachPristine() (x.Salvage, error) {
	o := f.owner
	select {
	case <-o.serial:
	default:
		return x.Salvage{}, refusal(x.CodeOwnerRecovering)
	}
	defer o.leave()
	if !o.pristine || !o.sleeping || o.stopped {
		return x.Salvage{}, refusal(x.CodeOwnerRecovering)
	}
	if err := o.releaseCapacity(); err != nil {
		return x.Salvage{}, err
	}
	o.stopped = true
	return x.Salvage{}, nil
}
func Import(cfg Config, state x.TrackerState) (Manager, error) {
	if cfg.Clock == nil || cfg.UTC == nil || cfg.Capacity == nil || state.Schema != x.StateSchema || state.Origin != cfg.Origin ||
		!state.Admission.Closed || state.Admission.Inflight != 0 || !state.ProvisioningFence.Closed || state.ProvisioningFence.Inflight != 0 || len(state.Entries)+len(state.Reaped) > transferTrackerMaxEntries {
		return nil, refusal(x.CodeOwnerRecovering)
	}
	boot, _, clockErr := cfg.Clock()
	if clockErr != nil || boot != state.Origin.BootID {
		return nil, refusal(x.CodeOwnerRecovering)
	}
	o := newOwner(cfg)
	accepted := false
	defer func() {
		if !accepted {
			_ = o.releaseCapacity()
		}
	}()
	payload, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(payload, &o.state); err != nil {
		return nil, err
	}
	o.sleeping, o.pristine = true, true
	watches := make(map[x.Identity]x.WatchdogDeadline)
	for _, w := range state.WatchdogDeadlines {
		if _, ok := watches[w.Identity]; ok {
			return nil, refusal(x.CodeTerminalConflict)
		}
		watches[w.Identity] = w
	}
	for _, row := range o.state.Entries {
		if err = validateRow(row); err != nil {
			return nil, err
		}
		if _, ok := o.entries[key(row.Identity)]; ok {
			return nil, refusal(x.CodeTerminalConflict)
		}
		watch, ok := watches[row.Identity]
		if !ok || watch.ByteDeadline.BootID != cfg.Origin.BootID || watch.ByteDeadline.Deadline <= 0 {
			return nil, refusal(x.CodeTerminalConflict)
		}
		e := &tracked{row: row, watch: watch, dirty: true, kind: mutationFor(row)}
		if err = o.reserveCapacity(e); err != nil {
			return nil, err
		}
		o.entries[key(row.Identity)] = e
	}
	if len(watches) != len(o.entries) {
		return nil, refusal(x.CodeTerminalConflict)
	}
	if err = validateClosure(o.state, o.entries); err != nil {
		return nil, err
	}
	if err = importReaped(o); err != nil {
		return nil, err
	}
	o.state.Census.Ready = false
	accepted = true
	return o, nil
}
func mutationFor(row x.LedgerRow) x.LedgerMutationKind {
	if row.Phase == x.PhaseSettled || row.Phase == x.PhaseCleanupPending {
		return x.MutationCleanup
	}
	if len(row.HistoryReceipts) > 0 {
		return x.MutationHistory
	}
	if row.ReceiverOutcome != nil {
		return x.MutationDecide
	}
	if row.Grant != nil {
		return x.MutationReserve
	}
	if row.SourceSeal != nil {
		return x.MutationSourceSeal
	}
	return x.MutationAdmit
}
func (o *owner) Bind(b Bindings, d x.Deadline) error {
	if err := o.take(d); err != nil {
		return err
	}
	defer o.leave()
	if !o.sleeping || o.stopped || b.Ledger == nil || b.OwnerGeneration < o.state.Generation {
		return refusal(x.CodeOwnerRecovering)
	}
	if o.joined != nil {
		select {
		case <-o.joined:
		default:
			return refusal(x.CodeOwnerRecovering)
		}
	}
	o.cfg.Ledger = b.Ledger
	o.state.Census.Ready = false
	if err := reconcileCensus(o, d, true); err != nil {
		return err
	}
	for _, e := range o.entries {
		if err := o.flush(e, d); err != nil {
			return err
		}
	}
	// Do not activate on the earlier conflict-only census. Every retained
	// proposal must now appear in a fresh, fully durable readback.
	o.state.Census.Ready = false
	if err := o.census(d); err != nil {
		return err
	}
	o.state.Generation = b.OwnerGeneration
	o.sleeping, o.pristine = false, false
	o.startLoop()
	return nil
}
func (o *owner) Stop(d x.Deadline) error {
	if err := o.take(d); err != nil {
		return err
	}
	if !o.sleeping && !o.stopped {
		close(o.halt)
	}
	o.sleeping, o.stopped = true, true
	joined := o.joined
	o.leave()
	if joined != nil {
		if err := o.wait(joined, d); err != nil {
			return err
		}
	}
	if err := o.take(d); err != nil {
		return err
	}
	defer o.leave()
	return o.releaseCapacity()
}

// LegacyWatchdogBudget exposes the actual legacy timer arm calculation.
func LegacyWatchdogBudget(e *LegacyEntry) time.Duration { return watchdogBudget(e) }
