package xferfinal

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

var _ Factory = New
var _ Finalizer = (*owner)(nil)

// The tracker owns every durable decision. This owner holds only admission and
// join state: a planned cut must not leave an asynchronous cleanup callback
// writing through an old backend after its tracker has frozen.
// origin: hot-update S7 plan §8.2 / T21
type owner struct {
	cfg      Config
	bootID   string
	born     time.Time
	gate     chan struct{}
	closed   chan struct{}
	mu       sync.Mutex
	active   chan struct{}
	stopped  bool
	eventSeq uint64
}

// New builds a sleeping finalizer. Advance is explicitly driven by the current
// tracker/reaper; construction neither scans disk nor starts a background task.
func New(cfg Config) (Finalizer, error) {
	if cfg.Clock == nil || cfg.Decisions == nil || cfg.Ledger == nil || cfg.History == nil ||
		cfg.Objects == nil || cfg.ResolveReceiver == nil {
		return nil, fmt.Errorf("%s: finalizer ports are incomplete", xfercontract.CodeOwnerRecovering)
	}
	boot, now, err := cfg.Clock()
	if err != nil {
		return nil, err
	}
	if boot == "" || now <= 0 {
		return nil, fmt.Errorf("%s: finalizer clock identity is unavailable", xfercontract.CodeOwnerRecovering)
	}
	o := &owner{cfg: cfg, bootID: boot, gate: make(chan struct{}, 1), closed: make(chan struct{})}
	if cfg.UTC != nil {
		o.born = cfg.UTC()
	}
	o.gate <- struct{}{}
	return o, nil
}

// Advance never decides a receiver outcome or manufactures a terminal. Even
// already-confirmed single-mode rows must traverse cleanup and durable consume.
// origin: hot-update S7 plan §5.1–§5.2 / T21
func (o *owner) Advance(id xfercontract.Identity, deadline controlbudget.Deadline) (xfercontract.Status, error) {
	status := xfercontract.Status{State: xfercontract.StatusLive, Identity: id, CleanupPending: true}
	if err := o.enter(deadline); err != nil {
		return finalFailure(status, xfercontract.CodeOwnerRecovering, err)
	}
	defer o.leave()
	key := xfercontract.Key{SID: id.SID, TransferID: id.TransferID}
	row, err := o.read(key, id, deadline)
	if err != nil {
		return finalFailure(status, xfercontract.CodeResultUnknown, err)
	}
	status = rowStatus(row)
	// A settled row was already retired from both ledger directories. A
	// second Advance (eager settlement racing the watchdog pass) must not
	// re-run cleanup and re-propose a revision the ledger no longer holds.
	// origin: hot-update S7 plan §5.2 / T21 — S7 production activation
	if row.Phase == xfercontract.PhaseSettled {
		status.Phase, status.CleanupPending = xfercontract.PhaseSettled, false
		return status, nil
	}
	if row.CanonicalStart == nil {
		return finalFailure(status, xfercontract.CodeResultUnknown, errors.New("canonical START is unavailable"))
	}
	if err = validateRecords(row); err != nil {
		return finalFailure(status, xfercontract.CodeTerminalConflict, err)
	}
	// START publication remains a live responsibility even before the receiver
	// decides. No byte deadline or result TTL can authorize dropping this row.
	row, err = o.confirm(row, *row.CanonicalStart, deadline)
	if err != nil {
		return finalFailure(status, xfercontract.CodeResultUnknown, err)
	}
	if row.CanonicalTerminal == nil {
		return rowStatus(row), nil
	}
	// Record the durable outcome while its exact history publication is still
	// pending, before Confirm can synchronously acknowledge and retire it.
	// origin: S7 internal review INTEG-O F5
	if _, confirmed := receiptFor(row, row.CanonicalTerminal); !confirmed {
		observePending(o, row, xfercontract.PhaseTerminalPending, 0)
	}
	row, err = o.confirm(row, *row.CanonicalTerminal, deadline)
	if err != nil {
		return finalFailure(rowStatus(row), xfercontract.CodeResultUnknown, err)
	}
	status = rowStatus(row)
	if status.Terminal == nil {
		return finalFailure(status, xfercontract.CodeTerminalConflict, errors.New("canonical terminal cannot be decoded"))
	}
	// Cleanup has its own boundary after exact history ACK and before any
	// receiver/object/ledger retirement. It cannot be inferred from complete.
	// origin: S7 internal review INTEG-O F5
	observePending(o, row, xfercontract.PhaseCleanupPending, status.Terminal.HistorySeq)
	proof, err := o.cleanup(row, deadline)
	if err != nil {
		return finalFailure(status, xfercontract.CodeResultUnknown, err)
	}
	// AcceptCleanup is a revision message, not permission to erase credentials.
	// Re-read its durable revision before constructing the consume proof, so a
	// delayed receipt can never retire a replacement entry sharing the same ID.
	if err = o.check(deadline); err != nil {
		return finalFailure(status, xfercontract.CodeOwnerRecovering, err)
	}
	if err = o.cfg.Decisions.AcceptCleanup(key, row.EntryNonce, row.Revision, proof.Cleanup, deadline); err != nil {
		return finalFailure(status, xfercontract.CodeResultUnknown, err)
	}
	current, err := o.read(key, id, deadline)
	if err != nil {
		return finalFailure(status, xfercontract.CodeResultUnknown, err)
	}
	if err = sameDecision(row, current); err != nil {
		return finalFailure(status, xfercontract.CodeTerminalConflict, err)
	}
	if current.Cleanup.TmpUnlinkPending || current.Cleanup.DirectorySyncPending || current.Cleanup.ObjectPurgePending ||
		!current.Cleanup.WriterFence.WriterRetired || !current.Cleanup.WriterFence.ConsumersRetired ||
		(current.Identity.ObjectVersion != "" && (!current.Cleanup.WriterFence.Purged ||
			current.Cleanup.WriterFence.Receipt == nil || current.Cleanup.WriterFence.Receipt.Sequence != proof.Cleanup.FenceSequence)) {
		return finalFailure(status, xfercontract.CodeResultUnknown, errors.New("cleanup acknowledgement is not durable"))
	}
	proof.Revision = current.Revision
	if err = o.check(deadline); err != nil {
		return finalFailure(status, xfercontract.CodeOwnerRecovering, err)
	}
	// The store owns both directories and must durably unlink primary FIRST.
	// A primary failure preserves the outbox terminal as a synthesis barrier.
	// origin: hot-update S7 plan §5.1 / T21
	if err = o.cfg.Ledger.Retire(key, current.EntryNonce, proof, deadline); err != nil {
		return finalFailure(status, xfercontract.CodeResultUnknown, err)
	}
	// Consume must converge the current tracker's retained result before this
	// Advance acknowledges settlement. Otherwise an immediate status or image
	// cut keeps cleanup_pending after both durable rows have disappeared.
	// origin: S7 internal review rv-recovery F08 / hot-update S7 plan §8.2
	if decisions, ok := o.cfg.Decisions.(interface {
		ReconcileRetirement(xfercontract.Key, string, uint64, controlbudget.Deadline) error
	}); ok {
		if err = decisions.ReconcileRetirement(key, current.EntryNonce, current.Revision, deadline); err != nil {
			return finalFailure(status, xfercontract.CodeResultUnknown, err)
		}
	}
	current.Phase = xfercontract.PhaseSettled
	o.observe(xfercontract.ObserveRetirement, current, proof.TerminalReceipt.Sequence, proof.Cleanup.Digest)
	status.Phase, status.CleanupPending = xfercontract.PhaseSettled, false
	return status, nil
}

// Stop closes future admission and joins the actual in-flight Advance. A
// timeout retains this owner and its ports; it cannot be treated as a cut ACK.
// origin: hot-update S7 plan §8.2 / T21
func (o *owner) Stop(deadline controlbudget.Deadline) error {
	if err := o.check(deadline); err != nil {
		return err
	}
	o.mu.Lock()
	if !o.stopped {
		o.stopped = true
		close(o.closed)
	}
	active := o.active
	o.mu.Unlock()
	if active == nil {
		return nil
	}
	remaining, err := o.remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-active:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

func (o *owner) remaining(deadline controlbudget.Deadline) (time.Duration, error) {
	if deadline <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	boot, now, err := o.cfg.Clock()
	if err != nil {
		return 0, err
	}
	if boot != o.bootID || now <= 0 {
		return 0, fmt.Errorf("%s: finalizer clock identity changed", xfercontract.CodeOwnerRecovering)
	}
	if now >= int64(deadline) {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

func (o *owner) check(deadline controlbudget.Deadline) error {
	_, err := o.remaining(deadline)
	return err
}

func (o *owner) enter(deadline controlbudget.Deadline) error {
	remaining, err := o.remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-o.closed:
		return fmt.Errorf("%s: finalizer is stopped", xfercontract.CodeOwnerRecovering)
	case <-timer.C:
		return controlbudget.ErrExpired
	case <-o.gate:
	}
	if err = o.check(deadline); err != nil {
		o.gate <- struct{}{}
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.stopped {
		o.gate <- struct{}{}
		return fmt.Errorf("%s: finalizer is stopped", xfercontract.CodeOwnerRecovering)
	}
	o.active = make(chan struct{})
	return nil
}

func (o *owner) leave() {
	o.mu.Lock()
	close(o.active)
	o.active = nil
	o.mu.Unlock()
	o.gate <- struct{}{}
}

func (o *owner) read(key xfercontract.Key, id xfercontract.Identity, deadline controlbudget.Deadline) (xfercontract.LedgerRow, error) {
	if err := o.check(deadline); err != nil {
		return xfercontract.LedgerRow{}, err
	}
	row, err := o.cfg.Decisions.Read(key, deadline)
	if err != nil {
		return row, err
	}
	if id.SID == "" || id.TransferID == "" || id.EntryNonce == "" || row.Identity != id ||
		row.EntryNonce != id.EntryNonce || row.Revision == 0 || row.Origin.Generation == 0 {
		return row, fmt.Errorf("%s: finalizer entry identity changed", xfercontract.CodeIntentConflict)
	}
	return row, nil
}

func finalFailure(status xfercontract.Status, code string, err error) (xfercontract.Status, error) {
	var fault settlementError
	if errors.As(err, &fault) {
		code = fault.code
	}
	status.Code = code
	// Backend errors remain wrapped for errors.Is, but status never echoes a
	// transport/capsule error whose detail could contain authentication material.
	status.Error = "transfer settlement remains pending"
	return status, fmt.Errorf("%s: %w", code, err)
}

func (o *owner) observe(kind xfercontract.ObservationKind, row xfercontract.LedgerRow, seq uint64, digest string) {
	if o.cfg.Observe == nil {
		return
	}
	boot, now, err := o.cfg.Clock()
	if err != nil {
		return
	}
	o.eventSeq++
	event := xfercontract.Observation{Kind: kind, BootID: boot, BoottimeNS: now,
		TransferID: row.Identity.TransferID, EntryNonce: row.EntryNonce, ObjectVersion: row.Identity.ObjectVersion,
		Generation: row.Origin.Generation, Phase: row.Phase, Revision: row.Revision,
		HistorySeq: seq, RetirementDigest: digest, EventSeq: o.eventSeq}
	if view, ok := o.cfg.Decisions.(interface {
		TransferObservation(xfercontract.Identity) (xfercontract.Observation, bool)
	}); ok {
		if original, available := view.TransferObservation(row.Identity); available {
			event.OriginalDeadline, event.Generation = original.OriginalDeadline, original.Generation
		}
	}
	if row.ReceiverOutcome != nil {
		event.Outcome, event.Primitive, event.ParentSynced = row.ReceiverOutcome.Outcome, row.ReceiverOutcome.Primitive, row.ReceiverOutcome.ParentSynced
		event.AdmissionClosed = event.Outcome == xfercontract.OutcomeNotEntered || event.Outcome == xfercontract.OutcomeLanded
	}
	if row.CanonicalTerminal != nil {
		event.CanonicalDigest = row.CanonicalTerminal.Digest
	}
	if kind == xfercontract.ObserveHistory {
		for _, receipt := range row.HistoryReceipts {
			if receipt.Sequence == seq {
				event.CanonicalDigest = receipt.RecordDigest
			}
		}
	}
	o.cfg.Observe(event)
}

// Extra boundary receipts belong only to authenticated private drill custody;
// ordinary observer callbacks retain their existing publication vocabulary.
// origin: S7 internal review INTEG-O F5
func observePending(o *owner, row xfercontract.LedgerRow, phase xfercontract.Phase, seq uint64) {
	if drillobserve.Hook == nil {
		return
	}
	row.Phase = phase // Local observation copy; the durable decision is unchanged.
	o.observe(xfercontract.ObserveFinish, row, seq, "")
}
