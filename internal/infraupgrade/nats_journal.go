package infraupgrade

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Adding NATS recovery operands changes the strict private sidecar codec.
// origin: S9 internal review rv-infra F08
const transactionStateVersion uint16 = 2

// NATSJournal is the trusted transaction owner's private recovery port. Like
// CaddyJournal it supplements the frozen Transaction interface without changing
// public status or the image ABI. Store must commit under the acquired domain
// before returning; an in-memory driver cache cannot implement this in production.
// origin: S9 internal review rv-infra F08
type NATSJournal interface {
	LoadNATS(c.Plan, c.Deadline) (NATSTransition, error)
	StoreNATS(c.Plan, NATSTransition, c.Deadline) error
}

// NATSTransition retains original operands, including a sent request whose reply
// was lost. Deadline is the original component deadline; the separate transient
// action deadline bounds each recovery attempt. TimedOut cannot be erased
// by later retirement. Route deadlines and nonces are never refreshed on restore.
// This private sidecar contains references/proofs, never credential secret bytes.
type NATSTransition struct {
	Version       uint16
	Plan          c.Plan
	Status        c.Status
	Deadline      c.Deadline
	TimedOut      bool
	Ready         c.Readiness
	Census        c.Census
	Cohort        c.Checkpoint
	Recovery      c.Checkpoint
	Transfers     c.TransferSeal
	Outbound      NATSMigration
	Inbound       NATSMigration
	Journal       c.Digest
	Sequence      uint64
	Pending       *c.HostRequest
	PhasePending  *TransactionChange
	Cut           bool
	Fenced        bool
	Promoted      bool
	LDMApplied    bool
	StopApplied   bool
	StartReleased bool
	OldExited     bool
	ACKVerified   bool
	Rebound       bool
	Verified      bool
	Returned      bool
	CohortHeld    bool
	RecoveryHeld  bool
	TransfersHeld bool
}

// NATSMigration preserves the exact prepare and observed move, not a recipe for
// reconnecting again. Even a partial Install shares the first absolute deadline.
type NATSMigration struct {
	Deadline     c.Deadline
	MoveDeadline c.Deadline
	Proofs       []c.RouteProof
	Installed    int
	Moved        *c.MoveResult
	Bound        []c.RouteProof
	Done         bool
}

func natsExportMigration(m natsMigration) NATSMigration {
	return NATSMigration{m.deadline, m.moveDeadline, m.proofs, m.installed, m.moved, m.bound, m.done}
}
func natsImportMigration(m NATSMigration) natsMigration {
	return natsMigration{m.Deadline, m.MoveDeadline, m.Proofs, m.Installed, m.Moved, m.Bound, m.Done}
}

func natsSnapshot(r *natsRun) NATSTransition {
	return NATSTransition{Version: 1, Plan: r.plan, Status: r.status, Deadline: r.deadline, TimedOut: r.timedOut,
		Ready: r.ready, Census: r.census, Cohort: r.cohort, Recovery: r.recovery, Transfers: r.transfers,
		Outbound: natsExportMigration(r.outbound), Inbound: natsExportMigration(r.inbound), Journal: r.journal,
		Sequence: r.sequence, Pending: r.pending, PhasePending: r.phasePending, Cut: r.cut, Fenced: r.fenced,
		Promoted: r.promoted, LDMApplied: r.ldmApplied, StopApplied: r.stopApplied, StartReleased: r.startReleased,
		OldExited: r.oldExited, ACKVerified: r.ackVerified, Rebound: r.rebound, Verified: r.verified, Returned: r.returned, CohortHeld: r.cohortHeld,
		RecoveryHeld: r.recoveryHeld, TransfersHeld: r.transfersHeld}
}
func natsRestore(x NATSTransition) *natsRun {
	return &natsRun{plan: x.Plan, status: x.Status, deadline: x.Deadline, timedOut: x.TimedOut,
		ready: x.Ready, census: x.Census, cohort: x.Cohort, recovery: x.Recovery, transfers: x.Transfers,
		outbound: natsImportMigration(x.Outbound), inbound: natsImportMigration(x.Inbound), journal: x.Journal,
		sequence: x.Sequence, pending: x.Pending, phasePending: x.PhasePending, cut: x.Cut, fenced: x.Fenced,
		promoted: x.Promoted, ldmApplied: x.LDMApplied, stopApplied: x.StopApplied, startReleased: x.StartReleased,
		oldExited: x.OldExited, ackVerified: x.ACKVerified, rebound: x.Rebound, verified: x.Verified, returned: x.Returned, cohortHeld: x.CohortHeld,
		recoveryHeld: x.RecoveryHeld, transfersHeld: x.TransfersHeld}
}

func natsTransitionValid(x NATSTransition, p c.Plan) bool {
	if x.Version != 1 || x.Plan != p || x.Deadline <= 0 || !caddyStatus(x.Status, p) {
		return false
	}
	if x.Cut && (!natsCheckpointComplete(x.Cohort) || !natsCheckpointComplete(x.Recovery) ||
		!natsTransferComplete(x.Transfers) || x.Cohort.Transfers != x.Transfers || x.Recovery.Transfers != x.Transfers) {
		return false
	}
	if (x.Promoted && !x.Fenced) || (x.StartReleased && !x.OldExited) || (x.Verified && (!x.StartReleased || !x.ACKVerified || !x.Rebound)) ||
		(x.Returned && !x.Verified) || (x.Sequence == 0 && x.Pending != nil) {
		return false
	}
	if x.Pending != nil {
		r := x.Pending
		if r.Version != c.RequestVersion || r.Tx != p.Tx || r.Domain != p.Domain || r.Step != p.Step ||
			r.PolicyRevision != p.PolicyRevision || r.Component != c.NATS || r.Expected != p.Before ||
			r.HostBootID != p.Before.BootID || r.Sequence != x.Sequence || r.ExpectedJournal != x.Journal ||
			r.Nonce == (c.ID{}) || r.RemainingNS == 0 {
			return false
		}
	}
	for _, m := range []NATSMigration{x.Outbound, x.Inbound} {
		if m.Installed < 0 || m.Installed > len(m.Proofs) ||
			(len(m.Proofs) > 0 && m.Deadline <= 0) || (m.MoveDeadline != 0 && m.MoveDeadline > m.Deadline) ||
			(m.Done && (m.Moved == nil || len(m.Bound) == 0)) {
			return false
		}
	}
	return true
}

func natsActionDeadline(r *natsRun) c.Deadline {
	if r.actionDeadline != 0 {
		return r.actionDeadline
	}
	return r.deadline
}

func (d *natsDriver) save(r *natsRun) error {
	if _, err := controlbudget.Remaining(r.deadline); err != nil {
		r.timedOut = true
	}
	return d.journal.StoreNATS(r.plan, natsSnapshot(r), natsActionDeadline(r))
}

var _ NATSJournal = (*TransactionAdapter)(nil)
var _ NATSJournal = (*ComponentTransactions)(nil)

func (t *TransactionAdapter) LoadNATS(p c.Plan, deadline c.Deadline) (NATSTransition, error) {
	state, err := t.options.Journal.Load(p.Tx, deadline)
	if err != nil || state.Plan != p || state.NATS == nil || !natsTransitionValid(*state.NATS, p) {
		return NATSTransition{}, c.ErrIncomplete
	}
	if _, err := t.record(state); err != nil {
		return NATSTransition{}, err
	}
	return *state.NATS, nil
}

func (t *TransactionAdapter) StoreNATS(p c.Plan, x NATSTransition, deadline c.Deadline) error {
	if !t.mu.TryLock() {
		return errors.New("infra_busy")
	}
	defer t.mu.Unlock()
	state, err := t.options.Journal.Load(p.Tx, deadline)
	if err != nil || t.lease == nil || t.holder != p.Tx || state.Plan != p || !natsTransitionValid(x, p) {
		return c.ErrIncomplete
	}
	if _, err := t.record(state); err != nil {
		return err
	}
	if before := state.NATS; before != nil && (x.Sequence < before.Sequence || x.Deadline != before.Deadline ||
		(before.TimedOut && !x.TimedOut) || (before.Outbound.Deadline != 0 && x.Outbound.Deadline != before.Outbound.Deadline) ||
		(before.Inbound.Deadline != 0 && x.Inbound.Deadline != before.Inbound.Deadline)) {
		return c.ErrIncomplete
	}
	state.NATS = &x
	return t.save(&state, deadline)
}

func (r *ComponentTransactions) LoadNATS(p c.Plan, deadline c.Deadline) (NATSTransition, error) {
	if p.Component != c.NATS {
		return NATSTransition{}, c.ErrUnsupported
	}
	return r.nats.LoadNATS(p, deadline)
}
func (r *ComponentTransactions) StoreNATS(p c.Plan, x NATSTransition, deadline c.Deadline) error {
	if p.Component != c.NATS {
		return c.ErrUnsupported
	}
	return r.nats.StoreNATS(p, x, deadline)
}

// Successful recovery reports retirement separately from the failed original
// handover. Keep this diagnostic in durable status as well as private evidence.
// origin: S9 internal review rv-infra F08
func natsTimeoutStatus(state *TransactionState) {
	if state.NATS != nil && state.NATS.TimedOut {
		state.Status.Code = "infra_budget_exceeded"
		state.Status.Detail = "Original NATS handover exceeded its deadline; recovery retired the remaining responsibilities"
	}
}
