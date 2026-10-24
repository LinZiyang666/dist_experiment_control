package infraupgrade

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// TransactionEvidence is implemented by the authenticated host/physical
// observer adapter. A status projection or a disk hash cannot implement this
// port: receipts must come from the real admission registry and current root
// journal. Read binds the requested proof to that journal's actual effects.
// origin: hot-update S9 plan §2.3 / §8 / T44
type TransactionEvidence interface {
	Read(c.Plan, upgradetx.Attempt, c.Phase, c.Digest, c.Deadline) (TransactionProof, error)
}

type TransactionProof struct {
	Plan             c.Plan
	Journal          c.Digest
	Proof            c.Digest
	Current          c.Process
	Census           c.Census
	FilesInstalled   bool
	RuntimeReady     bool
	ReturnVerified   bool
	TemporaryRetired bool
	Receipt          upgradetx.Receipt
	Observation      upgradetx.Observation
}

// The sidecar contains recovery operands, not another S8 execution journal.
// Pending is durable before Store.Apply; after a crash it is reconciled against
// the sole writer's record. Caddy's original nonces/deadline survive recovery.
// These are private disk roots, never S5/S6 image fields or public wire DTOs.
type TransactionState struct {
	Version  uint16
	Revision uint64
	Plan     c.Plan
	Deadline c.Deadline
	Grant    upgradetx.Grant
	Status   c.Status
	Pending  *TransactionChange
	Caddy    *CaddyTransition
	NATS     *NATSTransition
}

type TransactionChange struct {
	Phase c.Phase
	Proof c.Digest
}

type TransactionJournal interface {
	Load(c.ID, c.Deadline) (TransactionState, error)
	CompareAndSwap(uint64, TransactionState, c.Deadline) error
}

type TransactionOptions struct {
	Context     context.Context
	Key         upgradetx.Key
	Owner       upgradetx.Owner
	ActorFP     string
	Incarnation string
	ParentTxID  string
	ParentSlot  string
	ParentToken string
	Scope       upgradetx.Scope
	Store       upgradetx.Store
	Journal     TransactionJournal
	Evidence    TransactionEvidence
}

type TransactionAdapter struct {
	mu      sync.Mutex
	options TransactionOptions
	lease   upgradelock.Lease
	holder  c.ID
}

var _ c.Transaction = (*TransactionAdapter)(nil)
var _ ServiceTransactions = (*TransactionAdapter)(nil)
var _ CaddyJournal = (*TransactionAdapter)(nil)

// TransactionTransition is the S9 Store.Open transition binding. The generic
// predicate checks receipts first; only a verified control receipt can move
// the component's effective selector projection to its target SHA. Root owns
// the physical selector CAS, independently checked by TransactionEvidence.
// origin: hot-update S9 plan §2.3 / §8 / T44
func TransactionTransition(record upgradetx.Record, event upgradetx.Event) (upgradetx.Record, error) {
	next, err := upgradetx.Transition(record, event)
	if err != nil {
		return next, err
	}
	if event.Receipt.Kind == upgradetx.ReceiptControlReady {
		next.EffectiveSHA = transactionAttempt(next).TargetSHA
	}
	return next, nil
}

// NewTransaction never aliases an infra component to broker or agent to bypass
// S8's registration policy. Its real Store and Acquire ports must accept the
// registered component before any driver gets Prepared. Missing upstream
// registration is an explicit refusal, not a new lock/store namespace.
// origin: hot-update S9 plan §7.3 / §8 / T44 / M22
func NewTransaction(options TransactionOptions) (*TransactionAdapter, error) {
	if options.Context == nil || natsMissing(options.Store) || natsMissing(options.Journal) || natsMissing(options.Evidence) {
		return nil, c.ErrContractStub
	}
	if _, err := transactionDomain(options.Key); err != nil || !transactionParent(options) || options.Owner.BootID == "" || options.Owner.PID <= 0 ||
		options.Owner.StartTicks == 0 || options.Owner.InstanceID == "" || options.ActorFP == "" || options.Incarnation == "" ||
		len(options.Scope.Expected) == 0 || len(options.Scope.Participants) != 1 || options.Scope.Participants[0] != options.Key {
		return nil, c.ErrIncomplete
	}
	options.Scope.Participants = append([]upgradetx.Key(nil), options.Scope.Participants...)
	options.Scope.Domains = append([]upgradetx.Domain(nil), options.Scope.Domains...)
	options.Scope.Expected = append([]upgradetx.Resource(nil), options.Scope.Expected...)
	return &TransactionAdapter{options: options}, nil
}

// A child borrows its actual host reservation; it must never re-flock the same
// inode or supply a token without the durable parent/slot authorization. The
// real Store's ParentReservation callback verifies that slot and original TTL.
// origin: hot-update S9 plan §8 / T44
func transactionParent(o TransactionOptions) bool {
	if o.ParentTxID == "" && o.ParentSlot == "" && o.ParentToken == "" {
		return true
	}
	b, err := hex.DecodeString(o.ParentTxID)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == o.ParentTxID && o.ParentSlot != "" && o.ParentToken != ""
}

func transactionDomain(key upgradetx.Key) (c.ID, error) {
	var domain c.ID
	b, err := hex.DecodeString(key.DomainID)
	if err != nil || len(b) != len(domain) || hex.EncodeToString(b) != key.DomainID ||
		(key.Component != upgradetx.ComponentNATS && key.Component != upgradetx.ComponentCaddy) ||
		key.SID != "" || key.ConfiguredNID == "" || !filepath.IsAbs(key.ConfigPath) || filepath.Clean(key.ConfigPath) != key.ConfigPath {
		return domain, c.ErrUnsupported
	}
	copy(domain[:], b)
	if domain == (c.ID{}) {
		return domain, c.ErrIncomplete
	}
	return domain, nil
}

func (t *TransactionAdapter) matches(p c.Plan) bool {
	domain, err := transactionDomain(t.options.Key)
	component := upgradetx.ComponentNATS
	if p.Component == c.Caddy {
		component = upgradetx.ComponentCaddy
	}
	return err == nil && c.ValidatePlan(p) == nil && p.Domain == domain &&
		(p.Component == c.NATS || p.Component == c.Caddy) && component == t.options.Key.Component && p.Action != c.Recover
}

func transactionError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, c.ErrContractStub) {
		return c.ErrContractStub
	}
	var e *upgradetx.Error
	if errors.As(err, &e) {
		switch e.Code {
		case upgradetx.CodeNotWired:
			return c.ErrContractStub
		case upgradetx.CodeUnsupportedComponent:
			return c.ErrUnsupported
		case upgradetx.CodeDeferred:
			return errors.New("infra_busy")
		case upgradetx.CodeGrantExpired:
			return errors.New("infra_budget_exceeded")
		case upgradetx.CodeUnsettled, upgradetx.CodeStateCorrupt, upgradetx.CodeGrantUnknown:
			return errors.New("infra_recovery_required")
		case upgradetx.CodeRejected, upgradetx.CodeWindowClosed, upgradetx.CodeSessionIncarnationMismatch:
			return errors.New("infra_permission_denied")
		}
	}
	return errors.New("infra_result_unknown")
}

func (t *TransactionAdapter) reserve(id c.ID, deadline c.Deadline) error {
	if t.lease != nil {
		if t.holder != id {
			return errors.New("infra_busy")
		}
		return nil
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return errors.New("infra_budget_exceeded")
	}
	ctx, cancel := context.WithTimeout(t.options.Context, remaining)
	defer cancel()
	t.lease, err = upgradelock.Acquire(ctx, t.options.Key, t.options.ParentToken)
	if err != nil {
		return transactionError(err)
	}
	t.holder = id
	return nil
}

func (t *TransactionAdapter) release() error {
	if t.lease == nil {
		return nil
	}
	if err := t.lease.Close(); err != nil {
		return transactionError(err)
	}
	t.lease, t.holder = nil, c.ID{}
	return nil
}

func (t *TransactionAdapter) Acquire(p c.Plan, deadline c.Deadline) (c.Status, error) {
	if !t.mu.TryLock() {
		return c.Status{}, errors.New("infra_busy")
	}
	defer t.mu.Unlock()
	if !t.matches(p) {
		return c.Status{}, c.ErrUnsupported
	}
	if err := serviceBudget(deadline, nil); err != nil {
		return c.Status{}, err
	}
	if existing, err := t.options.Journal.Load(p.Tx, deadline); err == nil {
		return existing.Status, errors.New("infra_recovery_required")
	} else if !errors.Is(err, os.ErrNotExist) {
		return c.Status{}, transactionError(err)
	}
	if err := t.reserve(p.Tx, deadline); err != nil {
		return c.Status{}, err
	}
	remaining, _ := controlbudget.Remaining(deadline)
	expires := time.Now().Add(remaining).UnixNano()
	if t.options.ParentTxID != "" {
		expires = 0 // S8 uses the recorded parent deadline, never a new child TTL.
	}
	grant, err := t.options.Store.Prepare(t.options.Key, upgradetx.PrepareRequest{
		ParentTxID: t.options.ParentTxID, ParentSlot: t.options.ParentSlot,
		ClientNonce: hex.EncodeToString(p.Tx[:]), ActorFP: t.options.ActorFP, Mode: upgradetx.ModeHot,
		SourceRelease: p.Previous.Version, TargetRelease: p.Candidate.Version,
		SourceSHA: digestText(p.Previous.BinarySHA256), TargetSHA: digestText(p.Candidate.BinarySHA256),
		Scope: t.options.Scope, CapabilityDigest: digestText(transactionPlanDigest(p)), ExpiresAt: expires,
	})
	if err != nil {
		_ = t.release()
		return c.Status{}, transactionError(err)
	}
	state := TransactionState{Version: transactionStateVersion, Plan: p, Deadline: deadline, Grant: grant}
	if err := t.save(&state, deadline); err != nil {
		return c.Status{}, err // Keep reservation: grant acceptance is unresolved.
	}
	fence := upgradetx.Fence{Owner: t.options.Owner, RunningSHA: grant.SourceSHA, Incarnation: t.options.Incarnation}
	view, loadErr := t.options.Store.Load(t.options.Key, "")
	var unknown *upgradetx.Error
	if loadErr == nil {
		fence.Current, fence.PreviousSHA = transactionAttempt(view.Historical).Handle, view.Historical.RecordedPreviousSHA
	} else if !errors.As(loadErr, &unknown) || unknown.Code != upgradetx.CodeGrantUnknown {
		return c.Status{}, transactionError(loadErr)
	}
	record, err := t.options.Store.Start(grant, fence)
	if err != nil {
		return c.Status{}, transactionError(err)
	}
	state.Status = c.Status{Tx: p.Tx, Step: p.Step, Phase: c.Prepared, Revision: record.Revision, Before: p.Before, Current: p.Before}
	if err := t.save(&state, deadline); err != nil {
		return c.Status{}, err
	}
	return state.Status, nil
}

func (t *TransactionAdapter) save(state *TransactionState, deadline c.Deadline) error {
	expected := state.Revision
	state.Revision++
	if state.Revision == 0 {
		return c.ErrIncomplete
	}
	return t.options.Journal.CompareAndSwap(expected, *state, deadline)
}

func transactionAttempt(r upgradetx.Record) upgradetx.Attempt {
	if r.Reverse.Handle.TxID != "" {
		return r.Reverse
	}
	return r.Forward
}

// Root Tx and S8 Tx use different ID representations. Their persisted grant
// commitment binds the complete plan; matching two artifact hashes alone would
// allow another domain/policy/step to appropriate an accepted execution.
// origin: hot-update S9 plan §2.2 / §8 / T44
func (t *TransactionAdapter) record(state TransactionState) (upgradetx.Record, error) {
	if state.Version != transactionStateVersion || state.Revision == 0 || !t.matches(state.Plan) || state.Grant.Handle.Key != t.options.Key ||
		state.Grant.CapabilityDigest != digestText(transactionPlanDigest(state.Plan)) {
		return upgradetx.Record{}, c.ErrIncomplete
	}
	view, err := t.options.Store.Load(t.options.Key, state.Grant.Handle.TxID)
	if err != nil {
		return upgradetx.Record{}, transactionError(err)
	}
	if view.Historical.Key != t.options.Key || view.Historical.Grant != state.Grant {
		return upgradetx.Record{}, c.ErrIncomplete
	}
	return view.Historical, nil
}

// S9 Moving is still before effect. Persist S8's pristine and partial_active
// barriers together with Cutting's pending intent, before the root can signal.
// S9 Halted preserves S8's last cut and pending outcome; a diagnostic must not
// invent S8's independent owner-death/reverse authority.
// origin: hot-update S9 plan §7.2 / T44
func transactionEvent(a upgradetx.Attempt, phase c.Phase, proof TransactionProof) (upgradetx.Event, error) {
	e := upgradetx.Event{Reason: transactionReason(phase, proof.Proof)}
	switch phase {
	case c.Prepared:
		return e, c.ErrIncomplete
	case c.Staged:
		e.Phase = upgradetx.PhaseStaged
	case c.Ready, c.Moving:
		e.Phase = upgradetx.PhaseReady
	case c.Cutting:
		if a.Phase == upgradetx.PhaseReady {
			e.Phase, e.CutStage = upgradetx.PhaseFreezing, upgradetx.CutPristine
		} else {
			e.Phase, e.CutStage = upgradetx.PhaseActivating, upgradetx.CutPartialActive
		}
	case c.Verifying:
		e.Phase, e.CutStage, e.Receipt = upgradetx.PhaseServingNew, upgradetx.CutActive, proof.Receipt
	case c.Returning:
		e.Phase, e.CutStage = upgradetx.PhaseMigrating, upgradetx.CutActive
	case c.Retiring:
		e.Phase, e.CutStage, e.Receipt = upgradetx.PhaseMigrating, upgradetx.CutRetiring, proof.Receipt
	case c.Completed:
		e.Phase, e.CutStage, e.Outcome, e.Receipt = upgradetx.PhaseCompleted, upgradetx.CutReleased, upgradetx.OutcomeSuccess, proof.Receipt
	case c.Deferred:
		e.Phase, e.CutStage, e.Outcome = upgradetx.PhaseDeferred, upgradetx.CutNone, upgradetx.OutcomeDeferred
	case c.Halted:
		// Preserve the cut and receipts while the detailed root journal recovers.
	default:
		return e, c.ErrIncomplete
	}
	return e, nil
}

func transactionReason(phase c.Phase, proof c.Digest) string {
	b, _ := json.Marshal(TransactionChange{phase, proof})
	return "infra-v1:" + string(b)
}

// A positive live count is legal after migration. What must vanish is every
// original incarnation and temporary owner's pending responsibility. Reload
// deliberately retains its main PID; binary replacement never does.
// origin: hot-update S9 plan §2.3 / T44 / M22
func validTransactionProof(p c.Plan, phase c.Phase, proof c.Digest, x TransactionProof) bool {
	if x.Plan != p || x.Proof != proof || proof == (c.Digest{}) || x.Journal == (c.Digest{}) || !caddyCensus(x.Census) {
		return false
	}
	if phase == c.Staged && !x.FilesInstalled {
		return false
	}
	if phase >= c.Verifying && phase <= c.Completed {
		if !x.RuntimeReady || !caddyProcess(x.Current) || x.Current.BootID != p.Before.BootID || x.Current.ExeSHA256 != p.Candidate.BinarySHA256 || !caddyPresent(x.Census, x.Current) {
			return false
		}
	}
	if phase == c.Completed {
		if !x.ReturnVerified || !x.TemporaryRetired || x.Census.OldOwnerPending != 0 ||
			(p.Action != c.Reload && natsOwnsOld(x.Census, p.Before)) || (p.Action == c.Reload && x.Current != p.Before) {
			return false
		}
	}
	return true
}

func transactionEdge(before, after c.Phase) bool {
	if before == c.Completed || before == c.Deferred {
		return false
	}
	if after == c.Halted || after == c.Deferred {
		return after != c.Deferred || before < c.Cutting
	}
	if before == c.Halted {
		return after >= c.Cutting && after <= c.Completed
	}
	return after == before+1
}

// Mutable diagnostic/census fields are not CAS operands: the drivers enrich
// those from their own observation. Identity, phase, proof and revision are
// immutable operands; the physical census is independently read under lease.
// origin: hot-update S9 plan §7.2 / T44
func (t *TransactionAdapter) CompareAndSwap(expected c.Status, phase c.Phase, proof c.Digest, deadline c.Deadline) (c.Status, error) {
	if !t.mu.TryLock() {
		return c.Status{}, errors.New("infra_busy")
	}
	defer t.mu.Unlock()
	if err := serviceBudget(deadline, nil); err != nil {
		return c.Status{}, err
	}
	state, err := t.options.Journal.Load(expected.Tx, deadline)
	if err != nil {
		return c.Status{}, transactionError(err)
	}
	if t.lease == nil || t.holder != expected.Tx || state.Status.Tx != expected.Tx || state.Status.Step != expected.Step ||
		state.Status.Revision != expected.Revision || state.Status.Phase != expected.Phase || state.Status.Proof != expected.Proof || state.Status.Before != expected.Before ||
		!transactionEdge(expected.Phase, phase) || state.Pending != nil {
		return state.Status, errors.New("infra_recovery_required")
	}
	state.Pending = &TransactionChange{phase, proof}
	if err := t.save(&state, deadline); err != nil {
		return state.Status, err
	}
	return t.apply(&state, deadline)
}

func (t *TransactionAdapter) apply(state *TransactionState, deadline c.Deadline) (c.Status, error) {
	record, err := t.record(*state)
	if err != nil {
		return state.Status, err
	}
	change := *state.Pending
	for writes := 0; writes < 3; writes++ {
		if err := serviceBudget(deadline, nil); err != nil {
			return state.Status, err
		}
		a := transactionAttempt(record)
		x, readErr := t.options.Evidence.Read(state.Plan, a, change.Phase, change.Proof, deadline)
		if readErr != nil || !validTransactionProof(state.Plan, change.Phase, change.Proof, x) {
			return state.Status, c.ErrIncomplete
		}
		// An exact durable result resolves a lost Store.Apply reply. A partial
		// Cutting bridge still needs the activating boundary before publication.
		done := record.Reason == transactionReason(change.Phase, change.Proof) &&
			(change.Phase != c.Cutting || a.Phase == upgradetx.PhaseActivating)
		if !done {
			event, eventErr := transactionEvent(a, change.Phase, x)
			if eventErr != nil {
				return state.Status, eventErr
			}
			_, err = t.options.Store.Apply(a.Handle, event)
			if err != nil {
				return state.Status, transactionError(err)
			}
			record, err = t.record(*state)
			if err != nil || record.Revision <= a.Handle.Revision {
				return state.Status, errors.New("infra_result_unknown")
			}
			continue
		}
		if change.Phase == c.Completed && upgradetx.Completed(a, x.Observation) != nil {
			return state.Status, c.ErrIncomplete
		}
		state.Status = c.Status{Tx: state.Plan.Tx, Step: state.Plan.Step, Phase: change.Phase,
			Revision: record.Revision, Before: state.Plan.Before, Current: x.Current, Census: x.Census, Proof: change.Proof}
		natsTimeoutStatus(state)
		if change.Phase == c.Halted {
			state.Status.Code = "infra_recovery_required"
		}
		state.Pending = nil
		if err := t.save(state, deadline); err != nil {
			return state.Status, err
		}
		return state.Status, nil
	}
	return state.Status, errors.New("infra_result_unknown")
}

// Status is read-only, including when a writer died between sidecar intent and
// S8 commit. Only explicit ResumePlan obtains the reservation and reconciles;
// a query cannot quietly become a new effect, a renewed grant, or recovery.
// origin: hot-update S9 plan §7.2 / T44
func (t *TransactionAdapter) Load(id c.ID, deadline c.Deadline) (c.Status, error) {
	if err := serviceBudget(deadline, nil); err != nil {
		return c.Status{}, err
	}
	state, err := t.options.Journal.Load(id, deadline)
	if err != nil {
		return c.Status{}, transactionError(err)
	}
	record, err := t.record(state)
	if err != nil {
		return state.Status, err
	}
	if state.Pending != nil || state.Status.Revision != record.Revision || state.Status.Tx != id {
		return state.Status, errors.New("infra_recovery_required")
	}
	if state.Status.Phase != c.Prepared {
		x, err := t.options.Evidence.Read(state.Plan, transactionAttempt(record), state.Status.Phase, state.Status.Proof, deadline)
		if err != nil || !validTransactionProof(state.Plan, state.Status.Phase, state.Status.Proof, x) ||
			record.Reason != transactionReason(state.Status.Phase, state.Status.Proof) ||
			(state.Status.Phase == c.Completed && upgradetx.Completed(transactionAttempt(record), x.Observation) != nil) {
			return state.Status, c.ErrIncomplete
		}
		state.Status.Current, state.Status.Census = x.Current, x.Census
	}
	return state.Status, nil
}

func (t *TransactionAdapter) Plan(id c.ID, deadline c.Deadline) (c.Plan, error) {
	state, err := t.options.Journal.Load(id, deadline)
	if err == nil && (state.Plan.Tx != id || !t.matches(state.Plan)) {
		err = c.ErrIncomplete
	}
	return state.Plan, err
}

func (t *TransactionAdapter) ResumePlan(id c.ID, deadline c.Deadline) (c.Plan, error) {
	if !t.mu.TryLock() {
		return c.Plan{}, errors.New("infra_busy")
	}
	defer t.mu.Unlock()
	state, err := t.options.Journal.Load(id, deadline)
	if err != nil || state.Plan.Tx != id || !t.matches(state.Plan) {
		return c.Plan{}, c.ErrIncomplete
	}
	if err := t.reserve(id, deadline); err != nil {
		return c.Plan{}, err
	}
	if state.Pending != nil {
		if _, err := t.apply(&state, deadline); err != nil {
			return state.Plan, err
		}
	}
	return state.Plan, nil
}

// Finish follows the driver's final recovery-state write. Releasing in the
// Completed CAS would race Caddy's last StoreCaddy and allow an image cut while
// the coordinator still owns an outstanding durable responsibility.
func (t *TransactionAdapter) Finish(id c.ID, deadline c.Deadline) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	status, err := t.Load(id, deadline)
	if err != nil || (status.Phase != c.Completed && status.Phase != c.Deferred) || (t.lease != nil && t.holder != id) {
		return errors.New("infra_recovery_required")
	}
	return t.release()
}

func (t *TransactionAdapter) LoadCaddy(p c.Plan, deadline c.Deadline) (CaddyTransition, error) {
	state, err := t.options.Journal.Load(p.Tx, deadline)
	if err != nil || state.Plan != p || state.Caddy == nil || !caddyTransitionValid(*state.Caddy, p) {
		return CaddyTransition{}, c.ErrIncomplete
	}
	return *state.Caddy, nil
}

func (t *TransactionAdapter) StoreCaddy(p c.Plan, x CaddyTransition, deadline c.Deadline) error {
	if !t.mu.TryLock() {
		return errors.New("infra_busy")
	}
	defer t.mu.Unlock()
	state, err := t.options.Journal.Load(p.Tx, deadline)
	if err != nil || t.lease == nil || t.holder != p.Tx || state.Plan != p || !caddyTransitionValid(x, p) {
		return c.ErrIncomplete
	}
	if _, err := t.record(state); err != nil {
		return err
	}
	if state.Caddy != nil && (x.Cursor < state.Caddy.Cursor || x.Sequence < state.Caddy.Sequence || x.Deadline > state.Caddy.Deadline) {
		return c.ErrIncomplete
	}
	state.Caddy = &x
	return t.save(&state, deadline)
}

func digestText(d c.Digest) string { return hex.EncodeToString(d[:]) }
func transactionPlanDigest(p c.Plan) c.Digest {
	b, _ := json.Marshal(p)
	return sha256.Sum256(append([]byte("tether-infra-transaction-v1\n"), b...))
}

// FileTransactionJournal uses the existing S8 short guard only for its local
// RMW. Store calls and physical probes occur outside this guard. Atomic rename
// plus file and directory sync make the sidecar's pending intent durable.
type FileTransactionJournal struct {
	Key       upgradetx.Key
	Directory string
}

// Names contain only the component vocabulary and hex ID. Recovery operands
// and raw physical evidence stay in this private directory, never public status.
// origin: hot-update S9 plan §2.2 / T44
func (j FileTransactionJournal) path(id c.ID) (string, error) {
	if id == (c.ID{}) || !filepath.IsAbs(j.Directory) || filepath.Clean(j.Directory) != j.Directory {
		return "", c.ErrIncomplete
	}
	info, err := os.Lstat(j.Directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return "", c.ErrIncomplete
	}
	return filepath.Join(j.Directory, string(j.Key.Component)+"-"+hex.EncodeToString(id[:])+".infra.json"), nil
}

func (j FileTransactionJournal) Load(id c.ID, deadline c.Deadline) (TransactionState, error) {
	var state TransactionState
	if err := serviceBudget(deadline, nil); err != nil {
		return state, err
	}
	path, err := j.path(id)
	if err != nil {
		return state, err
	}
	f, err := openTransactionJournal(path)
	if err != nil {
		return state, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > 4<<20 {
		return state, c.ErrIncomplete
	}
	b, err := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	if err != nil || len(b) > 4<<20 {
		return state, c.ErrIncomplete
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || state.Version != transactionStateVersion || state.Revision == 0 || state.Plan.Tx != id || state.Grant.Handle.Key != j.Key {
		return TransactionState{}, c.ErrIncomplete
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(canonical, b) { // Also rejects duplicate keys and trailing values.
		return TransactionState{}, c.ErrIncomplete
	}
	return state, nil
}

func (j FileTransactionJournal) CompareAndSwap(expected uint64, next TransactionState, deadline c.Deadline) error {
	path, err := j.path(next.Plan.Tx)
	if err != nil || next.Revision != expected+1 || next.Revision == 0 || next.Version != transactionStateVersion || next.Grant.Handle.Key != j.Key {
		return c.ErrIncomplete
	}
	return upgradelock.WithGuard(j.Key, func() error {
		current, err := j.Load(next.Plan.Tx, deadline)
		if (expected == 0 && !errors.Is(err, os.ErrNotExist)) || (expected != 0 && (err != nil || current.Revision != expected || current.Plan != next.Plan || current.Grant != next.Grant)) {
			return errors.New("infra_recovery_required")
		}
		b, err := json.Marshal(next)
		if err != nil || len(b) > 4<<20 {
			return c.ErrIncomplete
		}
		f, err := os.CreateTemp(j.Directory, ".infra-intent-*")
		if err != nil {
			return errors.New("infra_internal")
		}
		defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
		if _, err = f.Write(b); err == nil {
			err = f.Sync()
		}
		if err == nil {
			err = f.Close()
		}
		if err == nil {
			err = os.Rename(f.Name(), path)
		}
		if err != nil {
			return errors.New("infra_result_unknown")
		}
		dir, err := os.Open(j.Directory)
		if err != nil {
			return errors.New("infra_result_unknown")
		}
		defer func() { _ = dir.Close() }()
		if dir.Sync() != nil {
			return errors.New("infra_result_unknown")
		}
		return nil
	})
}
