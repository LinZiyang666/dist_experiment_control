// registry.go keeps creation, retained results and generation-bound controls
// in one registry. Forgetting an outcome never renews a creation permit.
// origin: hot-update S7 plan §4 / §6.1
package xferworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const resultRetention = 10 * time.Minute
const registrySlots = 1024

func tokenValid(value string) bool {
	// These components become authenticated subject tokens. Reject wildcard,
	// separator and newline injection before they enter a retained identity.
	return value != "" && len(value) <= 256 && !strings.ContainsAny(value, ". *>\t\r\n/")
}

// actorValid accepts the canonical actor fingerprint ("SHA256:" plus raw
// standard base64, which contains '/' and '+'); it is identity data in a body,
// never a subject token. Every real ctl identity has this shape, so a
// token-only check refused every authenticated prepare.
// origin: hot-update S7 plan §6.2 / INTEG-D agent wiring
func actorValid(value string) bool {
	if tokenValid(value) {
		return true
	}
	body, ok := strings.CutPrefix(value, "SHA256:")
	if !ok || len(body) != 43 {
		return false
	}
	for _, c := range body {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '+', c == '/':
		default:
			return false
		}
	}
	return true
}

func (o *owner) identityValid(id x.Identity) bool {
	// The registry belongs to one agent incarnation. Optional SID/NID scope
	// supports both an agent-wide registry and a more narrowly bound adapter.
	for _, value := range []string{id.SID, id.NID, id.AgentInstance, id.TransferID, id.EntryNonce, id.IntentDigest} {
		if !tokenValid(value) {
			return false
		}
	}
	return actorValid(id.ActorFP) && (id.ObjectVersion == "" || tokenValid(id.ObjectVersion)) && id.AgentInstance == o.cfg.Identity.AgentInstance && (o.cfg.Identity.SID == "" || id.SID == o.cfg.Identity.SID) && (o.cfg.Identity.NID == "" || id.NID == o.cfg.Identity.NID)
}

func (o *owner) prepareValid(p x.Prepare) error {
	// A valid signature at the adapter is not permission to mix its identity
	// with another intent, object incarnation, origin or settlement capsule.
	if !o.identityValid(p.Identity) || p.Permit.Identity != p.Identity || p.Origin != p.Permit.Origin || p.Permit.PermitNonce == "" || p.Permit.Signature == "" {
		return workerError(x.CodeIntentConflict)
	}
	if !tokenValid(p.Origin.BrokerID) || !tokenValid(p.Origin.InstanceID) || !tokenValid(p.Origin.BootID) || p.Origin.Generation == 0 {
		return workerError(x.CodeIntentConflict)
	}
	if p.Intent.Verb != "push" && p.Intent.Verb != "pull" {
		return workerError(x.CodeIntentConflict)
	}
	if p.Intent.Size < 0 || p.Intent.Size > 2<<30 || p.Intent.RateLimitBPS < 0 {
		return workerError(x.CodeBudgetExceeded)
	}
	// Tier A has no object version; it still owns copied, authenticated bytes.
	// Tier B requires both a fixed version and representable receipt geometry.
	if p.Intent.Tier == "a" {
		if p.Identity.ObjectVersion != "" || p.Intent.Size > 8<<20 || (p.Intent.Verb == "push" && int64(len(p.Inline)) != p.Intent.Size) {
			return workerError(x.CodeObjectVersionMismatch)
		}
		if p.Intent.Verb == "push" {
			digest := sha256.Sum256(p.Inline)
			if p.Intent.SHA256 != hex.EncodeToString(digest[:]) {
				return workerError(x.CodeIntentConflict)
			}
		}
	} else if p.Intent.Tier != "b" || p.Identity.ObjectVersion == "" || len(p.Inline) != 0 || p.Intent.ChunkSize < 32<<10 || p.Intent.ChunkSize > 1<<20 {
		return workerError(x.CodeObjectVersionMismatch)
	}
	if p.Settlement != nil && (p.Settlement.Identity != p.Identity || p.Settlement.Origin != p.Origin) {
		return workerError(x.CodeIntentConflict)
	}
	// A continuation may tighten remaining time, but cannot mint a later start
	// or an allowance beyond the already sealed whole-transfer ceiling.
	if p.Budget.StartedAt.IsZero() || p.Budget.RemainingNS <= 0 || p.Budget.RemainingNS > p.Budget.EffectiveBudgetNS || p.Budget.EffectiveBudgetNS > int64(2108*time.Second) {
		return workerError(x.CodeBudgetExceeded)
	}
	if p.Intent.Tier == "a" && p.Budget.EffectiveBudgetNS > int64(30*time.Second) {
		return workerError(x.CodeBudgetExceeded)
	}
	return nil
}

// Prepare checks the retained entry before permit expiry. The opposite order
// breaks long transfers; omitting expiry after GC recreates their side effects.
// origin: hot-update S7 plan §6.1 / T27
func (o *owner) Prepare(p x.Prepare, deadline x.Deadline) (x.Status, error) {
	// Authorization latency consumes the received remaining allowance. Capture
	// receipt time before that callback rather than renewing it on admission.
	_, receivedAt, clockErr := o.cfg.Clock()
	if clockErr != nil {
		return x.Status{}, clockErr
	}
	if err := o.enter(deadline); err != nil {
		return x.Status{}, err
	}
	defer o.leave()
	// Only creation is registry-wide single-flight. Byte steps and controls use
	// per-entry tokens so one transfer cannot serialize every other transfer.
	creation := &entry{token: o.creating}
	if err := o.lockEntry(creation, deadline); err != nil {
		return x.Status{}, err
	}
	defer func() { <-o.creating }()
	if err := o.prepareValid(p); err != nil {
		return x.Status{}, err
	}
	if err := o.collectResults(); err != nil {
		return x.Status{}, err
	}
	o.mu.Lock()
	existing := o.entries[key(p.Identity)]
	o.mu.Unlock()
	if existing != nil {
		if !xferimage.SameWorkerPrepare(existing.value.Prepare, p) {
			return x.Status{}, workerError(x.CodeIntentConflict)
		}
		if err := o.cfg.Authority.Check(p.Identity, x.AccessContinue, deadline); err != nil {
			return x.Status{}, err
		}
		return o.Status(p.Identity)
	}
	now := o.cfg.UTC()
	// First creation must fit the original short-lived window. This check is
	// before Capacity.Reserve, factories, files, transport and grant work.
	// origin: hot-update S7 plan §6.1 / T27
	if !now.Before(p.Permit.CreateUntil) || now.Before(p.Permit.IssuedAt) || !p.Permit.CreateUntil.After(p.Permit.IssuedAt) || p.Permit.CreateUntil.Sub(p.Permit.IssuedAt) > 5*time.Minute {
		return x.Status{}, workerError(x.CodePermitExpired)
	}
	if err := o.cfg.Authority.Check(p.Identity, x.AccessCreate, deadline); err != nil {
		return x.Status{}, err
	}
	if _, err := o.remaining(deadline); err != nil {
		return x.Status{}, err
	}
	// Recheck UTC after authorization: a blocked callback cannot extend the
	// permit simply because it entered before the expiry boundary.
	if !o.cfg.UTC().Before(p.Permit.CreateUntil) {
		return x.Status{}, workerError(x.CodePermitExpired)
	}
	o.mu.Lock()
	full := len(o.entries) >= registrySlots
	for _, e := range o.entries {
		// A sender whose object is published, or an entry with an exact
		// terminal, has no remaining byte work to serialize against.
		// origin: hot-update S7 plan §1.2 / INTEG-D sequential transfers
		// A receiver's courier likewise owns only settlement after its outcome;
		// keep its entry/reservation while allowing the next byte operation.
		// origin: S7 internal review TRANSFER-MAKETEST-3 F1
		if p.Intent.Tier == "b" && e.value.Prepare.Intent.Tier == "b" && e.value.Prepare.Identity.SID == p.Identity.SID &&
			e.value.Status.Phase != x.PhaseSettled && e.value.Status.Terminal == nil && !e.published && e.courier == nil {
			full = true
		}
	}
	o.mu.Unlock()
	if full {
		return x.Status{}, workerError(x.CodeCapacityExceeded)
	}
	reservation := xferimage.WorkerPreparationReservation(p)
	// The account is supplied by the whole-cut owner. There is no independent
	// transfer pool, and no eviction of active responsibility to fit a new task.
	id, err := o.cfg.Capacity.Reserve(reservation)
	if err != nil {
		return x.Status{}, err
	}
	if id == "" {
		return x.Status{}, workerError(x.CodeCapacityExceeded)
	}
	boot, mono, err := o.cfg.Clock()
	if err != nil || mono <= 0 || boot != o.bootID || receivedAt <= 0 || p.Budget.RemainingNS > math.MaxInt64-receivedAt || mono-receivedAt >= p.Budget.RemainingNS {
		return x.Status{}, errors.Join(workerError(x.CodeBudgetExceeded), err, o.cfg.Capacity.Release(id))
	}
	status := x.Status{State: x.StatusLive, Phase: x.PhasePrepared, Identity: p.Identity, Origin: p.Origin, Budget: p.Budget}
	e := &entry{value: x.WorkerEntry{Prepare: xferimage.CloneWorkerPrepare(p), Status: status, OwnerGeneration: o.state.Lineage.Generation, Reservation: id}, token: make(chan struct{}, 1), byteDeadline: x.BootDeadline{BootID: boot, Deadline: x.Deadline(receivedAt + p.Budget.RemainingNS)}}
	// Before a byte actor exists, this is the only private deadline slot. Keep
	// the original local byte deadline through prepared-only imports as well.
	e.retention = x.RetentionState{Identity: p.Identity, CreateUntil: p.Permit.CreateUntil, Deadline: e.byteDeadline}
	o.mu.Lock()
	o.entries[key(p.Identity)] = e
	o.state.CapacityReservations = append(o.state.CapacityReservations, x.CapacityReservation{ID: id, Reservation: reservation})
	o.mu.Unlock()
	o.observe(x.Observation{Kind: x.ObserveControl, TransferID: p.Identity.TransferID, EntryNonce: p.Identity.EntryNonce, Phase: x.PhasePrepared})
	return status, nil
}

func (o *owner) collectResults() error {
	// Only settled entries can be removed. History/cleanup/grant responsibility
	// is deliberately independent of byte deadline and result TTL.
	// origin: hot-update S7 plan §1.2 / T27
	o.mu.Lock()
	var candidates []*entry
	for _, e := range o.entries {
		if !e.running && e.value.Status.Phase == x.PhaseSettled && !e.retention.ResultUntil.IsZero() && !o.cfg.UTC().Before(e.retention.ResultUntil) && !o.cfg.UTC().Before(e.retention.CreateUntil) {
			candidates = append(candidates, e)
		}
	}
	o.mu.Unlock()
	for _, e := range candidates {
		// A delayed receipt may already have looked up this entry. Serialize its
		// final acknowledgement with removal, and recheck the retained phase.
		select {
		case e.token <- struct{}{}:
		default:
			continue
		}
		o.mu.Lock()
		eligible := !e.running && e.value.Status.Phase == x.PhaseSettled
		o.mu.Unlock()
		if !eligible {
			<-e.token
			continue
		}
		if err := o.cfg.Capacity.Release(e.value.Reservation); err != nil {
			<-e.token
			return err
		}
		o.mu.Lock()
		delete(o.entries, key(e.value.Prepare.Identity))
		id := e.value.Prepare.Identity
		o.state.CanonicalOutcomes = slices.DeleteFunc(o.state.CanonicalOutcomes, func(c x.CanonicalOutcome) bool { return c.Identity == id })
		o.state.Revocations = slices.DeleteFunc(o.state.Revocations, func(r x.Revocation) bool { return r.Identity == id })
		for i, r := range o.state.CapacityReservations {
			if r.ID == e.value.Reservation {
				o.state.CapacityReservations = append(o.state.CapacityReservations[:i], o.state.CapacityReservations[i+1:]...)
				break
			}
		}
		o.mu.Unlock()
		<-e.token
	}
	return nil
}

func (o *owner) Status(id x.Identity) (x.Status, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.identityValid(id) || o.stopped {
		return x.Status{}, workerError(x.CodeOwnerRecovering)
	}
	e := o.entries[key(id)]
	if e == nil {
		return x.Status{}, workerError(x.CodeResultUnknown)
	}
	if e.value.Prepare.Identity != id {
		return x.Status{}, workerError(x.CodeIntentConflict)
	}
	return xferimage.CloneTransferStatus(e.value.Status), nil
}

// Start is idempotent while a runner owns the entry. The registry fence pauses
// between steps, while Freeze joins the one step already admitted.
func (o *owner) Start(id x.Identity) error {
	o.mu.Lock()
	e := o.entries[key(id)]
	if e == nil || e.value.Prepare.Identity != id || o.closed || o.stopped {
		o.mu.Unlock()
		return workerError(x.CodeOwnerRecovering)
	}
	deadline := e.byteDeadline.Deadline
	if deadline == 0 {
		o.mu.Unlock()
		return workerError(x.CodeOwnerRecovering)
	}
	o.mu.Unlock()
	if err := o.enter(deadline); err != nil {
		return err
	}
	defer o.leave()
	if err := o.lockEntry(e, deadline); err != nil {
		return err
	}
	defer func() { <-e.token }()
	o.mu.Lock()
	retained := o.entries[key(id)] == e
	o.mu.Unlock()
	if !retained {
		return workerError(x.CodeResultUnknown)
	}
	o.mu.Lock()
	already := e.running || e.value.GateClosed || e.value.Status.Terminal != nil
	o.mu.Unlock()
	if already {
		return nil
	}
	p := e.value.Prepare
	if e.actorError != nil {
		return e.actorError
	}
	// Continuation authority is separate from new creation/ONLINE permission;
	// replacing the control session cannot turn this into a fresh prepare.
	if err := o.cfg.Authority.Check(id, x.AccessContinue, deadline); err != nil {
		return err
	}
	budget, err := entryActorBudget(o, e, p.Budget)
	if err != nil {
		return err
	}
	if p.Intent.Verb == "pull" && e.sender == nil {
		if o.cfg.MakeSender == nil {
			return workerError(x.CodeOwnerRecovering)
		}
		actor, err := o.cfg.MakeSender(x.SendConfig{Base: o.cfg.Base, Identity: id, Intent: p.Intent, Budget: budget, Transport: o.transport})
		o.mu.Lock()
		e.sender = actor
		o.mu.Unlock()
		if err != nil || actor == nil {
			failure := errors.Join(workerError(x.CodeOwnerRecovering), err)
			// A nil actor owns no source or attachment. A replay may retry its
			// construction within this entry's original deadline after reconnect.
			// A partial actor remains owned and fenced: never construct over it.
			// origin: S7 internal review FIX-TIERA2 F7 / hot-update S7 plan §4 / §6.1 / T46
			if actor != nil {
				e.actorError = failure
			}
			return failure
		}
	} else if p.Intent.Verb == "push" && e.receiver == nil {
		if o.cfg.MakeReceiver == nil {
			return workerError(x.CodeOwnerRecovering)
		}
		actor, err := o.cfg.MakeReceiver(x.ReceiveConfig{Base: o.cfg.Base, Identity: id, Budget: budget, Transport: o.transport})
		o.mu.Lock()
		e.receiver = actor
		o.mu.Unlock()
		if err != nil || actor == nil {
			e.actorError = errors.Join(workerError(x.CodeOwnerRecovering), err)
			return e.actorError
		}
	}
	o.mu.Lock()
	e.value.Started, e.running = true, true
	e.done = make(chan struct{})
	o.mu.Unlock()
	go o.run(e)
	return nil
}

// errCourierNotDue keeps a still-pending courier on the runner's bounded wait.
var errCourierNotDue = workerError(x.CodeReconcileRequired)

// An actor can start long after prepare: a push receiver waits for the whole
// upload. RemainingNS is relative to the moment it is read, so the actor gets
// only what remains of this entry's original local deadline. Handing it the
// prepare-time value extended its byte deadline by that wait (9.3s for a 9MiB
// push at 1MiB/s). Local BOOTTIME keeps this independent of peer clocks.
// origin: S7 FIX-CUT actor deadline refresh / hot-update S7 plan §1.2 / LQ2 / M11
func entryActorBudget(o *owner, e *entry, budget x.BudgetSeal) (x.BudgetSeal, error) {
	boot, now, err := o.cfg.Clock()
	o.mu.Lock()
	original := e.byteDeadline
	o.mu.Unlock()
	if err != nil || now <= 0 || boot != original.BootID || int64(original.Deadline) <= now {
		return x.BudgetSeal{}, errors.Join(workerError(x.CodeBudgetExceeded), err)
	}
	budget.RemainingNS = min(budget.RemainingNS, int64(original.Deadline)-now)
	return budget, nil
}

func (o *owner) run(e *entry) {
	// The runner belongs to the explicit owner. Pause preserves its actor and
	// pending outcome; Stop wakes and joins it before releasing any resources.
	defer func() { o.mu.Lock(); e.running = false; close(e.done); o.signalLocked(); o.mu.Unlock() }()
	for {
		o.mu.Lock()
		if o.stopped || o.stopping || e.value.Status.Phase == x.PhaseSettled {
			o.mu.Unlock()
			return
		}
		closed, changed := o.closed, o.changed
		deadline := e.byteDeadline.Deadline
		if e.courier != nil {
			_, now, err := o.cfg.Clock()
			if err != nil || now > math.MaxInt64-int64(5*time.Second) {
				o.mu.Unlock()
				return
			}
			deadline = x.Deadline(now + int64(5*time.Second))
		}
		o.mu.Unlock()
		if closed {
			<-changed
			continue
		}
		if err := o.enter(deadline); err != nil {
			o.mu.Lock()
			paused := o.closed && !o.stopped && !o.stopping
			o.mu.Unlock()
			if paused {
				continue
			}
			return
		}
		err := o.lockEntry(e, deadline)
		if err == nil {
			err = o.step(e, deadline)
			<-e.token
		}
		o.leave()
		if err != nil {
			o.mu.Lock()
			if o.stopping || o.stopped {
				o.mu.Unlock()
				return
			}
			courierPending := e.courier != nil && e.value.Status.Phase != x.PhaseSettled
			// A receiver that has synced every byte parks for its durable Grant
			// instead of ending: the Grant can arrive between this step's error
			// and the runner's exit, and nothing else would resume the commit.
			// The byte deadline still bounds the wait through enter().
			// origin: hot-update S7 plan §4 / INTEG-D outer commit adapter
			awaitingGrant := e.receiver != nil && e.courier == nil && !e.value.GateClosed &&
				strings.Contains(err.Error(), x.CodeCommitInflight)
			// A sender retains its pending bytes/receipt across transport loss.
			// Ending the runner on the SDK's raw error strands that original
			// responsibility even after the same attachment reconnects.
			// origin: S7 internal review PULL-B F9 / hot-update S7 plan §2.2 / §4 / T46
			retrySender := e.sender != nil && retryWorkerTransport(err)
			changed := o.changed
			o.mu.Unlock()
			if !courierPending && !awaitingGrant && !retrySender && !strings.Contains(err.Error(), x.CodeOwnerRecovering) && !strings.Contains(err.Error(), x.CodeReconcileRequired) {
				return
			}
			// A settlement attempt has its own five-second deadline and bounded
			// retry cadence. Byte expiry cannot retire an unacknowledged courier.
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-timer.C:
			case <-changed:
			}
			timer.Stop()
		}
	}
}

// Receipt attempts can fail before or after their publish escaped. Retrying
// the retained actor resolves its pending attempt first; it cannot remint an
// object or rewind its source. Identity/content/permission refusals expressed
// as transfer codes retain their existing terminal behavior.
// origin: S7 internal review PULL-B F9 / hot-update S7 plan §2.2 / §4 / T46
func retryWorkerTransport(err error) bool {
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, controlbudget.ErrExpired) ||
		errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrNoServers) ||
		errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrReconnectBufExceeded) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrAuthorization) || errors.Is(err, jetstream.ErrNoStreamResponse)
}

func (o *owner) step(e *entry, deadline x.Deadline) error {
	// Receipt controls can settle a retained sender while this step waits for
	// its token. Such an entry has no courier and owns no further retirement.
	// origin: S10 internal review POLISH-A F1 / hot-update S7 plan §4
	if e.value.Status.Phase == x.PhaseSettled {
		return nil
	}
	// origin: S7 internal review rv-lifecycle F3
	// An ACK ends publication, not the runner that owns local retirement.
	if e.value.Status.Terminal != nil {
		return o.retireReceipt(e, deadline)
	}
	var err error
	if e.courier != nil {
		err = e.courier.Step(deadline)
		state, freezeErr := e.courier.Freeze(deadline)
		if freezeErr != nil {
			return errors.Join(err, freezeErr)
		}
		if state.Receipt != nil {
			return o.acceptReceipt(e, *state.Receipt, deadline)
		}
		// A courier inside its settlement backoff returns nil without sending.
		// Reporting that as progress re-entered Step/Freeze about once per
		// millisecond until the backoff elapsed, flooding its observer; the
		// runner's bounded wait owns the retry cadence instead.
		// origin: S7 FIX-CUT courier backoff spin / hot-update S7 plan §1.2 / §4 / T16
		if err == nil {
			err = errCourierNotDue
		}
	} else if e.value.GateClosed {
		return io.EOF
	} else if e.sender != nil {
		// An inline pull learns its size only from its pinned source seal, so
		// its entry reservation grows from that seal before the sender copies.
		// A finished copy is never re-sealed (it no longer consults the source),
		// and an imported sealed entry re-checks the same idempotent reservation
		// before any copy can be allocated.
		// origin: hot-update S7 plan §8.1 / §8.3 / FIX-INLINE
		// A Tier B pull likewise learns its receipt geometry from that seal;
		// it grows before the first chunk, never after an object effect.
		// origin: S7 internal review CUT-CAPACITY F1 / hot-update S7 plan §2.1
		inline := e.value.Prepare.Intent.Tier == "a"
		if done, ok := e.sender.(interface{ Finalized() bool }); inline && ok && done.Finalized() {
			return io.EOF
		}
		if !e.sealed || inline {
			seal, sealErr := e.sender.Seal(deadline)
			if sealErr != nil {
				return sealErr
			}
			if sealErr = o.reserveSealed(e, seal); sealErr != nil {
				return sealErr
			}
			e.sealed = true
		}
		// A lost chunk ACK must return to pending-receipt reconciliation within
		// one control window, not consume the whole 300–2108s byte allowance.
		// Sealing/hashing above keeps the original byte deadline; pacing inside
		// Step is reentered against that same original start after a slice ends.
		// origin: S7 internal review PULL-B F9 / hot-update S7 plan §1.2 / §2.2 / T46
		attemptDeadline, boundErr := controlbudget.After(5 * time.Second)
		if boundErr != nil {
			return boundErr
		}
		err = e.sender.Step(min(deadline, attemptDeadline))
		// A published object has no further byte step; ending the runner keeps
		// it from spinning on successful no-ops until the byte deadline.
		// origin: hot-update S7 plan §4 / INTEG-D agent sender wiring
		if done, ok := e.sender.(interface{ Finalized() bool }); err == nil && ok && done.Finalized() {
			o.mu.Lock()
			e.published = true
			o.mu.Unlock()
			return io.EOF
		}
	} else if e.receiver != nil {
		err = e.receiver.Step(deadline)
	} else {
		return workerError(x.CodeOwnerRecovering)
	}
	// A step error is unresolved, never synthetic failed/complete. The real
	// receiver's outcome alone may start the retained settlement courier.
	if e.receiver != nil {
		state, freezeErr := receiverState(e.receiver)
		if freezeErr != nil {
			return errors.Join(err, freezeErr)
		}
		if state.Identity != e.value.Prepare.Identity {
			return workerError(x.CodeIntentConflict)
		}
		o.mu.Lock()
		e.value.Status.DurableOffset = state.Checkpoint.DurableOffset
		e.value.Status.ChunkIndex = state.Checkpoint.ChunkIndex
		e.value.Status.LastConsumedStreamSeq = state.Checkpoint.LastConsumedStreamSeq
		o.mu.Unlock()
		if state.Outcome.Outcome == x.OutcomeNotEntered && !state.GateClosed {
			return workerError(x.CodeCommitInflight)
		}
		if state.Outcome.Outcome != "" && state.Outcome.Outcome != x.OutcomeInflight {
			courierErr := o.ensureCourier(e, state.Outcome)
			// A push receiver stays beside its courier. Ensuring that existing
			// courier is not progress and must not erase its backoff wait.
			// origin: S7 FIX-CUT courier backoff spin / hot-update S7 plan §4 / T16
			if courierErr == nil && errors.Is(err, errCourierNotDue) {
				return err
			}
			return courierErr
		}
	}
	return err
}

// Apply holds the admitted callback and entry token until the actual receiver
// gate or courier ACK returns. A stale nonce can never address another entry.
// origin: hot-update S7 plan §4 / T18
func (o *owner) Apply(c x.Control, deadline x.Deadline) (x.Status, error) {
	if !xferimage.ValidWorkerControl(c) {
		return x.Status{}, workerError(x.CodeIntentConflict)
	}
	if c.Kind == x.ControlStart {
		if err := o.Start(c.Identity); err != nil {
			return x.Status{}, err
		}
		return o.Status(c.Identity)
	}
	if err := o.enter(deadline); err != nil {
		return x.Status{}, err
	}
	defer o.leave()
	o.mu.Lock()
	e := o.entries[key(c.Identity)]
	generation := o.state.Lineage.Generation
	o.mu.Unlock()
	if e == nil || e.value.Prepare.Identity != c.Identity {
		return x.Status{}, workerError(x.CodeIntentConflict)
	}
	// origin: S7 internal review rv-lifecycle F4
	// Byte IO owns the entry token; cancellation must reach its Stop first.
	if c.Kind == x.ControlCancel || c.Kind == x.ControlRevoke {
		if err := o.stopBytes(e, deadline); err != nil {
			return x.Status{}, err
		}
	}
	if err := o.lockEntry(e, deadline); err != nil {
		return x.Status{}, err
	}
	defer func() { <-e.token }()
	// Admission pins this generation until the callback joins. Thaw cannot
	// rewrite it underneath a late grant/receipt handler.
	if e.value.OwnerGeneration != generation {
		return x.Status{}, workerError(x.CodeOwnerRecovering)
	}
	o.mu.Lock()
	retained := o.entries[key(c.Identity)] == e
	o.mu.Unlock()
	if !retained {
		return x.Status{}, workerError(x.CodeResultUnknown)
	}
	if c.Kind == x.ControlReceipt {
		if err := o.cfg.Authority.Check(c.Identity, x.AccessSettle, deadline); err != nil {
			return x.Status{}, err
		}
	}
	var err error
	switch c.Kind {
	case x.ControlGrant:
		if e.value.GateClosed || e.receiver == nil {
			return x.Status{}, workerError(x.CodeCanceled)
		}
		if err = o.cfg.Authority.Check(c.Identity, x.AccessContinue, deadline); err == nil {
			err = e.receiver.Reserve(*c.Grant)
		}
		if err == nil {
			o.mu.Lock()
			e.value.Status.Phase = x.PhaseCommitReserved
			// A receiver that already reached EOF returned "awaits commit grant"
			// and its runner exited. The durable Grant is the event that lets it
			// proceed, so the same owner resumes it rather than leaving the
			// committed bytes parked until an unrelated Start.
			// origin: hot-update S7 plan §4 / INTEG-D outer commit adapter
			if !e.running && !o.closed && !o.stopped && !o.stopping {
				e.running = true
				e.done = make(chan struct{})
				go o.run(e)
			}
			o.mu.Unlock()
		}
	case x.ControlCancel, x.ControlRevoke:
		o.mu.Lock()
		e.value.GateClosed = true
		// origin: S7 internal review rv-lifecycle F5
		// Replays retain the first irreversible revocation, including its deadline.
		if !slices.ContainsFunc(o.state.Revocations, func(r x.Revocation) bool { return r.Identity == c.Identity }) {
			o.state.Revocations = append(o.state.Revocations, x.Revocation{Identity: c.Identity, Reason: c.Reason, At: e.byteDeadline})
		}
		o.mu.Unlock()
		if e.receiver != nil {
			if err == nil {
				var state x.ReceiverState
				state, err = receiverState(e.receiver)
				if err == nil && state.Outcome.Outcome == x.OutcomeNotEntered && !state.GateClosed {
					err = workerError(x.CodeCommitInflight)
				}
				if err == nil && state.Outcome.Outcome != "" && state.Outcome.Outcome != x.OutcomeInflight {
					err = o.ensureCourier(e, state.Outcome)
				}
			}
		}
		if e.receiver == nil && e.sender == nil && e.courier == nil {
			err = o.ensureCourier(e, x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled})
		}
	case x.ControlReceipt:
		if e.courier == nil {
			err = o.acceptRetainedReceipt(e, *c.Receipt, deadline)
			break
		}
		err = o.acceptReceipt(e, *c.Receipt, deadline)
	case x.ControlStart:
		return x.Status{}, workerError(x.CodeIntentConflict)
	default:
		return x.Status{}, workerError(x.CodeIntentConflict)
	}
	if err != nil {
		return x.Status{}, err
	}
	o.observe(x.Observation{Kind: x.ObserveControl, TransferID: c.Identity.TransferID, EntryNonce: c.Identity.EntryNonce, Generation: generation})
	return o.Status(c.Identity)
}

func (o *owner) ensureCourier(e *entry, outcome x.ReceiverOutcome) error {
	if e.courier != nil {
		// origin: S7 internal review rv-lifecycle F5
		// Cancel/revoke retries keep the original courier and return its status.
		return nil
	}
	if o.cfg.MakeCourier == nil {
		return workerError(x.CodeOwnerRecovering)
	}
	// Without an issued settlement capsule the courier still needs the
	// retained original owner to address; this origin-only binding names the
	// authenticated prepare's identity/origin and carries no credential.
	// origin: hot-update S7 plan §4 / §6.4 / INTEG-D courier canonical carrier
	capsule := x.SettlementCapsule{Identity: e.value.Prepare.Identity, Origin: e.value.Prepare.Origin}
	if e.value.Prepare.Settlement != nil {
		capsule = *e.value.Prepare.Settlement
	}
	courier, err := o.cfg.MakeCourier(x.CourierConfig{Base: o.cfg.Base, Identity: e.value.Prepare.Identity, Outcome: outcome, Capsule: capsule, Transport: o.transport})
	o.mu.Lock()
	e.courier = courier
	e.value.Status.Phase = x.PhaseTerminalPending
	e.value.Status.CleanupPending = outcome.TmpCleanupPending || (outcome.Outcome == x.OutcomeLanded && !outcome.ParentSynced)
	o.mu.Unlock()
	if err != nil || courier == nil {
		return errors.Join(workerError(x.CodeOwnerRecovering), err)
	}
	o.mu.Lock()
	if !e.running {
		e.running = true
		e.done = make(chan struct{})
		go o.run(e)
	}
	o.mu.Unlock()
	return nil
}

func (o *owner) acceptReceipt(e *entry, receipt x.TerminalReceipt, deadline x.Deadline) error {
	// Both transport-delivered and explicit controls go through the same exact
	// courier validation; there is no second path that invents terminal success.
	if err := e.courier.Acknowledge(receipt); err != nil {
		return err
	}
	o.mu.Lock()
	e.value.Status.Terminal = xferimage.CloneTransferReceipt(&receipt)
	e.value.Status.State = x.StatusTerminal
	e.value.Status.Phase = x.PhaseHistoryConfirmed
	e.value.Status.CleanupPending = true
	o.mu.Unlock()
	err := o.retireReceipt(e, deadline)
	// An explicit ACK can arrive on a retained courier without an active byte
	// runner. Keep local retirement executable even when its first Stop fails.
	// origin: S7 internal review rv-lifecycle F3
	if err != nil {
		o.mu.Lock()
		if !e.running && !o.stopping {
			e.running, e.done = true, make(chan struct{})
			go o.run(e)
		}
		o.mu.Unlock()
	}
	return err
}

func (o *owner) retireReceipt(e *entry, deadline x.Deadline) error {
	// History proof stops publication, but does not stand in for actual data
	// retirement. Failed stops keep the slot and its cleanup responsibility.
	state, err := e.courier.Freeze(deadline)
	if err != nil {
		return err
	}
	o.mu.Lock()
	xferimage.RetainWorkerOutcome(&o.state, x.CanonicalOutcome{Identity: state.Identity, Outcome: state.Outcome, Canonical: state.Canonical, Receipt: xferimage.CloneTransferReceipt(e.value.Status.Terminal)})
	o.mu.Unlock()
	if err := errors.Join(o.stopBytes(e, deadline), e.courier.Stop(deadline)); err != nil {
		o.mu.Lock()
		e.value.Status.Phase, e.value.Status.CleanupPending = x.PhaseCleanupPending, true
		o.mu.Unlock()
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	// Successful actor Stop proves its tmp, directory and reader retirement.
	e.value.Status.CleanupPending = false
	e.value.Status.Phase = x.PhaseSettled
	// ACK replays cannot extend this cache forever. Creation expiry is a
	// separate lower bound checked by the collector before forgetting it.
	if e.retention.ResultUntil.IsZero() {
		e.retention.ResultUntil = o.cfg.UTC().Add(resultRetention)
	}
	return nil
}

// acceptRetainedReceipt settles an entry that owns no courier: a pull sender
// or a preparation that never received a commit. The deciding broker's exact
// receipt is the only authority to retire it; a complete receipt for a sender
// that never finalized its object is a conflict, not a settlement. Without
// this, a finished sender would hold its source, writer and the session's
// single tier-B slot forever.
// origin: hot-update S7 plan §4 / §5.3 — S7 production activation
func (o *owner) acceptRetainedReceipt(e *entry, receipt x.TerminalReceipt, deadline x.Deadline) error {
	id := e.value.Prepare.Identity
	sum := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	if receipt.TerminalID != hex.EncodeToString(sum[:]) || (receipt.Kind != "complete" && receipt.Kind != "failed") ||
		receipt.HistoryStream == "" || receipt.HistorySeq == 0 || receipt.CanonicalDigest == "" || receipt.ReqID == "" {
		return workerError(x.CodeTerminalConflict)
	}
	o.mu.Lock()
	previous := e.value.Status.Terminal
	o.mu.Unlock()
	if previous != nil {
		if *previous != receipt {
			return workerError(x.CodeTerminalConflict)
		}
		return nil
	}
	if e.receiver != nil {
		// A receiver's outcome is settled only by its own courier.
		return workerError(x.CodeOwnerRecovering)
	}
	if e.sender != nil {
		if f, ok := e.sender.(interface{ Finalized() bool }); receipt.Kind == "complete" && (!ok || !f.Finalized()) {
			return workerError(x.CodeTerminalConflict)
		}
		if err := e.sender.Stop(deadline); err != nil {
			return err
		}
	} else if receipt.Kind == "complete" {
		return workerError(x.CodeTerminalConflict)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	e.value.GateClosed = true
	e.value.Status.Terminal = xferimage.CloneTransferReceipt(&receipt)
	e.value.Status.State, e.value.Status.Phase = x.StatusTerminal, x.PhaseSettled
	if e.retention.ResultUntil.IsZero() {
		e.retention.ResultUntil = o.cfg.UTC().Add(resultRetention)
	}
	o.signalLocked()
	return nil
}
