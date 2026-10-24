// Package agentupgrade sequences the agent's durable upgrade transaction.
// Runtime adapters own image/FD operations; the engine never opens a second
// journal, infers a running image from a pathname, or consumes sealed material.
// origin: hot-update S8 plan §2.2 / §3 / §11.2 P09
// Cut identity excludes journal revision; action identity survives recovery.
// Completion holds the registry fence across the durable write. Death proofs
// authenticate an independent observer separately from the dead subject.
// origin: S8 internal review rv-txn F03 F04 F05 F06 F07 F08 F09 F12
package agentupgrade

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type engine struct {
	store    upgradetx.Store
	runtime  upgradeport.RuntimePort
	registry upgradeobserve.Registry
	gate     chan struct{}
}

// New keeps the frozen constructor shape. An incomplete wiring refuses work
// at the operation boundary rather than advertising an empty success.
func New(store upgradetx.Store, runtime upgradeport.RuntimePort, registry upgradeobserve.Registry) upgradeport.Endpoint {
	return &engine{store: store, runtime: runtime, registry: registry, gate: make(chan struct{}, 1)}
}

var _ upgradeport.NewFunc = New
var _ upgradeport.Endpoint = (*engine)(nil)

// The sequencer is process-local, not the short store guard or domain lease.
// Waiting for it is cancellable; store CAS still fences other processes and
// callbacks from an earlier engine instance.
func (e *engine) enter(ctx context.Context) error {
	if ctx == nil {
		return refusal(upgradetx.CodeRejected, "caller context is required", false)
	}
	// origin: hot-update S8 plan §2.2. A typed nil port is still missing;
	// accepting Prepare with it would issue a grant that cannot be serviced.
	if portMissing(reflect.ValueOf(e.store)) || portMissing(reflect.ValueOf(e.runtime)) || portMissing(reflect.ValueOf(e.registry)) {
		return refusal(upgradetx.CodeNotWired, "agent upgrade ports are unavailable", false)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case e.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			e.leave()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *engine) leave() { <-e.gate }

func (e *engine) Prepare(ctx context.Context, key upgradetx.Key, req upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	if err := e.enter(ctx); err != nil {
		return upgradetx.Grant{}, err
	}
	defer e.leave()
	// origin: hot-update S8 plan §7.2. Legacy interruption is issued by its
	// dedicated broker policy, never inferred from a missing hot-mode field.
	if !agentKey(key) || req.Mode != upgradetx.ModeHot {
		return upgradetx.Grant{}, refusal(upgradetx.CodeRejected, "agent hot upgrade scope is required", false)
	}
	// Prepare is an inert, target-issued authorization. Only the store may
	// issue IDs, bind the actor/parent, or validate an absolute grant expiry.
	grant, err := e.store.Prepare(key, req)
	if err != nil {
		return grant, publicError(err, false)
	}
	return grant, nil
}

func (e *engine) Start(ctx context.Context, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradetx.View, error) {
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2
	if grant.SourceSHA == grant.TargetSHA {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "observation-only grants cannot activate an image", false)
	}
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	// origin: hot-update S8 plan §5.2. Equal bytes are not equal publication
	// authority: actor, nonce, issuer and parent slot must all remain bound.
	if !validGrant(grant) || !reflect.DeepEqual(artifact.Grant, grant) || artifact.SourceSHA != grant.SourceSHA || artifact.TargetSHA != grant.TargetSHA {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "artifact does not belong to this grant", false)
	}
	// origin: hot-update S8 plan §3.1 / §3.2. A retry is a read of the
	// accepted execution, even if the previous reply was lost before any
	// result was journaled. Corruption is never interpreted as absence.
	prior, err := e.store.Load(grant.Handle.Key, grant.Handle.TxID)
	if err == nil {
		if !reflect.DeepEqual(prior.Historical.Grant, grant) {
			return prior, refusal(upgradetx.CodeRejected, "accepted grant identity differs", true)
		}
		return e.query(grant.Handle.Key, grant.Handle.TxID, newID())
	}
	var txErr *upgradetx.Error
	if !errors.As(err, &txErr) || txErr.Code != upgradetx.CodeGrantUnknown {
		return upgradetx.View{}, publicError(err, false)
	}
	// origin: S8 internal review rv-txn F06
	// The sole writer binds verified paths before acceptance; publication is inert.
	// Path-free runtime receipts leave publication lookup to the registered
	// store directory and preflight port; they never invent image paths.
	// origin: S8 internal review rv-fault F1
	if writer, ok := e.store.(interface {
		BindPublication(upgradetx.Grant, upgradeartifact.Receipt) error
	}); ok && (artifact.SourcePath != "" || artifact.TargetPath != "" || artifact.ManifestDigest != "" || artifact.CanonicalPath != "") {
		if err := writer.BindPublication(grant, artifact); err != nil {
			return upgradetx.View{}, err
		}
	}
	proof, err := e.runtime.Preflight(ctx, grant, artifact)
	if err != nil {
		return upgradetx.View{}, publicError(err, false)
	}
	if err = checkPreflight(proof, grant, nil, false); err != nil {
		return upgradetx.View{}, err
	}
	observed, err := e.observe(grant.Handle, grant.Issuer, grant.ScopeDigest, newID())
	if err != nil || !observed.OwnerKnown || observed.RunningSHA != grant.SourceSHA || observed.Census.Stamp.Producer != grant.Issuer {
		return upgradetx.View{}, refusal(upgradetx.CodeDeferred, "running source owner could not be verified", false)
	}
	fence := upgradetx.Fence{Current: observed.Current.Handle, Owner: grant.Issuer, RunningSHA: observed.RunningSHA, PreviousSHA: observed.Current.SourceSHA}
	if observed.Current.Handle.TxID != "" {
		old, err := e.store.Load(grant.Handle.Key, observed.Current.Handle.TxID)
		if err != nil {
			return old, err
		}
		fence.PreviousSHA = old.Historical.RecordedPreviousSHA
	}
	record, err := e.store.Start(grant, fence)
	if err != nil {
		return upgradetx.View{}, publicError(err, false)
	}
	if !sameAttempt(current(record).Handle, grant.Handle) || current(record).Phase != upgradetx.PhasePrepared || current(record).Direction != upgradetx.DirectionForward || current(record).SourceSHA != grant.SourceSHA || current(record).TargetSHA != grant.TargetSHA {
		return upgradetx.View{Historical: record}, refusal(upgradetx.CodeUnsettled, "accepted attempt does not match the grant", true)
	}
	record, err = e.run(ctx, record, grant, artifact, upgradetx.Handle{})
	if err != nil {
		return e.failed(ctx, record, grant, err)
	}
	return e.query(record.Key, grant.Handle.TxID, newID())
}

func (e *engine) run(ctx context.Context, record upgradetx.Record, grant upgradetx.Grant, artifact upgradeartifact.Receipt, source upgradetx.Handle) (upgradetx.Record, error) {
	if err := ctx.Err(); err != nil {
		return record, err
	}
	if current(record).Direction == upgradetx.DirectionForward {
		publication, err := e.intent(&record, upgradetx.ActionPublish, "", "")
		if err != nil {
			return record, err
		}
		if err = e.result(&record, publication, digest(artifact), upgradetx.Event{Phase: upgradetx.PhaseStaged, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending}); err != nil {
			return record, err
		}
	} else if err := e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseStaged, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending}); err != nil {
		// Reverse reuses the previously pinned artifact. No new publish
		// effect or fabricated publication receipt is recorded.
		return record, err
	}
	proof, err := e.runtime.Preflight(ctx, grant, artifact)
	if err != nil {
		return record, err
	}
	a := current(record)
	if err = checkPreflight(proof, grant, &a.Scope, false); err != nil {
		return record, err
	}
	if err = ctx.Err(); err != nil {
		return record, err
	}
	if err = e.apply(&record, upgradetx.Event{Phase: upgradetx.PhaseReady, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending}); err != nil {
		return record, err
	}
	freeze, err := e.intent(&record, upgradetx.ActionFreeze, upgradetx.PhaseFreezing, upgradetx.CutPristine)
	if err != nil {
		return record, err
	}
	var cut upgradeport.Cut
	if a.Direction == upgradetx.DirectionReverse {
		// origin: hot-update S8 plan §8.1. ExportCurrent includes partial-Take
		// salvage and fences current admissions. The pristine input is never
		// fed to a reverse activation, including after a failed first effect.
		cut, err = e.runtime.ExportCurrent(current(record).Handle)
	} else {
		cut, err = e.runtime.Freeze(current(record).Handle)
	}
	if err != nil {
		return record, err
	}
	if !sameAttempt(cut.Attempt, a.Handle) || cut.Owner != grant.Issuer || cut.Sequence == 0 || len(cut.Descriptor) == 0 || upgradetx.ValidateExportScope(a.Scope, cut.Scope) != nil {
		return record, refusal(upgradetx.CodeUnsettled, "final cut identity or scope differs", true)
	}
	if err = e.result(&record, freeze, cutDigest(cut), upgradetx.Event{}); err != nil {
		return record, err
	}
	if a.Direction == upgradetx.DirectionReverse {
		// Salvage must revoke the retained forward generation before a third
		// generation can execute. Reusing its artifact does not revive it.
		retireAction, retireErr := e.intent(&record, upgradetx.ActionRetire, "", "")
		if retireErr != nil {
			return record, retireErr
		}
		// Retirement signs the original forward commitment, which may differ
		// from the reverse scope after late admissions. Never borrow the new
		// grant's digest to validate another direction's receipt.
		// origin: S8 internal review rv-txn F05 F09
		original, loadErr := e.store.Load(source.Key, source.TxID)
		if loadErr != nil {
			return record, loadErr
		}
		var sourceScope string
		for _, prior := range []upgradetx.Attempt{original.Historical.Forward, original.Historical.Reverse} {
			if sameAttempt(prior.Handle, source) {
				sourceScope, loadErr = upgradetx.ScopeDigest(prior.Scope)
				break
			}
		}
		if loadErr != nil || sourceScope == "" {
			return record, refusal(upgradetx.CodeUnsettled, "source retirement scope is unavailable", true)
		}
		released, retireErr := e.runtime.Retire(source)
		if retireErr != nil || !validReceipt(released, upgradetx.ReceiptOldGenerationReleased, source, grant.Issuer, grant.SourceSHA, "", sourceScope) || len(released.Census.OldOwners) != 0 {
			return record, refusal(upgradetx.CodeUnsettled, "forward generation retirement is unproven", true)
		}
		// origin: S8 internal review rv3-leaves F3
		// Fresh salvage evidence belongs to this reverse action. An earlier
		// forward release is immutable, even when its nonce/time differs.
		if record.Forward.Handle.TxID != "" && record.Forward.Released.Kind == "" {
			writer, ok := e.store.(interface {
				RecordForwardRelease(upgradetx.Handle, upgradetx.Receipt) (upgradetx.Record, error)
			})
			if !ok {
				return record, refusal(upgradetx.CodeNotWired, "forward retirement writer unavailable", true)
			}
			next, writeErr := writer.RecordForwardRelease(current(record).Handle, released)
			err = writeErr
			if err != nil {
				return record, err
			}
			record = next
		}
		if err = e.result(&record, retireAction, digest(released), upgradetx.Event{}); err != nil {
			return record, err
		}
	}
	proof, err = e.runtime.Preflight(ctx, grant, artifact)
	if err != nil {
		return record, err
	}
	if err = checkPreflight(proof, grant, &a.Scope, a.Direction == upgradetx.DirectionReverse); err != nil {
		return record, err
	}
	if err = ctx.Err(); err != nil {
		return record, err
	}
	// The adapter performs selector/dispatch/activation together. Their
	// intents all precede its first possible effect; lost ACKs may only be
	// recovered through observation and current export, never redispatch.
	selectAction, err := e.intent(&record, upgradetx.ActionSelect, "", "")
	if err != nil {
		return record, err
	}
	dispatch, err := e.intent(&record, upgradetx.ActionDispatch, "", "")
	if err != nil {
		return record, err
	}
	// origin: hot-update S8 plan §4.1 / §6.1. An error from Activate cannot
	// prove that no external effect occurred. Persist uncertainty first.
	activate, err := e.intent(&record, upgradetx.ActionActivate, upgradetx.PhaseActivating, upgradetx.CutPartialActive)
	if err != nil {
		return record, err
	}
	// The activation intent is durable: the requester learns that its Start was
	// accepted right before the exec would take this reply with it.
	// origin: hot-update S8 plan §3.1 / §7.1; S8 FIX-DESCRIBE (real-cut finding)
	activation, err := agentupgradeport.ActivateNotifying(agentupgradeport.WithActivationRecord(ctx, record), e.runtime, cut)
	if err != nil {
		return record, err
	}
	if !sameAttempt(activation.Attempt, a.Handle) || !validOwner(activation.Owner) || activation.RunningSHA != grant.TargetSHA || activation.Stage != upgradetx.CutActive && activation.Stage != upgradetx.CutPartialActive {
		return record, refusal(upgradetx.CodeUnsettled, "running activation proof differs", true)
	}
	for _, action := range []upgradetx.Action{selectAction, dispatch, activate} {
		if err = e.result(&record, action, digest(activation), upgradetx.Event{}); err != nil {
			return record, err
		}
	}
	return e.finish(record, activation.Owner)
}

var _ upgradeport.Describer = (*engine)(nil)

// Describe answers upgrade.describe from the live runtime. It is serialized
// with every other operation, so a description never interleaves a cut, and
// it writes nothing: Prepare remains the only grant issuer, and Start
// re-proves the source against /proc. The runtime must describe exactly the
// component the selector names (agentupgradeport.Describe checks it).
// origin: hot-update S8 plan §2.1 / §3.1; S8 FIX-DESCRIBE (P19 request R1)
func (e *engine) Describe(ctx context.Context, selector upgradetx.Key, nonce string) (upgradeport.Description, error) {
	if err := e.enter(ctx); err != nil {
		return upgradeport.Description{}, err
	}
	defer e.leave()
	d, err := agentupgradeport.Describe(e.runtime, selector, nonce)
	if err != nil {
		return upgradeport.Description{}, publicError(err, false)
	}
	return d, nil
}

func (e *engine) Query(ctx context.Context, key upgradetx.Key, txID, nonce string) (upgradetx.View, error) {
	if err := e.enter(ctx); err != nil {
		return upgradetx.View{}, err
	}
	defer e.leave()
	return e.query(key, txID, nonce)
}

func (e *engine) query(key upgradetx.Key, txID, nonce string) (upgradetx.View, error) {
	if !agentKey(key) || !hexValue(txID, 32) || !hexValue(nonce, 32) {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "query identity is invalid", false)
	}
	view, err := e.store.Load(key, txID)
	var unknown *upgradetx.Error
	if errors.As(err, &unknown) && unknown.Code == upgradetx.CodeGrantUnknown {
		if prepared, ok := e.store.(interface {
			Prepared(upgradetx.Key, string) (upgradetx.View, error)
		}); ok {
			view, err = prepared.Prepared(key, txID)
			if err == nil {
				return agentupgradeport.ObservePrepared(e.runtime, view, nonce)
			}
		}
	}
	if err != nil {
		return view, err
	}
	a := current(view.Historical)
	if view.Historical.Version != upgradetx.RecordVersion || view.Historical.Key != key || a.Handle.Key != key || !hexValue(a.Handle.TxID, 32) || !hexValue(a.Handle.CutID, 32) || a.Handle.Generation == 0 || view.Historical.Revision == 0 || view.Historical.Grant.Handle.TxID != txID && view.Historical.Forward.Handle.TxID != txID && view.Historical.Reverse.Handle.TxID != txID {
		return view, refusal(upgradetx.CodeStateCorrupt, "historical transaction identity differs", true)
	}
	view.Observation, err = e.observe(a.Handle, upgradetx.Owner{}, view.Historical.Grant.ScopeDigest, nonce)
	if err != nil {
		return view, err
	}
	// Reading a truthful unknown result succeeds. Wait/CLI policy decides
	// whether it is settled; historical receipts cannot assert current health.
	if !view.Observation.OwnerKnown || !view.Observation.Census.Known || !view.Observation.Census.Complete {
		view.Code, view.Detail = upgradetx.CodeUnsettled, "current owner or custody is unknown"
	}
	return view, nil
}

func (e *engine) observe(handle upgradetx.Handle, owner upgradetx.Owner, scope, nonce string) (upgradetx.Observation, error) {
	start := time.Now().UnixNano()
	stamp := upgradetx.Stamp{Attempt: handle, Producer: owner, Nonce: nonce, At: start, ScopeDigest: scope}
	o, err := e.runtime.Observe(stamp)
	if err != nil {
		return o, publicError(err, true)
	}
	// origin: hot-update S8 plan §4.2 / M15. Freshness is both a nonce and
	// a sampling interval; cached completed responses cannot satisfy Query.
	if o.Nonce != nonce || o.At < start || o.At > time.Now().UnixNano() || o.Census.Stamp.Nonce != nonce {
		return o, refusal(upgradetx.CodeUnsettled, "fresh observation proof is invalid", true)
	}
	return o, nil
}

func (e *engine) apply(record *upgradetx.Record, event upgradetx.Event) error {
	// origin: hot-update S8 plan §3.3. The loaded revision is consumed by
	// exactly one store CAS; malformed readback cannot advance this engine's
	// cursor or authorize a later effect after a lost durability receipt.
	handle := current(*record).Handle
	handle.Revision = record.Revision
	next, err := e.store.Apply(handle, event)
	if err != nil {
		return &durabilityError{publicError(err, true)}
	}
	if next.Key != record.Key || !sameAttempt(current(next).Handle, handle) || next.Revision <= record.Revision {
		return &durabilityError{refusal(upgradetx.CodeUnsettled, "durable writer returned an invalid revision", true)}
	}
	*record = next
	return nil
}

func current(record upgradetx.Record) upgradetx.Attempt {
	if record.Reverse.Handle.TxID != "" {
		return record.Reverse
	}
	return record.Forward
}

// Revision is a write CAS, not a cut identity: receipts from earlier durable
// revisions remain evidence for the same cut, never for the next transaction.
func sameAttempt(a, b upgradetx.Handle) bool {
	return a.Key == b.Key && a.TxID == b.TxID && a.CutID == b.CutID && a.Generation == b.Generation
}

func agentKey(key upgradetx.Key) bool {
	return key.Component == upgradetx.ComponentAgent && key.DomainID != "" && key.SID != "" && key.ConfiguredNID != ""
}

func portMissing(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validOwner(owner upgradetx.Owner) bool {
	return owner.BootID != "" && owner.PID > 0 && owner.StartTicks > 0 && hexValue(owner.InstanceID, 32)
}

func validGrant(g upgradetx.Grant) bool {
	return agentKey(g.Handle.Key) && hexValue(g.Handle.TxID, 32) && hexValue(g.Handle.CutID, 32) && g.Handle.Generation != 0 && g.Mode == upgradetx.ModeHot && validOwner(g.Issuer) && hexValue(g.SourceSHA, 64) && hexValue(g.TargetSHA, 64) && hexValue(g.ScopeDigest, 64) && hexValue(g.CapabilityDigest, 64)
}

func hexValue(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func newID() string {
	var bytes [16]byte
	// crypto/rand.Read cannot fail on supported Go platforms. It terminates
	// the process if the operating system cannot provide secure randomness.
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}

type cutEvidence struct {
	Attempt       upgradetx.Handle
	Owner         upgradetx.Owner
	Sequence      uint64
	DescriptorSHA string
}

// The closed input set prevents arbitrary runtime/private objects being
// serialized as business evidence. None of these DTOs contains secret state.
func digest[T interface {
	upgradeartifact.Receipt | upgradeport.Activation | upgradetx.Receipt | upgradetx.Attempt | upgradetx.Grant | cutEvidence
}](value T) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func cutDigest(cut upgradeport.Cut) string {
	// Only a digest enters the journal. Raw image/SS bytes and owned FD
	// objects remain in the sealed handoff owned by the runtime adapter.
	sum := sha256.Sum256(cut.Descriptor)
	return digest(cutEvidence{cut.Attempt, cut.Owner, cut.Sequence, hex.EncodeToString(sum[:])})
}

type durabilityError struct{ error }

func (e *durabilityError) Unwrap() error { return e.error }

func refusal(code, detail string, accepted bool) error {
	return &upgradetx.Error{Code: code, Detail: detail, Accepted: accepted}
}

func publicError(err error, accepted bool) error {
	// origin: hot-update S8 plan §7.1. Acceptance reported by the store must
	// survive a missing reply; temporary refusal codes cannot invite another
	// Start once execution authority may already have been consumed.
	var txErr *upgradetx.Error
	if errors.As(err, &txErr) {
		accepted = accepted || txErr.Accepted
		code := txErr.Code
		if accepted && (code == upgradetx.CodeDeferred || code == upgradetx.CodeRejected) {
			code = upgradetx.CodeUnsettled
		}
		// origin: hot-update S8 plan §7.1; S8 FIX-FIXTURES R2. The operator
		// needs the exact refusal reason (legacy_session_busy, an unreadable
		// epoch); only the closed runtime vocabulary crosses, never free text.
		return refusal(code, agentupgradeport.PublicReason(txErr.Detail), accepted || txErr.Accepted)
	}
	// Adapter errors may contain file paths or cryptographic state. Public
	// errors report the stage's uncertainty without serializing that text.
	if accepted {
		return refusal(upgradetx.CodeUnsettled, "accepted upgrade operation failed", true)
	}
	return refusal(upgradetx.CodeDeferred, "upgrade preflight failed", false)
}
