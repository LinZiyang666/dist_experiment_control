// Package brokerupgrade coordinates the broker's durable upgrade transaction.
// The store is the only journal writer; the runtime adapter owns the current
// database, image, admission gate and inherited lease. This package never opens
// a database or restores a backup, including on rollback.
// origin: hot-update S8 plan §3 / §4 / §11.2 P10
// The collector owns the final admission fence through the store CAS. Boot
// reconciles immutable action identities with fresh runtime observations;
// Archive requires independently observed death and complete released custody.
// origin: S8 internal review rv-txn F03 F04 F05 F06 F08 F09 F12
package brokerupgrade

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type engine struct {
	// Never hold a store guard while calling the runtime. The process sequencer
	// rejects competing mutations; Store CAS also fences other endpoint instances.
	mu       sync.Mutex
	store    upgradetx.Store
	runtime  upgradeport.RuntimePort
	registry upgradeobserve.Registry
}

// New binds one component's store and current runtime. It creates no workers,
// timers or fallback state directory. Missing ports remain a refusal at use.
func New(store upgradetx.Store, runtime upgradeport.RuntimePort, registry upgradeobserve.Registry) upgradeport.Endpoint {
	return &engine{store: store, runtime: runtime, registry: registry}
}

var _ upgradeport.NewFunc = New

func (e *engine) enter(ctx context.Context, key upgradetx.Key) error {
	if err := e.check(ctx, key); err != nil {
		return err
	}
	if !e.mu.TryLock() {
		return refusal(upgradetx.CodeDeferred, "broker upgrade operation is in progress")
	}
	return nil
}

func (e *engine) check(ctx context.Context, key upgradetx.Key) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key.Component != upgradetx.ComponentBroker {
		return refusal(upgradetx.CodeUnsupportedComponent, "broker endpoint requires a broker key")
	}
	if key.DomainID == "" || key.ConfigPath == "" {
		return refusal(upgradetx.CodeRejected, "broker domain and configuration identity are required")
	}
	if e.store == nil || e.runtime == nil || e.registry == nil {
		return refusal(upgradetx.CodeNotWired, "broker upgrade ports are unavailable")
	}
	return nil
}

var _ upgradeport.Describer = (*engine)(nil)

// Describe answers upgrade.describe from the live runtime under the same
// sequencer as every mutation, so a description never interleaves a cut. It
// writes nothing; the runtime must describe exactly the broker the selector
// names, and an undescribable runtime refuses with its own public reason.
// origin: hot-update S8 plan §2.1 / §3.1; S8 FIX-DESCRIBE (P19 request R1)
func (e *engine) Describe(ctx context.Context, selector upgradetx.Key, nonce string) (upgradeport.Description, error) {
	if err := ctx.Err(); err != nil {
		return upgradeport.Description{}, err
	}
	if selector != (upgradetx.Key{Component: upgradetx.ComponentBroker}) {
		return upgradeport.Description{}, refusal(upgradetx.CodeUnsupportedComponent, "broker endpoint describes only a broker")
	}
	if e.store == nil || e.runtime == nil || e.registry == nil {
		return upgradeport.Description{}, refusal(upgradetx.CodeNotWired, "broker upgrade ports are unavailable")
	}
	if !e.mu.TryLock() {
		return upgradeport.Description{}, refusal(upgradetx.CodeDeferred, "broker upgrade operation is in progress")
	}
	defer e.mu.Unlock()
	describer, ok := e.runtime.(upgradeport.RuntimeDescriber)
	if !ok {
		return upgradeport.Description{}, refusal(upgradetx.CodeNotWired, "broker runtime cannot describe itself")
	}
	d, err := describer.DescribeRuntime(nonce)
	if err != nil {
		return upgradeport.Description{}, err
	}
	if d.Key.Component != upgradetx.ComponentBroker || d.Nonce != nonce {
		return upgradeport.Description{}, refusal(upgradetx.CodeDeferred, "target_description_unavailable")
	}
	return d, nil
}

func (e *engine) Prepare(ctx context.Context, key upgradetx.Key, req upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	if err := e.enter(ctx, key); err != nil {
		return upgradetx.Grant{}, err
	}
	defer e.mu.Unlock()
	// Legacy dispatch has a separate causal-proof owner. A missing mode cannot
	// silently select its interrupting path through this hot engine.
	if req.Mode != upgradetx.ModeHot {
		return upgradetx.Grant{}, refusal(upgradetx.CodeRejected, "broker hot upgrade requires explicit hot mode")
	}
	return e.store.Prepare(key, req)
}

func (e *engine) Start(ctx context.Context, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradetx.View, error) {
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2
	if grant.SourceSHA == grant.TargetSHA {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "observation-only grants cannot activate an image")
	}
	if err := e.enter(ctx, grant.Handle.Key); err != nil {
		return upgradetx.View{}, err
	}
	defer e.mu.Unlock()
	// origin: hot-update S8 plan §3.1 / §3.2
	// Accepted retries are observations only, even after an intent lost its
	// result. Replaying Freeze or dispatch could consume the same FD twice.
	prior, err := e.store.Load(grant.Handle.Key, grant.Handle.TxID)
	if err == nil && active(prior.Historical).Handle.TxID != "" {
		if prior.Historical.Grant != grant {
			return prior, refusal(upgradetx.CodeGrantUnknown, "grant differs from the accepted transaction")
		}
		return e.query(ctx, grant.Handle.Key, grant.Handle.TxID, nonce())
	}
	if err != nil && !hasCode(err, upgradetx.CodeGrantUnknown) {
		return prior, err
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
	proof, err := e.preflight(ctx, grant, artifact)
	if err != nil {
		return upgradetx.View{}, err
	}
	stamp := makeStamp(grant.Handle, grant.Issuer, grant.ScopeDigest, nonce())
	observed, err := e.runtime.Observe(stamp)
	if err != nil {
		return upgradetx.View{}, err
	}
	sourceStamp := stamp
	if observed.Current.Handle.TxID != "" {
		previous, err := e.store.Load(grant.Handle.Key, observed.Current.Handle.TxID)
		if err != nil {
			return previous, err
		}
		sourceStamp.ScopeDigest = previous.Historical.Grant.ScopeDigest
	}
	if !fresh(observed, sourceStamp) || observed.RunningSHA != grant.SourceSHA ||
		observed.Census.Stamp.Producer != grant.Issuer || !upgradetx.SameResources(observed.Census.Resources, proof.Census.Resources) {
		return upgradetx.View{}, refusal(upgradetx.CodeDeferred, "current source or ownership changed after preflight")
	}
	fence := upgradetx.Fence{Current: observed.Current.Handle, Owner: grant.Issuer, RunningSHA: observed.RunningSHA}
	if observed.Current.Handle.TxID != "" {
		old, err := e.store.Load(grant.Handle.Key, observed.Current.Handle.TxID)
		if err != nil {
			return old, err
		}
		fence.PreviousSHA = old.Historical.RecordedPreviousSHA
	}
	record, err := e.store.Start(grant, fence)
	if err != nil {
		return upgradetx.View{}, err
	}
	return e.drive(ctx, record, grant, artifact)
}

// Preflight is the integration boundary for artifact.Verify, reader/schema
// checks, full-fleet reservations and lease ownership. It must be side-effect
// free and must validate the two immutable slots, not a pathname's current SHA.
func (e *engine) preflight(ctx context.Context, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	if grant.Mode != upgradetx.ModeHot || !validSHA(grant.SourceSHA) || !validSHA(grant.TargetSHA) ||
		!validHandle(grant.Handle) || !knownOwner(grant.Issuer) || grant.ScopeDigest == "" || grant.CapabilityDigest == "" ||
		artifact.Grant != grant || artifact.SourceSHA != grant.SourceSHA || artifact.TargetSHA != grant.TargetSHA {
		return upgradeport.PreflightProof{}, refusal(upgradetx.CodeRejected, "invalid broker grant or artifact binding")
	}
	proof, err := e.runtime.Preflight(ctx, grant, artifact)
	if err != nil {
		return proof, err
	}
	if proof.Grant != grant || proof.ReadersDigest == "" || proof.ConfigDigest == "" ||
		!proof.Census.Known || !proof.Census.Complete || proof.Census.Admitting != 0 ||
		!sameAttempt(proof.Census.Stamp.Attempt, grant.Handle) || proof.Census.Stamp.Producer != grant.Issuer ||
		proof.Census.Stamp.ScopeDigest != grant.ScopeDigest || proof.Census.ExpectedDigest == "" {
		return proof, refusal(upgradetx.CodeDeferred, "broker preflight proof is incomplete")
	}
	return proof, nil
}

func (e *engine) Query(ctx context.Context, key upgradetx.Key, txID, queryNonce string) (upgradetx.View, error) {
	if err := e.check(ctx, key); err != nil {
		return upgradetx.View{}, err
	}
	return e.query(ctx, key, txID, queryNonce)
}

// A query samples again (same nonce, under the caller's deadline) when a
// concurrent writer moved the record during its observation. Query is not
// serialized with mutations (P10), and an imported target journals each
// completion step while a ctl waits: a single sample racing a journal write
// is not unknown health, yet an unknown answer ends every waiting ctl as 74.
// origin: hot-update S8 plan §7.1; S8 BROKER-EXPORTER R-11
const queryRevisionAttempts = 20

// transientObservation is the runtime's retryable refusal for a cut window
// (a non-accepted deferral): the query returns it as its own refusal, which a
// waiting ctl asks again, never as an unknown current view.
func transientObservation(err error) bool {
	var typed *upgradetx.Error
	return errors.As(err, &typed) && typed.Code == upgradetx.CodeDeferred && !typed.Accepted
}

func (e *engine) query(ctx context.Context, key upgradetx.Key, txID, queryNonce string) (upgradetx.View, error) {
	if queryNonce == "" || len(queryNonce) > 256 {
		return upgradetx.View{}, refusal(upgradetx.CodeRejected, "a bounded fresh query nonce is required")
	}
	// An unknown answer keeps the history first read: a writer that keeps
	// moving the record cannot substitute its later revision for it.
	var first upgradetx.View
	for attempt := 1; ; attempt++ {
		view, err := e.store.Load(key, txID)
		if hasCode(err, upgradetx.CodeGrantUnknown) {
			if prepared, ok := e.store.(interface {
				Prepared(upgradetx.Key, string) (upgradetx.View, error)
			}); ok {
				view, err = prepared.Prepared(key, txID)
				if err == nil {
					return e.observePrepared(view, queryNonce)
				}
			}
		}
		if err != nil {
			return view, err
		}
		if attempt == 1 {
			first = view
		}
		a := active(view.Historical)
		stamp := makeStamp(a.Handle, view.Historical.Grant.Issuer, view.Historical.Grant.ScopeDigest, queryNonce)
		observation, err := e.runtime.Observe(stamp)
		if transientObservation(err) {
			return first, err
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil || !fresh(observation, stamp) {
			// Historical completion stays immutable. A query with no current proof
			// still returns that history, explicitly separated from unknown health.
			first.Observation = upgradetx.Observation{Nonce: queryNonce, At: stamp.At}
			first.Code, first.Detail = upgradetx.CodeUnsettled, "current broker observation is unavailable"
			return first, nil //nolint:nilerr // Observation failure is reported in View.Code while preserving queryable history.
		}
		latest, err := e.store.Load(key, txID)
		if err == nil && latest.Historical.Revision == view.Historical.Revision {
			view.Observation = observation
			return view, nil
		}
		if err == nil && ctx.Err() == nil && attempt < queryRevisionAttempts {
			continue
		}
		first.Observation = upgradetx.Observation{Nonce: queryNonce, At: stamp.At}
		first.Code, first.Detail = upgradetx.CodeUnsettled, "transaction changed during observation"
		// Readback failure is reported in View.Code while preserving the original historical view.
		return first, nil
	}
}

func (e *engine) CloseWindow(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	if err := e.enter(ctx, expected.Key); err != nil {
		return upgradetx.View{}, err
	}
	defer e.mu.Unlock()
	metadata, err := e.store.CloseWindow(expected)
	if err != nil {
		return upgradetx.View{}, err
	}
	view, err := e.query(ctx, expected.Key, expected.TxID, nonce())
	view.Metadata = metadata
	return view, err
}

func (e *engine) Archive(ctx context.Context, expected upgradetx.Handle) (upgradetx.View, error) {
	if err := e.enter(ctx, expected.Key); err != nil {
		return upgradetx.View{}, err
	}
	defer e.mu.Unlock()
	view, err := e.loadExpected(expected)
	if err != nil {
		return view, err
	}
	// origin: hot-update S8 plan §8.1
	// RuntimePort has no independent death-proof operation. Observe/Retire
	// cannot manufacture Reaped/NoOwnedFDs from PID absence or an empty count.
	observer, ok := e.runtime.(interface {
		ObserveDeath(upgradetx.Stamp) (upgradetx.DeathProof, error)
	})
	if !ok {
		return view, accepted(refusal(upgradetx.CodeUnsettled, "independent owner death and custody proof is required for archive"))
	}
	a := active(view.Historical)
	owner := view.Historical.Grant.Issuer
	if a.Control.Known {
		owner = a.Control.Stamp.Producer
	}
	scope, err := upgradetx.ScopeDigest(a.Scope)
	if err != nil {
		return view, err
	}
	stamp := makeStamp(expected, owner, scope, nonce())
	death, err := observer.ObserveDeath(stamp)
	// origin: S8 internal review rv-txn F12
	if err != nil || death.Stamp.Nonce != stamp.Nonce || death.Stamp.At < stamp.At || upgradetx.ValidateDeath(a, owner, death, time.Now().UnixNano(), int64(upgradebudget.RequestLatency)) != nil {
		return view, accepted(refusal(upgradetx.CodeUnsettled, "owner death or custody is unproven"))
	}
	return e.store.Archive(expected, death)
}

func (e *engine) loadExpected(expected upgradetx.Handle) (upgradetx.View, error) {
	view, err := e.store.Load(expected.Key, expected.TxID)
	if err == nil && (!sameAttempt(active(view.Historical).Handle, expected) || view.Historical.Revision != expected.Revision) {
		err = refusal(upgradetx.CodeDeferred, "transaction fence is stale")
	}
	return view, err
}

func active(r upgradetx.Record) upgradetx.Attempt {
	if r.Reverse.Handle.TxID != "" {
		return r.Reverse
	}
	return r.Forward
}

func handle(r upgradetx.Record) upgradetx.Handle {
	h := active(r).Handle
	h.Revision = r.Revision
	return h
}

func sameAttempt(a, b upgradetx.Handle) bool {
	return a.Key == b.Key && a.TxID == b.TxID && a.CutID == b.CutID && a.Generation == b.Generation
}

func validHandle(h upgradetx.Handle) bool {
	return h.TxID != "" && h.CutID != "" && h.Generation != 0 && h.Revision != 0
}

func knownOwner(o upgradetx.Owner) bool {
	return o.BootID != "" && o.PID > 0 && o.StartTicks != 0 && o.InstanceID != ""
}

func validSHA(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

func makeStamp(h upgradetx.Handle, owner upgradetx.Owner, scope, n string) upgradetx.Stamp {
	return upgradetx.Stamp{Attempt: h, Producer: owner, Nonce: n, At: time.Now().UnixNano(), ScopeDigest: scope}
}

func fresh(o upgradetx.Observation, s upgradetx.Stamp) bool {
	c := o.Census
	return o.Nonce == s.Nonce && o.At >= s.At && o.At <= time.Now().UnixNano() && o.OwnerKnown &&
		validSHA(o.RunningSHA) && c.Known && c.Complete && c.Admitting == 0 && c.ExpectedDigest != "" &&
		c.Stamp.Nonce == s.Nonce && c.Stamp.At >= s.At && c.Stamp.At <= o.At &&
		c.Stamp.Attempt == s.Attempt && c.Stamp.ScopeDigest == s.ScopeDigest && knownOwner(c.Stamp.Producer)
}

func nonce() string {
	var b [16]byte
	// crypto/rand.Read either fills the buffer or terminates the process; a
	// zero/clock-derived fallback must never mint reusable execution identities.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func digest(v any) string {
	// Only the neutral, non-secret contract values reach this helper. Opaque
	// cut descriptors and auxiliary files are deliberately excluded.
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func refusal(code, detail string) error {
	return &upgradetx.Error{Code: code, Detail: detail}
}

func accepted(err error) error {
	var txErr *upgradetx.Error
	if errors.As(err, &txErr) {
		copyErr := *txErr
		copyErr.Accepted = true
		return &copyErr
	}
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "accepted broker upgrade needs current-state recovery", Accepted: true}
}

func hasCode(err error, code string) bool {
	var txErr *upgradetx.Error
	return errors.As(err, &txErr) && txErr.Code == code
}
