// runtime_port.go is the agent's production upgradeport.RuntimePort. The
// source half binds every reader to probed bytes, enumerates the carried
// runtime responsibility, freezes the real owners and dispatches the final
// re-export with the sealed upgrade-cut-v1 adjunct and the domain lease. The
// target half observes its own running image after import. The Agent only
// supplies package-level callbacks through Host; it gains no methods.
// A leaf of its own: the engine (agentupgrade) constructs it, it never imports
// the engine, and neither package crosses its structural thresholds.
// origin: hot-update S8 plan §2.2 / §4.2 / §5.1 / §6.1 / §11.2 P14 P17; S8 FIX-AGENTWIRE
package agentupgradeport

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"golang.org/x/sys/unix"
)

// Public refusal reasons. They travel as the wire Detail of upgrade_rejected
// (64) or upgrade_deferred (75); every other adapter text stays private.
// origin: hot-update S8 plan §7.1
const (
	ReasonImageABIUnsupported   = "image_abi_unsupported"
	ReasonUpgradeInProgress     = "upgrade_in_progress"
	ReasonFrozenTable           = "frozen_table"
	ReasonDomainCensus          = "domain_census_unavailable"
	ReasonIncarnationUnknown    = "authority_incarnation_unknown"
	ReasonRunningImageChanged   = "running_image_changed"
	ReasonArtifactMismatch      = "preflight_artifact_mismatch"
	ReasonArtifactUnavailable   = "artifact_unavailable"
	ReasonDomainLeaseHeld       = "domain_reservation_held"
	ReasonActivationUnavailable = "activation_unavailable"
)

// publicReasons is the closed set an engine refusal may carry on the wire:
// the runtime reasons above, the agent's documented deferred set and the
// pure reader-admission vocabulary. Anything else is replaced by a generic
// detail because adapter errors can name paths, identities or image data.
var publicReasons = func() map[string]bool {
	reasons := map[string]bool{ReasonImageABIUnsupported: true, ReasonUpgradeInProgress: true, ReasonFrozenTable: true,
		ReasonDomainCensus: true, ReasonIncarnationUnknown: true, ReasonRunningImageChanged: true, ReasonArtifactMismatch: true,
		ReasonArtifactUnavailable: true, ReasonDomainLeaseHeld: true, ReasonActivationUnavailable: true, "abi_late": true,
		// Describe's own refusal names no identity (upgradewire.DescriptionUnavailable).
		// origin: S8 FIX-DESCRIBE (P19 request R1)
		upgradewire.DescriptionUnavailable: true,
		// upgradecompat.CheckReaders / CheckEpochs reasons (pure policy words).
		"reader_capability_missing": true, "reader_domain_missing": true, "reader_role_mismatch": true,
		"reader_identity_invalid": true, "reader_declarations_invalid": true, "required_domains_invalid": true,
		"image_epoch_unknown": true, "image_epochs_invalid": true, "image_epoch_distance": true, "image_epoch_unreadable": true}
	for _, reason := range agentimage.DeferredReasons {
		reasons[reason] = true
	}
	return reasons
}()

// Image is one frozen cut of the complete current owner set. Prepare re-exports
// it through the agent's single checked funnel with the adjunct appended; the
// final bundle must pass both readers there. Thaw resumes the same owners.
type Image interface {
	Bundle() agentimage.Bundle
	Prepare(target string, adjunct handoff.Resource) (*handoff.Prepared, error)
	Thaw() error
}

// Host is the running agent's half. Callbacks are bounded local reads or the
// cut itself; none may call back into the engine or block on the network.
type Host struct {
	Key     upgradetx.Key
	Journal upgradetx.Store
	// Owner is this process: boot, PID, kernel start tick and the lineage
	// instance. Same-PID exec preserves all four; generation separates owners.
	Owner      func() (upgradetx.Owner, error)
	RunningSHA func() (string, error)
	// Source returns the running bytes' release and reader bound to sha.
	Source func(sha string) (string, agentimage.Capabilities, error)
	Probe  func(path string) (string, agentimage.Capabilities, error)
	// Candidate is the agent's retained-domain policy (CheckCandidateABI).
	Candidate func(agentimage.Capabilities) error
	// Deferrals returns busy reasons in the frozen order, including a cut
	// owned by another path; an empty result is not a census.
	Deferrals func() []agentimage.DeferredError
	Required  func() []string
	// Snapshot is a read-only fence of the runtime image and courier outbox.
	// Responsibility it cannot enumerate by identity is an error, never zero.
	Snapshot func() (execsession.Image, agentimage.CourierState, error)
	// Domains enumerates the actual flow/SS owners, including private retained
	// identities. Snapshot's blocked guard stays closed without this producer.
	// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2
	Domains func() (FlowSSState, error)
	// Transfers is the capable-transfer worker's census by identity
	// (transfer_census.go); like Snapshot it never reports what it cannot name.
	// origin: S8 FIX-DESCRIBE (transfer census)
	Transfers   func() ([]TransferEntry, error)
	Incarnation func() string
	Freeze      func(readers []agentimage.Capabilities) (Image, error)
	Exec        func(path string, prepared *handoff.Prepared) error
	// Lease defaults to upgradelock.Acquire; the exporter holds it from the
	// cut until exec replaces this process or the cut is thawed.
	Lease func(context.Context, upgradetx.Key) (upgradelock.Lease, error)
	// StaleAfter bounds how long a frozen cut may wait for Activate. An engine
	// that cannot journal its own failure (EROFS/ENOSPC after Freeze) never
	// calls Thaw; business must not stay frozen behind that unknown record.
	// Zero is upgradebudget.ComponentHandover.
	StaleAfter time.Duration
	// Target-only: the imported adjunct, the image stage and the control probe.
	Inherited *upgradecut.State
	Stage     func() upgradetx.CutStage
	Control   func(nonce string) error
}

type preflighted struct {
	target              string
	sourceSHA           string
	candidate, previous agentimage.Capabilities
}

type heldCut struct {
	// Parent authority follows the exact pre-freeze grant into the sealed adjunct.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
	parentToken string
	handle      upgradetx.Handle
	image       Image
	lease       upgradelock.Lease
	sequence    uint64
	timer       *time.Timer
	activating  bool // Activate owns the cut; the stale timer may not thaw it
}

type runtimePort struct {
	host     Host
	mu       sync.Mutex
	sequence uint64
	// One entry: the engine serializes transactions and a second Preflight
	// cannot succeed while another cut is held, so older entries are dead.
	ready       map[string]preflighted // by TxID; set only by a successful Preflight
	publication publicationEvidence
	probed      readerEvidence
	held        *heldCut
	// registry is the completion registry of the cut this process imported
	// (CutRegistry); its admission sequence stamps every target sample.
	registry upgradeobserve.Registry
	released map[string]upgradetx.Resource // monotonic settled/collected identities, carried by the next cut
	// activating is set only between a Start's early reply and the return of
	// its exec (activate): this process image is about to be replaced.
	// origin: S8 FIX-DESCRIBE (real-cut finding)
	activating atomic.Bool
}

var _ upgradeport.RuntimePort = (*runtimePort)(nil)

// NewRuntime returns the production adapter. Missing callbacks are refusals at
// the operation that needs them, never an empty census or success.
func NewRuntime(host Host) upgradeport.RuntimePort {
	return &runtimePort{host: host, ready: make(map[string]preflighted)}
}

func current(record upgradetx.Record) upgradetx.Attempt {
	if record.Reverse.Handle.TxID != "" {
		return record.Reverse
	}
	return record.Forward
}

func refusal(code, detail string, accepted bool) error {
	return &upgradetx.Error{Code: code, Detail: detail, Accepted: accepted}
}

func newID() string {
	var bytes [16]byte
	// crypto/rand.Read cannot fail on supported Go platforms.
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}

func hexValue(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func reject(reason string) error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: reason}
}

func deferral(reason string) error {
	return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: reason}
}

// deferredError keeps the documented busy reason of a typed agent deferral;
// any other cause is classified without echoing its private text.
func deferredError(err error) error {
	var deferred *agentimage.DeferredError
	var typed *upgradetx.Error
	switch {
	case errors.As(err, &typed):
		return typed
	case errors.As(err, &deferred) && publicReasons[deferred.Reason]:
		return deferral(deferred.Reason)
	case errors.Is(err, agentimage.ErrABIUnsupported):
		return reject("abi_late")
	}
	return deferral(ReasonDomainCensus)
}

func (p *runtimePort) Preflight(ctx context.Context, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	if ctx == nil || ctx.Err() != nil {
		return upgradeport.PreflightProof{}, deferral(ReasonActivationUnavailable)
	}
	h := p.host
	if h.Owner == nil || h.RunningSHA == nil || h.Source == nil || h.Probe == nil || h.Candidate == nil || h.Deferrals == nil ||
		h.Required == nil || h.Snapshot == nil || h.Incarnation == nil || h.Journal == nil {
		return upgradeport.PreflightProof{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent runtime adapter is incomplete"}
	}
	owner, err := h.Owner()
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	if grant.Handle.Key != h.Key || grant.Mode != upgradetx.ModeHot || grant.Issuer != owner {
		return upgradeport.PreflightProof{}, reject("preflight_identity_mismatch")
	}
	// origin: hot-update S8 plan §5.2 / §6.1. The source is the running bytes,
	// never the canonical pathname a co-hosted sibling may already have replaced.
	running, err := h.RunningSHA()
	if err != nil || running != grant.SourceSHA {
		return upgradeport.PreflightProof{}, deferral(ReasonRunningImageChanged)
	}
	record, started := p.record(grant.Handle)
	target, err := p.targetPath(grant, artifact, record, started)
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	sourceRelease, source, err := h.Source(running)
	if err != nil || source.SHA256 != running {
		return upgradeport.PreflightProof{}, deferral(ReasonRunningImageChanged)
	}
	// The candidate reader is the isolated probe of the exact target bytes.
	targetRelease, candidate, err := p.candidateReader(target, grant.TargetSHA)
	if err != nil {
		return upgradeport.PreflightProof{}, reject(ReasonImageABIUnsupported)
	}
	if candidate.SHA256 != grant.TargetSHA || sourceRelease != grant.SourceRelease || targetRelease != grant.TargetRelease {
		return upgradeport.PreflightProof{}, reject(ReasonArtifactMismatch)
	}
	// Forward records the running source as previous; a reverse's target is
	// the recorded previous image itself (upgradestore.Start). Before its
	// acceptance a reverse is recognizable by its absent publication receipt.
	reverse := !started && artifact == (upgradeartifact.Receipt{}) ||
		started && current(record.Historical).Direction == upgradetx.DirectionReverse
	previous, previousRelease := source, sourceRelease
	if reverse {
		previous, previousRelease = candidate, targetRelease
	}
	// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 FIX-FIXTURES R2. Every
	// S8 cut carries the required adjunct, so a bare-exec reader (ABI 0) is
	// never a target, even for an empty image. The agent's retained-domain
	// policy then runs on the probed candidate; an epoch refusal keeps the
	// admission reason, a missing domain the image_abi_unsupported class.
	if candidate.ABI < 1 {
		return upgradeport.PreflightProof{}, reject(ReasonImageABIUnsupported)
	}
	if err := h.Candidate(candidate); err != nil {
		var typed *upgradetx.Error
		if errors.As(err, &typed) && publicReasons[typed.Detail] {
			return upgradeport.PreflightProof{}, reject(typed.Detail)
		}
		return upgradeport.PreflightProof{}, reject(ReasonImageABIUnsupported)
	}
	required := h.Required()
	if err := upgradecompat.CheckReaders(source.Reader(sourceRelease, upgradetx.ComponentAgent), candidate.Reader(targetRelease, upgradetx.ComponentAgent),
		previous.Reader(previousRelease, upgradetx.ComponentAgent), required); err != nil {
		return upgradeport.PreflightProof{}, err
	}
	// The adjunct carries this agent's creation identity; an unregistered
	// agent has none to carry and cannot be cut.
	if incarnation := h.Incarnation(); !hexValue(incarnation, 64) {
		return upgradeport.PreflightProof{}, deferral(ReasonIncarnationUnknown)
	}
	p.mu.Lock()
	held := p.held
	p.mu.Unlock()
	var bundle *agentimage.Bundle
	if held != nil && upgradetx.SameCut(held.handle, grant.Handle) {
		// The gate after Freeze reads the frozen re-export: late revocations
		// and retained terminals are refused here, before the activation intent.
		b := held.image.Bundle()
		if err := agentimage.CheckBundleCapabilities(b, candidate, previous); err != nil {
			return upgradeport.PreflightProof{}, reject("abi_late")
		}
		bundle = &b
	} else if reasons := h.Deferrals(); len(reasons) != 0 {
		return upgradeport.PreflightProof{}, deferral(reasons[0].Reason)
	}
	generation, custody := p.custody(grant.Handle, record, started)
	rows, err := p.rows(bundle, owner, generation, custody)
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	stamp := p.stamp(grant.Handle, owner, newID(), grant.ScopeDigest, 0)
	census, err := p.census(stamp, rows, record, started)
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	p.mu.Lock()
	p.ready = map[string]preflighted{grant.Handle.TxID: {target: target, sourceSHA: running, candidate: candidate, previous: previous}}
	p.mu.Unlock()
	readers := sha256Hex(struct {
		Source, Candidate, Previous agentimage.Capabilities
		Required                    []string
	}{source, candidate, previous, required})
	config := sha256Hex(struct {
		Key                          upgradetx.Key
		SourceRelease, TargetRelease string
		ImageABI                     int
		ProtoVersion, ReleaseVersion string
	}{h.Key, sourceRelease, targetRelease, proto.ImageABIVersion, proto.SubjectVersionToken, proto.ReleaseVersion})
	return upgradeport.PreflightProof{Grant: grant, ReadersDigest: readers, ConfigDigest: config, Census: census}, nil
}

// record reads the accepted attempt for this cut. Absence before Start is not
// an error: the first Preflight runs before any acceptance.
func (p *runtimePort) record(handle upgradetx.Handle) (upgradetx.View, bool) {
	view, err := p.host.Journal.Load(p.host.Key, handle.TxID)
	if err != nil || !upgradetx.SameCut(current(view.Historical).Handle, handle) {
		return upgradetx.View{}, false
	}
	return view, true
}

// custody names whose generation the carried rows belong to. The source keeps
// the attempt's SourceGeneration until exec; only the process that imported
// this exact cut owns the target generation.
func (p *runtimePort) custody(handle upgradetx.Handle, view upgradetx.View, started bool) (uint64, string) {
	if inherited := p.host.Inherited; inherited != nil && upgradetx.SameCut(inherited.Attempt, handle) {
		return handle.Generation, "target"
	}
	if started {
		return current(view.Historical).SourceGeneration, "source"
	}
	if handle.Generation == 0 {
		return 0, "source" // an invalid stamp: its rows fail validation, never wrap
	}
	return handle.Generation - 1, "source"
}

// stamp binds one sample. A requested sequence (the completion registry's own
// probe and domain observers, which hold its gate) is echoed. Otherwise the
// importing target names its registry's current admission sequence: the
// barrier that the Control and Released receipts and the final completion
// census must all share (upgradetx.ValidateCompletionBarrier). Only a process
// that imported no cut (the source) uses its private sample counter; a
// private counter at the target failed every real completion as "stale".
// origin: hot-update S8 plan §4.2 / M04; S8 FIX-DESCRIBE (real-cut finding)
func (p *runtimePort) stamp(handle upgradetx.Handle, owner upgradetx.Owner, nonce, scope string, requested uint64) upgradetx.Stamp {
	p.mu.Lock()
	p.sequence++
	sequence, registry := p.sequence, p.registry
	p.mu.Unlock()
	if requested != 0 {
		sequence = requested
	} else if current, ok := upgradeobserve.Sequence(registry); ok {
		sequence = current
	}
	return upgradetx.Stamp{Attempt: handle, Producer: owner, Nonce: nonce, Sequence: sequence, At: time.Now().UnixNano(), ScopeDigest: scope}
}

// census binds rows to one stamp. After acceptance the expected digest is the
// sealed scope's; before it, the digest commits to this observation's rows.
func (p *runtimePort) census(stamp upgradetx.Stamp, rows []upgradetx.Resource, view upgradetx.View, started bool) (upgradetx.Census, error) {
	expected := sha256Hex(rows)
	if started {
		var err error
		if expected, err = upgradetx.ExpectedDigest(current(view.Historical).Scope); err != nil {
			return upgradetx.Census{}, err
		}
	}
	return upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: rows, ExpectedDigest: expected}, nil
}

func (p *runtimePort) Freeze(handle upgradetx.Handle) (upgradeport.Cut, error) {
	return p.cut(handle, upgradetx.CutPristine)
}

// ExportCurrent is the same fence over the CURRENT owners. Partial or active
// imported state is never replaced by the original pristine bundle.
func (p *runtimePort) ExportCurrent(handle upgradetx.Handle) (upgradeport.Cut, error) {
	stage := upgradetx.CutPristine
	if p.host.Stage != nil {
		if current := p.host.Stage(); current == upgradetx.CutPartialActive || current == upgradetx.CutActive {
			stage = current
		}
	}
	return p.cut(handle, stage)
}

func (p *runtimePort) cut(handle upgradetx.Handle, stage upgradetx.CutStage) (upgradeport.Cut, error) {
	h := p.host
	if h.Freeze == nil || h.Owner == nil {
		return upgradeport.Cut{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent runtime adapter is incomplete"}
	}
	view, started := p.record(handle)
	p.mu.Lock()
	ready, preflightDone := p.ready[handle.TxID]
	busy := p.held != nil
	p.mu.Unlock()
	if !started || !preflightDone || busy {
		return upgradeport.Cut{}, deferral(ReasonUpgradeInProgress)
	}
	owner, err := h.Owner()
	if err != nil {
		return upgradeport.Cut{}, err
	}
	// origin: hot-update S8 plan §5.1. The domain reservation is held from the
	// cut until exec hands its OFD to the adjunct or the cut is thawed.
	acquire := h.Lease
	if acquire == nil {
		acquire = func(ctx context.Context, key upgradetx.Key) (upgradelock.Lease, error) {
			return upgradelock.Acquire(ctx, key, view.Historical.Grant.ParentTxID)
		}
	}
	// The reservation attempt is bounded by the cut's safe-point budget.
	// ctx-none: the frozen RuntimePort.Freeze signature has no caller context
	ctx, cancel := context.WithTimeout(context.Background(), upgradebudget.SafePointWait)
	lease, err := acquire(ctx, h.Key)
	cancel()
	if err != nil || lease == nil {
		return upgradeport.Cut{}, deferral(ReasonDomainLeaseHeld)
	}
	image, err := h.Freeze([]agentimage.Capabilities{ready.candidate, ready.previous})
	if err != nil {
		_ = lease.Close()
		return upgradeport.Cut{}, deferredError(err)
	}
	a := current(view.Historical)
	b := image.Bundle()
	generation, custody := p.custody(handle, view, started)
	rows, err := p.rows(&b, owner, generation, custody)
	var scope upgradetx.Scope
	var descriptor []byte
	if err == nil {
		scope, err = exportScope(a.Scope, rows)
	}
	if err == nil {
		descriptor, err = agentimage.Encode(b)
	}
	if err != nil {
		_ = image.Thaw()
		_ = lease.Close()
		return upgradeport.Cut{}, deferredError(err)
	}
	stale := h.StaleAfter
	if stale <= 0 {
		stale = upgradebudget.ComponentHandover
	}
	p.mu.Lock()
	p.sequence++
	held := &heldCut{parentToken: view.Historical.Grant.ParentTxID, handle: handle, image: image, lease: lease, sequence: p.sequence}
	p.held = held
	// The callback takes p.mu, so it cannot observe the cut before this
	// registration completes.
	held.timer = time.AfterFunc(stale, func() { p.abandon(held) })
	p.mu.Unlock()
	return upgradeport.Cut{Attempt: handle, Owner: owner, Stage: stage, Descriptor: descriptor, Scope: scope, Sequence: held.sequence}, nil
}

// abandon thaws a cut nobody activated or thawed within its budget. The
// journal keeps whatever it recorded; a late Activate then finds no cut.
// origin: hot-update S8 plan §3.2 / §8.2 F3 F4; S8 FIX-AGENTWIRE
func (p *runtimePort) abandon(held *heldCut) {
	p.mu.Lock()
	if p.held != held || held.activating {
		p.mu.Unlock()
		return
	}
	p.held = nil
	p.mu.Unlock()
	_ = held.image.Thaw()
	_ = held.lease.Close()
}

// exportScope keeps the sealed authority (ID, participants, domains) and adds
// the current carried identities, so the adjunct names every original object.
func exportScope(original upgradetx.Scope, rows []upgradetx.Resource) (upgradetx.Scope, error) {
	scope := original
	scope.Expected = append([]upgradetx.Resource{}, original.Expected...)
	seen := make(map[string]bool, len(scope.Expected))
	for _, row := range scope.Expected {
		seen[string(row.Domain)+"\x00"+row.ID] = true
	}
	for _, row := range rows {
		if !seen[string(row.Domain)+"\x00"+row.ID] {
			scope.Expected = append(scope.Expected, row)
		}
	}
	return upgradetx.CanonicalScope(scope)
}

// Activate dispatches the final re-export with the adjunct. It returns only
// when exec did not replace the process: the cut is then thawed and the lease
// released, while the durable partial_active intent keeps Current for recovery.
// origin: hot-update S8 plan §4.1 / §5.1 / §6.1 / M08
func (p *runtimePort) Activate(cut upgradeport.Cut) (upgradeport.Activation, error) {
	return p.activate(cut, nil)
}

// activate runs notify (the request's early reply, ActivateNotifying) only
// once nothing but the exec itself remains.
func (p *runtimePort) activate(cut upgradeport.Cut, notify func()) (upgradeport.Activation, error) {
	h := p.host
	p.mu.Lock()
	held, ready := p.held, p.ready[cut.Attempt.TxID]
	if held == nil || !upgradetx.SameCut(held.handle, cut.Attempt) || held.sequence != cut.Sequence || h.Exec == nil {
		p.mu.Unlock()
		return upgradeport.Activation{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: ReasonActivationUnavailable, Accepted: true}
	}
	// From here Activate is the cut's only thawer: the stale timer stands down.
	held.activating = true
	held.timer.Stop()
	p.mu.Unlock()
	release := func() {
		p.mu.Lock()
		if p.held == held {
			p.held = nil
		}
		p.mu.Unlock()
		_ = held.image.Thaw()
		_ = held.lease.Close()
	}
	state := upgradecut.State{ParentToken: held.parentToken, Attempt: cut.Attempt, SourceSHA: ready.sourceSHA, TargetSHA: ready.candidate.SHA256,
		Authorities: []upgradecut.Authority{{SID: h.Key.SID, Incarnation: h.Incarnation()}}, Owner: cut.Owner, Scope: cut.Scope}
	// The exporter keeps its own lease FD until exec: Prepare duplicates the
	// borrowed copy, so the reservation survives both success and failure.
	duplicate, err := dupLease(held.lease)
	if err != nil {
		release()
		return upgradeport.Activation{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: ReasonActivationUnavailable, Accepted: true}
	}
	adjunct, err := upgradecut.Pack(state, map[uint32]*os.File{state.LockFD: duplicate})
	var prepared *handoff.Prepared
	if err == nil {
		prepared, err = held.image.Prepare(ready.target, adjunct)
	}
	_ = duplicate.Close()
	if err != nil {
		release()
		return upgradeport.Activation{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "abi_late", Accepted: true}
	}
	// From here until exec returns, every other request is refused at once
	// (ActivatingRefusal) instead of waiting behind this Start to be lost.
	p.activating.Store(true)
	if notify != nil {
		notify()
	}
	// A successful exec never returns. Any return, including a hook that
	// reports success without replacing the process, left the source running.
	execErr := h.Exec(ready.target, prepared)
	p.activating.Store(false)
	_ = prepared.Close()
	release()
	// Keep the initiating errno inspectable by local recovery without putting
	// private adapter text into the public failure vocabulary.
	// origin: S8 internal review activation-and-route-migration F1
	return upgradeport.Activation{}, errors.Join(&upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: ReasonActivationUnavailable, Accepted: true}, execErr)
}

// dupLease lends Pack a duplicate of the reservation's OFD. Closing the
// duplicate after Prepare never issues LOCK_UN on the exporter's lease.
func dupLease(lease upgradelock.Lease) (*os.File, error) {
	fd, err := unix.FcntlInt(lease.FD(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "upgrade-lease-export"), nil
}

func (p *runtimePort) Thaw(handle upgradetx.Handle) error {
	p.mu.Lock()
	held := p.held
	if held != nil && upgradetx.SameCut(held.handle, handle) && !held.activating {
		p.held = nil
		held.timer.Stop()
	} else {
		held = nil
	}
	p.mu.Unlock()
	if held == nil {
		return nil // nothing of this cut is frozen here, or Activate owns it
	}
	err := held.image.Thaw()
	if closeErr := held.lease.Close(); err == nil {
		err = closeErr
	}
	return err
}

// Observe is a fresh local sample. Control is signed only by the process that
// imported this exact cut and passes a real nonce probe; the source never
// certifies the target's readiness.
func (p *runtimePort) Observe(stamp upgradetx.Stamp) (upgradetx.Observation, error) {
	return p.observe(stamp, true)
}

// observe samples once; control asks for the external control probe. The
// completion registry's domain observers and Retire read only the census and
// running bytes: probing control there sent a dozen broker round trips per
// completion (one per frozen domain) and dominated a target's time to
// completed. The registry's own probe still runs the full Observe.
// origin: hot-update S8 plan §4.2; S8 FIX-DESCRIBE (real-cut finding)
func (p *runtimePort) observe(stamp upgradetx.Stamp, control bool) (upgradetx.Observation, error) {
	h := p.host
	if h.Owner == nil || h.RunningSHA == nil || h.Journal == nil || h.Snapshot == nil {
		return upgradetx.Observation{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent runtime adapter is incomplete"}
	}
	start := time.Now().UnixNano()
	owner, err := h.Owner()
	if err != nil {
		return upgradetx.Observation{}, err
	}
	running, err := h.RunningSHA()
	if err != nil {
		return upgradetx.Observation{}, deferral(ReasonRunningImageChanged)
	}
	view, started := p.record(stamp.Attempt)
	if !started && !control {
		view, started = retiringForwardRecord(p, stamp.Attempt)
	}
	if !started {
		if latest, err := h.Journal.Load(h.Key, ""); err == nil {
			view = latest
		}
	}
	a := current(view.Historical)
	o := upgradetx.Observation{Nonce: stamp.Nonce, Current: a, OwnerKnown: true, RunningSHA: running, EffectiveSHA: running}
	if view.Historical.EffectiveSHA != "" {
		o.EffectiveSHA = view.Historical.EffectiveSHA
	}
	target := h.Inherited != nil && started && upgradetx.SameCut(h.Inherited.Attempt, a.Handle) && running == a.TargetSHA
	// The image's activation refines only an attempt still activating; once
	// the journal recorded retiring or released, an older image stage must not
	// downgrade it (a completed cut otherwise never reads as completed).
	// origin: S8 FIX-DESCRIBE (real-cut finding)
	if target && h.Stage != nil && (a.CutStage == upgradetx.CutPartialActive || a.CutStage == upgradetx.CutActive) {
		if stage := h.Stage(); stage == upgradetx.CutPartialActive || stage == upgradetx.CutActive {
			o.Current.CutStage = stage
		}
	}
	s := p.stamp(stamp.Attempt, owner, stamp.Nonce, stamp.ScopeDigest, stamp.Sequence)
	s.At = max(s.At, start)
	generation, custody := p.custody(stamp.Attempt, view, started)
	p.mu.Lock()
	held := p.held
	p.mu.Unlock()
	var bundle *agentimage.Bundle
	if heldImageMatches(p, held, stamp.Attempt, running) {
		b := held.image.Bundle()
		bundle = &b
	}
	rows, rowsErr := p.rows(bundle, owner, generation, custody)
	census, err := p.census(s, rows, view, started)
	if err != nil || rowsErr != nil {
		// Unknown custody is a truthful observation, not an adapter failure.
		census = upgradetx.Census{Stamp: s}
	}
	// One observation has one sampling time: the census stamp's. A later
	// clock read made every fresh completed query unprovable (upgradetx.Completed).
	// origin: S8 FIX-DESCRIBE (real-cut finding)
	o.Census, o.At = census, s.At
	// A fresh Control receipt serves the completion still in progress. A
	// settled attempt is reported exactly as journaled: replacing its durable
	// receipt with one stamped by this query's nonce made every completed
	// query read as "historical evidence answering a fresh observation".
	// origin: S8 FIX-DESCRIBE (real-cut finding)
	settled := a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack
	if control && target && !settled && census.Known && h.Control != nil && h.Control(stamp.Nonce) == nil {
		o.Current.Control = upgradetx.Receipt{Kind: upgradetx.ReceiptControlReady, Stamp: census.Stamp, Known: true, Complete: true,
			RunningSHA: running, Census: census, EvidenceDigest: sha256Hex(census)}
	}
	return o, nil
}

// Retire is signed only after the exec: the importing process proves that no
// row remains with the source generation. The source cannot certify itself.
func (p *runtimePort) Retire(handle upgradetx.Handle) (upgradetx.Receipt, error) {
	h := p.host
	view, started := p.record(handle)
	// Two independent repairs cover the reverse that continues in the forward's
	// record: the read-only forward projection first, then the held-export proof.
	// origin: S8 internal review activation-and-route-migration F10 + TAGGED-SUITE-SWEEP-A F4
	if !started {
		view, started = retiringForwardRecord(p, handle)
	}
	if h.Inherited == nil || !upgradetx.SameCut(h.Inherited.Attempt, handle) {
		return upgradetx.Receipt{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "old generation release is unproven", Accepted: true}
	}
	if !started {
		return retireInheritedFromReverse(p, handle)
	}
	o, err := p.observe(upgradetx.Stamp{Attempt: handle, Producer: h.Inherited.Owner, Nonce: newID(), ScopeDigest: view.Historical.Grant.ScopeDigest}, false)
	if err != nil || !o.Census.Known || o.RunningSHA != current(view.Historical).TargetSHA {
		return upgradetx.Receipt{}, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "old generation release is unproven", Accepted: true}
	}
	return upgradetx.Receipt{Kind: upgradetx.ReceiptOldGenerationReleased, Stamp: o.Census.Stamp, Known: true, Complete: true,
		RunningSHA: o.RunningSHA, Census: o.Census, EvidenceDigest: sha256Hex(o.Census)}, nil
}

func sha256Hex(value any) string {
	data, _ := json.Marshal(value) // closed evidence DTOs; no secret state
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// PublicReason keeps a closed-vocabulary refusal reason for the engine's wire
// projection and replaces any other detail with a generic phrase.
func PublicReason(detail string) string {
	if publicReasons[detail] {
		return detail
	}
	return "upgrade port operation failed"
}
