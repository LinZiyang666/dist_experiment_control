// runtime.go is the broker's production upgradeport.RuntimePort. The
// source half binds every reader to probed bytes, enumerates what the broker
// holds by identity (census.go), reserves the domain lease, and arms the S6
// store cut with the S8 plan (cut.go): the immutable target slot, the
// published source slot as rollback reader, and the sealed upgrade-cut-v1
// adjunct carrying the transaction, the complete session authority map and
// the lease. The S6 pipeline owns the real freeze, store close, export and
// same-PID exec, exactly as for any broker image cut. The target half
// (observe.go) observes its own running image after import, signs Control
// only after a real request crosses its business subscriptions, and
// certifies Released only as the importing process.
//
// The adapter is its own package, as the agent's is (agentupgradeport): the
// engine (internal/brokerupgrade) owns the journal sequencer and binds this
// port through upgradeport.RuntimePort; nothing here imports the engine.
//
// Until the Broker attaches its port (Attach), every cut step still refuses
// before acceptance with a public reason, so a registered broker that is not
// running (or a unit fixture) can bind its journal without reporting a cut as
// frozen, activated or released.
// origin: hot-update S8 plan §2.2 / §4.2 / §5.1 / §6.1 / §11.2 P15 P17;
// S8 FIX-DESCRIBE (P19 request R4); S8 BROKER-EXPORTER R-11
package brokerupgradeport

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// RuntimeHost is the running broker's half known at boot. Owner and
// RunningSHA are local reads of this process (kernel identity and
// /proc/self/exe bytes). Source binds this process's own reader declarations
// to the running bytes; Reader probes another executable's bytes (a candidate
// slot, a recorded pin) through the isolated version probe; Candidate
// describes the executable staged at the registered Canonical path.
type RuntimeHost struct {
	Key        upgradetx.Key
	Journal    upgradetx.Store
	Owner      func() (upgradetx.Owner, error)
	RunningSHA func() (string, error)
	Source     func(sha string) (upgradecompat.Reader, error)
	Reader     func(path string) (upgradecompat.Reader, error)
	Canonical  string
	Candidate  func(path string) (*upgradetx.CandidateDescription, error)
	// Inherited is the verified adjunct of the cut this process imported.
	Inherited *upgradecut.State
	// Lease defaults to upgradelock.Acquire; the source holds it from Freeze
	// until the exec hands its OFD to the adjunct or the cut is released.
	Lease func(context.Context, upgradetx.Key) (upgradelock.Lease, error)
	// StaleAfter bounds how long a reserved cut may wait for Activate (zero:
	// upgradebudget.ComponentHandover): an engine that cannot journal its own
	// failure never calls Thaw, and the reservation must not outlive it.
	StaleAfter time.Duration
}

// BrokerPort is the running Broker's half, attached once the Broker exists.
// Facts is a bounded read-only sample of its owners; DB is its current store;
// Arm requests the S6 store cut and returns once the final image is armed
// (nil) or the cut refused; Control sends one real request through the
// business subscriptions and returns nil only for its answer.
type BrokerPort struct {
	Facts   func() (BrokerFacts, error)
	DB      func() *sql.DB
	Arm     func(controlbudget.Deadline) error
	Control func(nonce string) error
	// Released independently proves a missing original allocation's sealed
	// hard revoke and joined owners; a missing callback proves nothing.
	Released func(domain, id, origin string) bool
}

type preflighted struct {
	target, previous, sourceSHA string
}

type heldCut struct {
	handle     upgradetx.Handle
	lease      upgradelock.Lease
	sequence   uint64
	timer      *time.Timer
	activating bool // Activate owns the cut; the stale timer may not release it
}

type brokerRuntime struct {
	host     RuntimeHost
	mu       sync.Mutex
	sequence uint64
	ready    map[string]preflighted // by TxID; set only by a successful Preflight
	held     *heldCut
	// registry is the completion registry of the cut this process imported
	// (CutRegistry); its admission sequence stamps every target sample.
	registry upgradeobserve.Registry
	port     atomic.Pointer[BrokerPort]
	armed    atomic.Pointer[armedCut]
	// probed is the last candidate's probed declarations, by the digest its
	// slot was verified against (see candidateReader).
	probed      upgradecompat.Reader
	publication publicationEvidence
}

var (
	_ upgradeport.RuntimePort      = (*brokerRuntime)(nil)
	_ upgradeport.RuntimeDescriber = (*brokerRuntime)(nil)
)

// NewRuntime returns the broker runtime adapter. Missing callbacks are
// refusals at the operation that needs them, never an empty observation.
func NewRuntime(host RuntimeHost) upgradeport.RuntimePort {
	return &brokerRuntime{host: host, ready: make(map[string]preflighted)}
}

// Attach installs the running Broker's port into a runtime NewRuntime built;
// any other runtime is left as it is.
func Attach(runtime upgradeport.RuntimePort, port BrokerPort) {
	if rt := runtimeOf(runtime); rt != nil {
		rt.port.Store(&port)
	}
}

// runtimeOf is the adapter behind a bound runtime, or nil for another one.
func runtimeOf(runtime upgradeport.RuntimePort) *brokerRuntime {
	rt, _ := runtime.(*brokerRuntime)
	return rt
}

func cutUnavailable() error {
	return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: ReasonBrokerCutUnavailable}
}

func deferral(reason string) error {
	return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: reason}
}
func reject(reason string) error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: reason}
}

// typedOrCensus keeps a typed refusal and classifies anything else as the
// census gap, never echoing private adapter text (paths, identities).
func typedOrCensus(err error) error {
	var typed *upgradetx.Error
	if errors.As(err, &typed) {
		return typed
	}
	return deferral(ReasonDomainCensus)
}

// Preflight binds the grant to this process, its running bytes and the
// verified publication, probes the candidate and checks all three readers in
// both directions, and returns an identity census of the current owners. It
// has no effect: the reservation is taken by Freeze, the cut by Activate.
func (p *brokerRuntime) Preflight(ctx context.Context, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	h, port := p.host, p.port.Load()
	if port == nil || h.Owner == nil || h.RunningSHA == nil || h.Source == nil || h.Reader == nil || h.Journal == nil {
		return upgradeport.PreflightProof{}, cutUnavailable()
	}
	if ctx == nil || ctx.Err() != nil {
		return upgradeport.PreflightProof{}, deferral(ReasonActivationUnavailable)
	}
	owner, err := h.Owner()
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	if grant.Handle.Key != h.Key || grant.Mode != upgradetx.ModeHot || grant.Issuer != owner {
		return upgradeport.PreflightProof{}, reject("preflight_identity_mismatch")
	}
	// origin: hot-update S8 plan §5.2 / §6.1. The source is the running bytes,
	// never the canonical pathname a newer staging may already have replaced.
	running, err := h.RunningSHA()
	if err != nil || running != grant.SourceSHA {
		return upgradeport.PreflightProof{}, deferral(ReasonRunningImageChanged)
	}
	view, started := p.record(grant.Handle)
	target, previous, err := p.paths(grant, artifact, view, started)
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	source, err := h.Source(running)
	if err != nil || source.SHA256 != running {
		return upgradeport.PreflightProof{}, deferral(ReasonRunningImageChanged)
	}
	candidate, err := p.candidateReader(target, grant.TargetSHA)
	if err != nil {
		return upgradeport.PreflightProof{}, reject(ReasonImageABIUnsupported)
	}
	if candidate.SHA256 != grant.TargetSHA || source.Release != grant.SourceRelease || candidate.Release != grant.TargetRelease {
		return upgradeport.PreflightProof{}, reject(ReasonArtifactMismatch)
	}
	// A forward records the running source as previous; a reverse's target is
	// the recorded previous image itself (upgradestore.Start).
	reverse := !started && artifact.TargetPath == "" || started && active(view.Historical).Direction == upgradetx.DirectionReverse
	previousReader := source
	if reverse {
		previousReader = candidate
	}
	if p.busy(grant.Handle) {
		return upgradeport.PreflightProof{}, deferral(ReasonUpgradeInProgress)
	}
	facts, err := p.facts(port)
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	required := requiredDomains(facts)
	if err := upgradecompat.CheckReaders(source, candidate, previousReader, required); err != nil {
		return upgradeport.PreflightProof{}, err
	}
	generation, custody := p.custody(grant.Handle, view, started)
	rows, err := censusRows(facts, owner, generation, custody)
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	stamp := p.stamp(grant.Handle, owner, nonce(), grant.ScopeDigest, 0)
	census, err := p.census(stamp, rows, view, started)
	if err != nil {
		return upgradeport.PreflightProof{}, err
	}
	p.mu.Lock()
	p.ready = map[string]preflighted{grant.Handle.TxID: {target: target, previous: previous, sourceSHA: running}}
	p.mu.Unlock()
	readers := digest(struct {
		Source, Candidate, Previous upgradecompat.Reader
		Required                    []string
	}{source, candidate, previousReader, required})
	config := digest(struct {
		Key                          upgradetx.Key
		SourceRelease, TargetRelease string
		ImageABI                     int
		ProtoVersion                 string
	}{h.Key, source.Release, candidate.Release, proto.ImageABIVersion, proto.SubjectVersionToken})
	return upgradeport.PreflightProof{Grant: grant, ReadersDigest: readers, ConfigDigest: config, Census: census}, nil
}

// candidateReader probes the target slot once per digest. A Start preflights
// three times inside one request budget (before acceptance, after staging,
// after the final export), and each probe executes the candidate; its
// declarations are a function of its bytes, which paths re-verified against
// the grant's digest in this very call, so a repeated probe of the same
// digest adds latency and no evidence. A probe that does not bind that
// digest is never kept.
// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11 (real-cut finding)
func (p *brokerRuntime) candidateReader(path, sha string) (upgradecompat.Reader, error) {
	p.mu.Lock()
	cached := p.probed
	p.mu.Unlock()
	if cached.SHA256 == sha && sha != "" {
		return cached, nil
	}
	reader, err := p.host.Reader(path)
	if err == nil && reader.SHA256 == sha {
		p.mu.Lock()
		p.probed = reader
		p.mu.Unlock()
	}
	return reader, err
}

// requiredDomains is the S6 store cut's own reader requirement (the same set
// its preflight probes for), so both preflights refuse the same readers.
func requiredDomains(f BrokerFacts) []string {
	domains := []string{"broker-image-v1", "flow-v1"}
	if f.Transfers {
		domains = append(domains, "transfer-v1")
	}
	return domains
}

// paths binds the artifact to this grant. A forward Start supplies the
// publication receipt; a reverse resolves the recorded pins, never a pathname
// of matching bytes from another publication. The previous path is the S6
// rollback reader of the target image: the published source slot.
func (p *brokerRuntime) paths(grant upgradetx.Grant, artifact upgradeartifact.Receipt, view upgradetx.View, started bool) (string, string, error) {
	if artifact.TargetPath != "" {
		if err := p.verifyPublication(artifact, grant, started); err != nil {
			// An accepted grant may expire between gates; only the exact
			// receipt this adapter already verified for it keeps its pins.
			// Changed bytes, permissions or authority invalidate that evidence.
			// origin: S8 internal review upgrade-maketest F5
			return "", "", reject(ReasonArtifactMismatch)
		}
		if artifact.SourcePath == "" {
			return "", "", reject(ReasonArtifactMismatch)
		}
		return artifact.TargetPath, artifact.SourcePath, nil
	}
	pins := view.Metadata.Pins
	if !started {
		latest, err := p.host.Journal.Load(p.host.Key, grant.Handle.TxID)
		if err != nil {
			latest, err = p.host.Journal.Load(p.host.Key, "")
		}
		if err != nil {
			return "", "", deferral(ReasonArtifactUnavailable)
		}
		pins = latest.Metadata.Pins
	}
	target := pinned(pins, grant.TargetSHA)
	if target == "" {
		return "", "", deferral(ReasonArtifactUnavailable)
	}
	previous := pinned(pins, grant.SourceSHA)
	if previous == "" {
		previous = target
	}
	return target, previous, nil
}

// pinned returns the absolute pin named by sha whose bytes still hash to it.
func pinned(pins []string, sha string) string {
	for _, pin := range pins {
		if filepath.IsAbs(pin) && filepath.Base(pin) == sha {
			if got, err := upgradeartifact.FileSHA(pin); err == nil && got == sha {
				if info, err := os.Stat(pin); err == nil && info.Mode().IsRegular() {
					return pin
				}
			}
		}
	}
	return ""
}

// record reads the accepted attempt for this cut. Absence before Start is not
// an error: the first Preflight runs before any acceptance.
func (p *brokerRuntime) record(h upgradetx.Handle) (upgradetx.View, bool) {
	view, err := p.host.Journal.Load(p.host.Key, h.TxID)
	if err != nil || !upgradetx.SameCut(active(view.Historical).Handle, h) {
		return upgradetx.View{}, false
	}
	return view, true
}

// busy reports another cut reserved or armed in this process.
func (p *brokerRuntime) busy(h upgradetx.Handle) bool {
	p.mu.Lock()
	held := p.held
	p.mu.Unlock()
	return held != nil && !upgradetx.SameCut(held.handle, h) || p.armed.Load() != nil
}

// custody names whose generation the rows belong to. The source keeps the
// attempt's SourceGeneration until exec; only the process that imported this
// exact cut owns the target generation.
func (p *brokerRuntime) custody(h upgradetx.Handle, view upgradetx.View, started bool) (uint64, string) {
	if inherited := p.host.Inherited; inherited != nil && upgradetx.SameCut(inherited.Attempt, h) {
		return h.Generation, "target"
	}
	if started {
		return active(view.Historical).SourceGeneration, "source"
	}
	// origin: S8 internal review roll-recovery-stall F6
	// Cancelled candidates consume an allocation, not a running generation.
	// Describe and the next Start retain that source; pre-acceptance samples
	// must agree instead of inventing the cancelled target as a live owner.
	if p.host.Journal != nil {
		if latest, err := p.host.Journal.Load(p.host.Key, ""); err == nil && upgradetx.CancellationSettled(latest.Historical) {
			return active(latest.Historical).SourceGeneration, "source"
		}
	}
	if h.Generation == 0 {
		return 0, "source" // an invalid stamp: its rows fail validation, never wrap
	}
	return h.Generation - 1, "source"
}

// facts samples the Broker and completes it with the session authority map
// read from the current store. Neither half is ever read as an empty broker.
func (p *brokerRuntime) facts(port *BrokerPort) (BrokerFacts, error) {
	if port == nil || port.Facts == nil || port.DB == nil {
		return BrokerFacts{}, deferral(ReasonDomainCensus)
	}
	facts, err := port.Facts()
	if err != nil {
		return BrokerFacts{}, typedOrCensus(err)
	}
	if facts.Authorities, err = SessionAuthorities(port.DB()); err != nil {
		return BrokerFacts{}, err
	}
	return facts, nil
}

// stamp binds one sample. A requested sequence (the completion registry's own
// probe and domain observers, which hold its gate) is echoed. Otherwise the
// importing target names its registry's current admission sequence — the
// barrier its Control and Released receipts and the final completion census
// share — and a process that imported no cut uses its private counter.
func (p *brokerRuntime) stamp(h upgradetx.Handle, owner upgradetx.Owner, n, scope string, requested uint64) upgradetx.Stamp {
	p.mu.Lock()
	p.sequence++
	sequence, registry := p.sequence, p.registry
	p.mu.Unlock()
	if requested != 0 {
		sequence = requested
	} else if current, ok := upgradeobserve.Sequence(registry); ok {
		sequence = current
	}
	return upgradetx.Stamp{Attempt: h, Producer: owner, Nonce: n, Sequence: sequence, At: time.Now().UnixNano(), ScopeDigest: scope}
}

// census binds rows to one stamp. After acceptance the expected digest is the
// sealed scope's; before it, the digest commits to this observation's rows.
func (p *brokerRuntime) census(stamp upgradetx.Stamp, rows []upgradetx.Resource, view upgradetx.View, started bool) (upgradetx.Census, error) {
	rows = p.releasedRows(stamp, rows, view, started)
	expected := digest(rows)
	if started {
		var err error
		if expected, err = upgradetx.ExpectedDigest(active(view.Historical).Scope); err != nil {
			return upgradetx.Census{}, err
		}
	}
	return upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: rows, ExpectedDigest: expected}, nil
}

// Freeze reserves the domain and seals the scope of the current owners. The
// broker's business keeps running until Activate arms the S6 store cut, whose
// own freeze closes admission, joins every store user and exports the store
// it closed — so no request can fall between the sealed scope and the image
// (a later admission is carried by the image and named by the target's
// census). The reservation is held until the exec or the cut's release.
func (p *brokerRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	port := p.port.Load()
	if port == nil || p.host.Owner == nil {
		return upgradeport.Cut{}, cutUnavailable()
	}
	view, started := p.record(h)
	p.mu.Lock()
	_, ready := p.ready[h.TxID]
	reserved := p.held != nil
	p.mu.Unlock()
	if !started || !ready || reserved || p.armed.Load() != nil {
		return upgradeport.Cut{}, deferral(ReasonUpgradeInProgress)
	}
	acquire := p.host.Lease
	if acquire == nil {
		acquire = func(ctx context.Context, key upgradetx.Key) (upgradelock.Lease, error) {
			return upgradelock.Acquire(ctx, key, view.Historical.Grant.ParentTxID)
		}
	}
	// The reservation attempt is bounded by the cut's safe-point budget.
	// ctx-none: the frozen RuntimePort.Freeze signature has no caller context
	ctx, cancel := context.WithTimeout(context.Background(), upgradebudget.SafePointWait)
	lease, err := acquire(ctx, p.host.Key)
	cancel()
	if err != nil || lease == nil {
		return upgradeport.Cut{}, deferral(ReasonDomainLeaseHeld)
	}
	cut, err := p.export(h, view, port, lease)
	if err != nil {
		_ = lease.Close()
		return upgradeport.Cut{}, err
	}
	stale := p.host.StaleAfter
	if stale <= 0 {
		stale = upgradebudget.ComponentHandover
	}
	p.mu.Lock()
	held := &heldCut{handle: h, lease: lease, sequence: cut.Sequence}
	p.held = held
	// The callback takes p.mu, so it cannot observe the cut before this
	// registration completes.
	held.timer = time.AfterFunc(stale, func() { p.abandon(held) })
	p.mu.Unlock()
	return cut, nil
}

// ExportCurrent re-exports the CURRENT owners of the reserved cut at the
// engine's latest handle. Nothing pristine is replayed: the S6 image is made
// at the exec from the store it closes, and this scope names its identities.
func (p *brokerRuntime) ExportCurrent(h upgradetx.Handle) (upgradeport.Cut, error) {
	port := p.port.Load()
	if port == nil || p.host.Owner == nil {
		return upgradeport.Cut{}, cutUnavailable()
	}
	view, started := p.record(h)
	p.mu.Lock()
	held := p.held
	p.mu.Unlock()
	if !started || held == nil || !upgradetx.SameCut(held.handle, h) || held.activating {
		return upgradeport.Cut{}, deferral(ReasonUpgradeInProgress)
	}
	cut, err := p.export(h, view, port, held.lease)
	if err != nil {
		return upgradeport.Cut{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.held != held || held.activating {
		return upgradeport.Cut{}, deferral(ReasonUpgradeInProgress)
	}
	held.handle, held.sequence = h, cut.Sequence
	return cut, nil
}

// export seals one scope over the current owners' rows: the sealed authority
// (ID, participants, domains) plus every carried identity. The descriptor is
// the cut's non-secret plan; the S6 image itself is built at the exec.
func (p *brokerRuntime) export(h upgradetx.Handle, view upgradetx.View, port *BrokerPort, lease upgradelock.Lease) (upgradeport.Cut, error) {
	owner, err := p.host.Owner()
	if err != nil {
		return upgradeport.Cut{}, err
	}
	facts, err := p.facts(port)
	if err != nil {
		return upgradeport.Cut{}, err
	}
	generation, custody := p.custody(h, view, true)
	rows, err := censusRows(facts, owner, generation, custody)
	if err != nil {
		return upgradeport.Cut{}, err
	}
	scope, err := exportScope(active(view.Historical).Scope, rows)
	if err != nil {
		return upgradeport.Cut{}, deferral(ReasonDomainCensus)
	}
	p.mu.Lock()
	ready := p.ready[h.TxID]
	p.sequence++
	sequence := p.sequence
	p.mu.Unlock()
	if ready.target == "" {
		return upgradeport.Cut{}, deferral(ReasonUpgradeInProgress)
	}
	descriptor, err := json.Marshal(struct {
		Kind        string           `json:"kind"`
		Attempt     upgradetx.Handle `json:"attempt"`
		Target      string           `json:"target"`
		Previous    string           `json:"previous"`
		Source      string           `json:"source_sha"`
		LeaseDomain string           `json:"lease_domain"`
		Scope       string           `json:"scope_digest"`
	}{"broker-store-cut-v1", h, ready.target, ready.previous, ready.sourceSHA, lease.DomainID(), digest(scope)})
	if err != nil {
		return upgradeport.Cut{}, deferral(ReasonUpgradeInProgress)
	}
	return upgradeport.Cut{Attempt: h, Owner: owner, Stage: upgradetx.CutPristine, Descriptor: descriptor, Scope: scope, Sequence: sequence}, nil
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

// abandon releases a reserved cut nobody activated or released within its
// budget. The journal keeps whatever it recorded; a late Activate finds none.
// origin: hot-update S8 plan §3.2 / §8.2 F3 F4
func (p *brokerRuntime) abandon(held *heldCut) {
	p.mu.Lock()
	if p.held != held || held.activating {
		p.mu.Unlock()
		return
	}
	p.held = nil
	p.mu.Unlock()
	_ = held.lease.Close()
}

// release ends a reserved cut's custody exactly once (Thaw, a refused
// activation, or an armed cut whose S6 reload returned without exec).
func (p *brokerRuntime) release(held *heldCut) {
	p.mu.Lock()
	owned := p.held == held
	if owned {
		p.held = nil
	}
	p.mu.Unlock()
	if owned {
		held.timer.Stop()
		_ = held.lease.Close()
	}
}

// Thaw releases the reservation of a cut that was never activated. The
// broker's business never paused for it (the S6 freeze runs inside Activate).
func (p *brokerRuntime) Thaw(h upgradetx.Handle) error {
	p.mu.Lock()
	held := p.held
	if held == nil || !upgradetx.SameCut(held.handle, h) || held.activating {
		p.mu.Unlock()
		return nil // nothing of this cut is reserved here, or Activate owns it
	}
	p.mu.Unlock()
	p.release(held)
	return nil
}

// RuntimeOwner samples the configured process identity, never a caller field.
func RuntimeOwner(runtime upgradeport.RuntimePort) upgradetx.Owner {
	p := runtimeOf(runtime)
	if p == nil || p.host.Owner == nil {
		return upgradetx.Owner{}
	}
	owner, _ := p.host.Owner()
	return owner
}
