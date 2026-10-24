// Package infrahost owns root effect ordering and immutable executable slots.
// It deliberately has no knowledge of systemd, nft or business bootstrap.
// origin: hot-update S9 plan §2.1 / §6.1 / T09
package infrahost

import (
	"crypto/sha256"
	"errors"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost/budgetclock"
)

var (
	ErrCAS        = errors.New("infra_policy_stale")
	ErrBusy       = errors.New("infra_busy")
	ErrHalted     = errors.New("infra_result_unknown")
	ErrPermission = errors.New("infra_permission_denied")
	ErrStopped    = errors.New("infra_budget_exceeded")
	ErrJournal    = errors.New("infra_internal")
	ErrArtifact   = errors.New("infra_artifact_invalid")
)

// Authority verifies the current root policy AND the authenticated local peer.
// P03/P40 supply this adapter; a valid caller signature alone is insufficient.
// Authorize runs during the short journal CAS, and must not dispatch host jobs.
type Authority interface {
	Authorize(infracontract.HostRequest, infracontract.Deadline) (infracontract.Peer, error)
}

// JournalProvider is supplied by the P04 Stage binding. This keeps the frozen
// HostFactory signature free of globals, environment paths and implicit opens.
type JournalProvider interface{ HostJournal() *Journal }

type worker struct {
	bindings infracontract.HostBindings
	journal  *Journal
	mu       sync.Mutex
	closed   bool
	active   int
	done     chan struct{}
	stop     chan struct{}
}

var _ infracontract.HostFactory = NewWorker

// NewWorker validates and copies the complete table before admitting requests.
// Construction never creates a directory, starts a process or advertises a
// partial capability. P40 is responsible for supplying the real backend types.
// origin: hot-update S9 plan §2.1 / §10.3 / T09
func NewWorker(bindings infracontract.HostBindings) (infracontract.Host, error) {
	if bindings[0] != nil {
		return nil, infracontract.ErrContractStub
	}
	for op := infracontract.Inspect; op <= infracontract.VerifyIngress; op++ {
		if nilInterface(bindings[op]) {
			return nil, infracontract.ErrContractStub
		}
	}
	p, ok := bindings[infracontract.Stage].(JournalProvider)
	if !ok || p.HostJournal() == nil {
		return nil, infracontract.ErrContractStub
	}
	j := p.HostJournal()
	for _, op := range []infracontract.HostOp{infracontract.Promote, infracontract.RecoverHost, infracontract.UpdateSupport} {
		other, ok := bindings[op].(JournalProvider)
		if !ok || other.HostJournal() != j {
			return nil, infracontract.ErrContractStub
		}
	}
	return &worker{bindings: bindings, journal: j, done: make(chan struct{}), stop: make(chan struct{})}, nil
}

func nilInterface(v interface{}) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	default:
		return false
	}
}

func (w *worker) enter() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrStopped
	}
	if w.active == 4 {
		return ErrBusy
	}
	w.active++
	return nil
}

func (w *worker) leave() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.active--
	if w.closed && w.active == 0 {
		close(w.done)
	}
}

// Close cancels and joins admitted calls; it never abandons a backend goroutine.
// Backends own their children and must join them before returning from Do.
// The journal is shared across workers and is closed separately by its owner.
func (w *worker) Close() error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.stop)
		if w.active == 0 {
			close(w.done)
		}
	}
	w.mu.Unlock()
	<-w.done
	return nil
}

// P36 moves these two new root sub-budgets into upgradebudget/infra.go when it
// lands the shared budget rows. Existing business budgets are not duplicated.
const hostActionLimit = 10 * time.Second
const hostCommitLimit = 5 * time.Second

type hostClock = budgetclock.Clock
type systemHostClock = budgetclock.System

// The receiver's already bounded local deadline and the signed remaining
// budget may only shorten the action cap; a replay never calls this a renewal.
func actionDeadline(r infracontract.HostRequest, d infracontract.Deadline) (infracontract.Deadline, error) {
	return actionDeadlineAt(systemHostClock{}, r, d)
}

func actionDeadlineAt(clock hostClock, r infracontract.HostRequest, d infracontract.Deadline) (infracontract.Deadline, error) {
	if _, err := clock.Remaining(d); err != nil {
		return 0, err
	}
	now, err := clock.Now()
	if err != nil {
		return 0, err
	}
	if r.RemainingNS == 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	if r.RemainingNS < uint64(int64(d)-now) {
		d = infracontract.Deadline(now + int64(r.RemainingNS))
	}
	if int64(d)-now > int64(hostActionLimit) {
		d = infracontract.Deadline(now + int64(hostActionLimit))
	}
	return d, nil
}

func checkStop(d infracontract.Deadline, stop infracontract.Stop) error {
	return checkStopAt(systemHostClock{}, d, stop)
}

func checkStopAt(clock hostClock, d infracontract.Deadline, stop infracontract.Stop) error {
	select {
	case <-stop:
		return ErrStopped
	default:
	}
	_, err := clock.Remaining(d)
	return err
}

func (w *worker) Do(r infracontract.HostRequest, d infracontract.Deadline, caller infracontract.Stop) (infracontract.HostReceipt, error) {
	if err := w.enter(); err != nil {
		return infracontract.HostReceipt{}, err
	}
	defer w.leave()
	if err := checkStopAt(w.journal.clock, d, caller); err != nil {
		return infracontract.HostReceipt{}, err
	}
	// Validation precedes array indexing and any durable side effect.
	if _, err := infracontract.EncodeRequest(r); err != nil {
		return infracontract.HostReceipt{}, err
	}
	// P03 includes packet/queue time in its absolute worker deadline. The
	// result-persistence tail may not silently add five seconds to that clock.
	workerDeadline, err := w.journal.clock.After(hostActionLimit + hostCommitLimit)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	workerDeadline = min(workerDeadline, d)
	d, err = actionDeadlineAt(w.journal.clock, r, d)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	stop, join := budgetclock.RelayStop(w.journal.clock, d, caller, w.stop)
	defer join()
	if err := checkStopAt(w.journal.clock, d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	// Current observations never advance the mutation cursor or replay a cached
	// census. The signed driver Inspect (with Expected) keeps its journal semantics.
	// origin: hot-update S9 plan §5.2 / WIRE-2 R11
	if r.Op == infracontract.VerifyIngress || r.Op == infracontract.Inspect && r.Expected == (infracontract.Process{}) {
		return observeHost(w, r, d, stop)
	}
	intent, replay, err := w.journal.begin(r, d, stop)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, err
		}
		return infracontract.HostReceipt{}, err
	}
	// origin: hot-update S9 plan §6.1 / T09
	// No journal lock crosses this call. A nonblocking systemd start may cause
	// another worker to Claim/Ack the SAME permit before this call returns.
	result, effectErr := w.bindings[r.Op].Do(r, intent.Deadline, stop)
	// Result persistence has its own reserved tail even when the action used
	// its last nanosecond. The complete worker still has the fixed 15s ceiling.
	commitDeadline, err := w.journal.clock.After(hostCommitLimit)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	return w.journal.finish(intent, result, effectErr, min(commitDeadline, workerDeadline))
}

func (w *worker) Inspect(tx infracontract.ID, d infracontract.Deadline) (infracontract.Census, error) {
	if err := w.enter(); err != nil {
		return infracontract.Census{}, err
	}
	defer w.leave()
	if _, err := w.journal.clock.Remaining(d); err != nil {
		return infracontract.Census{}, err
	}
	capDeadline, err := w.journal.clock.After(hostActionLimit)
	if err != nil {
		return infracontract.Census{}, err
	}
	if capDeadline < d {
		d = capDeadline
	}
	return w.bindings[infracontract.Inspect].Inspect(tx, d)
}

// ArtifactSource opens a catalog-approved FD. Its pathname never comes from
// IPC. Stage still copies and hashes its own inode because the sender can retain
// a writable descriptor to the source after passing the FD.
type ArtifactSource interface {
	Open(infracontract.Artifact, infracontract.Digest, infracontract.Deadline) (*os.File, error)
}

// HostFacts supplies independent physical observations. Resolve is read-only:
// it proves the outcome of one journaled intent without repeating its effect.
// Unknown/partial evidence must return ErrHalted, never a guessed success.
type HostFacts interface {
	// Validate resolves Payload/Config/census proofs from root-owned records,
	// checks compatibility, and pins Expected's current physical incarnation.
	// It never accepts a digest merely because the caller signed that digest.
	Validate(infracontract.HostRequest, HostState, infracontract.Deadline, infracontract.Stop) error
	Inspect(infracontract.ID, infracontract.Deadline) (infracontract.Census, error)
	Resolve(JournalEvent, infracontract.Deadline, infracontract.Stop) (infracontract.HostReceipt, error)
}

type Backend struct {
	journal *Journal
	source  ArtifactSource
	facts   HostFacts
}

// Refuse marks a backend's proven before-effect rejection. An ordinary error
// remains ambiguous and therefore halts. The journal stores only a closed code.
type Refusal struct{ Cause error }

func (e *Refusal) Error() string { return "infra_internal" }
func (e *Refusal) Unwrap() error { return e.Cause }
func Refuse(err error) error     { return &Refusal{Cause: err} }

func NewBackend(j *Journal, source ArtifactSource, facts HostFacts) (*Backend, error) {
	if j == nil || nilInterface(source) || nilInterface(facts) {
		return nil, infracontract.ErrContractStub
	}
	return &Backend{j, source, facts}, nil
}
func (b *Backend) HostJournal() *Journal { return b.journal }
func (b *Backend) Inspect(tx infracontract.ID, d infracontract.Deadline) (infracontract.Census, error) {
	return b.facts.Inspect(tx, d)
}

// These four verbs never accept a path, argv, unit name or executable from IPC.
// Selectors live in the durable result, so a failed commit cannot activate bytes.
// origin: hot-update S9 plan §6.2 / T10 / T11
func (b *Backend) Do(r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (result infracontract.HostReceipt, err error) {
	// These operations publish their selector only in journal.finish. Failure
	// may leave an unselected immutable slot, but cannot change the active owner.
	defer func() {
		if err != nil {
			err = Refuse(err)
		}
	}()
	if err := checkStopAt(b.journal.clock, d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	state, _, err := b.journal.Read(d)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err := b.facts.Validate(r, state, d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	result = infracontract.HostReceipt{Applied: true, Current: r.Expected}
	switch r.Op {
	case infracontract.Stage:
		fd, err := b.source.Open(r.Candidate, r.Payload, d)
		if err != nil || fd == nil {
			return result, ErrArtifact
		}
		// The source is only read; copyArtifact authenticates the root-owned copy,
		// so closing the caller's descriptor cannot change the verdict.
		defer func() { _ = fd.Close() }()
		if err := copyArtifact(b.journal.directory, fd, r.Candidate, d, stop); err != nil {
			return result, err
		}
	case infracontract.Promote, infracontract.UpdateSupport:
		if err := verifyArtifact(b.journal.directory, r.Candidate, d, stop); err != nil {
			return result, err
		}
		if r.Op == infracontract.UpdateSupport {
			c, err := b.facts.Inspect(r.Tx, d)
			if err != nil || !completeCensus(c) {
				return result, infracontract.ErrIncomplete
			}
			// The switching worker must end its lease and exit, not wait for itself.
			// Other workers must already have joined; new old-generation admission
			// is fenced by the journal until a new worker proves retirement.
			if len(c.Processes) != 1 || c.Processes[0] != r.Expected {
				return result, ErrBusy
			}
			result.Census = c
			result.Code = "infra_recovery_required"
		}
	case infracontract.RecoverHost:
		if state.SupportPending {
			if r.ReturnProof == (infracontract.Digest{}) {
				return result, ErrPermission
			}
			c, err := b.facts.Inspect(r.Tx, d)
			if err != nil || !completeCensus(c) || c.OldOwnerPending != 0 {
				return result, infracontract.ErrIncomplete
			}
			found := false
			for _, p := range c.Processes {
				if sameLifetime(p, state.SupportBefore) {
					return result, ErrBusy
				}
				found = found || p == r.Expected
			}
			// Helper selection changes this hash; launcher selection does not.
			// For a launcher, Validate additionally resolves the later P06 exec
			// retirement proof. A new helper PID alone cannot establish that cut.
			if !found || sameLifetime(r.Expected, state.SupportBefore) || r.Expected.ExeSHA256 != state.Slots[infracontract.HelperBinary].Active.BinarySHA256 {
				return result, ErrPermission
			}
			result.Census = c
		} else {
			pending, err := b.journal.recoveryTarget(r.Payload, d)
			if err != nil {
				return result, err
			}
			return b.facts.Resolve(pending, d, stop)
		}
	// origin: hot-update S9 plan §10.2 / HOSTOP-SWITCHES
	case infracontract.Inspect, infracontract.FenceStart, infracontract.SignalLDM,
		infracontract.SignalStop, infracontract.SignalGraceful, infracontract.StartWarm,
		infracontract.SwitchIngress, infracontract.FinalizeStorage, infracontract.ReleaseStart,
		infracontract.RetireWarm, infracontract.RotatePolicy, infracontract.LaunchClaim,
		infracontract.LaunchAck, infracontract.VerifyIngress:
		return infracontract.HostReceipt{}, ErrPermission
	default:
		return infracontract.HostReceipt{}, ErrPermission
	}
	return result, nil
}

func completeCensus(c infracontract.Census) bool {
	return c.Complete && c.Generation != 0 && c.Digest != (infracontract.Digest{})
}
func sameLifetime(a, b infracontract.Process) bool {
	return a.BootID == b.BootID && a.PID == b.PID && a.StartTicks == b.StartTicks
}

// Closed public codes prevent an adapter's diagnostic text becoming durable
// protocol data. Additional operational detail belongs in private evidence.
func validResultCode(code string) bool {
	switch code {
	case "", "infra_unsupported", "infra_artifact_invalid", "infra_config_invalid", "infra_incompatible", "infra_permission_denied", "infra_policy_stale", "infra_not_enabled", "infra_busy", "infra_not_ready", "infra_inventory_incomplete", "infra_budget_exceeded", "infra_result_unknown", "infra_recovery_required", "infra_internal":
		return true
	default:
		return false
	}
}

// A seal is an observation of NOW, never the replay of an earlier zero count.
// Read the authorized journal head before and after physical evidence. No lock
// crosses nft/proc reads: a changed head refuses the observation, while a nested
// launcher or effect can still acquire its short CAS. No sequence is consumed.
// origin: hot-update S9 plan §2.2 / §5.2 / §6.1 / WIRE-2 R11
func observeHost(w *worker, r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
	last, err := observationHead(w, r, d, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	result, err := w.bindings[r.Op].Do(r, d, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err := checkStopAt(w.journal.clock, d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if !completeCensus(result.Census) || result.Code != "" {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	current, err := observationHead(w, r, d, stop)
	if err != nil || current.Digest != last.Digest {
		return infracontract.HostReceipt{}, ErrCAS
	}
	encoded, err := infracontract.EncodeRequest(r)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	result.Tx, result.Sequence, result.Op = r.Tx, r.Sequence, r.Op
	result.Applied = false // Read-only observations cannot claim a host effect.
	result.Revision, result.Journal, result.RequestDigest = last.Revision, last.Digest, sha256.Sum256(encoded)
	return result, nil
}

func observationHead(w *worker, r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (JournalEvent, error) {
	j := w.journal
	unlock, err := lockJournalAt(j.directory, d, stop, j.clock)
	if err != nil {
		return JournalEvent{}, err
	}
	defer unlock()
	if r.Domain != j.domain || r.HostBootID != j.boot {
		return JournalEvent{}, ErrPermission
	}
	peer, err := j.authority.Authorize(r, d)
	if err != nil || peer.PID <= 0 || peer.StartTicks == 0 || peer.BootID != j.boot {
		return JournalEvent{}, ErrPermission
	}
	im, err := j.load()
	if err != nil {
		return JournalEvent{}, err
	}
	last := im.Events[len(im.Events)-1]
	if r.PolicyRevision < last.State.PolicyRevision ||
		r.ExpectedJournal != (infracontract.Digest{}) && r.ExpectedJournal != last.Digest ||
		r.Op == infracontract.VerifyIngress && (last.State.Tx != r.Tx || last.State.Step != r.Step) {
		return JournalEvent{}, ErrCAS
	}
	return last, nil
}
