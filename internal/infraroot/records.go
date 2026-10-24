package infraroot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/infraroot/installfs"
)

// effectRecord is root-approved evidence, addressed by its complete canonical
// digest. Possessing a signing seed cannot manufacture one of these records.
// A producer must publish it only after verifying compatibility and the physical
// proof references; Validate also checks current observations before dispatch.
// origin: S9 internal review rv-infra F01
type effectRecord struct {
	Version                               uint16
	Purpose                               string
	Tx, Domain                            c.ID
	Step                                  uint32
	Op                                    c.HostOp
	Expected                              c.Process
	Candidate                             c.Artifact
	Config                                c.Digest
	Store                                 c.Store
	BeforeCensus, WarmCensus, ReturnProof c.Digest
	Deadline                              c.Deadline
}

type unitProjection struct {
	Version uint16
	Purpose string
	Request c.HostRequest
	Intent  c.Digest
	Fence   infrahost.StartupFence
}

type installedRecords struct {
	installation *installation
	record       installedRecord
	policy       infrapriv.Policy
	root         *Root
	mu           sync.Mutex
	inputs       map[c.Digest]*os.File
	held         []io.Closer
}

func (r *installedRecords) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	for _, file := range r.held {
		err = errors.Join(err, file.Close())
	}
	r.held = nil
	return err
}
func (r *installedRecords) hold(v io.Closer) { r.mu.Lock(); r.held = append(r.held, v); r.mu.Unlock() }
func digestName(d c.Digest) string           { return hex.EncodeToString(d[:]) }
func (r *installedRecords) immutable(digest c.Digest, value any) error {
	if digest == (c.Digest{}) {
		return c.ErrIncomplete
	}
	path := stateDirectory + "/records/" + digestName(digest) + ".json"
	data, err := r.installation.files.Read(path, c.MaxRequestBytes)
	if err != nil || sha256.Sum256(data) != digest {
		return c.ErrIncomplete
	}
	if err := r.installation.files.Decode(path, value); err != nil {
		return err
	}
	return nil
}
func (r *installedRecords) effect(request c.HostRequest) (effectRecord, error) {
	var e effectRecord
	if err := r.immutable(request.Payload, &e); err != nil {
		return e, err
	}
	if e.Version != 1 || e.Purpose != "tether-infra-effect-v1" || e.Tx != request.Tx || e.Domain != request.Domain || e.Step != request.Step || e.Op != request.Op || e.Expected != request.Expected || e.Candidate != request.Candidate || e.Config != request.Config || e.Store != request.Store || e.BeforeCensus != request.BeforeCensus || e.WarmCensus != request.WarmCensus || e.ReturnProof != request.ReturnProof {
		return e, infrapriv.ErrPermission
	}
	return e, nil
}

func (r *installedRecords) Validate(request c.HostRequest, state infrahost.HostState, d c.Deadline, stop c.Stop) error {
	if err := installedBudget(d, stop); err != nil {
		return err
	}
	if request.Domain != r.policy.Domain || request.HostBootID != r.policy.HostBootID || request.PolicyRevision != r.policy.Revision {
		return infrapriv.ErrPolicyStale
	}
	switch request.Op {
	case c.Inspect, c.VerifyIngress:
		return nil
	case c.RecoverHost:
		// The journal resolves this exact request digest under the authenticated
		// domain. It is not the digest of a separately manufactured proof file.
		return nil
	case c.RotatePolicy:
		_, err := r.resolvePolicy(request.Payload, d, stop)
		return err
	case c.SwitchIngress:
		_, err := (installedChanges{r}).Load(request.Payload)
		return err
	case c.ReleaseStart:
		if request.Component == c.Caddy {
			// FINAL and release share the exact storage payload; candidate/config
			// belong to the launch journal and must not change that identity.
			_, err := infrahost.ACMEFinalRevision(r.root.storage, request, d)
			return err
		}
	case c.Stage:
		if request.Candidate.Kind == c.NATSBinary || request.Candidate.Kind == c.CaddyBinary {
			catalog, err := infraartifact.New(infraartifact.Config{})
			if err != nil {
				return err
			}
			approved, err := catalog.Resolve(request.Candidate.Kind, request.Candidate.Version, request.Candidate.GOOS, request.Candidate.GOARCH)
			if err != nil || approved != request.Candidate {
				return infrahost.ErrArtifact
			}
			return nil
		}
	case c.LaunchClaim:
		if state.Permit != request.Payload || state.LaunchClaimed {
			return infrapriv.ErrPermission
		}
		return r.validateLauncher(request.Expected, request.Component, d, stop)
	case c.LaunchAck:
		// The sender writes this intent before exec. Completion requires an actual
		// post-exec observation with the same stable identity, never sender timing.
		// origin: S9 internal review rv-infra F04
		for {
			current, err := r.installation.process(request.Expected.PID, d)
			if err == nil && current == request.Expected {
				return nil
			}
			if err == nil && !sameRootInvocation(current, request.Expected) {
				return infrapriv.ErrPermission
			}
			if err := installedBudget(d, stop); err != nil {
				return err
			}
			timer := time.NewTimer(5 * time.Millisecond)
			select {
			case <-stop:
				timer.Stop()
				return infrapriv.ErrBudget
			case <-timer.C:
			}
		}
	case c.FenceStart, c.Promote, c.SignalLDM, c.SignalStop, c.SignalGraceful,
		c.StartWarm, c.FinalizeStorage, c.RetireWarm, c.UpdateSupport:
		// Mutations below require their immutable effect record.
	}
	evidence, err := r.effect(request)
	if err != nil {
		return err
	}
	if _, err := controlbudget.Remaining(evidence.Deadline); err != nil {
		return err
	}
	if request.BeforeCensus != (c.Digest{}) {
		census, err := r.Inspect(request.Domain, d)
		if err != nil || !census.Complete || census.Digest != request.BeforeCensus {
			return c.ErrIncomplete
		}
	}
	return nil
}
func (r *installedRecords) Inspect(domain c.ID, d c.Deadline) (c.Census, error) {
	if domain != r.policy.Domain {
		return c.Census{}, c.ErrIncomplete
	}
	return r.installation.census(r.record.Scope, d)
}
func (r *installedRecords) observe(expected c.Process, d c.Deadline) (c.Process, error) {
	actual, err := r.installation.process(expected.PID, d)
	if err != nil || actual != expected {
		return c.Process{}, infrapriv.ErrPermission
	}
	return actual, nil
}
func (r *installedRecords) Resolve(event infrahost.JournalEvent, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if err := installedBudget(d, stop); err != nil {
		return c.HostReceipt{}, err
	}
	census, err := r.Inspect(event.Request.Domain, d)
	if err != nil || !census.Complete {
		return c.HostReceipt{}, c.ErrIncomplete
	}
	switch event.Request.Op {
	case c.SignalLDM, c.SignalStop, c.SignalGraceful, c.RetireWarm:
		if infrahost.VerifyRetired([]c.Process{event.Request.Expected}, census, false) == nil {
			return c.HostReceipt{Applied: true, RequestDigest: event.RequestDigest, Census: census}, nil
		}
	case c.ReleaseStart:
		f, err := r.Load(event.Request.Component, d)
		if err == nil && f.Tx == event.Request.Tx && f.Domain == event.Request.Domain && f.Step == event.Request.Step {
			if f.Closed && event.Request.Component == c.Caddy {
				// A durable closed projection proves the unit CAS was never reached.
				// Querying the exact storage release is idempotent; then finish the
				// original unit CAS under its original, non-renewable deadline.
				// origin: S9 internal review rv-infra F09
				owner := r.root.bindings[c.ReleaseStart].(*operation) //nolint:forcetypeassert // New constructs and verifies every private binding as an operation before publishing Root.
				return owner.backend.Do(event.Request, min(d, event.Deadline), stop)
			}
			if !f.Closed {
				ack, err := r.root.journal.OperationRecord(c.HostRequest{Op: c.LaunchAck, Tx: event.Request.Tx, Step: event.Request.Step, Component: event.Request.Component}, d)
				if err == nil && ack.Phase == infrahost.EffectApplied {
					actual, err := r.observe(ack.Request.Expected, d)
					if err == nil && actual.ExeSHA256 == event.Request.Candidate.BinarySHA256 {
						return c.HostReceipt{Applied: true, RequestDigest: event.RequestDigest, Current: actual, Census: census}, nil
					}
				}
			}
		}
	case c.Inspect, c.Stage, c.FenceStart, c.Promote, c.StartWarm, c.SwitchIngress,
		c.FinalizeStorage, c.RecoverHost, c.RotatePolicy, c.UpdateSupport,
		c.LaunchClaim, c.LaunchAck, c.VerifyIngress:
		// No read-only observation here proves these effects completed.
	}
	// An unobserved effect remains unknown; no signal, selector or unit action is
	// redispatched merely because its worker died before returning a receipt.
	return c.HostReceipt{}, infrahost.ErrHalted
}

func (r *installedRecords) Open(artifact c.Artifact, payload c.Digest, d c.Deadline) (*os.File, error) {
	if err := installedBudget(d, nil); err != nil {
		return nil, err
	}
	r.mu.Lock()
	input := r.inputs[payload]
	r.mu.Unlock()
	if input != nil {
		return installfs.Duplicate(input)
	}
	return r.installation.files.Open(stateDirectory+"/binary-"+digestName(artifact.BinarySHA256), 0755)
}
func (r *installedRecords) Accept(request c.HostRequest, peer c.Peer, files []*os.File, d c.Deadline, stop c.Stop) error {
	if request.Op == c.LaunchClaim || request.Op == c.LaunchAck {
		if peer.PID != request.Expected.PID || peer.StartTicks != request.Expected.StartTicks || peer.BootID != request.Expected.BootID {
			return infrapriv.ErrPermission
		}
	}
	if len(files) != 0 {
		if request.Op != c.Stage || len(files) != 1 {
			return infrapriv.ErrPermission
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.inputs == nil {
			r.inputs = make(map[c.Digest]*os.File)
		}
		if previous := r.inputs[request.Payload]; previous != nil {
			if _, err := previous.Stat(); err == nil {
				return infrapriv.ErrBusy
			}
		}
		r.inputs[request.Payload] = files[0]
	}
	return installedBudget(d, stop)
}
func (*installedRecords) Reply(c.HostRequest, c.HostReceipt, c.Peer, c.Deadline, c.Stop) ([]*os.File, error) {
	return nil, nil
}

func (r *installedRecords) Load(component c.Component, d c.Deadline) (infrahost.StartupFence, error) {
	if component != c.NATS && component != c.Caddy {
		return infrahost.StartupFence{}, c.ErrUnsupported
	}
	var projection unitProjection
	path := stateDirectory + "/unit-" + componentText(component) + ".json"
	err := r.installation.files.Decode(path, &projection)
	if err == nil {
		record, err := r.root.journal.OperationRecord(projection.Request, d)
		if err != nil || record.RequestDigest != projection.Intent || projection.Version != 1 || projection.Purpose != "tether-infra-unit-projection-v1" {
			return infrahost.StartupFence{}, c.ErrIncomplete
		}
		return r.advanceFence(projection.Fence, d)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return infrahost.StartupFence{}, err
	}
	state, _, err := r.root.journal.Read(d)
	if err != nil {
		return infrahost.StartupFence{}, err
	}
	event, err := r.root.journal.OperationRecord(c.HostRequest{Op: c.FenceStart, Tx: state.Tx, Step: state.Step, Component: component}, d)
	if err != nil || event.Phase != infrahost.EffectIntent {
		return infrahost.StartupFence{}, c.ErrIncomplete
	}
	evidence, err := r.effect(event.Request)
	if err != nil {
		return infrahost.StartupFence{}, err
	}
	return infrahost.StartupFence{Tx: state.Tx, Domain: r.policy.Domain, Step: state.Step, Component: component, Revision: 1, Deadline: evidence.Deadline}, nil
}
func (r *installedRecords) CompareAndSwap(before, after infrahost.StartupFence, request c.HostRequest, d c.Deadline) (c.HostReceipt, error) {
	unlock, err := r.installation.files.Lock(stateDirectory + "/unit-state.lock")
	if err != nil {
		return c.HostReceipt{}, err
	}
	defer unlock()
	actual, err := r.Load(request.Component, d)
	if err != nil || !reflect.DeepEqual(actual, before) || after.Revision != before.Revision+1 {
		return c.HostReceipt{}, infrahost.ErrCAS
	}
	event, err := r.root.journal.OperationRecord(request, d)
	if err != nil || (event.Phase != infrahost.EffectIntent && (request.Op != c.ReleaseStart || event.Phase != infrahost.EffectUnknown)) {
		return c.HostReceipt{}, infrahost.ErrCAS
	}
	data, err := c.EncodeRequest(request)
	if err != nil || sha256.Sum256(data) != event.RequestDigest {
		return c.HostReceipt{}, infrahost.ErrCAS
	}
	projection := unitProjection{Version: 1, Purpose: "tether-infra-unit-projection-v1", Request: request, Intent: event.RequestDigest, Fence: after}
	if err := r.installation.files.Encode(stateDirectory+"/unit-"+componentText(request.Component)+".json", projection, 0600, uint32(os.Getegid())); err != nil {
		return c.HostReceipt{}, err
	}
	return c.HostReceipt{Tx: request.Tx, Sequence: request.Sequence, Op: request.Op, Applied: true, Revision: after.Revision, RequestDigest: event.RequestDigest, Journal: event.Digest}, nil
}
func componentText(component c.Component) string {
	if component == c.NATS {
		return "nats"
	}
	return "caddy"
}

func (r *installedRecords) writersExited(expected c.Process, d c.Deadline, stop c.Stop) (c.Census, error) {
	if err := installedBudget(d, stop); err != nil {
		return c.Census{}, err
	}
	// Use the original writer's scope so unrelated NATS or warm listener queues
	// cannot be mistaken for this storage tree's remaining responsibilities.
	scope := r.record.Scope
	scope.Before, scope.Cgroups = []c.Process{expected}, []string{expected.Cgroup}
	census, err := r.installation.census(scope, d)
	if err != nil || infrahost.VerifyRetired([]c.Process{expected}, census, false) != nil {
		return c.Census{}, c.ErrIncomplete
	}
	// Keep only writers in the closed Caddy scope; NATS or helper processes do
	// not become evidence of a remaining writer of this approved filesystem tree.
	var writers []c.Process
	for _, process := range census.Processes {
		if process.Cgroup == "/system.slice/caddy.service" {
			writers = append(writers, process)
		}
	}
	census.Processes = writers
	return census, nil
}

// advanceFence derives completion only from committed common-journal evidence.
// The unit projection stores CAS progress; it cannot invent old-writer exit or
// FINAL storage merely because a preceding worker returned without a receipt.
func (r *installedRecords) advanceFence(f infrahost.StartupFence, d c.Deadline) (infrahost.StartupFence, error) {
	if !f.Closed {
		return f, nil
	}
	if f.Component == c.NATS {
		event, err := r.root.journal.OperationRecord(c.HostRequest{Op: c.ReleaseStart, Tx: f.Tx, Step: f.Step, Component: f.Component}, d)
		// origin: hot-update S10 lint-L1 / nilerr journal recovery
		if errors.Is(err, infrahost.ErrPermission) {
			return f, nil // No release intent has been recorded yet.
		}
		if err != nil {
			return f, err
		}
		if event.Phase != infrahost.EffectIntent && event.Phase != infrahost.EffectUnknown {
			return f, nil
		}
		if _, err := r.effect(event.Request); err != nil {
			return f, err
		}
		census, err := r.Inspect(f.Domain, d)
		if err != nil || infrahost.VerifyRetired([]c.Process{event.Request.Expected}, census, false) != nil {
			return f, c.ErrIncomplete
		}
		f.OldExited, f.StorageFinalized = true, event.Request.Payload
		return f, nil
	}
	event, err := r.root.journal.OperationRecord(c.HostRequest{Op: c.FinalizeStorage, Tx: f.Tx, Step: f.Step, Component: f.Component}, d)
	// origin: hot-update S10 lint-L1 / nilerr journal recovery
	if errors.Is(err, infrahost.ErrPermission) {
		return f, nil // No FINAL intent has been recorded yet.
	}
	if err != nil {
		return f, err
	}
	if event.Phase != infrahost.EffectApplied {
		return f, nil
	}
	if _, err := infrahost.ACMEFinalRevision(r.root.storage, event.Request, d); err != nil {
		return f, err
	}
	f.OldExited, f.StorageFinalized = true, event.Request.Payload
	return f, nil
}
func (r *installedRecords) storageRevision(request c.HostRequest, d c.Deadline) (uint64, error) {
	return infrahost.ACMEFinalRevision(r.root.storage, request, d)
}
func (r *installedRecords) resolvePolicy(digest c.Digest, d c.Deadline, stop c.Stop) (infrapriv.Policy, error) {
	if err := installedBudget(d, stop); err != nil {
		return infrapriv.Policy{}, err
	}
	var policy infrapriv.Policy
	if err := r.immutable(digest, &policy); err != nil {
		return policy, err
	}
	data, _ := json.Marshal(policy)
	return infrapriv.DecodePolicy(data)
}
func (r *installedRecords) persistPolicy(before, after infrapriv.Policy, d c.Deadline, stop c.Stop) error {
	if err := installedBudget(d, stop); err != nil {
		return err
	}
	unlock, err := r.installation.files.Lock(stateDirectory + "/policy.lock")
	if err != nil {
		return err
	}
	defer unlock()
	current, err := r.installation.policy()
	if err != nil || !reflect.DeepEqual(current, before) {
		return infrapriv.ErrPolicyStale
	}
	data, err := infrapriv.EncodePolicy(after)
	if err != nil {
		return err
	}
	return r.installation.files.Replace(stateDirectory+"/policy.json", data, 0600, uint32(os.Getegid()))
}

func sameRootInvocation(a, b c.Process) bool {
	return a.BootID == b.BootID && a.PID == b.PID && a.StartTicks == b.StartTicks && a.InvocationID == b.InvocationID && a.Cgroup == b.Cgroup
}
