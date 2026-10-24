package upgradeobserve

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// DomainObserver is the S8-side port for a data-plane owner. Observe must take
// a bounded, read-only snapshot and must not call back into this Registry:
// admissions remain fenced while evidence is collected. It echoes the entire
// stamp, including nonce and sequence, rather than returning cached counters.
// NotApplicable requires a SHA-bound capability digest and an explicit
// ownerless not_applicable row in the original scope. An applicable, idle domain still needs a Known+Complete observation.
// origin: hot-update S8 plan §2.1 / §4.2 / P03
type DomainObserver struct {
	Domain           upgradetx.Domain
	CapabilityDigest string
	NotApplicable    bool
	Observe          func(upgradetx.Stamp) (upgradetx.Census, error)
}

// Config binds one cut to one live producer. Probe observes the running image
// and owner with a real nonce request; it must not hash the installed pathname.
// It echoes the requested stamp in Census.Stamp; the remaining Census fields
// are supplied by this registry, under the same admission gate. A
// fresh Control receipt in Current is additionally required by ControlProof.
// Callbacks are local ports, never a replaceable global function table.
type Config struct {
	Attempt   upgradetx.Attempt
	Producer  upgradetx.Owner
	Observers []DomainObserver
	Probe     func(upgradetx.Stamp) (upgradetx.Observation, error)
}

type resourceID struct {
	domain upgradetx.Domain
	id     string
}

type admission struct {
	original upgradetx.Resource
	finished bool
}

// sequencer has no background goroutine or FD custody. The mutex is both the
// Begin-before-effect barrier and the proof linearization point. Finished
// admissions are retained: forgetting them would let a shrinking observer
// roster sign completion after an unobserved late create.
type sequencer struct {
	mu         sync.Mutex
	attempt    upgradetx.Attempt
	producer   upgradetx.Owner
	observers  []DomainObserver
	probe      func(upgradetx.Stamp) (upgradetx.Observation, error)
	digest     string
	expected   string
	sequence   uint64
	pending    uint64
	admitted   map[uint64]admission
	identities map[resourceID]uint64
	sealed     bool
	completed  bool
}

var _ Registry = (*sequencer)(nil)

// New creates a cut-specific registry. Scope and bindings are copied; callers
// cannot mutate the original set after sealing its digest. All eleven domain
// bindings are required, including explicit capability-backed exclusions.
// The controller must construct it from the accepted cut's original scope;
// deriving that scope from a later, smaller live roster loses retired objects.
// origin: hot-update S8 plan §2.1 / §4.2 / P03
func New(config Config) (Registry, error) {
	attempt, err := canonicalAttempt(config.Attempt)
	if err != nil {
		return nil, err
	}
	if !validOwner(config.Producer) || config.Probe == nil {
		return nil, rejected("missing live producer or image observer")
	}
	observers, err := canonicalObservers(attempt.Scope, config.Observers)
	if err != nil {
		return nil, err
	}
	return &sequencer{
		attempt: attempt, producer: config.Producer, observers: observers, probe: config.Probe,
		// Sequence one represents the initial immutable scope. Even a known idle
		// cut has a nonzero barrier; zero remains an unknown stamp, never success.
		digest: digestOf(attempt.Scope), expected: digestOf(attempt.Scope.Expected), sequence: 1,
		admitted: make(map[uint64]admission), identities: make(map[resourceID]uint64),
	}, nil
}

// origin: hot-update S8 plan §4.2 / M04
func (r *sequencer) Begin(resource upgradetx.Resource) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !validResource(resource) || !hasDomain(r.attempt.Scope.Domains, resource.Domain) {
		return 0, rejected("admission is outside the observed scope")
	}
	for _, binding := range r.observers {
		if binding.Domain == resource.Domain && binding.NotApplicable {
			return 0, rejected("admission domain is not applicable")
		}
	}
	if resource.Generation != r.attempt.SourceGeneration && resource.Generation != r.attempt.Handle.Generation {
		return 0, rejected("admission belongs to another generation")
	}
	// A successful seal permanently fences old-generation effects. Later work
	// belongs to the target producer; failed seals leave this fence unchanged.
	if r.sealed && !r.completed {
		return 0, unsettled("completion publication still holds admission")
	}
	if r.sealed && (resource.Generation != r.attempt.Handle.Generation || resource.Owner != r.producer) {
		return 0, rejected("old generation admission after completion barrier")
	}
	identity := identityOf(resource)
	if _, exists := r.identities[identity]; exists {
		return 0, rejected("resource admission already registered")
	}
	if r.sequence == math.MaxUint64 {
		return 0, unsettled("admission sequence exhausted")
	}
	r.sequence++
	r.pending++
	r.admitted[r.sequence] = admission{original: resource}
	r.identities[identity] = r.sequence
	return r.sequence, nil
}

// Finish acknowledges only the admission that Begin issued. It does not prove
// migration or retirement: the independent domain observer proves custody.
// origin: hot-update S8 plan §4.2 / P03
func (r *sequencer) Finish(sequence uint64, resource upgradetx.Resource) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, exists := r.admitted[sequence]
	if !exists || sequence == 0 || entry.original != resource {
		return rejected("finish does not match its original admission")
	}
	if entry.finished {
		return nil // An exact retry cannot decrement another admission's count.
	}
	entry.finished = true
	r.admitted[sequence] = entry
	r.pending--
	return nil
}

// Snapshot retains Admitting even when domain collection succeeded. Such a
// census is useful for diagnostics, but cannot authorize a completion proof.
// A caller cannot choose an earlier sequence to omit an already accepted effect.
// origin: hot-update S8 plan §4.2 / M04 / M15
func (r *sequencer) Snapshot(stamp upgradetx.Stamp) (upgradetx.Census, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !upgradetx.SameCut(stamp.Attempt, r.attempt.Handle) || stamp.Attempt.Revision < r.attempt.Handle.Revision || stamp.Producer != r.producer ||
		stamp.ScopeDigest != r.digest || stamp.Sequence != r.sequence || !validStamp(stamp) {
		return upgradetx.Census{}, rejected("snapshot stamp does not match the current cut")
	}
	r.attempt.Handle.Revision = stamp.Attempt.Revision
	return r.snapshot(stamp)
}

// Sequence reports the registry's current admission sequence: the barrier a
// runtime's fresh sample of this cut must name so its Control, Released and
// final completion census line up with Seal and Commit. Reading it grants
// nothing; Snapshot and Commit still compare it under the gate. Other
// implementations (and a nil registry) report false. It must not be called
// from a callback the registry itself invokes while holding its gate.
// origin: hot-update S8 plan §4.2 / M04; S8 FIX-DESCRIBE (real-cut finding)
func Sequence(registry Registry) (uint64, bool) {
	r, ok := registry.(*sequencer)
	if !ok || r == nil {
		return 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sequence, true
}

// Seal returns resources_migrated only; Control and Released are independent
// proofs. The original Scope stays immutable, while Census includes the union
// of its expected objects and every admission up to the sealed sequence.
// origin: hot-update S8 plan §4.2 / M04 / M06
func (r *sequencer) Seal(attempt upgradetx.Attempt, nonce string) (upgradetx.Receipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	attempt, err := canonicalAttempt(attempt)
	if err != nil || !sameCut(attempt, r.attempt) || attempt.Handle.Revision < r.attempt.Handle.Revision || !validID(nonce) {
		return upgradetx.Receipt{}, rejected("seal does not match the original cut")
	}
	if r.pending != 0 {
		return upgradetx.Receipt{}, unsettled("resource admission is still in progress")
	}
	r.attempt = attempt
	stamp := upgradetx.Stamp{Attempt: r.attempt.Handle, Producer: r.producer,
		Nonce: nonce, Sequence: r.sequence, At: time.Now().UnixNano(), ScopeDigest: r.digest}
	census, err := r.snapshot(stamp)
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	observation, err := r.probe(stamp)
	if err != nil || observation.Census.Stamp != stamp {
		// Do not propagate owner errors: they can contain private paths/state.
		return upgradetx.Receipt{}, unsettled("running image observation failed")
	}
	observation.Census = census
	receipt, err := ResourcesProof(attempt, observation, nonce)
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	r.sealed = true
	return receipt, nil
}

// snapshot runs only with mu held. Observers independently enumerate retained
// responsibility, not just executing workers. Missing/unknown observations
// return no usable census rather than manufacturing Known=true from zero.
func (r *sequencer) snapshot(stamp upgradetx.Stamp) (upgradetx.Census, error) {
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true,
		ExpectedDigest: r.expected, Admitting: r.pending}
	resources := make(map[resourceID]upgradetx.Resource)
	owners := make(map[upgradetx.Owner]bool)
	for _, binding := range r.observers {
		if binding.NotApplicable {
			for _, row := range r.attempt.Scope.Expected {
				if row.Domain == binding.Domain {
					resources[identityOf(row)] = row
				}
			}
			continue
		}
		observed, err := binding.Observe(stamp)
		if err != nil || observed.Stamp != stamp || !observed.Known || !observed.Complete {
			return upgradetx.Census{}, unsettled("domain observation is missing or unknown")
		}
		if observed.Admitting != 0 || observed.ExpectedDigest != r.expected {
			return upgradetx.Census{}, unsettled("domain observation has unsettled admissions or scope")
		}
		for _, resource := range observed.Resources {
			if !validResource(resource) || resource.Domain != binding.Domain {
				return upgradetx.Census{}, unsettled("domain observation has an invalid resource")
			}
			identity := identityOf(resource)
			if _, duplicate := resources[identity]; duplicate {
				return upgradetx.Census{}, unsettled("domain observation repeats a resource")
			}
			resources[identity] = resource
			if !terminal(resource) && (resource.Owner != r.producer || resource.Generation != r.attempt.Handle.Generation) {
				owners[resource.Owner] = true
			}
		}
		for _, owner := range observed.OldOwners {
			if !validOwner(owner) {
				return upgradetx.Census{}, unsettled("domain observation has unknown retained custody")
			}
			owners[owner] = true
		}
	}
	// Identity includes domain, ID and immutable origin. An equal count cannot
	// conceal one substituted process, permit, terminal or late-created worker.
	originals := make(map[resourceID]upgradetx.Resource, len(r.attempt.Scope.Expected))
	for _, expected := range r.attempt.Scope.Expected {
		originals[identityOf(expected)] = expected
		if current, exists := resources[identityOf(expected)]; !exists || current.Origin != expected.Origin {
			return upgradetx.Census{}, unsettled("original resource is absent from the census")
		}
	}
	for _, entry := range r.admitted {
		if current, exists := resources[identityOf(entry.original)]; !exists || current.Origin != entry.original.Origin {
			return upgradetx.Census{}, unsettled("admitted resource is absent from the census")
		}
	}
	// An observer can expose an effect whose adapter forgot Begin. Accepting
	// that unregistered extra would hide the very missing-admission defect the
	// gate is meant to prevent. Existing originals need no duplicate Begin.
	// The identity lookup is linear in total resources; a large live flow or
	// transfer cohort must not turn its local freeze budget into a quadratic scan.
	for identity, resource := range resources {
		if _, admitted := r.identities[identity]; admitted {
			continue
		}
		expected, original := originals[identity]
		if !original || expected.Origin != resource.Origin {
			return upgradetx.Census{}, unsettled("observed resource has no original or admission identity")
		}
	}
	for _, resource := range resources {
		census.Resources = append(census.Resources, resource)
	}
	sortResources(census.Resources)
	for owner := range owners {
		census.OldOwners = append(census.OldOwners, owner)
	}
	sort.Slice(census.OldOwners, func(i, j int) bool { return ownerLess(census.OldOwners[i], census.OldOwners[j]) })
	return census, nil
}

// Configuration closes the domain union before any callback runs. Removing an
// idle domain is otherwise indistinguishable from observing it as truly empty.
// Exclusion is explicit in both capability binding and scope membership.
// origin: hot-update S8 plan §2.1 / §4.2 / M06
func canonicalObservers(scope upgradetx.Scope, bindings []DomainObserver) ([]DomainObserver, error) {
	bindings = append([]DomainObserver(nil), bindings...)
	seen := make(map[upgradetx.Domain]bool)
	for _, binding := range bindings {
		if !validDomain(binding.Domain) || seen[binding.Domain] {
			return nil, rejected("unknown or duplicate domain observer")
		}
		seen[binding.Domain] = true
		applicable := true
		for _, row := range scope.Expected {
			if row.Domain == binding.Domain && upgradetx.ResourceNotApplicable(row) {
				applicable = false
			}
		}
		if !hasDomain(scope.Domains, binding.Domain) || !validSHA(binding.CapabilityDigest) || binding.NotApplicable == applicable ||
			(binding.NotApplicable && binding.Observe != nil) || (applicable && binding.Observe == nil) {
			return nil, rejected("domain applicability lacks capability and scope proof")
		}
	}
	if len(seen) != len(allDomains) {
		return nil, rejected("domain observer set is incomplete")
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Domain < bindings[j].Domain })
	return bindings, nil
}
