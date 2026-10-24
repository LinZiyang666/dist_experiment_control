package upgradeobserve

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"strings"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

var allDomains = [...]upgradetx.Domain{
	upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainPTY,
	upgradetx.DomainFlow, upgradetx.DomainSS, upgradetx.DomainTransferWorker,
	upgradetx.DomainTransferTracker, upgradetx.DomainBrokerStore,
	upgradetx.DomainBrokerRuntime, upgradetx.DomainAuthority, upgradetx.DomainUpgrade,
}

// ScopeDigest is shared by grant/observer adapters. It normalizes ordering but
// rejects duplicates: dropping an ambiguous original would shrink the proof.
func ScopeDigest(scope upgradetx.Scope) (string, error) {
	scope, err := canonicalScope(scope)
	if err != nil {
		return "", err
	}
	return digestOf(scope), nil
}

// ExpectedDigest binds the immutable original identities, separately from the
// observed resources (which also contain admissions after the initial cut).
func ExpectedDigest(scope upgradetx.Scope) (string, error) {
	scope, err := canonicalScope(scope)
	if err != nil {
		return "", err
	}
	return digestOf(scope.Expected), nil
}

// ResourcesProof verifies current running-image evidence and exact original
// custody. Terminal means State="terminal", Custody="durable": the owner port
// promises the terminal is durable and has no remaining execution authority.
// Other state/custody values require the target producer and target generation.
// origin: hot-update S8 plan §4.2 / M02 / M06 / M15
func ResourcesProof(attempt upgradetx.Attempt, observation upgradetx.Observation, nonce string) (upgradetx.Receipt, error) {
	attempt, census, err := freshCensus(attempt, observation, nonce)
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	for _, resource := range census.Resources {
		if !terminal(resource) && (resource.Owner != census.Stamp.Producer || resource.Generation != attempt.Handle.Generation) {
			return upgradetx.Receipt{}, unsettled("resource has not migrated to the target owner")
		}
	}
	return makeReceipt(upgradetx.ReceiptResourcesMigrated, observation, census), nil
}

// ControlProof cannot promote a register or running SHA into control readiness.
// The runtime adapter must supply its own fresh receipt after SUB/Flush,
// reconciliation and the nonce request succeed. Resources may still be old.
// origin: hot-update S8 plan §4.2 / M05 / M15
func ControlProof(attempt upgradetx.Attempt, observation upgradetx.Observation, nonce string) (upgradetx.Receipt, error) {
	_, census, err := freshCensus(attempt, observation, nonce)
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	control := observation.Current.Control
	if control.Kind != upgradetx.ReceiptControlReady || control.Stamp != census.Stamp ||
		!control.Known || !control.Complete || control.RunningSHA != observation.RunningSHA || !validSHA(control.EvidenceDigest) {
		return upgradetx.Receipt{}, unsettled("fresh control readiness proof is missing")
	}
	return makeReceipt(upgradetx.ReceiptControlReady, observation, census), nil
}

// ReleasedProof is independent of migration. A retained timer/duplicate FD/
// courier in OldOwners prevents release even when every business object moved.
// origin: hot-update S8 plan §4.2 / M03
func ReleasedProof(attempt upgradetx.Attempt, observation upgradetx.Observation, nonce string) (upgradetx.Receipt, error) {
	resources, err := ResourcesProof(attempt, observation, nonce)
	if err != nil {
		return upgradetx.Receipt{}, err
	}
	if len(resources.Census.OldOwners) != 0 {
		return upgradetx.Receipt{}, unsettled("old generation retains resource custody")
	}
	return makeReceipt(upgradetx.ReceiptOldGenerationReleased, observation, resources.Census), nil
}

// This validates a trusted local observer's evidence, not an arbitrary wire
// response. Wire/auth adapters must first authenticate the producer; aggregate
// Known/Complete flags originate only from the registry's full domain census.
// A resource receipt arriving before Control is valid, so phase/readiness are
// deliberately not inferred from the running SHA or used to gate this helper.
// origin: hot-update S8 plan §4.2 / M06 / M15
func freshCensus(attempt upgradetx.Attempt, observation upgradetx.Observation, nonce string) (upgradetx.Attempt, upgradetx.Census, error) {
	attempt, err := canonicalAttempt(attempt)
	if err != nil {
		return upgradetx.Attempt{}, upgradetx.Census{}, err
	}
	current, err := canonicalAttempt(observation.Current)
	census := observation.Census
	stamp := census.Stamp
	// All stamps are from the same local probe, not comparable remote clocks.
	// Exact nonce echo is what prevents a historical completed response from
	// authorizing a current no-op. Caller-generated nonces must never be reused.
	if err != nil || !sameCut(attempt, current) || current.Handle.Revision > attempt.Handle.Revision || !validID(nonce) || observation.Nonce != nonce ||
		!observation.OwnerKnown || observation.RunningSHA != attempt.TargetSHA || observation.EffectiveSHA != attempt.TargetSHA ||
		!validStamp(stamp) || stamp.Attempt != attempt.Handle || stamp.Nonce != nonce || stamp.At != observation.At ||
		stamp.ScopeDigest != digestOf(attempt.Scope) || census.ExpectedDigest != digestOf(attempt.Scope.Expected) ||
		!census.Known || !census.Complete || census.Admitting != 0 {
		return upgradetx.Attempt{}, upgradetx.Census{}, unsettled("current image or census proof is unknown or stale")
	}
	// Shared disposition and commitment validation keeps collector output
	// readable by Transition and the journal without projection changes.
	// origin: S8 internal review rv-txn F09 F10
	if err := upgradetx.ValidateCensus(attempt, census, false); err != nil {
		return upgradetx.Attempt{}, upgradetx.Census{}, unsettled("invalid census evidence")
	}
	resources := make(map[resourceID]upgradetx.Resource)
	for _, resource := range census.Resources {
		if !validResource(resource) || !hasDomain(attempt.Scope.Domains, resource.Domain) {
			return upgradetx.Attempt{}, upgradetx.Census{}, unsettled("census resource is outside the original scope")
		}
		identity := identityOf(resource)
		if _, duplicate := resources[identity]; duplicate {
			return upgradetx.Attempt{}, upgradetx.Census{}, unsettled("census contains a duplicate identity")
		}
		resources[identity] = resource
	}
	for _, expected := range attempt.Scope.Expected {
		if current, exists := resources[identityOf(expected)]; !exists || current.Origin != expected.Origin {
			return upgradetx.Attempt{}, upgradetx.Census{}, unsettled("census omits an original identity")
		}
	}
	for _, owner := range census.OldOwners {
		if !validOwner(owner) {
			return upgradetx.Attempt{}, upgradetx.Census{}, unsettled("retained owner is unknown")
		}
	}
	census.Resources = append([]upgradetx.Resource(nil), census.Resources...)
	census.OldOwners = append([]upgradetx.Owner(nil), census.OldOwners...)
	sortResources(census.Resources)
	sort.Slice(census.OldOwners, func(i, j int) bool { return ownerLess(census.OldOwners[i], census.OldOwners[j]) })
	return attempt, census, nil
}

func makeReceipt(kind upgradetx.ReceiptKind, observation upgradetx.Observation, census upgradetx.Census) upgradetx.Receipt {
	receipt := upgradetx.Receipt{Kind: kind, Stamp: census.Stamp, Known: true, Complete: true,
		RunningSHA: observation.RunningSHA, Census: census}
	// Kind, current cut (including direction/SHAs), live image and complete
	// census all enter the evidence digest; forward evidence cannot sign reverse.
	receipt.EvidenceDigest = digestOf(struct {
		Kind    upgradetx.ReceiptKind
		Current upgradetx.Attempt
		Receipt upgradetx.Receipt
	}{kind, observation.Current, receipt})
	return receipt
}

// A reverse cut gets a new handle/generation even when its target bytes match
// a historical forward source. Comparing only TxID or two SHAs permits reuse
// of forward evidence and can revive an old generation's execution authority.
// origin: hot-update S8 plan §2.1 / §4.2 / M07
func canonicalAttempt(attempt upgradetx.Attempt) (upgradetx.Attempt, error) {
	h := attempt.Handle
	if !validKey(h.Key) || !validID(h.TxID) || !validID(h.CutID) || h.Generation == 0 || h.Revision == 0 ||
		attempt.SourceGeneration == 0 || attempt.SourceGeneration >= h.Generation ||
		!validSHA(attempt.SourceSHA) || !validSHA(attempt.TargetSHA) ||
		(attempt.Direction != upgradetx.DirectionForward && attempt.Direction != upgradetx.DirectionReverse) {
		return upgradetx.Attempt{}, rejected("cut identity is incomplete")
	}
	scope, err := canonicalScope(attempt.Scope)
	if err != nil {
		return upgradetx.Attempt{}, err
	}
	found := false
	for _, participant := range scope.Participants {
		found = found || participant == h.Key
	}
	if !found {
		return upgradetx.Attempt{}, rejected("cut participant is absent from the scope")
	}
	attempt.Scope = scope
	return attempt, nil
}

// Scope hashing has one canonical representation: lexicographic participant
// fields, then domains, then (domain, resource ID); an empty Expected is [].
// Owners and custody remain in ExpectedDigest even after the census observes
// their migrated counterparts. Dropping those original fields masks ID reuse.
// origin: hot-update S8 plan §2.1 / §4.2 / M06
func canonicalScope(scope upgradetx.Scope) (upgradetx.Scope, error) {
	return upgradetx.CanonicalScope(scope)
}

func sameCut(a, b upgradetx.Attempt) bool {
	return upgradetx.SameCut(a.Handle, b.Handle) && a.Direction == b.Direction && a.SourceSHA == b.SourceSHA &&
		a.TargetSHA == b.TargetSHA && a.SourceGeneration == b.SourceGeneration && reflect.DeepEqual(a.Scope, b.Scope)
}

func identityOf(resource upgradetx.Resource) resourceID {
	return resourceID{resource.Domain, resource.ID}
}

func terminal(resource upgradetx.Resource) bool {
	return upgradetx.ResourceReleased(resource) || upgradetx.ResourceNotApplicable(resource)
}

// IDs describe original runtime objects and may use each owner's own format;
// only transaction/nonce/instance IDs use the frozen 128-bit hex format. The
// strings contain identity/custody references, never sealed secret material.
func validResource(resource upgradetx.Resource) bool {
	return upgradetx.ValidateResource(resource) == nil
}

func validStamp(stamp upgradetx.Stamp) bool {
	return validOwner(stamp.Producer) && validID(stamp.Nonce) && stamp.Sequence > 0 && stamp.At > 0 && validSHA(stamp.ScopeDigest)
}

// All four fields are required. A reused PID or same-PID exec cannot by itself
// identify the producer or prove that retained worker/FD custody disappeared.
func validOwner(owner upgradetx.Owner) bool {
	return bounded(owner.BootID) && owner.PID > 0 && owner.StartTicks > 0 && validID(owner.InstanceID)
}

// S9 uses the real sequencer and proof collectors with its own role-bearing
// key. Registration does not supply missing domain or custody evidence, nor
// allow another component's receipt to stand in for this one.
// origin: hot-update S8 plan §2.1 / §4.2; S9 plan §8 / T44
func validKey(key upgradetx.Key) bool { return upgradetx.ValidateKey(key) == nil }

func validID(id string) bool   { return validHex(id, 32) }
func validSHA(sha string) bool { return validHex(sha, 64) }

func validHex(value string, size int) bool {
	if len(value) != size || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func bounded(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsRune(value, '\x00')
}

func validDomain(domain upgradetx.Domain) bool { return hasDomain(allDomains[:], domain) }

func hasDomain(domains []upgradetx.Domain, domain upgradetx.Domain) bool {
	for _, candidate := range domains {
		if candidate == domain {
			return true
		}
	}
	return false
}

func sortResources(resources []upgradetx.Resource) {
	sort.Slice(resources, func(i, j int) bool {
		if resources[i].Domain != resources[j].Domain {
			return resources[i].Domain < resources[j].Domain
		}
		return resources[i].ID < resources[j].ID
	})
}

func ownerLess(a, b upgradetx.Owner) bool {
	if a.BootID != b.BootID {
		return a.BootID < b.BootID
	}
	if a.PID != b.PID {
		return a.PID < b.PID
	}
	if a.StartTicks != b.StartTicks {
		return a.StartTicks < b.StartTicks
	}
	return a.InstanceID < b.InstanceID
}

func digestOf(value any) string {
	// All callers use closed DTOs with JSON-supported fields; no codec failure
	// or secrets are possible here. This digest never authorizes by itself.
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func rejected(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: detail}
}

func unsettled(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: detail}
}
