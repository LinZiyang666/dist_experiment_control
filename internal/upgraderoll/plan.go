// Package upgraderoll drives a sealed parent plan through component, host and
// cluster completion. It knows no roster discovery or release-version shortcut:
// a resumed driver has authority only for the original target-issued handles.
// origin: hot-update S8 plan §3.1 / §5.1 / §6.3 / P11
package upgraderoll

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// PlanStore extends the sole journal writer, rather than creating a second
// driver journal. SealPlan atomically persists the complete plan before returning
// and returns the existing immutable plan on retry. It must reject a different
// parent/current identity and never overwrite a sealed plan. LoadPlan does not
// discover participants or issue grants. P02 supplies this durable adapter.
type PlanStore interface {
	upgradetx.Store
	SealPlan(upgradetx.Plan) (upgradetx.Plan, error)
	LoadPlan(upgradetx.Handle) (upgradetx.Plan, error)
	// CompletePlan commits only the aggregate parent, after independently
	// checking all three receipts and the final expected-holder release. It does
	// not reinterpret a parent's bookkeeping as a component image activation.
	CompletePlan(upgradetx.Handle, upgradetx.Observation) (upgradetx.View, error)
}

// Endpoint supplies S8-side ports for P15/P19 integration. Artifact reads the
// original grant's staged receipt; it neither stages nor starts a new transaction.
// Converge returns fresh home/authority proof for the fixed host, using the step's
// handle. Reserve joins the original expected-holder reservation, including its
// release tombstone on resume, and rejects unknown ownership or changed routes.
type Endpoint interface {
	upgradeport.Endpoint
	Reserve(context.Context, upgradetx.Plan) (Reservation, error)
	Artifact(context.Context, upgradetx.Grant) (upgradeartifact.Receipt, error)
	Converge(context.Context, upgradetx.Step, string) (upgradetx.Receipt, error)
	ObserveParent(context.Context, upgradetx.Plan, string) (upgradetx.Observation, error)
}

// Reservation belongs to the durable parent, not the driver's process. Renew
// validates the exact holder and original deadline; it never extends execution
// grants. Release CASes a host slot or, for the last step, the cluster holder and
// returns independent fresh readback. Stop closes only driver-local resources;
// it must never unlock a host/cluster reservation after cancellation or failure.
// RenewalInterval comes from the adapter's existing lease budget (cluster uses
// cluster.LockLeaseRenewInterval), so this leaf creates no competing lease TTL.
type Reservation interface {
	Check(context.Context, upgradetx.Plan, string) (FleetProof, error)
	Renew(context.Context, upgradetx.Handle) error
	Release(context.Context, upgradetx.Step, string) (upgradetx.Receipt, error)
	RenewalInterval() time.Duration
	Stop() error
}

// FleetMember includes non-voters, siblings and retained helpers alike. There is
// intentionally no voter flag that a collector could use to reduce the scope.
// Each retained executable owner needs a known release and generation.
type FleetMember struct {
	Participant upgradetx.Key
	Owner       upgradetx.Owner
	Release     string
	Generation  uint64
	Known       bool
}

// FleetProof is a current observation under the expected holder, never a cached
// roster. Participants is the complete managed snapshot, including offline keys;
// Census retains every original resource ID, including helper/permit custody.
// HolderHeld may be false only after the final release's independent readback.
type FleetProof struct {
	Parent       upgradetx.Handle
	Nonce        string
	Known        bool
	Complete     bool
	Participants []upgradetx.Key
	Members      []FleetMember
	Census       upgradetx.Census
	HolderHeld   bool
}

// PlanDigest and ScopeDigest use the frozen DTO's ordered JSON fields and the
// already canonical collection order. Adapters must use these exact bytes when
// binding a parent action or scope; a hash of a later roster is different proof.
func PlanDigest(plan upgradetx.Plan) string    { return digest(plan) }
func ScopeDigest(scope upgradetx.Scope) string { return digest(scope) }

func digest(value any) string {
	data, _ := json.Marshal(value) // Frozen DTOs contain no unsupported JSON values.
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func refusal(code, detail string, accepted bool) error {
	return &upgradetx.Error{Code: code, Detail: detail, Accepted: accepted}
}

func sameAttempt(a, b upgradetx.Handle) bool {
	return a.Key == b.Key && a.TxID == b.TxID && a.CutID == b.CutID && a.Generation == b.Generation
}

func validHex(value string, size int) bool {
	if len(value) != size || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validOwner(owner upgradetx.Owner) bool {
	return owner.BootID != "" && owner.PID > 0 && owner.StartTicks > 0 && validHex(owner.InstanceID, 32)
}

func validKey(key upgradetx.Key) bool {
	// origin: S8 internal review remaining-orchestration F5 (R5)
	// Planner and registration address the same journal key. Requiring a
	// broker node name or an agent config path here rejects valid P00 keys.
	return upgradetx.ValidateKey(key) == nil &&
		(key.Component == upgradetx.ComponentAgent || key.Component == upgradetx.ComponentBroker)
}

func validHandle(handle upgradetx.Handle) bool {
	return validKey(handle.Key) && validHex(handle.TxID, 32) && validHex(handle.CutID, 32) && handle.Generation > 0 && handle.Revision > 0
}

func keyOrder(a, b upgradetx.Key) int {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return strings.Compare(string(left), string(right))
}

func resourceID(resource upgradetx.Resource) string {
	return string(resource.Domain) + "\x00" + resource.ID + "\x00" + resource.Origin
}

func validScope(scope upgradetx.Scope) bool {
	if !validHex(scope.ID, 32) || len(scope.Participants) == 0 || len(scope.Participants) > 4096 || len(scope.Domains) == 0 || len(scope.Expected) > 65536 {
		return false
	}
	for i, key := range scope.Participants {
		if !validKey(key) || (i > 0 && keyOrder(scope.Participants[i-1], key) >= 0) {
			return false
		}
	}
	for i, domain := range scope.Domains {
		if !slices.Contains([]upgradetx.Domain{upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainPTY, upgradetx.DomainFlow, upgradetx.DomainSS, upgradetx.DomainTransferWorker, upgradetx.DomainTransferTracker, upgradetx.DomainBrokerStore, upgradetx.DomainBrokerRuntime, upgradetx.DomainAuthority, upgradetx.DomainUpgrade}, domain) || (i > 0 && scope.Domains[i-1] >= domain) {
			return false
		}
	}
	for i, resource := range scope.Expected {
		if resource.ID == "" || resource.Origin == "" || !slices.Contains(scope.Domains, resource.Domain) || !validOwner(resource.Owner) || resource.Generation == 0 || (i > 0 && resourceID(scope.Expected[i-1]) >= resourceID(resource)) {
			return false
		}
	}
	return true
}

// validatePlan is the roll-specific closure in addition to the neutral P01
// predicate: every participant has one cut, each host converges/releases after
// all its children, and the cluster release depends on every host release.
// origin: hot-update S8 plan §3.1 / §5.1 / §8.3 M09
func validatePlan(plan upgradetx.Plan) error {
	bad := func() error { return refusal(upgradetx.CodeRejected, "invalid sealed parent plan", false) }
	if !validHandle(plan.Parent) || !validScope(plan.Scope) || !validHex(plan.SourceManifestDigest, 64) || !validHex(plan.TargetManifestDigest, 64) || plan.Deadline <= 0 || len(plan.Steps) == 0 || len(plan.Steps) > 16384 {
		return bad()
	}
	cuts := make(map[upgradetx.Key]uint32)
	hostCuts := make(map[string][]uint32)
	converged := make(map[string]uint32)
	released := make(map[string]uint32)
	ids := map[string]bool{plan.Parent.TxID: true}
	slots := make(map[string]string)
	slotDomains := make(map[string]string)
	actor := ""
	for i, step := range plan.Steps {
		grant := step.Grant
		if step.Ordinal != uint32(i+1) || !validKey(step.Participant) || !validHandle(grant.Handle) || grant.Handle.Key != step.Participant || grant.ParentTxID != plan.Parent.TxID || grant.ParentSlot == "" || len(grant.ParentSlot) > 256 || !validHex(grant.ClientNonce, 32) || !validOwner(grant.Issuer) || grant.ActorFP == "" || grant.Mode != upgradetx.ModeHot || !validHex(grant.SourceSHA, 64) || !validHex(grant.TargetSHA, 64) || !validHex(grant.ScopeDigest, 64) || !validHex(grant.CapabilityDigest, 64) || grant.SourceRelease == "" || grant.TargetRelease == "" || grant.ExpiresAt != plan.Deadline || ids[grant.Handle.TxID] {
			return bad()
		}
		ids[grant.Handle.TxID] = true
		if actor != "" && actor != grant.ActorFP {
			return bad()
		}
		actor = grant.ActorFP
		for j, dep := range step.DependsOn {
			if dep == 0 || dep >= step.Ordinal || (j > 0 && step.DependsOn[j-1] >= dep) {
				return bad()
			}
		}
		host := step.Participant.DomainID
		// A host slot is an opaque reservation token, not the domain pathname.
		// Siblings borrow the same token; a token bound to another domain cannot
		// be adopted merely because two components share an artifact SHA.
		// origin: hot-update S8 plan §3.1 / §5.1 / P11
		if slot, found := slots[host]; found && slot != grant.ParentSlot {
			return bad()
		}
		if domain, found := slotDomains[grant.ParentSlot]; found && domain != host {
			return bad()
		}
		slots[host] = grant.ParentSlot
		slotDomains[grant.ParentSlot] = host
		switch step.Kind {
		case upgradetx.StepBrokerCut, upgradetx.StepAgentCut:
			if !slices.Contains(plan.Scope.Participants, step.Participant) || cuts[step.Participant] != 0 || converged[host] != 0 || released[host] != 0 {
				return bad()
			}
			if (step.Kind == upgradetx.StepBrokerCut) != (step.Participant.Component == upgradetx.ComponentBroker) {
				return bad()
			}
			if step.Kind == upgradetx.StepBrokerCut && len(hostCuts[host]) != 0 {
				return bad()
			}
			for _, dep := range hostCuts[host] {
				if !slices.Contains(step.DependsOn, dep) {
					return bad()
				}
			}
			cuts[step.Participant] = step.Ordinal
			hostCuts[host] = append(hostCuts[host], step.Ordinal)
		case upgradetx.StepHomeConverge:
			if len(hostCuts[host]) == 0 || converged[host] != 0 || !slices.Contains(plan.Scope.Participants, step.Participant) {
				return bad()
			}
			for _, dep := range hostCuts[host] {
				if !slices.Contains(step.DependsOn, dep) {
					return bad()
				}
			}
			converged[host] = step.Ordinal
		case upgradetx.StepRelease:
			if i == len(plan.Steps)-1 {
				if step.Participant != plan.Parent.Key || len(released) != len(hostCuts) {
					return bad()
				}
				for _, dep := range released {
					if !slices.Contains(step.DependsOn, dep) {
						return bad()
					}
				}
			} else {
				if converged[host] == 0 || released[host] != 0 || !slices.Contains(step.DependsOn, converged[host]) || !slices.Contains(plan.Scope.Participants, step.Participant) {
					return bad()
				}
				released[host] = step.Ordinal
			}
		default:
			return bad()
		}
	}
	if len(cuts) != len(plan.Scope.Participants) || plan.Steps[len(plan.Steps)-1].Kind != upgradetx.StepRelease || len(hostCuts) != len(released) {
		return bad()
	}
	return nil
}

// checkFleet runs after Reserve and before every step. Including the proposed
// target in the release set catches a third release before dispatch, even if
// the two currently running releases are both on non-voters or helper owners.
// origin: hot-update S8 plan §6.3 / §8.3 M18
func checkFleet(plan upgradetx.Plan, proof FleetProof, nonce string, holderRequired bool) error {
	bad := func() error {
		return refusal(upgradetx.CodeUnsettled, "full fleet or reservation proof is unknown", true)
	}
	if !sameAttempt(proof.Parent, plan.Parent) || proof.Nonce != nonce || !proof.Known || !proof.Complete || (holderRequired && !proof.HolderHeld) || !reflect.DeepEqual(proof.Participants, plan.Scope.Participants) || !validCensus(proof.Census, plan.Parent, plan.Scope, ScopeDigest(plan.Scope), nonce) {
		return bad()
	}
	releases := make(map[string]bool)
	generations := make(map[upgradetx.Key]map[uint64]bool)
	owners := make(map[upgradetx.Owner]FleetMember)
	for _, member := range proof.Members {
		if !member.Known || !validOwner(member.Owner) || member.Release == "" || member.Generation == 0 || !slices.Contains(plan.Scope.Participants, member.Participant) {
			return bad()
		}
		if _, exists := owners[member.Owner]; exists {
			return bad()
		}
		owners[member.Owner] = member
		releases[member.Release] = true
		if generations[member.Participant] == nil {
			generations[member.Participant] = make(map[uint64]bool)
		}
		generations[member.Participant][member.Generation] = true
		if len(generations[member.Participant]) > upgradebudget.MaxGenerations {
			return bad()
		}
	}
	for _, key := range plan.Scope.Participants {
		if len(generations[key]) == 0 {
			return bad()
		}
	}
	for _, resource := range proof.Census.Resources {
		member, found := owners[resource.Owner]
		if !found || member.Generation != resource.Generation {
			return bad()
		}
	}
	for _, step := range plan.Steps {
		if isCut(step) {
			releases[step.Grant.TargetRelease] = true
		}
	}
	if len(releases) > upgradebudget.MaxGenerations {
		return refusal(upgradetx.CodeRejected, "full fleet would contain a third release", true)
	}
	return nil
}

func isCut(step upgradetx.Step) bool {
	return step.Kind == upgradetx.StepBrokerCut || step.Kind == upgradetx.StepAgentCut
}
