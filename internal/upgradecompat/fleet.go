// Package upgradecompat counts execution generations from validated custody.
// Retained terminals occupy a generation; durable/released terminals do not.
// origin: S8 internal review rv-txn F11
package upgradecompat

import (
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Participant identifies one managed broker, agent, sibling or retained helper.
// Voter is descriptive only: every participant contributes to the release set.
// PendingTarget is the reserved release of an unresolved accepted transaction,
// even before those bytes start running. Historical artifact pins are excluded.
type Participant struct {
	// Helpers and co-located siblings need independent identities even when
	// they share a binary pathname or configured node with another participant.
	ID        string
	Component upgradetx.Component
	Voter     bool
	Helper    bool
	// Offline is still managed. Its last advertised release cannot certify a
	// current observation; uncertainty must remain explicit in Known.
	Known   bool
	Release string
	Pending bool
	// Count reserved target bytes before activation so two concurrent plans
	// cannot each pass against a census containing only the running releases.
	PendingTarget string
}

// FleetCensus is sampled after acquiring the common release reservation and
// again before every step. Expected is the immutable full participant snapshot
// owned by the parent; unavailable members stay in it. Reserved attests to the
// real P11 lease, not a lock implemented by this pure package. Missing/extra
// members require explicit replanning, never a reduced success scope.
type FleetCensus struct {
	Stamp        upgradetx.Stamp
	Known        bool
	Complete     bool
	Reserved     bool
	Expected     []string
	Participants []Participant
}

// CheckFleet includes the proposed release and all pending target releases.
// This is a predicate under the reservation owner's serialization boundary;
// calling it without holding that reservation cannot prevent competing Start.
// origin: hot-update S8 plan §6.3 / TestFullFleetReleaseReservation
func CheckFleet(census FleetCensus, targetRelease string) error {
	if targetRelease == "" {
		return reject("fleet_target_unknown")
	}
	if !census.Known || !census.Complete || !census.Reserved || len(census.Expected) == 0 {
		return deferUpgrade("fleet_census_incomplete")
	}
	// Compare by original participant identity. Looking only at row counts
	// would let one new registration replace a missing offline participant.
	expected := make(map[string]bool, len(census.Expected))
	for _, id := range census.Expected {
		if id == "" || expected[id] {
			return reject("fleet_roster_invalid")
		}
		expected[id] = true
	}
	seen := make(map[string]bool, len(census.Participants))
	// Include the proposed release before examining members. A fleet already
	// holding A and B cannot introduce C merely because C has not started yet.
	releases := map[string]bool{targetRelease: true}
	for _, participant := range census.Participants {
		if participant.ID == "" || seen[participant.ID] {
			return reject("fleet_participant_invalid")
		}
		if !expected[participant.ID] {
			return deferUpgrade("fleet_roster_changed")
		}
		seen[participant.ID] = true
		if participant.Component != upgradetx.ComponentBroker && participant.Component != upgradetx.ComponentAgent {
			return reject("fleet_component_unsupported")
		}
		// Unknown members are unresolved responsibility, not permission to omit
		// them. The same rule applies to retained helpers and non-voters.
		if !participant.Known || participant.Release == "" {
			return deferUpgrade("fleet_release_unknown")
		}
		releases[participant.Release] = true
		// An unresolved target cannot be assigned the running release by default.
		// Doing so would erase the reservation for a different pending artifact.
		if participant.Pending && participant.PendingTarget == "" {
			return deferUpgrade("fleet_pending_unknown")
		}
		if participant.PendingTarget != "" {
			releases[participant.PendingTarget] = true
		}
	}
	if len(seen) != len(expected) {
		return deferUpgrade("fleet_roster_incomplete")
	}
	// This limit is separate from generation accounting. A helper with a retained
	// execution right counts even when all configured voters report one release.
	// A competing reserved release can disappear only after its owner settles;
	// defer this attempt rather than inviting a driver to start another plan.
	if len(releases) > 2 {
		return deferUpgrade("fleet_third_release")
	}
	return nil
}

// CheckGenerations validates a component's final resource census before its
// next cut. The exporter itself and proposed successor count even when idle;
// retained owners count until retirement, while rollback artifact pins do not.
// Current is explicit: the census Stamp identifies the new attempt, whose
// Generation is the target, not the exporting runtime's generation.
// origin: hot-update S8 plan §6.3 / TestLiveGenerationLimitIncludesRetainedOwners
func CheckGenerations(census upgradetx.Census, current, target uint64) error {
	// Admission and seal share the collector's sequence barrier. A positive
	// in-flight count prevents a stale empty census from certifying the next cut.
	if !census.Known || !census.Complete || census.Admitting != 0 || !validSHA(census.ExpectedDigest) {
		return deferUpgrade("generation_census_incomplete")
	}
	// A reverse cut also advances generation; choosing old code cannot revive
	// the old generation's execution identity. Reject overflow before addition.
	// origin: S8 internal review rv3-leaves F6
	// Cancelled allocations advance the grant high-water without becoming live
	// owners. Store CAS excludes reuse; compatibility counts actual live owners.
	if current == 0 || target <= current {
		return reject("generation_target_invalid")
	}
	if !validOwner(census.Stamp.Producer) {
		return deferUpgrade("generation_owner_unknown")
	}
	// Even an idle exporter still exists. Counting only resources could admit
	// a third owner when the current generation happens to have no task rows.
	generations := map[uint64]bool{current: true, target: true}
	seen := make(map[string]bool, len(census.Resources))
	owners := make(map[upgradetx.Owner]bool, len(census.Resources))
	for _, resource := range census.Resources {
		// Identity is domain-scoped; the same textual ID in two domains is legal.
		id := string(resource.Domain) + "\x00" + resource.ID
		if resource.ID == "" || resource.Domain == "" || seen[id] {
			return reject("generation_resource_invalid")
		}
		seen[id] = true
		// origin: S8 internal review rv-txn F11
		// Historical terminal evidence keeps its identity without live custody.
		if !upgradetx.ResourceNotApplicable(resource) && (resource.Generation == 0 || !validOwner(resource.Owner)) {
			return deferUpgrade("generation_owner_unknown")
		}
		if err := upgradetx.ValidateResource(resource); err != nil {
			return reject("generation_resource_invalid")
		}
		if upgradetx.ResourceReleased(resource) || upgradetx.ResourceNotApplicable(resource) {
			continue
		}
		// PID alone cannot identify a retained owner across reuse or a restart.
		// Never guess its generation from the attempt currently being prepared.
		if resource.Generation == 0 || !validOwner(resource.Owner) {
			return deferUpgrade("generation_owner_unknown")
		}
		if resource.Generation > current {
			return reject("generation_resource_future")
		}
		generations[resource.Generation] = true
		owners[resource.Owner] = true
	}
	// An old owner with no corresponding resource has unknown generation or
	// custody. It cannot be treated as released because its PID disappeared.
	for _, owner := range census.OldOwners {
		if !validOwner(owner) || !owners[owner] {
			return deferUpgrade("generation_owner_unknown")
		}
	}
	if len(generations) > upgradebudget.MaxGenerations {
		return reject("generation_third_live")
	}
	return nil
}
