package upgradecompat

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §6.3 / §9.1 / P06
func TestFullFleetReleaseReservation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*FleetCensus)
		want string
		code string
	}{
		{"full fleet two releases", func(c *FleetCensus) {}, "", ""},
		{"no reservation", func(c *FleetCensus) { c.Reserved = false }, "fleet_census_incomplete", upgradetx.CodeDeferred},
		{"unknown census", func(c *FleetCensus) { c.Known = false }, "fleet_census_incomplete", upgradetx.CodeDeferred},
		{"incomplete census", func(c *FleetCensus) { c.Complete = false }, "fleet_census_incomplete", upgradetx.CodeDeferred},
		{"unobserved empty fleet", func(c *FleetCensus) { c.Expected = nil; c.Participants = nil }, "fleet_census_incomplete", upgradetx.CodeDeferred},
		{"missing offline participant", func(c *FleetCensus) { c.Participants = c.Participants[:len(c.Participants)-1] }, "fleet_roster_incomplete", upgradetx.CodeDeferred},
		{"new registration requires replan", func(c *FleetCensus) {
			c.Participants = append(c.Participants, Participant{ID: "new", Component: upgradetx.ComponentAgent, Known: true, Release: "release-a"})
		}, "fleet_roster_changed", upgradetx.CodeDeferred},
		{"duplicate expected", func(c *FleetCensus) { c.Expected = append(c.Expected, c.Expected[0]) }, "fleet_roster_invalid", upgradetx.CodeRejected},
		{"empty expected", func(c *FleetCensus) { c.Expected[0] = "" }, "fleet_roster_invalid", upgradetx.CodeRejected},
		{"duplicate participant", func(c *FleetCensus) { c.Participants[1].ID = c.Participants[0].ID }, "fleet_participant_invalid", upgradetx.CodeRejected},
		{"empty participant", func(c *FleetCensus) { c.Participants[1].ID = "" }, "fleet_participant_invalid", upgradetx.CodeRejected},
		{"infrastructure participant", func(c *FleetCensus) { c.Participants[1].Component = upgradetx.ComponentCaddy }, "fleet_component_unsupported", upgradetx.CodeRejected},
		{"unresolved transaction lacks target", func(c *FleetCensus) { c.Participants[0].Pending = true }, "fleet_pending_unknown", upgradetx.CodeDeferred},
		{"concurrent reservation before target starts", func(c *FleetCensus) { c.Participants[0].Pending = true; c.Participants[0].PendingTarget = "release-c" }, "fleet_third_release", upgradetx.CodeDeferred},
		{"retained pending target cannot disappear", func(c *FleetCensus) { c.Participants[0].PendingTarget = "release-c" }, "fleet_third_release", upgradetx.CodeDeferred},
		{"pending same release does not add generation", func(c *FleetCensus) { c.Participants[0].Pending = true; c.Participants[0].PendingTarget = "release-b" }, "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			census := validFleet()
			test.edit(&census)
			assertRefusal(t, CheckFleet(census, "release-b"), test.want, test.code)
		})
	}
	// The four non-voter categories are all adversarial: each alone can be the
	// second existing release that makes the proposed third release illegal.
	for _, id := range []string{"nonvoter", "agent", "sibling", "helper"} {
		for _, defect := range []string{"third release", "unknown", "empty release"} {
			t.Run(id+"/"+defect, func(t *testing.T) {
				census := validFleet()
				want, code := "fleet_third_release", upgradetx.CodeDeferred
				for i := range census.Participants {
					if census.Participants[i].ID != id {
						continue
					}
					switch defect {
					case "third release":
						census.Participants[i].Release = "release-c"
					case "unknown":
						census.Participants[i].Known = false
						want, code = "fleet_release_unknown", upgradetx.CodeDeferred
					case "empty release":
						census.Participants[i].Release = ""
						want, code = "fleet_release_unknown", upgradetx.CodeDeferred
					}
				}
				assertRefusal(t, CheckFleet(census, "release-b"), want, code)
			})
		}
	}
	assertRefusal(t, CheckFleet(validFleet(), "release-c"), "fleet_third_release", upgradetx.CodeDeferred)
	assertRefusal(t, CheckFleet(validFleet(), ""), "fleet_target_unknown", upgradetx.CodeRejected)
}

// origin: hot-update S8 plan §6.3 / §11.2 / P06
func TestLiveGenerationLimitIncludesRetainedOwners(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*upgradetx.Census, *uint64, *uint64)
		want string
		code string
	}{
		{"generation seven is independent of ABI epoch one", func(c *upgradetx.Census, current, next *uint64) {}, "", ""},
		{"observed idle still counts exporter", func(c *upgradetx.Census, current, next *uint64) { c.Resources = nil }, "", ""},
		{"retained worker blocks third generation", func(c *upgradetx.Census, current, next *uint64) { c.Resources[0].Generation = 6 }, "generation_third_live", upgradetx.CodeRejected},
		{"unknown census cannot mean empty", func(c *upgradetx.Census, current, next *uint64) { c.Resources = nil; c.Known = false }, "generation_census_incomplete", upgradetx.CodeDeferred},
		{"incomplete census", func(c *upgradetx.Census, current, next *uint64) { c.Complete = false }, "generation_census_incomplete", upgradetx.CodeDeferred},
		{"unsealed expected set", func(c *upgradetx.Census, current, next *uint64) { c.ExpectedDigest = "" }, "generation_census_incomplete", upgradetx.CodeDeferred},
		{"admission not sealed", func(c *upgradetx.Census, current, next *uint64) { c.Admitting = 1 }, "generation_census_incomplete", upgradetx.CodeDeferred},
		{"unknown current generation", func(c *upgradetx.Census, current, next *uint64) { *current = 0 }, "generation_target_invalid", upgradetx.CodeRejected},
		{"unknown target generation", func(c *upgradetx.Census, current, next *uint64) { *next = 0 }, "generation_target_invalid", upgradetx.CodeRejected},
		{"target cannot reuse current", func(c *upgradetx.Census, current, next *uint64) { *next = 7 }, "generation_target_invalid", upgradetx.CodeRejected},
		{"target cannot regress generation", func(c *upgradetx.Census, current, next *uint64) { *next = 6 }, "generation_target_invalid", upgradetx.CodeRejected},
		{"generation cannot wrap", func(c *upgradetx.Census, current, next *uint64) { *current = ^uint64(0); *next = 1 }, "generation_target_invalid", upgradetx.CodeRejected},
		{"unknown producer", func(c *upgradetx.Census, current, next *uint64) { c.Stamp.Producer = upgradetx.Owner{} }, "generation_owner_unknown", upgradetx.CodeDeferred},
		{"missing resource identity", func(c *upgradetx.Census, current, next *uint64) { c.Resources[0].ID = "" }, "generation_resource_invalid", upgradetx.CodeRejected},
		{"missing resource domain", func(c *upgradetx.Census, current, next *uint64) { c.Resources[0].Domain = "" }, "generation_resource_invalid", upgradetx.CodeRejected},
		{"duplicate resource", func(c *upgradetx.Census, current, next *uint64) { c.Resources = append(c.Resources, c.Resources[0]) }, "generation_resource_invalid", upgradetx.CodeRejected},
		{"same ID in different domains", func(c *upgradetx.Census, current, next *uint64) {
			r := c.Resources[0]
			r.Domain = upgradetx.DomainTransferTracker
			c.Resources = append(c.Resources, r)
		}, "", ""},
		{"unknown retained generation", func(c *upgradetx.Census, current, next *uint64) { c.Resources[0].Generation = 0 }, "generation_owner_unknown", upgradetx.CodeDeferred},
		{"unknown retained owner", func(c *upgradetx.Census, current, next *uint64) { c.Resources[0].Owner.StartTicks = 0 }, "generation_owner_unknown", upgradetx.CodeDeferred},
		{"future resource", func(c *upgradetx.Census, current, next *uint64) { c.Resources[0].Generation = 8 }, "generation_resource_future", upgradetx.CodeRejected},
		{"old owner lacking generation evidence", func(c *upgradetx.Census, current, next *uint64) {
			owner := c.Stamp.Producer
			owner.StartTicks++
			c.OldOwners = []upgradetx.Owner{owner}
		}, "generation_owner_unknown", upgradetx.CodeDeferred},
		{"known old owner with retained resource", func(c *upgradetx.Census, current, next *uint64) {
			c.OldOwners = []upgradetx.Owner{c.Resources[0].Owner}
		}, "", ""},
		{"unknown old owner", func(c *upgradetx.Census, current, next *uint64) { c.OldOwners = []upgradetx.Owner{{}} }, "generation_owner_unknown", upgradetx.CodeDeferred},
	} {
		t.Run(test.name, func(t *testing.T) {
			census := validPreflight(upgradetx.ActionDispatch).Census
			census.Resources = []upgradetx.Resource{{Domain: upgradetx.DomainTransferWorker, ID: "retained-worker", Origin: "original", State: "active", Custody: "retained", Generation: 7, Owner: census.Stamp.Producer}}
			current, next := uint64(7), uint64(8)
			test.edit(&census, &current, &next)
			assertRefusal(t, CheckGenerations(census, current, next), test.want, test.code)
		})
	}
}

func validFleet() FleetCensus {
	return FleetCensus{Known: true, Complete: true, Reserved: true, Expected: []string{"voter", "nonvoter", "agent", "sibling", "helper"}, Participants: []Participant{
		{ID: "voter", Component: upgradetx.ComponentBroker, Voter: true, Known: true, Release: "release-a"},
		{ID: "nonvoter", Component: upgradetx.ComponentBroker, Known: true, Release: "release-a"},
		{ID: "agent", Component: upgradetx.ComponentAgent, Known: true, Release: "release-a"},
		{ID: "sibling", Component: upgradetx.ComponentAgent, Known: true, Release: "release-b"},
		{ID: "helper", Component: upgradetx.ComponentBroker, Helper: true, Known: true, Release: "release-a"},
	}}
}

// origin: S8 internal review rv-txn F11
func TestTerminalDispositionKeepsOnlyActualLiveGenerations(t *testing.T) {
	for _, custody := range []string{"released", "durable", "retained", "unknown"} {
		t.Run(custody, func(t *testing.T) {
			c := validPreflight(upgradetx.ActionDispatch).Census
			c.Resources = []upgradetx.Resource{{Domain: upgradetx.DomainExec, ID: "terminal", Origin: "original", State: "terminal", Custody: custody, Generation: 1, Owner: c.Stamp.Producer}}
			err := CheckGenerations(c, 2, 3)
			if custody == "released" || custody == "durable" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("retained or unknown custody allowed a third generation")
			}
		})
	}
}
