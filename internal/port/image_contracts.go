// origin: hot-update S6 plan §2.1 / §12.1 P00
package port

import (
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
)

// A guard is created only inside the live Node.Propose database callback.
// Empty maintenance values explicitly mean absence; caller booleans are no proof.
type MutationGuard struct {
	Expected            *Allocation
	ExpectAbsent        bool
	FromHome            string
	ToHome              string
	ExpectedMaintenance map[string]string
	Proofs              map[string]brokermaint.LocalLease
	NowBootNS           int64
	// The original allocation/maintenance guard is still required; authority is
	// an additional fence against SID reuse, including an absent allocation.
	// origin: hot-update S8 plan §6.2 / P35
	Authority upgradeauthority.Guard
}
type AllocationIntent struct {
	SID         string
	NID         string
	Name        string
	LocalPort   int
	DesiredPort int
	CreatedByFP string
	HomeBroker  string
	RebuildOff  bool
	Config      *Config
}

// A valid creation for another SID must not authorize this allocation's write.
// Empty authority is the legacy contract; a partially filled guard fails closed.
// origin: hot-update S8 plan §6.2 / TestPortMutationRejectsRecreatedSession
func allocationAuthorityMatches(guard upgradeauthority.Guard, sid, nid string) bool {
	return guard == (upgradeauthority.Guard{}) || (guard.SID == sid && guard.Incarnation != "" &&
		(guard.ConfiguredNID == "" || guard.ConfiguredNID == nid))
}
