package agentupgrade

import (
	"github.com/LinZiyang666/tether/internal/agentidentity"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ProcessOwner retains the engine API while the pure evidence reader lives
// beside the running-image reader. The original invariant comment moved intact.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
func ProcessOwner(instanceID string) (upgradetx.Owner, error) {
	return agentupgradeport.ProcessOwner(instanceID)
}

// origin: hot-update S8 internal review FIX-AGENTSLIM.
func ReadBootID() string { return agentidentity.ReadBootID() }

// origin: hot-update S8 internal review FIX-AGENTSLIM.
func ReadStartTimeTicks(osPID int) (int64, error) {
	return agentidentity.ReadStartTimeTicks(osPID)
}
