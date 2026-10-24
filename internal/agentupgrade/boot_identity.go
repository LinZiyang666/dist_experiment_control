package agentupgrade

import (
	"sync"

	"github.com/LinZiyang666/tether/internal/agentupgradeport"
)

// upgradeBootProof is populated only by the pre-Cobra boot shim in this
// process. Unlike BootCount (shared on disk) or a path-based SHA (borrowable
// after rename on non-Linux systems), the marker's unique UpgradeID can be
// carried into Agent.New only by a process that actually traversed the
// pending boot decision.
var upgradeBootProof struct {
	sync.RWMutex
	id string
}

func RememberBootProof(id string) {
	upgradeBootProof.Lock()
	upgradeBootProof.id = id
	upgradeBootProof.Unlock()
}

func CurrentBootProof() string {
	upgradeBootProof.RLock()
	defer upgradeBootProof.RUnlock()
	return upgradeBootProof.id
}

func Nonce() string { return agentupgradeport.Nonce() }

// The original running-image invariant and implementation live together in
// agentupgradeport/running_image.go, beside the executing-inode cache.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.2
func RunningImageSHA(path string, executable func() (string, error)) (string, error) {
	return agentupgradeport.RunningImageSHA(path, executable)
}
