//go:build !linux

package infraroot

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
)

// origin: hot-update S9 plan §10.2 / T83 — a foreign process list cannot prove the managed launcher.
func installedMainProcess(infrahost.UnitTarget, c.Deadline) (c.Process, error) {
	return c.Process{}, c.ErrUnsupported
}
