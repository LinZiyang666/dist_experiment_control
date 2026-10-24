//go:build !linux

package agentimage

import "github.com/LinZiyang666/tether/internal/sealedstate"

// Reader capability does not make Linux sealed-state ownership portable.
// origin: hot-update S5 plan §7.4
func retainDomainSnapshot(*sealedstate.Snapshot) (*sealedstate.Snapshot, error) {
	return nil, sealedstate.ErrUnsupported
}
