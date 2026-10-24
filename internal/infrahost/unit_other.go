//go:build !linux

package infrahost

import "github.com/LinZiyang666/tether/internal/infracontract"

func unitPlatform() error { return infracontract.ErrUnsupported }

func NewSystemdManager() (UnitManager, error) {
	return nil, infracontract.ErrUnsupported
}
