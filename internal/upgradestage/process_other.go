//go:build !linux

package upgradestage

import "github.com/LinZiyang666/tether/internal/upgradetx"

// ProcessOwner has no kernel start-tick witness on this OS.
func ProcessOwner(int) (upgradetx.Owner, error) {
	return upgradetx.Owner{}, &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "process identity requires Linux"}
}
