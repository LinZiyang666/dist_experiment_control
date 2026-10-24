//go:build !linux

package upgradecli

import (
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// S8 same-PID selection and its kernel identity witnesses are Linux-only. A
// registered component on another OS refuses boot rather than running bytes
// whose selector it cannot verify.
// origin: hot-update S8 plan §5.2 / §11.2 P19
func bootProcess() (string, upgradetx.Owner, error) {
	return "", upgradetx.Owner{}, &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "registered upgrade selection requires Linux"}
}

// ProcessOwner retains the CLI's fail-closed platform contract.
func ProcessOwner(pid int) (upgradetx.Owner, error) { return upgradestage.ProcessOwner(pid) }
