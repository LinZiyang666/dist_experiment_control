//go:build linux

package upgradecli

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// bootProcess reads this process's running image digest from /proc/self/exe
// (the kernel's mapping, never the pathname it was started as) and a
// read-only journal identity. The reader identity is never an issuer: boot
// selection only Loads, so no grant can be minted under it.
// origin: hot-update S8 plan §5.2 / §11.2 P19
func bootProcess() (string, upgradetx.Owner, error) {
	running, err := upgradeartifact.FileSHA("/proc/self/exe")
	if err != nil {
		return "", upgradetx.Owner{}, err
	}
	owner, err := ProcessOwner(os.Getpid())
	if err != nil {
		return "", upgradetx.Owner{}, err
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("tether-canonical-boot\x00%s\x00%d\x00%d", owner.BootID, owner.PID, owner.StartTicks)))
	owner.InstanceID = hex.EncodeToString(sum[:16])
	return running, owner, nil
}

// ProcessOwner retains the CLI's kernel-identity contract.
func ProcessOwner(pid int) (upgradetx.Owner, error) { return upgradestage.ProcessOwner(pid) }
