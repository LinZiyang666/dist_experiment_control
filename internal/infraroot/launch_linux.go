//go:build linux

package infraroot

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"os"
	"strconv"
	"strings"
)

// The managed cgroup must contain only its fixed main launcher. Accepting a
// same-UID process merely for residing somewhere below the unit is insufficient.
func installedMainProcess(target infrahost.UnitTarget, d c.Deadline) (c.Process, error) {
	name, err := infrahost.UnitName(target)
	if err != nil {
		return c.Process{}, err
	}
	group := "/system.slice/" + name
	data, err := os.ReadFile("/sys/fs/cgroup" + group + "/cgroup.procs")
	if err != nil {
		return c.Process{}, c.ErrIncomplete
	}
	fields := strings.Fields(string(data))
	if len(fields) != 1 {
		return c.Process{}, c.ErrIncomplete
	}
	pid, err := strconv.ParseInt(fields[0], 10, 32)
	if err != nil {
		return c.Process{}, c.ErrIncomplete
	}
	process, err := infrahost.ReadProcess(int32(pid), d)
	if err != nil || process.Cgroup != group {
		return c.Process{}, c.ErrIncomplete
	}
	return process, nil
}
