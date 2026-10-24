//go:build linux

package upgradestage

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ProcessOwner reads the boot/PID/start-ticks triple of a live process. PID
// alone is never an identity; the start tick binds it to one incarnation.
// InstanceID is left to the caller, which knows whose identity it states.
func ProcessOwner(pid int) (upgradetx.Owner, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || pid <= 0 {
		return upgradetx.Owner{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "process identity is unavailable"}
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return upgradetx.Owner{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "process identity is unavailable"}
	}
	// comm may contain spaces and ')': fields are counted after the LAST ')'.
	line := string(stat)
	end := strings.LastIndexByte(line, ')')
	fields := strings.Fields(line[end+1:])
	if end < 0 || len(fields) < 20 {
		return upgradetx.Owner{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "process identity is malformed"}
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 {
		return upgradetx.Owner{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "process identity is malformed"}
	}
	return upgradetx.Owner{BootID: strings.TrimSpace(string(boot)), PID: pid, StartTicks: ticks}, nil
}
