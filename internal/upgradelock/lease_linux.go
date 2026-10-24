//go:build linux

package upgradelock

import (
	"os"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// fdinfo reports locks belonging to this open file description. Merely opening
// the same inode while somebody else holds its flock produces no such record.
// Do not test ownership by taking another flock: that can create false custody
// when the original owner has already gone away.
// origin: hot-update S8 plan §5.1 / M08
func requireHeldOFD(fd uintptr) error {
	info, err := os.ReadFile("/proc/self/fdinfo/" + strconv.FormatUint(uint64(fd), 10))
	if err == nil {
		for _, line := range strings.Split(string(info), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 9 && fields[0] == "lock:" && fields[2] == "FLOCK" && fields[3] == "ADVISORY" && fields[4] == "WRITE" && fields[7] == "0" && fields[8] == "EOF" {
				return nil
			}
		}
	}
	return refusal(upgradetx.CodeRejected, "inherited descriptor has no original reservation flock")
}
