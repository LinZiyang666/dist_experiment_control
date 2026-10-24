//go:build linux

package infralaunch

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// inheritedLock observes the lock on the target's actual open description.
// Merely finding the same inode under /proc/PID/fd would also accept a freshly
// opened, unlocked descriptor. Linux reports the FLOCK entry only on descriptions
// that own it; SCM_RIGHTS and exec retain that same kernel lock identity.
// origin: S9 internal review rv-infra F04
func inheritedLock(lock *Lock, process infracontract.Process) error {
	if err := lock.verify(); err != nil {
		return err
	}
	own, err := os.ReadFile("/proc/self/fdinfo/" + strconv.FormatUint(uint64(lock.file.Fd()), 10))
	if err != nil {
		return ErrLaunchRejected
	}
	record := flockRecord(own)
	if record == "" {
		return ErrLaunchRejected
	}
	base := "/proc/" + strconv.Itoa(int(process.PID))
	entries, err := os.ReadDir(base + "/fd")
	if err != nil {
		return ErrLaunchRejected
	}
	for _, entry := range entries {
		var st unix.Stat_t
		if unix.Stat(filepath.Join(base, "fd", entry.Name()), &st) != nil || uint64(st.Dev) != lock.store.LockDevice || st.Ino != lock.store.LockInode {
			continue
		}
		data, err := os.ReadFile(filepath.Join(base, "fdinfo", entry.Name()))
		if err == nil && flockRecord(data) == record {
			return nil
		}
	}
	return ErrLaunchRejected
}

func flockRecord(data []byte) string {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 9 && fields[0] == "lock:" && fields[2] == "FLOCK" && fields[3] == "ADVISORY" && fields[4] == "WRITE" && fields[7] == "0" && fields[8] == "EOF" {
			return strings.Join(fields[2:], " ")
		}
	}
	return ""
}
