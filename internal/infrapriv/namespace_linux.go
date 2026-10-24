//go:build linux

package infrapriv

import (
	"os"
	"strconv"

	"golang.org/x/sys/unix"
)

// NewPinnedClient is the dirfd form of the fixed control.sock endpoint. Trusted
// namespace composition supplies the directory and numeric owner; no request,
// command-line argument or environment variable can select it. Tests use private
// unprivileged namespaces without weakening the production root-peer checks.
// origin: S9 internal review rv-infra F03
func NewPinnedClient(directory *os.File, uid, gid uint32) (*Client, error) {
	if directory == nil {
		return nil, ErrPermission
	}
	var st unix.Stat_t
	if unix.Fstat(int(directory.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != uid || st.Mode&0022 != 0 {
		return nil, ErrPermission
	}
	if unix.Fstatat(int(directory.Fd()), "control.sock", &st, unix.AT_SYMLINK_NOFOLLOW) != nil || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Uid != uid || st.Gid != gid || st.Mode&07777 != 0660 {
		return nil, ErrPermission
	}
	return &Client{path: "/proc/self/fd/" + strconv.FormatUint(uint64(directory.Fd()), 10) + "/control.sock", serverUID: uid, gid: gid}, nil
}

// NewPinnedWorkerLimit duplicates a trusted namespace directory. The same four
// permanent OFD slots apply to both systemd workers and hermetic socket fixtures.
func NewPinnedWorkerLimit(directory *os.File) (*WorkerLimit, error) {
	if directory == nil {
		return nil, ErrPermission
	}
	var st unix.Stat_t
	if unix.Fstat(int(directory.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != uint32(os.Geteuid()) || st.Mode&0022 != 0 {
		return nil, ErrPermission
	}
	fd, err := unix.FcntlInt(directory.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	return &WorkerLimit{dir: os.NewFile(uintptr(fd), "infra-worker-limit"), owner: st.Uid}, nil
}
