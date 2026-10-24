package agentupgradeport

import (
	"errors"

	"golang.org/x/sys/unix"
)

// ctime detects an in-place rewrite even when an owner restores mtime and size.
// Directory timestamps are excluded: the journal's atomic rename may change
// them on every action without changing the directory's authority or identity.
// origin: hot-update S8 plan §6.1 / §10; S8 internal review FREEZE-WINDOW R6
type evidenceStamp struct {
	device, inode     uint64
	mode, uid, gid    uint32
	size              int64
	modified, changed unix.Timespec
}

func readEvidenceStamp(path string) (evidenceStamp, error) {
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		return evidenceStamp{}, err
	}
	kind := stat.Mode & unix.S_IFMT
	if kind != unix.S_IFREG && kind != unix.S_IFDIR {
		return evidenceStamp{}, errors.New("reader evidence path is not a regular file or directory")
	}
	stamp := evidenceStamp{device: stat.Dev, inode: stat.Ino, mode: stat.Mode, uid: stat.Uid, gid: stat.Gid}
	if kind == unix.S_IFREG {
		stamp.size, stamp.modified, stamp.changed = stat.Size, stat.Mtim, stat.Ctim
	}
	return stamp, nil
}
