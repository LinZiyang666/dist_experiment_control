package brokercontrol

import (
	"fmt"
	"syscall"
)

// diskUsage returns (used, total) bytes on the filesystem containing
// path, via syscall.Statfs. Linux-only; Windows / Plan 9 builds would
// need a different implementation but v1 ships Linux-only (see
// architecture I.5).
func DiskUsage(path string) (used, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	// Statfs_t.Bsize is int64 on linux, so the conversion is real. (There used to be a
	// //nolint here naming unconvert+gosec; neither is enabled, so it suppressed nothing while
	// reading as a considered exemption. TestNolintDirectivesNameEnabledLinters now forbids that.)
	bsize := uint64(st.Bsize)
	total = uint64(st.Blocks) * bsize
	free := uint64(st.Bavail) * bsize
	if free > total {
		return 0, total, nil
	}
	used = total - free
	return used, total, nil
}
