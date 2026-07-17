//go:build linux

package cluster

import (
	"errors"

	"golang.org/x/sys/unix"
)

// exchangedir_linux.go — the Linux fast path for the offline raft-store swap.
//
// Round-5 S5-01/S5-24: RENAME_EXCHANGE is a Linux-only syscall (`golang.org/x/sys/unix` only defines
// Renameat2/RENAME_EXCHANGE in its linux build), and it is not supported by every filesystem (notably some
// overlayfs/network mounts return EINVAL/ENOSYS/EOPNOTSUPP). Keeping it behind a build tag + a capability
// error lets `cmd/tether` build for darwin (goreleaser ships darwin/amd64+arm64) and lets the caller fall
// back instead of failing AFTER the roster prune has already mutated the node.

// errExchangeUnsupported reports that an ATOMIC directory exchange is unavailable on this
// platform/filesystem. The caller MUST fall back to a non-atomic swap (and say so).
var errExchangeUnsupported = errors.New("cluster: atomic directory exchange (RENAME_EXCHANGE) unsupported on this platform/filesystem")

// exchangeDirs atomically swaps the two directories in one syscall: after it returns nil, a holds what b
// held and vice versa, with NO window in which either path is missing. Returns errExchangeUnsupported when
// the kernel or filesystem cannot do it.
func exchangeDirs(a, b string) error {
	err := unix.Renameat2(unix.AT_FDCWD, a, unix.AT_FDCWD, b, unix.RENAME_EXCHANGE)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, unix.EINVAL), errors.Is(err, unix.ENOSYS), errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.ENOTTY):
		// EINVAL is what most filesystems that lack RENAME_EXCHANGE return; ENOSYS is a pre-3.15 kernel.
		return errExchangeUnsupported
	default:
		return err
	}
}
